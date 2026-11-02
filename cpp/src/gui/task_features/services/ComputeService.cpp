// Compute Service Implementation
#include "gui/task_features/services/ComputeService.hpp"
#include "features/Method/Fundamental.hpp"
#include "features/Node/Common/Label/LabelHold.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "features/Storage/FeatureSnap.hpp"
#include "features/Storage/FeatureStoreConfig.hpp" // with_tier
#include "misc/logging.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/FeatureLib.hpp"
#include "shared/Pool.hpp"
#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <optional>
#include <string>
#include <vector>

namespace GUI::Features {

static_assert(ComputeConfig::kMinRingSlots == static_cast<int>(LabelHold::MIN_RING_SLOTS), "ComputeConfig::kMinRingSlots 与 LabelHold::MIN_RING_SLOTS 不符");
static_assert(TIER_COUNT == 2 && static_cast<size_t>(TierId::Lob) == 0 && static_cast<size_t>(TierId::Bar) == 1, "ComputeConfig 按档默认值按 Lob / Bar 序写死");

ComputeService::ComputeService(SharedData &data)
    : data_(data) {
  Start();
}

std::vector<misc::Ymd> source_dates(const SharedData &data, TierId tier) {
  return with_tier(tier, [&]<class T>() { return ::source_dates<T>(data); });
}

bool resumable(const Config &cfg, const FeatureLib &lib) {
  if (!lib.ready())
    return false;
  if (axis_lib(lib.id) != lib.id && load_lib(cfg, axis_lib(lib.id)).axis.hash != lib.axis.hash)
    return false;
  const SnapHeader want = with_tier(lib.tier(), [&]<class T>() { return snap_header<T>(lib, lib.dates.back()); });
  return FeatureSnap::head(lib.dir) == want;
}

void ComputeService::Request(ComputeConfig config) {
  for (const TierId t : TIER_IDS)
    assert(config.pf_cores[t] >= 1 && config.cs_cores[t] >= 1 && config.io_cores[t] >= 1 && "预取 / 截面 / 落盘各至少 1 核");
  assert(config.adopt_pct >= 0 && config.adopt_pct <= 100 && "adopt pct must be in [0, 100]");
  assert(!config.dates.empty() && std::is_sorted(config.dates.begin(), config.dates.end()) && "日期轴为空 / 非升序");
  Service::Request(config);
}

// 每库同一流程: 规划本库 → 基本面面板 → 池面板 (A 轴 + 逐日成员) → 清库 + 落清单 → 日志 → pipeline<T> (见 worker/feature_pipeline.hpp)
// 一库一趟 TS + CS, 不拆不跨池共享 (约束与理由见 FeatureLib.hpp 文件头)
// 续算同一流程, 只差: 轴冻结为旧清单轴, 管线日期轴 = 尾窗 + 新日, 不清库, 跑完才落新清单
bool ComputeService::run(ComputeConfig &config) {
  // 规划 (日期轴 × A 轴; 写库清单 = 读端唯一来源, 见 FeatureLib.hpp):
  //   日期轴 = 请求带来的规划 (源日历截到依赖库末日, 见 source_dates)
  //   A 轴归池 (pool::Panel): 定轴库全量 = 池的自然轴, 续算 = 旧清单轴; 其余档的库 = 定轴库清单轴 (同池各档同一 A 轴)
  //   逐日成员 = Meta cs_valid (all = 当日活跃; 子池 = all Bar 库 D−1 收盘行的池列)
  //   Lob 截面旁读同池 Bar 库
  // 张量/调度/落盘全按本库 A 轴, 需要 items/码表/基本面时经 axis.global() 映射.
  // 一次只算一个库 (TS + CS 同库一趟, 不拆不跨库去重, 见 FeatureLib.hpp).
  // 库集 = 本库 + 同池旁读档的库, 余档空
  const Config &cfg = data_.config;
  const PoolId pool = lib_pool(config.lib);
  const TierId tier = lib_tier(config.lib);
  PerTier<FeatureLib> libs;
  for (const TierId s : TIER_IDS)
    if (tier_side(tier, s))
      libs[s] = load_lib(cfg, lib_id(pool, s));
  FeatureLib &lib = libs[tier];
  lib = FeatureLib{.id = config.lib, .dir = lib_dir(cfg, config.lib), .dates = std::move(config.dates)};
  for (const LibDep &dep : lib_deps(lib.id)) {
    const FeatureLib d = load_lib(cfg, dep.lib);
    assert(d.ready() && lib.dates.back() <= d.dates.back() && (!dep.prev || lib.dates.front() > d.dates.front()) &&
           "日期轴越过依赖库 (先把依赖库算到这天)");
  }
  // 续算: 旧日期轴须是规划的真前缀; 尾窗 = 旧末 min(PEND_DAYS, 旧日数) 日 (标签可能仍悬挂, 此轮结清), 管线日期轴从尾窗起
  const FeatureLib old = config.resume ? load_lib(cfg, lib.id) : FeatureLib{};
  size_t tail = 0, first = 0; // first = 管线日期轴在库日期轴里的起点
  if (config.resume) {
    assert(resumable(cfg, old) && "续算: 库没有对得上的续算点 (先全量)");
    assert(lib.dates.size() > old.dates.size() && std::equal(old.dates.begin(), old.dates.end(), lib.dates.begin()) &&
           "续算: 库日期轴须是规划的真前缀");
    tail = std::min(old.dates.size(), LabelHold::PEND_DAYS);
    first = old.dates.size() - tail;
  }
  const std::vector<misc::Ymd> dates(lib.dates.begin() + static_cast<std::ptrdiff_t>(first), lib.dates.end());
  // 冻结轴: 定轴库清单轴 (随轴库; 续算时 resumable 已保证 = 旧清单轴) / 旧清单轴 (定轴库续算); 定轴库全量 = 无 (池的自然轴)
  std::optional<PoolAxis> frozen;
  if (axis_lib(lib.id) != lib.id)
    frozen = load_lib(cfg, axis_lib(lib.id)).axis;
  else if (config.resume)
    frozen = old.axis;

  // Enable High Performance Mode: GUI low-refresh, compute owns cores
  data_.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI low-refresh\n"
            << std::endl;
  begin(Phase::Running, dates.size());

  // 日频 PIT 基本面数据源 (全市场 × 库日期轴: 切片起点 / 复权锚随库首日, 续算与全量同一面板), Fund 节点在 worker 内逐日推进; worker 只读
  fund::Panel fund;
  fund.build(lib.dates);
  // 池面板 (本库 A 轴 × 管线日期轴逐日成员); 轴外 = 旧清单轴外 ∪ 本轮自然轴 \ 本库轴
  const pool::Panel members = pool::Panel::build(pool, cfg, fund, dates, frozen ? &*frozen : nullptr);
  lib.axis = members.axis();
  std::ranges::set_union(old.outside, members.outside(), std::back_inserter(lib.outside));
  std::cout << "Lib " << lib_name(lib.id) << ": " << lib.dir << " → " << lib.axis.size() << " / "
            << asset_axis().size() << " assets (轴外 " << lib.outside.size() << ")"
            << (config.resume ? " | 续算 " + std::to_string(dates.size() - tail) + " 日 + 尾窗 " + std::to_string(tail) + " 日" : "") << "\n"
            << std::endl;

  // 全量重算: 显式清本库 (清库是调用方的决定, 不是 store 构造的副作用;
  // 别的库不碰 —— 按目录分片, 见 lib_dir), 随即落新清单 (读端此后按它读; 中途取消 = 清单在, 已落盘日是其子集).
  // 续算: 不清库, 跑完才落新清单 (中途取消 = 旧清单 + 旧续算点仍对得上, 再续即可; 已重落盘的尾窗日标签按新日结清, 再续同值重写)
  if (!config.resume) {
    FeatureRead::wipe_dir(lib.dir); // 可能与 reader 判废删库并发
    FeatureRead::set_dir_stale(lib.dir, false);
    save_lib(lib);
  }

  // 日志: 先关 (释放上一轮文件句柄), 清空重建 log_dir, 全部 worker 共用一个日志文件
  namespace fs = std::filesystem;
  Logger::close();
  fs::path log_path(cfg.log_dir);
  if (fs::exists(log_path))
    fs::remove_all(log_path);
  fs::create_directories(log_path);
  Logger::init(cfg.log_dir);

  // 已落盘天数镜像进进度面
  const PipelineArgs args{data_, libs, fund, members, dates, tail, static_cast<size_t>(config.ring_slots(tier)), static_cast<uint64_t>(config.adopt_pct),
                          config.pf_cores[tier], config.cs_cores[tier], config.io_cores[tier], cancel_,
                          [this](size_t io_done) { publish(io_done - done.load(std::memory_order_relaxed)); }};
  const bool completed = with_tier(tier, [&]<class T>() { return pipeline<T>(args); });
  if (completed && config.resume)
    save_lib(lib);

  // Disable High Performance Mode: GUI resumes
  data_.DisableHighPerformanceMode();
  std::cout << "[High Performance Mode] Disabled - GUI full-refresh\n"
            << std::endl;
  return completed;
}

} // namespace GUI::Features
