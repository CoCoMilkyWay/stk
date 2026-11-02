// Compute Service Implementation
#include "gui/task_features/services/ComputeService.hpp"
#include "features/Method/Fundamental.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "features/Storage/FeatureSnap.hpp"
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
#include <string>
#include <vector>

namespace GUI::Features {

ComputeService::ComputeService(SharedData &data)
    : data_(data) {
  Start();
}

PerLib<LibPlan> plan_libs(const SharedData &data) {
  const Config &cfg = data.config;
  PerTier<std::vector<misc::Ymd>> source;
  for (const TierId t : TIER_IDS)
    source[t] = tier_ops(t).source_dates(data);
  PerLib<LibPlan> out;
  for (const LibId l : lib_order()) {
    LibPlan &p = out[l];
    p.lib = load_lib(cfg, l);
    std::vector<misc::Ymd> landed; // 已落盘日 (升序)
    if (p.lib.ready()) {
      std::ranges::copy_if(p.lib.dates, std::back_inserter(landed), [&](misc::Ymd d) { return FeatureRead::has_date(p.lib.tier(), p.lib.dir, d); });
      p.done = landed.size();
      p.resumable = resumable(cfg, p.lib);
    }
    p.plan = source[p.lib.tier()];
    if (const auto up = lib_parent(l)) {
      const LibPlan &u = out[up->lib];
      if (!u.complete()) {
        p.plan.clear();
        p.blocked = up->lib;
      } else
        std::erase_if(p.plan, [&](misc::Ymd d) { return !parent_covers(u.lib.dates, up->prev, d); });
    }
    p.pending = static_cast<size_t>(std::ranges::count_if(p.plan, [&](misc::Ymd d) { return !std::ranges::binary_search(landed, d); }));
  }
  return out;
}

bool resumable(const Config &cfg, const FeatureLib &lib) {
  if (!lib.ready())
    return false;
  if (axis_lib(lib.id) != lib.id && !axis_within(lib.axis, load_lib(cfg, axis_lib(lib.id)).axis))
    return false;
  return FeatureSnap::head(lib.dir) == tier_ops(lib.tier()).snap_header(lib, lib.dates.back());
}

LibRun lib_run(const Config &cfg, LibId id, std::vector<misc::Ymd> plan, bool resume) {
  assert(!plan.empty() && std::is_sorted(plan.begin(), plan.end()) && "规划日期轴为空 / 非升序");
  LibRun r;
  r.lib = FeatureLib{.id = id, .dir = lib_dir(cfg, id), .dates = std::move(plan)};
  if (const auto up = lib_parent(id)) {
    const FeatureLib u = load_lib(cfg, up->lib);
    assert(u.ready() && std::ranges::all_of(r.lib.dates, [&](misc::Ymd d) { return parent_covers(u.dates, up->prev, d); }) &&
           "日期轴越出上游库 (先把上游库算到这天)");
  }
  if (resume) {
    r.old = load_lib(cfg, id);
    assert(resumable(cfg, r.old) && "续算: 库没有对得上的续算点 (先全量)");
    assert(r.lib.dates.size() > r.old.dates.size() && std::equal(r.old.dates.begin(), r.old.dates.end(), r.lib.dates.begin()) &&
           "续算: 库日期轴须是规划的真前缀");
    r.tail = std::min(r.old.dates.size(), tier_ops(r.lib.tier()).pend_days);
  }
  r.dates.assign(r.lib.dates.begin() + static_cast<std::ptrdiff_t>(r.old.dates.size() - r.tail), r.lib.dates.end());
  return r;
}

void ComputeService::Request(ComputeConfig config) {
  for (const TierId t : TIER_IDS)
    assert(config.pf_cores[t] >= 1 && config.cs_cores[t] >= 1 && config.io_cores[t] >= 1 && "预取 / 截面 / 落盘各至少 1 核");
  assert(!config.dates.empty() && std::is_sorted(config.dates.begin(), config.dates.end()) && "日期轴为空 / 非升序");
  Service::Request(config);
}

// 每库同一流程: 本轮规划 (lib_run) → 基本面面板 → 池面板 (A 轴 + 逐日成员 + 轴外) → 清库 + 落清单 → 日志 → 档的管线 (tier_ops)
// 一库一趟 TS + CS, 不拆不跨池共享 (约束与理由见 FeatureLib.hpp 文件头)
// 续算同一流程, 只差: 轴冻结为旧清单轴, 管线日期轴 = 尾窗 + 新日, 不清库, 跑完才落新清单
bool ComputeService::run(ComputeConfig &config) {
  // 写库清单 = 读端唯一来源 (见 FeatureLib.hpp); 张量/调度/落盘全按本库 A 轴, 需要 items/码表/基本面时经 axis.global() 映射.
  // 库集 = 本库 + 同池旁读档的库, 余档空
  const Config &cfg = data_.config;
  const PoolId pool = lib_pool(config.lib);
  const TierId tier = lib_tier(config.lib);
  LibRun r = lib_run(cfg, config.lib, std::move(config.dates), config.resume);

  // Enable High Performance Mode: GUI low-refresh, compute owns cores
  data_.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI low-refresh\n"
            << std::endl;
  begin(Phase::Running, r.dates.size());

  // 日频 PIT 基本面数据源 (全市场 × 库日期轴: 切片起点 / 复权锚随库首日, 续算与全量同一面板), Fund 节点在 worker 内逐日推进; worker 只读
  fund::Panel fund;
  fund.build(r.lib.dates);
  // 池面板 (本库 A 轴 × 管线日期轴逐日成员 + 轴外, 见 shared/Pool.hpp)
  const pool::Panel members = pool::Panel::build(r.lib.id, cfg, fund, r.dates, r.old);
  r.lib.axis = members.axis();
  r.lib.outside = members.outside();
  PerTier<FeatureLib> libs;
  for (const TierId s : TIER_IDS)
    if (tier_side(tier, s))
      libs[s] = load_lib(cfg, lib_id(pool, s));
  FeatureLib &lib = libs[tier] = std::move(r.lib);
  std::cout << "Lib " << lib_name(lib.id) << ": " << lib.dir << " → " << lib.axis.size() << " / "
            << asset_axis().size() << " assets (轴外 " << lib.outside.size() << ")"
            << (config.resume ? " | 续算 " + std::to_string(r.dates.size() - r.tail) + " 日 + 尾窗 " + std::to_string(r.tail) + " 日" : "") << "\n"
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
  const PipelineArgs args{data_, libs, fund, members, r.dates, r.tail,
                          config.pf_cores[tier], config.cs_cores[tier], config.io_cores[tier], cancel_,
                          [this](size_t io_done) { publish(io_done - done.load(std::memory_order_relaxed)); }};
  const bool completed = tier_ops(tier).pipeline(args);
  if (completed && config.resume)
    save_lib(lib);

  // Disable High Performance Mode: GUI resumes
  data_.DisableHighPerformanceMode();
  std::cout << "[High Performance Mode] Disabled - GUI full-refresh\n"
            << std::endl;
  return completed;
}

} // namespace GUI::Features
