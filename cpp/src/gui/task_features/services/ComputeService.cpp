// Compute Service Implementation
#include "gui/task_features/services/ComputeService.hpp"
#include "features/Method/Fundamental.hpp"
#include "features/Node/Bar/TS/Label/LabelBar.hpp"
#include "features/Node/Lob/TS/Label/LabelLob.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "features/Storage/FeatureSnap.hpp"
#include "features/Storage/FeatureStoreConfig.hpp" // lob::Tier / bar::Tier
#include "misc/logging.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/FeatureLib.hpp"
#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <vector>

namespace GUI::Features {

static_assert(ComputeConfig::kMinRingSlots == static_cast<int>(LabelLob::MIN_RING_SLOTS), "ComputeConfig::kMinRingSlots 与 LabelHold::MIN_RING_SLOTS 不符");
static_assert(ComputeConfig::kMinRingSlots == static_cast<int>(LabelBar::MIN_RING_SLOTS), "ComputeConfig::kMinRingSlots 与 LabelHold::MIN_RING_SLOTS 不符");
static_assert(TIER_COUNT == 2 && static_cast<size_t>(TierId::Lob) == 0 && static_cast<size_t>(TierId::Bar) == 1, "ComputeConfig 按档默认值按 Lob / Bar 序写死");

ComputeService::ComputeService(SharedData &data)
    : data_(data) {
  Start();
}

std::vector<misc::Ymd> source_dates(const SharedData &data, TierId tier) {
  std::vector<misc::Ymd> dates;
  if (tier == TierId::Lob) {
    assert(data.asset.backtest.start == data.config.start_date && "回测日历与配置起点不符 (改配置后没重算覆盖?)");
    for (const Asset::BacktestDay &day : data.asset.backtest.days)
      if (day.date <= data.asset.binary.max_date)
        dates.push_back(day.date);
    return dates;
  }
  const StockDaysVec &days = data.assetinfo.get_stock_days();
  assert(!days.empty() && days.back().trading && "交易日历未就绪 / 末行不是交易日");
  for (const CalendarDay &day : days)
    if (day.trading && day.date >= ::config::PIPELINE_START_DATE && day.date <= data.assetinfo.bar1m_end && day.date < days.back().date)
      dates.push_back(day.date);
  return dates;
}

bool resumable(const FeatureLib &lib) {
  if (!lib.ready())
    return false;
  SnapHeader want;
#define TIER_SNAP_HEADER(T, ns) \
  if (lib.tier == TierId::T)    \
    want = snap_header<ns::Tier>(lib, lib.dates.back());
  ALL_TIERS(TIER_SNAP_HEADER)
#undef TIER_SNAP_HEADER
  return FeatureSnap::head(lib.dir) == want;
}

void ComputeService::Request(ComputeConfig config) {
  for (const TierId t : TIER_IDS)
    assert(config.pf_cores[t] >= 1 && config.cs_cores[t] >= 1 && config.io_cores[t] >= 1 && "预取 / 截面 / 落盘各至少 1 核");
  assert(config.adopt_pct >= 0 && config.adopt_pct <= 100 && "adopt pct must be in [0, 100]");
  assert(!config.dates.empty() && std::is_sorted(config.dates.begin(), config.dates.end()) && "日期轴为空 / 非升序");
  Service::Request(config);
}

// 两档同一流程: 规划本池 → fund_panel → 清库 + 落清单 → 日志 → pipeline<T> (见 worker/feature_pipeline.hpp)
// 续算同一流程, 只差: 轴沿用旧清单, 管线日期轴 = 尾窗 + 新日, 不清库, 跑完才落新清单
bool ComputeService::run(ComputeConfig &config) {
  // 规划 (日期轴 × A 轴; 写库清单 = 读端唯一来源, 见 FeatureLib.hpp):
  //   日期轴 = 请求带来的规划 (见 source_dates), 末日不晚于各旁读库末日
  //   Bar (all)  × 全轴: 整段算, 不随回测日历收窄; 无旁读档
  //   Lob (池) × 池并集 (Bar 池 D−1 收盘行的 pool 列 → pool::Panel, 逐日成员 = Meta cs_valid); 截面旁读 Bar 池
  // 张量/调度/落盘全按本池 A 轴, 需要 items/码表时经 axis.global() 映射.
  // 一次只算一个池 (TS + CS 同池一趟, 不拆不跨池去重: 将来 TS 可吃池内 CS 输出, 见 FeatureLib.hpp); Lob 只在本池轴内算
  //  —— 一般只算一个池, 不为多池合算做并集轴.
  // 池集 = 本池 + 旁读档的当前池 (Selection::pools), 余档空
  const PoolDef &def = pool_def(config.pool);
  PerTier<FeatureLib> libs;
  for (const TierId s : TIER_IDS)
    if (tier_side(def.tier, s))
      libs[s] = load_lib(data_.config, data_.feature.selection.pool_of(s));
  FeatureLib &lib = libs[def.tier];
  lib = FeatureLib{.tier = def.tier, .name = config.pool, .dir = data_.config.FeatureLibDir(config.pool), .dates = std::move(config.dates)};
  for (const TierId s : TIER_IDS)
    assert((!tier_side(def.tier, s) || (libs[s].ready() && lib.dates.back() <= libs[s].dates.back())) && "日期轴越过旁读库末日 (先把旁读档算到这天)");
  // 续算: 旧日期轴须是规划的真前缀; 尾窗 = 旧末 min(PEND_DAYS, 旧日数) 日 (标签可能仍悬挂, 此轮结清), 管线日期轴从尾窗起
  size_t tail = 0, first = 0; // first = 管线日期轴在库日期轴里的起点
  if (config.resume) {
    const FeatureLib old = load_lib(data_.config, config.pool);
    assert(resumable(old) && "续算: 库没有对得上的续算点 (先全量)");
    assert(lib.dates.size() > old.dates.size() && std::equal(old.dates.begin(), old.dates.end(), lib.dates.begin()) &&
           "续算: 库日期轴须是规划的真前缀");
    lib.axis = old.axis;
    lib.outside = old.outside;
    tail = std::min(old.dates.size(), LabelHold::PEND_DAYS);
    first = old.dates.size() - tail;
  }
  const std::vector<misc::Ymd> dates(lib.dates.begin() + static_cast<std::ptrdiff_t>(first), lib.dates.end());
  if (lib.tier == TierId::Lob) {
    data_.pool_panel = pool::Panel::build(libs[TierId::Bar], def.pool_col, dates, config.resume ? &lib.axis : nullptr);
    if (config.resume) {
      std::vector<uint32_t> outside;
      std::ranges::set_union(lib.outside, data_.pool_panel.outside(), std::back_inserter(outside));
      lib.outside = std::move(outside);
    } else {
      lib.axis = data_.pool_panel.axis();
    }
  } else if (config.resume) {
    lib.outside.clear();
    for (size_t g = lib.axis.size(); g < data_.asset.items.size(); ++g)
      lib.outside.push_back(static_cast<uint32_t>(g));
  } else {
    lib.axis = full_axis(data_.asset.items.size());
  }
  const size_t num_assets = lib.axis.size();
  std::cout << "Lib " << lib.name << " (" << tier_name(lib.tier) << "): " << lib.dir << " → " << num_assets << " / "
            << data_.asset.items.size() << " assets (轴外 " << lib.outside.size() << ")"
            << (config.resume ? " | 续算 " + std::to_string(dates.size() - tail) + " 日 + 尾窗 " + std::to_string(tail) + " 日" : "") << "\n"
            << std::endl;

  // Enable High Performance Mode: GUI low-refresh, compute owns cores
  data_.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI low-refresh\n"
            << std::endl;
  begin(Phase::Running, dates.size());

  // 日频 PIT 基本面数据源 (网格切片 + 事件链), Fund 节点在 worker 内逐日推进; worker 只读.
  // 只 build 本档 A 轴码表 × 库日期轴 (不是管线日期轴: 切片起点 / 复权锚随库首日, 续算与全量同一面板) —— 池大小随轴缩.
  {
    std::vector<std::string> axis_codes(num_assets);
    for (size_t sub = 0; sub < num_assets; ++sub)
      axis_codes[sub] = asset_axis().code(lib.axis.global(sub));
    data_.fund_panel.build(axis_codes, lib.dates, lib.tier != TierId::Lob);
  }

  // 全量重算: 显式清本库 (清库是调用方的决定, 不是 store 构造的副作用;
  // 别的池不碰 —— 按目录分片, 见 Config), 随即落新清单 (读端此后按它读; 中途取消 = 清单在, 已落盘日是其子集).
  // 续算: 不清库, 跑完才落新清单 (中途取消 = 旧清单 + 旧续算点仍对得上, 再续即可; 已重落盘的尾窗日标签按新日结清, 再续同值重写)
  if (!config.resume) {
    FeatureRead::wipe_dir(lib.dir); // 可能与 reader 判废删库并发
    FeatureRead::set_dir_stale(lib.dir, false);
    save_lib(lib);
  }

  // 日志: 先关 (释放上一轮文件句柄), 清空重建 log_dir, 全部 worker 共用一个日志文件
  namespace fs = std::filesystem;
  Logger::close();
  fs::path log_path(data_.config.log_dir);
  if (fs::exists(log_path))
    fs::remove_all(log_path);
  fs::create_directories(log_path);
  Logger::init(data_.config.log_dir);

  // 已落盘天数镜像进进度面
  const TierId tier = lib.tier;
  const PipelineArgs args{data_, libs, dates, tail, static_cast<size_t>(config.ring_slots(tier)), static_cast<uint64_t>(config.adopt_pct),
                          config.pf_cores[tier], config.cs_cores[tier], config.io_cores[tier], cancel_,
                          [this](size_t io_done) { publish(io_done - done.load(std::memory_order_relaxed)); }};
  bool completed = false;
#define TIER_PIPELINE(T, ns) \
  if (lib.tier == TierId::T) \
    completed = pipeline<ns::Tier>(args);
  ALL_TIERS(TIER_PIPELINE)
#undef TIER_PIPELINE
  if (completed && config.resume)
    save_lib(lib);

  // Disable High Performance Mode: GUI resumes
  data_.DisableHighPerformanceMode();
  std::cout << "[High Performance Mode] Disabled - GUI full-refresh\n"
            << std::endl;
  return completed;
}

} // namespace GUI::Features
