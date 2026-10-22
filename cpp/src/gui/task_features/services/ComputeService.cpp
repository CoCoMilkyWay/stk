// Compute Service Implementation
#include "gui/task_features/services/ComputeService.hpp"
#include "features/Method/Fundamental.hpp"
#include "features/Node/Bar/TS/Label/LabelBar.hpp"
#include "features/Node/Lob/TS/Label/LabelLob.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "features/Storage/FeatureStoreConfig.hpp" // lob::Tier / bar::Tier
#include "misc/logging.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/FeatureLib.hpp"
#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <vector>

namespace GUI::Features {

static_assert(ComputeConfig::kMinRingSlots == static_cast<int>(LabelLob::MIN_RING_SLOTS), "ComputeConfig::kMinRingSlots 与 LabelHold::MIN_RING_SLOTS 不符");
static_assert(ComputeConfig::kMinRingSlots == static_cast<int>(LabelBar::MIN_RING_SLOTS), "ComputeConfig::kMinRingSlots 与 LabelHold::MIN_RING_SLOTS 不符");
static_assert(TIER_COUNT == 2 && static_cast<size_t>(TierId::Lob) == 0 && static_cast<size_t>(TierId::Bar) == 1, "ComputeConfig 按档默认值按 Lob / Bar 序写死");

ComputeService::ComputeService(SharedData &data)
    : data_(data) {
  Start();
}

void ComputeService::Request(ComputeConfig config) {
  for (const TierId t : TIER_IDS) {
    assert(config.ring_slots[t] >= ComputeConfig::kMinRingSlots && "ring slots 低于 LabelHold::MIN_RING_SLOTS (T+N 标签悬挂日 + 1), 会死锁");
    assert(config.pf_cores[t] >= 1 && config.cs_cores[t] >= 1 && config.io_cores[t] >= 1 && "预取 / 截面 / 落盘各至少 1 核");
  }
  assert(config.adopt_pct >= 0 && config.adopt_pct <= 100 && "adopt pct must be in [0, 100]");
  Service::Request(config);
}

// 两档同一流程: 规划本池 → fund_panel → 清库 + 落清单 → 日志 → pipeline<T> (见 worker/feature_pipeline.hpp)
bool ComputeService::run(ComputeConfig &config) {
  // 规划 (日期轴 × A 轴; 写库清单 = 读端唯一来源, 见 FeatureLib.hpp):
  //   Bar (all)  交易日 [PIPELINE_START_DATE, end_date] × 全轴: 整段算, 不随回测窗口收窄; 无旁读档
  //   Lob (池) 回测日历 (交易日历 ∩ 回测区间, 与编码同一条轴: binary/archive 缺日也要落一份默认张量)
  //              × 池并集 (Bar 池 D−1 收盘行的 pool 列 → pool::Panel, 逐日成员 = Meta cs_valid); 截面旁读 Bar 池
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
  lib = FeatureLib{.tier = def.tier, .name = config.pool, .dir = data_.config.FeatureLibDir(config.pool)};
  if (lib.tier == TierId::Lob) {
    assert(data_.asset.backtest.start == data_.config.start_date && data_.asset.backtest.end == data_.config.end_date &&
           "回测日历与配置区间不符 (改配置后没重算覆盖?)");
    lib.dates = data_.asset.backtest_dates();
    data_.pool_panel = pool::Panel::build(libs[TierId::Bar], def.pool_col, lib.dates);
    lib.axis = data_.pool_panel.axis();
  } else {
    for (const CalendarDay &day : data_.assetinfo.get_stock_days())
      if (day.trading && day.date >= ::config::PIPELINE_START_DATE && day.date <= data_.config.end_date)
        lib.dates.push_back(day.date);
    lib.axis = full_axis(data_.asset.items.size());
  }
  assert(!lib.dates.empty() && std::is_sorted(lib.dates.begin(), lib.dates.end()) && "日期轴为空 / 交易日历非升序, 不应触发特征计算");
  const std::vector<misc::Ymd> &dates = lib.dates;
  const size_t num_assets = lib.axis.size();
  std::cout << "Lib " << lib.name << " (" << tier_name(lib.tier) << "): " << lib.dir << " → " << num_assets << " / "
            << data_.asset.items.size() << " assets\n"
            << std::endl;

  // Enable High Performance Mode: GUI low-refresh, compute owns cores
  data_.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI low-refresh\n"
            << std::endl;
  begin(Phase::Running, dates.size());

  // 日频 PIT 基本面数据源 (网格切片 + 事件链), Fund 节点在 worker 内逐日推进; worker 只读.
  // 只 build 本档 A 轴码表 × 本档日期轴 —— 池大小随轴缩.
  {
    std::vector<std::string> axis_codes(num_assets);
    for (size_t sub = 0; sub < num_assets; ++sub)
      axis_codes[sub] = asset_axis().code(lib.axis.global(sub));
    data_.fund_panel.build(axis_codes, dates, lib.tier != TierId::Lob);
  }

  // 全量重算: 显式清本库 (清库是调用方的决定, 不是 store 构造的副作用;
  // 别的池不碰 —— 按目录分片, 见 Config), 随即落新清单 (读端此后按它读; 中途取消 = 清单在, 已落盘日是其子集)
  FeatureRead::wipe_dir(lib.dir); // 可能与 reader 判废删库并发
  FeatureRead::set_dir_stale(lib.dir, false);
  save_lib(lib);

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
  const PipelineArgs args{data_, libs, dates, static_cast<size_t>(config.ring_slots[tier]), static_cast<uint64_t>(config.adopt_pct),
                          config.pf_cores[tier], config.cs_cores[tier], config.io_cores[tier], cancel_,
                          [this](size_t io_done) { publish(io_done - done.load(std::memory_order_relaxed)); }};
  bool completed = false;
#define TIER_PIPELINE(T, ns) \
  if (lib.tier == TierId::T) \
    completed = pipeline<ns::Tier>(args);
  ALL_TIERS(TIER_PIPELINE)
#undef TIER_PIPELINE

  // Disable High Performance Mode: GUI resumes
  data_.DisableHighPerformanceMode();
  std::cout << "[High Performance Mode] Disabled - GUI full-refresh\n"
            << std::endl;
  return completed;
}

} // namespace GUI::Features
