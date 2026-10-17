// Compute Service Implementation
#include "gui/task_features/services/ComputeService.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "features/Backend/FeatureStore.hpp"
#include "features/Method/Fundamental.hpp"
#include "features/Operator/Bar/TS/Label/LabelBar.hpp"
#include "features/Operator/Lob/TS/Label/LabelLob.hpp"
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

static_assert(ComputeConfig::kMinPoolSlots == static_cast<int>(LabelLob::MIN_POOL_SLOTS), "ComputeConfig::kMinPoolSlots 与 LabelHold::MIN_POOL_SLOTS 不符");
static_assert(ComputeConfig::kMinPoolSlots == static_cast<int>(LabelBar::MIN_POOL_SLOTS), "ComputeConfig::kMinPoolSlots 与 LabelHold::MIN_POOL_SLOTS 不符");

ComputeService::ComputeService(SharedData &data)
    : data_(data) {
  Start();
}

void ComputeService::Request(ComputeConfig config) {
  assert(config.pool_slots >= ComputeConfig::kMinPoolSlots && "pool slots 低于 LabelHold::MIN_POOL_SLOTS (T+N 标签悬挂日 + 1), 会死锁");
  assert(config.adopt_pct >= 0 && config.adopt_pct <= 100 && "adopt pct must be in [0, 100]");
  Worker::Request(config);
}

// 两档同一流程: 日期轴 → 写读库 (FeatureLib) → fund_pool → 清库 → 日志 → pipeline<T> (见 worker/feature_pipeline.hpp)
bool ComputeService::run(ComputeConfig &config) {
  // 日期轴: Lob = 回测日历 (交易日历 ∩ 回测区间), 与编码同一条轴: binary/archive 缺日也要落一份默认张量;
  //         Bar = 交易日 [PIPELINE_START_DATE, end_date]: 整段算, 不随回测窗口收窄
  std::vector<misc::Ymd> dates;
  if (config.tier == TierId::Lob) {
    assert(data_.asset.backtest.start == data_.config.start_date && data_.asset.backtest.end == data_.config.end_date &&
           "回测日历与配置区间不符 (改配置后没重算覆盖?)");
    dates = data_.asset.backtest_dates();
  } else {
    for (const CalendarDay &day : data_.assetinfo.get_stock_days())
      if (day.trading && day.date >= ::config::PIPELINE_START_DATE && day.date <= data_.config.end_date)
        dates.push_back(day.date);
  }
  assert(!dates.empty() && std::is_sorted(dates.begin(), dates.end()) && "日期轴为空 / 交易日历非升序, 不应触发特征计算");

  // 写库 = 读端同一推导 (feature_lib): Lob = universe 子轴 + features/<universe>/, Bar = 全轴 + features/all/.
  // 旁读库 = CS 旁读档 (Lob 截面读 Bar 列); 张量/调度/落盘全按本档 A 轴, 需要 items/码表时经 uni.global 映射.
  const size_t num_items = data_.asset.items.size();
  const FeatureLib lib = feature_lib(data_.config, num_items, config.tier);
  const FeatureLib side = feature_lib(data_.config, num_items, TierId::Bar);
  const size_t num_assets = lib.uni.size();
  std::cout << "Tier " << tier_name(config.tier) << ": " << lib.dir << " → " << num_assets << " / " << num_items << " assets\n"
            << std::endl;

  // Enable High Performance Mode: GUI low-refresh, compute owns cores
  data_.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI low-refresh\n"
            << std::endl;
  begin(Phase::Running, dates.size());

  // 日频 PIT 基本面数据源 (网格切片 + 事件链), Fund 算子在 worker 内逐日推进; worker 只读.
  // 只 build 本档 A 轴码表 × 本档日期轴 —— 池大小随轴缩.
  {
    std::vector<std::string> axis_codes(num_assets);
    for (size_t sub = 0; sub < num_assets; ++sub)
      axis_codes[sub] = asset_axis().code(lib.uni.global(sub));
    data_.fund_pool.build(axis_codes, dates);
  }

  // 全量重算: 显式清本档的特征库 (清库是调用方的决定, 不是 store 构造的副作用;
  // 别的库不碰 —— 按目录分片, 见 Config)
  FeatureRead::wipe_dir(lib.dir); // 可能与 reader 判废删库并发
  FeatureRead::set_dir_stale(lib.dir, false);

  // 日志: 先关 (释放上一轮文件句柄), 清空重建 log_dir, 全部 worker 共用一个日志文件
  namespace fs = std::filesystem;
  Logger::close();
  fs::path log_path(data_.config.log_dir);
  if (fs::exists(log_path))
    fs::remove_all(log_path);
  fs::create_directories(log_path);
  Logger::init(data_.config.log_dir);

  // 已落盘天数镜像进进度面
  const PipelineArgs args{data_, lib, side, dates, static_cast<size_t>(config.pool_slots), static_cast<uint64_t>(config.adopt_pct), cancel_,
                          [this](size_t io_done) { publish(io_done - done.load(std::memory_order_relaxed)); }};
  const bool completed = config.tier == TierId::Lob ? pipeline<lob::Tier>(args) : pipeline<bar::Tier>(args);

  // Disable High Performance Mode: GUI resumes
  data_.DisableHighPerformanceMode();
  std::cout << "[High Performance Mode] Disabled - GUI full-refresh\n"
            << std::endl;
  return completed;
}

} // namespace GUI::Features
