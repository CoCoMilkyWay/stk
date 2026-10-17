// Compute Service Implementation
#include "gui/task_features/services/ComputeService.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "features/Backend/FeatureStore.hpp"
#include "features/Operator/TS/Label/LabelReturn.hpp"
#include "misc/affinity.hpp"
#include "misc/logging.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <vector>

namespace GUI::Features {

static_assert(ComputeConfig::kMinPoolSlots == static_cast<int>(LabelReturn::MIN_POOL_SLOTS), "ComputeConfig::kMinPoolSlots 与 LabelReturn::MIN_POOL_SLOTS 不符");

ComputeService::ComputeService(SharedData &data)
    : data_(data) {
  Start();
}

void ComputeService::Request(ComputeConfig config) {
  assert(config.pool_slots >= ComputeConfig::kMinPoolSlots && "pool slots 低于 LabelReturn::MIN_POOL_SLOTS (T+N 标签悬挂日 + 1), 会死锁");
  assert(config.adopt_pct >= 0 && config.adopt_pct <= 100 && "adopt pct must be in [0, 100]");
  Worker::Request(config);
}

bool ComputeService::run(ComputeConfig &config) {
  // 核布局: 预取 → TS → CS → IO 四个 stage, 全部 pin 死 (见 StageLayout)
  const StageLayout L = StageLayout::make(static_cast<int>(misc::Affinity::core_count()), config.prefetch_share_io);

  // Enable High Performance Mode: GUI low-refresh, compute owns cores
  data_.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI low-refresh\n"
            << std::endl;

  // 计算日期轴 = 回测日历 (交易日历 ∩ 回测区间), 与编码同一条轴: binary/archive 缺日也要落一份默认张量.
  const misc::Ymd backtest_start = data_.config.start_date;
  const misc::Ymd backtest_end = data_.config.end_date;
  assert(data_.asset.backtest.start == backtest_start && data_.asset.backtest.end == backtest_end &&
         "回测日历与配置区间不符 (改配置后没重算覆盖?)");
  const std::vector<misc::Ymd> backtest_dates = data_.asset.backtest_dates();
  assert(!backtest_dates.empty() && "回测区间内无交易日, 不应触发特征计算");

  const std::string stage_summary = L.summary();

  std::cout << "\n=== Phase 2: Feature Computation ===\n"
            << stage_summary << "\n"
            << "Assets: " << data_.asset.items.size()
            << " | Pool slots: " << config.pool_slots
            << " | Adopt pct: " << config.adopt_pct << "%"
            << " | Backtest dates: " << backtest_dates.size()
            << " (" << backtest_start << " - " << backtest_end << ")\n"
            << std::endl;

  const unsigned int num_ts_workers = static_cast<unsigned int>(L.num_ts);
  const size_t total_dates = backtest_dates.size();
  begin(Phase::Running, total_dates);

  // universe 子轴 = 特征管线的 A 轴 (见 AssetAxis.hpp 的 UniverseAxis):
  // 张量/调度/落盘全按子轴大小, 需要 items/码表时经 global_ids 映射.
  // 子轴推导与 Dist/Transform/OrderFlow 读端同一函数 (universe_axis), 永远一致.
  const UniverseAxis uni = universe_axis(data_.config, data_.asset.items.size());
  const size_t num_assets = uni.size();
  std::cout << "Universe: " << data_.config.universe << " → " << num_assets
            << " / " << data_.asset.items.size() << " assets\n"
            << std::endl;

  // Phase 2 前置: 动态池逐日名单 (与 universe_axis 同一文件: 并集 = 子轴, 逐日成员位 → Meta cs_valid).
  // 回测区间每一天都必须出过名单, 否则是名单没跑到这段 —— 启动即死, 不要静默算出半截 cs_valid.
  data_.universe_pool = (data_.config.universe == "all") ? universe::Pool::all() : universe::Pool::load(data_.config.UniversePath());
  assert(std::all_of(backtest_dates.begin(), backtest_dates.end(), [&](misc::Ymd d) { return data_.universe_pool.has_date(d); }) &&
         "回测日不在动态池名单里: 重跑 py/universe/<name>.py 覆盖到 end_date");

  // Load balancing (初始形态): 按回测区间内的逐笔条数降序 + 轮询分配 ——
  // 标的数每核严格均匀 (±1), 权重也近似均衡. 贪心 LPT 会把大量小标的堆到
  // 少数核上 (标的数悬殊), 而每日固定开销 (decode/begin_day/分钟网格)
  // 随标的数走, 反而更歪. 运行期差距由 worker 间的处置权转移收敛
  // (见 sequential_worker 的 try_adopt), 这里只求起点均衡.
  //
  // 条数是扫描时随日文件目录一并读好的 (见 Asset::scan_binary),
  // 这里直接累加, 不必再碰文件系统.
  // 帧目录切成 回测日 × 子轴 (行 = backtest_dates 下标, 列 = 子轴下标), 持有到
  // 全部 worker 退出 (随 WorkerCtx 下发), 不碰 Asset 上随重扫描换代的那一份.
  const OrdersCatalog orders = data_.asset.orders()->slice(backtest_dates, uni.ids);
  std::vector<size_t> weights(num_assets, 0);
  for (size_t d = 0; d < backtest_dates.size(); ++d) {
    const std::span<const FrameRef> row = orders.row(d);
    for (size_t sub = 0; sub < num_assets; ++sub)
      weights[sub] += row[sub].head.count;
  }

  std::vector<std::pair<size_t, size_t>> asset_workloads; // (sub, weight)
  asset_workloads.reserve(num_assets);
  for (size_t sub = 0; sub < num_assets; ++sub)
    asset_workloads.push_back({sub, weights[sub]});

  std::sort(asset_workloads.begin(), asset_workloads.end(),
            [](const auto &a, const auto &b) { return a.second > b.second; });

  // Round-robin assignment: 降序轮询, 第 k 重的标的给 worker k % N
  TsSchedule ts_schedule(num_assets, num_ts_workers);
  ts_schedule.adopt_pct = static_cast<uint64_t>(config.adopt_pct);
  ts_schedule.global_ids = uni.ids;
  for (size_t k = 0; k < asset_workloads.size(); ++k) {
    const auto &[sub, weight] = asset_workloads[k];
    ts_schedule.owner[sub].store(static_cast<int32_t>(k % num_ts_workers), std::memory_order_relaxed);
    ts_schedule.weight[sub] = weight;
  }

  // Phase 2 前置: 日频 PIT 基本面数据源 (网格切片 + 事件链), Fund 算子在
  // worker 内逐日推进; worker 只读. 只 build 子轴码表 —— 池大小随 universe 缩.
  {
    std::vector<std::string> axis_codes(num_assets);
    for (size_t sub = 0; sub < num_assets; ++sub)
      axis_codes[sub] = asset_axis().code(uni.global(sub));
    data_.fund_pool.build(axis_codes, backtest_dates);
  }

  // 回测全量重算: 显式清本 universe 的特征库 (清库是调用方的决定, 不是
  // store 构造的副作用; 别的 universe 的库不碰 —— 按目录分片, 见 Config)
  const std::string feature_dir = data_.config.FeatureUniverseDir();
  FeatureRead::wipe_dir(feature_dir); // 可能与 reader 判废删库并发
  FeatureRead::set_dir_stale(feature_dir, false);

  // Initialize global feature store
  GlobalFeatureStore feature_store(
      num_assets, uni.hash,
      feature_dir, static_cast<size_t>(config.pool_slots));
  assert(feature_store.query_slots() >= LabelReturn::MIN_POOL_SLOTS && "内存上限把池砍到 LabelReturn::MIN_POOL_SLOTS 之下, 会死锁");

  // Clean up directories before compute
  namespace fs = std::filesystem;

  // Close logger first (releases file handles from previous run)
  Logger::close();

  // Delete and recreate log_dir
  fs::path log_path(data_.config.log_dir);
  if (fs::exists(log_path)) {
    fs::remove_all(log_path);
  }
  fs::create_directories(log_path);

  // Initialize logger for all workers (shared log file)
  Logger::init(data_.config.log_dir);

  // Launch workers: 统一 launch(core, fn) —— pin 到 core, worker_id = core.
  // 四角色同签名 (WorkerCtx, 见 feature_workers.hpp), 调度面 sched 随 ctx
  // 带入, 只有 TS 用. 进度 = 仪表盘自行采样 store/sched/stats (拉取式).
  ComputeStats stats(static_cast<size_t>(L.num_ts));
  FeatureProgress progress(feature_store, ts_schedule, stats, backtest_dates);
  std::vector<std::future<void>> workers;
  workers.reserve(static_cast<size_t>(L.total_threads()));

  using WorkerFn = void (*)(WorkerCtx);
  auto worker_fn = [](ComputeStage stage) -> WorkerFn {
    switch (stage) {
    case ComputeStage::Prefetch:
      return prefetch_worker;
    case ComputeStage::TS:
      return sequential_worker;
    case ComputeStage::CS:
      return crosssectional_worker;
    case ComputeStage::IO:
      return io_worker;
    }
    assert(false && "unknown compute stage");
    return nullptr;
  };

  auto launch = [&](int core, WorkerFn fn) {
    workers.push_back(std::async(std::launch::async, [&, core, fn]() {
      if (misc::Affinity::supported()) {
        misc::Affinity::pin_to_core(static_cast<unsigned int>(core));
      }
      fn({core, data_, backtest_dates, orders, feature_store, ts_schedule, cancel_, stats});
    }));
  };

  for (const StageLayout::Stage &s : L.stages())
    for (int i = 0; i < s.threads; ++i)
      launch(s.first_core + i, worker_fn(s.kind));

  // 等收尾: 已落盘天数镜像进进度面 (worker 自己查 cancel_, 这里只报一声)
  auto mirror = [&]() { publish(feature_store.query_io_done() - done.load(std::memory_order_relaxed)); };
  bool announced = false;
  for (auto &worker : workers)
    while (worker.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready) {
      mirror();
      if (!announced && cancel_.load(std::memory_order_relaxed)) {
        announced = true;
        std::cout << "[Compute] Cancelling..." << std::endl;
      }
    }
  mirror();
  // 仪表盘引用 store/sched/backtest_dates, 必须先于它们的析构停掉 (局部逆序析构: 仪表盘 → 统计 → store → 调度面)
  progress.stop();

  // Finalize
  const bool completed = !cancel_.load(std::memory_order_relaxed);

  std::cout << "\n=== Feature Computation "
            << (completed ? "Complete" : "Cancelled") << " ===\n"
            << "Processed: " << total_dates << " dates\n"
            << std::endl;

  // Disable High Performance Mode: GUI resumes
  data_.DisableHighPerformanceMode();
  std::cout << "[High Performance Mode] Disabled - GUI full-refresh\n"
            << std::endl;
  return completed;
}

} // namespace GUI::Features
