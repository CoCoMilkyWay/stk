// Compute Service Implementation
#include "gui/task_features/services/ComputeService.hpp"
#include "features/Backend/FeatureStore.hpp"
#include "misc/affinity.hpp"
#include "misc/logging.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>

namespace GUI::Features {

ComputeService::ComputeService(SharedData &data)
    : data_(data) {}

ComputeService::~ComputeService() {
  if (is_running()) {
    stop_compute();
  }
}

void ComputeService::start_compute(ComputeConfig config) {
  if (status_.load(std::memory_order_relaxed) == ComputeStatus::Running)
    return;
  assert(config.pool_slots >= 2 && "pool slots must be at least 2");
  assert(config.adopt_pct >= 0 && config.adopt_pct <= 100 && "adopt pct must be in [0, 100]");

  status_.store(ComputeStatus::Running, std::memory_order_relaxed);
  cancel_flag_.store(false);
  config_ = config;
  layout_ = StageLayout::make(static_cast<int>(misc::Affinity::core_count()), config_.prefetch_share_io);
  start_time_ = std::chrono::steady_clock::now();

  // Enable High Performance Mode: GUI low-refresh, compute owns cores
  data_.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI low-refresh\n"
            << std::endl;

  // Launch compute in background thread
  compute_thread_ = std::async(std::launch::async, [this]() {
    // Filter dates to backtest period only
    std::string backtest_start = data_.config.start_date;
    std::string backtest_end = data_.config.end_date;

    // Convert YYYY-MM-DD to YYYYMMDD
    backtest_start.erase(std::remove(backtest_start.begin(), backtest_start.end(), '-'), backtest_start.end());
    backtest_end.erase(std::remove(backtest_end.begin(), backtest_end.end(), '-'), backtest_end.end());

    // 计算日期轴以交易日历为准: binary/archive 缺日也要落一份默认张量.
    std::vector<std::string> backtest_dates;
    backtest_dates.reserve(data_.asset.backtest.required_dates.size());
    for (const auto &date : data_.asset.backtest.required_dates) {
      if (date >= backtest_start && date <= backtest_end) {
        backtest_dates.push_back(date);
      }
    }
    assert(!backtest_dates.empty() && "回测区间内无交易日, 不应触发特征计算");

    // 核布局: 预取 → TS → CS → IO 四个 stage, 全部 pin 死 (见 StageLayout)
    const StageLayout &L = layout_;
    const std::string stage_summary = L.summary();

    std::cout << "\n=== Phase 2: Feature Computation ===\n"
              << stage_summary << "\n"
              << "Assets: " << data_.asset.items.size()
              << " | Pool slots: " << config_.pool_slots
              << " | Adopt pct: " << config_.adopt_pct << "%"
              << " | Backtest dates: " << backtest_dates.size()
              << " (" << backtest_start << " - " << backtest_end << ")\n"
              << std::endl;

    const unsigned int num_ts_workers = static_cast<unsigned int>(L.num_ts);
    const size_t total_dates = backtest_dates.size();

    // universe 子轴 = 特征管线的 A 轴 (见 AssetAxis.hpp 的 UniverseAxis):
    // 张量/调度/落盘全按子轴大小, 需要 items/码表时经 global_ids 映射.
    // 子轴推导与 Dist/Transform/OrderFlow 读端同一函数 (universe_axis), 永远一致.
    const UniverseAxis uni = universe_axis(data_.config, data_.asset.items.size());
    const size_t num_assets = uni.size();
    std::cout << "Universe: " << data_.config.universe << " → " << num_assets
              << " / " << data_.asset.items.size() << " assets\n"
              << std::endl;

    // Load balancing (初始形态): 按回测区间内的逐笔条数降序 + 轮询分配 ——
    // 标的数每核严格均匀 (±1), 权重也近似均衡. 贪心 LPT 会把大量小标的堆到
    // 少数核上 (标的数悬殊), 而每日固定开销 (decode 头/begin_day/分钟网格)
    // 随标的数走, 反而更歪. 运行期差距由 worker 间的处置权转移收敛
    // (见 sequential_worker 的 try_adopt), 这里只求起点均衡.
    //
    // 条数是扫描时随文件头一并读好的 (见 Asset::coro_scan_binary_database),
    // 这里直接累加, 不必再碰文件系统.
    std::vector<std::pair<size_t, size_t>> asset_workloads; // (sub, weight)
    asset_workloads.reserve(num_assets);

    // 回测日期 → 日期轴下标, 只查一次 (内层 资产 × 日期 是百万量级)
    std::vector<size_t> backtest_didx(backtest_dates.size());
    for (size_t d = 0; d < backtest_dates.size(); ++d)
      backtest_didx[d] = data_.asset.date_idx(backtest_dates[d]);

    for (size_t sub = 0; sub < num_assets; ++sub) {
      const AssetItem &item = data_.asset.items[uni.global(sub)];

      size_t weight = 0;
      for (const size_t didx : backtest_didx)
        weight += item.date_at(didx).order_count;

      asset_workloads.push_back({sub, weight});
    }

    std::sort(asset_workloads.begin(), asset_workloads.end(),
              [](const auto &a, const auto &b) { return a.second > b.second; });

    // Round-robin assignment: 降序轮询, 第 k 重的标的给 worker k % N
    ts_schedule_ = std::make_unique<TsSchedule>(num_assets, num_ts_workers);
    ts_schedule_->adopt_pct = static_cast<uint64_t>(config_.adopt_pct);
    ts_schedule_->global_ids = uni.ids;
    for (size_t k = 0; k < asset_workloads.size(); ++k) {
      const auto &[sub, weight] = asset_workloads[k];
      ts_schedule_->owner[sub].store(static_cast<int32_t>(k % num_ts_workers), std::memory_order_relaxed);
      ts_schedule_->weight[sub] = weight;
    }

    // Phase 2 前置: 日频 PIT 基本面数据源 (网格切片 + 事件链), Fund 算子在
    // worker 内逐日推进; worker 只读. 只 build 子轴码表 —— 池大小随 universe 缩.
    {
      std::vector<std::string> axis_codes(num_assets);
      for (size_t sub = 0; sub < num_assets; ++sub)
        axis_codes[sub] = asset_axis().code(uni.global(sub));
      data_.fund_pool.build(axis_codes, backtest_dates);
    }

    // Temporarily replace all_dates with backtest_dates for workers
    // Save original dates and restore after computation
    std::vector<std::string> original_dates = std::move(data_.asset.all_dates);
    data_.asset.all_dates = backtest_dates;

    // 回测全量重算: 显式清本 universe 的特征库 (清库是调用方的决定, 不是
    // store 构造的副作用; 别的 universe 的库不碰 —— 按目录分片, 见 Config)
    const std::string feature_dir = data_.config.FeatureUniverseDir();
    if (std::filesystem::exists(feature_dir)) {
      std::filesystem::remove_all(feature_dir);
    }

    // Initialize global feature store
    feature_store_ = std::make_unique<GlobalFeatureStore>(
        num_assets, num_ts_workers, uni.hash,
        feature_dir, static_cast<size_t>(config_.pool_slots));

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
    stats_ = std::make_unique<ComputeStats>(static_cast<size_t>(L.num_ts));
    progress_ = std::make_unique<FeatureProgress>(*feature_store_, *ts_schedule_, *stats_, data_.asset.all_dates);
    workers_.clear();
    workers_.reserve(static_cast<size_t>(L.total_threads()));

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

    auto launch = [this](int core, WorkerFn fn) {
      workers_.push_back(std::async(std::launch::async, [this, core, fn]() {
        if (misc::Affinity::supported()) {
          misc::Affinity::pin_to_core(static_cast<unsigned int>(core));
        }
        fn({core, data_, *feature_store_, *ts_schedule_, cancel_flag_, *stats_});
      }));
    };

    for (const StageLayout::Stage &s : L.stages())
      for (int i = 0; i < s.threads; ++i)
        launch(s.first_core + i, worker_fn(s.kind));

    // Wait for completion
    for (auto &worker : workers_)
      worker.wait();
    // 仪表盘引用 store/sched/all_dates, 必须先于它们的复原/析构停掉
    progress_->stop();
    progress_.reset();
    stats_.reset();
    workers_.clear();

    // Restore original all_dates
    data_.asset.all_dates = std::move(original_dates);

    // Cleanup feature store + TS 调度面 (cores 随之析构)
    feature_store_.reset();
    ts_schedule_.reset();

    // Finalize
    const ComputeStatus final_status = cancel_flag_.load(std::memory_order_relaxed) ? ComputeStatus::Cancelled : ComputeStatus::Completed;
    status_.store(final_status, std::memory_order_relaxed);

    std::cout << "\n=== Feature Computation "
              << (final_status == ComputeStatus::Completed ? "Complete" : "Cancelled") << " ===\n"
              << "Processed: " << total_dates << " dates\n"
              << std::endl;

    // Disable High Performance Mode: GUI resumes
    data_.DisableHighPerformanceMode();
    std::cout << "[High Performance Mode] Disabled - GUI full-refresh\n"
              << std::endl;
  });
}

void ComputeService::request_cancel() {
  if (status_.load(std::memory_order_relaxed) != ComputeStatus::Running) {
    return;
  }

  if (cancel_flag_.exchange(true, std::memory_order_relaxed)) {
    return;
  }
  std::cout << "[Compute] Cancelling..." << std::endl;
}

void ComputeService::stop_compute() {
  request_cancel();

  // Wait for compute thread to finish
  if (compute_thread_.valid()) {
    compute_thread_.wait();
  }
}

} // namespace GUI::Features
