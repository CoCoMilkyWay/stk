// Compute Service Implementation
#include "gui/task_features/services/ComputeService.hpp"
#include "features/Backend/FeatureStore.hpp"
#include "misc/affinity.hpp"
#include "misc/logging.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/SharedData.hpp"
#include "worker/crosssectional_worker.hpp"
#include "worker/io_worker.hpp"
#include "worker/sequential_worker.hpp"

#include <algorithm>
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

void ComputeService::start_compute(int num_workers) {
  if (status_ == ComputeStatus::Running)
    return;

  status_ = ComputeStatus::Running;
  cancel_flag_.store(false);
  num_workers_ = num_workers;
  start_time_ = std::chrono::steady_clock::now();

  // Enable High Performance Mode: GUI sleeps, all CPU for computation
  data_.EnableHighPerformanceMode();
  std::cout << "[High Performance Mode] Enabled - GUI thread sleeping\n"
            << std::endl;

  // Launch compute in background thread
  compute_thread_ = std::async(std::launch::async, [this]() {
    // Filter dates to backtest period only
    std::string backtest_start = data_.config.start_date;
    std::string backtest_end = data_.config.end_date;

    // Convert YYYY-MM-DD to YYYYMMDD
    backtest_start.erase(std::remove(backtest_start.begin(), backtest_start.end(), '-'), backtest_start.end());
    backtest_end.erase(std::remove(backtest_end.begin(), backtest_end.end(), '-'), backtest_end.end());

    // Filter all_dates to backtest period
    std::vector<std::string> backtest_dates;
    for (const auto &date : data_.asset.all_dates) {
      if (date >= backtest_start && date <= backtest_end) {
        backtest_dates.push_back(date);
      }
    }

    std::cout << "\n=== Phase 2: Feature Computation ===\n"
              << "Workers: " << num_workers_ << " | Assets: " << data_.asset.items.size()
              << " | Backtest dates: " << backtest_dates.size()
              << " (" << backtest_start << " - " << backtest_end << ")\n"
              << std::endl;

    // Analysis phase: (N-2) TS workers + 1 CS worker + 1 Flush IO worker = N total workers
    const unsigned int num_ts_workers = num_workers_ - 2;
    const unsigned int cs_worker_core = num_workers_ - 2; // Second-to-last core for CS
    const unsigned int io_worker_core = num_workers_ - 1; // Last core for Flush IO
    const size_t num_assets = data_.asset.items.size();
    const size_t total_dates = backtest_dates.size();

    // Load balancing: sort assets by order count (in backtest period only)
    std::vector<std::pair<size_t, size_t>> asset_workloads; // (asset_id, order_count)
    asset_workloads.reserve(data_.asset.items.size());

    for (size_t i = 0; i < data_.asset.items.size(); ++i) {
      // Calculate order count only for backtest dates
      size_t backtest_order_count = 0;
      for (const auto &date : backtest_dates) {
        auto it = data_.asset.items[i].date_info.find(date);
        if (it != data_.asset.items[i].date_info.end()) {
          backtest_order_count += it->second.order_count;
        }
      }
      asset_workloads.push_back({i, backtest_order_count});
    }

    std::sort(asset_workloads.begin(), asset_workloads.end(),
              [](const auto &a, const auto &b) { return a.second > b.second; });

    // Greedy assignment: each asset goes to TS worker with minimum current load
    std::vector<size_t> worker_loads(num_ts_workers, 0);

    for (const auto &[asset_id, order_count] : asset_workloads) {
      size_t min_worker = std::min_element(worker_loads.begin(), worker_loads.end()) - worker_loads.begin();
      data_.asset.items[asset_id].assigned_worker_id = min_worker;
      worker_loads[min_worker] += order_count;
    }

    // Temporarily replace all_dates with backtest_dates for workers
    // Save original dates and restore after computation
    std::vector<std::string> original_dates = std::move(data_.asset.all_dates);
    data_.asset.all_dates = backtest_dates;

    // Initialize global feature store
    feature_store_ = std::make_unique<GlobalFeatureStore>(
        num_assets, num_ts_workers, asset_axis().hash_at(num_assets),
        data_.config.feature_dir,
        static_cast<int>(cs_worker_core), static_cast<int>(io_worker_core));

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

    // Launch workers: IO + TS[] + CS
    progress_ = std::make_shared<misc::ParallelProgress>(num_workers_);
    workers_.clear();
    workers_.reserve(num_workers_);

    // IO worker (core N-1, last core)
    workers_.push_back(std::async(std::launch::async, [this, io_worker_core, total_dates]() {
      if (misc::Affinity::supported()) {
        misc::Affinity::pin_to_core(io_worker_core);
      }
      io_worker(static_cast<int>(io_worker_core), *feature_store_, progress_->get_handle(static_cast<int>(io_worker_core)), total_dates);
    }));

    // TS workers (cores 0 to N-3)
    for (unsigned int i = 0; i < num_ts_workers; ++i) {
      workers_.push_back(std::async(std::launch::async, [this, i]() {
        if (misc::Affinity::supported()) {
          misc::Affinity::pin_to_core(i);
        }
        sequential_worker(static_cast<int>(i), data_, *feature_store_, progress_->get_handle(static_cast<int>(i)));
      }));
    }

    // CS worker (core N-2, second-to-last core)
    workers_.push_back(std::async(std::launch::async, [this, cs_worker_core]() {
      if (misc::Affinity::supported()) {
        misc::Affinity::pin_to_core(cs_worker_core);
      }
      crosssectional_worker(static_cast<int>(cs_worker_core), data_, *feature_store_, progress_->get_handle(static_cast<int>(cs_worker_core)));
    }));

    // Wait for completion
    for (auto &worker : workers_)
      worker.wait();
    progress_->stop();
    workers_.clear();

    // Restore original all_dates
    data_.asset.all_dates = std::move(original_dates);

    // Cleanup feature store
    feature_store_.reset();

    // Finalize
    status_ = cancel_flag_.load() ? ComputeStatus::Cancelled : ComputeStatus::Completed;

    std::cout << "\n=== Feature Computation "
              << (status_ == ComputeStatus::Completed ? "Complete" : "Cancelled") << " ===\n"
              << "Processed: " << total_dates << " dates\n"
              << std::endl;

    // Disable High Performance Mode: GUI resumes
    data_.DisableHighPerformanceMode();
    std::cout << "[High Performance Mode] Disabled - GUI thread resumed\n"
              << std::endl;
  });
}

void ComputeService::stop_compute() {
  if (status_ != ComputeStatus::Running) {
    return;
  }

  cancel_flag_.store(true);
  std::cout << "[Compute] Cancelling..." << std::endl;

  // Wait for compute thread to finish
  if (compute_thread_.valid()) {
    compute_thread_.wait();
  }
}

} // namespace GUI::Features
