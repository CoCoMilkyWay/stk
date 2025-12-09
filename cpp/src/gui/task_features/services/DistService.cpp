#include "gui/task_features/services/DistService.hpp"
#include "shared/SharedData.hpp"
#include "shared/Feature.hpp"
#include "shared/Config.hpp"
#include "shared/Asset.hpp"
#include "misc/affinity.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>

namespace GUI::Features {

// ============================================================================
// SimpleThreadPool Implementation
// ============================================================================

SimpleThreadPool::SimpleThreadPool(size_t num_threads) {
  threads_.reserve(num_threads);
  for (size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back(&SimpleThreadPool::worker_thread, this);
  }
}

SimpleThreadPool::~SimpleThreadPool() {
  stop_flag_ = true;
  condition_.notify_all();
  
  for (auto &thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

void SimpleThreadPool::worker_thread() {
  while (!stop_flag_) {
    std::function<void()> task;
    
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      condition_.wait(lock, [this] {
        return stop_flag_ || !tasks_.empty();
      });
      
      if (stop_flag_ && tasks_.empty()) {
        return;
      }
      
      if (!tasks_.empty()) {
        task = std::move(tasks_.back());
        tasks_.pop_back();
      }
    }
    
    if (task) {
      ++active_tasks_;
      task();
      --active_tasks_;
    }
  }
}

void SimpleThreadPool::wait_all() {
  while (true) {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      if (tasks_.empty() && active_tasks_ == 0) {
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

// ============================================================================
// DistService Implementation
// ============================================================================

DistService::DistService(const std::string &features_dir)
    : reader_(features_dir),
      features_dir_(features_dir),
      thread_pool_(std::make_unique<SimpleThreadPool>(misc::Affinity::core_count())) {
  std::cout << "[DistService] Initialized with " << misc::Affinity::core_count() 
            << " worker threads" << std::endl;
}

DistService::~DistService() {
  // Stop coroutine if running
  coro_should_stop_ = true;
  
  // Wait for coroutine to finish (with timeout)
  int wait_count = 0;
  while (coro_running_ && wait_count < 1000) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    ++wait_count;
  }
  
  // Coroutine handle will auto-cancel on destruction
  coro_.reset();
  
  // Thread pool will be automatically destroyed
}

// ============================================================================
// Coroutine Loop
// ============================================================================

asio::awaitable<void> DistService::ComputeLoop(SharedData &data) {
  coro_running_ = true;
  std::cout << "[DistService] Coroutine loop started" << std::endl;

  while (!coro_should_stop_) {
    // Check for full compute request
    if (compute_requested_.exchange(false)) {
      std::cout << "[DistService] Starting computation" << std::endl;
      try {
        data.dist.compute.status = Dist::Compute::Status::Idle;
        data.dist.compute.error_message.clear();

        // Check if recompute is needed
        if (data.dist.need_recompute(data.feature, data.config)) {
          data.dist.input.update_cache(data.feature, data.config);
          data.dist.invalidate_all();
        }

        // Start async data loading (dispatches to thread pool, returns immediately)
        load_data(data.dist, data.feature, data.config, data.asset);
        if (data.dist.compute.cancel_flag) {
          data.dist.compute.status = Dist::Compute::Status::Cancelled;
          continue;
        }
        
        // Continue to wait_for_loading state (will be handled in main loop)
        continue; 
      } catch (const std::exception &e) {
        data.dist.compute.status = Dist::Compute::Status::Error;
        data.dist.compute.error_message = e.what();
        std::cerr << "[DistService] Error: " << e.what() << std::endl;
      }
    }
    
    // Poll async loading progress
    if (data.dist.compute.status == Dist::Compute::Status::LoadingData &&
        data.dist.compute.load_completed_counter) {
      
      size_t completed = data.dist.compute.load_completed_counter->load();
      size_t failed = data.dist.compute.load_failed_counter->load();
      size_t total = data.dist.compute.load_tasks_total;
      
      // Update progress
      if (completed + failed >= total) {
        std::cout << "[DistService] Loading complete: " << completed << " succeeded, " 
                  << failed << " failed" << std::endl;
        
        try {
          // Integrate per-date buffers via Dist's API
          using BufferVec = std::vector<Dist::PerDateBuffer>;
          auto buffers_ptr = std::static_pointer_cast<BufferVec>(data.dist.compute.load_buffers);
          
          std::cout << "  Integrating " << completed << " date buffers..." << std::endl;
          data.dist.raw.merge_buffers(*buffers_ptr, data.asset.items.size());
          std::cout << "  Integration complete: " << data.dist.raw.n_samples << " samples" << std::endl;
          
          // Clear async loading state
          data.dist.compute.load_buffers.reset();
          data.dist.compute.load_completed_counter.reset();
          data.dist.compute.load_failed_counter.reset();
          
          // Continue to next stage
          if (data.dist.compute.cancel_flag) {
            data.dist.compute.status = Dist::Compute::Status::Cancelled;
            continue;
          }
          
          // Apply grouping
          apply_grouping(data.dist);
          if (data.dist.compute.cancel_flag) {
            data.dist.compute.status = Dist::Compute::Status::Cancelled;
            continue;
          }

          // Compute statistics
          compute_statistics(data.dist);
          if (data.dist.compute.cancel_flag) {
            data.dist.compute.status = Dist::Compute::Status::Cancelled;
            continue;
          }

          // Build cache
          build_cache(data.dist);
          
          data.dist.compute.status = Dist::Compute::Status::Completed;
          std::cout << "[DistService] Computation completed" << std::endl;

        } catch (const std::exception &e) {
          data.dist.compute.status = Dist::Compute::Status::Error;
          data.dist.compute.error_message = e.what();
          std::cerr << "[DistService] Error during integration: " << e.what() << std::endl;
        }
      }
    }

    // Check for cache rebuild request
    if (cache_rebuild_requested_.exchange(false)) {
      if (data.dist.stats.valid) {
        try {
          build_cache(data.dist);
        } catch (const std::exception &e) {
          data.dist.compute.status = Dist::Compute::Status::Error;
          data.dist.compute.error_message = e.what();
        }
      }
    }

    // Yield to allow other tasks
    co_await asio::steady_timer(co_await asio::this_coro::executor,
                                 std::chrono::milliseconds(16))
        .async_wait(asio::use_awaitable);
  }

  coro_running_ = false;
}

void DistService::StartCompute(CoroManager &coro_mgr, SharedData &data) {
  if (coro_running_)
    return;

  coro_should_stop_ = false;
  coro_ = coro_mgr.Spawn(ComputeLoop(data));

  // Blocking wait until coroutine starts
  while (!coro_running_) {
    coro_mgr.Poll();
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

void DistService::StopCompute(CoroManager &coro_mgr, SharedData & /*data*/) {
  if (!coro_running_)
    return;

  coro_should_stop_ = true;

  // Blocking wait until coroutine exits
  while (coro_running_) {
    coro_mgr.Poll();
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }

  coro_.reset();
}

// ============================================================================
// UI Triggers
// ============================================================================

void DistService::RequestCompute() {
  std::cout << "[DistService] Compute requested" << std::endl;
  compute_requested_ = true;
}

void DistService::RequestCacheRebuild() {
  cache_rebuild_requested_ = true;
}

void DistService::CancelCompute() {
  // This will be checked by compute functions
  // (Placeholder - actual cancel mechanism in Dist structure)
}

// ============================================================================
// Query Status
// ============================================================================

bool DistService::is_computing() const {
  // Check if any compute operation is active
  return compute_requested_.load() || cache_rebuild_requested_.load();
}

float DistService::get_progress() const {
  // Progress is tracked in Dist::Compute structure
  return 0.0f; // Placeholder
}

std::string DistService::get_status_text() const {
  if (!coro_running_) {
    return "Idle";
  }
  if (compute_requested_) {
    return "Computing...";
  }
  if (cache_rebuild_requested_) {
    return "Building cache...";
  }
  return "Ready";
}

// ============================================================================
// Internal Computation Functions
// ============================================================================

void DistService::load_data(Dist &dist, const Feature &feature,
                             const Config &config, const Asset &asset) {
  dist.compute.status = Dist::Compute::Status::LoadingData;
  std::cout << "[DistService] Loading data (async)..." << std::endl;
  std::cout << "  Primary feature idx: " << feature.selection.primary_feature_idx << std::endl;
  std::cout << "  Level: " << feature.selection.selected_level << std::endl;
  std::cout << "  Date range: " << config.start_date << " - " << config.end_date << std::endl;
  std::cout << "  Features dir: " << features_dir_ << std::endl;

  // Helper: Remove dashes from date string (2025-01-01 -> 20250101)
  auto normalize_date = [](const std::string &date) -> std::string {
    std::string normalized;
    for (char c : date) {
      if (c != '-') normalized += c;
    }
    return normalized;
  };
  
  std::string start_date_norm = normalize_date(config.start_date);
  std::string end_date_norm = normalize_date(config.end_date);
  std::cout << "  Normalized date range: " << start_date_norm << " - " << end_date_norm << std::endl;
  
  // Step 1: Scan available dates (fast, can be synchronous)
  std::vector<std::string> available_dates;
  
  if (std::filesystem::exists(features_dir_)) {
    std::cout << "  Scanning directory: " << features_dir_ << std::endl;
    for (const auto &year_entry : std::filesystem::directory_iterator(features_dir_)) {
      if (!year_entry.is_directory()) continue;
      std::string year = year_entry.path().filename().string();
      if (year.size() != 4) continue;
      
      for (const auto &month_entry : std::filesystem::directory_iterator(year_entry.path())) {
        if (!month_entry.is_directory()) continue;
        std::string month = month_entry.path().filename().string();
        if (month.size() != 2) continue;
        
        for (const auto &day_entry : std::filesystem::directory_iterator(month_entry.path())) {
          if (!day_entry.is_directory()) continue;
          std::string day = day_entry.path().filename().string();
          if (day.size() != 2) continue;
          
          std::string date = year + month + day;
          
          // Check if date is in config range (both in YYYYMMDD format now)
          if (date >= start_date_norm && date <= end_date_norm) {
            // Check if feature file exists for selected level
            int level = feature.selection.selected_level;
            std::string feature_file = day_entry.path().string() + "/features_L" + 
                                       std::to_string(level) + ".bin";
            if (std::filesystem::exists(feature_file)) {
              available_dates.push_back(date);
            }
          }
        }
      }
    }
  } else {
    std::cout << "  ERROR: Features directory does not exist: " << features_dir_ << std::endl;
    return;
  }
  
  std::sort(available_dates.begin(), available_dates.end());
  std::cout << "  Found " << available_dates.size() << " available dates" << std::endl;
  
  if (available_dates.empty()) {
    std::cout << "  WARNING: No dates found in range" << std::endl;
    return;
  }
  
  // Step 2: Dispatch parallel loading via Dist's API
  // Dist manages buffers and worker tasks internally
  std::cout << "  Dispatching " << available_dates.size() << " complete workflows to " 
            << misc::Affinity::core_count() << " workers..." << std::endl;
  
  auto submit_fn = [this](std::function<void()> task) {
    thread_pool_->submit(std::move(task));
  };
  
  auto [buffers, completed, failed] = dist.dispatch_parallel_loading(
      available_dates, features_dir_, feature, config, asset, submit_fn);
  
  // Store state for async polling
  dist.compute.load_tasks_total = available_dates.size();
  dist.compute.load_buffers = buffers;
  dist.compute.load_completed_counter = completed;
  dist.compute.load_failed_counter = failed;
  
  std::cout << "  Workflows dispatched, polling asynchronously..." << std::endl;
}

void DistService::apply_grouping(Dist &dist) {
  dist.compute.status = Dist::Compute::Status::Grouping;
  dist.apply_time_grouping_async();
}

void DistService::compute_statistics(Dist &dist) {
  dist.compute.status = Dist::Compute::Status::Computing;
  
  // Compute statistics in parallel using thread pool
  // Each statistic can be computed independently
  
  dist.compute_integrity();
  
  // Submit parallel tasks
  std::vector<std::function<void()>> tasks;
  
  tasks.push_back([&dist]() { dist.compute_moments_parallel(); });
  tasks.push_back([&dist]() { dist.compute_quantiles_parallel(); });
  tasks.push_back([&dist]() { dist.compute_trajectory_parallel(); });
  tasks.push_back([&dist]() { dist.compute_heterogeneity_parallel(); });
  tasks.push_back([&dist]() { dist.compute_pdf_parallel(); });
  
  for (auto &task : tasks) {
    thread_pool_->submit(std::move(task));
  }
  
  // Wait for all tasks to complete
  thread_pool_->wait_all();
  
  // Compute scale robustness (sequential)
  dist.compute_scale_robustness();
  
  dist.stats.valid = true;
  dist.stats.grouped_version = dist.grouped.raw_version;
}

void DistService::build_cache(Dist &dist) {
  dist.compute.status = Dist::Compute::Status::BuildingCache;
  dist.build_visualization_cache();
  dist.compute.status = Dist::Compute::Status::Completed;
}

} // namespace GUI::Features

