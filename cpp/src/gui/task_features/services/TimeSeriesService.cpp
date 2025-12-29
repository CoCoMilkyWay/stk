#include "gui/task_features/services/TimeSeriesService.hpp"
#include "misc/affinity.hpp"
#include "shared/Config.hpp"
#include "shared/SharedData.hpp"

#include <cassert>

namespace GUI::Features {

// ============================================================================
// TimeSeriesThreadPool
// ============================================================================

TimeSeriesThreadPool::TimeSeriesThreadPool(size_t num_threads) {
  threads_.reserve(num_threads);
  for (size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back(&TimeSeriesThreadPool::worker, this);
  }
}

TimeSeriesThreadPool::~TimeSeriesThreadPool() {
  stop_ = true;
  cv_.notify_all();
  for (auto &t : threads_) {
    if (t.joinable())
      t.join();
  }
}

void TimeSeriesThreadPool::worker() {
  while (!stop_) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
      if (stop_ && tasks_.empty())
        return;
      if (!tasks_.empty()) {
        task = std::move(tasks_.back());
        tasks_.pop_back();
      }
    }
    if (task) {
      ++active_;
      task();
      --active_;
    }
  }
}

void TimeSeriesThreadPool::wait_all() {
  while (true) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (tasks_.empty() && active_ == 0)
        break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

// ============================================================================
// TimeSeriesService
// ============================================================================

TimeSeriesService::TimeSeriesService(const std::string &features_dir)
    : features_dir_(features_dir),
      pool_(std::make_unique<TimeSeriesThreadPool>(
          misc::Affinity::core_count())) {}

TimeSeriesService::~TimeSeriesService() {
  coro_stop_ = true;
  int wait = 0;
  while (coro_running_ && wait < 1000) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    ++wait;
  }
  coro_.reset();
}

// ============================================================================
// Coroutine Loop
// ============================================================================

asio::awaitable<void> TimeSeriesService::ComputeLoop(SharedData &data) {
  coro_running_ = true;
  auto &ts = data.timeseries;

  while (!coro_stop_) {
    // Handle compute request
    if (compute_requested_.exchange(false)) {
      // Guard: require a selected feature
      if (data.feature.selection.primary_feature_idx < 0) {
        ts.compute.status = TimeSeries::Compute::Status::Idle;
        continue;
      }

      ts.compute.reset();
      ts.compute.status = TimeSeries::Compute::Status::Building;

      // Collect months from config date range
      std::vector<std::string> months;
      {
        // Parse start/end dates (YYYY-MM-DD or YYYYMMDD)
        auto parse_month = [](const std::string &date) -> std::string {
          std::string d;
          for (char c : date)
            if (c != '-')
              d += c;
          assert(d.size() >= 6);
          return d.substr(0, 6);
        };

        std::string start_month = parse_month(data.config.start_date);
        std::string end_month = parse_month(data.config.end_date);

        // Enumerate months
        int y = std::stoi(start_month.substr(0, 4));
        int m = std::stoi(start_month.substr(4, 2));
        while (true) {
          char buf[8];
          snprintf(buf, sizeof(buf), "%04d%02d", y, m);
          std::string month_key = buf;
          if (month_key > end_month)
            break;
          months.push_back(month_key);
          ++m;
          if (m > 12) {
            m = 1;
            ++y;
          }
        }
      }

      if (!months.empty()) {
        auto submit = [this](std::function<void()> task) {
          pool_->submit(std::move(task));
        };

        // ========== Unified Build: All Stages ==========
        ts.build_all(months, features_dir_, data.feature, data.asset, submit);

        // Wait for completion (poll)
        while (ts.compute.done.load() < ts.compute.total.load()) {
          if (ts.compute.cancel.load()) {
            ts.compute.status = TimeSeries::Compute::Status::Cancelled;
            break;
          }
          co_await asio::steady_timer(co_await asio::this_coro::executor,
                                      std::chrono::milliseconds(16))
              .async_wait(asio::use_awaitable);
        }

        if (ts.compute.status != TimeSeries::Compute::Status::Cancelled) {
          pool_->wait_all();
          ts.finalize_all();
        }
      } else {
        ts.compute.status = TimeSeries::Compute::Status::Done;
      }

      // Update input cache
      ts.input.update_cache(data.feature.selection.primary_feature_idx,
                            data.feature.selection.selected_level,
                            data.config.start_date + "-" +
                                data.config.end_date);
    }

    // Yield
    co_await asio::steady_timer(co_await asio::this_coro::executor,
                                std::chrono::milliseconds(16))
        .async_wait(asio::use_awaitable);
  }

  coro_running_ = false;
}

void TimeSeriesService::StartCompute(CoroManager &coro, SharedData &data) {
  if (coro_running_)
    return;
  coro_stop_ = false;
  coro_ = coro.Spawn(ComputeLoop(data));
  while (!coro_running_) {
    coro.Poll();
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

void TimeSeriesService::StopCompute(CoroManager &coro, SharedData & /*data*/) {
  if (!coro_running_)
    return;
  coro_stop_ = true;
  while (coro_running_) {
    coro.Poll();
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  coro_.reset();
}

void TimeSeriesService::RequestCompute() { compute_requested_ = true; }

} // namespace GUI::Features

