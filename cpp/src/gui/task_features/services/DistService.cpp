#include "gui/task_features/services/DistService.hpp"
#include "misc/affinity.hpp"
#include "shared/Config.hpp"
#include "shared/SharedData.hpp"

#include <cassert>

namespace GUI::Features {

// ============================================================================
// SimpleThreadPool
// ============================================================================

SimpleThreadPool::SimpleThreadPool(size_t num_threads) {
  threads_.reserve(num_threads);
  for (size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back(&SimpleThreadPool::worker, this);
  }
}

SimpleThreadPool::~SimpleThreadPool() {
  stop_ = true;
  cv_.notify_all();
  for (auto &t : threads_) {
    if (t.joinable())
      t.join();
  }
}

void SimpleThreadPool::worker() {
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

void SimpleThreadPool::wait_all() {
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
// DistService
// ============================================================================

DistService::DistService(const std::string &features_dir)
    : features_dir_(features_dir),
      pool_(std::make_unique<SimpleThreadPool>(misc::Affinity::core_count())) {}

DistService::~DistService() {
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

asio::awaitable<void> DistService::ComputeLoop(SharedData &data) {
  coro_running_ = true;
  auto &dist = data.dist;

  while (!coro_stop_) {
    // Handle compute request
    if (compute_requested_.exchange(false)) {
      // Guard: require a selected feature
      if (data.feature.selection.primary_feature_idx < 0) {
        dist.compute.status = Dist::Compute::Status::Idle;
        continue;
      }

      dist.compute.reset();
      dist.compute.status = Dist::Compute::Status::Building;

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
        // Dispatch build_all to thread pool
        auto submit = [this](std::function<void()> task) {
          pool_->submit(std::move(task));
        };

        dist.build_all(months, features_dir_, data.feature, data.asset, submit);

        // Wait for completion (poll)
        while (dist.compute.done.load() < dist.compute.total.load()) {
          if (dist.compute.cancel.load()) {
            dist.compute.status = Dist::Compute::Status::Cancelled;
            break;
          }
          co_await asio::steady_timer(co_await asio::this_coro::executor,
                                      std::chrono::milliseconds(16))
              .async_wait(asio::use_awaitable);
        }

        if (dist.compute.status != Dist::Compute::Status::Cancelled) {
          // Query with default grouping
          dist.compute.status = Dist::Compute::Status::Querying;
          dist.query(Dist::Input::GroupBy::MONTH);
          dist.build_trajectory();
          dist.compute.status = Dist::Compute::Status::Done;
        }
      } else {
        dist.compute.status = Dist::Compute::Status::Done;
      }

      // Update input cache
      dist.input.update_cache(data.feature.selection.primary_feature_idx,
                              data.feature.selection.selected_level,
                              data.config.start_date + "-" +
                                  data.config.end_date);
    }

    // Handle query request
    if (query_requested_.exchange(false)) {
      if (!dist.cache.empty()) {
        dist.compute.status = Dist::Compute::Status::Querying;
        dist.query(dist.input.group_by);
        dist.build_trajectory();
        dist.compute.status = Dist::Compute::Status::Done;
      }
    }

    // Yield
    co_await asio::steady_timer(co_await asio::this_coro::executor,
                                std::chrono::milliseconds(16))
        .async_wait(asio::use_awaitable);
  }

  coro_running_ = false;
}

void DistService::StartCompute(CoroManager &coro, SharedData &data) {
  if (coro_running_)
    return;
  coro_stop_ = false;
  coro_ = coro.Spawn(ComputeLoop(data));
  while (!coro_running_) {
    coro.Poll();
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

void DistService::StopCompute(CoroManager &coro, SharedData & /*data*/) {
  if (!coro_running_)
    return;
  coro_stop_ = true;
  while (coro_running_) {
    coro.Poll();
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  coro_.reset();
}

void DistService::RequestCompute() { compute_requested_ = true; }

void DistService::RequestQuery() { query_requested_ = true; }

} // namespace GUI::Features
