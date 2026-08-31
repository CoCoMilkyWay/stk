#pragma once

#include <atomic>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace misc {

// Forward declaration
class ParallelProgress;

// Worker handle: lightweight handle for updating progress
// Each worker thread gets one handle bound to a fixed slot index
class ProgressHandle {
public:
  ProgressHandle() : progress_(nullptr), worker_id_(-1) {}

  // Move constructor
  ProgressHandle(ProgressHandle &&other) noexcept
      : progress_(other.progress_), worker_id_(other.worker_id_) {
    other.progress_ = nullptr;
    other.worker_id_ = -1;
  }

  // Move assignment
  ProgressHandle &operator=(ProgressHandle &&other) noexcept {
    if (this != &other) {
      progress_ = other.progress_;
      worker_id_ = other.worker_id_;
      other.progress_ = nullptr;
      other.worker_id_ = -1;
    }
    return *this;
  }

  // Delete copy (move-only)
  ProgressHandle(const ProgressHandle &) = delete;
  ProgressHandle &operator=(const ProgressHandle &) = delete;

  // Update progress (fast, lock-free)
  void update(size_t current, size_t total, const std::string &msg = "") const;

  // Set label (e.g., asset code)
  void set_label(const std::string &label) const;

  // 全局汇总计数 +n (跨 worker 共享, 用于"已完成 N / 共 M"那一行)
  void bump_summary(size_t n = 1) const;

  // 汇总行上的第二个计数 (如主计数是 pair, 第二个是 asset)
  void bump_summary_secondary(size_t n = 1) const;

  // Check if handle is valid
  bool valid() const { return progress_ != nullptr && worker_id_ >= 0; }

private:
  friend class ParallelProgress;
  ProgressHandle(ParallelProgress *progress, int worker_id)
      : progress_(progress), worker_id_(worker_id) {}

  ParallelProgress *progress_;
  int worker_id_;
};

// Parallel progress tracker: manages all worker progress displays
// Usage:
//   auto tracker = std::make_shared<ParallelProgress>(num_workers);
//   auto handle = tracker->get_handle(worker_id);  // Get handle for specific slot
//   handle.update(i, total, "processing...");  // Worker updates progress
class ParallelProgress : public std::enable_shared_from_this<ParallelProgress> {
private:
  // Cache-line aligned worker slot (prevents false sharing)
  struct alignas(64) WorkerSlot {
    std::atomic<size_t> current{0};
    std::atomic<size_t> total{0};
    std::atomic<bool> dirty{false};
    char label[64] = {0};
    char message[96] = {0};
  };

public:
  // summary_total > 0 时在 worker 条上方多渲染一行全局汇总 (已完成/总数 + ETA),
  // 由各 worker 调 ProgressHandle::bump_summary 推进.
  explicit ParallelProgress(int num_workers, int refresh_interval_ms = 100,
                            size_t summary_total = 0,
                            const std::string &summary_unit = "")
      : num_workers_(num_workers),
        refresh_interval_ms_(refresh_interval_ms),
        slots_(num_workers),
        summary_total_(summary_total),
        summary_unit_(summary_unit),
        start_time_(std::chrono::steady_clock::now()),
        running_(true),
        initialized_(false) {

    // Print initial empty progress bars (+1 line for the summary if enabled)
    for (int i = 0; i < total_lines(); ++i) {
      std::cout << std::string(bar_width_ + 60, ' ') << "\n";
    }
    std::cout << std::flush;

    initialized_ = true;

    // Start refresh thread
    refresh_thread_ = std::thread(&ParallelProgress::refresh_loop, this);
  }

  ~ParallelProgress() {
    stop();
  }

  // Get handle for specific worker slot (no acquisition, just direct binding)
  ProgressHandle get_handle(int worker_id) {
    return ProgressHandle(this, worker_id);
  }

  // 汇总行上再挂一个计数 (主计数走 pair 这类细粒度单位以便 ETA 平滑,
  // 第二个走 asset 这类粗粒度单位). 须在开跑前设置.
  void set_summary_secondary(size_t total, const std::string &unit) {
    summary_total2_ = total;
    summary_unit2_ = unit;
  }

  // Stop refresh thread and finalize display
  void stop() {
    if (running_.exchange(false, std::memory_order_release)) {
      if (refresh_thread_.joinable()) {
        refresh_thread_.join();
      }

      if (initialized_) {
        refresh_all_lines(true);
        std::cout << "\n"
                  << std::flush;
      }
    }
  }

private:
  friend class ProgressHandle;

  // Internal update (called by handle)
  void update_internal(int worker_id, size_t current, size_t total, const std::string &msg) {
    WorkerSlot &slot = slots_[worker_id];
    slot.current.store(current, std::memory_order_relaxed);
    slot.total.store(total, std::memory_order_relaxed);

    if (!msg.empty()) {
      size_t len = std::min(msg.size(), sizeof(slot.message) - 1);
      std::memcpy(slot.message, msg.c_str(), len);
      slot.message[len] = '\0';
    }

    slot.dirty.store(true, std::memory_order_release);
  }

  // Internal set label (called by handle)
  void set_label_internal(int worker_id, const std::string &label) {
    WorkerSlot &slot = slots_[worker_id];
    size_t len = std::min(label.size(), sizeof(slot.label) - 1);
    std::memcpy(slot.label, label.c_str(), len);
    slot.label[len] = '\0';
  }

  void bump_summary_internal(size_t n) {
    summary_done_.fetch_add(n, std::memory_order_relaxed);
  }

  void bump_summary_secondary_internal(size_t n) {
    summary_done2_.fetch_add(n, std::memory_order_relaxed);
  }

  // 汇总行占一行, 排在 worker 条上方
  bool has_summary() const { return summary_total_ > 0; }
  int total_lines() const { return num_workers_ + (has_summary() ? 1 : 0); }

  // "12m34s"
  static std::string fmt_duration(long long seconds) {
    std::ostringstream os;
    if (seconds >= 3600)
      os << seconds / 3600 << "h" << std::setw(2) << std::setfill('0') << (seconds % 3600) / 60 << "m";
    else
      os << seconds / 60 << "m" << std::setw(2) << std::setfill('0') << seconds % 60 << "s";
    return os.str();
  }

  void render_summary(std::ostringstream &buffer) {
    const size_t done = summary_done_.load(std::memory_order_relaxed);
    const int lines_up = total_lines();

    buffer << "\033[" << lines_up << "A\r";

    const float progress = static_cast<float>(done) / static_cast<float>(summary_total_);
    const int filled = static_cast<int>(bar_width_ * progress);

    buffer << "[";
    for (int j = 0; j < bar_width_; ++j)
      buffer << (j < filled ? '#' : (j == filled && done < summary_total_ ? '>' : ' '));
    buffer << "] " << std::setw(3) << static_cast<int>(progress * 100) << "% "
           << done << "/" << summary_total_;
    if (!summary_unit_.empty())
      buffer << " " << summary_unit_;

    if (summary_total2_ > 0) {
      buffer << " | " << summary_done2_.load(std::memory_order_relaxed) << "/" << summary_total2_;
      if (!summary_unit2_.empty())
        buffer << " " << summary_unit2_;
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::steady_clock::now() - start_time_)
                             .count();
    buffer << " | " << fmt_duration(elapsed);
    if (done > 0 && done < summary_total_) {
      const long long eta = static_cast<long long>(
          elapsed * (static_cast<double>(summary_total_ - done) / static_cast<double>(done)));
      buffer << " elapsed, ETA " << fmt_duration(eta);
      buffer << " (" << std::fixed << std::setprecision(1)
             << (elapsed > 0 ? static_cast<double>(done) / static_cast<double>(elapsed) : 0.0)
             << "/s)";
    }

    buffer << "\033[K";
    buffer << "\033[" << lines_up << "B";
  }

  void refresh_loop() {
    while (running_.load(std::memory_order_acquire)) {
      refresh_all_lines(false);
      std::this_thread::sleep_for(std::chrono::milliseconds(refresh_interval_ms_));
    }
  }

  void refresh_all_lines(bool force) {
    std::ostringstream buffer;

    // 汇总行每次刷新都重画 (ETA/elapsed 一直在走, 不看 dirty)
    if (has_summary())
      render_summary(buffer);

    for (int i = 0; i < num_workers_; ++i) {
      WorkerSlot &slot = slots_[i];

      bool is_dirty = slot.dirty.exchange(false, std::memory_order_acquire);
      if (!is_dirty && !force)
        continue;

      size_t current = slot.current.load(std::memory_order_relaxed);
      size_t total = slot.total.load(std::memory_order_relaxed);

      // Move cursor to target line
      int lines_up = num_workers_ - i;
      buffer << "\033[" << lines_up << "A\r";

      // Render progress bar
      float progress = (total > 0) ? static_cast<float>(current) / total : 0.0f;
      int filled = static_cast<int>(bar_width_ * progress);

      buffer << "[";
      for (int j = 0; j < bar_width_; ++j) {
        if (j < filled)
          buffer << "=";
        else if (j == filled && current < total)
          buffer << ">";
        else
          buffer << " ";
      }
      buffer << "] " << std::setw(3) << static_cast<int>(progress * 100) << "% "
             << "(" << current << "/" << total << ")";

      if (slot.label[0] != '\0') {
        buffer << " " << slot.label;
      }
      if (slot.message[0] != '\0') {
        buffer << " - " << slot.message;
      }

      buffer << "\033[K";
      buffer << "\033[" << lines_up << "B";
    }

    std::cout << buffer.str() << std::flush;
  }

  int num_workers_;
  int bar_width_ = 40;
  int refresh_interval_ms_;

  std::vector<WorkerSlot> slots_;

  // 全局汇总 (可选)
  std::atomic<size_t> summary_done_{0};
  size_t summary_total_;
  std::string summary_unit_;
  std::atomic<size_t> summary_done2_{0};
  size_t summary_total2_ = 0;
  std::string summary_unit2_;
  std::chrono::steady_clock::time_point start_time_;

  std::atomic<bool> running_;
  bool initialized_;
  std::thread refresh_thread_;
};

// ProgressHandle member function implementations (after ParallelProgress is defined)
inline void ProgressHandle::update(size_t current, size_t total, const std::string &msg) const {
  if (progress_ && worker_id_ >= 0) {
    progress_->update_internal(worker_id_, current, total, msg);
  }
}

inline void ProgressHandle::set_label(const std::string &label) const {
  if (progress_ && worker_id_ >= 0) {
    progress_->set_label_internal(worker_id_, label);
  }
}

inline void ProgressHandle::bump_summary(size_t n) const {
  if (progress_) {
    progress_->bump_summary_internal(n);
  }
}

inline void ProgressHandle::bump_summary_secondary(size_t n) const {
  if (progress_) {
    progress_->bump_summary_secondary_internal(n);
  }
}

} // namespace misc
