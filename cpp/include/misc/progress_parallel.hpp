#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
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

  // 全局汇总计数 +n (跨 worker 共享, 推进汇总行). worked 见 ParallelProgress
  void bump_summary(size_t n = 1, bool worked = true) const;

  // 汇总行附加说明 (如 "20230103: 2311/4800 assets").
  // 每个 worker 各自节流到最多 refresh_interval_ms_ 写一次, 避免细粒度任务
  // 反复抢 note_mutex_ (见 ParallelProgress::set_summary_note_from_worker).
  void set_summary_note(const std::string &note) const;

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
    // 本 worker 上次真正写汇总附注的时刻 (ms), -1 = 从未写过.
    // 节流用, 见 set_summary_note_from_worker.
    std::atomic<long long> last_note_ms{-1};
    char label[64] = {0};
    char message[96] = {0};
  };

public:
  // summary_unit 非空时在 worker 条上方多渲染一行全局汇总:
  //   各 worker 调 bump_summary 推进计数, 生产端调 set_summary_total 设置
  //   总量 (总量未知时可以给估算值, 边跑边收敛, exact=true 后停止标记 '~'),
  //   set_summary_note 附加一段说明 (如 "27/781 days listed").
  explicit ParallelProgress(int num_workers, int refresh_interval_ms = 100,
                            const std::string &summary_unit = "")
      : num_workers_(num_workers),
        refresh_interval_ms_(refresh_interval_ms),
        slots_(num_workers),
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

  // 汇总行总量 — 生产端边列举边更新估算, 全部列举完后 exact=true
  void set_summary_total(size_t total, bool exact) {
    summary_total_.store(total, std::memory_order_relaxed);
    summary_exact_.store(exact, std::memory_order_relaxed);
  }

  // 汇总行附加说明 (显示在计数之后). 供生产端 (非 worker, 调用频率低,
  // 一天一次量级) 直接调用, 不节流.
  void set_summary_note(const std::string &note) {
    std::lock_guard<std::mutex> lock(note_mutex_);
    summary_note_ = note;
  }

  // 供 worker 端 (ProgressHandle) 调用的节流版本 —— 每个 worker 各自最多
  // refresh_interval_ms_ 写一次: 附注只是刷新线程每 100ms 读一次的展示状态,
  // 逐资产粒度的细任务却会让 72 个 worker 反复抢同一把 note_mutex_,
  // 节流后大多数调用在拿锁之前就返回.
  void set_summary_note_from_worker(int worker_id, const std::string &note) {
    WorkerSlot &slot = slots_[worker_id];
    const long long now = elapsed_ms();
    const long long last = slot.last_note_ms.load(std::memory_order_relaxed);
    if (last >= 0 && now - last < refresh_interval_ms_)
      return;
    slot.last_note_ms.store(now, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(note_mutex_);
    summary_note_ = note;
  }

  // 汇总计数 +n — 与 ProgressHandle::bump_summary 等价, 给非 worker 线程用.
  //
  // worked=false 表示这一单位是"秒回"的 (如增量编码里整天命中完成标记):
  // 计进度但不参与 ETA. 跳过与真干活的单位成本差三四个数量级, 混在一起算
  // 平均速率, ETA 会荒谬地乐观 (实测 82% 时报 53s, 实际还有半小时).
  void bump_summary(size_t n = 1, bool worked = true) { bump_summary_internal(n, worked); }

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

  void bump_summary_internal(size_t n, bool worked) {
    summary_done_.fetch_add(n, std::memory_order_relaxed);
    if (!worked)
      return;
    // 第一个真干活单位的完成时刻 = 计时起点 (它之前的跳过阶段与速率无关).
    // 单位成本取之后的平均到达间隔, 所以要 worked ≥ 2 才有得算.
    long long unset = -1;
    first_worked_ms_.compare_exchange_strong(unset, elapsed_ms(), std::memory_order_relaxed);
    summary_worked_.fetch_add(n, std::memory_order_relaxed);
  }

  long long elapsed_ms() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start_time_)
        .count();
  }

  // 汇总行占一行, 排在 worker 条上方
  bool has_summary() const { return !summary_unit_.empty(); }
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

  // 终端显示宽度: CJK/全角占 2 列, 其余按 1 列
  static size_t display_width(const char *s) {
    size_t w = 0;
    const unsigned char *p = reinterpret_cast<const unsigned char *>(s);
    while (*p) {
      unsigned cp = *p;
      int n = 1;
      if (cp >= 0xF0)
        n = 4, cp &= 0x07;
      else if (cp >= 0xE0)
        n = 3, cp &= 0x0F;
      else if (cp >= 0xC0)
        n = 2, cp &= 0x1F;
      for (int k = 1; k < n && p[k]; ++k)
        cp = (cp << 6) | (p[k] & 0x3F);
      p += n;
      w += (cp >= 0x1100 &&
            ((cp >= 0x2E80 && cp <= 0xA4CF) || (cp >= 0xAC00 && cp <= 0xD7A3) ||
             (cp >= 0xF900 && cp <= 0xFAFF) || (cp >= 0xFE30 && cp <= 0xFE4F) ||
             (cp >= 0xFF00 && cp <= 0xFF60) || (cp >= 0xFFE0 && cp <= 0xFFE6) ||
             cp <= 0x115F))
               ? 2
               : 1;
    }
    return w;
  }

  // 补空格到固定显示宽度; 超宽则按字符截断 — 各行同列必须严格对齐
  static std::string pad_display(const char *s, size_t cols) {
    std::string out(s);
    size_t w = display_width(out.c_str());
    while (w > cols && !out.empty()) {
      // 去掉最后一个完整 UTF-8 字符
      size_t cut = out.size() - 1;
      while (cut > 0 && (static_cast<unsigned char>(out[cut]) & 0xC0) == 0x80)
        --cut;
      out.resize(cut);
      w = display_width(out.c_str());
    }
    out.append(cols - w, ' ');
    return out;
  }

  void render_summary(std::ostringstream &buffer) {
    const size_t done = summary_done_.load(std::memory_order_relaxed);
    const size_t total = summary_total_.load(std::memory_order_relaxed);
    const bool exact = summary_exact_.load(std::memory_order_relaxed);
    const int lines_up = total_lines();

    buffer << "\033[" << lines_up << "A\r";

    const float progress = total > 0
                               ? std::min(1.0f, static_cast<float>(done) / static_cast<float>(total))
                               : 0.0f;
    const int filled = static_cast<int>(bar_width_ * progress);

    buffer << "[";
    for (int j = 0; j < bar_width_; ++j)
      buffer << (j < filled ? '#' : (j == filled && done < total ? '>' : ' '));
    buffer << "] " << std::setw(3) << static_cast<int>(progress * 100) << "% ";

    buffer << done << "/" << (exact ? "" : "~") << total;
    buffer << " " << summary_unit_;

    {
      std::lock_guard<std::mutex> lock(note_mutex_);
      if (!summary_note_.empty())
        buffer << " | " << summary_note_;
    }

    const long long now_ms = elapsed_ms();
    buffer << " | " << fmt_duration(now_ms / 1000);

    // ETA 只用"真干活"的单位标定成本, 并假设剩下的单位都要真干活.
    //
    // 这个悲观假设是刻意的: 增量编码里没编过的天几乎总是聚在末尾, 按已完成
    // 的跳过/真编混合比例外推, 等于拿 3ms/天 去摊剩下 20s/天 的活, 只会给出
    // 一个越走越离谱的乐观数. 宁可先报高再往下收敛.
    const size_t worked = summary_worked_.load(std::memory_order_relaxed);
    const long long first_ms = first_worked_ms_.load(std::memory_order_relaxed);
    const double per_unit_s =
        (worked >= 2 && first_ms >= 0) ? (now_ms - first_ms) / 1000.0 / (worked - 1) : 0.0;
    if (per_unit_s > 0.0 && done < total) {
      buffer << " elapsed, ETA " << fmt_duration(static_cast<long long>(per_unit_s * (total - done)));
      buffer << " (" << std::fixed << std::setprecision(1);
      if (per_unit_s < 1.0)
        buffer << 1.0 / per_unit_s << "/s)";
      else
        buffer << per_unit_s << "s each)";
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

      // 所有字段定宽 — 多行并排滚动时列必须严格对齐:
      //   百分比 3, 计数各 3, 标签按显示宽度补齐 (中文占 2 列)
      buffer << "] " << std::setw(3) << static_cast<int>(progress * 100) << "% "
             << "(" << std::setw(3) << current << "/" << std::setw(3) << total << ") "
             << pad_display(slot.label, label_cols_);

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
  int label_cols_ = 24; // 标签列显示宽度 (代码 6 + 空格 + 中文名 ≤8 字)
  int refresh_interval_ms_;

  std::vector<WorkerSlot> slots_;

  // 全局汇总 (可选): 累计数 + 总量 (可为估算) + 附加说明
  std::atomic<size_t> summary_done_{0};
  std::atomic<size_t> summary_total_{0};
  std::atomic<bool> summary_exact_{false};
  // ETA 标定用: 真干活的单位数, 与第一个此类单位的完成时刻 (ms, 未发生为 -1)
  std::atomic<size_t> summary_worked_{0};
  std::atomic<long long> first_worked_ms_{-1};
  std::string summary_unit_;
  std::mutex note_mutex_;
  std::string summary_note_;
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

inline void ProgressHandle::bump_summary(size_t n, bool worked) const {
  if (progress_) {
    progress_->bump_summary_internal(n, worked);
  }
}

inline void ProgressHandle::set_summary_note(const std::string &note) const {
  if (progress_ && worker_id_ >= 0) {
    progress_->set_summary_note_from_worker(worker_id_, note);
  }
}

} // namespace misc
