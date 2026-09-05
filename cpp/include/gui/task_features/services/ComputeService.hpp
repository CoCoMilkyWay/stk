// Compute Service - Manages feature computation with multi-threaded workers
// High-performance mode: Prefetch + TS workers + CS worker + IO worker
#pragma once

#include "misc/progress_parallel.hpp"
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

// Forward declarations
struct SharedData;
class GlobalFeatureStore;
struct TsSchedule;

namespace GUI::Features {

// ============================================================================
// Stage Layout - 核布局: 各 stage 占哪些核, 由机器核数唯一推导 (worker 数不可配)
//
//   [TS × num_ts] [CS] [IO] [预取]   预取独占核 (默认)
//   [TS × num_ts] [CS] [IO+预取]     预取与落盘共核 (两者都 IO-bound 大部分时间
//                                    阻塞, 互不打架; 小核数机器把预取核让给 TS)
//
// 全部线程 pin 死 —— 没有浮动线程抢占 pinned worker 的时间片.
// 每个 stage 一对 {core, row}: core = pin 的物理核, row = 进度条行号.
// stage 顺序 = UI 表格顺序 = 终端进度顺序 = 预取, TS..., CS, IO.
// ============================================================================
enum class ComputeStage : size_t {
  Prefetch = 0,
  TS,
  CS,
  IO,
};

struct StageLayout {
  struct Stage {
    ComputeStage kind = ComputeStage::Prefetch;
    const char *name = "";
    const char *desc = "";
    int threads = 0;    // worker thread 数
    int cores = 0;      // 独占核心数 (共核 stage 可为 0)
    int first_core = 0; // pin 的首核
    int last_core = 0;  // pin 的尾核
    int first_row = 0;  // 进度首行
    bool shared = false;
  };

  static constexpr size_t kStageCount = 4;

  int total_cores = 0;
  int num_ts = 0; // TS worker 数, 核/行 0..num_ts-1
  bool prefetch_shared = false;

  int progress_rows() const { return num_ts + 3; }

  Stage stage(ComputeStage kind) const {
    switch (kind) {
    case ComputeStage::Prefetch:
      return {kind, "预取 Prefetch", "顺日期把 .bin 读进 page cache, 领先最慢时序核不超过 pool slots + 2 天",
              1, prefetch_shared ? 0 : 1, prefetch_shared ? num_ts + 1 : num_ts + 2, prefetch_shared ? num_ts + 1 : num_ts + 2, 0, prefetch_shared};
    case ComputeStage::TS:
      return {kind, "时序 TS", "逐资产 decode + LOB 重建 + DAG, 写 L0/L1/DEPTH 张量; 领跑核自动接手落后核的资产 (处置权转移)",
              num_ts, num_ts, 0, num_ts - 1, 1, false};
    case ComputeStage::CS:
      return {kind, "截面 CS", "本日全部时序核写完后整日扫截面列",
              1, 1, num_ts, num_ts, num_ts + 1, false};
    case ComputeStage::IO:
      return {kind, "落盘 IO", "摘 DONE slot 落盘并归还池",
              1, 1, num_ts + 1, num_ts + 1, num_ts + 2, false};
    }
    assert(false && "unknown compute stage");
    return {};
  }

  std::array<Stage, kStageCount> stages() const {
    return {stage(ComputeStage::Prefetch), stage(ComputeStage::TS), stage(ComputeStage::CS), stage(ComputeStage::IO)};
  }

  static std::string core_text(const Stage &s) {
    std::ostringstream os;
    os << "#" << s.first_core;
    if (s.last_core != s.first_core)
      os << "-" << s.last_core;
    if (s.shared)
      os << " (共享落盘核)";
    return os.str();
  }

  std::string summary() const {
    std::ostringstream os;
    os << "Stages:";
    bool first = true;
    for (const Stage &s : stages()) {
      os << (first ? " " : " | ") << s.name << "×" << s.cores << " (" << core_text(s) << ")";
      first = false;
    }
    return os.str();
  }

  static StageLayout make(int total_cores, bool prefetch_share_io) {
    StageLayout l;
    l.total_cores = total_cores;
    l.prefetch_shared = prefetch_share_io;
    l.num_ts = total_cores - (prefetch_share_io ? 2 : 3);
    assert(l.num_ts >= 1 && "核数不足: 至少 TS + CS + IO (+ 预取)");
    return l;
  }
};

// ============================================================================
// Compute Status
// ============================================================================

struct ComputeConfig {
  int pool_slots = 4;
  int adopt_pct = 10; // TS 负载再平衡阈值 N (%), 0 = 关闭领养
  bool prefetch_share_io = false;
};

enum class ComputeStatus {
  Idle,
  Running,
  Completed,
  Cancelled,
  Error
};

// ============================================================================
// Compute Service
// ============================================================================

class ComputeService {
private:
  SharedData &data_;

  std::atomic<bool> cancel_flag_{false};
  std::shared_ptr<misc::ParallelProgress> progress_;
  std::vector<std::future<void>> workers_;
  std::atomic<ComputeStatus> status_{ComputeStatus::Idle};

  StageLayout layout_;
  ComputeConfig config_;
  std::chrono::steady_clock::time_point start_time_;

  std::future<void> compute_thread_; // Background compute thread

  // Feature store (allocated during compute)
  std::unique_ptr<GlobalFeatureStore> feature_store_;

  // TS 调度面: per-asset 处置权 / 认领游标 / cores (见 feature_workers.hpp)
  std::unique_ptr<TsSchedule> ts_schedule_;

public:
  ComputeService(SharedData &data);
  ~ComputeService();

  // Lifecycle (background thread, non-blocking)
  // 核布局由机器核数自动推导 (StageLayout::make), UI 只交 ComputeConfig
  void start_compute(ComputeConfig config);
  void request_cancel();
  void stop_compute();

  // Query
  ComputeStatus get_status() const { return status_.load(std::memory_order_relaxed); }
  bool is_running() const { return get_status() == ComputeStatus::Running; }
  bool is_cancelling() const { return cancel_flag_.load(std::memory_order_relaxed); }
  bool is_idle() const { return get_status() == ComputeStatus::Idle || get_status() == ComputeStatus::Completed || get_status() == ComputeStatus::Cancelled; }
};

} // namespace GUI::Features
