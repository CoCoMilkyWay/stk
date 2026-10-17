// Compute Service - Manages feature computation with multi-threaded workers
// High-performance mode: Prefetch + TS workers + CS worker + IO worker
// 线程模型 (gui/util/Worker.hpp, 随对象): 进度面 = 自身 (done/total = 已落盘天 / 回测交易日);
//   store / 调度面 / 仪表盘全是 run 的局部, 一轮一建
#pragma once

#include "gui/util/Worker.hpp"

#include <array>
#include <cassert>
#include <cstddef>
#include <sstream>
#include <string>

// Forward declarations
struct SharedData;

namespace GUI::Features {

// ============================================================================
// Stage Layout - 核布局: 各 stage 占哪些核, 由机器核数唯一推导 (worker 数不可配)
//
//   [TS × num_ts] [CS] [IO] [预取]   预取独占核 (默认)
//   [TS × num_ts] [CS] [IO+预取]     预取与落盘共核 (两者都 IO-bound 大部分时间
//                                    阻塞, 互不打架; 小核数机器把预取核让给 TS)
//
// 全部线程 pin 死 —— 没有浮动线程抢占 pinned worker 的时间片.
// stage 顺序 = UI 表格顺序 = 预取, TS..., CS, IO.
// 进度不再按行挂 worker: 仪表盘 (FeatureProgress) 自行采样, 见 feature_workers.hpp.
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
    bool shared = false;
  };

  static constexpr size_t kStageCount = 4;

  int total_cores = 0;
  int num_ts = 0; // TS worker 数, 核 0..num_ts-1
  bool prefetch_shared = false;

  int total_threads() const { return num_ts + 3; }

  Stage stage(ComputeStage kind) const {
    switch (kind) {
    case ComputeStage::Prefetch:
      return {kind, "预取 Prefetch", "顺日期把逐笔帧读进 page cache, 领先最慢时序核不超过 pool slots + 2 天",
              1, prefetch_shared ? 0 : 1, prefetch_shared ? num_ts + 1 : num_ts + 2, prefetch_shared ? num_ts + 1 : num_ts + 2, prefetch_shared};
    case ComputeStage::TS:
      return {kind, "时序 TS", "逐资产 decode + LOB 重建 + DAG, 写 L0/L1 张量; 领跑核自动接手落后核的资产 (处置权转移)",
              num_ts, num_ts, 0, num_ts - 1, false};
    case ComputeStage::CS:
      return {kind, "截面 CS", "本日全部时序核写完后逐秒重放截面 (与实盘同一调用)",
              1, 1, num_ts, num_ts, false};
    case ComputeStage::IO:
      return {kind, "落盘 IO", "本日截面写完后逐层落盘, 清零交还 slot",
              1, 1, num_ts + 1, num_ts + 1, false};
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
// Compute Config (= 请求)
// ============================================================================

struct ComputeConfig {
  // 池下限 = LabelReturn::MIN_POOL_SLOTS (开盘档标签 T+N 跨日回填, 旧日 slot 持到结清; 少于此死锁), ComputeService.cpp static_assert 对账
  static constexpr int kMinPoolSlots = 12;
  int pool_slots = kMinPoolSlots + 2;
  int adopt_pct = 50; // TS 负载再平衡阈值 N (%), 0 = 关闭领养
  bool prefetch_share_io = false;
};

// ============================================================================
// Compute Service
// ============================================================================

class ComputeService : public Worker<ComputeService, ComputeConfig>, public Progress {
  friend Worker<ComputeService, ComputeConfig>;

public:
  static constexpr const char *kName = "ComputeWorker";
  explicit ComputeService(SharedData &data); // 末尾 Start
  ~ComputeService() { Stop(); }

  // 核布局由机器核数自动推导 (StageLayout::make), UI 只交 ComputeConfig; 先断言再转 Worker::Request
  void Request(ComputeConfig config);
  bool cancelling() const { return busy() && cancel_.load(std::memory_order_relaxed); }

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return *this; }
  bool run(ComputeConfig &config);

  SharedData &data_;
};

} // namespace GUI::Features
