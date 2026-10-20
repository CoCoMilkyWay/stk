// Compute Service - Manages feature computation with multi-threaded workers
// High-performance mode: Prefetch + TS workers + CS worker + IO worker (两档同一管线, 见 worker/feature_pipeline.hpp)
// 线程模型 (gui/util/Service.hpp, 随对象): 进度面 = 自身 (done/total = 已落盘天 / 档的日期轴);
//   store / 调度面 / 仪表盘全是 pipeline<T> 的局部, 一轮一建
#pragma once

#include "features/Storage/FeatureLevels.hpp" // TierId / PerTier
#include "gui/util/Service.hpp"
#include "worker/feature_workers.hpp" // StageLayout (UI 核布局表)

#include <string>

// Forward declarations
struct SharedData;

namespace GUI::Features {

// ============================================================================
// Compute Config (= 请求)
// ============================================================================

struct ComputeConfig {
  // 日环下限 = LabelHold::MIN_RING_SLOTS (开盘型标签 T+N 跨日回填, 旧日 slot 持到结清; 截面跨日读 d−1 再持 1 日; 少于此死锁),
  // ComputeService.cpp static_assert 对账
  static constexpr int kMinRingSlots = 13;
  // 按档 (默认值按 Lob / Bar 序, ComputeService.cpp 对账). 预取 / 截面 / 落盘核数: 各自按日轮转, 从时序让核 (Lob 时序是瓶颈, 不让).
  // 日环: 截面 / 落盘多核要有多日同时可开 —— 悬挂日 (kMinRingSlots − 2) + 截面跨日 1 + 时序当日 + 截面 K_cs 日 + 落盘 K_io 日, 少了只降吞吐不死锁
  PerTier<int> ring_slots{{kMinRingSlots + 2, kMinRingSlots + 2}};
  PerTier<int> pf_cores{{1, 1}};
  PerTier<int> cs_cores{{1, 1}};
  PerTier<int> io_cores{{1, 1}};
  int adopt_pct = 50; // TS 负载再平衡阈值 N (%), 0 = 关闭领养
  std::string pool;   // 算哪个池 (池名, 档由名定, 见 FeaturePool.hpp; Compute 页每池一个按钮)
};

// ============================================================================
// Compute Service
// ============================================================================

class ComputeService : public Service<ComputeService, ComputeConfig>, public Progress {
  friend Service<ComputeService, ComputeConfig>;

public:
  static constexpr const char *kName = "ComputeWorker";
  explicit ComputeService(SharedData &data); // 末尾 Start
  ~ComputeService() { Stop(); }

  // 核布局由机器核数 + 本档预取 / 截面 / 落盘核数推导 (StageLayout::make), UI 只交 ComputeConfig; 先断言再转 Service::Request
  void Request(ComputeConfig config);
  bool cancelling() const { return busy() && cancel_.load(std::memory_order_relaxed); }

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return *this; }
  bool run(ComputeConfig &config);

  SharedData &data_;
};

} // namespace GUI::Features
