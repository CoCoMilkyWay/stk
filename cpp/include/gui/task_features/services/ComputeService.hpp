// Compute Service - Manages feature computation with multi-threaded workers
// High-performance mode: Prefetch + TS workers + CS worker + IO worker (两档同一管线, 见 worker/feature_pipeline.hpp)
// 线程模型 (gui/util/Service.hpp, 随对象): 进度面 = 自身 (done/total = 已落盘天 / 档的日期轴);
//   store / 调度面 / 仪表盘全是 pipeline<T> 的局部, 一轮一建
#pragma once

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
  // 池下限 = LabelHold::MIN_POOL_SLOTS (开盘档标签 T+N 跨日回填, 旧日 slot 持到结清; 少于此死锁), ComputeService.cpp static_assert 对账
  static constexpr int kMinPoolSlots = 12;
  int pool_slots = kMinPoolSlots + 2;
  int adopt_pct = 50; // TS 负载再平衡阈值 N (%), 0 = 关闭领养
  std::string lib;    // 算哪个库 (库名, 档由名定, 见 FeatureLib.hpp; Compute 页每库一个按钮)
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
