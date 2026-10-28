// Compute Service - Manages feature computation with multi-threaded workers
// High-performance mode: Prefetch + TS workers + CS worker + IO worker (两档同一管线, 见 worker/feature_pipeline.hpp)
// 线程模型 (gui/util/Service.hpp, 随对象): 进度面 = 自身 (done/total = 已落盘天 / 档的日期轴);
//   store / 调度面 / 仪表盘全是 pipeline<T> 的局部, 一轮一建
#pragma once

#include "features/Storage/FeatureLevels.hpp" // TierId / PerTier
#include "gui/util/Service.hpp"
#include "misc/date.hpp"
#include "worker/feature_workers.hpp" // StageLayout (UI 核布局表)

#include <string>
#include <vector>

// Forward declarations
struct SharedData;

namespace GUI::Features {

// 档的源日历: 本档原始数据现在能服务的交易日, 只读数据层已换进的产物 (GUI 线程), 随同步 / 编码前移.
//   Bar  交易日 [PIPELINE_START_DATE, assetinfo.bar1m_end]; Fund 收盘推进到次日盘前 ⇒ 还须早于日历末个交易日 (日历截到 today)
//   Lob  回测日历 ≤ 末个已编码日 (中间缺编码的日也在轴内, 落默认张量)
// 本池日期轴 = 源日历截到各旁读档 (tier_side) 当前池的库末日, 旁读库须落盘齐 (池表规划, 随请求快照给 worker) ⇒ Bar 末日 ≥ Lob 末日
std::vector<misc::Ymd> source_dates(const SharedData &data, TierId tier);

// ============================================================================
// Compute Config (= 请求)
// ============================================================================

struct ComputeConfig {
  // 日环下限 = LabelHold::MIN_RING_SLOTS (日级标签 / 平仓顺延跨日回填, 旧日 slot 持到结清; 截面跨日读 d−1 再持 1 日; 少于此死锁),
  // ComputeService.cpp static_assert 对账
  static constexpr int kMinRingSlots = 11;
  // 日环 = 最慢时序核永不被池卡住的上界: kMinRingSlots (悬挂 PEND_DAYS + 时序下一日 + 截面跨日) + 截面在飞 ≤ K_cs + 落盘在飞 ≤ K_io,
  // + kRingJitter 吸收截面 / 落盘单日耗时波动与落盘前缀乱序. 不给时序核间前沿差留: 领跑核撞池边正是领养的触发条件
  static constexpr int kRingJitter = 1;
  int ring_slots(TierId t) const { return kMinRingSlots + cs_cores[t] + io_cores[t] + kRingJitter; }
  // 按档 (默认值按 Lob / Bar 序, ComputeService.cpp 对账). 预取 / 截面 / 落盘核数: 各自按日轮转, 从时序让核 (Lob 时序是瓶颈, 不让).
  PerTier<int> pf_cores{{1, 1}};
  PerTier<int> cs_cores{{1, 1}};
  PerTier<int> io_cores{{1, 1}};
  int adopt_pct = 50;           // TS 负载再平衡阈值 N (%), 0 = 关闭领养
  std::string pool;             // 算哪个池 (池名, 档由名定, 见 FeatureLib.hpp; Compute 页每池一个按钮)
  std::vector<misc::Ymd> dates; // 本池日期轴 (池表规划, 见 source_dates)
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
