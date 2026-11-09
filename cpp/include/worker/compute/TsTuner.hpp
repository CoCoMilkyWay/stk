#pragma once

#include "worker/compute/Dashboard.hpp"

#include <chrono>
#include <cstddef>
#include <functional>
#include <vector>

class StoreRing;
struct TsSchedule;

// ============================================================================
// 时序调参 —— 日环深度与领养额度全自动 (无 UI 参数). 时序核间的偏差分两类, 各有各的解:
//   轮换 (各核负载随时段起伏, 均值相当): 日环深吸收. 领跑核在日环边干等, 之后它自己又成了最慢者
//     —— 当初的干等落在了关键路径上, 记为已确认损失; 加深日环只对这一类有收益.
//   漂移 (某核持续偏慢: 权重模型误差 / 核速差): 领跑核干等但最慢者始终是同一个 (损失永不确认),
//     扩格只是推迟撞日环边, 总耗时不变 —— 由领养修正 (TsSchedule::quota).
//   下游瓶颈 (截面 / 落盘跟不上, 时序再快也不多出活): 两者都无益.
// 收益上限 = 下游饥饿: 窗内截面 / 落盘各段平均干等占比取小 × 窗长 —— 下游空等时序的时间, 时序提速至多换回这么多.
//
// 初始日环 (开跑前, 见 pipeline): 下游段 (悬挂 + 截面 / 落盘在飞, 少于 MIN_RING_SLOTS 死锁) + 时序余量 S;
//   S = 初始分配下各核逐日负载 (TierFeed::weight, 同速假设) 的离散模拟 (ts_lead_days), 再按内存截断.
// 运行中 (TsTuner, 协调线程每 100ms tick), 窗口 = 最慢前沿推进 kWindowDays 天 (且不短于 kWindowMinMs):
//   扩格: min(窗内已确认损失, 下游饥饿) ≥ kLossFrac × 窗长, 且内存余量够 (RingBudget) ⇒ 扩一格 (每窗至多一格)
//   配额: 下游饥饿 ≥ kLossFrac × 窗长且窗内有日环边干等 ⇒ 各核忙碌占比 β (1 − 干等 / 墙钟) 偏离均值的半量:
//         quota = ½ × 持仓权重 × (β − β̄) / β (正 = 该让出, 负 = 可接手); 误差逐窗减半, 不被噪声带着过冲. 否则清零
// ============================================================================

// 日环内存预算 (Linux MemAvailable 口径: 可回收的 page cache 算可用): 用到总内存的 1 − kMemMargin 为止
struct RingBudget {
  static constexpr double kMemMargin = 0.1;
  size_t per_slot;           // 一格的字节 (张量 + 源格)
  size_t affordable() const; // 现在还能再添的格数
  size_t cap() const;        // 总内存口径的格数上限
};

// 时序余量 S (天): load[w][d] = 第 w 核第 d 日的负载; 核开工日 d 须 ≤ 最慢核已完成日数 + S.
// 取模拟总耗时不超过无限深 (1 + kLeadSlack) 倍的最小 S (≥ 1: 同负载也有日内相位噪声), 至多 max_lead
size_t ts_lead_days(const std::vector<std::vector<double>> &load, size_t max_lead);

class TsTuner {
public:
  static constexpr size_t kWindowDays = 10;
  static constexpr long long kWindowMinMs = 5000;
  static constexpr double kLossFrac = 0.01;

  // grow = 扩一格 (FeatureStore::grow); total = 日期轴天数 (前沿到底的核不参与)
  TsTuner(const StoreRing &store, TsSchedule &sched, const ComputeStats &stats, size_t total, RingBudget budget, std::function<void()> grow);
  void tick();

private:
  void close_window(long long now_us, size_t f_min, const std::vector<size_t> &f);

  const StoreRing &store_;
  TsSchedule &sched_;
  const ComputeStats &stats_;
  const size_t total_;
  const RingBudget budget_;
  const std::function<void()> grow_;
  const std::chrono::steady_clock::time_point t0_;

  std::vector<size_t> edge_;    // [ts] 上一 tick 的 edge_us
  std::vector<size_t> pending_; // [ts] 自上次是最慢者以来的日环边干等 (µs), 它再成最慢者时确认
  std::vector<size_t> idle0_;   // [ts] 窗口起点的 idle_us
  size_t cs_idle0_ = 0;         // 窗口起点截面各核 idle_us 之和
  size_t io_idle0_ = 0;         // 窗口起点落盘各核 idle_us 之和
  long long win_us_ = 0;        // 窗口起点
  size_t win_f_ = 0;            // 窗口起点的最慢前沿
  size_t loss_us_ = 0;          // 窗内已确认损失
  size_t edge_win_us_ = 0;      // 窗内领跑核的日环边干等
};
