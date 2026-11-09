#pragma once

#include "features/Storage/FeatureLevels.hpp" // PerTier / LEVEL_COUNT
#include "misc/date.hpp"
#include "worker/compute/Dashboard.hpp" // ComputeStats::Core

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <span>
#include <thread>
#include <vector>

struct FeatureLib;
template <class T>
class FeatureStore;

// ============================================================================
// TS 调度面: per-asset 处置权 + 认领/完成游标 (负载再平衡)
//
// 资产静态分给 worker 只是初始形态: 权重模型有偏差 + 核速不一, 差距持续拉开 (漂移),
// 而落后者的活别人接不了就只能全员等它. 所以处置权可转移 —— 领跑 worker 在日环边
// 干等 slot 时, 对落后 ≥2 天且该让出 (配额 > 0) 的候选按前沿升序逐个尝试, 接手其
// 放得进双方余额的最重资产, 立刻回填该资产落下的旧日期 —— 含 victim 的当天 (claim CAS
// 裁决, victim 见 owner 已变即跳过, 当天即刻变轻; 旧日 slot 未计满必为
// BUSY, 见 FeatureStore), 之后并入自己的日循环. 反馈零延迟.
// 额度 = quota (按权重计, 协调线程按实测忙碌占比每窗重算, 半量修正, 见 TsTuner): 偏差小于一个资产就不动.
//
// 不要改成"自由领任务 (全局队列 / 每日游标)"或"按日窃取 (借一天, 次日归还)":
//   各核专注自己的资产是刻意的 —— per-asset core 状态 (~百 KB, first-touch 在初始 owner 核)
//   跟着 owner 走, 换核才付冷启动. 日间负载起伏 (轮换) 由日环深吸收 (各核前沿可相差至多 P 天, 按需扩格), 不搬资产;
//   只有持续落后 (领跑者被日环卡住 且 对方落后 ≥ 2 天) 才永久转移处置权 —— 一次转移修正一次
//   权重偏差, 之后不再换核. 自由领 / 按日窃取会把每次噪声都变成换核, 且偏差不收敛 (天天重复偷).
//
//   owner:    处置权路标 (领养 CAS victim→adopter; 原 owner 观察到即弃)
//   claimed:  谁处理 (asset, d) 由 CAS claimed d-1→d 唯一裁决 (转移瞬间的
//             双写/漏写由它兜底, owner 只是"该不该去试"的提示)
//   done:     已完成日 didx, release 发布 core 状态 (接手方 acquire 后可续算)
//             (两者从 first − 1 起: first = 首个要算的日, 续算 = 尾窗日数, 全量 0)
//   frontier: per-worker 已完成日数 (领养的观测面; 0 起 —— 续算也是, f ≥ 1 才说明对方的核已构造 / 接回)
//
// cores (per-asset 跨日状态, 档的 Core, 见 Pipeline.hpp TsRole) 由初始 owner 在自己核上构造 (first-touch);
// 领养方经 frontier 的 release/acquire 链观察到构造完成后才会触碰.
// ============================================================================
struct TsSchedule {
  // 全部数组按档 A 轴下标 (sub) 索引, 大小 = A 轴大小 (= 张量 A 维)
  std::vector<std::atomic<int32_t>> owner;   // 处置权 worker id (每个资产都有 owner)
  std::vector<std::atomic<int32_t>> claimed; // 已认领日 didx, first − 1 起
  std::vector<std::atomic<int32_t>> done;    // 已完成日 didx, first − 1 起
  std::vector<int64_t> weight;               // 区间内源负载 (TierFeed::weight 逐日和) —— 初始分配 / 领养配额的单位

  // Per-worker 已完成日数 (worker 自报, release; 领养方 acquire 读 —— 与被领养资产的
  // core 构造/状态写入构成 happens-before). 大小 = TS worker 数.
  std::vector<std::atomic<uint32_t>> frontier;

  // Per-worker 领养配额 (权重): 正 = 该让出, 负 = 可接手, 0 = 不动. 协调线程每窗重写 (TsTuner), 领养方 CAS 扣减
  std::vector<std::atomic<int64_t>> quota;
  TsSchedule(size_t num_assets, size_t num_workers, size_t first)
      : owner(num_assets), claimed(num_assets), done(num_assets),
        weight(num_assets, 0), frontier(num_workers), quota(num_workers) {
    const int32_t before = static_cast<int32_t>(first) - 1;
    for (size_t i = 0; i < num_assets; ++i) {
      owner[i].store(-1, std::memory_order_relaxed);
      claimed[i].store(before, std::memory_order_relaxed);
      done[i].store(before, std::memory_order_relaxed);
    }
    for (size_t w = 0; w < num_workers; ++w) {
      frontier[w].store(0, std::memory_order_relaxed);
      quota[w].store(0, std::memory_order_relaxed);
    }
  }
};

// ============================================================================
// 日流水线四角色 (两档同一骨架, 档只换源 TierFeed<T>, 见 Pipeline.hpp), 统一骨架:
//   for d in dates: wait_open(store.X_try_open(d)) → 干活 → store.X_close
// 分核依据 = 两条一致性契约 (见 CoreTs.hpp / CoreCs.hpp):
// (核按物理核分, 超线程透明, 见 StageLayout)
//   预取 K 线程: 备源 —— 软源 (Lob: 只暖 page cache, TS 不等它) / 硬源 (Bar: 解码 bar1m 进源格, TS 等 pf_ready); 日间无状态 ⇒ 按日轮转
//   时序 N 线程: TS 是资产局部纯函数 ⇒ 沿 A 维并行; 其余物理核全给它 (每逻辑 CPU 一个; 处置权转移再平衡, 见 TsSchedule)
//   截面 K 线程: CS 只读张量第 t 行, 要整个截面到齐 ⇒ 日内沿 T 逐拍串行; 日间无状态 ⇒ 按日轮转
//   落盘 K 线程: 逐列编码 + 写盘 (见 FeatureDay); 日间无状态 ⇒ 按日轮转
//   (按日轮转 = 日 d 归第 d % K 个, 核内日序递增; 各独占一个物理核)
// 段间握手只经 FeatureStore 的日窗 (日环, 见 FeatureStore.hpp).
//
// StageCtx = 四角色共有的面; 预取 / 时序另带档的源 (TierFeed) / 调度面 (TsSchedule), 见 Pipeline.hpp.
// dates = 本库日期轴 (本档源日历 ∩ 上游库日期轴; 见 FeatureLib.hpp lib_parent), 四角色的 date_idx 都是它的下标.
//   续算: dates = 库末 tail 日 (尾窗) + 新日; 尾窗日张量启动前已读回日环 (FeatureStore::seed),
//   预取 / 截面对它们只 close, 时序从 tail 起 (核由续算点接回, CoreTs::resume), 落盘照常重写 (标签此轮才结清).
// worker_id = 全局 worker 序号 (TS 0..N−1 在前, 见 StageLayout), 与 pin 的 CPU 号无关; pin 由 launch 侧完成,
// worker 内只用它做日志 / stats / frontier 下标.
// ============================================================================
template <class T>
struct StageCtx {
  int worker_id;
  FeatureStore<T> &store;
  const PerTier<FeatureLib> &libs; // 本库 libs[T::kId] (A 轴 / 目录) + 同池 CS 旁读档的库 libs[TierSide<T>] (无旁读列时不读); 余档空
  std::span<const misc::Ymd> dates;
  size_t tail; // 尾窗日数 (dates 前 tail 日; 全量 0)
  const std::atomic<bool> &cancel;
  ComputeStats::Core &stat; // 本核发布槽 (单写者)
};

// 干等原语 (四角色共用): try_open() 为真即返回它; 否则 cancel → 返回空, 未 cancel → on_idle()
// (干等时可做的活: 时序 = 领养回填, 其余无), sleep kIdleMs 并把实测睡眠时长记入 idle_us (仪表盘忙碌占比; on_idle 算忙).
// on_idle 返回 true = 这一轮是日环边干等 (时序: 源已就绪而日环满), 睡眠另记入 edge_us.
// 先试后查 cancel: 已就绪的照做 —— 落盘靠它在取消后刷完截面已关的日子.
inline constexpr size_t kIdleMs = 10;

template <class TryOpen, class OnIdle>
auto wait_open(const std::atomic<bool> &cancel, ComputeStats::Core &stat, TryOpen &&try_open, OnIdle &&on_idle) {
  for (;;) {
    if (auto opened = try_open())
      return opened;
    if (cancel.load(std::memory_order_relaxed))
      return decltype(try_open()){};
    const bool edge = on_idle();
    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(kIdleMs));
    const size_t slept = static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0).count());
    stat.idle_us.fetch_add(slept, std::memory_order_relaxed);
    if (edge)
      stat.edge_us.fetch_add(slept, std::memory_order_relaxed);
  }
}

template <class TryOpen>
auto wait_open(const std::atomic<bool> &cancel, ComputeStats::Core &stat, TryOpen &&try_open) {
  return wait_open(cancel, stat, try_open, [] { return false; });
}

// 截面 / 落盘: 第 lane 核 (共 lanes 核) 处理日 lane, lane + lanes, ...
// 截面: cs_try_open 等本日全部 TS 写完, 逐拍 compute_and_store (实盘同一调用), cs_close 交给 IO.
// 落盘: io_try_open 等本日截面写完, 逐层落盘, io_close 清零交还 slot, 直到本核的日期刷完.
// 档无关, 各自 TU 显式实例化两档 (预取 / 时序随档的源进档 TU, 见 Pipeline.hpp)
// 截面用时 (特征面板 Node Time): 每截面核一份, 跑完由 pipeline 合并落盘.
//   cols[层][列] = CoreCs::col_cycles; total = 各日 begin_day → end_day 的 TSC 周期 (total − Σ列 = 有效集扫描 / 旁读读入 / 节拍调度)
struct CsClock {
  std::array<std::vector<uint64_t>, LEVEL_COUNT> cols;
  uint64_t total = 0;
  uint64_t days = 0;
};
template <class T>
void cs_loop(const StageCtx<T> &ctx, size_t lane, size_t lanes, CsClock &clock);
template <class T>
void io_loop(const StageCtx<T> &ctx, size_t lane, size_t lanes);
