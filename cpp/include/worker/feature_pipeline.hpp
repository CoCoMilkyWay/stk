#pragma once

#include "features/Storage/FeatureStore.hpp"
#include "misc/affinity.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"
#include "shared/FeatureLib.hpp"
#include "worker/feature_workers.hpp"

#include <algorithm>
#include <cassert>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// ============================================================================
// 特征管线骨架 pipeline<T> (两档同一份, 只许档 TU include: 实例化要档的 Core 完整类型 + 档的数学模式).
// 档只写它的源 TierFeed<T> (档 TU 特化, 先于 pipeline<T> 显式实例化):
//   using Core                          per-asset 跨日状态 (CoreLob / CoreBar)
//   kHardFeed                           TS 是否等预取 (硬源: 预取解码进源环, TS 读环; 软源: 预取只暖 page cache)
//   kUnit                               时序工作量单位 (仪表盘)
//   TierFeed(const PipelineArgs &, const StoreRing &)       全管线一份 (帧目录 / 源环 / 名单 ...)
//   size_t weight(sub) const            初始分配 / 领养挑最轻的权重
//   std::unique_ptr<Core> make_core(sub, worker_id) const   初始 owner 在自己核上构造 (first-touch)
//   size_t prefetch(d)                  预取线程备日 d 的源, 返回字节 (多预取核对不同 d 并发调用)
//   Runner(TierFeed &, worker_id)       每 TS worker 一份的工作区 (Lob: 订单簿 + decoder + reader)
//     size_t run(core, sub, d, day, store)   一个 asset-day (缺源 = 零事件日同路径), 返回源条数
// 截面 / 落盘档无关 (cs_loop / io_loop, 各自 TU).
// ============================================================================
template <class T>
struct TierFeed;

template <class T>
using CoreOf = typename TierFeed<T>::Core;

// 时序角色另带的面: 档的源 + 调度面 + per-asset 跨日状态 (领养方经 frontier 的 release/acquire 链接手)
template <class T>
struct TsRole {
  TierFeed<T> &feed;
  TsSchedule &sched;
  std::vector<std::unique_ptr<CoreOf<T>>> &cores;
};

// 预取: 第 lane 核 (共 lanes 核) 备日 lane, lane + lanes, ... 的源 (各日写各自的源环格, 互不握手).
// 流控 = store.pf_try_open (领先最慢 TS 不超过 P + 余量; 硬源的源环格由此可复用, 见 feed_ring_size).
template <class T>
void prefetch_loop(const StageCtx<T> &ctx, TierFeed<T> &feed, size_t lane, size_t lanes) {
  assert(lane < lanes);
  const int worker_id = ctx.worker_id;
  StoreRing &store = ctx.store;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  // 本核发布槽 (单写者): work = 累计备源字节, idle_us = 累计流控干等微秒
  ComputeStats::Core &stat = ctx.stat;

  TraceNS("PrefetchWorker", 5);
  TraceValue(worker_id);
  TraceThread(("prefetch_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = ctx.dates.size();

  Logger::log("worker_" + std::to_string(worker_id), "Prefetch started: " + std::to_string(total_dates) + " dates");

  size_t cumulative_bytes = 0;
  size_t completed_dates = 0;

  for (size_t date_idx = lane; date_idx < total_dates; date_idx += lanes) {
    if (cancel_requested.load(std::memory_order_relaxed))
      break;

    TraceN("DateLoop");
    TraceTextS(misc::dense(ctx.dates[date_idx]).c_str());

    bool open;
    {
      TraceN("GateWait");
      TraceColor(C_Orange);
      open = wait_open(cancel_requested, stat, [&] { return store.pf_try_open(date_idx); });
    }
    if (!open)
      break;

    {
      TraceN("ReadFiles");
      cumulative_bytes += feed.prefetch(date_idx);
      stat.work.store(cumulative_bytes, std::memory_order_relaxed);
    }

    store.pf_close(date_idx, TierFeed<T>::kHardFeed);
    ++completed_dates;

    TraceFrame;
  }

  const bool cancelled = cancel_requested.load(std::memory_order_relaxed);

  if (cancelled)
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch cancelled: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(completed_dates) + " dates");
  else
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch completed: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(completed_dates) + " dates");
}

// 时序: 逐资产喂源 + DAG, 写张量 (日期主序遍历); 领跑核在池边干等时领养落后核的资产并回填
// (wait_open 的 on_idle, 处置权转移见 TsSchedule).
// 领养节流: 单一阈值 N (%) = sched.adopt_pct (UI 可调, 0 = 关闭领养).
// 触发时机 = 领跑者在池边干等 slot (它已领先, 等待时间白白浪费, 正好拿来帮最慢者).
//   victim 侧: 每天 (预算窗口按其前沿滚动) 最多让出自己持仓的 N% ——
//              让完这波它下一轮几乎不再是最慢者, 也不会被过度掏空;
//   leader 侧: 每天最多领养 (A 轴资产数 / worker 数) 的 N% —— 单核不会一天暴涨吃成新的 lagger.
template <class T>
void ts_loop(const StageCtx<T> &ctx, TsRole<T> &role) {
  using Store = FeatureStore<T>;
  using Day = typename Store::Day;
  const int worker_id = ctx.worker_id;
  Store &store = ctx.store;
  TierFeed<T> &feed = role.feed;
  TsSchedule &sched = role.sched;
  std::vector<std::unique_ptr<CoreOf<T>>> &cores = role.cores;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  // 本核发布槽 (单写者): work = 累计源条数, idle_us = 累计干等微秒;
  // 前沿/持仓在 store/sched 里, 渲染线程自取
  ComputeStats::Core &stat = ctx.stat;

  TraceNS("TSWorker", 5);
  TraceValue(worker_id);
  TraceThread(("ts_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = ctx.dates.size();

  // Find assets initially assigned to this worker
  std::vector<size_t> my_asset_ids;
  for (size_t i = 0; i < sched.owner.size(); ++i) {
    if (sched.owner[i].load(std::memory_order_relaxed) == worker_id) {
      my_asset_ids.push_back(i);
    }
  }

  if (my_asset_ids.empty()) {
    // 无资产 (资产数 < worker 数才会发生): 前沿报到底, 不挡任何人
    sched.frontier[static_cast<size_t>(worker_id)].store(static_cast<uint32_t>(total_dates), std::memory_order_release);
    return;
  }

  // 档的工作区: 每 worker 一个 (非每资产), worker 内资产串行复用 (Lob: 订单簿 + decoder + reader, 跨资产 cache/TLB 常驻)
  typename TierFeed<T>::Runner runner(feed, worker_id);

  // Per-asset 跨日状态 (DAG 暖历史 + Fund 状态机 + 分钟缓冲 [+ TickData]), ~百 KB/资产.
  // 初始 owner 在本核构造 (first-touch); 处置权转移时对象不动, 只换驱动它的工作区.
  {
    TraceN("InitCores");
    // asset_id = A 轴下标: 既是张量列 (fstore::ts_write), 也是 fund_panel 的 A 轴下标 (Panel::build 用同一码表)
    for (const size_t asset_id : my_asset_ids)
      cores[asset_id] = feed.make_core(asset_id, worker_id);
  }

  Logger::log("worker_" + std::to_string(worker_id), "Started: " + std::to_string(my_asset_ids.size()) + " assets, " +
                                                         std::to_string(total_dates) + " dates");

  size_t cumulative_work = 0;
  size_t completed_dates = 0;
  size_t date_work = 0;
  size_t date_assets_processed = 0;

  // 源就绪: 硬源等预取 close 本日 (acquire, 解码写入先于读); 软源不等
  auto feed_ready = [&store](size_t d) { return !TierFeed<T>::kHardFeed || store.pf_ready(d); };

  // 认领 (asset, d): CAS d-1→d 唯一裁决谁处理这个 asset-day —— 处置权转移
  // 瞬间的双写/漏写由它兜底, owner 只是"该不该去试"的路标.
  auto claim = [&sched](size_t asset_id, int32_t d) {
    int32_t expected = d - 1;
    return sched.claimed[asset_id].compare_exchange_strong(expected, d, std::memory_order_acq_rel);
  };

  // 处理一个 asset-day, 返回源条数. 主循环与领养回填共用 —— 两者只差句柄来自哪一天.
  // 本 asset-day 的 ts_close 由 core 决定何时做: 开盘型标签 (T+N) 跨日回填, 旧日句柄持到结清
  // (见 CoreTs / LabelHold 文件头); 最后一日之后由 finish_all 全部结清.
  auto process_asset_day = [&](size_t asset_id, size_t date_idx, const Day &day) -> size_t {
    ++date_assets_processed;
    return runner.run(*cores[asset_id], asset_id, date_idx, day, store);
  };

  // Leader 侧记账: 本 worker 今天已领养数 (每日在日循环头重置).
  // 上限 = 平均每核持仓 (A 轴大小 / worker 数) 的 adopt_pct%; 0 = 关闭领养.
  size_t adopted_today = 0;
  const size_t leader_cap =
      (sched.adopt_pct == 0) ? 0 : std::max<size_t>(1, sched.owner.size() / sched.frontier.size() * sched.adopt_pct / 100);

  // 负载再平衡: 只在池边干等 slot 时调用 (本 worker 已领先满池深, 等待时间正好拿来
  // 帮落后者). 接手 victim 的最轻资产, 立刻回填落下的旧日期 —— 含 victim
  // 的"当天" (claim CAS 唯一裁决: victim 日循环还没轮到它就被本 worker 先
  // claim 走, victim 见 owner 已变直接跳过, 当天即刻变轻). 旧日未计满 ⇒
  // 仍在飞, ts_try_open 必得; 硬源的旧日源格同理未复用 (见 feed_ring_size); 回填后并入本 worker 日循环 (永久转移, 不归还).
  // 两侧 N% 预算限量, 不会一下子掏空/吃撑.
  // 触发条件 (池边干等 + 落后 ≥ 2 天) 与永久转移都是刻意的: 不要改成自由领 / 按日窃取 (理由见 TsSchedule).
  auto try_adopt = [&](size_t my_days_done) {
    if (adopted_today >= leader_cap)
      return;

    // 候选 victim: 所有落后 ≥ 2 天者 (1 天差是相位噪声), 按前沿升序逐个试
    // —— 最慢者预算耗尽/撞车时顺延帮第二慢者, 而不是空转干等.
    // f ≥ 1 兼保证 victim 的 core 构造已通过 frontier 的 release/acquire 发布.
    std::vector<std::pair<size_t, int>> laggers; // (前沿, worker)
    for (int w = 0; w < static_cast<int>(sched.frontier.size()); ++w) {
      const size_t f = sched.frontier[static_cast<size_t>(w)].load(std::memory_order_acquire);
      if (w != worker_id && f >= 1 && my_days_done >= f + 2)
        laggers.emplace_back(f, w);
    }
    std::sort(laggers.begin(), laggers.end());

    for (const auto &[victim_days, victim] : laggers) {
      // victim 手里最轻的资产 (顺便数持仓; 至少给它留 2 个)
      size_t victim_count = 0, pick = SIZE_MAX, pick_weight = SIZE_MAX;
      for (size_t i = 0; i < sched.owner.size(); ++i) {
        if (sched.owner[i].load(std::memory_order_relaxed) != victim)
          continue;
        ++victim_count;
        if (sched.weight[i] < pick_weight) {
          pick_weight = sched.weight[i];
          pick = i;
        }
      }
      if (victim_count < 2 || pick == SIZE_MAX)
        continue;

      // victim 侧预算 CAS: 本窗 (其前沿推进即换窗) 最多让出持仓的 adopt_pct%.
      // 编码 (前沿 << 16) | 已让出数.
      const uint64_t victim_budget = std::max<uint64_t>(1, victim_count * sched.adopt_pct / 100);
      bool budget_ok = false;
      {
        std::atomic<uint64_t> &win = sched.adopt_window[victim];
        uint64_t w = win.load(std::memory_order_relaxed);
        while (true) {
          if ((w >> 16) != victim_days) {
            // 新窗口: 重置并领第 1 个
            if (win.compare_exchange_weak(w, (static_cast<uint64_t>(victim_days) << 16) | 1, std::memory_order_relaxed)) {
              budget_ok = true;
              break;
            }
          } else if ((w & 0xFFFF) >= victim_budget) {
            break; // 本窗预算已耗尽, 试下一个 victim
          } else if (win.compare_exchange_weak(w, w + 1, std::memory_order_relaxed)) {
            budget_ok = true;
            break;
          }
        }
      }
      if (!budget_ok)
        continue;

      // 处置权 CAS: 路标改指本 worker, victim 下一天起直接跳过 pick.
      // 失败 = 与其他领养方撞车 (预算多扣一格, 无妨, 试下一个 victim).
      int32_t expect = victim;
      if (!sched.owner[pick].compare_exchange_strong(expect, worker_id, std::memory_order_acq_rel))
        continue;
      ++adopted_today;

      // victim 可能正在算 pick 的在飞一天: 等它交割 (窗口 = 单个 asset-day)
      auto settled = [&] { return sched.claimed[pick].load(std::memory_order_acquire) == sched.done[pick].load(std::memory_order_acquire); };
      if (!wait_open(cancel_requested, stat, settled))
        return;

      const int32_t from = sched.done[pick].load(std::memory_order_acquire) + 1;
      const int32_t upto = static_cast<int32_t>(my_days_done) - 1; // 回填到本 worker 前沿
      Logger::log("worker_" + std::to_string(worker_id),
                  "adopt: asset " + std::to_string(pick) + " from worker " + std::to_string(victim) +
                      " (gap " + std::to_string(my_days_done - victim_days) + "d), backfill didx " +
                      std::to_string(from) + ".." + std::to_string(upto));

      for (int32_t d = from; d <= upto;) {
        if (cancel_requested.load(std::memory_order_relaxed))
          return;
        TraceN("AdoptBackfill");
        // 处置权可能被更快的 leader 再领养 (本 worker 转做 victim): 剩余回填
        // 归新 owner, 本 worker 不再持有此资产.
        if (sched.owner[pick].load(std::memory_order_relaxed) != worker_id)
          return;
        if (!claim(pick, d)) {
          // 对手凭陈旧 owner 路标赢下在飞一天 (claim CAS 唯一裁决, 见日循环):
          // victim "查 owner → claim" 非原子, 其 claim 可落在上面等交割之后.
          // 等它交割, 从其 done 之后继续.
          if (!wait_open(cancel_requested, stat, settled))
            return;
          d = sched.done[pick].load(std::memory_order_acquire) + 1;
          continue;
        }
        assert(feed_ready(static_cast<size_t>(d)) && "回填日 < 本 worker 前沿, 源必已就绪");
        const Day bday = store.ts_try_open(static_cast<size_t>(d));
        assert(bday && "回填日未计满 ⇒ 仍在飞 (d < 本 worker 前沿 ≤ io_done + P), slot 必得");
        cumulative_work += process_asset_day(pick, static_cast<size_t>(d), bday);
        stat.work.store(cumulative_work, std::memory_order_relaxed);
        sched.done[pick].store(d, std::memory_order_release);
        ++d; // 回填日 ≤ 本 worker 前沿 − 1 < 最后一日, 不会走到 finish_all
      }
      my_asset_ids.push_back(pick);
      return; // 一次领养一个; 还在等 slot 的话下轮再来
    }
  };

  for (size_t date_idx = 0; date_idx < total_dates; ++date_idx) {
    if (cancel_requested.load(std::memory_order_relaxed))
      break;

    TraceN("DateLoop");
    const misc::Ymd date = ctx.dates[date_idx];
    TraceTextS(misc::dense(date).c_str());
    date_work = 0;
    date_assets_processed = 0;
    adopted_today = 0; // leader 侧领养预算按日重置

    // 本日写句柄: 每 worker 每日 open 一次, 之后所有写回是纯指针算术.
    // 池满 (或硬源未就绪) = 本 worker 领先到把池子吃满 —— 干等的时间拿去领养最慢者的资产
    // 并回填 (回填日仍在飞, 不占新 slot), 等 IO 交还 slot 后再继续本日.
    // 只有 sleep 算干等 (领养回填是干活, 见 wait_open).
    Day day;
    {
      TraceN("PoolWait");
      TraceColor(C_Orange);
      day = wait_open(
          cancel_requested, stat, [&] { return feed_ready(date_idx) ? store.ts_try_open(date_idx) : Day{}; },
          [&] { try_adopt(date_idx); }); // date_idx = 本 worker 已完成日数
    }
    if (!day)
      break;

    // Process each asset at this date
    for (size_t i = 0; i < my_asset_ids.size();) {
      const size_t asset_id = my_asset_ids[i];
      // 处置权已转走 (或 claim 输给领养方的回填): 移除, 计数由新 owner 负责
      if (sched.owner[asset_id].load(std::memory_order_relaxed) != worker_id ||
          !claim(asset_id, static_cast<int32_t>(date_idx))) {
        my_asset_ids[i] = my_asset_ids.back();
        my_asset_ids.pop_back();
        continue;
      }
      const size_t work = process_asset_day(asset_id, date_idx, day);
      if (date_idx + 1 == total_dates)
        cores[asset_id]->finish_all(store); // 区间末: 悬挂的开盘型按最近收盘价结算, 旧日句柄全部归还
      date_work += work;
      cumulative_work += work;
      sched.done[asset_id].store(static_cast<int32_t>(date_idx), std::memory_order_release);
      ++i;
      stat.work.store(cumulative_work, std::memory_order_relaxed);
    }

    if (date_assets_processed > 0) {
      Logger::log("worker_" + std::to_string(worker_id), std::to_string(date) + " completed: " + std::to_string(date_assets_processed) + " assets, " + std::to_string(date_work) + " " + TierFeed<T>::kUnit);
    }

    // 本 worker 本日计完; 自报前沿 (release 发布本日全部 core 状态)
    sched.frontier[static_cast<size_t>(worker_id)].store(static_cast<uint32_t>(date_idx + 1), std::memory_order_release);

    completed_dates = date_idx + 1;

    TraceFrame; // Mark frame boundary for timeline
  }

  if (cancel_requested.load(std::memory_order_relaxed)) {
    Logger::log("worker_" + std::to_string(worker_id), "Cancelled: processed " + std::to_string(cumulative_work) + " " + TierFeed<T>::kUnit + " across " + std::to_string(completed_dates) + " dates");
  } else {
    Logger::log("worker_" + std::to_string(worker_id), "Completed: processed " + std::to_string(cumulative_work) + " " + TierFeed<T>::kUnit + " across " + std::to_string(total_dates) + " dates");
  }
}

// 管线: 日环 → 档的源 → 初始分配 → 四角色按 StageLayout pin 死启动 (统一 launch) → 等收尾.
template <class T>
bool pipeline(const PipelineArgs &args) {
  // 核布局: 预取 → TS → CS → IO 四个 stage, 按物理核分配, 全部 pin 死 (见 StageLayout)
  const StageLayout L = StageLayout::make(misc::Affinity::physical_cores(), args.pf_cores, args.cs_cores, args.io_cores);
  const std::vector<misc::Ymd> &dates = args.dates;
  const FeatureLib &lib = args.libs[T::kId];
  const size_t num_assets = lib.axis.size();
  const size_t num_pf = static_cast<size_t>(L.num_pf);
  const size_t num_ts = static_cast<size_t>(L.num_ts);
  const size_t num_cs = static_cast<size_t>(L.num_cs);
  const size_t num_io = static_cast<size_t>(L.num_io);
  assert(!dates.empty() && num_assets >= 1);

  std::cout << "\n=== Phase 2: Feature Computation (" << tier_name(T::kId) << ") ===\n"
            << L.summary() << "\n"
            << "Assets: " << num_assets
            << " | Ring slots: " << args.ring_slots
            << " | Adopt pct: " << args.adopt_pct << "%"
            << " | Dates: " << dates.size() << " (" << dates.front() << " - " << dates.back() << ")\n"
            << std::endl;

  FeatureStore<T> store(num_assets, lib.axis.hash, lib.dir, args.ring_slots);
  assert(store.query_slots() >= CoreOf<T>::kMinRingSlots && "内存上限把日环砍到 MIN_RING_SLOTS 之下, 会死锁");

  TierFeed<T> feed(args, store);

  // 初始分配: A 轴切 num_ts 个连续段, 各段权重和均衡 (资产归其权重中点所在的那一份).
  // 张量 [T][F][A] 的一条缓存行 = 相邻 32 个资产的同一 (t, f): 连续段同核写, 核间只在段界共一行;
  // 交错分配则每次写都跨核 (跨插槽) 抢行, TS 退化为缓存一致性流量.
  // 运行期差距由 worker 间的处置权转移收敛 (见 ts_loop 的 try_adopt), 这里只求起点均衡.
  TsSchedule sched(num_assets, num_ts);
  sched.adopt_pct = args.adopt_pct;
  {
    size_t total = 0;
    for (size_t sub = 0; sub < num_assets; ++sub)
      total += sched.weight[sub] = feed.weight(sub);
    assert(total > 0 && "初始分配: 全轴权重为 0");
    size_t acc = 0;
    for (size_t sub = 0; sub < num_assets; ++sub) {
      const size_t w = std::min(num_ts - 1, (2 * acc + sched.weight[sub]) * num_ts / (2 * total));
      sched.owner[sub].store(static_cast<int32_t>(w), std::memory_order_relaxed);
      acc += sched.weight[sub];
    }
  }
  std::vector<std::unique_ptr<CoreOf<T>>> cores(num_assets);
  TsRole<T> role{feed, sched, cores};

  // 统一 launch(worker_id, name, cpus, fn): 线程名 name (StageLayout::thread_name), pin 到 cpus (StageLayout::cpus_of), worker_id 与 CPU 号无关.
  // 进度 = 仪表盘自行采样 store/sched/stats (拉取式); 采样窗 (PROFILE 模式) 同样拉取式, 见 FeatureProfile.
  ComputeStats stats(num_pf, num_ts, num_cs, num_io);
  FeatureProgress progress(store, sched, stats, dates, TierFeed<T>::kUnit);
  FeatureProfile profile({lib.name, T::kId, TierFeed<T>::kUnit, num_assets, dates, args.ring_slots, args.adopt_pct}, L, store, stats);
  auto ctx = [&](int worker_id, ComputeStats::Core &stat) {
    return StageCtx<T>{worker_id, store, args.libs, dates, args.cancel, stat};
  };
  std::vector<std::future<void>> workers;
  workers.reserve(static_cast<size_t>(L.total_threads()));
  auto launch = [&](int worker_id, std::string name, std::vector<unsigned int> cpus, auto fn) {
    workers.push_back(std::async(std::launch::async, [worker_id, name = std::move(name), cpus = std::move(cpus), fn]() {
      misc::name_thread(name);
      if (misc::Affinity::supported()) {
        const bool pinned = misc::Affinity::pin_to_cores(cpus);
        assert(pinned && "worker pin 失败");
        (void)pinned;
      }
      fn(worker_id);
    }));
  };
  for (const StageLayout::Stage &s : L.stages())
    for (int i = 0; i < s.threads; ++i) {
      const int id = s.first_worker + i;
      std::string name = StageLayout::thread_name(s, i);
      switch (s.kind) {
      case ComputeStage::Prefetch:
        launch(id, std::move(name), L.cpus_of(s, i), [&, lane = static_cast<size_t>(i)](int w) { prefetch_loop<T>(ctx(w, stats.prefetch[lane]), feed, lane, num_pf); });
        break;
      case ComputeStage::TS:
        launch(id, std::move(name), L.cpus_of(s, i), [&](int w) { ts_loop<T>(ctx(w, stats.ts[static_cast<size_t>(w)]), role); });
        break;
      case ComputeStage::CS:
        launch(id, std::move(name), L.cpus_of(s, i), [&, lane = static_cast<size_t>(i)](int w) { cs_loop<T>(ctx(w, stats.cs[lane]), lane, num_cs); });
        break;
      case ComputeStage::IO:
        launch(id, std::move(name), L.cpus_of(s, i), [&, lane = static_cast<size_t>(i)](int w) { io_loop<T>(ctx(w, stats.io[lane]), lane, num_io); });
        break;
      }
    }

  // 等收尾: 已落盘天数镜像进进度面, 采样窗随之推进 (worker 自己查 cancel, 这里报一声并就地收采样窗出报告)
  bool announced = false;
  for (auto &worker : workers)
    while (worker.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready) {
      args.on_io(store.query_io_done());
      profile.tick();
      if (!announced && args.cancel.load(std::memory_order_relaxed)) {
        announced = true;
        std::cout << "[Compute] Cancelling..." << std::endl;
        profile.finish();
      }
    }
  args.on_io(store.query_io_done());
  profile.finish();
  // 仪表盘引用 store/sched/dates, 必须先于它们的析构停掉
  progress.stop();

  const bool completed = !args.cancel.load(std::memory_order_relaxed);
  std::cout << "\n=== Feature Computation (" << tier_name(T::kId) << ") " << (completed ? "Complete" : "Cancelled") << " ===\n"
            << "Processed: " << store.query_io_done() << "/" << dates.size() << " dates\n"
            << std::endl;
  return completed;
}
