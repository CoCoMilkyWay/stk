#pragma once

#include "features/GraphNode.hpp" // graph::NodeClock (节点用时抽样)
#include "features/Panel/Pool.hpp"
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureSnap.hpp"
#include "features/Storage/FeatureStore.hpp"
#include "misc/affinity.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"
#include "misc/snap.hpp"
#include "worker/compute/Dashboard.hpp"
#include "worker/compute/Layout.hpp"
#include "worker/compute/Profile.hpp"
#include "worker/compute/Stages.hpp"
#include "worker/compute/TierOps.hpp"
#include "worker/compute/TsTuner.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// ============================================================================
// 特征管线骨架 pipeline<T> (两档同一份, 只许档 TU include: 实例化要档的 Core 完整类型 + 档的数学模式).
// 档只写它的源 TierFeed<T> (档 TU 特化), 再定义档入口表 const TierOps <ns>_ops = tier_ops_of<ns::Tier>() (取址即实例化):
//   using Core                          per-asset 跨日状态 (CoreLob / CoreBar), 构造 Core(fund, members, sub)
//   kHardFeed                           TS 是否等预取 (硬源: 预取解码进源格, TS 读格; 软源: 预取只暖 page cache)
//   kUnit                               时序工作量单位 (仪表盘)
//   static source_dates(const SharedData &)                 源日历 (库表规划, 见 ComputeService.hpp)
//   static cell_bytes(num_assets)       一个源格的字节 (日环内存预算; 软源 0)
//   TierFeed(const PipelineArgs &)      全管线一份 (帧目录 / 源格 / 名单 ...)
//   void bind(const StoreRing &)        日环建好后接上 (硬源经源窗给日 d 定址源格, 见 StoreRing::feed_cell)
//   size_t weight(sub, d) const         (资产, 日) 的负载 —— 初始分配 / 初始日环模拟 / 领养配额的单位
//   size_t prefetch(d)                  预取线程备日 d 的源, 返回字节 (多预取核对不同 d 并发调用)
//   cell(sub, d) const                  (资产, 日) 的源格 (Lob: 帧引用; Bar: 255 根 bar1m), 空 = 缺源
//   Runner(TierFeed &, worker_id)       每 TS worker 一份的工作区 (Lob: 订单簿 + decoder + reader)
//     size_t feed(core, sub, d)         源格的源单位逐个进 core (begin_day 与 end_day 之间), 返回源条数; 缺源 = 不喂 (零事件日)
//     void release()                    core 收盘之后归还工作区
//     std::string day_log()             自上次取走以来 (含领养回填) 的源诊断, 接在本日日志行尾 (无 = 空); 取走即清
// 一个 asset-day (骨架唯一一份): core.begin_day → runner.feed → core.end_day → runner.release.
//   与实盘同一路径 (TS → CS 同趟), 回测才能与实盘逐位一致 (见 FeatureLib.hpp 文件头).
// 截面 / 落盘档无关 (cs_loop / io_loop, 各自 TU).
// ============================================================================
template <class T>
struct TierFeed;

template <class T>
using CoreOf = typename TierFeed<T>::Core;

// 时序角色另带的面: 档的源 + 调度面 + per-asset 跨日状态 (领养方经 frontier 的 release/acquire 链接手)
//   + 续算点: 读入 (续算, 各资产构造后接回) / 写出 (末日收盘各资产一条, 跑完由 pipeline 落盘)
template <class T>
struct TsRole {
  TierFeed<T> &feed;
  const fund::Panel &fund;    // Core 构造 (日频 PIT)
  const pool::Panel &members; // Core 构造 (逐日成员)
  TsSchedule &sched;
  std::vector<std::unique_ptr<CoreOf<T>>> &cores;
  const FeatureSnap *snap_in;                          // 全量 = nullptr
  std::span<const typename FeatureStore<T>::Day> tail; // 尾窗日句柄 (老 → 新; 全量空)
  std::vector<std::vector<std::byte>> &snap_out;       // [A] 末日记录 (各由处理该资产末日的 worker 写)
  size_t clock_every;                                  // 节点用时: 每 clock_every 个 asset-day 抽一个 (node_clock_pick)
  std::vector<graph::NodeClock> &clocks;               // [TS worker] 各自累计, 跑完由 pipeline 合并落盘
};

// 节点用时抽样: (资产, 日) 散列后按 every 取模, 抽中的 asset-day 在资产 / 日期两向都散开
inline bool node_clock_pick(size_t asset_id, size_t date_idx, size_t every) {
  uint64_t h = (static_cast<uint64_t>(asset_id) << 32 | date_idx) + 0x9E3779B97F4A7C15ULL;
  h = (h ^ (h >> 30)) * 0xBF58476D1CE4E5B9ULL;
  h = (h ^ (h >> 27)) * 0x94D049BB133111EBULL;
  return (h ^ (h >> 31)) % every == 0;
}

// 用时落盘 output/run/features/<池>/<档>/node_time.json (本轮, 覆盖上一轮), 时序 / 截面两段各自成 100% (不同 worker, 不互比):
//   ts = 抽样 asset-day 的 {samples, total, label, write, nodes: [{node, file, cycles}]}
//   cs = 全部截面日的 {days, total, cols: [{level, code, cycles}]} (本档各层 CS 列)
// 特征面板 Node Time 按文件归并
template <class T>
void write_node_time(LibId lib, const graph::NodeClock &ts, const CsClock &cs) {
  using Dag = typename CoreOf<T>::Graph;
  std::ostringstream f;
  {
    f << "{\n \"ts\": {\"samples\": " << ts.samples << ", \"total\": " << ts.total << ", \"label\": " << ts.label << ", \"write\": " << ts.write
      << ", \"nodes\": [\n";
    for (size_t i = 0; i < Dag::kNodeCount; ++i)
      f << "  {\"node\": \"" << Dag::kNodes[i].node << "\", \"file\": \"" << Dag::kNodes[i].file << "\", \"cycles\": " << ts.cycles[i] << "}"
        << (i + 1 < Dag::kNodeCount ? ",\n" : "\n");
    f << " ]},\n \"cs\": {\"days\": " << cs.days << ", \"total\": " << cs.total << ", \"cols\": [";
    const char *sep = "\n";
    for (size_t l = 0; l < LEVEL_COUNT; ++l)
      for (size_t i = 0; i < cs.cols[l].size(); ++i)
        if (T::LEVELS[l].fields[i].kind == FeatureDataType::CS) {
          f << sep << "  {\"level\": " << l << ", \"code\": \"" << T::LEVELS[l].fields[i].code << "\", \"cycles\": " << cs.cols[l][i] << "}";
          sep = ",\n";
        }
    f << "\n ]}\n}\n";
  }
  disk::write(disk::RunLibNodeTime, lib_key(lib), f.str());
}

// 续算点头: 状态指纹 = kSnapSemver (snap 清单口径) + 各层字段表指纹 (张量口径) + 核布局 (sizeof Core)
template <class T>
SnapHeader snap_header(const FeatureLib &lib, misc::Ymd date) {
  uint64_t state = fnv1a_u64(0xcbf29ce484222325ULL, kSnapSemver);
  for (const LevelInfo &L : T::LEVELS)
    state = fnv1a_u64(state, L.fingerprint);
  state = fnv1a_u64(state, sizeof(CoreOf<T>));
  return {SnapHeader::kMagic, static_cast<uint64_t>(T::kId), static_cast<uint64_t>(date), lib.axis.size(), lib.axis.hash, state};
}

// 预取: 第 lane 核 (共 lanes 核) 备日 lane, lane + lanes, ... 的源 (各日写各自的源格, 互不握手).
// 流控 = store.pf_try_open (日 d 在源窗: 领先计满日不超过 P + 余量; 硬源的源格由此可复用, 见 StoreRing).
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

    if (date_idx >= ctx.tail) { // 尾窗日只 close (TS 不读其源)
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

// 时序: 逐资产喂源 + DAG, 写张量 (日期主序遍历); 领跑核在日环边干等时领养落后核的资产并回填
// (wait_open 的 on_idle, 处置权转移见 TsSchedule).
// 触发时机 = 领跑者在日环边干等 slot (它已领先, 等待时间白白浪费, 正好拿来帮最慢者);
// 额度 = 两侧配额 (sched.quota, 协调线程按实测忙碌占比每窗重算, 见 TsTuner): victim 让出 / leader 接手都不超过各自余额.
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
  // 本核发布槽 (单写者): work = 累计源条数, idle_us = 累计干等微秒 (edge_us = 其中日环边干等);
  // 前沿/持仓在 store/sched 里, 渲染线程 / 协调线程自取
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
  // 续算: 构造后读回续算点 (尾窗末日收盘态), 再把尾窗日句柄接回 (已结清的尾窗日即刻 ts_close)
  {
    TraceN("InitCores");
    // asset_id = 本库 A 轴下标 (张量列); fund_panel 的行 = 其全局 id (Core 内经 members.axis() 映射)
    for (const size_t asset_id : my_asset_ids) {
      cores[asset_id] = std::make_unique<CoreOf<T>>(role.fund, role.members, asset_id);
      if (role.snap_in) {
        misc::snap::Load in(role.snap_in->record(asset_id));
        cores[asset_id]->snap(in);
        assert(in.done() && "续算点记录长于核的 snap 清单");
        cores[asset_id]->resume(role.tail, store);
      }
    }
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
  // 抽中的 asset-day 挂本 worker 的节点钟 (只在本 asset-day 内; 领养回填同一路径)
  graph::NodeClock &clock = role.clocks[static_cast<size_t>(worker_id)];
  auto process_asset_day = [&](size_t asset_id, size_t date_idx, const Day &day) -> size_t {
    ++date_assets_processed;
    CoreOf<T> &core = *cores[asset_id];
    const bool timed = node_clock_pick(asset_id, date_idx, role.clock_every);
    uint64_t t0 = 0;
    if (timed) {
      graph::node_clock = &clock;
      ++clock.samples;
      t0 = __builtin_ia32_rdtsc();
    }
    core.begin_day(ctx.dates[date_idx], day);
    const size_t work = runner.feed(core, asset_id, date_idx);
    core.end_day(store);
    runner.release();
    if (timed) {
      clock.total += __builtin_ia32_rdtsc() - t0;
      graph::node_clock = nullptr;
    }
    return work;
  };

  // 负载再平衡: 只在日环边干等 slot 时调用 (本 worker 已领先满日环深, 等待时间正好拿来
  // 帮落后者). 本核可接手 (配额 < 0) 才领: 接手 victim 放得进双方余额的最重资产 (一次修正得多、换核少;
  // 放不进 = 偏差小于一个资产, 不动), 立刻回填落下的旧日期 —— 含 victim
  // 的"当天" (claim CAS 唯一裁决: victim 日循环还没轮到它就被本 worker 先
  // claim 走, victim 见 owner 已变直接跳过, 当天即刻变轻). 旧日未计满 ⇒
  // 仍在飞, ts_try_open 必得; 硬源的旧日源格同理未复用 (见 StoreRing 源窗); 回填后并入本 worker 日循环 (永久转移, 不归还).
  // 触发条件 (日环边干等 + 落后 ≥ 2 天) 与永久转移都是刻意的: 不要改成自由领 / 按日窃取 (理由见 TsSchedule).
  std::atomic<int64_t> &my_quota = sched.quota[static_cast<size_t>(worker_id)];
  auto try_adopt = [&](size_t my_days_done) {
    const int64_t room = -my_quota.load(std::memory_order_relaxed);
    if (room <= 0)
      return;

    // 候选 victim: 落后 ≥ 2 天 (1 天差是相位噪声) 且该让出者, 按前沿升序逐个试
    // —— 最慢者余额耗尽/撞车时顺延帮第二慢者, 而不是空转干等.
    // f ≥ 1 兼保证 victim 的 core 构造已通过 frontier 的 release/acquire 发布.
    std::vector<std::pair<size_t, int>> laggers; // (前沿, worker)
    for (int w = 0; w < static_cast<int>(sched.frontier.size()); ++w) {
      const size_t f = sched.frontier[static_cast<size_t>(w)].load(std::memory_order_acquire);
      if (w != worker_id && f >= 1 && my_days_done >= f + 2 && sched.quota[static_cast<size_t>(w)].load(std::memory_order_relaxed) > 0)
        laggers.emplace_back(f, w);
    }
    std::sort(laggers.begin(), laggers.end());

    for (const auto &[victim_days, victim] : laggers) {
      std::atomic<int64_t> &owe = sched.quota[static_cast<size_t>(victim)];
      const int64_t budget = std::min(owe.load(std::memory_order_relaxed), room);
      // victim 手里放得进 budget 的最重资产 (顺便数持仓; 至少给它留 2 个; 零负载的不搬)
      size_t victim_count = 0, pick = SIZE_MAX;
      int64_t pick_weight = 0;
      for (size_t i = 0; i < sched.owner.size(); ++i) {
        if (sched.owner[i].load(std::memory_order_relaxed) != victim)
          continue;
        ++victim_count;
        if (sched.weight[i] <= budget && sched.weight[i] > pick_weight) {
          pick_weight = sched.weight[i];
          pick = i;
        }
      }
      if (victim_count < 2 || pick == SIZE_MAX)
        continue;

      // victim 侧扣额 CAS: 余额不够 = 别的领养方先扣了 (或协调线程刚重算), 试下一个 victim
      int64_t q = owe.load(std::memory_order_relaxed);
      bool budget_ok = false;
      while (q >= pick_weight && !budget_ok)
        budget_ok = owe.compare_exchange_weak(q, q - pick_weight, std::memory_order_relaxed);
      if (!budget_ok)
        continue;

      // 处置权 CAS: 路标改指本 worker, victim 下一天起直接跳过 pick.
      // 失败 = 与其他领养方撞车 (退回扣额, 试下一个 victim).
      int32_t expect = victim;
      if (!sched.owner[pick].compare_exchange_strong(expect, worker_id, std::memory_order_acq_rel)) {
        owe.fetch_add(pick_weight, std::memory_order_relaxed);
        continue;
      }
      my_quota.fetch_add(pick_weight, std::memory_order_relaxed);
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

  for (size_t date_idx = ctx.tail; date_idx < total_dates; ++date_idx) {
    if (cancel_requested.load(std::memory_order_relaxed))
      break;

    TraceN("DateLoop");
    const misc::Ymd date = ctx.dates[date_idx];
    TraceTextS(misc::dense(date).c_str());
    date_work = 0;
    date_assets_processed = 0;

    // 本日写句柄: 每 worker 每日 open 一次, 之后所有写回是纯指针算术.
    // 日环满 (或硬源未就绪) = 本 worker 领先到把日环吃满 —— 干等的时间拿去领养最慢者的资产
    // 并回填 (回填日仍在飞, 不占新 slot), 等 IO 交还 slot 后再继续本日.
    // 只有 sleep 算干等 (领养回填是干活, 见 wait_open); 源已就绪的那部分是日环边干等 (扩格的观测面).
    Day day;
    {
      TraceN("PoolWait");
      TraceColor(C_Orange);
      day = wait_open(
          cancel_requested, stat, [&] { return feed_ready(date_idx) ? store.ts_try_open(date_idx) : Day{}; },
          [&] {
            try_adopt(date_idx); // date_idx = 本 worker 已完成日数
            return feed_ready(date_idx);
          });
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
      if (date_idx + 1 == total_dates) {
        // 续算点 = 末日收盘态 (先于 finish_all: 区间末的估值结清是本轮的收尾, 不是跨天态)
        misc::snap::Save out(role.snap_out[asset_id]);
        cores[asset_id]->snap(out);
        cores[asset_id]->finish_all(store); // 区间末: 悬挂的开盘型按最近收盘价结算, 旧日句柄全部归还
      }
      date_work += work;
      cumulative_work += work;
      sched.done[asset_id].store(static_cast<int32_t>(date_idx), std::memory_order_release);
      ++i;
      stat.work.store(cumulative_work, std::memory_order_relaxed);
    }

    if (date_assets_processed > 0) {
      Logger::log("worker_" + std::to_string(worker_id), std::to_string(date) + " completed: " + std::to_string(date_assets_processed) + " assets, " + std::to_string(date_work) + " " + TierFeed<T>::kUnit + runner.day_log());
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

// 管线: 档的源 → 初始分配 → 日环 (按初始分配模拟定深) → 四角色按 StageLayout pin 死启动 (统一 launch) → 等收尾 (协调线程调参).
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
  const size_t tail = args.tail;
  assert(!dates.empty() && num_assets >= 1);
  assert(tail < dates.size() && tail <= CoreOf<T>::Label::PEND_DAYS && "续算: 尾窗之后须有新日, 且不超过悬挂日环");
  assert(args.members.axis().hash == lib.axis.hash && std::ranges::equal(args.members.dates(), dates) && "池面板须是本库 A 轴 × 管线日期轴");

  TierFeed<T> feed(args);

  // 初始分配: A 轴切 num_ts 个连续段, 各段权重和均衡 (资产归其权重中点所在的那一份; 权重 = 待算日的负载和).
  // 张量 [T][F][A] 的一条缓存行 = 相邻 32 个资产的同一 (t, f): 连续段同核写, 核间只在段界共一行;
  // 交错分配则每次写都跨核 (跨插槽) 抢行, TS 退化为缓存一致性流量.
  // 运行期差距由 worker 间的处置权转移收敛 (见 ts_loop 的 try_adopt), 这里只求起点均衡.
  TsSchedule sched(num_assets, num_ts, tail);
  std::vector<std::vector<double>> load(num_ts, std::vector<double>(dates.size() - tail, 0.0)); // [核][待算日] 负载 (初始日环模拟)
  {
    int64_t total = 0;
    for (size_t sub = 0; sub < num_assets; ++sub) {
      for (size_t d = tail; d < dates.size(); ++d)
        sched.weight[sub] += static_cast<int64_t>(feed.weight(sub, d));
      total += sched.weight[sub];
    }
    assert(total > 0 && "初始分配: 全轴权重为 0");
    const int64_t n = static_cast<int64_t>(num_ts);
    int64_t acc = 0;
    for (size_t sub = 0; sub < num_assets; ++sub) {
      const size_t w = static_cast<size_t>(std::min(n - 1, (2 * acc + sched.weight[sub]) * n / (2 * total)));
      sched.owner[sub].store(static_cast<int32_t>(w), std::memory_order_relaxed);
      acc += sched.weight[sub];
      for (size_t d = tail; d < dates.size(); ++d)
        load[w][d - tail] += static_cast<double>(feed.weight(sub, d));
    }
  }

  // 日环 (口径见 TsTuner.hpp): 下游段 = MIN_RING_SLOTS (悬挂 + 时序下一日 + 截面跨日, 少于此死锁)
  //   + 截面在飞 ≤ K_cs + 落盘在飞 ≤ K_io + 1 (截面 / 落盘单日耗时波动与落盘前缀乱序); 再加时序余量 (模拟), 按内存截断.
  // 格数上限 = 总内存口径 ∧ 日期轴天数 (窗口比整轴还长无益)
  const RingBudget budget{FeatureStore<T>::Slot::bytes(num_assets) + TierFeed<T>::cell_bytes(num_assets)};
  const size_t floor_slots = CoreOf<T>::kMinRingSlots + num_cs + num_io + 1;
  const size_t mem_cap = budget.cap();
  const size_t lead = ts_lead_days(load, mem_cap > floor_slots ? mem_cap - floor_slots : 0);
  const size_t slots = std::min(floor_slots + lead, budget.affordable());
  const size_t cap = std::max(slots, std::min(mem_cap, dates.size()));
  assert(slots >= CoreOf<T>::kMinRingSlots && slots > tail && "内存不够: 日环不到 MIN_RING_SLOTS, 会死锁");

  std::cout << "\n=== Compute (" << tier_name(T::kId) << ") ===\n"
            << L.summary() << "\n"
            << "Assets: " << num_assets
            << " | Ring 初值: " << slots << " (下游 " << floor_slots << " + 时序余量 " << lead << ", 上限 " << cap << ")"
            << " | Dates: " << dates.size() << " (" << dates.front() << " - " << dates.back() << ")"
            << " | Tail: " << tail << "\n"
            << std::endl;

  FeatureStore<T> store(num_assets, lib.axis.hash, lib.id, slots, cap);
  feed.bind(store);

  // 续算: 尾窗日张量读回日环 + 续算点整读 (头须对上尾窗末日 / 本库轴 / 状态指纹), 尾窗日句柄给 TS 接回
  std::optional<FeatureSnap> snap_in;
  std::vector<typename FeatureStore<T>::Day> tail_days;
  if (tail > 0) {
    store.seed(std::span(dates).first(tail));
    snap_in.emplace(disk::path(disk::LibSnapshot, lib_key(lib.id)).string(), snap_header<T>(lib, dates[tail - 1]));
    for (size_t d = 0; d < tail; ++d)
      tail_days.push_back(store.ts_try_open(d));
  }
  std::vector<std::vector<std::byte>> snap_out(num_assets);

  std::vector<std::unique_ptr<CoreOf<T>>> cores(num_assets);
  // 节点用时: 本轮约 kClockTarget 个 asset-day 计时 (小轮次全计)
  constexpr size_t kClockTarget = 4096;
  const size_t clock_every = std::max<size_t>(1, num_assets * (dates.size() - tail) / kClockTarget);
  std::vector<graph::NodeClock> clocks(num_ts);
  std::vector<CsClock> cs_clocks(num_cs);
  TsRole<T> role{feed, args.fund, args.members, sched, cores, snap_in ? &*snap_in : nullptr, tail_days, snap_out, clock_every, clocks};

  // 统一 launch(worker_id, name, cpus, fn): 线程名 name (StageLayout::thread_name), pin 到 cpus (StageLayout::cpus_of), worker_id 与 CPU 号无关.
  // 进度 = 仪表盘自行采样 store/sched/stats (拉取式); 采样窗 (PROFILE 模式) / 时序调参同样拉取式, 见 ComputeProfile / TsTuner.
  ComputeStats stats(num_pf, num_ts, num_cs, num_io);
  ComputeDashboard dashboard(store, sched, stats, dates, TierFeed<T>::kUnit);
  ComputeProfile profile({pool_def(lib.pool()).name, T::kId, TierFeed<T>::kUnit, num_assets, dates}, L, store, stats);
  TsTuner tuner(store, sched, stats, dates.size(), budget, [&store] { store.grow(); });
  auto ctx = [&](int worker_id, ComputeStats::Core &stat) {
    return StageCtx<T>{worker_id, store, args.libs, dates, tail, args.cancel, stat};
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
        launch(id, std::move(name), L.cpus_of(s, i), [&, lane = static_cast<size_t>(i)](int w) { cs_loop<T>(ctx(w, stats.cs[lane]), lane, num_cs, cs_clocks[lane]); });
        break;
      case ComputeStage::IO:
        launch(id, std::move(name), L.cpus_of(s, i), [&, lane = static_cast<size_t>(i)](int w) { io_loop<T>(ctx(w, stats.io[lane]), lane, num_io); });
        break;
      }
    }

  // 等收尾: 已落盘天数镜像进进度面, 采样窗 / 时序调参随之推进 (worker 自己查 cancel, 这里报一声并就地收采样窗出报告)
  bool announced = false;
  for (auto &worker : workers)
    while (worker.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready) {
      args.on_io(store.query_io_done());
      profile.tick();
      tuner.tick();
      if (!announced && args.cancel.load(std::memory_order_relaxed)) {
        announced = true;
        std::cout << "[Compute] Cancelling..." << std::endl;
        profile.finish();
      }
    }
  args.on_io(store.query_io_done());
  profile.finish();
  // 仪表盘引用 store/sched/dates, 必须先于它们的析构停掉
  dashboard.stop();

  const bool completed = !args.cancel.load(std::memory_order_relaxed);
  if (completed)
    FeatureSnap::write(disk::path(disk::LibSnapshot, lib_key(lib.id)).string(), snap_header<T>(lib, dates.back()), snap_out);
  graph::NodeClock ts_sum;
  for (const graph::NodeClock &c : clocks) {
    ts_sum.samples += c.samples;
    ts_sum.label += c.label;
    ts_sum.write += c.write;
    ts_sum.total += c.total;
    for (size_t i = 0; i < graph::NodeClock::kMaxNodes; ++i)
      ts_sum.cycles[i] += c.cycles[i];
  }
  CsClock cs_sum;
  for (const CsClock &c : cs_clocks) {
    cs_sum.total += c.total;
    cs_sum.days += c.days;
    for (size_t l = 0; l < LEVEL_COUNT; ++l) {
      cs_sum.cols[l].resize(std::max(cs_sum.cols[l].size(), c.cols[l].size()), 0);
      for (size_t i = 0; i < c.cols[l].size(); ++i)
        cs_sum.cols[l][i] += c.cols[l][i];
    }
  }
  if (ts_sum.samples > 0 || cs_sum.days > 0)
    write_node_time<T>(lib.id, ts_sum, cs_sum);
  std::cout << "\n=== Feature Computation (" << tier_name(T::kId) << ") " << (completed ? "Complete" : "Cancelled") << " ===\n"
            << "Processed: " << store.query_io_done() << "/" << dates.size() << " dates\n"
            << std::endl;
  return completed;
}

template <class T>
constexpr TierOps tier_ops_of() {
  return {TierFeed<T>::kUnit, CoreOf<T>::Label::PEND_DAYS, &TierFeed<T>::source_dates, &snap_header<T>, &pipeline<T>};
}
