#include "worker/feature_workers.hpp"

#include "features/Storage/FeatureStore.hpp"
#include "features/CoreCs.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <cstdio>

// 本核发布槽 (单写者): work = 累计已扫拍格, idle_ms = 累计干等毫秒; 天数前沿 = store.query_cs_done(), 渲染线程自取
template <class T>
void cs_loop(const StageCtx<T> &ctx) {
  const int worker_id = ctx.worker_id;
  FeatureStore<T> &store = ctx.store;
  const std::span<const misc::Ymd> dates = ctx.dates;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  ComputeStats::Core &stat = ctx.stat;
  TraceNS("CSWorker", 5);
  TraceValue(worker_id);
  TraceThread(("cs_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = dates.size();
  size_t completed_dates = 0;
  size_t timeslots_done = 0;

  Logger::log("worker_" + std::to_string(worker_id), "Started: " + std::to_string(total_dates) + " dates to process");

  // Initialize CoreCs (manages buffers and 3-level computation)
  using Core = CoreCs<T>;
  Core core(store, ctx.lib.axis, ctx.side);

  // Date-first traversal
  for (size_t date_idx = 0; date_idx < total_dates; ++date_idx) {
    if (cancel_requested.load(std::memory_order_relaxed))
      break;
    TraceN("DateLoop");
    const misc::Ymd date = dates[date_idx];
    TraceTextS(misc::dense(date).c_str());

    // 按日流控: 等全部 TS 写完本日, 返回后两层张量整体可读.
    // 数值一致性由输入契约锚定 (CS 只读秒网格张量行, 见 CoreCs.hpp),
    // 与"CS 流式伴随还是整日后扫"的调度形态无关, 结果逐值相同.
    // Cancel 即退 (循环顶): 取消后的截面是弃子, 多算是浪费, 更不能拿退出条件去赌 TS 侧计数的时序.
    typename FeatureStore<T>::Day day;
    {
      TraceN("WaitSync");
      TraceColor(C_Orange);
      day = wait_open(cancel_requested, stat, [&] { return store.cs_try_open(date_idx); });
    }
    if (!day)
      break;

    // 有效拍 (Lob = 秒 [0, 15300), Bar = 分钟 [0, 255)): 末行是哨兵 (label lookahead 落点), 不进截面
    core.begin_day(day, date);
    const size_t beats = level_valid_rows(Core::kBeatLevel);
    for (size_t t = 0; t < beats; ++t) {
      TraceN("TimeslotLoop");
      TraceValue(t);
      core.compute_and_store(t);
      stat.work.store(++timeslots_done, std::memory_order_relaxed); // 秒格计数 (带宽标定用)
    }
    core.end_day();

    ++completed_dates;

    // 本日截面完成, slot 交给 IO 落盘
    store.cs_close(day);

    Logger::log("worker_" + std::to_string(worker_id), std::to_string(date) + " completed: " + std::to_string(beats) + " timeslots");

    TraceFrame;
  }

  const bool cancelled = cancel_requested.load(std::memory_order_relaxed);

  if (cancelled)
    Logger::log("worker_" + std::to_string(worker_id), "Cancelled after " + std::to_string(completed_dates) + " dates");
  else
    Logger::log("worker_" + std::to_string(worker_id), "Completed: " + std::to_string(total_dates) + " dates processed");
}

#define CS_LOOP_INSTANTIATE(T, ns) template void cs_loop<ns::Tier>(const StageCtx<ns::Tier> &);
ALL_TIERS(CS_LOOP_INSTANTIATE)
#undef CS_LOOP_INSTANTIATE
