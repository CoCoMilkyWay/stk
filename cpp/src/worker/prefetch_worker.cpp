#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include "features/Backend/FeatureStore.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <string>
#include <thread>

void prefetch_worker(WorkerCtx ctx) {
  const int worker_id = ctx.worker_id;
  SharedData &data = ctx.data;
  GlobalFeatureStore &store = ctx.store;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  // 本核发布槽 (单写者): work = 累计读入字节, idle_ms = 累计门控干等毫秒
  ComputeStats::Core &stat = ctx.stats.prefetch;

  TraceNS("PrefetchWorker", 5);
  TraceValue(worker_id);
  TraceThread(("prefetch_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = ctx.dates.size();

  // 窗口 = TS 在飞跨度上限 (pool slots) + 超前余量: 保证最快 TS 到达前缓存已暖
  const size_t window_days = store.query_slots() + 2;

  Logger::log("worker_" + std::to_string(worker_id), "Prefetch started: " + std::to_string(total_dates) + " dates, window " + std::to_string(window_days));

  size_t cumulative_bytes = 0;
  size_t completed_dates = 0;

  for (size_t date_idx = 0; date_idx < total_dates; ++date_idx) {
    if (cancel_requested.load(std::memory_order_relaxed))
      break;

    TraceN("DateLoop");
    const misc::Ymd date = ctx.dates[date_idx];
    TraceTextS(misc::dense(date).c_str());

    // 门控: 领先最慢 TS 不超过窗口
    {
      TraceN("GateWait");
      TraceColor(C_Orange);
      while (date_idx > store.query_ts_days_done() + window_days) {
        if (cancel_requested.load(std::memory_order_relaxed))
          break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        stat.idle_ms.fetch_add(10, std::memory_order_relaxed);
      }
      if (cancel_requested.load(std::memory_order_relaxed))
        break;
    }

    {
      TraceN("ReadFiles");
      // 资产集 = universe 子轴 (帧目录切片的一行): TS 只 decode 这些. reader 按偏移
      // 排序、首尾相接的帧并成一段顺序读 (空格子跳过) —— universe 覆盖全市场时就是
      // 整个数据区一段段读完.
      // 只为把帧拖进 page cache, 内容即弃 (decode 在 TS worker 内联).
      const std::span<const FrameRef> refs = ctx.orders.row(date_idx);
      if (std::any_of(refs.begin(), refs.end(), [](const FrameRef &ref) { return !ref.empty(); })) {
        cumulative_bytes += DayReader(data.config.orders_dir, date).prefetch(refs);
        stat.work.store(cumulative_bytes, std::memory_order_relaxed);
      }
    }

    completed_dates = date_idx + 1;
    ctx.stats.prefetch_days.store(completed_dates, std::memory_order_relaxed);

    TraceFrame;
  }

  const bool cancelled = cancel_requested.load(std::memory_order_relaxed);

  if (cancelled)
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch cancelled: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(completed_dates) + " dates");
  else
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch completed: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(total_dates) + " dates");
}
