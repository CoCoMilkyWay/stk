#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include "features/Backend/FeatureStore.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <string>

void prefetch_worker(WorkerCtx ctx) {
  const int worker_id = ctx.worker_id;
  SharedData &data = ctx.data;
  PoolFeatureStore &store = ctx.store;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  // 本核发布槽 (单写者): work = 累计下发预读字节, idle_ms = 累计门控干等毫秒
  ComputeStats::Core &stat = ctx.stats.prefetch;

  TraceNS("PrefetchWorker", 5);
  TraceValue(worker_id);
  TraceThread(("prefetch_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = ctx.dates.size();

  Logger::log("worker_" + std::to_string(worker_id), "Prefetch started: " + std::to_string(total_dates) + " dates");

  size_t cumulative_bytes = 0;
  size_t completed_dates = 0;

  for (size_t date_idx = 0; date_idx < total_dates; ++date_idx) {
    if (cancel_requested.load(std::memory_order_relaxed))
      break;

    TraceN("DateLoop");
    const misc::Ymd date = ctx.dates[date_idx];
    TraceTextS(misc::dense(date).c_str());

    // 门控: 领先最慢 TS 不超过窗口 (见 PoolFeatureStore::pf_try_open)
    bool open;
    {
      TraceN("GateWait");
      TraceColor(C_Orange);
      open = wait_open(ctx, stat, [&] { return store.pf_try_open(date_idx); });
    }
    if (!open)
      break;

    {
      TraceN("ReadFiles");
      // 资产集 = universe 子轴 (帧目录切片的一行): TS 只 decode 这些. reader 按偏移
      // 升序交给内核预读 (空格子跳过), 字节不进用户态 (decode 在 TS worker 内联).
      const std::span<const FrameRef> refs = ctx.orders.row(date_idx);
      if (std::any_of(refs.begin(), refs.end(), [](const FrameRef &ref) { return !ref.empty(); })) {
        cumulative_bytes += DayReader(data.config.orders_dir, date).prefetch(refs);
        stat.work.store(cumulative_bytes, std::memory_order_relaxed);
      }
    }

    store.pf_close(date_idx);
    completed_dates = date_idx + 1;

    TraceFrame;
  }

  const bool cancelled = cancel_requested.load(std::memory_order_relaxed);

  if (cancelled)
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch cancelled: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(completed_dates) + " dates");
  else
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch completed: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(total_dates) + " dates");
}
