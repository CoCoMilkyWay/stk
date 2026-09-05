#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include "features/Backend/FeatureStore.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

void io_worker(WorkerCtx ctx) {
  const int worker_id = ctx.worker_id;
  SharedData &data = ctx.data;
  GlobalFeatureStore &store = ctx.store;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  // 本核发布槽 (单写者): work = 已落盘天数, idle_ms = 累计等 CS_DONE 毫秒
  ComputeStats::Core &stat = ctx.stats.io;

  TraceNS("IOWorker", 5);
  TraceValue(worker_id);
  TraceThread(("io_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = data.asset.all_dates.size();
  size_t flush_count = 0;

  Logger::log("worker_" + std::to_string(worker_id), "Started: " + std::to_string(total_dates) + " dates to flush");

  size_t wait_count = 0;
  bool cancelled = false;
  while (flush_count < total_dates) {
    TraceN("FlushLoop");
    TraceValue(flush_count);

    // Flush oldest CS_DONE tensor (one at a time, maintains date order)
    bool flushed = false;
    {
      TraceN("TryFlush");
      TraceColor(C_Red);
      flushed = store.io_try_flush();
    }

    if (flushed) {
      flush_count++;
      wait_count = 0;
      stat.work.store(flush_count, std::memory_order_relaxed);
      Logger::log("worker_" + std::to_string(worker_id), "Flushed: " + std::to_string(flush_count) + "/" + std::to_string(total_dates));

      TraceFrame;
    } else {
      TraceN("WaitForData");
      TraceColor(C_Orange);
      // Cancel 退出: CS 关掉的日子都已刷完即走. 不能再和 ts_days_done 比 ——
      // CS 按 cancel 即退, 而某个 TS 可能之后才把在飞的一天计满, ts_days_done
      // 反超定格的 cs_days_done, 等相等就是死等 (cancel 卡死的根因).
      if (cancel_requested.load(std::memory_order_relaxed) &&
          flush_count >= store.query_cs_days_done()) {
        Logger::log("worker_" + std::to_string(worker_id), "Cancelled after " + std::to_string(flush_count) + " dates");
        cancelled = true;
        break;
      }
      // No tensors ready yet, sleep briefly
      wait_count++;
      if (wait_count % 100 == 0) {
        Logger::log("worker_" + std::to_string(worker_id), "Waiting for tensors (" + std::to_string(wait_count * 10) + "ms)");
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      stat.idle_ms.fetch_add(10, std::memory_order_relaxed);
    }
  }

  if (!cancelled)
    Logger::log("worker_" + std::to_string(worker_id), "Completed: " + std::to_string(total_dates) + " dates flushed");
}
