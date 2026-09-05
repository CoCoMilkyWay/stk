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
  const misc::ProgressHandle progress_handle = std::move(ctx.progress);

  TraceNS("IOWorker", 5);
  TraceValue(worker_id);
  TraceThread(("io_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = data.asset.all_dates.size();
  size_t flush_count = 0;

  // Update initial label
  char label_buf[128];
  snprintf(label_buf, sizeof(label_buf), "落盘核心%2d:", worker_id);
  progress_handle.set_label(label_buf);
  progress_handle.update(0, total_dates, "等待数据");

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
      Logger::log("worker_" + std::to_string(worker_id), "Flushed: " + std::to_string(flush_count) + "/" + std::to_string(total_dates));

      progress_handle.update(flush_count, total_dates, "");
      TraceFrame;
    } else {
      TraceN("WaitForData");
      TraceColor(C_Orange);
      if (cancel_requested.load(std::memory_order_relaxed) &&
          flush_count >= store.query_cs_days_done() &&
          store.query_cs_days_done() >= store.query_ts_days_done()) {
        progress_handle.update(flush_count, total_dates, "Cancelled");
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
    }
  }

  progress_handle.update(cancelled ? flush_count : total_dates, total_dates, cancelled ? "Cancelled" : "Complete");

  if (!cancelled)
    Logger::log("worker_" + std::to_string(worker_id), "Completed: " + std::to_string(total_dates) + " dates flushed");
}
