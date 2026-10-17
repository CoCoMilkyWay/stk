#include "worker/feature_workers.hpp"

#include "features/Backend/FeatureDay.hpp"
#include "features/Backend/FeatureStore.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>
#include <thread>

void io_worker(WorkerCtx ctx) {
  const int worker_id = ctx.worker_id;
  GlobalFeatureStore &store = ctx.store;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  // 本核发布槽 (单写者): work = 已落盘字节, idle_ms = 累计等 CS_DONE 毫秒; 天数另计 io_days
  ComputeStats::Core &stat = ctx.stats.io;
  size_t cumulative_bytes = 0;

  TraceNS("IOWorker", 5);
  TraceValue(worker_id);
  TraceThread(("io_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = ctx.dates.size();
  size_t flush_count = 0;

  Logger::log("worker_" + std::to_string(worker_id), "Started: " + std::to_string(total_dates) + " dates to flush");

  FeatureDay::Scratch scratch; // 跨日复用, 稳态零分配
  size_t wait_count = 0;
  bool cancelled = false;
  while (flush_count < total_dates) {
    TraceN("FlushLoop");
    TraceValue(flush_count);

    // 摘一个 CS 完成的日子, 按层序落盘 (最后一层 = 本日 commit 标记), 归还 slot
    const GlobalFeatureStore::Day day = store.io_try_open();
    if (day) {
      size_t flushed_bytes = 0;
      {
        TraceN("FlushDay");
        TraceColor(C_Red);
        const std::string day_dir = feature_day_dir(store.query_dir(), day.slot->date);
        std::filesystem::create_directories(day_dir);
        for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
          flushed_bytes += FeatureDay::write(feature_file(day_dir, lvl), lvl, day.slot->data[lvl], day.A, store.query_axis_hash(), scratch);
        store.io_close(day);
      }

      flush_count++;
      wait_count = 0;
      cumulative_bytes += flushed_bytes;
      stat.work.store(cumulative_bytes, std::memory_order_relaxed);
      ctx.stats.io_days.store(flush_count, std::memory_order_relaxed);
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
