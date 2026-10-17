#include "worker/feature_workers.hpp"

#include "features/Storage/FeatureDay.hpp"
#include "features/Storage/FeatureStore.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <cstdio>
#include <filesystem>
#include <string>

// 本核发布槽 (单写者): work = 已落盘字节, idle_us = 累计等截面 close 微秒; 天数前沿 = store.query_io_done()
template <class T>
void io_loop(const StageCtx<T> &ctx) {
  const int worker_id = ctx.worker_id;
  FeatureStore<T> &store = ctx.store;
  const std::span<const misc::Ymd> dates = ctx.dates;
  const std::atomic<bool> &cancel = ctx.cancel;
  ComputeStats::Core &stat = ctx.stat;
  size_t cumulative_bytes = 0;

  TraceNS("IOWorker", 5);
  TraceValue(worker_id);
  TraceThread(("io_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = dates.size();
  size_t flush_count = 0;

  Logger::log("worker_" + std::to_string(worker_id), "Started: " + std::to_string(total_dates) + " dates to flush");

  FeatureDay::Scratch scratch; // 跨日复用, 稳态零分配
  // 循环顶不查 cancel (与其余三角色唯一的不同): 截面已 close 的日子刷完再走 ——
  // wait_open 先试后查 cancel, 刷到首个未截面的日才返回空. 不能和 ts_done 比:
  // CS 按 cancel 即退, 而某个 TS 可能之后才把在飞的一天计满, 等它就是死等.
  for (size_t date_idx = 0; date_idx < total_dates; ++date_idx) {
    TraceN("FlushLoop");
    TraceValue(date_idx);

    typename FeatureStore<T>::Day day;
    {
      TraceN("WaitForData");
      TraceColor(C_Orange);
      day = wait_open(cancel, stat, [&] { return store.io_try_open(date_idx); });
    }
    if (!day)
      break;

    // 按层序落盘 (最后一层 = 本日 commit 标记), 清零交还 slot
    size_t flushed_bytes = 0;
    {
      TraceN("FlushDay");
      TraceColor(C_Red);
      const std::string day_dir = feature_day_dir(store.query_dir(), dates[date_idx]);
      std::filesystem::create_directories(day_dir);
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
        if (T::has(lvl))
          flushed_bytes += FeatureDay::write(feature_file(day_dir, lvl), T::LEVELS[lvl], day.slot->data[lvl], day.A, store.query_axis_hash(), scratch);
      store.io_close(day);
    }

    flush_count = date_idx + 1;
    cumulative_bytes += flushed_bytes;
    stat.work.store(cumulative_bytes, std::memory_order_relaxed);
    Logger::log("worker_" + std::to_string(worker_id), "Flushed: " + std::to_string(flush_count) + "/" + std::to_string(total_dates));

    TraceFrame;
  }

  if (flush_count < total_dates)
    Logger::log("worker_" + std::to_string(worker_id), "Cancelled after " + std::to_string(flush_count) + " dates");
  else
    Logger::log("worker_" + std::to_string(worker_id), "Completed: " + std::to_string(total_dates) + " dates flushed");
}

#define IO_LOOP_INSTANTIATE(T, ns) template void io_loop<ns::Tier>(const StageCtx<ns::Tier> &);
ALL_TIERS(IO_LOOP_INSTANTIATE)
#undef IO_LOOP_INSTANTIATE
