#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include "features/Backend/FeatureStore.hpp"
#include "features/CoreCrosssection.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <chrono>
#include <cstdio>
#include <thread>

void crosssectional_worker(WorkerCtx ctx) {
  const int worker_id = ctx.worker_id;
  SharedData &data = ctx.data;
  GlobalFeatureStore &store = ctx.store;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  // 本核发布槽 (单写者): work = 累计已扫秒格, idle_ms = 累计干等毫秒;
  // 天数前沿 = store.query_cs_days_done(), 渲染线程自取
  ComputeStats::Core &stat = ctx.stats.cs;

  TraceNS("CSWorker", 5);
  TraceValue(worker_id);
  TraceThread(("cs_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = data.asset.all_dates.size();
  size_t completed_dates = 0;
  size_t timeslots_done = 0;

  Logger::log("worker_" + std::to_string(worker_id), "Started: " + std::to_string(total_dates) + " dates to process");

  // Initialize CoreCrosssection (manages buffers and 3-level computation)
  CoreCrosssection core(store);

  // Date-first traversal
  for (size_t date_idx = 0; date_idx < data.asset.all_dates.size(); ++date_idx) {
    if (cancel_requested.load(std::memory_order_relaxed))
      break;
    TraceN("DateLoop");
    const std::string &date_str = data.asset.all_dates[date_idx];
    TraceTextS(date_str.c_str());

    // 按日门控: 阻塞至全部 TS 写完本日, 返回后三层张量整体可读.
    // 数值一致性由输入契约锚定 (CS 只读秒网格张量行, 见 CoreCrosssection.hpp),
    // 与"CS 流式伴随还是整日后扫"的调度形态无关, 结果逐值相同.
    GlobalFeatureStore::Day day;
    {
      TraceN("WaitSync");
      TraceColor(C_Orange);
      // Cancel 即退 (不等 ts_days_done 追平): 取消后的截面是弃子, 多算是
      // 浪费, 更不能拿退出条件去赌 TS 侧计数的时序.
      while (!(day = store.cs_try_open(date_str))) {
        if (cancel_requested.load(std::memory_order_relaxed)) {
          Logger::log("worker_" + std::to_string(worker_id), "Cancelled after " + std::to_string(completed_dates) + " dates");
          return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        stat.idle_ms.fetch_add(1, std::memory_order_relaxed);
      }
    }
    core.set_day(day);

    // 有效秒 [0, 15300): 末行是哨兵 (label lookahead 落点), 不进截面
    const size_t T = level_valid_rows(0);
    for (size_t t = 0; t < T; ++t) {
      TraceN("TimeslotLoop");
      TraceValue(t);
      core.compute_and_store(t);
      stat.work.store(++timeslots_done, std::memory_order_relaxed); // 秒格计数 (带宽标定用)
    }

    ++completed_dates;

    // 本日截面完成, slot 交给 IO 落盘
    store.cs_close(day);

    Logger::log("worker_" + std::to_string(worker_id), date_str + " completed: " + std::to_string(T) + " timeslots");

    TraceFrame;
  }

  const bool cancelled = cancel_requested.load(std::memory_order_relaxed);

  if (cancelled)
    Logger::log("worker_" + std::to_string(worker_id), "Cancelled after " + std::to_string(completed_dates) + " dates");
  else
    Logger::log("worker_" + std::to_string(worker_id), "Completed: " + std::to_string(total_dates) + " dates processed");
}
