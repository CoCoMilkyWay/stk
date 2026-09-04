#include "worker/crosssectional_worker.hpp"
#include "shared/SharedData.hpp"

#include "features/Backend/FeatureStore.hpp"
#include "features/CoreCrosssection.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <cstdio>

void crosssectional_worker(int worker_id,
                           SharedData &data,
                           GlobalFeatureStore &store,
                           misc::ProgressHandle progress_handle) {
  TraceNS("CSWorker", 5);
  TraceValue(worker_id);
  TraceThread(("cs_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = data.asset.all_dates.size();
  size_t completed_dates = 0;

  Logger::log("worker_" + std::to_string(worker_id), "Started: " + std::to_string(total_dates) + " dates to process");

  // Initialize CoreCrosssection (manages buffers and 3-level computation)
  CoreCrosssection core(store);

  // Date-first traversal
  for (size_t date_idx = 0; date_idx < data.asset.all_dates.size(); ++date_idx) {
    TraceN("DateLoop");
    const std::string &date_str = data.asset.all_dates[date_idx];
    TraceTextS(date_str.c_str());

    // Update progress label
    char label_buf[128];
    snprintf(label_buf, sizeof(label_buf), "截面核心%2d: %3zu/%3zu 日期: %s",
             worker_id, date_idx + 1, total_dates, date_str.c_str());
    progress_handle.set_label(label_buf);

    // 按日门控: 阻塞至全部 TS 写完本日, 返回后三层张量整体可读.
    // 数值一致性由输入契约锚定 (CS 只读秒网格张量行, 见 CoreCrosssection.hpp),
    // 与"CS 流式伴随还是整日后扫"的调度形态无关, 结果逐值相同.
    GlobalFeatureStore::CsDay day;
    {
      TraceN("WaitSync");
      TraceColor(C_Orange);
      day = store.cs_open(date_str);
    }
    core.set_day(day);

    // 有效秒 [0, 15300): 末行是哨兵 (label lookahead 落点), 不进截面
    const size_t T = level_valid_rows(0);
    for (size_t t = 0; t < T; ++t) {
      TraceN("TimeslotLoop");
      TraceValue(t);
      core.compute_and_store(t);
    }

    ++completed_dates;
    progress_handle.update(completed_dates, total_dates, "");

    // 本日截面完成, slot 交给 IO 落盘
    store.cs_close(day);

    Logger::log("worker_" + std::to_string(worker_id), date_str + " completed: " + std::to_string(T) + " timeslots");

    TraceFrame;
  }

  // Final update
  char label_buf[128];
  snprintf(label_buf, sizeof(label_buf), "截面核心%2d: %3zu/%3zu 日期: Complete",
           worker_id, total_dates, total_dates);
  progress_handle.set_label(label_buf);
  progress_handle.update(total_dates, total_dates, "");

  Logger::log("worker_" + std::to_string(worker_id), "Completed: " + std::to_string(total_dates) + " dates processed");
}
