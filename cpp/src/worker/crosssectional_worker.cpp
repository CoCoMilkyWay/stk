#include "worker/crosssectional_worker.hpp"
#include "shared/SharedData.hpp"

#include "features/CoreCrosssection.hpp"
#include "features/Backend/FeatureStore.hpp"
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
    const size_t capacity = store.query_T(0);

    // Update progress label
    char label_buf[128];
    snprintf(label_buf, sizeof(label_buf), "截面核心%2d: %3zu/%3zu 日期: %s",
             worker_id, date_idx + 1, total_dates, date_str.c_str());
    progress_handle.set_label(label_buf);

    core.set_date(date_str);

    // Process each time slot (per-slot sync for live trading compatibility)
    for (size_t t = 0; t < capacity; ++t) {
      TraceN("TimeslotLoop");
      TraceValue(t);

      {
        TraceN("WaitSync");
        TraceColor(C_Orange);
        store.cs_wait(date_str, t);
      }

      core.compute_and_store(t);
    }

    ++completed_dates;
    progress_handle.update(completed_dates, total_dates, "");

    // Mark this date as complete for tensor pool recycling
    store.cs_done(date_str);

    Logger::log("worker_" + std::to_string(worker_id), date_str + " completed: " + std::to_string(capacity) + " timeslots");

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
