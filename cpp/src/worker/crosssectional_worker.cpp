#include "worker/crosssectional_worker.hpp"
#include "worker/shared_state.hpp"

#include "features/CoreCrosssection.hpp"
#include "features/backend/FeatureStore.hpp"
#include "misc/logging.hpp"

#include <chrono>
#include <cstdio>
#include <thread>

void crosssectional_worker(const SharedState &state,
                           GlobalFeatureStore *feature_store,
                           int worker_id,
                           misc::ProgressHandle progress_handle) {

  const size_t total_dates = state.all_dates.size();
  size_t completed_dates = 0;

  Logger::log_worker(worker_id, "Started: " + std::to_string(total_dates) + " dates to process");

  // Date-first traversal
  for (size_t date_idx = 0; date_idx < state.all_dates.size(); ++date_idx) {
    const std::string &date_str = state.all_dates[date_idx];
    const size_t capacity = feature_store->query_T(0);

    // Update progress label
    char label_buf[128];
    snprintf(label_buf, sizeof(label_buf), "截面核心%2d: %3zu/%3zu 日期: %s",
             worker_id, date_idx + 1, total_dates, date_str.c_str());
    progress_handle.set_label(label_buf);

    // Process each time slot as it becomes ready
    size_t t = 0;
    size_t total_wait_time = 0;
    while (t < capacity) {
      // Wait for time slot t to be ready (simple polling)
      size_t wait_count = 0;
      while (!feature_store->cs_check_ready(date_str, t)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        wait_count++;
        if (wait_count % 10000 == 0) {
          Logger::log_worker(worker_id, date_str + " waiting for timeslot " + std::to_string(t) + "/" + std::to_string(capacity) + " (" + std::to_string(wait_count) + "ms)");
        }
      }
      total_wait_time += wait_count;

      // Compute CS features for this time slot
      compute_cs_for_timeslot(feature_store, date_str, t);

      ++t;

      // Update progress every 100 time slots
      if (t % 100 == 0) {
        progress_handle.update(date_idx + 1, total_dates, "");
      }
    }

    ++completed_dates;
    progress_handle.update(completed_dates, total_dates, "");

    // Mark this date as complete for tensor pool recycling
    feature_store->cs_mark_complete(date_str);
    
    Logger::log_worker(worker_id, date_str + " completed: " + std::to_string(capacity) + " timeslots" + 
                      (total_wait_time > 0 ? ", waited " + std::to_string(total_wait_time) + "ms" : ""));
  }

  // Final update
  char label_buf[128];
  snprintf(label_buf, sizeof(label_buf), "截面核心%2d: %3zu/%3zu 日期: Complete",
           worker_id, total_dates, total_dates);
  progress_handle.set_label(label_buf);
  progress_handle.update(total_dates, total_dates, "");

  Logger::log_worker(worker_id, "Completed: " + std::to_string(total_dates) + " dates processed");
}
