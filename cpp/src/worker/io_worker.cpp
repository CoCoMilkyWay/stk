#include "worker/io_worker.hpp"
#include "misc/logging.hpp"
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

void io_worker(GlobalFeatureStore *store, misc::ProgressHandle handle, size_t total_dates, int worker_id) {
  Logger::log_worker(worker_id, "IO worker started");

  size_t flush_count = 0;

  // Update initial label
  char label_buf[1024];
  snprintf(label_buf, sizeof(label_buf), "IO核心  %2d:   0/%3zu 等待数据", worker_id, total_dates);
  handle.set_label(label_buf);
  handle.update(0, total_dates, "");

  Logger::log_worker(worker_id, "Starting flush loop, total dates: " + std::to_string(total_dates));

  size_t wait_count = 0;
  while (flush_count < total_dates) {
    // Flush oldest CS_DONE tensor (one at a time, maintains date order)
    // Logger::log_worker(worker_id, "Attempting flush, current count: " + std::to_string(flush_count) + "/" + std::to_string(total_dates));
    bool flushed = store->io_flush_once();

    if (flushed) {
      flush_count++;
      wait_count = 0;
      Logger::log_worker(worker_id, "Flushed successfully, count: " + std::to_string(flush_count) + "/" + std::to_string(total_dates));

      // Update progress with pool status
      snprintf(label_buf, sizeof(label_buf), "IO核心  %2d: %3zu/%3zu", worker_id, flush_count, total_dates);
      std::string label_with_status = std::string(label_buf);
      handle.set_label(label_with_status);
      handle.update(flush_count, total_dates, "");
    } else {
      // No tensors ready yet, sleep briefly
      wait_count++;
      if (wait_count % 100 == 0) {
        Logger::log_worker(worker_id, "Still waiting for tensors to be ready (waited " + std::to_string(wait_count * 10) + "ms)");
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  // Final update
  snprintf(label_buf, sizeof(label_buf), "IO核心  %2d: %3zu/%3zu Complete", worker_id, total_dates, total_dates);
  handle.set_label(label_buf);
  handle.update(total_dates, total_dates, "");

  Logger::log_worker(worker_id, "IO worker completed all flushes");
}
