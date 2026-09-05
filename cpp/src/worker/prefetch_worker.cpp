#include "worker/prefetch_worker.hpp"
#include "shared/SharedData.hpp"

#include "features/Backend/FeatureStore.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

void prefetch_worker(int worker_id,
                     SharedData &data,
                     GlobalFeatureStore &store,
                     const std::atomic<bool> &cancel_requested,
                     misc::ProgressHandle progress_handle) {
  TraceNS("PrefetchWorker", 5);
  TraceValue(worker_id);
  TraceThread(("prefetch_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = data.asset.all_dates.size();

  // 窗口 = TS 在飞跨度上限 (pool slots) + 超前余量: 保证最快 TS 到达前缓存已暖
  const size_t window_days = store.query_slots() + 2;

  char label_buf[128];
  snprintf(label_buf, sizeof(label_buf), "预取核心%2d:", worker_id);
  progress_handle.set_label(label_buf);
  progress_handle.update(0, total_dates, "");

  Logger::log("worker_" + std::to_string(worker_id), "Prefetch started: " + std::to_string(total_dates) + " dates, window " + std::to_string(window_days));

  // 读缓冲: 只为把文件拖进 page cache, 内容即弃 (decode 在 TS worker 内联)
  std::vector<char> scratch(1 << 20);

  size_t cumulative_bytes = 0;
  size_t completed_dates = 0;
  auto start_time = std::chrono::steady_clock::now();

  for (size_t date_idx = 0; date_idx < total_dates; ++date_idx) {
    if (cancel_requested.load(std::memory_order_relaxed))
      break;

    TraceN("DateLoop");
    const std::string &date_str = data.asset.all_dates[date_idx];
    TraceTextS(date_str.c_str());

    // 门控: 领先最慢 TS 不超过窗口
    {
      TraceN("GateWait");
      TraceColor(C_Orange);
      while (date_idx > store.query_ts_days_done() + window_days) {
        if (cancel_requested.load(std::memory_order_relaxed))
          break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      if (cancel_requested.load(std::memory_order_relaxed))
        break;
    }

    // 日期 → 日期轴下标, 一天查一次 (与 sequential_worker 同姿态)
    const size_t didx = data.asset.date_idx(date_str);

    {
      TraceN("ReadFiles");
      for (const AssetItem &asset : data.asset.items) {
        if (!asset.date_at(didx).has_binaries())
          continue;

        const std::string orders_file = Utils::generate_orders_path(
            data.config.orders_dir, date_str, asset.asset_code, asset.exchange,
            config::BINARY_EXTENSION);

        // 缺文件不在这里报: TS worker decode 失败时记 WARNING, 预取只是暖缓存
        FILE *f = std::fopen(orders_file.c_str(), "rb");
        if (!f)
          continue;
        size_t n;
        while ((n = std::fread(scratch.data(), 1, scratch.size(), f)) > 0)
          cumulative_bytes += n;
        std::fclose(f);
      }
    }

    // Update progress
    auto current_time = std::chrono::steady_clock::now();
    float elapsed_seconds = std::chrono::duration<float>(current_time - start_time).count();
    float speed_MB_per_sec = (elapsed_seconds > 0) ? (cumulative_bytes / 1e6) / elapsed_seconds : 0.0;

    char msg_buf[128];
    snprintf(msg_buf, sizeof(msg_buf), "%s [%.0fMB/s (%.1fGB)]", date_str.c_str(), speed_MB_per_sec, cumulative_bytes / 1e9);
    completed_dates = date_idx + 1;
    progress_handle.update(completed_dates, total_dates, msg_buf);

    TraceFrame;
  }

  const bool cancelled = cancel_requested.load(std::memory_order_relaxed);
  progress_handle.update(cancelled ? completed_dates : total_dates, total_dates, cancelled ? "Cancelled" : "Complete");

  if (cancelled)
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch cancelled: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(completed_dates) + " dates");
  else
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch completed: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(total_dates) + " dates");
}
