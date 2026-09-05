#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include "features/Backend/FeatureStore.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

void prefetch_worker(WorkerCtx ctx) {
  const int worker_id = ctx.worker_id;
  SharedData &data = ctx.data;
  GlobalFeatureStore &store = ctx.store;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  ComputeStats &stats = ctx.stats; // 单写者: 渲染线程只读

  TraceNS("PrefetchWorker", 5);
  TraceValue(worker_id);
  TraceThread(("prefetch_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = data.asset.all_dates.size();

  // 窗口 = TS 在飞跨度上限 (pool slots) + 超前余量: 保证最快 TS 到达前缓存已暖
  const size_t window_days = store.query_slots() + 2;

  Logger::log("worker_" + std::to_string(worker_id), "Prefetch started: " + std::to_string(total_dates) + " dates, window " + std::to_string(window_days));

  // 读缓冲: 只为把文件拖进 page cache, 内容即弃 (decode 在 TS worker 内联)
  std::vector<char> scratch(1 << 20);

  size_t cumulative_bytes = 0;
  size_t completed_dates = 0;

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
        stats.prefetch_bytes.store(cumulative_bytes, std::memory_order_relaxed);
      }
    }

    completed_dates = date_idx + 1;
    stats.prefetch_days.store(completed_dates, std::memory_order_relaxed);

    TraceFrame;
  }

  const bool cancelled = cancel_requested.load(std::memory_order_relaxed);

  if (cancelled)
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch cancelled: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(completed_dates) + " dates");
  else
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch completed: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(total_dates) + " dates");
}
