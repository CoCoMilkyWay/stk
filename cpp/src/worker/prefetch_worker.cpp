#include "shared/SharedData.hpp"
#include "worker/feature_workers.hpp"

#include "features/Backend/FeatureStore.hpp"
#include "misc/cross_platform.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>

void prefetch_worker(WorkerCtx ctx) {
  const int worker_id = ctx.worker_id;
  SharedData &data = ctx.data;
  GlobalFeatureStore &store = ctx.store;
  const std::atomic<bool> &cancel_requested = ctx.cancel;
  // 本核发布槽 (单写者): work = 累计读入字节, idle_ms = 累计门控干等毫秒
  ComputeStats::Core &stat = ctx.stats.prefetch;

  TraceNS("PrefetchWorker", 5);
  TraceValue(worker_id);
  TraceThread(("prefetch_worker_" + std::to_string(worker_id)).c_str());

  const size_t total_dates = data.asset.all_dates.size();

  // 窗口 = TS 在飞跨度上限 (pool slots) + 超前余量: 保证最快 TS 到达前缓存已暖
  const size_t window_days = store.query_slots() + 2;

  Logger::log("worker_" + std::to_string(worker_id), "Prefetch started: " + std::to_string(total_dates) + " dates, window " + std::to_string(window_days));

  // 读缓冲: 只为把帧拖进 page cache, 内容即弃 (decode 在 TS worker 内联)
  std::vector<char> scratch(1 << 20);

  // 当天要读的帧 (偏移, 帧长), 跨日复用容量
  std::vector<std::pair<uint64_t, uint64_t>> spans;

  size_t cumulative_bytes = 0;
  size_t completed_dates = 0;

  for (size_t date_idx = 0; date_idx < total_dates; ++date_idx) {
    if (cancel_requested.load(std::memory_order_relaxed))
      break;

    TraceN("DateLoop");
    const misc::Ymd date = data.asset.all_dates[date_idx];
    TraceTextS(misc::dense(date).c_str());

    // 门控: 领先最慢 TS 不超过窗口
    {
      TraceN("GateWait");
      TraceColor(C_Orange);
      while (date_idx > store.query_ts_days_done() + window_days) {
        if (cancel_requested.load(std::memory_order_relaxed))
          break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        stat.idle_ms.fetch_add(10, std::memory_order_relaxed);
      }
      if (cancel_requested.load(std::memory_order_relaxed))
        break;
    }

    // 日期 → 日期轴下标, 一天查一次 (与 sequential_worker 同姿态)
    const size_t didx = data.asset.date_idx(date);

    {
      TraceN("ReadFiles");
      // 资产集 = universe 子轴 (sched.global_ids): TS 只 decode 这些. 按偏移排序,
      // 首尾相接的帧并成一段顺序读 —— universe 覆盖全市场时就是整个数据区一口气读完.
      spans.clear();
      for (const uint32_t global_id : ctx.sched.global_ids) {
        const DateInfo &di = data.asset.items[global_id].date_at(didx);
        if (di.has_binaries())
          spans.emplace_back(di.offset, di.compressed_size);
      }
      std::sort(spans.begin(), spans.end());
      size_t merged = 0;
      for (size_t i = 0; i < spans.size(); ++i) {
        if (merged > 0 && spans[merged - 1].first + spans[merged - 1].second == spans[i].first)
          spans[merged - 1].second += spans[i].second;
        else
          spans[merged++] = spans[i];
      }
      spans.resize(merged);

      if (!spans.empty()) {
        const std::string day_path =
            Utils::generate_orders_path(data.config.orders_dir, date, config::BINARY_EXTENSION);
        const int fd = file_open_read(day_path.c_str());
        assert(fd >= 0 && "prefetch: 扫描登记过的日文件打不开");
        for (const auto &[offset, size] : spans) {
          for (uint64_t done = 0; done < size;) {
            const size_t want = static_cast<size_t>(std::min<uint64_t>(scratch.size(), size - done));
            const size_t got = file_pread(fd, scratch.data(), want, offset + done);
            assert(got == want && "prefetch: 帧越过日文件末尾 (目录与文件不符)");
            done += got;
            cumulative_bytes += got;
          }
          stat.work.store(cumulative_bytes, std::memory_order_relaxed);
        }
        file_close(fd);
      }
    }

    completed_dates = date_idx + 1;
    ctx.stats.prefetch_days.store(completed_dates, std::memory_order_relaxed);

    TraceFrame;
  }

  const bool cancelled = cancel_requested.load(std::memory_order_relaxed);

  if (cancelled)
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch cancelled: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(completed_dates) + " dates");
  else
    Logger::log("worker_" + std::to_string(worker_id), "Prefetch completed: " + std::to_string(cumulative_bytes) + " bytes across " + std::to_string(total_dates) + " dates");
}
