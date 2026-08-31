#include "worker/encoding_worker.hpp"
#include "shared/SharedData.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "misc/affinity.hpp"
#include "misc/archive.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <string>

// ============================================================================
// BATCH QUEUE
// ============================================================================

bool BatchQueue::push(EncodeBatch batch) {
  std::unique_lock<std::mutex> lock(mutex_);
  not_full_.wait(lock, [this] { return closed_ || queue_.size() < capacity_; });
  if (closed_)
    return false;
  queue_.push_back(std::move(batch));
  lock.unlock();
  not_empty_.notify_one();
  return true;
}

bool BatchQueue::pop(EncodeBatch &out) {
  std::unique_lock<std::mutex> lock(mutex_);
  not_empty_.wait(lock, [this] { return closed_ || !queue_.empty(); });
  if (queue_.empty())
    return false; // 已 close 且排空
  out = std::move(queue_.front());
  queue_.pop_front();
  lock.unlock();
  not_full_.notify_one();
  return true;
}

void BatchQueue::close() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
  }
  not_empty_.notify_all();
  not_full_.notify_all();
}

// ============================================================================
// ENCODING WORKER
// ============================================================================
//
// worker 只写文件, 不回填 data.asset.items 的 date_info.
//
// 从"资产为主序"改成"日期为主批"之后, 同一个资产的不同日期会落在不同批上,
// 由不同 worker 并发处理 —— 再去写同一个 asset 的 date_info (std::map) 就是
// 数据竞争. 编码产物的真相由编码后的重新扫描统一读取 (ScanService).

namespace {

// 批内一个待读文件在流上的位置
struct StreamSlot {
  size_t task_idx;
  bool is_trade;
};

} // namespace

void encoding_worker(SharedData &data,
                     BatchQueue &queue,
                     std::atomic<bool> *cancel_flag,
                     unsigned int worker_id,
                     misc::ProgressHandle progress_handle) {
  TraceNS("EncodingWorker", 5);
  TraceValue(worker_id);
  TraceThread(("encoding_worker_" + std::to_string(worker_id)).c_str());

  static thread_local bool affinity_set = false;
  if (!affinity_set && misc::Affinity::supported()) {
    const unsigned int core_count = misc::Affinity::core_count();
    affinity_set = misc::Affinity::pin_to_core(core_count > 0 ? worker_id % core_count : 0);
  }

  L2::BinaryEncoder_L2 encoder(L2::DEFAULT_ENCODER_ORDER_SIZE);
  progress_handle.set_label("Idle");
  progress_handle.update(1, 1, "");

  Logger::log("encoding", "[Worker " + std::to_string(worker_id) + "] Started");

  // 归档序排好的读取计划, 跨批复用容量
  std::vector<std::string> paths;
  std::vector<size_t> sizes;
  std::vector<StreamSlot> slots;

  EncodeBatch batch;
  while (!cancel_flag->load() && queue.pop(batch)) {
    TraceN("BatchLoop");
    TraceTextS(batch.date.c_str());

    if (batch.tasks.empty())
      continue;

    // 批内所有文件按归档序排成一条读取计划.
    //
    // 这一步是必须的: unrar p 的输出顺序是归档顺序, 与命令行上模式的顺序无关
    // (已实测). 排错了就会拿 A 的尺寸去切 B 的字节, 静默产出垃圾.
    paths.clear();
    sizes.clear();
    slots.clear();

    {
      std::vector<std::pair<size_t, StreamSlot>> plan; // (归档序, slot)
      plan.reserve(batch.tasks.size() * 2);
      for (size_t t = 0; t < batch.tasks.size(); ++t) {
        const EncodeTask &task = batch.tasks[t];
        plan.emplace_back(task.order_index, StreamSlot{t, false});
        if (task.trade_size > 0)
          plan.emplace_back(task.trade_index, StreamSlot{t, true});
      }
      std::sort(plan.begin(), plan.end(),
                [](const auto &a, const auto &b) { return a.first < b.first; });

      paths.reserve(plan.size());
      sizes.reserve(plan.size());
      slots.reserve(plan.size());
      for (const auto &[archive_index, slot] : plan) {
        const AssetItem &asset = data.asset.items[batch.tasks[slot.task_idx].asset_id];
        const std::string asset_full = asset.asset_code + "." + asset.exchange;
        paths.push_back(batch.date + "/" + asset_full + "/" +
                        (slot.is_trade ? data.config.csv_market_trade : data.config.csv_market_order));
        sizes.push_back(slot.is_trade ? batch.tasks[slot.task_idx].trade_size
                                      : batch.tasks[slot.task_idx].order_size);
        slots.push_back(slot);
      }

#ifndef NDEBUG
      // 下面的流式消费假定"同一资产的两块在归档序上相邻" —— 包是按
      // date/ASSET/ 目录逐个写进去的, 所以本来就相邻. 万一某天的包不是这样,
      // 一个资产会被 begin/finish 两次, 第二次把第一次的产物覆盖成半截数据,
      // 而且完全无声. 与其信任, 不如在这里当场炸.
      std::vector<bool> closed(batch.tasks.size(), false);
      for (size_t i = 0; i < slots.size(); ++i) {
        const size_t t = slots[i].task_idx;
        assert(!closed[t] && "encoding_worker: 同一资产的 CSV 在归档里不相邻, 批内切分会错位");
        if (i + 1 < slots.size() && slots[i + 1].task_idx != t)
          closed[t] = true;
      }
#endif
    }

    // 目标目录建一次即可 (一批同属一天)
    std::filesystem::create_directories(
        Utils::generate_date_dir(data.config.orders_dir, batch.date));

    // 流式消费: 每个文件到达就地解析, 一个资产的两块都喂完就落盘.
    // 一次只持有一个文件的字节 — 批可以开得很大而内存不涨.
    size_t current_task = paths.empty() ? 0 : slots[0].task_idx;
    bool fed_any = false;
    size_t done_in_batch = 0;

    auto flush_task = [&](size_t task_idx) {
      if (!fed_any)
        return;
      const EncodeTask &task = batch.tasks[task_idx];
      const AssetItem &asset = data.asset.items[task.asset_id];
      const std::string asset_full = asset.asset_code + "." + asset.exchange;
      const std::string out_path = Utils::generate_orders_path(
          data.config.orders_dir, batch.date, asset.asset_code, asset.exchange,
          data.config.binary_extension);

      if (!encoder.finish_asset(out_path, batch.date + " " + asset_full)) {
        Logger::log("encoding", "[Worker " + std::to_string(worker_id) + "] [FAILED] " +
                                    batch.date + " " + asset_full);
      }
      ++done_in_batch;
      progress_handle.update(done_in_batch, batch.tasks.size(), batch.date);
      progress_handle.bump_summary();
      fed_any = false;
    };

    {
      TraceN("StreamBatch");
      misc::stream_archive_files(
          batch.archive_path, data.config.archive_tool, paths, sizes,
          [&](size_t i, const char *csv, size_t len) {
            const StreamSlot &slot = slots[i];

            // 换资产了 → 先把上一个收尾落盘
            if (slot.task_idx != current_task) {
              flush_task(current_task);
              current_task = slot.task_idx;
            }
            if (!fed_any) {
              encoder.begin_asset();
              const AssetItem &asset = data.asset.items[batch.tasks[slot.task_idx].asset_id];
              progress_handle.set_label(asset.asset_code + " (" + asset.asset_name + ")");
              fed_any = true;
            }

            if (slot.is_trade)
              encoder.feed_trade_csv(csv, len);
            else
              encoder.feed_order_csv(csv, len);
          });
    }

    flush_task(current_task); // 批内最后一个资产

    TraceFrame;
  }

  progress_handle.set_label("Idle");
  Logger::log("encoding", "[Worker " + std::to_string(worker_id) + "] Finished");
}
