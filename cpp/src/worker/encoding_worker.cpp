#include "worker/encoding_worker.hpp"
#include "shared/SharedData.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "misc/affinity.hpp"
#include "misc/archive.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"
#include "shared/AssetAxis.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include <utility>

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
// ENCODING PRODUCER — 列举 + 顺序预读页缓存 + 推元数据批
// ============================================================================

namespace {

// 一批多少个资产 — 权衡两件事:
//   摊薄 unrar 固定开销 (进程启动 + 三万条目包头扫描): 批越大越省, 实测 20 个
//   资产一次调用比 20 次单独调用快 3.3x;
//   负载均衡: 批是 worker 的调度粒度, 批太大会在收尾时让部分 worker 空转.
constexpr size_t kAssetsPerBatch = 64;

// 把整个文件顺序读一遍丢弃 — 内容进 OS 页缓存, 之后 worker 的多路 unrar
// 全部命中内存, HDD 只见到这一条顺序流. 不自己持有字节: 页缓存由内核管理,
// 永远不会 OOM.
void warm_file_cache(const std::string &path, const std::atomic<bool> *cancel,
                     std::vector<char> &scratch) {
  TraceN("WarmFileCache");
  std::FILE *f = std::fopen(path.c_str(), "rb");
  assert(f && "warm_file_cache: 归档打不开 (列举刚成功过?)");
  while (!cancel->load() &&
         std::fread(scratch.data(), 1, scratch.size(), f) == scratch.size()) {
  }
  std::fclose(f);
}

} // namespace

void encoding_producer(SharedData &data,
                       BatchQueue &queue,
                       std::atomic<bool> *cancel_flag,
                       bool skip_existing,
                       EncodeStats &stats,
                       misc::ParallelProgress *progress) {
  TraceNS("EncodingProducer", 5);

  const AssetAxis &axis = asset_axis();
  assert(data.asset.items.size() == axis.size() &&
         "items 未与 A 轴对齐 (AssetLoader::load 没跑?)");

  const size_t total_days = data.asset.all_dates.size();
  std::vector<char> scratch(8 << 20); // 预读用的丢弃缓冲, 跨天复用

  for (size_t idx = 0; idx < total_days && !cancel_flag->load(); ++idx) {
    const std::string &date_str = data.asset.all_dates[idx];
    TraceN("ProducerDay");
    TraceTextS(date_str.c_str());

    // ------------------------------------------------------------------
    // 列举 — 当日包里"实际有哪些资产、每个文件多大"
    //
    // 全市场下不能拿 ipo/退市区间去盲试: 那会对当天包里没有的资产各发一次
    // unrar (每次重走三万条目的包头). 先列举拿到当天真实 universe, 再按
    // A 轴过滤掉 ETF/基金 (轴只含股票).
    //
    // 尺寸在这一步就要拿到 —— worker 靠它在 unrar p 的输出流上切分边界.
    // ------------------------------------------------------------------
    const std::string archive_path = Utils::generate_archive_path(
        data.config.archive_dir, date_str, data.config.archive_extension);
    const auto entries = misc::list_archive(archive_path, data.config.archive_tool);

    // 增量新鲜度规则: 产物 (.bin 或 .skip 墓碑) 存在且不老于归档 → 跳过.
    //   - 原子落盘保证"存在即完整", 一次 stat 就够, 不需要读内容;
    //   - 归档被重下/修复过 → mtime 变新 → 产物判过期, 重编覆盖.
    //     "修复受损数据"由此不需要单独的通道 (盘上位腐烂走离线 Verify).
    std::filesystem::file_time_type archive_mtime{};
    {
      std::error_code ec;
      archive_mtime = std::filesystem::last_write_time(archive_path, ec);
    }
    auto fresh = [&archive_mtime](const std::string &p) {
      std::error_code ec;
      const auto t = std::filesystem::last_write_time(p, ec);
      return !ec && t >= archive_mtime;
    };

    // 同一资产的委托/成交条目在包里是分开的两条, 先按资产归并
    struct Pending {
      size_t order_index = 0, trade_index = 0;
      size_t order_size = 0, trade_size = 0;
    };
    std::unordered_map<size_t, Pending> by_asset;

    for (const auto &entry : entries) {
      // "20260803/000001.SZ/逐笔委托.csv" → code_ex, filename
      const size_t first = entry.path.find('/');
      if (first == std::string::npos)
        continue;
      const size_t second = entry.path.find('/', first + 1);
      if (second == std::string::npos)
        continue;

      const std::string code_ex = entry.path.substr(first + 1, second - first - 1);
      const std::string filename = entry.path.substr(second + 1);

      const bool is_order = (filename == data.config.csv_market_order);
      const bool is_trade = (filename == data.config.csv_market_trade);
      if (!is_order && !is_trade)
        continue; // 行情.csv (快照) 不再编码

      const size_t asset_id = axis.find(code_ex);
      if (asset_id == axis.size())
        continue; // 非股票 (ETF/基金) 或轴外代码

      Pending &p = by_asset[asset_id];
      if (is_order) {
        p.order_index = entry.index;
        p.order_size = entry.size;
      } else {
        p.trade_index = entry.index;
        p.trade_size = entry.size;
      }
    }

    // 断点续跑: 产物新鲜就跳过 (见上方 fresh 规则)
    std::vector<EncodeTask> tasks;
    tasks.reserve(by_asset.size());
    for (const auto &[asset_id, p] : by_asset) {
      if (p.order_size == 0)
        continue; // 没有委托文件, 无从重建盘口

      const AssetItem &asset = data.asset.items[asset_id];
      if (skip_existing &&
          (fresh(Utils::generate_orders_path(
               data.config.orders_dir, date_str, asset.asset_code, asset.exchange,
               data.config.binary_extension)) ||
           fresh(Utils::generate_orders_path(
               data.config.orders_dir, date_str, asset.asset_code, asset.exchange,
               kEncodeTombstoneExt)))) {
        stats.pairs_skipped.fetch_add(1);
        continue;
      }
      tasks.push_back({asset_id, p.order_index, p.trade_index, p.order_size, p.trade_size});
    }

    // 收尾统计计数
    if (!tasks.empty()) {
      std::lock_guard<std::mutex> lock(stats.assets_mutex);
      for (const auto &task : tasks)
        stats.assets_with_work.insert(task.asset_id);
    }
    stats.pairs_listed.fetch_add(tasks.size());

    if (tasks.empty()) {
      // 整天跳过 (产物全部新鲜/包里没活) 也是完成了一天
      if (progress)
        progress->bump_summary();
      continue;
    }

    // 按归档序排, 再切成批 — unrar p 的输出顺序是归档顺序 (已实测),
    // worker 按这个顺序在流上切分.
    std::sort(tasks.begin(), tasks.end(),
              [](const EncodeTask &a, const EncodeTask &b) { return a.order_index < b.order_index; });

    // 注册天粒度账本 — 必须在推批之前 (worker 落盘时要查账).
    // 若这是当前最老的在编天, 顺手把附注立起来 (否则第一天列举+预读的
    // 几十秒里汇总行是空的).
    {
      std::lock_guard<std::mutex> lock(stats.days_mutex);
      const bool oldest = stats.days_inflight.empty();
      stats.days_inflight.emplace(date_str, std::pair<size_t, size_t>{0, tasks.size()});
      if (oldest && progress)
        progress->set_summary_note(date_str + ": 0/" + std::to_string(tasks.size()) +
                                   " assets");
    }

    // 目标目录建一次即可 (一天一个目录), 必须在推批之前
    std::filesystem::create_directories(
        Utils::generate_date_dir(data.config.orders_dir, date_str));

    // 预读必须在推批之前 — 批一旦可见, worker 的 unrar 就会去碰这个文件,
    // 冷文件会退化成几十路并发寻道.
    warm_file_cache(archive_path, cancel_flag, scratch);

    for (size_t i = 0; i < tasks.size() && !cancel_flag->load(); i += kAssetsPerBatch) {
      EncodeBatch batch;
      batch.date = date_str;
      batch.archive_path = archive_path;
      batch.tasks.assign(tasks.begin() + static_cast<long>(i),
                         tasks.begin() + static_cast<long>(std::min(i + kAssetsPerBatch, tasks.size())));
      if (!queue.push(std::move(batch)))
        return; // 队列已关 = 取消
    }

    TraceFrame;
  }
}

// ============================================================================
// ENCODING WORKER — 每批自己 unrar (命中页缓存) + 解码 + 原子写 .bin
// ============================================================================
//
// worker 只写文件, 不回填 data.asset.items 的 date_info.
//
// 同一个资产的不同日期会落在不同批上, 由不同 worker 并发处理 —— 再去写同一个
// asset 的 date_info (std::map) 就是数据竞争. 编码产物的真相由编码后的重新
// 扫描统一读取 (ScanService).

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
                     EncodeStats &stats,
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
  std::vector<size_t> stragglers; // 两块在归档序上不相邻的任务 (批内下标)

  EncodeBatch batch;
  while (!cancel_flag->load() && queue.pop(batch)) {
    TraceN("BatchLoop");
    TraceTextS(batch.date.c_str());

    if (batch.tasks.empty())
      continue;

    // 批内所有文件按归档序排成一条读取计划.
    //
    // 这一步是必须的: unrar p 的输出顺序是归档顺序, 与名单顺序无关 (已实测).
    // 排错了就会拿 A 的尺寸去切 B 的字节, 静默产出垃圾.
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

      // 主流的流式切分假定"同一资产的两块在归档序上相邻" — 包按 date/ASSET/
      // 目录逐个写入时天然成立, 但修补过的归档会把补的文件追加到包尾
      // (实测 20230714 的 600265.SH: 委托在条目 ~8995, 成交在 ~20049).
      // 不相邻的任务从主计划剔除, 批尾单独小流处理 (见下方 stragglers 循环).
      stragglers.clear();
      {
        constexpr size_t kNone = static_cast<size_t>(-1);
        std::vector<std::pair<size_t, size_t>> pos(batch.tasks.size(), {kNone, kNone});
        for (size_t i = 0; i < plan.size(); ++i) {
          auto &p = pos[plan[i].second.task_idx];
          (p.first == kNone ? p.first : p.second) = i;
        }
        std::vector<char> non_adjacent(batch.tasks.size(), 0);
        for (size_t t = 0; t < batch.tasks.size(); ++t) {
          if (pos[t].second != kNone && pos[t].second != pos[t].first + 1) {
            non_adjacent[t] = 1;
            stragglers.push_back(t);
          }
        }
        if (!stragglers.empty())
          plan.erase(std::remove_if(
                         plan.begin(), plan.end(),
                         [&](const auto &e) { return non_adjacent[e.second.task_idx]; }),
                     plan.end());
      }

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
      // 剔除 stragglers 之后, 相邻性在主计划里是按构造保证的 (剔除只会
      // 拉近剩余元素, 不会往中间插新东西). 万一还破 — 一个资产会被
      // begin/finish 两次, 第二次把第一次的产物覆盖成半截数据, 完全无声,
      // 与其信任不如当场炸.
      std::vector<bool> closed(batch.tasks.size(), false);
      for (size_t i = 0; i < slots.size(); ++i) {
        const size_t t = slots[i].task_idx;
        assert(!closed[t] && "encoding_worker: 主计划里仍有不相邻的资产, 切分会错位");
        if (i + 1 < slots.size() && slots[i + 1].task_idx != t)
          closed[t] = true;
      }
#endif
    }

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
      const std::string skip_path = Utils::generate_orders_path(
          data.config.orders_dir, batch.date, asset.asset_code, asset.exchange,
          kEncodeTombstoneExt);

      std::error_code ec;
      switch (encoder.finish_asset(out_path, batch.date + " " + asset_full)) {
      case L2::EncodeResult::Ok:
        // 源修复后重新可编 → 清掉陈旧墓碑
        std::filesystem::remove(skip_path, ec);
        break;
      case L2::EncodeResult::TooFewOrders:
        // 持久否定缓存: touch 墓碑 (mtime = now), 增量重跑不再碰它;
        // 旧归档编出的陈旧产物一并清掉 (新归档判定它不可编)
        std::ofstream(skip_path, std::ios::trunc).flush();
        std::filesystem::remove(out_path, ec);
        break;
      case L2::EncodeResult::Error:
        // 环境错误 (磁盘满/压缩失败): 不留任何产物, 下次增量重试
        Logger::log("encoding", "[Worker " + std::to_string(worker_id) + "] [FAILED] " +
                                    batch.date + " " + asset_full);
        break;
      }
      ++done_in_batch;
      progress_handle.update(done_in_batch, batch.tasks.size(), batch.date);

      // 天粒度账本: 本天 +1, 清零则整天完成 (汇总 days +1);
      // 附注显示最老在编天的资产进度 (多天在飞时以它为准)
      {
        std::lock_guard<std::mutex> lock(stats.days_mutex);
        auto it = stats.days_inflight.find(batch.date);
        assert(it != stats.days_inflight.end() && "flush_task: 本天未在账本注册");
        if (++it->second.first == it->second.second) {
          stats.days_inflight.erase(it);
          progress_handle.bump_summary();
        }
        if (!stats.days_inflight.empty()) {
          const auto &[day, counts] = *stats.days_inflight.begin();
          progress_handle.set_summary_note(day + ": " + std::to_string(counts.first) + "/" +
                                           std::to_string(counts.second) + " assets");
        }
      }
      fed_any = false;
    };

    if (!paths.empty()) {
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
              progress_handle.set_label(asset.asset_code + " " + asset.asset_name);
              fed_any = true;
            }

            if (slot.is_trade)
              encoder.feed_trade_csv(csv, len);
            else
              encoder.feed_order_csv(csv, len);
          },
          cancel_flag);
    }

    // 取消时流是中途断的, 手上可能只喂了半个资产 — 直接丢弃, 不落盘
    // (落了会写出半截 .bin, 且续跑时被新鲜度规则跳过).
    if (cancel_flag->load())
      break;

    flush_task(current_task); // 主流的最后一个资产

    // 不相邻任务的兜底: 每个任务单独一次两文件小流. 只开这一个资产,
    // 两块到达先后无所谓 (finish_asset 统一合并排序); 多付一次 unrar
    // 固定开销, 但这种任务一天最多个位数, 无关紧要.
    for (const size_t t : stragglers) {
      if (cancel_flag->load())
        break;
      const EncodeTask &task = batch.tasks[t];
      const AssetItem &asset = data.asset.items[task.asset_id];
      const std::string base = batch.date + "/" + asset.asset_code + "." + asset.exchange + "/";

      // 两个文件仍要按归档序请求 (unrar p 按归档序输出)
      const bool trade_first = task.trade_index < task.order_index;
      std::vector<std::string> two_paths{base + data.config.csv_market_order,
                                         base + data.config.csv_market_trade};
      std::vector<size_t> two_sizes{task.order_size, task.trade_size};
      if (trade_first) {
        std::swap(two_paths[0], two_paths[1]);
        std::swap(two_sizes[0], two_sizes[1]);
      }

      encoder.begin_asset();
      progress_handle.set_label(asset.asset_code + " " + asset.asset_name);
      fed_any = true;
      misc::stream_archive_files(
          batch.archive_path, data.config.archive_tool, two_paths, two_sizes,
          [&](size_t i, const char *csv, size_t len) {
            if ((i == 0) == trade_first)
              encoder.feed_trade_csv(csv, len);
            else
              encoder.feed_order_csv(csv, len);
          },
          cancel_flag);
      if (cancel_flag->load())
        break;
      flush_task(t);
    }
    if (cancel_flag->load())
      break;

    TraceFrame;
  }

  progress_handle.set_label("Idle");
  Logger::log("encoding", "[Worker " + std::to_string(worker_id) + "] Finished");
}
