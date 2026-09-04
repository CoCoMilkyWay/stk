#include "worker/sequential_worker.hpp"
#include "shared/SharedData.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/binary_decoder_L2.hpp"
#include "features/Backend/FeatureStore.hpp"
#include "lob/LimitOrderBook.hpp"
#include "misc/logging.hpp"
#include "misc/profiler.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <memory>
#include <vector>

void sequential_worker(int worker_id,
                       SharedData &data,
                       GlobalFeatureStore &store,
                       misc::ProgressHandle progress_handle) {
  TraceNS("TSWorker", 5);
  TraceValue(worker_id);
  TraceThread(("ts_worker_" + std::to_string(worker_id)).c_str());

  // Initialize thread-local state (MUST be first - handles thread reuse across compute runs)
  store.ts_worker_init(worker_id);

  // Initialize as idle (will be updated if assets are assigned)
  progress_handle.set_label("Idle");
  progress_handle.update(1, 1, "");

  // Find assets assigned to this worker
  std::vector<size_t> my_asset_ids;
  for (size_t i = 0; i < data.asset.items.size(); ++i) {
    if (data.asset.items[i].assigned_worker_id == worker_id) {
      my_asset_ids.push_back(i);
    }
  }

  // Initialize LOBs for each asset
  std::vector<std::unique_ptr<LimitOrderBook>> lobs;

  {
    TraceN("InitLOBs");
    for (size_t i = 0; i < my_asset_ids.size(); ++i) {
      const size_t asset_id = my_asset_ids[i];
      const auto &asset = data.asset.items[asset_id];
      lobs.push_back(std::make_unique<LimitOrderBook>(L2::LOB_ORDER_CAPACITY, store, data.fund_pool, asset.asset_code, asset.exchange_type, asset.asset_id, worker_id));
    }
  }

  // 每 worker 一个 decoder (非每资产): decode 是按 (asset, date) 文件的无状态操作,
  // 且 worker 内资产串行 —— 解码结果在下一次 decode 前已被 process_batch 消费完,
  // 零拷贝契约天然满足. 缓冲跨资产/跨日复用, 自动长到本 worker 最忙资产的单日
  // 条数后稳定 (零分配稳态);
  L2::BinaryDecoder_L2 decoder(L2::DEFAULT_ENCODER_ORDER_SIZE);

  Logger::log("worker_" + std::to_string(worker_id), "Started: " + std::to_string(my_asset_ids.size()) + " assets, " +
                                                         std::to_string(data.asset.all_dates.size()) + " dates");

  // Progress label
  char label_buf[128];
  if (!my_asset_ids.empty()) {
    snprintf(label_buf, sizeof(label_buf), "时序核心%2d: %2zu Assets: %s(%s)",
             worker_id,
             my_asset_ids.size(),
             data.asset.items[my_asset_ids[0]].asset_code.c_str(),
             data.asset.items[my_asset_ids[0]].asset_name.c_str());
  } else {
    snprintf(label_buf, sizeof(label_buf), "时序核心%2d: Idle", worker_id);
  }
  progress_handle.set_label(label_buf);

  size_t cumulative_orders = 0;
  auto start_time = std::chrono::steady_clock::now();

  // Zero-copy streaming: decoder maintains internal buffer, worker receives const pointer
  // No memory allocation in worker - decoder reuses buffer across all decode calls

  for (size_t date_idx = 0; date_idx < data.asset.all_dates.size(); ++date_idx) {
    TraceN("DateLoop");
    const std::string &date_str = data.asset.all_dates[date_idx];
    TraceTextS(date_str.c_str());
    size_t date_orders = 0;
    size_t date_assets_processed = 0;

    // Process each asset at this date
    for (size_t i = 0; i < my_asset_ids.size(); ++i) {
      const size_t asset_id = my_asset_ids[i];
      const auto &asset = data.asset.items[asset_id];
      auto it = asset.date_info.find(date_str);
      lobs[i]->begin_day(date_str); // 盘前: DAG reset + onDay (Fund 状态机推进到当日)
      // Hot path: has data and binaries
      if (it != asset.date_info.end() && it->second.has_binaries()) [[likely]] {

        // 路径由 (date, code, exchange) 现算 — DateInfo 不再为五百万条记录
        // 各存一份字符串
        const std::string orders_file = Utils::generate_orders_path(
            data.config.orders_dir, date_str, asset.asset_code, asset.exchange,
            config::BINARY_EXTENSION);

      size_t order_num = 0;
      const L2::Order *orders = nullptr;
      {
        TraceN("DecodeOrders");
        orders = decoder.decode_orders_stream(orders_file, order_num);
      }

      if (orders != nullptr) [[likely]] {
        // 档位索引基准来自这一天的文件头, 必须先于第一条订单设进去 —— 绝对价
        // 要减去它才是档位下标 (见 L2_DataType.hpp 的 kPriceIndexRange).
        lobs[i]->set_price_base(decoder.last_price_base());

          // Batch processing: zero-overhead inlined loop (process_impl inlined into process_batch)
          size_t order_invalid_cnt = 0;
          {
            TraceN("ProcessLobs");
            TraceValue(order_num);
            order_invalid_cnt = lobs[i]->process_batch(orders, order_num);
          }

          if (order_invalid_cnt > 100) {
            Logger::log("worker_" + std::to_string(worker_id), "ERROR: " + date_str + " asset_id=" + std::to_string(asset_id) + " order_invalid=" + std::to_string(order_invalid_cnt));
            std::exit(1);
          }

          if (order_num > 0) {
            Logger::log("worker_" + std::to_string(worker_id),
                        date_str + " asset:" + std::to_string(asset_id) + " " + asset.asset_code + "." + asset.exchange + " " + asset.asset_name +
                            " decoded=" + std::to_string(order_num) +
                            " order_invalid=" + std::to_string(order_invalid_cnt) +
                            " tob_invalid=" + std::to_string(lobs[i]->get_tob_invalid_count()) +
                            " tob_refresh=" + std::to_string(lobs[i]->get_tob_refresh_count()));
          }

          lobs[i]->end_day();
          lobs[i]->clear();
          date_orders += order_num;
          date_assets_processed++;
          cumulative_orders += order_num;
        } else {
          Logger::log("worker_" + std::to_string(worker_id), "WARNING: " + date_str + " failed to decode " + orders_file);
        }
      }
    }

    if (date_assets_processed > 0) {
      Logger::log("worker_" + std::to_string(worker_id), date_str + " completed: " + std::to_string(date_assets_processed) + " assets, " + std::to_string(date_orders) + " orders");
    }

    // Mark this worker done for this date (will also set all asset progress atomically)
    {
      TraceN("StoreDone");
      store.ts_done(date_str, worker_id);
    }

    // Update progress
    auto current_time = std::chrono::steady_clock::now();
    float elapsed_seconds = std::chrono::duration<float>(current_time - start_time).count();
    float speed_M_per_sec = (elapsed_seconds > 0) ? (cumulative_orders / 1e6) / elapsed_seconds : 0.0;

    char msg_buf[128];
    snprintf(msg_buf, sizeof(msg_buf), "%s [%.1fM/s (%.1fM)]", date_str.c_str(), speed_M_per_sec, cumulative_orders / 1e6);
    progress_handle.update(date_idx + 1, data.asset.all_dates.size(), msg_buf);

    TraceFrame; // Mark frame boundary for timeline
  }

  Logger::log("worker_" + std::to_string(worker_id), "Completed: processed " + std::to_string(cumulative_orders) + " orders across " + std::to_string(data.asset.all_dates.size()) + " dates");
}
