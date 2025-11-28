#include "worker/sequential_worker.hpp"
#include "shared/SharedData.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/binary_decoder_L2.hpp"
#include "features/backend/FeatureStore.hpp"
#include "lob/LimitOrderBook.hpp"
#include "misc/logging.hpp"

#include <chrono>
#include <cstdio>
#include <memory>
#include <vector>

// Profiling control: only profile worker_0 with gperftools
#ifdef PROFILE_MODE
#include <gperftools/profiler.h>

namespace {
void start_profiling(int worker_id) {
  if (worker_id == 0) {
    if (ProfilerStart("lob.prof")) {
      Logger::log("profile", "Started");
    } else {
      Logger::log("profile", "ERROR: Failed to start!");
    }
  }
}

void stop_profiling(int worker_id) {
  if (worker_id == 0) {
    ProfilerStop();
    Logger::log("profile", "Stopped");
  }
}
} // namespace
#endif

void sequential_worker(SharedData &data,
                       int worker_id,
                       GlobalFeatureStore *feature_store,
                       misc::ProgressHandle progress_handle) {

  // Initialize as idle (will be updated if assets are assigned)
  progress_handle.set_label("Idle");
  progress_handle.update(1, 1, "");

  // Find assets assigned to this worker
  std::vector<size_t> my_asset_ids;
  size_t total_orders = 0;
  for (size_t i = 0; i < data.asset.items.size(); ++i) {
    if (data.asset.items[i].assigned_worker_id == worker_id) {
      my_asset_ids.push_back(i);
      // Count orders only in current date range (backtest period)
      for (const auto &date : data.asset.all_dates) {
        auto it = data.asset.items[i].date_info.find(date);
        if (it != data.asset.items[i].date_info.end()) {
          total_orders += it->second.order_count;
        }
      }
    }
  }

  // Initialize LOBs and decoders for each asset
  std::vector<std::unique_ptr<LimitOrderBook>> lobs;
  std::vector<std::unique_ptr<L2::BinaryDecoder_L2>> decoders;

  for (size_t i = 0; i < my_asset_ids.size(); ++i) {
    const size_t asset_id = my_asset_ids[i];
    const auto &asset = data.asset.items[asset_id];
    lobs.push_back(std::make_unique<LimitOrderBook>(L2::DEFAULT_ENCODER_ORDER_SIZE, feature_store, asset.exchange_type, asset.asset_id, worker_id));
    decoders.push_back(std::make_unique<L2::BinaryDecoder_L2>(L2::DEFAULT_ENCODER_SNAPSHOT_SIZE, L2::DEFAULT_ENCODER_ORDER_SIZE));
  }

  Logger::log("worker_" + std::to_string(worker_id), "Started: " + std::to_string(my_asset_ids.size()) + " assets, " +
                                                         std::to_string(data.asset.all_dates.size()) + " dates, " +
                                                         std::to_string(total_orders) + " total orders");

#ifdef PROFILE_MODE
  // Start profiling: only worker_0
  start_profiling(worker_id);
#endif

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
    const std::string &date_str = data.asset.all_dates[date_idx];
    size_t date_orders = 0;
    size_t date_assets_processed = 0;

    // Process each asset at this date
    for (size_t i = 0; i < my_asset_ids.size(); ++i) {
      const size_t asset_id = my_asset_ids[i];
      const auto &asset = data.asset.items[asset_id];
      auto it = asset.date_info.find(date_str);
      lobs[i]->set_current_date(date_str);
      // Hot path: has data and binaries
      if (it != asset.date_info.end() && it->second.has_binaries() && !it->second.orders_file.empty()) [[likely]] {

        size_t order_num = 0;
        const L2::Order *orders = decoders[i]->decode_orders_stream(it->second.orders_file, order_num);

        if (orders != nullptr) [[likely]] {
          // Batch processing: zero-overhead inlined loop (process_impl inlined into process_batch)
          size_t order_invalid_cnt = lobs[i]->process_batch(orders, order_num);

          if (order_invalid_cnt > 100) {
            Logger::log("worker_" + std::to_string(worker_id), "ERROR: " + date_str + " asset_id=" + std::to_string(asset_id) + " order_invalid=" + std::to_string(order_invalid_cnt));
            std::exit(1);
          }

          if (order_num > 0) {
            Logger::log("worker_" + std::to_string(worker_id),
                        date_str + " asset_id=" + std::to_string(asset_id) +
                            " decoded=" + std::to_string(order_num) +
                            " order_invalid=" + std::to_string(order_invalid_cnt) +
                            " tob_invalid=" + std::to_string(lobs[i]->get_tob_invalid_count()) +
                            " tob_refresh=" + std::to_string(lobs[i]->get_tob_refresh_count()));
          }

          lobs[i]->clear();
          date_orders += order_num;
          date_assets_processed++;
          cumulative_orders += order_num;
        } else {
          Logger::log("worker_" + std::to_string(worker_id), "WARNING: " + date_str + " failed to decode " + it->second.orders_file);
        }
      }
    }

    if (date_assets_processed > 0) {
      Logger::log("worker_" + std::to_string(worker_id), date_str + " completed: " + std::to_string(date_assets_processed) + " assets, " + std::to_string(date_orders) + " orders");
    }

    // Mark this worker done for this date (will also set all asset progress atomically)
    feature_store->ts_done(date_str, worker_id);

    // Update progress
    auto current_time = std::chrono::steady_clock::now();
    double elapsed_seconds = std::chrono::duration<double>(current_time - start_time).count();
    double speed_M_per_sec = (elapsed_seconds > 0) ? (cumulative_orders / 1e6) / elapsed_seconds : 0.0;

    char msg_buf[128];
    snprintf(msg_buf, sizeof(msg_buf), "%s [%.1fM/s (%.1fM)]", date_str.c_str(), speed_M_per_sec, total_orders / 1e6);
    progress_handle.update(date_idx + 1, data.asset.all_dates.size(), msg_buf);
  }

  Logger::log("worker_" + std::to_string(worker_id), "Completed: processed " + std::to_string(cumulative_orders) + " orders across " + std::to_string(data.asset.all_dates.size()) + " dates");

#ifdef PROFILE_MODE
  // Stop profiling: only worker_0
  stop_profiling(worker_id);
#endif
}
