#include "worker/sequential_worker.hpp"
#include "worker/shared_state.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/binary_decoder_L2.hpp"
#include "features/backend/FeatureStore.hpp"
#include "lob/LimitOrderBook.hpp"
#include "misc/logging.hpp"

#include <chrono>
#include <cstdio>
#include <memory>
#include <vector>

void sequential_worker(const SharedState &state,
                       int worker_id,
                       GlobalFeatureStore *feature_store,
                       misc::ProgressHandle progress_handle) {

  // Initialize as idle (will be updated if assets are assigned)
  progress_handle.set_label("Idle");
  progress_handle.update(1, 1, "");

  // Find assets assigned to this worker
  std::vector<size_t> my_asset_ids;
  size_t total_orders = 0;
  for (size_t i = 0; i < state.assets.size(); ++i) {
    if (state.assets[i].assigned_worker_id == worker_id) {
      my_asset_ids.push_back(i);
      total_orders += state.assets[i].get_total_order_count();
    }
  }

  // Initialize LOBs and decoders for each asset
  std::vector<std::unique_ptr<LimitOrderBook>> lobs;
  std::vector<std::unique_ptr<L2::BinaryDecoder_L2>> decoders;

  for (size_t i = 0; i < my_asset_ids.size(); ++i) {
    const size_t asset_id = my_asset_ids[i];
    const auto &asset = state.assets[asset_id];
    lobs.push_back(std::make_unique<LimitOrderBook>(L2::DEFAULT_ENCODER_ORDER_SIZE, feature_store, asset.exchange_type, asset.asset_id, worker_id));
    decoders.push_back(std::make_unique<L2::BinaryDecoder_L2>(L2::DEFAULT_ENCODER_SNAPSHOT_SIZE, L2::DEFAULT_ENCODER_ORDER_SIZE));
  }

  Logger::log_worker(worker_id, "Started: " + std::to_string(my_asset_ids.size()) + " assets, " +
                                    std::to_string(state.all_dates.size()) + " dates, " +
                                    std::to_string(total_orders) + " total orders");

  // Progress label
  char label_buf[128];
  if (!my_asset_ids.empty()) {
    snprintf(label_buf, sizeof(label_buf), "时序核心%2d: %2zu Assets: %s(%s)",
             worker_id,
             my_asset_ids.size(),
             state.assets[my_asset_ids[0]].asset_code.c_str(),
             state.assets[my_asset_ids[0]].asset_name.c_str());
  } else {
    snprintf(label_buf, sizeof(label_buf), "时序核心%2d: Idle", worker_id);
  }
  progress_handle.set_label(label_buf);

  size_t cumulative_orders = 0;
  auto start_time = std::chrono::steady_clock::now();

  // Preallocate decoded_orders vector once (zero-overhead reuse across all dates/assets)
  // CRITICAL: Use resize() not reserve() - decode_orders writes directly to data()
  // Using reserve() alone would leave size=0, making data() access undefined behavior
  std::vector<L2::Order> decoded_orders;
  decoded_orders.resize(1000000); // Size once at init, never resize again (max capacity)

  // Date-first traversal
  for (size_t date_idx = 0; date_idx < state.all_dates.size(); ++date_idx) {
    const std::string &date_str = state.all_dates[date_idx];
    size_t date_orders = 0;
    size_t date_assets_processed = 0;

    // Process each asset at this date
    for (size_t i = 0; i < my_asset_ids.size(); ++i) {
      const size_t asset_id = my_asset_ids[i];
      const auto &asset = state.assets[asset_id];

      // Check if this asset has data for this date
      auto it = asset.date_info.find(date_str);
      if (it == asset.date_info.end()) {
        const size_t capacity = feature_store->query_T(0);
        feature_store->ts_mark_progress(date_str, worker_id, asset_id, capacity - 1);
        continue;
      }

      const auto &date_info = it->second;

      // Set date for feature computation
      lobs[i]->set_current_date(date_str);

      if (date_info.has_binaries()) {
        size_t order_num = 0;
        if (!date_info.orders_file.empty()) {
          // decode_orders fills the preallocated vector without resizing, returns actual count
          if (decoders[i]->decode_orders(date_info.orders_file, decoded_orders, order_num)) {

            // Sanity check: order_num should be reasonable
            assert(order_num <= decoded_orders.size() && "order_num exceeds vector size");

            // Process only the actual orders (0 to order_num-1)
            size_t invalid_count = 0;
            for (size_t ord_idx = 0; ord_idx < order_num; ++ord_idx) {
              const auto &order = decoded_orders[ord_idx];

              bool success = lobs[i]->process(order);
              if (!success) [[unlikely]] {
                ++invalid_count;
              }
            }
            if (invalid_count > 100) {
              Logger::log_worker(worker_id, "ERROR: " + date_str + " asset_id=" + std::to_string(asset_id) + " skipped " + std::to_string(invalid_count) + " invalid orders");
              std::exit(1);
            }

            Logger::log_worker(worker_id, date_str + " asset_id=" + std::to_string(asset_id) + " decoded=" + std::to_string(order_num) + (invalid_count > 0 ? " invalid=" + std::to_string(invalid_count) : ""));

            lobs[i]->clear();
            date_orders += order_num;
            date_assets_processed++;

          } else {
            Logger::log_worker(worker_id, "WARNING: " + date_str + " failed to decode " + date_info.orders_file);
          }
        }
        cumulative_orders += order_num;
      }
      const size_t capacity = feature_store->query_T(0);
      feature_store->ts_mark_progress(date_str, worker_id, asset_id, capacity - 1);
    }

    if (date_assets_processed > 0) {
      Logger::log_worker(worker_id, date_str + " completed: " + std::to_string(date_assets_processed) + " assets, " + std::to_string(date_orders) + " orders");
    }

    // Mark this core as done for this date (for CS sync)
    // Even if this core had no data, CS worker needs to know this core is done
    // By marking the last timeslot, all previous timeslots are implicitly marked as done
    {
      // L0: Update progress to last timeslot
      feature_store->ts_mark_done(date_str, worker_id);
    }

    // Update progress
    auto current_time = std::chrono::steady_clock::now();
    double elapsed_seconds = std::chrono::duration<double>(current_time - start_time).count();
    double speed_M_per_sec = (elapsed_seconds > 0) ? (cumulative_orders / 1e6) / elapsed_seconds : 0.0;

    char msg_buf[128];
    snprintf(msg_buf, sizeof(msg_buf), "%s [%.1fM/s (%.1fM)]", date_str.c_str(), speed_M_per_sec, total_orders / 1e6);
    progress_handle.update(date_idx + 1, state.all_dates.size(), msg_buf);
  }

  Logger::log_worker(worker_id, "Completed: processed " + std::to_string(cumulative_orders) + " orders across " + std::to_string(state.all_dates.size()) + " dates");
}
