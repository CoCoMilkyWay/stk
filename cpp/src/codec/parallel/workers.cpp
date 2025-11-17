/*
 * PARALLEL PROCESSING WORKERS - SIMPLE, ROBUST, CLOSEABLE QUEUE + RAII
 *
 * =============================================================================
 * CORE PRINCIPLE: CLOSEABLE BOUNDED QUEUE + RAII FOLDER OWNERSHIP + SINGLE-ACTIVE-FOLDER
 * =============================================================================
 *
 * Decompressors produce per-folder work items. Encoders operate cooperatively on one active folder.
 * All encoders work the same folder together; no encoder starts the next folder until the active
 * folder is fully processed and cleaned up.
 * Lifecycle and backpressure are handled by a closeable queue and a counting semaphore.
 *
 * GLOBAL STATE (MINIMAL, CLEAR LIFECYCLES):
 * - temp_slots (std::counting_semaphore)
 *     Limits the number of concurrent temp folders on disk. Acquire before creating a temp folder,
 *     release automatically via RAII when a folder finishes or is discarded.
 * - folder_queue (bounded closeable MPMC)
 *     Blocking push/pop with close() semantics. When closed and drained, consumers exit.
 * - No producers_done flag
 *     Completion is signaled by closing the queue, eliminating timing races.
 * - Optional global progress (simple atomics) if needed for aggregate display; otherwise per-folder
 *     progress is computed locally by the owning encoder and printed directly.
 * - active_folder (shared state)
 *     Tracks the currently active folder: {date_str, temp_root, asset_list, folder_token,
 *     std::atomic<int> processed, int total, std::atomic<int> next_asset_index}.
 *     Only one active folder exists at a time. Encoders cooperate by grabbing work with
 *     next_asset_index.fetch_add(1) over asset_list; spare encoders idle when indices exhaust.
 *
 * WORK ITEM FORMAT (FOLDER OWNERSHIP VIA RAII):
 *   {folder_id, date_str, asset_list, folder_token}
 *   where:
 *     - asset_list = [{asset_dir, asset_code}, ...]
 *     - folder_token owns temp_root and releases temp_slots on destruction
 *
 * =============================================================================
 * DECOMPRESSION WORKER LOOP (PRODUCER):
 * =============================================================================
 *
 * STEP 1: ACQUIRE DISK SLOT (BLOCKING)
 *   - temp_slots.acquire() before creating any temp folder.
 *
 * STEP 2: TAKE NEXT ARCHIVE AND CREATE FOLDER TOKEN
 *   - Pop next archive from archive_queue (brief mutex lock).
 *   - Create unique temp_root for this archive (per-day folder).
 *   - Construct folder_token(temp_root, temp_slots) that will delete temp_root and release a slot
 *     when destroyed.
 *
 * STEP 3: DECOMPRESS
 *   - Extract archive into temp_root (may contain nested assets directory).
 *
 * STEP 4: DISCOVER ASSETS AND BUILD ASSET LIST
 *   - Enumerate asset directories under the day folder.
 *   - Build complete asset_list = [{asset_dir, asset_code}, ...].
 *
 * STEP 5: ENQUEUE ONE COMPLETE FOLDER (BLOCKING PUSH)
 *   - If asset_list.empty(): let folder_token go out of scope (deletes temp_root, releases slot).
 *   - Else push {folder_id, date_str, asset_list, std::move(folder_token)} into folder_queue.
 *     Ownership of temp_root moves to the consumer via the token.
 *
 * STEP 6: LOOP & FINALIZE
 *   - Repeat until no archives remain. After all producers finish enqueuing, close folder_queue.
 *
 * =============================================================================
 * ENCODING WORKER LOOP (CONSUMER):
 * =============================================================================
 *
 * STEP 1: ACTIVATE NEXT FOLDER (BLOCKING)
 *   - If no active folder is present, one encoder pops a folder from folder_queue and publishes it
 *     as the active folder, initializing processed=0, total=asset_list.size(), next_asset_index=0.
 *   - If the queue is closed and empty (and there is no active folder), exit worker.
 *
 * STEP 2: COOPERATIVE ASSET PROCESSING WITHIN ACTIVE FOLDER
 *   - Encoders claim work via i = next_asset_index.fetch_add(1). If i >= total, they wait/idle;
 *     they do not fetch a new folder.
 *   - After each processed asset: processed++ and print per-folder progress (processed/total):
 *     use function print_progress
 *     "[=====>] 14% (430/3033) 20170104 002789.SZ (2.5x)".
 *
 * STEP 3: CLEANUP AUTOMATIC VIA RAII
 *   - When processed == total for the active folder, end the active folder scope: the folder_token
 *     deletes temp_root and releases the disk slot back to temp_slots. Encoders then activate the
 *     next folder (if any).
 *
 * STEP 4: LOOP
 *   - Continue until the queue is closed and drained.
 *
 * =============================================================================
 * WHY THIS DESIGN IS SIMPLE & ROBUST:
 * =============================================================================
 *
 * 1. MINIMAL SHARED STATE: Closeable queue + semaphore; no producers_done races.
 * 2. CLEAR LIFECYCLE: Folder ownership is explicit via RAII; cleanup cannot be leaked.
 * 3. EXPLICIT BACKPRESSURE: Disk via temp_slots; CPU via bounded folder_queue.
 * 4. FOLDER ISOLATION: Each encoder owns one complete folder—no cross-folder coordination.
 * 5. EASY PROGRESS: Per-folder progress printed locally; optional simple global totals.
 * 6. BLOCKING SEMANTICS: No polling sleeps needed for steady-state operation.
 * 7. STRICT FOLDER SEQUENCING ACROSS FOLDERS: Only one folder is active at a time; within that
 *    folder, encoders cooperate for parallelism. No encoder advances to the next folder early.
 *
 * MEMORY USAGE: At most MAX_TEMP_FOLDERS temp folders on disk (via semaphore).
 * PERFORMANCE: Folder-level parallelism with minimal contention (queue + semaphore).
 * ROBUSTNESS: If a worker dies, ownership cleanup occurs via token destruction on scope exit.
 *
 * =============================================================================
 * FOLDER-LEVEL QUEUE DETAILS:
 * =============================================================================
 *
 * QUEUE SEMANTICS (CLOSEABLE MPMC):
 *   - Bounded to cap memory and encourage steady-state processing.
 *   - Producers push complete folder items; consumers pop independently.
 *   - queue.close() signals completion; consumers exit when closed and empty.
 *
 * FOLDER PROGRESS & DISPLAY (LOCAL):
 *   - total = asset_list.size()
 *   - processed increments after each completed asset
 *   - percent = (processed * 100) / total
 *   - Example: "[=====>                                  ]  14% (430/3033) 20170104 002789.SZ (2.5x)"
 *
 * COMPLETION CONDITION:
 *   - A folder is complete when processed == total for the active folder.
 *   - All encoders exit when folder_queue is closed and empty and no active folder exists.
 *
 * =============================================================================
 * GRACEFUL SHUTDOWN:
 * =============================================================================
 *
 * - SIGINT/SIGTERM set g_shutdown_requested = true (atomic bool).
 * - Producers stop producing new work and close folder_queue when done.
 * - Consumers finish the current folder and exit when the queue is closed and drained.
 * - Temp folder cleanup is automatic via folder_token RAII; no partial folder state.
 */

#include "codec/parallel/workers.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "codec/L2_DataType.hpp"
#include "codec/parallel/processing_types.hpp"
#include "misc/affinity.hpp"
#include "misc/progress_single.hpp"
#include "misc/logging.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>

// External shutdown flag
extern std::atomic<bool> g_shutdown_requested;

namespace L2 {
namespace Parallel {



bool process_stock_data(const std::string &asset_dir,
                        const std::string &asset_code,
                        const std::string &date_str,
                        const std::string &OUTPUT_DIR,
                        double &compression_ratio) {
  if (!std::filesystem::exists(asset_dir)) {
    return false;
  }

  // Create output directory structure
  std::string output_dir = OUTPUT_DIR + "/" + date_str.substr(0, 4) + "/" +
                           date_str.substr(4, 2) + "/" + date_str.substr(6, 2) + "/" + asset_code;
  std::filesystem::create_directories(output_dir);

  // Create encoder instance with optimized buffer sizes
  L2::BinaryEncoder_L2 encoder(500000, 2000000);

  std::vector<L2::Snapshot> snapshots;
  std::vector<L2::Order> all_orders;

  // Process snapshots from 行情.csv
  std::string snapshot_file = asset_dir + "/行情.csv";
  if (std::filesystem::exists(snapshot_file)) {
    std::vector<L2::CSVSnapshot> csv_snapshots;
    if (encoder.parse_snapshot_csv(snapshot_file, csv_snapshots)) {
      snapshots.reserve(csv_snapshots.size());
      for (const auto &csv_snap : csv_snapshots) {
        snapshots.emplace_back(L2::BinaryEncoder_L2::csv_to_snapshot(csv_snap));
      }
    }
  }

  // // Asset must have snapshots (market data) - fail immediately if missing
  // if (snapshots.empty()) {
  //   std::cerr << "[ERROR] " << asset_code << " (" << date_str << ") - no snapshot data found, terminating" << std::endl;
  //   std::exit(1);
  // }

  // Process orders from 逐笔委托.csv
  std::string order_file = asset_dir + "/逐笔委托.csv";
  if (std::filesystem::exists(order_file)) {
    std::vector<L2::CSVOrder> csv_orders;
    if (encoder.parse_order_csv(order_file, csv_orders)) {
      all_orders.reserve(csv_orders.size());
      for (const auto &csv_order : csv_orders) {
        all_orders.emplace_back(L2::BinaryEncoder_L2::csv_to_order(csv_order));
      }
    }
  }

  // Process trades from 逐笔成交.csv
  std::string trade_file = asset_dir + "/逐笔成交.csv";
  if (std::filesystem::exists(trade_file)) {
    std::vector<L2::CSVTrade> csv_trades;
    if (encoder.parse_trade_csv(trade_file, csv_trades)) {
      all_orders.reserve(all_orders.size() + csv_trades.size());
      for (const auto &csv_trade : csv_trades) {
        all_orders.emplace_back(L2::BinaryEncoder_L2::csv_to_trade(csv_trade));
      }
    }
  }

  // Sort orders by time if any exist
  if (!all_orders.empty()) {
    std::sort(all_orders.begin(), all_orders.end(), [](const L2::Order &a, const L2::Order &b) {
      if (a.hour != b.hour)
        return a.hour < b.hour;
      if (a.minute != b.minute)
        return a.minute < b.minute;
      if (a.second != b.second)
        return a.second < b.second;
      return a.millisecond < b.millisecond;
    });
  }

  // Encode snapshots (always present)
  encoder.encode_snapshots(snapshots, output_dir + "/snapshots_" + std::to_string(snapshots.size()) + ".bin");
  compression_ratio = encoder.get_compression_stats().ratio;

  // Encode orders if any exist
  if (!all_orders.empty()) {
    encoder.encode_orders(all_orders, output_dir + "/orders_" + std::to_string(all_orders.size()) + ".bin");
    compression_ratio = encoder.get_compression_stats().ratio;
  }

  return true;
}

bool decompress_7z(const std::string &archive_path, const std::string &output_dir) {
  if (!std::filesystem::exists(archive_path)) {
    std::cerr << "[ERROR] Archive not found: " << archive_path << std::endl;
    return false;
  }

  std::filesystem::create_directories(output_dir);

  // Use 7z decompression with hidden output
  std::string command = "7z x \"" + archive_path + "\" -o\"" + output_dir + "\" -y > /dev/null 2>&1";
  int result = std::system(command.c_str());
  if (result != 0) {
    std::cerr << "[ERROR] Failed to decompress: " << archive_path << " (exit code: " << result << ")" << std::endl;
    return false;
  }
  return true;
}

void decompression_worker(unsigned int worker_id) {
  Logger::log("decompression", "Worker " + std::to_string(worker_id) + " Started");

  while (!g_shutdown_requested.load()) {
    // STEP 2: TAKE NEXT ARCHIVE
    std::string archive_path;
    {
      std::lock_guard<std::mutex> lock(archive_queue_mutex);
      if (archive_queue.empty()) {
        break; // no more archives
      }
      archive_path = archive_queue.front();
      archive_queue.pop();
    }

    // STEP 1: ACQUIRE DISK SLOT (BLOCKING) - moved after archive selection
    temp_slots.acquire();

    // Create unique temp_root for this archive (per-day folder)
    std::string archive_name = std::filesystem::path(archive_path).stem().string();
    std::string temp_root = std::string(L2::TEMP_DIR) + "/" + archive_name + "_" +
                            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

    // Construct folder_token early for RAII (will cleanup on any failure)
    FolderToken folder_token(temp_root);

    Logger::log("decompression", "Worker " + std::to_string(worker_id) + " Processing: " + archive_name);

    // STEP 3: DECOMPRESS
    if (!decompress_7z(archive_path, temp_root)) {
      std::cerr << "[ERROR] Failed to decompress: " << archive_path << std::endl;
      continue; // folder_token destructor will cleanup
    }

    // STEP 4: DISCOVER ASSETS AND BUILD ASSET LIST
    std::string assets_folder = temp_root;
    std::string nested_assets = temp_root + "/" + archive_name;
    if (std::filesystem::exists(nested_assets)) {
      assets_folder = nested_assets;
    }

    if (!std::filesystem::exists(assets_folder)) {
      std::cerr << "[ERROR] No assets found in " << temp_root << " after decompression" << std::endl;
      continue; // folder_token destructor will cleanup
    }

    // Build complete asset_list
    std::vector<AssetInfo> asset_list;
    try {
      for (const auto &asset_entry : std::filesystem::directory_iterator(assets_folder)) {
        if (asset_entry.is_directory()) {
          std::string asset_code = asset_entry.path().filename().string();
          if (L2::is_valid_market_asset(asset_code)) {
            asset_list.emplace_back(asset_entry.path().string(), asset_code);
          }
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "[ERROR] Failed to scan assets in " << assets_folder << ": " << e.what() << std::endl;
      continue; // folder_token destructor will cleanup
    }

    // STEP 5: ENQUEUE ONE COMPLETE FOLDER (BLOCKING PUSH)
    if (asset_list.empty()) {
      continue; // folder_token destructor will cleanup
    }

    size_t asset_count = asset_list.size();
    FolderWorkItem folder_item(archive_name, archive_name, std::move(asset_list), std::move(folder_token));

    // If queue is full or closed, poll/sleep briefly until space or give up
    while (!folder_queue.try_push(std::move(folder_item)) && !g_shutdown_requested.load()) {
      if (folder_queue.is_closed()) {
        // Queue closed, cleanup and exit
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    Logger::log("decompression", "Worker " + std::to_string(worker_id) + " Completed: " + archive_name +
               " (" + std::to_string(asset_count) + " assets)");
  }

  Logger::log("decompression", "Worker " + std::to_string(worker_id) + " Finished");
}

void encoding_worker(unsigned int core_id) {
  // Set thread affinity
  if (misc::Affinity::supported()) {
    misc::Affinity::pin_to_core(core_id);
  }

  while (!g_shutdown_requested.load()) {
    // STEP 1: ACTIVATE NEXT FOLDER (BLOCKING)
    bool folder_activated = false;
    {
      std::lock_guard<std::mutex> lock(active_folder_mutex);

      // If no active folder, try to pop from queue and activate it
      if (!active_folder) {
        auto folder_item_opt = folder_queue.try_pop();
        if (folder_item_opt.has_value()) {
          // Activate this folder
          auto &folder_item = folder_item_opt.value();
          active_folder = std::make_unique<ActiveFolder>(
              std::move(folder_item.date_str),
              std::move(folder_item.asset_list),
              std::move(folder_item.folder_token));
          folder_activated = true;
        } else if (folder_queue.is_closed_and_empty()) {
          break; // Exit worker
        }
      } else {
        folder_activated = true;
      }
    }

    if (!folder_activated) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    // STEP 2: COOPERATIVE ASSET PROCESSING WITHIN ACTIVE FOLDER
    int asset_index;
    std::string asset_dir, asset_code, date_str;
    bool has_work = false;
    bool folder_complete = false;

    {
      std::lock_guard<std::mutex> lock(active_folder_mutex);
      if (active_folder) {
        asset_index = active_folder->next_asset_index.fetch_add(1);
        if (asset_index < active_folder->total) {
          has_work = true;
          asset_dir = active_folder->asset_list[asset_index].asset_dir;
          asset_code = active_folder->asset_list[asset_index].asset_code;
          date_str = active_folder->date_str;
        } else {
          // Check if folder is complete
          folder_complete = (active_folder->processed.load() == active_folder->total);
          if (folder_complete) {
            active_folder.reset(); // This will trigger FolderToken destructor
          }
        }
      }
    }

    if (!has_work) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    // Process the asset
    double compression_ratio = 1.0;
    if (process_stock_data(asset_dir, asset_code, date_str, L2::OUTPUT_DIR, compression_ratio)) {
      // Update progress: processed++
      std::lock_guard<std::mutex> lock(active_folder_mutex);
      if (active_folder) {
        int processed_count = active_folder->processed.fetch_add(1) + 1;
        std::ostringstream message;
        message << date_str << " " << asset_code
                << " (" << std::fixed << std::setprecision(1) << compression_ratio << "x)";
        misc::print_progress(processed_count, active_folder->total, message.str());
      }
    }
  }
}


} // namespace Parallel
} // namespace L2
