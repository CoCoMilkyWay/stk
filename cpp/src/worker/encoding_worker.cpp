#include "worker/encoding_worker.hpp"
#include "worker/shared_state.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "misc/affinity.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>

namespace Config {
extern const char *ARCHIVE_EXTENSION;
extern const char *ARCHIVE_TOOL;
extern const char *ARCHIVE_EXTRACT_CMD;
extern const char *BINARY_EXTENSION;
} // namespace Config

// ============================================================================
// RAR LOCK MANAGER
// ============================================================================

class RarLockManager {
  inline static std::mutex map_mutex;
  inline static std::unordered_map<std::string, std::unique_ptr<std::mutex>> locks;

public:
  static std::mutex *get_or_create_lock(const std::string &archive_path) {
    std::lock_guard<std::mutex> lock(map_mutex);
    if (locks.find(archive_path) == locks.end()) {
      locks[archive_path] = std::make_unique<std::mutex>();
    }
    return locks[archive_path].get();
  }
};

// ============================================================================
// ENCODING HELPER
// ============================================================================

static bool extract_and_encode(const std::string &archive_path,
                               const std::string &asset_code,
                               const std::string &date_str,
                               const std::string &database_dir,
                               L2::BinaryEncoder_L2 &encoder,
                               AssetInfo::DateInfo &date_info) {
  // Acquire lock (blocking)
  std::mutex *archive_lock = RarLockManager::get_or_create_lock(archive_path);
  std::lock_guard<std::mutex> lock(*archive_lock);

  // Extract from archive
  const std::string temp_extract_dir = database_dir + "/tmp_" + asset_code;
  std::filesystem::create_directories(temp_extract_dir);

  const std::string archive_name = std::filesystem::path(archive_path).stem().string();
  const std::string asset_path_in_archive = archive_name + "/" + asset_code + "/*";

  // Use unrar to extract
  std::string command = std::string(Config::ARCHIVE_TOOL) + " " +
                        std::string(Config::ARCHIVE_EXTRACT_CMD) + " \"" +
                        archive_path + "\" \"" + asset_path_in_archive + "\" \"" +
                        temp_extract_dir + "/\" -y > /dev/null 2>&1";

  if (std::system(command.c_str()) != 0) {
    std::filesystem::remove_all(temp_extract_dir);
    return false;
  }

  const std::string extracted_dir = temp_extract_dir + "/" + date_str + "/" + asset_code;
  if (!std::filesystem::exists(extracted_dir)) {
    std::filesystem::remove_all(temp_extract_dir);
    return false;
  }

  // Move to final location
  std::filesystem::create_directories(std::filesystem::path(date_info.database_dir).parent_path());

  // Remove target if exists to avoid rename failure
  if (std::filesystem::exists(date_info.database_dir)) {
    std::filesystem::remove_all(date_info.database_dir);
  }

  std::filesystem::rename(extracted_dir, date_info.database_dir);
  std::filesystem::remove_all(temp_extract_dir);

  // Parse and encode
  std::vector<L2::Snapshot> snapshots;
  std::vector<L2::Order> orders;
  if (!encoder.process_stock_data(date_info.database_dir, date_info.database_dir, asset_code, &snapshots, &orders)) {
    return false;
  }

  // Record order count
  date_info.order_count = orders.size();

  // Scan for generated binary files
  for (const auto &entry : std::filesystem::directory_iterator(date_info.database_dir)) {
    const std::string path = entry.path().string();
    if (path.ends_with(".csv")) {
      std::filesystem::remove(entry.path()); // Clean up CSV
    } else if (path.ends_with(Config::BINARY_EXTENSION)) {
      const std::string filename = entry.path().filename().string();
      if (filename.starts_with(asset_code + "_snapshots_")) {
        date_info.snapshots_file = path;
      } else if (filename.starts_with(asset_code + "_orders_")) {
        date_info.orders_file = path;
      }
    }
  }

  return true;
}

void encoding_worker(SharedState &state,
                     std::vector<size_t> &asset_id_queue,
                     std::mutex &queue_mutex,
                     const std::string &l2_archive_base,
                     const std::string &database_dir,
                     unsigned int core_id,
                     misc::ProgressHandle progress_handle) {
  static thread_local bool affinity_set = false;
  if (!affinity_set && misc::Affinity::supported()) {
    affinity_set = misc::Affinity::pin_to_core(core_id);
  }

  L2::BinaryEncoder_L2 encoder(L2::DEFAULT_ENCODER_SNAPSHOT_SIZE, L2::DEFAULT_ENCODER_ORDER_SIZE);

  // Initialize as idle (in case no asset is assigned)
  progress_handle.set_label("Idle");
  progress_handle.update(1, 1, "");

  while (true) {
    size_t asset_id;

    // Get an asset
    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      if (asset_id_queue.empty()) {
        break;
      }
      asset_id = asset_id_queue.back();
      asset_id_queue.pop_back();
    }

    AssetInfo &asset = state.assets[asset_id];
    progress_handle.set_label(asset.asset_code + " (" + asset.asset_name + ")");

    // Collect and shuffle dates to spread RAR access
    std::vector<std::string> date_keys;
    date_keys.reserve(asset.date_info.size());
    for (const auto &[date_str, _] : asset.date_info) {
      date_keys.push_back(date_str);
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(date_keys.begin(), date_keys.end(), g);

    // Process all dates for this asset
    for (size_t i = 0; i < date_keys.size(); ++i) {
      const std::string &date_str = date_keys[i];
      auto &date_info = asset.date_info[date_str];

      // Skip if binary already exists
      if (date_info.encoded) {
        progress_handle.update(i + 1, date_keys.size(), date_str);
        continue;
      }

      // Check archive exists before attempting extraction
      const std::string archive_path = Utils::generate_archive_path(l2_archive_base, date_str);
      if (!std::filesystem::exists(archive_path)) {
        progress_handle.update(i + 1, date_keys.size(), date_str);
        continue;
      }

      // Encode (blocking on RAR lock if needed)
      bool success = extract_and_encode(archive_path, asset.asset_code, date_str, database_dir, encoder, date_info);

      if (success) {
        date_info.encoded = 1;
      }

      progress_handle.update(i + 1, date_keys.size(), date_str);
    }
  }
}
