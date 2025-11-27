#include "worker/encoding_worker.hpp"
#include "shared/SharedData.hpp"

#include "codec/L2_DataType.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "misc/affinity.hpp"
#include "misc/logging.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>

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
                               const std::string &asset_exchange,
                               const std::string &date_str,
                               const std::string &database_dir,
                               const std::string &archive_tool,
                               const std::string &archive_extract_cmd,
                               const std::string &binary_extension,
                               L2::BinaryEncoder_L2 &encoder,
                               DateInfo &date_info) {
  namespace fs = std::filesystem;

  // Lock archive for extraction
  std::lock_guard<std::mutex> lock(*RarLockManager::get_or_create_lock(archive_path));

  // Extract to temp dir
  const std::string asset_full = asset_code + "." + asset_exchange; // e.g. "603269.SH"
  const std::string temp_dir = database_dir + "/tmp_" + asset_code;
  fs::create_directories(temp_dir);

  const std::string archive_name = fs::path(archive_path).stem().string();
  const std::string asset_path_in_archive = archive_name + "/" + asset_full + "/*";
  const std::string command = archive_tool + " " + archive_extract_cmd + " \"" + archive_path +
                              "\" \"" + asset_path_in_archive + "\" \"" +
                              temp_dir + "/\" -y > /dev/null 2>&1";

  if (std::system(command.c_str()) != 0) {
    fs::remove_all(temp_dir);
    return false;
  }

  // Move extracted data to final location
  const std::string extracted_dir = temp_dir + "/" + date_str + "/" + asset_full;
  if (!fs::exists(extracted_dir)) {
    fs::remove_all(temp_dir);
    return false;
  }

  fs::create_directories(fs::path(date_info.database_dir).parent_path());
  if (fs::exists(date_info.database_dir)) {
    fs::remove_all(date_info.database_dir);
  }
  fs::rename(extracted_dir, date_info.database_dir);
  fs::remove_all(temp_dir);

  // Encode CSV to binary
  std::vector<L2::Snapshot> snapshots;
  std::vector<L2::Order> orders;
  if (!encoder.process_stock_data(date_info.database_dir, date_info.database_dir,
                                  asset_full, &snapshots, &orders)) {
    return false;
  }

  date_info.order_count = orders.size();

  // Scan for binary files and clean up CSV
  for (const auto &entry : fs::directory_iterator(date_info.database_dir)) {
    const std::string path = entry.path().string();
    const std::string filename = entry.path().filename().string();

    if (path.ends_with(".csv")) {
      fs::remove(entry.path());
    } else if (path.ends_with(binary_extension)) {
      if (filename.starts_with(asset_full + "_snapshots_")) {
        date_info.snapshots_file = path;
      } else if (filename.starts_with(asset_full + "_orders_")) {
        date_info.orders_file = path;
      }
    }
  }

  return true;
}

void encoding_worker(SharedData &data,
                     std::vector<size_t> &asset_id_queue,
                     std::mutex &queue_mutex,
                     std::atomic<bool> *cancel_flag,
                     unsigned int worker_id,
                     misc::ProgressHandle progress_handle) {
  // Pin to core
  static thread_local bool affinity_set = false;
  if (!affinity_set && misc::Affinity::supported()) {
    const unsigned int core_count = misc::Affinity::core_count();
    affinity_set = misc::Affinity::pin_to_core(core_count > 0 ? worker_id % core_count : 0);
  }

  L2::BinaryEncoder_L2 encoder(L2::DEFAULT_ENCODER_SNAPSHOT_SIZE, L2::DEFAULT_ENCODER_ORDER_SIZE);
  progress_handle.set_label("Idle");
  progress_handle.update(1, 1, "");

  Logger::log("encoding", "[Worker " + std::to_string(worker_id) + "] Started");

  while (!cancel_flag->load()) {
    // Get next asset
    size_t asset_id;
    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      if (asset_id_queue.empty())
        break;
      asset_id = asset_id_queue.back();
      asset_id_queue.pop_back();
    }

    AssetItem &asset = data.asset.items[asset_id];
    progress_handle.set_label(asset.asset_code + " (" + asset.asset_name + ")");

    // Shuffle dates to spread RAR contention
    std::vector<std::string> date_keys;
    date_keys.reserve(asset.date_info.size());
    for (const auto &[date_str, _] : asset.date_info) {
      date_keys.push_back(date_str);
    }
    std::random_device rd;
    std::shuffle(date_keys.begin(), date_keys.end(), std::mt19937{rd()});

    // Process dates
    for (size_t i = 0; i < date_keys.size() && !cancel_flag->load(); ++i) {
      const std::string &date_str = date_keys[i];
      auto &date_info = asset.date_info[date_str];

      progress_handle.update(i + 1, date_keys.size(), date_str);

      // Skip if already encoded
      if (date_info.snapshots_encoded && date_info.orders_encoded)
        continue;

      // Check archive exists
      const std::string archive_path = Utils::generate_archive_path(
          data.config.archive_dir, date_str, data.config.archive_extension);
      if (!std::filesystem::exists(archive_path))
        continue;

      // Extract and encode
      if (extract_and_encode(archive_path, asset.asset_code, asset.exchange, date_str, data.config.database_dir,
                             data.config.archive_tool, data.config.archive_extract_cmd,
                             data.config.binary_extension, encoder, date_info)) {
        date_info.snapshots_encoded = 1;
        date_info.orders_encoded = 1;
      } else {
        Logger::log("encoding", "[Worker " + std::to_string(worker_id) + "] [FAILED] " + date_str + " " +
                                    asset.asset_code + "." + asset.exchange);
      }
    }
  }
}
