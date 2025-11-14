#pragma once

#include "FeatureStoreConfig.hpp"
#include "misc/logging.hpp"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <vector>

// ============================================================================
// FEATURE STORE CONFIGURATION
// ============================================================================
// Control tensor flush strategy:
// - true:  Flush unified daily tensor [T_L0, F_total, A] (GPU-friendly, single file)
// - false: Flush separate level tensors [T_L0, F0, A], [T_L1, F1, A], [T_L2, F2, A]
#define STORE_UNIFIED_DAILY_TENSOR false

// ============================================================================
// FEATURE STORE - Single class interface
// ============================================================================
// Design: [T][F][A] layout for optimal CS operations
// Lockfree sync: per-TS-core progress tracking
// ============================================================================

class GlobalFeatureStore {
private:
  // Tensor lifecycle states
  enum class TensorState : uint8_t {
    UNUSED = 0,  // Not in use, ready to allocate
    IN_USE = 1,  // Allocated to a date, TS/CS processing ongoing
    CS_DONE = 2, // CS processing complete, ready for IO thread to flush
    FLUSHING = 3 // IO thread is flushing, will be UNUSED soon
  };

  // Per-date storage
  struct DayData {
    feature_storage_t *data[LEVEL_COUNT] = {nullptr}; // [level][T][F][A] stored as _Float16

    // Simple per-asset progress tracking
    std::atomic<size_t> *ts_progress = nullptr;          // [asset_id] - L0 time index progress per asset
    std::atomic<bool> *ts_done = nullptr;                // [core_id] - explicit done flag from TS worker
    std::atomic<TensorState> state{TensorState::IN_USE}; // Lifecycle state
    mutable size_t cs_safe_index = 0;                    // CS cached safe index (min of all ts_progress)

    void allocate(size_t num_assets, size_t num_ts_cores) {
      // Allocate feature data
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        const size_t T = MAX_ROWS_PER_LEVEL[lvl];
        const size_t F = FIELDS_PER_LEVEL[lvl];
        const size_t A = num_assets;

        if (!data[lvl]) {
          const size_t total_elements = T * F * A;
          const size_t total_bytes = total_elements * sizeof(feature_storage_t);
          const size_t aligned_bytes = ((total_bytes + 63) / 64) * 64;
          data[lvl] = static_cast<feature_storage_t *>(std::aligned_alloc(64, aligned_bytes));
          assert(data[lvl] && "aligned_alloc failed");
        }
      }

      // Allocate ts_progress array [asset_id]
      if (!ts_progress) {
        ts_progress = new std::atomic<size_t>[num_assets];
        for (size_t i = 0; i < num_assets; ++i) {
          ts_progress[i].store(0, std::memory_order_relaxed);
        }
      }

      // Allocate ts_done flags [core_id]
      if (!ts_done) {
        ts_done = new std::atomic<bool>[num_ts_cores];
        for (size_t i = 0; i < num_ts_cores; ++i) {
          ts_done[i].store(false, std::memory_order_relaxed);
        }
      }
    }

    // Release only the data arrays, keep metadata (ts_progress, ts_done, state)
    void release_data() {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        if (data[lvl]) {
          std::free(data[lvl]);
          data[lvl] = nullptr;
        }
      }
    }

    ~DayData() {
      release_data();
      if (ts_progress) {
        delete[] ts_progress;
      }
      if (ts_done) {
        delete[] ts_done;
      }
    }
  };

  // ===== Core Data Structures (No Pool, Direct Map) =====
  std::map<std::string, DayData *> date_to_daydata_; // Date string -> DayData pointer
  mutable std::mutex map_mutex_;                     // Protects date_to_daydata_

  // ===== Worker Caches (Lock-free Optimization) =====
  mutable std::vector<std::string> ts_cache_date_; // [worker_id] -> current date string
  mutable std::vector<DayData *> ts_cache_data_;   // [worker_id] -> current DayData*
  mutable std::string cs_cache_date_;              // Current CS worker date string
  mutable DayData *cs_cache_data_ = nullptr;       // Current CS worker DayData*

  // ===== Configuration (Immutable) =====
  const size_t num_assets_;
  const size_t num_ts_cores_;
  std::string output_dir_ = "./output/features";
  const int cs_worker_id_; // CS worker ID for logging
  const int io_worker_id_; // IO worker ID for logging

  // ===== Internal Helpers =====
  // Flush tensor to disk (called by IO worker only, no concurrent access)
  void flush_to_disk(const std::string &date_str, DayData *day) {
    Logger::log_worker(io_worker_id_, "flush_to_disk: START for " + date_str);
    if (!day || date_str.size() != 8) {
      Logger::log_worker(io_worker_id_, "FATAL: flush_to_disk: Invalid day or date_str for " + date_str);
      return;
    }

    // Validate data pointers before any operations
    Logger::log_worker(io_worker_id_, "flush_to_disk: Validating data pointers for " + date_str);
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      if (!day->data[lvl]) {
        Logger::log_worker(io_worker_id_, "FATAL: flush_to_disk: day->data[" + std::to_string(lvl) + "] is null for date " + date_str);
        return;
      }
    }

    // Create directory: output/features/YYYY/MM/DD
    Logger::log_worker(io_worker_id_, "flush_to_disk: Creating directory for " + date_str);
    std::string year = date_str.substr(0, 4);
    std::string month = date_str.substr(4, 2);
    std::string day_str = date_str.substr(6, 2);
    std::string out_dir = output_dir_ + "/" + year + "/" + month + "/" + day_str;
    std::filesystem::create_directories(out_dir);
    Logger::log_worker(io_worker_id_, "flush_to_disk: Directory created: " + out_dir);

    const size_t T0 = MAX_ROWS_PER_LEVEL[0];
    const size_t T1 = MAX_ROWS_PER_LEVEL[1];
    const size_t T2 = MAX_ROWS_PER_LEVEL[2];
    const size_t F0 = FIELDS_PER_LEVEL[0];
    const size_t F1 = FIELDS_PER_LEVEL[1];
    const size_t F2 = FIELDS_PER_LEVEL[2];
    const size_t A = num_assets_;

#if STORE_UNIFIED_DAILY_TENSOR
    // Unified mode: [T_L0, F_total, A] single file
    const size_t F_total = F0 + F1 + F2;
    std::string tensor_file = out_dir + "/features.bin";
    std::ofstream ofs(tensor_file, std::ios::binary);
    if (!ofs) {
      Logger::log_worker(io_worker_id_, "FATAL: flush_to_disk: Failed to open unified tensor file: " + tensor_file);
      return;
    }

    // Write header: [T, F, A]
    ofs.write(reinterpret_cast<const char *>(&T0), sizeof(size_t));
    ofs.write(reinterpret_cast<const char *>(&F_total), sizeof(size_t));
    ofs.write(reinterpret_cast<const char *>(&A), sizeof(size_t));

    // Get link feature offsets from L0
    const size_t link_to_L1_offset = L0_FieldOffset::_link_to_L1;
    const size_t link_to_L2_offset = L0_FieldOffset::_link_to_L2;

    // Write data: for each L0 time t0
    for (size_t t0 = 0; t0 < T0; ++t0) {
      // Read L1/L2 indices from L0 link features (stored as uint16_t, reinterpret as index)
      // Use first asset's link (all assets share same time mapping)
      const size_t t1 = static_cast<size_t>(day->data[0][t0 * F0 * A + link_to_L1_offset * A]);
      const size_t t2 = static_cast<size_t>(day->data[0][t0 * F0 * A + link_to_L2_offset * A]);

      // L0 features
      for (size_t f = 0; f < F0; ++f) {
        ofs.write(reinterpret_cast<const char *>(day->data[0] + t0 * F0 * A + f * A), A * sizeof(feature_storage_t));
      }

      // L1 features (upsampled via link)
      for (size_t f = 0; f < F1; ++f) {
        ofs.write(reinterpret_cast<const char *>(day->data[1] + t1 * F1 * A + f * A), A * sizeof(feature_storage_t));
      }

      // L2 features (upsampled via link)
      for (size_t f = 0; f < F2; ++f) {
        ofs.write(reinterpret_cast<const char *>(day->data[2] + t2 * F2 * A + f * A), A * sizeof(feature_storage_t));
      }
    }
#else
    // Separate mode: 3 level files (link already in L0 features, no separate metadata file needed)
    // L0 tensor: [T0, F0, A] (includes _link_to_L1 and _link_to_L2 features)
    Logger::log_worker(io_worker_id_, "flush_to_disk: Writing L0 for " + date_str);
    {
      std::string l0_file = out_dir + "/features_L0.bin";
      Logger::log_worker(io_worker_id_, "flush_to_disk: Opening L0 file: " + l0_file);
      std::ofstream ofs(l0_file, std::ios::binary);
      if (!ofs) {
        Logger::log_worker(io_worker_id_, "FATAL: flush_to_disk: Failed to open L0 file: " + l0_file);
        return;
      }
      Logger::log_worker(io_worker_id_, "flush_to_disk: Writing L0 header");
      ofs.write(reinterpret_cast<const char *>(&T0), sizeof(size_t));
      ofs.write(reinterpret_cast<const char *>(&F0), sizeof(size_t));
      ofs.write(reinterpret_cast<const char *>(&A), sizeof(size_t));
      Logger::log_worker(io_worker_id_, "flush_to_disk: Writing L0 data T0=" + std::to_string(T0) + " F0=" + std::to_string(F0) + " A=" + std::to_string(A));

      // Pre-check: verify data pointer is valid
      if (!day->data[0]) {
        Logger::log_worker(io_worker_id_, "FATAL: flush_to_disk: day->data[0] became null during flush for " + date_str);
        return;
      }

      for (size_t t = 0; t < T0; ++t) {
        if (t % 1000 == 0) {
          Logger::log_worker(io_worker_id_, "flush_to_disk: L0 progress t=" + std::to_string(t) + "/" + std::to_string(T0));
        }
        for (size_t f = 0; f < F0; ++f) {
          size_t offset = t * F0 * A + f * A;
          size_t max_offset = T0 * F0 * A;
          if (offset >= max_offset) {
            Logger::log_worker(io_worker_id_, "FATAL: flush_to_disk: L0 offset=" + std::to_string(offset) + " >= max=" + std::to_string(max_offset) + " at t=" + std::to_string(t) + " f=" + std::to_string(f));
            return;
          }
          ofs.write(reinterpret_cast<const char *>(day->data[0] + offset), A * sizeof(feature_storage_t));
          if (!ofs.good()) {
            Logger::log_worker(io_worker_id_, "FATAL: flush_to_disk: L0 write failed at t=" + std::to_string(t) + " f=" + std::to_string(f) + " for " + date_str);
            return;
          }
        }
      }
      Logger::log_worker(io_worker_id_, "flush_to_disk: L0 data write loop complete");
    }
    Logger::log_worker(io_worker_id_, "flush_to_disk: L0 written for " + date_str);

    // L1 tensor: [T1, F1, A]
    Logger::log_worker(io_worker_id_, "flush_to_disk: Writing L1 for " + date_str);
    {
      std::string l1_file = out_dir + "/features_L1.bin";
      std::ofstream ofs(l1_file, std::ios::binary);
      if (!ofs) {
        Logger::log_worker(io_worker_id_, "FATAL: flush_to_disk: Failed to open L1 file: " + l1_file);
        return;
      }
      ofs.write(reinterpret_cast<const char *>(&T1), sizeof(size_t));
      ofs.write(reinterpret_cast<const char *>(&F1), sizeof(size_t));
      ofs.write(reinterpret_cast<const char *>(&A), sizeof(size_t));
      for (size_t t = 0; t < T1; ++t) {
        for (size_t f = 0; f < F1; ++f) {
          ofs.write(reinterpret_cast<const char *>(day->data[1] + t * F1 * A + f * A), A * sizeof(feature_storage_t));
        }
      }
    }
    Logger::log_worker(io_worker_id_, "flush_to_disk: L1 written for " + date_str);

    // L2 tensor: [T2, F2, A]
    Logger::log_worker(io_worker_id_, "flush_to_disk: Writing L2 for " + date_str);
    {
      std::string l2_file = out_dir + "/features_L2.bin";
      std::ofstream ofs(l2_file, std::ios::binary);
      if (!ofs) {
        Logger::log_worker(io_worker_id_, "FATAL: flush_to_disk: Failed to open L2 file: " + l2_file);
        return;
      }
      ofs.write(reinterpret_cast<const char *>(&T2), sizeof(size_t));
      ofs.write(reinterpret_cast<const char *>(&F2), sizeof(size_t));
      ofs.write(reinterpret_cast<const char *>(&A), sizeof(size_t));
      for (size_t t = 0; t < T2; ++t) {
        for (size_t f = 0; f < F2; ++f) {
          ofs.write(reinterpret_cast<const char *>(day->data[2] + t * F2 * A + f * A), A * sizeof(feature_storage_t));
        }
      }
    }
    Logger::log_worker(io_worker_id_, "flush_to_disk: L2 written for " + date_str);
    Logger::log_worker(io_worker_id_, "flush_to_disk: END for " + date_str);
#endif
  }

  // Allocate or find tensor from pool (internal function, called by get_cached_data on cache miss)
  // NOTE: This function maintains both TS and CS worker caches
  //       Does NOT check cache - cache checking is done by get_cached_data
  DayData *get_or_create_daydata(const std::string &date, int worker_id) {
    // Use worker_id directly for logging
    const int log_id = worker_id;

    DayData *result = nullptr;

    // Fast path: check if date exists (minimal lock time)
    {
      std::lock_guard<std::mutex> lock(map_mutex_);
      auto it = date_to_daydata_.find(date);
      if (it != date_to_daydata_.end()) {
        result = it->second;
      }
    }

    // If found, validate and cache outside lock
    if (result) {
      // Check state atomically (no lock needed)
      auto state = result->state.load(std::memory_order_acquire);
      if (state == TensorState::FLUSHING || state == TensorState::UNUSED) {
        Logger::log_worker(log_id, "FATAL: get_or_create_daydata: REFUSING to return invalid " + std::to_string(static_cast<int>(state)) + " tensor for " + date);
        return nullptr;
      }

      // Update cache (per-worker, no lock needed)
      if (worker_id == cs_worker_id_) {
        cs_cache_date_ = date;
        cs_cache_data_ = result;
      } else if (worker_id >= 0 && worker_id < static_cast<int>(ts_cache_date_.size())) {
        ts_cache_date_[worker_id] = date;
        ts_cache_data_[worker_id] = result;
      }

      return result;
    }

    // Slow path: create new DayData (outside lock to reduce contention)
    Logger::log_worker(log_id, "get_or_create_daydata: Creating new DayData for " + date + ", A_num=" + std::to_string(num_assets_) + ", cores_num=" + std::to_string(num_ts_cores_));
    DayData *day = new DayData();
    day->allocate(num_assets_, num_ts_cores_);
    day->state.store(TensorState::IN_USE, std::memory_order_release);

    // Insert into map (need lock, but minimal time)
    {
      std::lock_guard<std::mutex> lock(map_mutex_);
      // Double-check: another thread might have created it
      auto it = date_to_daydata_.find(date);
      if (it != date_to_daydata_.end()) {
        // Race condition: another thread created it, delete our copy and use theirs
        delete day;
        result = it->second;
      } else {
        // We won the race, insert ours
        date_to_daydata_[date] = day;
        result = day;
        Logger::log_worker(log_id, "get_or_create_daydata: Successfully created DayData for " + date);
      }
    }

    // Update cache (outside lock)
    if (worker_id == cs_worker_id_) {
      cs_cache_date_ = date;
      cs_cache_data_ = result;
    } else if (worker_id >= 0 && worker_id < static_cast<int>(ts_cache_date_.size())) {
      ts_cache_date_[worker_id] = date;
      ts_cache_data_[worker_id] = result;
    }

    return result;
  }

public:
  GlobalFeatureStore(size_t num_assets, size_t num_ts_cores,
                     const std::string &output_dir = "",
                     int cs_worker_id = -1, int io_worker_id = -1)
      : num_assets_(num_assets), num_ts_cores_(num_ts_cores),
        cs_worker_id_(cs_worker_id >= 0 ? cs_worker_id : static_cast<int>(num_ts_cores)),
        io_worker_id_(io_worker_id >= 0 ? io_worker_id : static_cast<int>(num_ts_cores) + 1) {

    if (!output_dir.empty()) {
      output_dir_ = output_dir;
      // Auto wipe and create
      if (std::filesystem::exists(output_dir_)) {
        std::cout << "Wiping and creating output directory: " << output_dir_ << std::endl;
        std::filesystem::remove_all(output_dir_);
      }
      std::filesystem::create_directories(output_dir_);
    }

    size_t bytes_per_day = 0;
    size_t bytes_per_level[LEVEL_COUNT] = {0};
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      bytes_per_level[lvl] = MAX_ROWS_PER_LEVEL[lvl] * num_assets * FIELDS_PER_LEVEL[lvl] * sizeof(feature_storage_t);
      bytes_per_day += bytes_per_level[lvl];
    }

    // Calculate storage sizes
    const size_t total_features = FIELDS_PER_LEVEL[0] + FIELDS_PER_LEVEL[1] + FIELDS_PER_LEVEL[2];

    std::cout << "\n=== Feature Store (Dynamic Allocation) ===\n";
    std::cout << "Assets: " << num_assets << " | cores(TS): " << num_ts_cores << "\n";

    std::cout << "Level  Features   Time×Asset    PerDay(MB)  Description\n";
    std::cout << "-----  --------  -----------  -----------  -----------\n";

    const char *level_desc[] = {"1s tick", "1min bar", "1h bar"};
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      const size_t T = MAX_ROWS_PER_LEVEL[lvl];
      const size_t F = FIELDS_PER_LEVEL[lvl];
      const double per_day_mb = bytes_per_level[lvl] / (1024.0 * 1024.0);

      printf("  L%zu   %4zu       %5zu×%-4zu      %8.2f  %s\n",
             lvl, F, T, num_assets, per_day_mb, level_desc[lvl]);
    }

    std::cout << "-----  --------  -----------  -----------  -----------\n";
    printf("Total  %4zu                        %8.1f  per daily tensor\n",
           total_features,
           bytes_per_day / (1024.0 * 1024.0));
    std::cout << "\n";

    // Initialize per-worker cache
    ts_cache_date_.resize(num_ts_cores);
    ts_cache_data_.resize(num_ts_cores, nullptr);
  }

  ~GlobalFeatureStore() {
    // Clean up all DayData objects
    for (auto &[date, day] : date_to_daydata_) {
      delete day;
    }
  }

  // ===== Cache Layer (Public Access Point) =====
  // Get DayData* from cache (for metadata access: ts_progress, etc.)
  // Maintains both TS and CS worker caches transparently
  DayData *get_cached_data(const std::string &date, int worker_id) const {
    // Fast path 1: CS worker cache
    if (worker_id == cs_worker_id_ && cs_cache_date_ == date && cs_cache_data_) {
      return cs_cache_data_;
    }

    // Fast path 2: TS worker cache
    if (worker_id >= 0 && worker_id < static_cast<int>(ts_cache_date_.size())) {
      if (ts_cache_date_[worker_id] == date && ts_cache_data_[worker_id]) {
        return ts_cache_data_[worker_id];
      }
    }

    // Slow path: get or create from map (need const_cast as it may create new date)
    return const_cast<GlobalFeatureStore *>(this)->get_or_create_daydata(date, worker_id);
  }

  // Get feature_storage_t* from cache (for feature data read/write)
  // Delegates to get_cached_data for cache consistency
  feature_storage_t *get_cached_ptr(const std::string &date, size_t level_idx, int worker_id) {
    // Use worker_id directly for logging
    const int log_id = worker_id;

    DayData *day = get_cached_data(date, worker_id);
    if (!day) [[unlikely]] {
      Logger::log_worker(log_id, "FATAL: get_cached_ptr: day is null for " + date);
      return nullptr;
    }

    return day->data[level_idx];
  }

  // ===== TS Worker Interface =====
  // Update fine-grained timeslot progress (called per-tick, asset-level granularity)
  // asset_id: global asset identifier (dense 0 to num_assets-1), used as direct index
  void ts_mark_progress(const std::string &date, size_t core_id, size_t asset_id, size_t l0_time_index) {
    // Log every 100 calls for L0 level (per core) - MOVED BEFORE get_cached_data to detect blocking

    DayData *day = get_cached_data(date, core_id);
    if (!day) {
      Logger::log_worker(core_id, "FATAL: Failed to get DayData for " + date);
      std::exit(1);
    }

    // Simple update: asset_id is direct index into ts_progress array
    day->ts_progress[asset_id].store(l0_time_index + 1, std::memory_order_release);
  }

  // Explicitly mark this worker as completely done with this date (called once per date)
  void ts_mark_done(const std::string &date, size_t core_id) {
    Logger::log_worker(core_id, "ts_mark_done: date=" + date + ", core=" + std::to_string(core_id));

    DayData *day = get_cached_data(date, core_id);
    if (!day) {
      Logger::log_worker(core_id, "FATAL: ts_mark_done: Failed to get DayData for " + date);
      std::exit(1);
    }

    day->ts_done[core_id].store(true, std::memory_order_release);
    Logger::log_worker(core_id, "ts_mark_done: date " + date + " core " + std::to_string(core_id) + " marked DONE");
  }

  void ts_write_link(const std::string &date, size_t l0_t, size_t asset_idx, size_t link_feature_offset, _Float16 link_value, int worker_id) {
    // Use worker_id directly for logging
    const int log_id = worker_id;

    feature_storage_t *base = get_cached_ptr(date, 0, worker_id);
    if (!base) {
      Logger::log_worker(log_id, "FATAL: ts_write_link failed to get base ptr for " + date);
      std::exit(1);
    }
    const size_t F0 = FIELDS_PER_LEVEL[0];
    const size_t A = num_assets_;
    const size_t T0 = MAX_ROWS_PER_LEVEL[0];

    // Bounds check
    if (l0_t >= T0) {
      Logger::log_worker(log_id, "FATAL: ts_write_link l0_t=" + std::to_string(l0_t) + " >= T0=" + std::to_string(T0) + " for date " + date);
      std::exit(1);
    }
    if (asset_idx >= A) {
      Logger::log_worker(log_id, "FATAL: ts_write_link asset_idx=" + std::to_string(asset_idx) + " >= A=" + std::to_string(A) + " for date " + date);
      std::exit(1);
    }
    if (link_feature_offset >= F0) {
      Logger::log_worker(log_id, "FATAL: ts_write_link link_offset=" + std::to_string(link_feature_offset) + " >= F0=" + std::to_string(F0) + " for date " + date);
      std::exit(1);
    }

    base[l0_t * F0 * A + link_feature_offset * A + asset_idx] = link_value;
  }

  // ===== CS Worker Interface =====
  bool cs_check_ready(const std::string &date, size_t l0_time_index) const {
    DayData *day = get_cached_data(date, cs_worker_id_);
    if (!day) {
      Logger::log_worker(cs_worker_id_, "FATAL: cs_check_ready: Failed to get DayData for " + date);
      std::exit(1);
      return false;
    }

    // If requested index reaches or exceeds cached safe_index, rescan all ts_progress
    if (l0_time_index >= day->cs_safe_index) {
      // Scan all asset progress to find minimum
      size_t min_progress = SIZE_MAX;
      for (size_t asset_id = 0; asset_id < num_assets_; ++asset_id) {
        size_t progress = day->ts_progress[asset_id].load(std::memory_order_acquire);
        if (progress < min_progress) {
          min_progress = progress;
          Logger::log_worker(cs_worker_id_, "cs_check_ready: date=" + date + " asset=" + std::to_string(asset_id) + " progress=" + std::to_string(progress) + " min_progress=" + std::to_string(min_progress));
        }
      }
      // Update cached safe_index
      day->cs_safe_index = min_progress;
    }

    // Check if safe_index > l0_time_index (meaning all assets have progressed past this point)
    return day->cs_safe_index > l0_time_index;
  }

  void cs_mark_complete(const std::string &date) {
    Logger::log_worker(cs_worker_id_, "cs_mark_complete: date=" + date);

    // Find DayData (minimal lock time)
    DayData *day = nullptr;
    {
      std::lock_guard<std::mutex> lock(map_mutex_);
      auto it = date_to_daydata_.find(date);
      if (it != date_to_daydata_.end()) {
        day = it->second;
      }
    }

    if (!day) {
      Logger::log_worker(cs_worker_id_, "WARNING: cs_mark_complete called for non-existent date: " + date);
      return;
    }

    // Check TS completion outside lock (all atomic operations)
    bool all_ts_done = true;
    for (size_t core_id = 0; core_id < num_ts_cores_; ++core_id) {
      if (!day->ts_done[core_id].load(std::memory_order_acquire)) {
        all_ts_done = false;
        Logger::log_worker(cs_worker_id_, "cs_mark_complete: date " + date + " waiting for TS core " + std::to_string(core_id));
        break;
      }
    }

    if (all_ts_done) {
      day->state.store(TensorState::CS_DONE, std::memory_order_release);
      Logger::log_worker(cs_worker_id_, "cs_mark_complete: date " + date + " marked CS_DONE (all TS cores confirmed done)");
    } else {
      Logger::log_worker(cs_worker_id_, "cs_mark_complete: date " + date + " NOT marked CS_DONE (waiting for TS cores)");
    }
  }

  // ===== Query Interface =====
  size_t query_F(size_t level_idx) const { return FIELDS_PER_LEVEL[level_idx]; }
  size_t query_A() const { return num_assets_; }
  size_t query_T(size_t level_idx) const { return MAX_ROWS_PER_LEVEL[level_idx]; }
  size_t query_num_assets() const { return num_assets_; }
  size_t query_num_dates() const {
    std::lock_guard<std::mutex> lock(map_mutex_);
    return date_to_daydata_.size();
  }
  int query_cs_worker_id() const { return cs_worker_id_; }
  int query_io_worker_id() const { return io_worker_id_; }

  // Debug: get pool status as a single line string
  std::string debug_get_pool_status() const {
    std::lock_guard<std::mutex> lock(map_mutex_);

    std::string result = " [";
    bool first = true;

    for (const auto &[date, day] : date_to_daydata_) {
      if (!first)
        result += ", ";
      first = false;

      const char *state_str = "?";
      auto state = day->state.load(std::memory_order_acquire);
      switch (state) {
      case TensorState::UNUSED:
        state_str = "U";
        break;
      case TensorState::IN_USE:
        state_str = "I";
        break;
      case TensorState::CS_DONE:
        state_str = "D";
        break;
      case TensorState::FLUSHING:
        state_str = "F";
        break;
      }

      result += date + ":" + state_str;
    }

    result += "]";
    return result;
  };

  // ===== IO Worker Interface =====
  // Flush the oldest CS_DONE tensor to disk (returns true if flushed, false if none ready)
  bool io_flush_once() {
    // Find oldest (first in map order) CS_DONE tensor
    std::string date_to_flush;
    DayData *day_to_flush = nullptr;

    {
      std::lock_guard<std::mutex> lock(map_mutex_);
      // std::map is ordered by key (date string), so iterate in order
      for (const auto &[date, day] : date_to_daydata_) {
        if (day->state.load(std::memory_order_acquire) == TensorState::CS_DONE) {
          date_to_flush = date;
          day_to_flush = day;
          Logger::log_worker(io_worker_id_, "io_flush_once: Found CS_DONE date: " + date);
          break; // Found oldest CS_DONE
        }
      }
    }

    if (!day_to_flush) {
      return false; // No CS_DONE tensor found
    }

    // Mark FLUSHING
    day_to_flush->state.store(TensorState::FLUSHING, std::memory_order_release);
    Logger::log_worker(io_worker_id_, "io_flush_once: Starting flush for " + date_to_flush);

    // Flush to disk
    flush_to_disk(date_to_flush, day_to_flush);
    Logger::log_worker(io_worker_id_, "io_flush_once: Completed flush for " + date_to_flush);

    // TEMP: Comment out release to debug
    // Release only the data arrays, keep DayData structure with asset_progress/core_progress/slowest_asset/ts_done/state
    // day_to_flush->release_data();
    // Logger::log_worker(io_worker_id_, "io_flush_once: Released data for " + date_to_flush);

    // Mark as UNUSED (but keep in map, memory can be reclaimed by OS)
    day_to_flush->state.store(TensorState::UNUSED, std::memory_order_release);

    return true;
  }

  // ===== Configuration Interface =====
  void config_set_output_dir(const std::string &dir) {
    output_dir_ = dir;
  }

  // Preallocate all DayData for given dates (avoids runtime malloc contention)
  // CRITICAL: Forces physical page allocation by writing to memory, not just VMA allocation
  void preallocate_dates(const std::vector<std::string> &dates) {
    std::cout << "\n=== Preallocating DayData for " << dates.size() << " dates ===\n";
    std::cout << "  (Forcing physical page allocation with memory writes...)\n";
    auto start = std::chrono::steady_clock::now();

    for (size_t i = 0; i < dates.size(); ++i) {
      const std::string &date = dates[i];

      // Create DayData (this will trigger VMA allocation)
      DayData *day = new DayData();
      day->allocate(num_assets_, num_ts_cores_);
      day->state.store(TensorState::IN_USE, std::memory_order_release);

      // CRITICAL: Touch every page to force physical allocation (page fault now, not at runtime)
      // Write zeros to trigger COW and allocate real pages (Linux lazy allocation workaround)
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        const size_t total_bytes = MAX_ROWS_PER_LEVEL[lvl] * num_assets_ * FIELDS_PER_LEVEL[lvl] * sizeof(feature_storage_t);
        const size_t page_size = 4096; // Linux page size
        // Touch every page (write at page boundaries)
        for (size_t offset = 0; offset < total_bytes; offset += page_size) {
          reinterpret_cast<volatile char *>(day->data[lvl])[offset] = 0;
        }
        // Touch last byte to ensure last page is allocated
        reinterpret_cast<volatile char *>(day->data[lvl])[total_bytes - 1] = 0;
      }

      // Insert into map
      {
        std::lock_guard<std::mutex> lock(map_mutex_);
        date_to_daydata_[date] = day;
      }

      if ((i + 1) % 10 == 0 || i + 1 == dates.size()) {
        std::cout << "  Preallocated " << (i + 1) << "/" << dates.size() << " dates\r" << std::flush;
      }
    }

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "\n  Physical allocation completed in " << elapsed << " ms\n";
    std::cout << "  (All physical pages allocated, no runtime page faults)\n\n";
  }
};

// ============================================================================
// DATA ACCESS MACROS
// ============================================================================
// Note: All macros work with feature_storage_t (_Float16)
// Automatic conversion between float and _Float16 (like float <-> double)

// TS worker: write features for asset a at time t (src is feature_storage_t*)
// worker_id: TS worker ID for cache optimization
#define TS_WRITE_FEATURES(store, date, level_idx, t, a, f_start, f_end, src, worker_id) \
  do {                                                                                  \
    feature_storage_t *base = (store)->get_cached_ptr(date, level_idx, worker_id);      \
    const size_t F = (store)->query_F(level_idx);                                       \
    const size_t A = (store)->query_A();                                                \
    const size_t base_offset = (t) * F * A + (a);                                       \
    for (size_t f = (f_start); f < (f_end); ++f) {                                      \
      base[base_offset + f * A] = (src)[f];                                             \
    }                                                                                   \
  } while (0)

// CS worker: read all assets for feature f at time t (returns feature_storage_t*)
#define CS_READ_ALL_ASSETS(store, date, level_idx, t, f) \
  ((store)->get_cached_ptr(date, level_idx, (store)->query_cs_worker_id()) + (t) * (store)->query_F(level_idx) * (store)->query_A() + (f) * (store)->query_A())

// CS worker: write all assets for feature f at time t (src is feature_storage_t*)
#define CS_WRITE_ALL_ASSETS(store, date, level_idx, t, f, src, count)                                                                                                      \
  std::memcpy((store)->get_cached_ptr(date, level_idx, (store)->query_cs_worker_id()) + (t) * (store)->query_F(level_idx) * (store)->query_A() + (f) * (store)->query_A(), \
              (src), (count) * sizeof(feature_storage_t))

// Read single value (returns feature_storage_t)
#define READ_FEATURE(store, date, level_idx, t, f, a) \
  ((store)->get_cached_ptr(date, level_idx, (store)->query_cs_worker_id())[(t) * (store)->query_F(level_idx) * (store)->query_A() + (f) * (store)->query_A() + (a)])

// Write single value (value is feature_storage_t)
#define WRITE_FEATURE(store, date, level_idx, t, f, a, value)                                                                                                                   \
  do {                                                                                                                                                                          \
    (store)->get_cached_ptr(date, level_idx, (store)->query_cs_worker_id())[(t) * (store)->query_F(level_idx) * (store)->query_A() + (f) * (store)->query_A() + (a)] = (value); \
  } while (0)

// Write link feature (L0 only): map L0 time to L1/L2 time
// link_feature_offset: L0_FieldOffset::_link_to_L1 or _link_to_L2
// link_value: L1 or L2 time index (stored as _Float16, auto-converted from size_t)
// worker_id: TS worker ID for cache and logging
#define WRITE_LINK_FEATURE(store, date, l0_t, asset_idx, link_feature_offset, link_value, worker_id)                  \
  do {                                                                                                                \
    (store)->ts_write_link(date, l0_t, asset_idx, link_feature_offset, static_cast<_Float16>(link_value), worker_id); \
  } while (0)
