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
#include <thread>
#include <vector>

// ============================================================================
// FEATURE STORE CONFIGURATION
// ============================================================================
#define STORE_UNIFIED_DAILY_TENSOR false

// ============================================================================
// FEATURE STORE - Simple, robust, no race conditions
// ============================================================================
class GlobalFeatureStore {
private:
  // Tensor lifecycle states
  enum class TensorState : uint8_t {
    UNUSED = 0,
    IN_USE = 1,
    CS_DONE = 2,
    FLUSHING = 3
  };

  // Per-date tensor data
  struct DayData {
    feature_storage_t *data[LEVEL_COUNT] = {nullptr};
    std::atomic<size_t> *ts_progress = nullptr;
    std::atomic<bool> *ts_done = nullptr;
    mutable size_t cs_safe_index = 0;

    void allocate(size_t num_assets, size_t num_ts_cores) {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        const size_t total_bytes = MAX_ROWS_PER_LEVEL[lvl] * FIELDS_PER_LEVEL[lvl] * num_assets * sizeof(feature_storage_t);
        const size_t aligned_bytes = ((total_bytes + 63) / 64) * 64;
        data[lvl] = static_cast<feature_storage_t *>(std::aligned_alloc(64, aligned_bytes));
        assert(data[lvl]);
      }
      ts_progress = new std::atomic<size_t>[num_assets]();
      ts_done = new std::atomic<bool>[num_ts_cores]();
    }

    void reset(size_t num_assets, size_t num_ts_cores) {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        const size_t total_bytes = MAX_ROWS_PER_LEVEL[lvl] * FIELDS_PER_LEVEL[lvl] * num_assets * sizeof(feature_storage_t);
        std::memset(data[lvl], 0, total_bytes);
      }
      for (size_t i = 0; i < num_assets; ++i) {
        ts_progress[i].store(0, std::memory_order_relaxed);
      }
      for (size_t i = 0; i < num_ts_cores; ++i) {
        ts_done[i].store(false, std::memory_order_relaxed);
      }
      cs_safe_index = 0;
    }

    ~DayData() {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        if (data[lvl]) std::free(data[lvl]);
      }
      delete[] ts_progress;
      delete[] ts_done;
    }
  };

  // Pool management
  DayData **pool_ = nullptr;
  std::atomic<TensorState> *pool_states_ = nullptr;
  std::map<std::string, size_t> date_to_pool_idx_;
  mutable std::mutex pool_mutex_;
  
  // Per-date allocation control (call_once)
  std::map<std::string, std::once_flag> alloc_flags_;
  mutable std::mutex alloc_flags_mutex_;

  // Worker cache (lock-free hot path)
  mutable std::vector<std::string> worker_cache_date_;
  mutable std::vector<DayData *> worker_cache_data_;

  // Config
  const size_t num_assets_;
  const size_t num_ts_cores_;
  const size_t pool_size_;
  std::string output_dir_;
  const int cs_worker_id_;
  const int io_worker_id_;

  // Flush to disk
  void flush_to_disk(const std::string &date_str, DayData *day) {
    assert(day && date_str.size() == 8);
    Logger::log_worker(io_worker_id_, "flush_to_disk: START " + date_str);

    std::string year = date_str.substr(0, 4);
    std::string month = date_str.substr(4, 2);
    std::string day_str = date_str.substr(6, 2);
    std::string out_dir = output_dir_ + "/" + year + "/" + month + "/" + day_str;
    std::filesystem::create_directories(out_dir);

    const size_t T[3] = {MAX_ROWS_PER_LEVEL[0], MAX_ROWS_PER_LEVEL[1], MAX_ROWS_PER_LEVEL[2]};
    const size_t F[3] = {FIELDS_PER_LEVEL[0], FIELDS_PER_LEVEL[1], FIELDS_PER_LEVEL[2]};
    const size_t A = num_assets_;

#if STORE_UNIFIED_DAILY_TENSOR
    // Unified mode: [T_L0, F_total, A]
    const size_t F_total = F[0] + F[1] + F[2];
    std::ofstream ofs(out_dir + "/features.bin", std::ios::binary);
    assert(ofs);
    ofs.write(reinterpret_cast<const char *>(&T[0]), sizeof(size_t));
    ofs.write(reinterpret_cast<const char *>(&F_total), sizeof(size_t));
    ofs.write(reinterpret_cast<const char *>(&A), sizeof(size_t));

    const size_t link_L1 = L0_FieldOffset::_link_to_L1;
    const size_t link_L2 = L0_FieldOffset::_link_to_L2;

    for (size_t t0 = 0; t0 < T[0]; ++t0) {
      const size_t t1 = static_cast<size_t>(day->data[0][t0 * F[0] * A + link_L1 * A]);
      const size_t t2 = static_cast<size_t>(day->data[0][t0 * F[0] * A + link_L2 * A]);
      for (size_t f = 0; f < F[0]; ++f) {
        ofs.write(reinterpret_cast<const char *>(day->data[0] + t0 * F[0] * A + f * A), A * sizeof(feature_storage_t));
      }
      for (size_t f = 0; f < F[1]; ++f) {
        ofs.write(reinterpret_cast<const char *>(day->data[1] + t1 * F[1] * A + f * A), A * sizeof(feature_storage_t));
      }
      for (size_t f = 0; f < F[2]; ++f) {
        ofs.write(reinterpret_cast<const char *>(day->data[2] + t2 * F[2] * A + f * A), A * sizeof(feature_storage_t));
      }
    }
#else
    // Separate mode: 3 files
    for (size_t lvl = 0; lvl < 3; ++lvl) {
      std::string filename = out_dir + "/features_L" + std::to_string(lvl) + ".bin";
      std::ofstream ofs(filename, std::ios::binary);
      assert(ofs);
      ofs.write(reinterpret_cast<const char *>(&T[lvl]), sizeof(size_t));
      ofs.write(reinterpret_cast<const char *>(&F[lvl]), sizeof(size_t));
      ofs.write(reinterpret_cast<const char *>(&A), sizeof(size_t));
      for (size_t t = 0; t < T[lvl]; ++t) {
        for (size_t f = 0; f < F[lvl]; ++f) {
          ofs.write(reinterpret_cast<const char *>(day->data[lvl] + t * F[lvl] * A + f * A), A * sizeof(feature_storage_t));
        }
      }
    }
#endif
    Logger::log_worker(io_worker_id_, "flush_to_disk: END " + date_str);
  }

  // Allocate pool slot for date (called exactly once per date via call_once)
  void allocate_for_date(const std::string &date, int worker_id) {
    while (true) {
      std::unique_lock<std::mutex> lock(pool_mutex_);

      // Find UNUSED slot
      for (size_t i = 0; i < pool_size_; ++i) {
        if (pool_states_[i].load(std::memory_order_acquire) == TensorState::UNUSED) {
          pool_states_[i].store(TensorState::IN_USE, std::memory_order_release);
          date_to_pool_idx_[date] = i;
          DayData *allocated = pool_[i];
          
          Logger::log_worker(worker_id, "Bound " + date + " to pool[" + std::to_string(i) + "], resetting...");
          lock.unlock();
          
          // Reset outside lock (safe: call_once guarantees single execution)
          allocated->reset(num_assets_, num_ts_cores_);
          Logger::log_worker(worker_id, "Pool[" + std::to_string(i) + "] reset complete");
          return;
        }
      }

      Logger::log_worker(worker_id, "Pool exhausted, waiting...");
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

public:
  GlobalFeatureStore(size_t num_assets, size_t num_ts_cores,
                     const std::string &output_dir = "",
                     int cs_worker_id = -1, int io_worker_id = -1)
      : num_assets_(num_assets), num_ts_cores_(num_ts_cores),
        pool_size_(num_ts_cores * 15),
        cs_worker_id_(cs_worker_id >= 0 ? cs_worker_id : static_cast<int>(num_ts_cores)),
        io_worker_id_(io_worker_id >= 0 ? io_worker_id : static_cast<int>(num_ts_cores) + 1) {

    if (!output_dir.empty()) {
      output_dir_ = output_dir;
      if (std::filesystem::exists(output_dir_)) {
        std::filesystem::remove_all(output_dir_);
      }
      std::filesystem::create_directories(output_dir_);
    }

    // Calculate sizes
    size_t bytes_per_day = 0;
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      bytes_per_day += MAX_ROWS_PER_LEVEL[lvl] * num_assets * FIELDS_PER_LEVEL[lvl] * sizeof(feature_storage_t);
    }

    std::cout << "\n=== Feature Store Tensor Pool ===\n";
    std::cout << "Pool: " << pool_size_ << " tensors | Assets: " << num_assets << " | TS cores: " << num_ts_cores << "\n";
    printf("Per-day tensor: %.2f MB | Pool total: %.2f GB\n",
           bytes_per_day / (1024.0 * 1024.0),
           (bytes_per_day * pool_size_) / (1024.0 * 1024.0 * 1024.0));

    // Allocate pool
    pool_ = new DayData *[pool_size_];
    pool_states_ = new std::atomic<TensorState>[pool_size_];

    std::cout << "Allocating and touching physical pages...\n";
    auto start = std::chrono::steady_clock::now();

    for (size_t i = 0; i < pool_size_; ++i) {
      pool_[i] = new DayData();
      pool_[i]->allocate(num_assets_, num_ts_cores_);

      // Touch all pages to force physical allocation
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        const size_t total_bytes = MAX_ROWS_PER_LEVEL[lvl] * FIELDS_PER_LEVEL[lvl] * num_assets * sizeof(feature_storage_t);
        const size_t page_size = 4096;
        for (size_t offset = 0; offset < total_bytes; offset += page_size) {
          reinterpret_cast<volatile char *>(pool_[i]->data[lvl])[offset] = 0;
        }
        reinterpret_cast<volatile char *>(pool_[i]->data[lvl])[total_bytes - 1] = 0;
      }

      pool_states_[i].store(TensorState::UNUSED, std::memory_order_release);

      if ((i + 1) % 5 == 0 || i + 1 == pool_size_) {
        std::cout << "  " << (i + 1) << "/" << pool_size_ << " tensors\r" << std::flush;
      }
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    std::cout << "\nPhysical allocation complete in " << elapsed << " ms\n";
    std::cout << "=================================\n\n";

    // Initialize worker cache
    worker_cache_date_.resize(num_ts_cores + 1);
    worker_cache_data_.resize(num_ts_cores + 1, nullptr);
  }

  ~GlobalFeatureStore() {
    if (pool_) {
      for (size_t i = 0; i < pool_size_; ++i) {
        delete pool_[i];
      }
      delete[] pool_;
    }
    delete[] pool_states_;
  }

  DayData *get(const std::string &date, int worker_id, bool allow_allocate = true) {
    // Fast path: cache hit
    if (worker_cache_date_[worker_id] == date && worker_cache_data_[worker_id]) {
      return worker_cache_data_[worker_id];
    }

    // Check if already allocated
    {
      std::lock_guard<std::mutex> lock(pool_mutex_);
      auto it = date_to_pool_idx_.find(date);
      if (it != date_to_pool_idx_.end()) {
        DayData *day = pool_[it->second];
        worker_cache_date_[worker_id] = date;
        worker_cache_data_[worker_id] = day;
        return day;
      }
    }

    // Not allocated yet
    if (!allow_allocate) {
      return nullptr;
    }

    // Get or create once_flag for this date
    std::once_flag *flag;
    {
      std::lock_guard<std::mutex> lock(alloc_flags_mutex_);
      flag = &alloc_flags_[date];
    }

    // Exactly one thread will allocate, others wait
    std::call_once(*flag, [this, date, worker_id]() {
      allocate_for_date(date, worker_id);
    });

    // Now it's allocated, get pointer
    std::lock_guard<std::mutex> lock(pool_mutex_);
    auto it = date_to_pool_idx_.find(date);
    assert(it != date_to_pool_idx_.end());
    DayData *day = pool_[it->second];
    
    worker_cache_date_[worker_id] = date;
    worker_cache_data_[worker_id] = day;
    return day;
  }

  // ===== TS WORKER API =====
  void ts_mark_progress(const std::string &date, size_t core_id, size_t asset_id, size_t l0_time_index) {
    DayData *day = get(date, core_id);
    day->ts_progress[asset_id].store(l0_time_index + 1, std::memory_order_release);
  }

  void ts_mark_done(const std::string &date, size_t core_id) {
    DayData *day = get(date, core_id);
    day->ts_done[core_id].store(true, std::memory_order_release);
    Logger::log_worker(core_id, "ts_mark_done: " + date);
  }

  // ===== CS WORKER API =====
  bool cs_check_ready(const std::string &date, size_t l0_time_index) const {
    DayData *day = const_cast<GlobalFeatureStore *>(this)->get(date, cs_worker_id_, false);
    if (!day) {
      return false;
    }

    if (l0_time_index >= day->cs_safe_index) {
      size_t min_progress = SIZE_MAX;
      for (size_t a = 0; a < num_assets_; ++a) {
        size_t progress = day->ts_progress[a].load(std::memory_order_acquire);
        if (progress < min_progress) {
          min_progress = progress;
        }
      }
      day->cs_safe_index = min_progress;
    }

    return day->cs_safe_index > l0_time_index;
  }

  void cs_mark_complete(const std::string &date) {
    size_t pool_idx;
    {
      std::lock_guard<std::mutex> lock(pool_mutex_);
      auto it = date_to_pool_idx_.find(date);
      assert(it != date_to_pool_idx_.end());
      pool_idx = it->second;
    }

    for (size_t i = 0; i < num_ts_cores_; ++i) {
      assert(pool_[pool_idx]->ts_done[i].load(std::memory_order_acquire));
    }

    pool_states_[pool_idx].store(TensorState::CS_DONE, std::memory_order_release);
    Logger::log_worker(cs_worker_id_, "cs_mark_complete: " + date);
  }

  // ===== IO WORKER API =====
  bool io_flush_once() {
    std::string date_to_flush;
    size_t pool_idx = pool_size_;

    {
      std::lock_guard<std::mutex> lock(pool_mutex_);
      for (const auto &[date, idx] : date_to_pool_idx_) {
        if (pool_states_[idx].load(std::memory_order_acquire) == TensorState::CS_DONE) {
          date_to_flush = date;
          pool_idx = idx;
          break;
        }
      }
    }

    if (pool_idx >= pool_size_) {
      return false;
    }

    pool_states_[pool_idx].store(TensorState::FLUSHING, std::memory_order_release);
    flush_to_disk(date_to_flush, pool_[pool_idx]);

    // Clean up
    {
      std::lock_guard<std::mutex> lock(pool_mutex_);
      date_to_pool_idx_.erase(date_to_flush);
      
      // Invalidate caches
      for (size_t w = 0; w <= num_ts_cores_; ++w) {
        if (worker_cache_date_[w] == date_to_flush) {
          worker_cache_date_[w].clear();
          worker_cache_data_[w] = nullptr;
        }
      }
      
      pool_states_[pool_idx].store(TensorState::UNUSED, std::memory_order_release);
    }
    
    // Clean up once_flag
    {
      std::lock_guard<std::mutex> lock(alloc_flags_mutex_);
      alloc_flags_.erase(date_to_flush);
    }
    
    Logger::log_worker(io_worker_id_, "io_flush_once: " + date_to_flush + " complete");
    return true;
  }

  // ===== QUERY API =====
  size_t query_F(size_t level_idx) const { return FIELDS_PER_LEVEL[level_idx]; }
  size_t query_A() const { return num_assets_; }
  size_t query_T(size_t level_idx) const { return MAX_ROWS_PER_LEVEL[level_idx]; }
  int query_cs_worker_id() const { return cs_worker_id_; }
  int query_io_worker_id() const { return io_worker_id_; }

  std::string debug_pool_status() const {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    std::string result = "[";
    bool first = true;
    for (const auto &[date, idx] : date_to_pool_idx_) {
      if (!first) result += ", ";
      first = false;
      const char *state = "?";
      switch (pool_states_[idx].load(std::memory_order_acquire)) {
      case TensorState::UNUSED: state = "U"; break;
      case TensorState::IN_USE: state = "I"; break;
      case TensorState::CS_DONE: state = "D"; break;
      case TensorState::FLUSHING: state = "F"; break;
      }
      result += date + ":" + state;
    }
    return result + "]";
  }
};

// ============================================================================
// MACRO API - High-performance direct access
// ============================================================================

// ===== TS WORKER API (explicitly pass worker_id) =====

// Batch write: write features [f_start, f_end) for asset a at time t
#define TS_WRITE_FEATURES(store, date, lvl, t, a, f_start, f_end, src, worker_id) \
  do { \
    auto *_day = (store)->get(date, worker_id); \
    assert(_day && "_day is null"); \
    assert(_day->data[lvl] && "data[lvl] is null"); \
    const size_t _F = (store)->query_F(lvl); \
    const size_t _A = (store)->query_A(); \
    [[maybe_unused]] const size_t _T = (store)->query_T(lvl); \
    assert((t) < _T && "time index out of bounds"); \
    assert((a) < _A && "asset index out of bounds"); \
    assert((f_start) <= (f_end) && "invalid feature range"); \
    assert((f_end) <= _F && "feature end out of bounds"); \
    for (size_t _f = (f_start); _f < (f_end); ++_f) { \
      const size_t _idx = ((t) * _F + _f) * _A + (a); \
      _day->data[lvl][_idx] = (src)[_f - (f_start)]; \
    } \
  } while (0)

// Single write: write feature f for asset a at time t
#define TS_WRITE_SINGLE(store, date, lvl, t, f, a, value, worker_id) \
  do { \
    auto *_day = (store)->get(date, worker_id); \
    assert(_day && "_day is null"); \
    assert(_day->data[lvl] && "data[lvl] is null"); \
    const size_t _F = (store)->query_F(lvl); \
    const size_t _A = (store)->query_A(); \
    [[maybe_unused]] const size_t _T = (store)->query_T(lvl); \
    assert((t) < _T && "time index out of bounds"); \
    assert((f) < _F && "feature index out of bounds"); \
    assert((a) < _A && "asset index out of bounds"); \
    const size_t _idx = ((t) * _F + (f)) * _A + (a); \
    _day->data[lvl][_idx] = (value); \
  } while (0)

// Write link metadata (L0 only, auto-triggered by resampling)
#define TS_WRITE_LINK(store, date, l0_t, a, link_offset, value, worker_id) \
  do { \
    auto *_day = (store)->get(date, worker_id); \
    assert(_day && "_day is null"); \
    assert(_day->data[0] && "data[0] is null"); \
    const size_t _F = (store)->query_F(0); \
    const size_t _A = (store)->query_A(); \
    [[maybe_unused]] const size_t _T = (store)->query_T(0); \
    assert((l0_t) < _T && "l0_t out of bounds"); \
    assert((link_offset) < _F && "link_offset out of bounds"); \
    assert((a) < _A && "asset index out of bounds"); \
    const size_t _idx = ((l0_t) * _F + (link_offset)) * _A + (a); \
    _day->data[0][_idx] = static_cast<_Float16>(value); \
  } while (0)

// ===== CS WORKER API (automatically use cs_worker_id) =====

// Read all assets for feature f at time t → returns _Float16*
#define CS_READ_ALL(store, date, lvl, t, f) \
  [&]() -> feature_storage_t* { \
    auto *_day = (store)->get(date, (store)->query_cs_worker_id()); \
    assert(_day && "_day is null"); \
    assert(_day->data[lvl] && "data[lvl] is null"); \
    const size_t _F = (store)->query_F(lvl); \
    const size_t _A = (store)->query_A(); \
    [[maybe_unused]] const size_t _T = (store)->query_T(lvl); \
    assert((t) < _T && "time index out of bounds"); \
    assert((f) < _F && "feature index out of bounds"); \
    const size_t _offset = ((t) * _F + (f)) * _A; \
    return _day->data[lvl] + _offset; \
  }()

// Write all assets for feature f at time t
#define CS_WRITE_ALL(store, date, lvl, t, f, src, count) \
  do { \
    auto *_day = (store)->get(date, (store)->query_cs_worker_id()); \
    assert(_day && "_day is null"); \
    assert(_day->data[lvl] && "data[lvl] is null"); \
    const size_t _F = (store)->query_F(lvl); \
    const size_t _A = (store)->query_A(); \
    [[maybe_unused]] const size_t _T = (store)->query_T(lvl); \
    assert((t) < _T && "time index out of bounds"); \
    assert((f) < _F && "feature index out of bounds"); \
    assert((count) <= _A && "count exceeds num_assets"); \
    const size_t _offset = ((t) * _F + (f)) * _A; \
    std::memcpy(_day->data[lvl] + _offset, (src), (count) * sizeof(feature_storage_t)); \
  } while (0)
