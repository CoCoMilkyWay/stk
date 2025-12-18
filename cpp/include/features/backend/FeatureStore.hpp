#pragma once

#include "FeatureStoreConfig.hpp"
#include "ZstdHelper.hpp"
#include "misc/logging.hpp"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <io.h>
#include <fcntl.h>
#include <share.h>
#include <windows.h>
#undef min
#undef max

// ============================================================================
// FEATURE STORE CONFIGURATION
// ============================================================================
#define STORE_UNIFIED_DAILY_TENSOR false
#define POOL_SIZE_FACTOR 2

// ============================================================================
// FEATURE STORE - Simple, robust, no race conditions
// ============================================================================
class GlobalFeatureStore {
private:
  // Tensor lifecycle states (NEW ARCHITECTURE)
  enum class TensorState : uint8_t {
    FREE = 0, // Available for allocation
    INIT = 1, // Being initialized
    BUSY = 2, // Active (TS writing + CS reading/writing)
    DONE = 3, // CS finished, ready for flush
    FLUSH = 4 // IO worker writing to disk
  };

  // Per-date slot (NEW ARCHITECTURE with two-level sync)
  struct Slot {
    // State machine and lifecycle
    std::atomic<TensorState> state{TensorState::FREE};
    std::atomic<uint32_t> epoch{0}; // Generation number for cache invalidation
    char date[16] = {0};            // Fixed-size date string "YYYYMMDD"
    feature_storage_t *data[LEVEL_COUNT] = {nullptr};

    // Depth data [T][F_depth][A] (separate storage for orderflow)
    feature_storage_t *depth_data = nullptr;

    // Two-level synchronization (key optimization: O(W) scan instead of O(A))
    size_t *ts_write_pos = nullptr;                   // [A] per-asset write position (TS-local, no sync)
    std::atomic<uint64_t> *ts_worker_state = nullptr; // [W] packed: [48b:min_pos][1b:done][15b:reserved]
    size_t cs_read_pos{0};                            // CS cached safe position (CS-local, no sync)

    // WorkerState bit layout helpers
    static constexpr uint64_t MIN_POS_MASK = 0x0000FFFFFFFFFFFF; // bits 0-47
    static constexpr uint64_t DONE_FLAG_BIT = 1ULL << 48;        // bit 48

    static uint64_t pack_worker_state(size_t min_pos, bool done) {
      return (min_pos & MIN_POS_MASK) | (done ? DONE_FLAG_BIT : 0);
    }

    static size_t unpack_min_pos(uint64_t state) {
      return state & MIN_POS_MASK;
    }

    static bool unpack_done(uint64_t state) {
      return (state & DONE_FLAG_BIT) != 0;
    }

    // Async reset support
    bool needs_reset{false};

    void allocate(size_t num_assets, size_t num_ts_workers) {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        const size_t total_bytes = MAX_ROWS_PER_LEVEL[lvl] * FIELDS_PER_LEVEL[lvl] * num_assets * sizeof(feature_storage_t);
        const size_t aligned_bytes = ((total_bytes + 63) / 64) * 64;
        data[lvl] = static_cast<feature_storage_t *>(_aligned_malloc(aligned_bytes, 64));
        assert(data[lvl]);
      }

      // Allocate depth data (same T as L0)
      const size_t depth_total_bytes = MAX_ROWS_PER_LEVEL[0] * DEPTH_TOTAL_WIDTH * num_assets * sizeof(feature_storage_t);
      const size_t depth_aligned_bytes = ((depth_total_bytes + 63) / 64) * 64;
      depth_data = static_cast<feature_storage_t *>(_aligned_malloc(depth_aligned_bytes, 64));
      assert(depth_data);

      ts_write_pos = new size_t[num_assets]();
      ts_worker_state = new std::atomic<uint64_t>[num_ts_workers]();
    }

    void reset(size_t num_assets, size_t num_ts_workers) {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        const size_t total_bytes = MAX_ROWS_PER_LEVEL[lvl] * FIELDS_PER_LEVEL[lvl] * num_assets * sizeof(feature_storage_t);
        std::memset(data[lvl], 0, total_bytes);
      }

      // Reset depth data
      const size_t depth_total_bytes = MAX_ROWS_PER_LEVEL[0] * DEPTH_TOTAL_WIDTH * num_assets * sizeof(feature_storage_t);
      std::memset(depth_data, 0, depth_total_bytes);

      for (size_t i = 0; i < num_assets; ++i) {
        ts_write_pos[i] = 0;
      }
      for (size_t i = 0; i < num_ts_workers; ++i) {
        ts_worker_state[i].store(pack_worker_state(0, false), std::memory_order_relaxed);
      }
      cs_read_pos = 0;
      std::memset(date, 0, sizeof(date));
    }

    ~Slot() {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        if (data[lvl])
          _aligned_free(data[lvl]);
      }
      if (depth_data)
        _aligned_free(depth_data);
      delete[] ts_write_pos;
      delete[] ts_worker_state;
    }
  };

  // Config
  const size_t num_assets_;
  const size_t num_ts_workers_;
  const size_t pool_size_;
  std::string output_dir_;
  const int cs_worker_id_;
  const int io_worker_id_;

  // Pool management (NEW ARCHITECTURE)
  Slot *pool_ = nullptr;               // Slot array instead of pointer-to-pointer
  mutable std::vector<int> free_list_; // Simple freelist (protected by pool_mutex_)
  mutable std::map<std::string, size_t> date_to_slot_;
  mutable std::mutex pool_mutex_; // For date_to_slot_ and free_list_

  // TS worker cache (lock-free hot path with epoch validation)
  struct TSCacheEntry {
    std::string date;
    size_t slot_idx = SIZE_MAX;
    uint32_t epoch = 0;
  };
  mutable std::vector<TSCacheEntry> ts_cache_;

  // CS worker cache (single-threaded with epoch validation)
  struct CSCacheEntry {
    std::string date;
    size_t slot_idx = SIZE_MAX;
    uint32_t epoch = 0;
  };
  mutable CSCacheEntry cs_cache_;

  // Dynamic asset tracking: lazily populated during ts_update()
  // Each worker records which assets it has actually written (decoupled from top-level assignment)
  mutable std::vector<std::unordered_set<size_t>> assigned_assets_; // [worker_id] → {asset_ids}

public:
  GlobalFeatureStore(size_t num_assets, size_t num_ts_workers,
                     const std::string &output_dir = "",
                     int cs_worker_id = -1, int io_worker_id = -1)
      : num_assets_(num_assets), num_ts_workers_(num_ts_workers),
        pool_size_(num_ts_workers * POOL_SIZE_FACTOR),
        cs_worker_id_(cs_worker_id >= 0 ? cs_worker_id : static_cast<int>(num_ts_workers)),
        io_worker_id_(io_worker_id >= 0 ? io_worker_id : static_cast<int>(num_ts_workers) + 1) {

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
    // Add depth data size
    const size_t depth_bytes = MAX_ROWS_PER_LEVEL[0] * num_assets * DEPTH_TOTAL_WIDTH * sizeof(feature_storage_t);
    bytes_per_day += depth_bytes;

    std::cout << "\n=== Feature Store Tensor Pool ===\n";

    // Detailed dimension breakdown
    std::cout << "Dimension Details:\n";
    // Add depth info
    {
      const size_t T = MAX_ROWS_PER_LEVEL[0];
      const size_t A = num_assets;
      const size_t F = DEPTH_TOTAL_WIDTH;
      printf("  Depth  [T=%6zu][F=%4zu][A=%4zu] = %.2f MB\n", T, F, A, depth_bytes / (1024.0 * 1024.0));
    }
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      const size_t T = MAX_ROWS_PER_LEVEL[lvl];
      const size_t A = num_assets;
      const size_t F = FIELDS_PER_LEVEL[lvl];
      const size_t bytes = T * A * F * sizeof(feature_storage_t);
      printf("  L%-5zu [T=%6zu][F=%4zu][A=%4zu] = %.2f MB\n", lvl, T, F, A, bytes / (1024.0 * 1024.0));
    }

    printf("\nPer-day tensor: %.2f MB (features + depth) | Pool size: %zu slots | Total: %.2f GB\n",
           bytes_per_day / (1024.0 * 1024.0),
           pool_size_,
           (bytes_per_day * pool_size_) / (1024.0 * 1024.0 * 1024.0));

    // Allocate pool (Slot array)
    pool_ = new Slot[pool_size_];

    std::cout << "Allocating physical pages...\n";
    auto start = std::chrono::steady_clock::now();

    for (size_t i = 0; i < pool_size_; ++i) {
      pool_[i].allocate(num_assets_, num_ts_workers_);

      // Touch all pages to force physical allocation
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        const size_t total_bytes = MAX_ROWS_PER_LEVEL[lvl] * FIELDS_PER_LEVEL[lvl] * num_assets * sizeof(feature_storage_t);
        const size_t page_size = 4096;
        for (size_t offset = 0; offset < total_bytes; offset += page_size) {
          reinterpret_cast<volatile char *>(pool_[i].data[lvl])[offset] = 0;
        }
        reinterpret_cast<volatile char *>(pool_[i].data[lvl])[total_bytes - 1] = 0;
      }

      // Touch depth data pages
      {
        const size_t total_bytes = MAX_ROWS_PER_LEVEL[0] * DEPTH_TOTAL_WIDTH * num_assets * sizeof(feature_storage_t);
        const size_t page_size = 4096;
        for (size_t offset = 0; offset < total_bytes; offset += page_size) {
          reinterpret_cast<volatile char *>(pool_[i].depth_data)[offset] = 0;
        }
        reinterpret_cast<volatile char *>(pool_[i].depth_data)[total_bytes - 1] = 0;
      }

      // Initialize state and reset slot
      pool_[i].state.store(TensorState::FREE, std::memory_order_relaxed);
      pool_[i].reset(num_assets_, num_ts_workers_);
      pool_[i].needs_reset = false;

      if ((i + 1) % 5 == 0 || i + 1 == pool_size_) {
        std::cout << "  " << (i + 1) << "/" << pool_size_ << " slots\r" << std::flush;
      }
    }

    // Initialize free_list (all slots available, reverse order for stack-like LIFO)
    free_list_.reserve(pool_size_);
    for (int i = static_cast<int>(pool_size_) - 1; i >= 0; --i) {
      free_list_.push_back(i);
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    std::cout << "\nPhysical allocation complete in " << elapsed << " ms\n";

    // Initialize TS worker cache and dynamic asset tracking
    ts_cache_.resize(num_ts_workers_);
    assigned_assets_.resize(num_ts_workers_); // Empty sets, populated lazily during ts_update()

    std::cout << "=================================\n\n";
  }

  ~GlobalFeatureStore() {
    if (pool_) {
      delete[] pool_; // Slot destructors called automatically
    }
  }

  // ===== TS WORKER API (NEW ARCHITECTURE with two-level sync) =====

  // Update progress after writing to (asset_id, l0_time_index)
  // Called by TS_WRITE_* macros or LimitOrderBook internally
  void ts_update(const std::string &date, int worker_id, size_t asset_id, size_t l0_time_index) const {
    Slot &slot = ts_get_slot(date, worker_id);
    // Note: ts_get_slot handles INIT→BUSY transition wait internally, no need to assert here

    // Thread-local tracking: O(1) bitset instead of O(log N) unordered_set
    static thread_local std::vector<std::vector<bool>> asset_written(num_ts_workers_);
    if (asset_written[worker_id].empty()) [[unlikely]] {
      asset_written[worker_id].resize(num_assets_, false);
    }

    // Mark asset as written (O(1), cache-friendly)
    if (!asset_written[worker_id][asset_id]) [[unlikely]] {
      asset_written[worker_id][asset_id] = true;
      assigned_assets_[worker_id].insert(asset_id); // Global record for ts_done (cold path)
    }

    // Update per-asset write position (TS-local, no sync needed)
    size_t new_pos = l0_time_index + 1;
    slot.ts_write_pos[asset_id] = new_pos;

    // Update worker_min (only if this asset affects the min)
    // Initialize to SIZE_MAX (no progress yet)
    static thread_local std::vector<size_t> worker_local_min;
    if (worker_local_min.empty()) [[unlikely]] {
      worker_local_min.resize(num_ts_workers_, SIZE_MAX);
    }

    size_t old_min = worker_local_min[worker_id];

    // Optimization: skip update if current asset is not the bottleneck
    // If new_pos > worker_min, this asset is ahead, min unchanged, no publish needed
    if (new_pos <= old_min) {
      // This asset is at or behind the min, need to rescan (TS-local read)
      size_t new_min = SIZE_MAX;
      for (size_t a : assigned_assets_[worker_id]) {
        new_min = std::min(new_min, slot.ts_write_pos[a]);
      }
      worker_local_min[worker_id] = new_min;
      // Publish only when worker_min actually changed (TS→CS sync)
      slot.ts_worker_state[worker_id].store(Slot::pack_worker_state(new_min, false), std::memory_order_release);
    }
    // else: fast asset, doesn't affect worker_min, skip publish (huge optimization!)
  }

  // Mark worker done for this date (API name from architecture doc)
  void ts_done(const std::string &date, int worker_id) const {
    Slot &slot = ts_get_slot(date, worker_id);
    const size_t final_progress = MAX_ROWS_PER_LEVEL[0];

    // Update TS-local metadata (not read by CS, no sync needed)
    for (size_t a : assigned_assets_[worker_id]) {
      slot.ts_write_pos[a] = final_progress;
    }

    // Publish final worker state: min_pos and done flag (TS→CS sync point)
    uint64_t old_state = slot.ts_worker_state[worker_id].load(std::memory_order_acquire);
    assert(!Slot::unpack_done(old_state) && "TS worker already marked done");
    slot.ts_worker_state[worker_id].store(Slot::pack_worker_state(final_progress, true), std::memory_order_release);

    // Invalidate cache
    ts_cache_[worker_id] = {"", SIZE_MAX, 0};

    Logger::log("worker_" + std::to_string(worker_id), "ts_done: " + date);
  }

  // ===== CS WORKER API (NEW ARCHITECTURE with O(W) scan) =====

  // Wait until time_index is ready (API name from architecture doc)
  void cs_wait(const std::string &date, size_t l0_time_index) const {
    // Step 1: Get Slot (with cache and epoch validation)
    Slot *slot = nullptr;
    if (cs_cache_.date == date && cs_cache_.slot_idx < pool_size_) [[likely]] {
      Slot &s = pool_[cs_cache_.slot_idx];
      if (s.epoch.load(std::memory_order_acquire) == cs_cache_.epoch) {
        slot = &s; // Cache hit
      }
    }

    if (!slot) {
      // Wait for TS to allocate and publish BUSY state
      while (!slot) {
        {
          std::lock_guard<std::mutex> lock(pool_mutex_);
          auto it = date_to_slot_.find(date);
          if (it != date_to_slot_.end()) {
            size_t slot_idx = it->second;
            Slot &s = pool_[slot_idx];
            TensorState state = s.state.load(std::memory_order_acquire);
            if (state == TensorState::BUSY || state == TensorState::DONE) {
              slot = &s;
              cs_cache_.date = date;
              cs_cache_.slot_idx = slot_idx;
              cs_cache_.epoch = s.epoch.load(std::memory_order_acquire);
            }
          }
        }
        if (!slot) {
          std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      }
    }

    // Fast path: cs_read_pos cache (cs_read_pos = N means [0, N-1] verified, CS-local)
    if (l0_time_index < slot->cs_read_pos) [[likely]] {
      return;
    }

    // Slow path: scan ts_worker_min_pos (O(W) = O(10), not O(A) = O(1000))
    size_t backoff_us = 1;
    while (true) {
      // Scan all workers for min_pos and check if all done (single pass optimization)
      size_t min_pos = SIZE_MAX;
      bool all_done = true;
      for (size_t w = 0; w < num_ts_workers_; ++w) {
        uint64_t state = slot->ts_worker_state[w].load(std::memory_order_acquire);
        min_pos = std::min(min_pos, Slot::unpack_min_pos(state));
        if (!Slot::unpack_done(state)) {
          all_done = false;
        }
      }

      // If min_pos > l0_time_index, time_index is ready
      if (min_pos > l0_time_index) {
        slot->cs_read_pos = min_pos; // Batch verify [0, min_pos), CS-local
        return;
      }

      // If all TS workers done, no more progress expected
      if (all_done) {
        slot->cs_read_pos = MAX_ROWS_PER_LEVEL[0]; // Force all ready, CS-local
        return;
      }

      // Exponential backoff
      std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
      backoff_us = std::min(backoff_us * 2, 100ULL);
    }
  }

  // Mark CS done for this date (API name from architecture doc)
  void cs_done(const std::string &date) const {
    // Get Slot from cache
    assert(cs_cache_.date == date && cs_cache_.slot_idx < pool_size_ && "CS cache mismatch");
    Slot &slot = pool_[cs_cache_.slot_idx];

    // Wait for all TS workers to finish
    size_t backoff_us = 1;
    while (true) {
      bool all_done = true;
      for (size_t w = 0; w < num_ts_workers_; ++w) {
        uint64_t state = slot.ts_worker_state[w].load(std::memory_order_acquire);
        if (!Slot::unpack_done(state)) {
          all_done = false;
          break;
        }
      }
      if (all_done)
        break;

      std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
      backoff_us = std::min(backoff_us * 2, 100ULL);
    }

    // Atomic state transition BUSY -> DONE
    {
      std::lock_guard<std::mutex> lock(pool_mutex_);
      TensorState expected = TensorState::BUSY;
      [[maybe_unused]] bool success = slot.state.compare_exchange_strong(expected, TensorState::DONE, std::memory_order_acq_rel, std::memory_order_acquire);
      assert(success && "CS state transition failed");
    }

    // Invalidate cache
    cs_cache_ = {"", SIZE_MAX, 0};

    Logger::log("worker_" + std::to_string(cs_worker_id_), "cs_done: " + date);
  }

  // ===== IO WORKER API (NEW ARCHITECTURE with lock-free freelist) =====
  bool io_try_flush_one() {
    std::string date_to_flush;
    size_t slot_idx = pool_size_;

    // Atomically find and transition DONE → FLUSH
    {
      std::lock_guard<std::mutex> lock(pool_mutex_);
      for (const auto &[date, idx] : date_to_slot_) {
        TensorState expected = TensorState::DONE;
        if (pool_[idx].state.compare_exchange_strong(expected, TensorState::FLUSH, std::memory_order_acq_rel, std::memory_order_acquire)) {
          date_to_flush = date;
          slot_idx = idx;
          break;
        }
      }
    }

    if (slot_idx >= pool_size_) {
      return false;
    }

    Slot &slot = pool_[slot_idx];

    // Copy date before writing (slot may be recycled)
    char date_copy[16];
    errno_t err = strncpy_s(date_copy, sizeof(date_copy), slot.date, _TRUNCATE);
    assert(err == 0);

    // Synchronous write + immediate flush (blocks until disk write completes)
    disk_write(date_copy, &slot);

    // Reset after write completes
    slot.reset(num_assets_, num_ts_workers_);
    slot.needs_reset = false;

    // Clean up: erase mapping, invalidate epoch, recycle slot
    {
      std::lock_guard<std::mutex> lock(pool_mutex_);
      date_to_slot_.erase(date_copy);

      // Invalidate epoch (makes all cached references stale)
      slot.epoch.fetch_add(1, std::memory_order_acq_rel);

      // Transition to FREE and push to freelist
      slot.state.store(TensorState::FREE, std::memory_order_release);
      freelist_push(slot_idx);
    }

    Logger::log("worker_" + std::to_string(io_worker_id_), "io_try_flush_one: " + std::string(date_copy) + " complete");
    return true;
  }

  // ===== QUERY API =====
  size_t query_F(size_t level_idx) const { return FIELDS_PER_LEVEL[level_idx]; }
  size_t query_A() const { return num_assets_; }
  size_t query_T(size_t level_idx) const { return MAX_ROWS_PER_LEVEL[level_idx]; }
  int query_cs_worker_id() const { return cs_worker_id_; }
  int query_io_worker_id() const { return io_worker_id_; }

  // ===== MACRO SUPPORT API (used by TS_WRITE_*/CS_READ_ALL/CS_WRITE_ALL) =====
  // TS worker get: cached with epoch validation, allocates on demand
  [[gnu::hot, gnu::always_inline]]
  inline Slot &ts_get_slot(const std::string &date, int worker_id) const {
    assert(worker_id >= 0 && worker_id < static_cast<int>(num_ts_workers_));

    // Fast path: cache hit with epoch validation
    if (ts_cache_[worker_id].date == date && ts_cache_[worker_id].slot_idx < pool_size_) [[likely]] {
      Slot &s = pool_[ts_cache_[worker_id].slot_idx];
      if (s.epoch.load(std::memory_order_acquire) == ts_cache_[worker_id].epoch) {
        return s;
      }
    }

    // Slow path: cache miss or epoch mismatch, need allocation
    return slot_get_ts_slow(date, worker_id);
  }

  // CS worker get: use cached slot (set by cs_wait)
  [[gnu::hot, gnu::always_inline]]
  inline Slot &cs_get_slot(const std::string &date) const {
    // Fast path: cache hit with epoch validation
    if (cs_cache_.date == date && cs_cache_.slot_idx < pool_size_) [[likely]] {
      Slot &s = pool_[cs_cache_.slot_idx];
      if (s.epoch.load(std::memory_order_acquire) == cs_cache_.epoch) {
        return s;
      }
    }

    // Slow path: cache miss or epoch mismatch
    std::lock_guard<std::mutex> lock(pool_mutex_);
    auto it = date_to_slot_.find(date);
    assert(it != date_to_slot_.end() && "CS accessing non-existent date");

    size_t slot_idx = it->second;
    Slot &s = pool_[slot_idx];
    [[maybe_unused]] TensorState state = s.state.load(std::memory_order_acquire);
    assert((state == TensorState::BUSY || state == TensorState::DONE) && "CS accessing wrong state");

    cs_cache_.date = date;
    cs_cache_.slot_idx = slot_idx;
    cs_cache_.epoch = s.epoch.load(std::memory_order_acquire);

    return s;
  }

private:
  // ===== INTERNAL HELPERS =====

  // Freelist management
  int freelist_pop() const {
    if (free_list_.empty()) {
      return -1;
    }
    int slot_idx = free_list_.back();
    free_list_.pop_back();
    return slot_idx;
  }

  void freelist_push(int slot_idx) const {
    free_list_.push_back(slot_idx);
  }

  // Slot management
  void slot_alloc_locked(const std::string &date, int worker_id, int wait_count = 0) const {
    constexpr int RETRY_MS = 10;

    if (date_to_slot_.find(date) != date_to_slot_.end()) {
      return;
    }

    int slot_idx = freelist_pop();

    if (slot_idx < 0) {
      if (wait_count == 0) {
        Logger::log("worker_" + std::to_string(worker_id), "Pool exhausted, waiting...");
      }
      pool_mutex_.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_MS));
      pool_mutex_.lock();
      return slot_alloc_locked(date, worker_id, wait_count + 1);
    }

    if (wait_count > 0) {
      int wait_ms = wait_count * RETRY_MS;
      Logger::log("worker_" + std::to_string(worker_id), "Pool slot available, resuming... (waited " + std::to_string(wait_ms) + "ms)");
    }

    Slot &s = pool_[slot_idx];

    s.epoch.fetch_add(1, std::memory_order_acq_rel);
    s.state.store(TensorState::INIT, std::memory_order_release);
    date_to_slot_[date] = slot_idx;

    Logger::log("worker_" + std::to_string(worker_id), "Bound " + date + " to pool[" + std::to_string(slot_idx) + "]");

    pool_mutex_.unlock();

    if (s.needs_reset) {
      s.reset(num_assets_, num_ts_workers_);
      Logger::log("worker_" + std::to_string(worker_id), "Pool[" + std::to_string(slot_idx) + "] reset complete");
    } else {
      Logger::log("worker_" + std::to_string(worker_id), "Pool[" + std::to_string(slot_idx) + "] skip reset (async)");
    }

    errno_t err = strncpy_s(s.date, sizeof(s.date), date.c_str(), _TRUNCATE);
    assert(err == 0);
    s.needs_reset = true;

    s.state.store(TensorState::BUSY, std::memory_order_release);
    Logger::log("worker_" + std::to_string(worker_id), "Published " + date + " to pool[" + std::to_string(slot_idx) + "] (BUSY)");

    pool_mutex_.lock();
  }

  [[gnu::cold, gnu::noinline]]
  Slot &slot_get_ts_slow(const std::string &date, int worker_id) const {
    pool_mutex_.lock();

    auto it = date_to_slot_.find(date);
    if (it != date_to_slot_.end()) {
      size_t slot_idx = it->second;
      Slot &s = pool_[slot_idx];
      TensorState state = s.state.load(std::memory_order_acquire);

      if (state == TensorState::INIT) {
        pool_mutex_.unlock();
        std::this_thread::sleep_for(std::chrono::microseconds(10));
        return slot_get_ts_slow(date, worker_id);
      }

      ts_cache_[worker_id].date = date;
      ts_cache_[worker_id].slot_idx = slot_idx;
      ts_cache_[worker_id].epoch = s.epoch.load(std::memory_order_acquire);
      pool_mutex_.unlock();
      return s;
    }

    slot_alloc_locked(date, worker_id);

    it = date_to_slot_.find(date);
    assert(it != date_to_slot_.end());
    size_t slot_idx = it->second;
    Slot &s = pool_[slot_idx];

    TensorState state = s.state.load(std::memory_order_acquire);
    if (state == TensorState::INIT) {
      pool_mutex_.unlock();
      std::this_thread::sleep_for(std::chrono::microseconds(10));
      return slot_get_ts_slow(date, worker_id);
    }

    ts_cache_[worker_id].date = date;
    ts_cache_[worker_id].slot_idx = slot_idx;
    ts_cache_[worker_id].epoch = s.epoch.load(std::memory_order_acquire);
    pool_mutex_.unlock();
    return s;
  }

  // Disk IO (write columnar compressed files)
  void disk_write(const std::string &date_str, Slot *slot) {
    assert(slot && date_str.size() == 8);

    auto t_start = std::chrono::high_resolution_clock::now();
    Logger::log("worker_" + std::to_string(io_worker_id_), "write_to_disk: START " + date_str + " (L0-columnar+L1/L2-merged)");

    std::string year = date_str.substr(0, 4);
    std::string month = date_str.substr(4, 2);
    std::string day_str = date_str.substr(6, 2);
    std::string out_dir = output_dir_ + "/" + year + "/" + month + "/" + day_str;

    auto t_before_mkdir = std::chrono::high_resolution_clock::now();
    std::error_code ec;
    std::filesystem::create_directories(out_dir, ec);
    auto t_after_mkdir = std::chrono::high_resolution_clock::now();

    const size_t T[3] = {MAX_ROWS_PER_LEVEL[0], MAX_ROWS_PER_LEVEL[1], MAX_ROWS_PER_LEVEL[2]};
    const size_t F[3] = {FIELDS_PER_LEVEL[0], FIELDS_PER_LEVEL[1], FIELDS_PER_LEVEL[2]};
    const size_t A = num_assets_;

    size_t total_files = 0;

    // L0: Write individual feature columns (for Dist analysis selective loading)
    for (size_t f = 0; f < F[0]; ++f) {
      const size_t col_elements = T[0] * A;
      const size_t col_bytes = col_elements * sizeof(feature_storage_t);
      std::vector<feature_storage_t> column(col_elements);

      for (size_t t = 0; t < T[0]; ++t) {
        for (size_t a = 0; a < A; ++a) {
          column[t * A + a] = slot->data[0][t * F[0] * A + f * A + a];
        }
      }

      auto compressed = ZstdHelper::compress(column.data(), col_bytes);

      std::string filename = out_dir + "/features_L0_f" + std::to_string(f) + ".zst";
      int fd;
      errno_t err = _sopen_s(&fd, filename.c_str(), _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY, _SH_DENYNO, _S_IREAD | _S_IWRITE);
      assert(err == 0 && fd >= 0);

      int ret = 0;
      ret = _write(fd, &T[0], sizeof(size_t));
      assert(ret == sizeof(size_t));
      const size_t F_single = 1;
      ret = _write(fd, &F_single, sizeof(size_t));
      assert(ret == sizeof(size_t));
      ret = _write(fd, &A, sizeof(size_t));
      assert(ret == sizeof(size_t));
      ret = _write(fd, compressed.data(), static_cast<unsigned int>(compressed.size()));
      assert(ret == static_cast<int>(compressed.size()));

      _commit(fd);
      _close(fd);
      total_files++;
    }

    // L1 and L2: Write merged compressed files (for GUI, no selective loading needed)
    for (size_t lvl = 1; lvl < 3; ++lvl) {
      const size_t total_elements = T[lvl] * F[lvl] * A;
      const size_t total_bytes = total_elements * sizeof(feature_storage_t);

      auto compressed = ZstdHelper::compress(slot->data[lvl], total_bytes);

      std::string filename = out_dir + "/features_L" + std::to_string(lvl) + ".zst";
      int fd;
      errno_t err = _sopen_s(&fd, filename.c_str(), _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY, _SH_DENYNO, _S_IREAD | _S_IWRITE);
      assert(err == 0 && fd >= 0);

      int ret = 0;
      ret = _write(fd, &T[lvl], sizeof(size_t));
      assert(ret == sizeof(size_t));
      ret = _write(fd, &F[lvl], sizeof(size_t));
      assert(ret == sizeof(size_t));
      ret = _write(fd, &A, sizeof(size_t));
      assert(ret == sizeof(size_t));
      ret = _write(fd, compressed.data(), static_cast<unsigned int>(compressed.size()));
      assert(ret == static_cast<int>(compressed.size()));

      _commit(fd);
      _close(fd);
      total_files++;
    }

    // Write depth.zst (compressed, single file)
    {
      const size_t T_depth = T[0];
      const size_t F_depth = DEPTH_TOTAL_WIDTH;
      const size_t total_elements = T_depth * F_depth * A;
      const size_t total_bytes = total_elements * sizeof(feature_storage_t);

      // Compress entire depth tensor
      auto compressed = ZstdHelper::compress(slot->depth_data, total_bytes);

      std::string filename = out_dir + "/depth.zst";
      int fd;
      errno_t err = _sopen_s(&fd, filename.c_str(), _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY, _SH_DENYNO, _S_IREAD | _S_IWRITE);
      assert(err == 0 && fd >= 0);

      int ret = 0;
      ret = _write(fd, &T_depth, sizeof(size_t));
      assert(ret == sizeof(size_t));
      ret = _write(fd, &F_depth, sizeof(size_t));
      assert(ret == sizeof(size_t));
      ret = _write(fd, &A, sizeof(size_t));
      assert(ret == sizeof(size_t));

      ret = _write(fd, compressed.data(), static_cast<unsigned int>(compressed.size()));
      assert(ret == static_cast<int>(compressed.size()));

      _commit(fd);
      _close(fd);
      total_files++;
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    auto mkdir_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_after_mkdir - t_before_mkdir).count();
    auto write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_after_mkdir).count();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();

    Logger::log("worker_" + std::to_string(io_worker_id_),
                "write_to_disk: END " + date_str +
                    " [files:" + std::to_string(total_files) + "(L0-columnar+L1/L2-merged+depth)" +
                    " mkdir:" + std::to_string(mkdir_ms) + "ms" +
                    " write:" + std::to_string(write_ms) + "ms" +
                    " total:" + std::to_string(total_ms) + "ms]");
  }

public:
  // ===== DEBUG API =====

  void dbg_dump(const std::string &date, int worker_id) const {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    std::string msg;

    // Line 1: Pool status (condensed)
    msg += "Pool[" + std::to_string(pool_size_) + "/" + std::to_string(date_to_slot_.size()) + "]: ";
    for (const auto &[d, slot_idx] : date_to_slot_) {
      char state_char = '?';
      switch (pool_[slot_idx].state.load(std::memory_order_acquire)) {
      case TensorState::FREE:
        state_char = 'F';
        break;
      case TensorState::INIT:
        state_char = 'I';
        break;
      case TensorState::BUSY:
        state_char = 'B';
        break;
      case TensorState::DONE:
        state_char = 'D';
        break;
      case TensorState::FLUSH:
        state_char = 'X';
        break;
      }
      msg += d + "[" + std::to_string(slot_idx) + ":" + state_char + "] ";
    }
    msg += "\n";

    // Line 2: Worker min_pos for specified date
    auto it = date_to_slot_.find(date);
    if (it != date_to_slot_.end()) {
      size_t slot_idx = it->second;
      Slot &slot = pool_[slot_idx];

      msg += date + " worker_min: ";
      for (size_t w = 0; w < num_ts_workers_; ++w) {
        uint64_t state = slot.ts_worker_state[w].load(std::memory_order_acquire);
        size_t min_pos = Slot::unpack_min_pos(state);
        bool done = Slot::unpack_done(state);
        msg += std::to_string(min_pos) + (done ? "✓" : "");
        if (w + 1 < num_ts_workers_)
          msg += ",";
      }
    } else {
      msg += date + " not found in pool";
    }

    Logger::log("worker_" + std::to_string(worker_id), msg);
  }
};

// ============================================================================
// PUBLIC API - 4 macros, only use field enums, 100% static compilation
// ============================================================================
// TS_WRITE_SINGLE(store, date, lvl, t, field_enum, asset, value, worker_id)
// TS_WRITE_FEATURES(store, date, lvl, t, asset, f_start, f_end, src, worker_id)
// CS_READ_ALL(store, date, lvl, t, field_enum)
// CS_WRITE_ALL(store, date, lvl, t, field_enum, src, count)
//
// Usage: Pass field enum directly (e.g., L0_FieldOffset::_data_valid)
//        lvl must be literal 0, 1, or 2 for static compilation
// ============================================================================

// TS_WRITE_SINGLE: Write single field for one asset
#define TS_WRITE_SINGLE(store, date, lvl, t, field_enum, a, value, worker_id) \
  do {                                                                        \
    auto &_slot = (store)->ts_get_slot(date, worker_id);                      \
    assert(_slot.data[lvl] && "data[lvl] is null");                           \
    const size_t _F = (store)->query_F(lvl);                                  \
    const size_t _A = (store)->query_A();                                     \
    [[maybe_unused]] const size_t _T = (store)->query_T(lvl);                 \
    const size_t _f = L##lvl##_FIELD_OFFSETS[field_enum];                     \
    assert((t) < _T && "time index out of bounds");                           \
    assert(_f < _F && "feature index out of bounds");                         \
    assert((a) < _A && "asset index out of bounds");                          \
    const size_t _idx = ((t) * _F + _f) * _A + (a);                           \
    _slot.data[lvl][_idx] = (value);                                          \
  } while (0)

// TS_WRITE_FEATURES: Write field range [f_start_enum, f_end_enum) for one asset
// Pass enums directly, macro converts to offsets via L##lvl##_FIELD_OFFSETS
#define TS_WRITE_FEATURES(store, date, lvl, t, a, f_start_enum, f_end_enum, src, worker_id) \
  do {                                                                                      \
    auto &_slot = (store)->ts_get_slot(date, worker_id);                                    \
    assert(_slot.data[lvl] && "data[lvl] is null");                                         \
    const size_t _F = (store)->query_F(lvl);                                                \
    const size_t _A = (store)->query_A();                                                   \
    [[maybe_unused]] const size_t _T = (store)->query_T(lvl);                               \
    const size_t _f_start = L##lvl##_FIELD_OFFSETS[f_start_enum];                           \
    const size_t _f_end = L##lvl##_FIELD_OFFSETS[f_end_enum];                               \
    assert((t) < _T && "time index out of bounds");                                         \
    assert((a) < _A && "asset index out of bounds");                                        \
    assert(_f_start <= _f_end && "invalid feature range");                                  \
    assert(_f_end <= _F && "feature end out of bounds");                                    \
    for (size_t _f = _f_start; _f < _f_end; ++_f) {                                         \
      const size_t _idx = ((t) * _F + _f) * _A + (a);                                       \
      _slot.data[lvl][_idx] = (src)[_f - _f_start];                                         \
    }                                                                                       \
  } while (0)

// DEPTH_WRITE_SINGLE: Write single depth field (similar to TS_WRITE_SINGLE but for depth store)
#define DEPTH_WRITE_SINGLE(store, date, t, field_enum, a, value, worker_id) \
  do {                                                                      \
    auto &_slot = (store)->ts_get_slot(date, worker_id);                    \
    assert(_slot.depth_data && "depth_data is null");                       \
    const size_t _F = DEPTH_TOTAL_WIDTH;                                    \
    const size_t _A = (store)->query_A();                                   \
    [[maybe_unused]] const size_t _T = (store)->query_T(0);                 \
    const size_t _f = DEPTH_FIELD_OFFSETS[field_enum];                      \
    assert((t) < _T && "time index out of bounds");                         \
    assert(_f < _F && "feature index out of bounds");                       \
    assert((a) < _A && "asset index out of bounds");                        \
    const size_t _idx = ((t) * _F + _f) * _A + (a);                         \
    _slot.depth_data[_idx] = (value);                                       \
  } while (0)

// DEPTH_WRITE_FEATURES: Write depth data (similar to TS_WRITE_FEATURES but for depth store)
#define DEPTH_WRITE_FEATURES(store, date, t, a, f_start_enum, f_end_enum, src, worker_id) \
  do {                                                                                    \
    auto &_slot = (store)->ts_get_slot(date, worker_id);                                  \
    assert(_slot.depth_data && "depth_data is null");                                     \
    const size_t _F = DEPTH_TOTAL_WIDTH;                                                  \
    const size_t _A = (store)->query_A();                                                 \
    [[maybe_unused]] const size_t _T = (store)->query_T(0);                               \
    const size_t _f_start = DEPTH_FIELD_OFFSETS[f_start_enum];                            \
    const size_t _f_end = DEPTH_FIELD_OFFSETS[f_end_enum];                                \
    assert((t) < _T && "time index out of bounds");                                       \
    assert((a) < _A && "asset index out of bounds");                                      \
    assert(_f_start <= _f_end && "invalid feature range");                                \
    assert(_f_end <= _F && "feature end out of bounds");                                  \
    for (size_t _f = _f_start; _f < _f_end; ++_f) {                                       \
      const size_t _idx = ((t) * _F + _f) * _A + (a);                                     \
      _slot.depth_data[_idx] = (src)[_f - _f_start];                                      \
    }                                                                                     \
  } while (0)

// CS_READ_ALL: Read all assets for one field → returns _Float16*
#define CS_READ_ALL(store, date, lvl, t, field_enum)          \
  [&]() -> feature_storage_t * {                              \
    auto &_slot = (store)->cs_get_slot(date);                 \
    assert(_slot.data[lvl] && "data[lvl] is null");           \
    const size_t _F = (store)->query_F(lvl);                  \
    const size_t _A = (store)->query_A();                     \
    [[maybe_unused]] const size_t _T = (store)->query_T(lvl); \
    const size_t _f = L##lvl##_FIELD_OFFSETS[field_enum];     \
    assert((t) < _T && "time index out of bounds");           \
    assert(_f < _F && "feature index out of bounds");         \
    const size_t _offset = ((t) * _F + _f) * _A;              \
    return _slot.data[lvl] + _offset;                         \
  }()

// CS_WRITE_ALL: Write all assets for one field
#define CS_WRITE_ALL(store, date, lvl, t, field_enum, src, count)                       \
  do {                                                                                  \
    auto &_slot = (store)->cs_get_slot(date);                                           \
    assert(_slot.data[lvl] && "data[lvl] is null");                                     \
    const size_t _F = (store)->query_F(lvl);                                            \
    const size_t _A = (store)->query_A();                                               \
    [[maybe_unused]] const size_t _T = (store)->query_T(lvl);                           \
    const size_t _f = L##lvl##_FIELD_OFFSETS[field_enum];                               \
    assert((t) < _T && "time index out of bounds");                                     \
    assert(_f < _F && "feature index out of bounds");                                   \
    assert((count) <= _A && "count exceeds num_assets");                                \
    const size_t _offset = ((t) * _F + _f) * _A;                                        \
    std::memcpy(_slot.data[lvl] + _offset, (src), (count) * sizeof(feature_storage_t)); \
  } while (0)
