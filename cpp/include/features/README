# Features Architecture

## GlobalFeatureStore Architecture Design

### Tensor Organization

Data is partitioned by date:
```
{[T, F, A]_level0(tick), [T, F, A]_level1(minute), [T, F, A]_level2(hour)}_dayN
```

**Dimensions:**
- T (Time):    max = 100,000  (100K time indices per day)
- A (Asset):   max = 1,000    (1K assets in universe)
- F (Feature): max = 1,000    (all feature types combined)

**Sub-features:**
- F_TS:  ~60-70%  (e.g., 600 TS features)
- F_CS:  ~20-30%  (e.g., 250 CS features)
- F_LB:  ~5-10%   (e.g., 50 Label features)
- F_OT:  ~5-10%   (e.g., 100 Other features(shared intermediate values))

### Access Pattern Analysis (Optimization Target: Minimize Total Memory Access Time)

| Operation | Loop Structure | Vector Access | Total Memory Access | Weight | Parallelism |
|-----------|----------------|---------------|---------------------|--------|-------------|
| TS_write_TS | for a: for t: write[F_TS] | Sequential write 600 | T×A×F_TS = 240GB | 39% | 10 cores |
| CS_read_TS | for t: for f: read[A] | Sequential read 1000 | T×(F_TS+F_OT)×A = 280GB | 45% | 1 core |
| CS_write_CS | for t: for f: write[A] | Sequential write 1000 | T×F_CS×A = 100GB | 16% | 1 core |

**Details:**
- TS_write: Each core processes ~100 assets, iterates over time for each asset, writes F_TS features sequentially at (a,t)
- CS_read: At each time t, for each feature f, read all A assets sequentially (cross-sectional calculation)
- CS_write: At each time t, for each feature f, write all A assets sequentially (cross-sectional result)

### Layout Comparison

| Layout | Address Formula | TS_write(39%) | CS_read(45%) | CS_write(16%) | Weighted Score | Recommendation |
|--------|-----------------|---------------|--------------|---------------|----------------|----------------|
| [T][A][F] | (t*A+a)*F+f | Sequential/100 | Jump 4KB/60 | Jump 4KB/60 | 75.6 | Good |
| **[T][F][A]** | **(t*F+f)*A+a** | **Jump 4KB/60** | **Sequential/100** | **Sequential/100** | **84.4** | **Optimal √√√** |
| [A][T][F] | (a*T+t)*F+f | Sequential/100 | Jump 400MB/5 | Jump 400MB/5 | 42.0 | Poor |
| [A][F][T] | (a*F+f)*T+t | Jump 400KB/20 | Jump 400MB/5 | Jump 400MB/5 | 10.9 | Very Poor |
| [F][T][A] | (f*T+t)*A+a | Jump 400MB/5 | Sequential/100 | Sequential/100 | 63.0 | Not Recommended |
| [F][A][T] | (f*A+a)*T+t | Jump 400MB/5 | Jump 400KB/20 | Jump 400KB/20 | 14.2 | Very Poor |

**Memory Stride Optimization:**
- stride=4B: Sequential access (SIMD/prefetch) optimization
- stride=64B: Cache line access optimization
- stride<4KB: TLB/Page optimization
- stride<32KB: L1 cache access optimization
- stride<1MB: L2 cache access optimization

## Tensor Pool Architecture - Event-Driven + Lock-Free Design

### Worker Division

N cores = (N-2) TS + 1 CS + 1 IO (separate CPU affinity settings)
- **TS Workers (10):** Time-series feature calculation, assigned by asset, date-first traversal (sequential, sparse, per-asset different at time index level, requires progress mechanism)
- **CS Worker (1):** Cross-sectional feature calculation, waits for TS completion (sync granularity must be fine-grained to time index level for consistency with live trading)
- **IO Worker (1):** Async flush, independently scans pool, writes to disk

### Core Data Structures

```cpp
// Tensor lifecycle states
enum class TensorState : uint8_t {
  FREE = 0,        // Available for allocation
  INIT = 1,        // Being initialized
  BUSY = 2,        // Active (TS writing + CS reading/writing)
  DONE = 3,        // CS finished, ready for flush
  FLUSH = 4        // IO worker writing to disk
};

// Per-date tensor slot (position-based sync + event-driven)
struct Slot {
  // State machine: FREE → INIT → BUSY → DONE → FLUSH → FREE
  std::atomic<TensorState> state;
  std::atomic<uint32_t> epoch;               // Generation number (detect slot recycling, use acq_rel)
  char date[16];                             // Currently bound date (format: "YYYYMMDD\0", fixed size, atomic copy)
  _Float16* data[3];                         // [T,F,A] tensors for L0/L1/L2

  // Synchronization mechanism (core: two-level write_pos, optimize CS scanning)
  std::atomic<size_t>* ts_write_pos;         // length = A (L0 write position for each asset)
                                             // ts_write_pos[a] = maximum L0 time index written for asset a + 1
                                             // TS worker: after writing (a, t), store(t+1, release)
                                             // Used for precise maintenance, supports fine-grained queries
  std::atomic<size_t>* ts_worker_min_pos;    // length = num_ts_workers (minimum write position for each TS worker)
                                             // ts_worker_min_pos[w] = min(write_pos) for all assets managed by worker w
                                             // TS worker: maintains thread-local min, periodically store(min, release)
                                             // CS: fast scan O(W) vs O(A), W=10, A=1000
  std::atomic<bool>* ts_done_flag;           // length = num_ts_workers (completion flag for each TS worker)
                                             // TS worker: after completing date, store(true, release)
                                             // CS: when all true, no longer wait for min_pos to grow

  // CS read position cache (avoid repeated scanning of ts_worker_min_pos)
  std::atomic<size_t> cs_read_pos;           // CS verified safe read position (atomic supports future multi-CS worker extension)
                                             // cs_read_pos = N means time_idx [0, N-1] is verified ready
                                             // CS sweeps ts_worker_min_pos each time to get min_pos,
                                             // can advance hundreds of time_idx at once (O(W) scan, very fast)
                                             // Current single CS worker: relaxed load/store suffices

  // Freelist linkage
  std::atomic<int> next_free;                // Freelist linked list pointer (-1 = end)
};

// TS worker cache entry (per-worker, lock-free hot path)
struct TSCacheEntry {
  std::string date;                          // Cached date
  int idx;                                   // Slot index in pool
  uint32_t epoch;                            // Cached epoch (detect slot recycling)
};

// CS worker cache entry (single-threaded)
struct CSCacheEntry {
  std::string date;                          // Cached date
  int idx;                                   // Slot index in pool
  uint32_t epoch;                            // Cached epoch (detect slot recycling)
};
```

### Global Variables

```cpp
Slot pool[POOL_SIZE];                      // Pre-allocated slot array
std::atomic<uint64_t> free_head{0};        // Lock-free stack head + ABA protection
                                           // Format: (tag << 32) | slot_idx
                                           // High 32 bits: version tag (prevent ABA)
                                           // Low 32 bits: slot index (-1 = empty)
                                           // Initial value: 0 (tag=0, idx=0→1→2...)
std::counting_semaphore<POOL_SIZE> pool_sem(POOL_SIZE);  // Semaphore, blocks allocation
                                           // acquire: before slot allocation; release: after slot reclaim
std::map<string, int> date_map;            // date → slot_idx mapping (requires mutex protection)
std::mutex map_mtx;                        // Only protects date_map
TSCacheEntry ts_cache[num_ts_workers];     // TS worker cache array (lock-free)
CSCacheEntry cs_cache;                     // CS worker cache (single-threaded)

// Event queues (lock-free/bounded)
lf_queue<pair<string,int>> ready_queue;    // (date, slot_idx) - CS waits for slot allocation completion
                                           // TS pushes after state=BUSY
lf_queue<int> flush_queue;                 // slot_idx - CS pushes after completion
                                           // IO worker pops and async writes to disk

// TS worker dynamic asset tracking (runtime recording, decouples business allocation logic)
std::unordered_set<size_t> assigned_assets_[num_ts_workers];  // Set of assets actually written by each worker
                                           // Automatically recorded on first write in ts_update()
                                           // Store layer learns allocation relationship through actual write behavior
                                           // Supports arbitrary load balancing strategies in business layer (no need to inform Store beforehand)
```

### Thread-local Variables

```cpp
thread_local size_t worker_local_min[num_ts_workers];  // Current worker_min cache
```

### Constants

```cpp
constexpr size_t A = 1000;                 // Maximum asset count
constexpr size_t T0 = 14400/1440000 + 1;   // L0 maximum time_idx (4 hours × 3600 sec/360000*10μs)
constexpr size_t F = 1000;                 // Maximum feature count
constexpr size_t num_ts_workers = 10;      // TS worker count
constexpr size_t POOL_SIZE = 30;           // Slot pool size
constexpr size_t PUBLISH_INTERVAL = 10;    // worker_min publish interval (every N writes)
```

### Memory Estimation (Design Maximum Capacity)

- data[L0]: T × F × A × 2B = 100,000 × 1,000 × 1,000 × 2 = 200 GB
- data[L1]: T × F × A × 2B (minute-level, T ~hundreds, F ~hundreds) ≈ hundreds of MB
- data[L2]: T × F × A × 2B (hour-level, T ~dozens, F ~hundreds) ≈ tens of MB
- remaining_count: T0 × 4B = 100,000 × 4 = 400 KB
- **Total per slot:** ~200 GB (L0 dominates)
- **Design limit:** Pool 30 slots = 6 TB (requires large memory machine or external storage)

**Current Test Configuration Example:**
- A=107, F0=15, T0=14401 → ~47 MB/slot, Pool 30 = 1.4 GB

**Pool Size Recommendation:** pool_size >= max(2 × TS_workers, TS_workers + 10)
  - Typical configuration: 10 TS workers → pool >= 20 slots

### Initialization

```cpp
void init_pool() {
  // 1. Initialize freelist: chain all slots into linked list
  for (size_t i = 0; i < POOL_SIZE - 1; ++i) {
    pool[i].next_free.store(i + 1, memory_order_relaxed);
    pool[i].state.store(FREE, memory_order_relaxed);
  }
  pool[POOL_SIZE - 1].next_free.store(-1, memory_order_relaxed);  // End
  pool[POOL_SIZE - 1].state.store(FREE, memory_order_relaxed);
  
  // 2. Initialize free_head: tag=0, idx=0 (points to first slot)
  free_head.store(0, memory_order_release);  // (0 << 32) | 0
  
  // 3. Initialize dynamic asset tracking sets (empty sets, auto-populated at runtime)
  //    assigned_assets_[w] = {}  // All worker sets initially empty
  //    Store layer doesn't need to know business layer's load balancing strategy
  //    Automatically records asset_id on first ts_update(date, worker_id, asset_id, t)
}
```

## Complete Data Flow - Pseudocode

### TS Worker: Get Slot

```cpp
int ts_get_slot(date, worker_id) {
  // 1. Check cache (lock-free)
  if (ts_cache[worker_id].date == date && pool[ts_cache[worker_id].idx].epoch.load(memory_order_acquire) == ts_cache[worker_id].epoch) {
    return ts_cache[worker_id].idx;  // Cache hit
  }

  // 2. Cache miss: need to allocate or wait for other worker's allocation completion
  //    Key: use map_mtx + state dual protection, avoid duplicate allocation for same date
  {
    lock_guard lk(map_mtx);
    auto it = date_map.find(date);
    
    // 2a. Already allocated by other worker: wait for state != INIT then use
    if (it != date_map.end()) {
      int slot_idx = it->second;
      lk.unlock();  // Release lock before waiting (avoid blocking other workers)
      
      // Spin-wait until slot initialization completes (INIT → BUSY)
      //   State order: INIT (allocating) → BUSY (available)
      //   Use acquire to ensure initialization operations are visible
      while (pool[slot_idx].state.load(memory_order_acquire) == INIT) {
        std::this_thread::yield();  // Brief wait, avoid busy-loop
      }
      
      // Update cache and return
      uint32_t epoch = pool[slot_idx].epoch.load(memory_order_acquire);
      ts_cache[worker_id] = {date, slot_idx, epoch};
      return slot_idx;
    }
    
    // 2b. We are first: allocate new slot (holding map_mtx)
    pool_sem.acquire();                        // Block until free slot available
    int slot_idx = pop_free();                 // CAS lock-free pop
    Slot &s = pool[slot_idx];
    
    // Immediately mark as INIT and insert into map (atomically publish allocation intent)
    s.state.store(INIT, memory_order_relaxed);
    s.epoch.fetch_add(1, memory_order_acq_rel);  // Invalidate old cache
    date_map[date] = slot_idx;  // Holding lock, atomic insert
  }  // Release map_mtx, allow other workers to discover this slot is initializing
  
  // 3. Initialize slot (expensive operation, not holding lock)
  int slot_idx = date_map[date];  // Safe: we already inserted into map
  Slot &s = pool[slot_idx];
  std::strncpy(s.date, date.c_str(), sizeof(s.date) - 1);  // Fixed size copy "YYYYMMDD"
  s.date[sizeof(s.date) - 1] = '\0';  // Ensure null-terminated
  
  // 3a. Initialize sync variables
  for (int a = 0; a < A; ++a) {
    s.ts_write_pos[a].store(0, memory_order_relaxed);  // A = total assets
  }
  for (int w = 0; w < num_ts_workers; ++w) {
    s.ts_worker_min_pos[w].store(0, memory_order_relaxed);
    s.ts_done_flag[w].store(false, memory_order_relaxed);
  }
  s.cs_read_pos.store(0, memory_order_relaxed);
  memset(s.data, 0, ...);  // Zero data
  
  // 4. Atomic publish: transition state to BUSY (ensure all initialization visible to subsequent readers)
  s.state.store(BUSY, memory_order_release);
  
  // 5. Notify CS worker: slot is ready (event-driven, avoid CS polling map)
  ready_queue.push({date, slot_idx});
  
  // 6. Update cache
  ts_cache[worker_id] = {date, slot_idx, s.epoch.load(memory_order_relaxed)};
  
  // 7. Initialize thread-local worker_min (new date start)
  worker_local_min[worker_id] = 0;
  
  return slot_idx;
}
```

### TS Worker: Helper Function (Incremental Min Maintenance Optimization)

```cpp
void update_local_min(int worker_id, int a, size_t old_pos, size_t new_pos, Slot &s) {
  // Incrementally maintain worker_min (avoid scanning all assets each time)
  if (old_pos == worker_local_min[worker_id]) {
    // Modified value was current min, rescan
    size_t new_min = SIZE_MAX;
    for (int a : assigned_assets_[worker_id]) {  // Scan actually written assets
      new_min = std::min(new_min, s.ts_write_pos[a].load(memory_order_relaxed));
    }
    worker_local_min[worker_id] = new_min;
  } else {
    // Modified value wasn't min, simple update
    worker_local_min[worker_id] = std::min(worker_local_min[worker_id], new_pos);
  }
}
```

### TS Worker: Write Feature (Incremental Worker_min Maintenance)

```cpp
void ts_write(date, t, a, worker_id, data) {
  int slot_idx = ts_get_slot(date, worker_id);
  Slot &s = pool[slot_idx];

  // 1. Dynamically record asset (auto-insert on first write, decouples business allocation logic)
  assigned_assets_[worker_id].insert(a);  // O(1) amortized, duplicate insert no side effect

  // 2. Write data to slot (normal write, not atomic)
  //    Order: record asset → write_data → store write_pos → update local_min → publish worker_min
  write_data(s, t, a, data);

  // 3. Get old position (for incremental min maintenance)
  size_t old_pos = s.ts_write_pos[a].load(memory_order_relaxed);

  // 4. Update asset write position (memory_order_release)
  //    release: ensure data write visible to subsequent acquire
  //    Note: each TS worker manages different assets, no contention, simple store suffices
  size_t new_pos = t + 1;
  s.ts_write_pos[a].store(new_pos, memory_order_release);

  // 5. Incrementally maintain worker_min (thread-local cache)
  //    Only need to rescan when old_pos == worker_min
  update_local_min(worker_id, a, old_pos, new_pos, s);

  // 6. Publish worker_min_pos
  size_t local_min = worker_local_min[worker_id];  // Return cached worker_min
  s.ts_worker_min_pos[worker_id].store(local_min, memory_order_release);
  
  // 7. CS determines time_idx ready by scanning min(ts_worker_min_pos[all workers])
  //    Scan complexity: O(W) vs O(A), W=10, A=1000, 100x faster
}
```

### TS Worker: Done Marker

```cpp
void ts_done(date, worker_id) {
  int slot_idx = ts_cache[worker_id].idx;
  Slot &s = pool[slot_idx];

  // 1. Batch update all actually written asset positions to final value (use relaxed for performance)
  //    Dynamic recording mechanism: only update actually written assets, no need to know allocation beforehand
  for (int a : assigned_assets_[worker_id]) {
    s.ts_write_pos[a].store(T0, memory_order_relaxed);
  }

  // 2. Insert fence to ensure above writes visible to subsequent acquire
  //    Key: release fence establishes happens-before relationship
  //    CS's acquire load can see all relaxed stores before fence
  std::atomic_thread_fence(memory_order_release);

  // 3. Publish final worker_min_pos = T0 (ensure CS fast scan doesn't block)
  s.ts_worker_min_pos[worker_id].store(T0, memory_order_release);

  // 4. Finally mark this worker done (memory_order_release)
  //    CS reads this flag with acquire, establishing sync point
  s.ts_done_flag[worker_id].store(true, memory_order_release);

  // 5. Clear cache (avoid stale access, prevent subsequent misuse)
  ts_cache[worker_id] = {"",-1,0};
}
```

### CS Worker: Wait for Time_idx Ready (Two-level Position Sweep)

```cpp
void cs_wait(date, t) {
  // 1. Get slot (cache hit or get from ready_queue)
  Slot &s = cs_get_slot(date);  // Details in cs_get_slot below

  // 2. Fast path: cs_read_pos cache already verified read position
  //    If t < cs_read_pos, this time_idx is verified ready, return directly
  if (t < s.cs_read_pos.load(memory_order_relaxed)) [[likely]] {
    return;  // Cache hit, no scan needed (single CS worker uses relaxed)
  }

  // 3. Slow path: scan all workers' ts_worker_min_pos, calculate min_pos
  //    (O(W) scan, W=10, very fast)
  size_t backoff_us = 1;  // Initial backoff time 1us
  while (true) {
    size_t min_pos = SIZE_MAX;
    for (size_t w = 0; w < num_ts_workers; ++w) {
      min_pos = std::min(min_pos,
                         s.ts_worker_min_pos[w].load(memory_order_acquire));
    }

    // 4. If min_pos > t, all assets have written past t
    //    Update cs_read_pos = min_pos (one sweep advances hundreds of time_idx)
    if (min_pos > t) {
      s.cs_read_pos.store(min_pos, memory_order_relaxed);  // Batch verify [cs_read_pos, min_pos) ready
      return;  // This time_idx ready
    }

    // 5. If all TS workers are done, min_pos won't grow anymore
    //    Return directly (handles cases where some assets have no data)
    bool all_done = true;
    for (size_t w = 0; w < num_ts_workers; ++w) {
      if (!s.ts_done_flag[w].load(memory_order_acquire)) {
        all_done = false;
        break;
      }
    }
    if (all_done) {
      s.cs_read_pos.store(T0, memory_order_relaxed);  // Force all time_idx ready
      return;
    }

    // 6. Wait and retry (exponential backoff to avoid busy-loop)
    //    Backoff strategy: 1us → 2us → 4us → ... → 100us (cap)
    //    Reason: TS publish frequency ~every 10 writes, interval < 1us, initial 1us sufficient
    std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
    backoff_us = std::min(backoff_us * 2, 100UL);  // Exponential growth, max 100us
  }
}
```

### CS Worker: Get Slot (Cache + Event Queue)

```cpp
Slot& cs_get_slot(const string& date) {
  // Cache hit
  if (cs_cache.date == date) {
    int slot_idx = cs_cache.idx;
    uint32_t epoch = cs_cache.epoch;
    if (pool[slot_idx].epoch.load(memory_order_acquire) == epoch) {
      return pool[slot_idx];  // Cache valid
    }
  }

  // Cache miss: get from ready_queue (blocking wait for TS allocation completion)
  pair<string, int> ev;
  while (true) {
    if (ready_queue.pop_blocking(ev)) {
      if (ev.first == date) {
        int slot_idx = ev.second;
        uint32_t epoch = pool[slot_idx].epoch.load(memory_order_acquire);
        cs_cache = {date, slot_idx, epoch};
        return pool[slot_idx];
      } else {
        // Not the current date to process, push back to queue (or cache multiple dates)
        ready_queue.push(ev);
      }
    }
  }
}
```

### CS Worker: Done Marker

```cpp
void cs_done(date) {
  int slot_idx = cs_cache.idx;
  Slot &s = pool[slot_idx];

  // 1. Wait for all TS workers to complete (ensure no new data writes)
  //    Use exponential backoff to avoid busy-loop
  size_t backoff_us = 1;
  while (true) {
    bool all_done = true;
    for (size_t w = 0; w < num_ts_workers; ++w) {
      if (!s.ts_done_flag[w].load(memory_order_acquire)) {
        all_done = false;
        break;
      }
    }
    if (all_done) break;
    
    std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
    backoff_us = std::min(backoff_us * 2, 100UL);  // Exponential growth, max 100us
  }

  // 2. CAS transition state BUSY → DONE (atomic state transition)
  TensorState expected = BUSY;
  if (!s.state.compare_exchange_strong(expected, DONE,
                                       memory_order_acq_rel,
                                       memory_order_relaxed)) {
    // Abnormal state, log error (theoretically shouldn't happen)
    // expected now contains actual state, can be used for debugging
  }

  // 3. Notify IO worker to flush (event-driven)
  flush_queue.push(slot_idx);

  // 4. Clear cache (prevent subsequent misuse)
  cs_cache = {"", -1, 0};
}
```

### CS Worker: Helper Functions

```cpp
float* cs_read(Slot &s, size_t lvl, size_t t, size_t f) {
  // Return pointer to all A assets at (lvl, t, f)
  // Ensure cs_wait(date, t) has verified time_idx t ready before calling
  return &s.data[lvl][(t * F + f) * A];
}

void cs_write(Slot &s, size_t lvl, size_t t, size_t f, const _Float16* src, size_t count) {
  // Write first count assets at (lvl, t, f)
  memcpy(&s.data[lvl][(t * F + f) * A], src, count * sizeof(_Float16));
}
```

### IO Worker: Main Loop (Event-Driven)

```cpp
void io_flush() {
  while (running) {
    // 1. Blocking get next slot to flush from flush_queue
    //    flush_queue is pushed by CS worker in cs_done
    int slot_idx;
    if (!flush_queue.pop_blocking(slot_idx)) continue;  // Blocking wait

    Slot &s = pool[slot_idx];

    // 2. CAS transition state DONE → FLUSH (atomic preemption, prevent duplicate flush)
    TensorState expected = DONE;
    if (!s.state.compare_exchange_strong(expected, FLUSH,
                                         memory_order_acq_rel,
                                         memory_order_relaxed)) {
      continue;  // Wrong state (slot already reclaimed or other anomaly), skip
    }

    // 3. Async write to disk (not holding lock, exclusive slot)
    //    Can use io_uring/AIO internally for async I/O
    //    Simplified here as sync interface (actual implementation can be async)
    char date_copy[16];  // Copy date (avoid modification during disk write)
    std::strncpy(date_copy, s.date, sizeof(date_copy));
    date_copy[sizeof(date_copy) - 1] = '\0';
    write_disk(date_copy, s);   // Async write to disk (can block until complete)

    // 4. Reclaim slot
    {
      lock_guard lk(map_mtx);
      date_map.erase(date_copy);  // Use copied date string
    }

    // 5. Invalidate cache and reclaim slot
    s.epoch.fetch_add(1, memory_order_acq_rel);  // Invalidate all caches
    s.state.store(FREE, memory_order_release);   // Publish FREE state
    push_free(slot_idx);   // CAS lock-free push
    pool_sem.release();    // Wake blocked TS worker
  }
}
```

### Lock-Free Freelist Operations (Tagged Pointer Prevents ABA)

```cpp
int pop_free() {
  while (true) {
    uint64_t head_tagged = free_head.load(memory_order_acquire);
    int head_idx = static_cast<int>(head_tagged & 0xFFFFFFFF);
    uint32_t head_tag = static_cast<uint32_t>(head_tagged >> 32);
    
    if (head_idx < 0) return -1;  // Empty (defensive check)
    
    int next_idx = pool[head_idx].next_free.load(memory_order_relaxed);
    uint64_t next_tagged = (static_cast<uint64_t>(head_tag + 1) << 32) | static_cast<uint32_t>(next_idx);
    
    // CAS: success uses acq_rel (acquire old head, publish new head, tag+1 prevents ABA)
    if (free_head.compare_exchange_weak(head_tagged, next_tagged,
                                        memory_order_acq_rel,
                                        memory_order_acquire)) {
      return head_idx;  // Successfully popped
    }
    // CAS failed (modified by other thread or ABA), retry
  }
}

void push_free(int slot_idx) {
  while (true) {
    uint64_t old_head_tagged = free_head.load(memory_order_acquire);
    int old_head_idx = static_cast<int>(old_head_tagged & 0xFFFFFFFF);
    uint32_t old_tag = static_cast<uint32_t>(old_head_tagged >> 32);
    
    pool[slot_idx].next_free.store(old_head_idx, memory_order_relaxed);
    uint64_t new_head_tagged = (static_cast<uint64_t>(old_tag + 1) << 32) | static_cast<uint32_t>(slot_idx);
    
    // CAS: success uses acq_rel (acquire old head, publish new head, tag+1 prevents ABA)
    if (free_head.compare_exchange_weak(old_head_tagged, new_head_tagged,
                                        memory_order_acq_rel,
                                        memory_order_acquire)) {
      return;  // Successfully pushed
    }
    // CAS failed (modified by other thread or ABA), retry
  }
}
```

## Memory Order Summary

Key operations and their memory_order choices:

| Operation | Memory Order | Reason |
|-----------|--------------|--------|
| epoch.fetch_add | acq_rel | Sync slot invalidation, bidirectional visibility |
| state.store(BUSY) | release | Publish slot initialization complete |
| state.load | acquire | Acquire slot state and sync data |
| state.compare_exchange_strong | acq_rel, relaxed | State transition needs bidirectional sync |
| ts_write_pos[a].store(0) | relaxed | Initialization no sync needed (subsequent release establishes) |
| ts_write_pos[a].store(t+1) | release | Publish data write complete (write → store pos) |
| ts_write_pos[a].load | acquire | Read position and sync data (debug only) |
| ts_worker_min_pos[w].store(0) | relaxed | Initialization no sync needed |
| ts_worker_min_pos[w].store(min) | release | Publish worker minimum position |
| ts_worker_min_pos[w].load | acquire | CS reads worker min and syncs |
| ts_done_flag[w].store(false) | relaxed | Initialization no sync needed |
| ts_done_flag[w].store(true) | release | Publish worker done state |
| ts_done_flag[w].load | acquire | Read done state and sync |
| cs_read_pos.store | relaxed | Single CS worker exclusive write, no contention |
| cs_read_pos.load | relaxed | Single CS worker exclusive read, no contention |
| free_head CAS (tagged) | acq_rel, acquire | Freelist pop/push bidirectional sync + ABA protection<br>Format: (tag << 32) \| idx, tag++ prevents ABA |

## Time Mapping

- **L0 ↔ L1/L2:** L0 contains `_link_to_L1`, `_link_to_L2` two META features
- Stored as `_Float16`, value is L1/L2 time index
- Supports non-uniform mapping (multiple L0 ticks point to same L1 minute)

## Export Format

Output directory: `output/features/YYYY/MM/DD/`
- `features_L0.bin`: [T0, F0, A] (includes link features)
- `features_L1.bin`: [T1, F1, A]
- `features_L2.bin`: [T2, F2, A]

## Public API Quick Reference

**TS Worker:**
- `ts_write(date, t, a, worker_id, data)` - Write feature
- `ts_update(date, worker_id, asset_id, l0_t)` - Update time progress
- `ts_done(date, worker_id)` - Done marker

**CS Worker:**
- `cs_wait(date, t)` - Wait for time_idx ready
- `cs_read(slot, lvl, t, f) → _Float16*` - Read all assets
- `cs_write(slot, lvl, t, f, src, count)` - Write all assets
- `cs_done(date)` - Done marker

**IO Worker:**
- `io_flush()` - Main loop

**Freelist:**
- `pop_free() → slot_idx` - Pop free slot
- `push_free(slot_idx)` - Push free slot

**Metadata Query:**
- `query_F(lvl)`, `query_A()`, `query_T(lvl)`, `query_num_dates()`

