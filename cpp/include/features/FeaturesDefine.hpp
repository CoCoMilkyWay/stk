#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

// ============================================================================
// GlobalFeatureStore 架构设计
// ============================================================================
//
// 按照日期, 切分Tensor: {[T, F, A]_level0(tick), [T, F, A]_level1(minute), [T, F, A]_level2(hour)}_dayN
//
// T (Time):    max = 100,000  (100K time indices per day)
// A (Asset):   max = 1,000    (1K assets in universe)
// F (Feature): max = 1,000    (all feature types combined)
//
// 子特征例子:
// F_TS: (600 时序特征) F_CS (250 截面特征) F_LB (50 标签) F_SH (50 共享中间值) F_META (50 元数据)
//
// 访问模式分析 (优化目标: 最小化总内存访问时间)
//
// 操作           循环结构                       单次vector访问   总内存访问量                权重    并行度
// TS_write_TS    for a: for t: write[F_TS]     连续写600个      TxAxF_TS        = 240GB     39%   10 cores
// CS_read_TS     for t: for f: read[A]         连续读1000个     Tx(F_TS+F_OT)xA = 280GB     45%   1 core
// CS_write_CS    for t: for f: write[A]        连续写1000个     TxF_CSxA        = 100GB     16%   1 core
//
// 详细说明:
// - TS_write: 每个core处理~100个assets, 对每个asset遍历时间, 在(a,t)处连续写F_TS个features
// - CS_read:  在每个时刻t, 对每个feature f, 连续读取所有A个assets的值(截面计算)
// - CS_write: 在每个时刻t, 对每个feature f, 连续写入所有A个assets的值(截面结果)
//
// 布局          地址公式               TS_write(39%)      CS_read(45%)      CS_write(16%)   加权总分   推荐度
// [T][A][F]    (t*A+a)*F+f            连续/100分         跳4KB/60分        跳4KB/60分        75.6      良好
// [T][F][A]    (t*F+f)*A+a            跳4KB/60分         连续/100分        连续/100分        84.4      最优 √√√
// [A][T][F]    (a*T+t)*F+f            连续/100分         跳400MB/5分       跳400MB/5分       42.0      较差
// [A][F][T]    (a*F+f)*T+t            跳400KB/20分       跳400MB/5分       跳400MB/5分       10.9      极差
// [F][T][A]    (f*T+t)*A+a            跳400MB/5分        连续/100分        连续/100分        63.0      不推荐
// [F][A][T]    (f*A+a)*T+t            跳400MB/5分        跳400KB/20分      跳400KB/20分      14.2      极差
//
// stride=4B:连续访问(SIMD/prefetch)优化; 
// stride=64B: cache line访问优化;
// stride<4KB: TLB/Page优化; 
// stride<32KB: L1访问优化; 
// stride<1MB: L2访问优化; 
//
// ============================================================================
// 【Tensor Pool 架构 - 事件驱动 + 无锁设计
// ============================================================================
//
// 【Worker 分工】N cores = (N-2) TS + 1 CS + 1 IO (分别设置cpu亲和)
//   - TS Workers (10): 时序特征计算,按 asset 分配,date-first 遍历(在time index级别是 顺序, 稀疏, 而且per asset不同的, 所以需要progress机制)
//   - CS Worker  (1):  截面特征计算,等待 TS 完成后处理 (与TS的sync粒度必须细, 到time index级别, 这样和实盘行为才是一致的)
//   - IO Worker  (1):  异步 flush,独立扫描 pool,写磁盘
//
// 【核心数据结构】
//
//   // Tensor lifecycle states
//   enum class TensorState : uint8_t {
//     FREE = 0,        // Available for allocation
//     INIT = 1,        // Being initialized
//     BUSY = 2,        // Active (TS writing + CS reading/writing)
//     DONE = 3,        // CS finished, ready for flush
//     FLUSH = 4        // IO worker writing to disk
//   };
//
//   // Per-date tensor slot (position-based 同步 + 事件驱动)
//   struct Slot {
//     // 状态机: FREE → INIT → BUSY → DONE → FLUSH → FREE
//     std::atomic<TensorState> state;
//     std::atomic<uint32_t> epoch;               // Generation number (检测 slot 回收, 用 acq_rel)
//     char date[16];                             // 当前绑定的日期 (format: "YYYYMMDD\0", 固定大小, 可原子拷贝)
//     _Float16* data[3];                         // [T,F,A] tensors for L0/L1/L2
//
//     // 同步机制(核心:two-level write_pos,优化 CS 扫描)
//     std::atomic<size_t>* ts_write_pos;         // length = A (每个 asset 的 L0 写入位置)
//                                                // ts_write_pos[a] = asset a 已写入的最大 L0 时间索引 + 1
//                                                // TS worker: 写完 (a, t) 后 store(t+1, release)
//                                                // 用于准确维护,支持细粒度查询
//     std::atomic<size_t>* ts_worker_min_pos;    // length = num_ts_workers (每个 TS worker 的最小写入位置)
//                                                // ts_worker_min_pos[w] = worker w 负责的所有 assets 的 min(write_pos)
//                                                // TS worker: 维护 thread-local min,定期 store(min, release)
//                                                // CS: 快速扫描 O(W) vs O(A),W=10, A=1000
//     std::atomic<bool>* ts_done_flag;           // length = num_ts_workers (每个 TS worker 的完成标志)
//                                                // TS worker: 完成 date 后 store(true, release)
//                                                // CS: 全部 true 时不再等待 min_pos 增长
//
//     // CS 读取位置缓存(避免重复扫描 ts_worker_min_pos)
//     std::atomic<size_t> cs_read_pos;           // CS 已验证的安全读位置 (atomic 支持未来多 CS worker 扩展)
//                                                // cs_read_pos = N 表示 [0, N-1] 的 time_idx 已验证就绪
//                                                // CS 每次 sweep 扫描 ts_worker_min_pos 得到 min_pos,
//                                                // 一次可以推进几百个 time_idx(O(W) 扫描,极快)
//                                                // 当前单 CS worker: 使用 relaxed load/store 即可
//
//     // Freelist linkage
//     std::atomic<int> next_free;                // Freelist 链表指针 (-1 = end)
//   };
//
//   // TS worker cache entry (per-worker, lock-free hot path)
//   struct TSCacheEntry {
//     std::string date;                          // Cached date
//     int idx;                                   // Slot index in pool
//     uint32_t epoch;                            // Cached epoch (detect slot recycling)
//   };
//
//   // CS worker cache entry (single-threaded)
//   struct CSCacheEntry {
//     std::string date;                          // Cached date
//     int idx;                                   // Slot index in pool
//     uint32_t epoch;                            // Cached epoch (detect slot recycling)
//   };
//
//   全局变量:
//     Slot pool[POOL_SIZE];                      // 预分配 slot 数组
//     std::atomic<uint64_t> free_head{0};        // 无锁栈头 + ABA 防护
//                                                // 格式: (tag << 32) | slot_idx
//                                                // 高 32 位: version tag (防 ABA)
//                                                // 低 32 位: slot index (-1 = empty)
//                                                // 初始值: 0 (tag=0, idx=0→1→2...)
//     std::counting_semaphore<POOL_SIZE> pool_sem(POOL_SIZE);  // 信号量,阻塞分配
//                                                // acquire: 分配 slot 前; release: 回收 slot 后
//     std::map<string, int> date_map;            // date → slot_idx 映射 (需要 mutex 保护)
//     std::mutex map_mtx;                        // 仅保护 date_map
//     TSCacheEntry ts_cache[num_ts_workers];     // TS worker cache 数组 (lock-free)
//     CSCacheEntry cs_cache;                     // CS worker cache (单线程)
//
//     // 事件队列(无锁/有界)
//     lf_queue<pair<string,int>> ready_queue;    // (date, slot_idx) - CS 等待 slot 分配完成
//                                                // TS 在 state=BUSY 后 push
//     lf_queue<int> flush_queue;                 // slot_idx - CS 在完成后 push
//                                                // IO worker pop 并异步写盘
//
//     // TS worker 动态资产跟踪(运行时记录,解耦业务分配逻辑)
//     std::unordered_set<size_t> assigned_assets_[num_ts_workers];  // 每个 worker 实际写过的 asset 集合
//                                                // 在 ts_update() 首次写入时自动记录
//                                                // Store 层通过实际写入行为学习分配关系
//                                                // 支持业务层任意负载均衡策略(无需预先告知 Store)
//
//   Thread-local 变量 (每个 TS worker 独立维护):
//     thread_local size_t worker_local_min[num_ts_workers];  // 当前 worker_min 缓存
//
//   常量定义:
//     constexpr size_t A = 1000;                 // 最大 asset 数量
//     constexpr size_t T0 = 14400/1440000 + 1;   // L0 最大 time_idx (4 小时 x 3600 秒/360000*10微秒)
//     constexpr size_t F = 1000;                 // 最大 feature 数量
//     constexpr size_t num_ts_workers = 10;      // TS worker 数量
//     constexpr size_t POOL_SIZE = 30;           // Slot pool 大小
//     constexpr size_t PUBLISH_INTERVAL = 10;    // worker_min 发布间隔(每 N 次写入)
//
//   内存估算 (按设计最大容量):
//     - data[L0]: T x F x A x 2B = 100,000 x 1,000 x 1,000 x 2 = 200 GB
//     - data[L1]: T x F x A x 2B (分钟级,T 约几百,F 约几百) ≈ 数百 MB
//     - data[L2]: T x F x A x 2B (小时级,T 约十几,F 约几百) ≈ 数十 MB
//     - remaining_count: T0 x 4B = 100,000 x 4 = 400 KB
//     - Total per slot: ~200 GB (L0 占主导)
//     - 设计上限: Pool 30 slots = 6 TB (需大内存机器或外存支持)
//
//   实际配置示例 (当前测试规模):
//     - A=107, F0=15, T0=14401 → ~47 MB/slot, Pool 30 = 1.4 GB
//
//   Pool Size 建议: pool_size >= max(2 x TS_workers, TS_workers + 10)
//                   典型配置: 10 TS workers → pool >= 20 slots
//
//   辅助函数声明:
//     void write_data(Slot &s, size_t t, size_t a, const data&);  // 写入特征数据到 slot
//     void write_disk(const char date[16], Slot &s);   // 异步写盘
//     std::atomic<bool> running{true};               // IO worker 运行标志
//
//   初始化:
//     void init_pool() {
//       // 1. 初始化 freelist: 所有 slot 串成链表
//       for (size_t i = 0; i < POOL_SIZE - 1; ++i) {
//         pool[i].next_free.store(i + 1, memory_order_relaxed);
//         pool[i].state.store(FREE, memory_order_relaxed);
//       }
//       pool[POOL_SIZE - 1].next_free.store(-1, memory_order_relaxed);  // 末尾
//       pool[POOL_SIZE - 1].state.store(FREE, memory_order_relaxed);
//       
//       // 2. 初始化 free_head: tag=0, idx=0 (指向第一个 slot)
//       free_head.store(0, memory_order_release);  // (0 << 32) | 0
//       
//       // 3. 初始化动态资产跟踪集合 (空集合,运行时自动填充)
//       //    assigned_assets_[w] = {}  // 所有 worker 的集合初始为空
//       //    Store 层无需知道业务层的负载均衡策略
//       //    首次 ts_update(date, worker_id, asset_id, t) 时自动记录 asset_id
//     }
//
//
// ============================================================================
// 【完整数据流 - 伪代码】
// ============================================================================
//
// === TS Worker: 获取 slot ===
// int ts_get_slot(date, worker_id) {
//   // 1. 检查 cache (lock-free)
//   if (ts_cache[worker_id].date == date && pool[ts_cache[worker_id].idx].epoch.load(memory_order_acquire) == ts_cache[worker_id].epoch) {
//     return ts_cache[worker_id].idx;  // Cache hit
//   }
//
//   // 2. Cache miss: 需要分配或等待其他 worker 分配完成
//   //    关键: 使用 map_mtx + state 双重保护,避免重复分配同一 date
//   {
//     lock_guard lk(map_mtx);
//     auto it = date_map.find(date);
//     
//     // 2a. 其他 worker 已分配: 等待 state != INIT 后使用
//     if (it != date_map.end()) {
//       int slot_idx = it->second;
//       lk.unlock();  // 释放锁后等待(避免阻塞其他 worker)
//       
//       // Spin-wait 直到 slot 初始化完成(INIT → BUSY)
//       //   state 顺序: INIT (分配中) → BUSY (可用)
//       //   使用 acquire 确保看到初始化操作
//       while (pool[slot_idx].state.load(memory_order_acquire) == INIT) {
//         std::this_thread::yield();  // 短暂等待,避免 busy-loop
//       }
//       
//       // 更新 cache 并返回
//       uint32_t epoch = pool[slot_idx].epoch.load(memory_order_acquire);
//       ts_cache[worker_id] = {date, slot_idx, epoch};
//       return slot_idx;
//     }
//     
//     // 2b. 我们是第一个: 分配新 slot (持有 map_mtx)
//     pool_sem.acquire();                        // 阻塞直到有空闲 slot
//     int slot_idx = pop_free();                 // CAS 无锁弹出
//     Slot &s = pool[slot_idx];
//     
//     // 立即标记为 INIT 并插入 map (原子发布分配意图)
//     s.state.store(INIT, memory_order_relaxed);
//     s.epoch.fetch_add(1, memory_order_acq_rel);  // 使旧 cache 失效
//     date_map[date] = slot_idx;  // 持有锁,原子插入
//   }  // 释放 map_mtx,允许其他 worker 发现此 slot 正在初始化
//   
//   // 3. 初始化 slot (昂贵操作,不持有锁)
//   int slot_idx = date_map[date];  // Safe: 我们已插入 map
//   Slot &s = pool[slot_idx];
//   std::strncpy(s.date, date.c_str(), sizeof(s.date) - 1);  // 固定大小拷贝 "YYYYMMDD"
//   s.date[sizeof(s.date) - 1] = '\0';  // 确保 null-terminated
//   
//   // 3a. 初始化同步变量
//   for (int a = 0; a < A; ++a) {
//     s.ts_write_pos[a].store(0, memory_order_relaxed);  // A = total assets
//   }
//   for (int w = 0; w < num_ts_workers; ++w) {
//     s.ts_worker_min_pos[w].store(0, memory_order_relaxed);
//     s.ts_done_flag[w].store(false, memory_order_relaxed);
//   }
//   s.cs_read_pos.store(0, memory_order_relaxed);
//   memset(s.data, 0, ...);  // 清零数据
//   
//   // 4. 原子发布: 状态转为 BUSY (确保所有初始化对后续 reader 可见)
//   s.state.store(BUSY, memory_order_release);
//   
//   // 5. 通知 CS worker: slot 已可用 (事件驱动,避免 CS 轮询 map)
//   ready_queue.push({date, slot_idx});
//   
//   // 6. 更新 cache
//   ts_cache[worker_id] = {date, slot_idx, s.epoch.load(memory_order_relaxed)};
//   
//   // 7. 初始化 thread-local worker_min (新 date 开始)
//   worker_local_min[worker_id] = 0;
//   
//   return slot_idx;
// }
//
// === TS Worker: 辅助函数 (增量 min 维护优化) ===
// void update_local_min(int worker_id, int a, size_t old_pos, size_t new_pos, Slot &s) {
//   // 增量维护 worker_min(避免每次都扫描所有 assets)
//   if (old_pos == worker_local_min[worker_id]) {
//     // 被修改的是当前 min,重新扫描
//     size_t new_min = SIZE_MAX;
//     for (int a : assigned_assets_[worker_id]) {  // 扫描实际写过的 assets
//       new_min = std::min(new_min, s.ts_write_pos[a].load(memory_order_relaxed));
//     }
//     worker_local_min[worker_id] = new_min;
//   } else {
//     // 被修改的不是 min,简单更新
//     worker_local_min[worker_id] = std::min(worker_local_min[worker_id], new_pos);
//   }
// }
//
// === TS Worker: 写入特征 (增量维护 worker_min) ===
// void ts_write(date, t, a, worker_id, data) {
//   int slot_idx = ts_get_slot(date, worker_id);
//   Slot &s = pool[slot_idx];
//
//   // 1. 动态记录 asset (首次写入时自动插入,解耦业务分配逻辑)
//   assigned_assets_[worker_id].insert(a);  // O(1) amortized, 重复插入无副作用
//
//   // 2. 写入数据到 slot (普通写,非 atomic)
//   //    顺序: record asset → write_data → store write_pos → update local_min → publish worker_min
//   write_data(s, t, a, data);
//
//   // 3. 获取旧位置 (用于增量维护 min)
//   size_t old_pos = s.ts_write_pos[a].load(memory_order_relaxed);
//
//   // 4. 更新 asset 写入位置 (memory_order_release)
//   //    release: 确保数据写入对后续 acquire 可见
//   //    注意: 每个 TS worker 负责不同的 asset,无竞争,简单 store 即可
//   size_t new_pos = t + 1;
//   s.ts_write_pos[a].store(new_pos, memory_order_release);
//
//   // 5. 增量维护 worker_min (thread-local 缓存)
//   //    只有 old_pos == worker_min 时才需要重新扫描
//   update_local_min(worker_id, a, old_pos, new_pos, s);
//
//   // 6. 发布 worker_min_pos
//     size_t local_min = worker_local_min[worker_id];  // 返回缓存的 worker_min
//     s.ts_worker_min_pos[worker_id].store(local_min, memory_order_release);
//   
//   // 7. CS 通过扫描 min(ts_worker_min_pos[all workers]) 判断 time_idx 就绪
//   //    扫描复杂度: O(W) vs O(A),W=10, A=1000,快 100 倍
// }
//
// === TS Worker: 完成标记 ===
// void ts_done(date, worker_id) {
//   int slot_idx = ts_cache[worker_id].idx;
//   Slot &s = pool[slot_idx];
//
//   // 1. 批量更新所有实际写过的 asset 位置为最终值 (使用 relaxed 提高性能)
//   //    动态记录机制:只更新实际写过的 assets,无需预知分配
//   for (int a : assigned_assets_[worker_id]) {
//     s.ts_write_pos[a].store(T0, memory_order_relaxed);
//   }
//
//   // 2. 插入 fence 确保上述写入对后续 acquire 可见
//   //    关键: release fence 建立 happens-before 关系
//   //    CS 的 acquire load 能看到 fence 之前的所有 relaxed store
//   std::atomic_thread_fence(memory_order_release);
//
//   // 3. 发布最终 worker_min_pos = T0 (确保 CS 快速扫描时不会阻塞)
//   s.ts_worker_min_pos[worker_id].store(T0, memory_order_release);
//
//   // 4. 最后标记该 worker 完成 (memory_order_release)
//   //    CS 通过 acquire 读取此 flag,建立同步点
//   s.ts_done_flag[worker_id].store(true, memory_order_release);
//
//   // 5. 清除 cache (避免 stale access,防止后续误用)
//   ts_cache[worker_id] = {"",-1,0};
// }
//
// === CS Worker: 等待 time_idx 就绪 (two-level position sweep) ===
// void cs_wait(date, t) {
//   // 1. 获取 slot (cache hit 或从 ready_queue 获取)
//   Slot &s = cs_get_slot(date);  // 细节见下方 cs_get_slot
//
//   // 2. Fast path: cs_read_pos 缓存已验证的读位置
//   //    如果 t < cs_read_pos,说明该 time_idx 已验证就绪,直接返回
//   if (t < s.cs_read_pos.load(memory_order_relaxed)) [[likely]] {
//     return;  // Cache hit,无需扫描 (单 CS worker 用 relaxed 即可)
//   }
//
//   // 3. Slow path: 扫描所有 worker 的 ts_worker_min_pos,计算 min_pos
//   //    (O(W) 扫描,W=10, 极快)
//   size_t backoff_us = 1;  // 初始退避时间 1us
//   while (true) {
//     size_t min_pos = SIZE_MAX;
//     for (size_t w = 0; w < num_ts_workers; ++w) {
//       min_pos = std::min(min_pos,
//                          s.ts_worker_min_pos[w].load(memory_order_acquire));
//     }
//
//     // 4. 如果 min_pos > t,说明所有 asset 都写到了 t 之后
//     //    更新 cs_read_pos = min_pos (一次 sweep 可推进几百个 time_idx)
//     if (min_pos > t) {
//       s.cs_read_pos.store(min_pos, memory_order_relaxed);  // 批量验证 [cs_read_pos, min_pos) 就绪
//       return;  // 该 time_idx 就绪
//     }
//
//     // 5. 如果所有 TS worker 都完成了,min_pos 不会再增长
//     //    直接返回(处理部分 asset 无数据的情况)
//     bool all_done = true;
//     for (size_t w = 0; w < num_ts_workers; ++w) {
//       if (!s.ts_done_flag[w].load(memory_order_acquire)) {
//         all_done = false;
//         break;
//       }
//     }
//     if (all_done) {
//       s.cs_read_pos.store(T0, memory_order_relaxed);  // 强制所有 time_idx 就绪
//       return;
//     }
//
//     // 6. 等待后重试 (exponential backoff 避免 busy-loop)
//     //    backoff 策略: 1us → 2us → 4us → ... → 100us (上限)
//     //    理由: TS 发布频率约每 10 次写入,间隔 < 1us,初始 1us 足够
//     std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
//     backoff_us = std::min(backoff_us * 2, 100UL);  // 指数增长,最大 100us
//   }
// }
//
// === CS Worker: 获取 slot (cache + event queue) ===
// Slot& cs_get_slot(const string& date) {
//   // Cache hit
//   if (cs_cache.date == date) {
//     int slot_idx = cs_cache.idx;
//     uint32_t epoch = cs_cache.epoch;
//     if (pool[slot_idx].epoch.load(memory_order_acquire) == epoch) {
//       return pool[slot_idx];  // Cache valid
//     }
//   }
//
//   // Cache miss: 从 ready_queue 获取 (阻塞等待 TS 分配完成)
//   pair<string, int> ev;
//   while (true) {
//     if (ready_queue.pop_blocking(ev)) {
//       if (ev.first == date) {
//         int slot_idx = ev.second;
//         uint32_t epoch = pool[slot_idx].epoch.load(memory_order_acquire);
//         cs_cache = {date, slot_idx, epoch};
//         return pool[slot_idx];
//       } else {
//         // 不是当前要处理的 date,push 回队列 (或缓存多个 date)
//         ready_queue.push(ev);
//       }
//     }
//   }
// }
//
// === CS Worker: 完成标记 ===
// void cs_done(date) {
//   int slot_idx = cs_cache.idx;
//   Slot &s = pool[slot_idx];
//
//   // 1. 等待所有 TS worker 完成 (确保不会有新数据写入)
//   //    使用 exponential backoff 避免 busy-loop
//   size_t backoff_us = 1;
//   while (true) {
//     bool all_done = true;
//     for (size_t w = 0; w < num_ts_workers; ++w) {
//       if (!s.ts_done_flag[w].load(memory_order_acquire)) {
//         all_done = false;
//         break;
//       }
//     }
//     if (all_done) break;
//     
//     std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
//     backoff_us = std::min(backoff_us * 2, 100UL);  // 指数增长,最大 100us
//   }
//
//   // 2. CAS 转换状态 BUSY → DONE (原子状态转换)
//   TensorState expected = BUSY;
//   if (!s.state.compare_exchange_strong(expected, DONE,
//                                        memory_order_acq_rel,
//                                        memory_order_relaxed)) {
//     // 状态异常,记录错误 (理论上不应该发生)
//     // expected 现在包含实际状态,可用于调试
//   }
//
//   // 3. 通知 IO worker flush (事件驱动)
//   flush_queue.push(slot_idx);
//
//   // 4. 清除 cache (防止后续误用)
//   cs_cache = {"", -1, 0};
// }
//
// === CS Worker: 辅助函数 ===
// float* cs_read(Slot &s, size_t lvl, size_t t, size_t f) {
//   // 返回指向 (lvl, t, f) 的所有 A 个 assets 的指针
//   // 调用前确保 cs_wait(date, t) 已验证 time_idx t 就绪
//   return &s.data[lvl][(t * F + f) * A];
// }
//
// void cs_write(Slot &s, size_t lvl, size_t t, size_t f, const _Float16* src, size_t count) {
//   // 写入 (lvl, t, f) 的前 count 个 assets
//   memcpy(&s.data[lvl][(t * F + f) * A], src, count * sizeof(_Float16));
// }
//
// === IO Worker: 主循环 (事件驱动) ===
// void io_flush() {
//   while (running) {
//     // 1. 从 flush_queue 阻塞获取下一个要 flush 的 slot
//     //    flush_queue 由 CS worker 在 cs_done 时 push
//     int slot_idx;
//     if (!flush_queue.pop_blocking(slot_idx)) continue;  // 阻塞等待
//
//     Slot &s = pool[slot_idx];
//
//     // 2. CAS 转换状态 DONE → FLUSH (原子抢占,防止重复 flush)
//     TensorState expected = DONE;
//     if (!s.state.compare_exchange_strong(expected, FLUSH,
//                                          memory_order_acq_rel,
//                                          memory_order_relaxed)) {
//       continue;  // 状态不对 (slot 已被回收或其他异常), skip
//     }
//
//     // 3. 异步写盘 (不持有锁,独占 slot)
//     //    内部可以使用 io_uring/AIO 实现异步 I/O
//     //    这里简化为同步接口 (实际实现可以异步)
//     char date_copy[16];  // 拷贝 date (避免写盘期间 slot 被修改)
//     std::strncpy(date_copy, s.date, sizeof(date_copy));
//     date_copy[sizeof(date_copy) - 1] = '\0';
//     write_disk(date_copy, s);   // 异步写盘 (可阻塞直到完成)
//
//     // 4. 回收 slot
//     {
//       lock_guard lk(map_mtx);
//       date_map.erase(date_copy);  // 使用拷贝的 date 字符串
//     }
//
//     // 5. 使 cache 失效并回收 slot
//     s.epoch.fetch_add(1, memory_order_acq_rel);  // 使所有 cache 失效
//     s.state.store(FREE, memory_order_release);   // 发布 FREE 状态
//     push_free(slot_idx);   // CAS 无锁压栈
//     pool_sem.release();    // 唤醒阻塞的 TS worker
//   }
// }
//
// === 无锁 Freelist 操作 (Tagged pointer 防 ABA) ===
// int pop_free() {
//   while (true) {
//     uint64_t head_tagged = free_head.load(memory_order_acquire);
//     int head_idx = static_cast<int>(head_tagged & 0xFFFFFFFF);
//     uint32_t head_tag = static_cast<uint32_t>(head_tagged >> 32);
//     
//     if (head_idx < 0) return -1;  // Empty (防御性检查)
//     
//     int next_idx = pool[head_idx].next_free.load(memory_order_relaxed);
//     uint64_t next_tagged = (static_cast<uint64_t>(head_tag + 1) << 32) | static_cast<uint32_t>(next_idx);
//     
//     // CAS: success 用 acq_rel (获取旧 head,发布新 head,tag+1 防 ABA)
//     if (free_head.compare_exchange_weak(head_tagged, next_tagged,
//                                         memory_order_acq_rel,
//                                         memory_order_acquire)) {
//       return head_idx;  // 成功弹出
//     }
//     // CAS 失败(被其他线程修改或 ABA),重试
//   }
// }
//
// void push_free(int slot_idx) {
//   while (true) {
//     uint64_t old_head_tagged = free_head.load(memory_order_acquire);
//     int old_head_idx = static_cast<int>(old_head_tagged & 0xFFFFFFFF);
//     uint32_t old_tag = static_cast<uint32_t>(old_head_tagged >> 32);
//     
//     pool[slot_idx].next_free.store(old_head_idx, memory_order_relaxed);
//     uint64_t new_head_tagged = (static_cast<uint64_t>(old_tag + 1) << 32) | static_cast<uint32_t>(slot_idx);
//     
//     // CAS: success 用 acq_rel (获取旧 head,发布新 head,tag+1 防 ABA)
//     if (free_head.compare_exchange_weak(old_head_tagged, new_head_tagged,
//                                         memory_order_acq_rel,
//                                         memory_order_acquire)) {
//       return;  // 成功压入
//     }
//     // CAS 失败(被其他线程修改或 ABA),重试
//   }
// }
//
// ============================================================================
// MEMORY ORDER SUMMARY (关键操作的 memory_order 选择)
// ============================================================================
//
// 操作                             Memory Order         理由
// ---------------------------------------------------------------------------------
// epoch.fetch_add                  acq_rel             同步 slot 失效,双向可见性
// state.store(BUSY)                release             发布 slot 初始化完成
// state.load                       acquire             获取 slot 状态并同步数据
// state.compare_exchange_strong    acq_rel, relaxed    状态转换需要双向同步
// ts_write_pos[a].store(0)         relaxed             初始化无需同步(后续 release 建立)
// ts_write_pos[a].store(t+1)       release             发布数据写入完成(写入 → store pos)
// ts_write_pos[a].load             acquire             读取位置并同步数据(仅调试用)
// ts_worker_min_pos[w].store(0)    relaxed             初始化无需同步
// ts_worker_min_pos[w].store(min)  release             发布 worker 最小位置
// ts_worker_min_pos[w].load        acquire             CS 读取 worker min 并同步
// ts_done_flag[w].store(false)     relaxed             初始化无需同步
// ts_done_flag[w].store(true)      release             发布 worker 完成状态
// ts_done_flag[w].load             acquire             读取完成状态并同步
// cs_read_pos.store                relaxed             单 CS worker 独占写,无竞争
// cs_read_pos.load                 relaxed             单 CS worker 独占读,无竞争
// free_head CAS (tagged)           acq_rel, acquire    Freelist pop/push 双向同步 + ABA 防护
//                                                      格式: (tag << 32) | idx, tag++ 防 ABA
//
// ============================================================================
//
// 【时间映射】L0 ↔ L1/L2
//   - L0 包含 _link_to_L1, _link_to_L2 两个 META feature
//   - 存储为 _Float16,值为 L1/L2 时间索引
//   - 支持非均匀映射 (多个 L0 tick 指向同一 L1 minute)
//
// 【导出格式】输出目录: output/features/YYYY/MM/DD/
//   - features_L0.bin: [T0, F0, A] (包含 link features)
//   - features_L1.bin: [T1, F1, A]
//   - features_L2.bin: [T2, F2, A]
//
// 【Public API 速查】
//
// TS Worker:
//   ts_write(date, t, a, worker_id, data)              写入特征
//   ts_update(date, worker_id, asset_id, l0_t)         更新时间进度
//   ts_done(date, worker_id)                           完成标记
//
// CS Worker:
//   cs_wait(date, t)                                   等待 time_idx 就绪
//   cs_read(slot, lvl, t, f) → _Float16*               读取所有 assets
//   cs_write(slot, lvl, t, f, src, count)              写入所有 assets
//   cs_done(date)                                      完成标记
//
// IO Worker:
//   io_flush()                                         主循环
//
// Freelist:
//   pop_free() → slot_idx                              弹出空闲 slot
//   push_free(slot_idx)                                压入空闲 slot
//
// 元数据查询:
//   query_F(lvl), query_A(), query_T(lvl), query_num_dates()
//
// ============================================================================

// ============================================================================
// FEATURE METADATA ENCODING SYSTEM
// ============================================================================

// Data type classification
enum class FeatureDataType : uint8_t {
  TS = 0,  // Time-series (时序)
  CS = 1,  // Cross-sectional (截面)
  LB = 2,  // Label (标签)
  SH = 3,  // Shared (TS/CS共享中间值)
  META = 4 // Metadata (backend系统元数据)
};

// Primary category
enum class FeatureCategoryL1 : uint8_t {
  PRICE         = 0,  // 价格
  VOLUME        = 1,  // 量能
  VOLATILITY    = 2,  // 波动率
  MOMENTUM      = 3,  // 动量
  LIQUIDITY     = 4,  // 流动性
  IMBALANCE     = 5,  // 失衡
  MICROSTRUCTURE = 6, // 微结构
  LABEL         = 7,  // 标签/目标
  META          = 8   // 元数据/共享变量
};

// Secondary category
enum class FeatureCategoryL2 : uint8_t {
  RAW        = 0,  // 原始
  NORMALIZED = 1,  // 标准化
  OSCILLATOR = 2,  // 震荡器
  DEVIATION  = 3,  // 偏离
  RATIO      = 4,  // 比率
  RANK       = 5,  // 排名
  FUTURE_RET = 6,  // 未来收益
  SCORE      = 7,  // 评分
  UNIVERSE   = 8,  // 全域统计
  BENCHMARK  = 9   // 基准/市场
};

// Normalization method
enum class NormMethod : uint8_t {
  NONE      = 0,  // 无
  ZSCORE    = 1,  // z-score标准化
  RANK_NORM = 2,  // rank + inverse normal
  CLIP      = 3,  // clip到[-3,3]
  TANH      = 4,  // tanh激活
  WINSOR    = 5,  // winsorize
  LOG_NORM  = 6,  // log后标准化
  PCT_RANK  = 7   // percentile rank
};

// ============================================================================
// LEVEL 0: Tick-level Features (瞬时微结构信号, 短窗口: 5-200 ticks)
// ============================================================================
// Format: X(code, name_cn, name_en, data_type, cat_l1, cat_l2, norm_method, formula, description)

#define LEVEL_0_FIELDS(X) \
  X(tick_ret_z,           "微小对数收益",       "Tick Return Z-score",        TS,   MOMENTUM,       NORMALIZED, ZSCORE,    "(r-μ_W)/σ_W, r=log(mid_t/mid_{t-1}), W=50",                 "滚动窗口标准化的tick级对数收益,中性动量/瞬时冲击") \
  X(tobi_osc,             "订单失衡震荡",       "TOBI Oscillator",            TS,   IMBALANCE,      OSCILLATOR, CLIP,      "clip((tobi-mean_W)/MAD_W, -3, 3), W=50",                    "top-of-book买卖压力震荡器,对称性好") \
  X(micro_gap_norm,       "微观价差标准化",     "Micro Gap Normalized",       TS,   MICROSTRUCTURE, NORMALIZED, TANH,      "tanh((micro_price-mid)/σ_W), W=50",                         "micro_price与mid_price的标准化偏离,有界对称") \
  X(spread_momentum,      "价差动量",           "Spread Momentum",            TS,   LIQUIDITY,      DEVIATION,  ZSCORE,    "Δs = s - EMA_α(s), α~20ticks",                              "spread的短期变动,表示流动性瞬变") \
  X(signed_volume_imb,    "签名成交量失衡",     "Signed Volume Imbalance",    TS,   VOLUME,         OSCILLATOR, NONE,      "Σ(sign_ixsize_i)/Σ|size_i|, N ticks",                      "近N ticks签名成交量不对称,直接为[-1,1]") \
  X(cs_spread_rank,       "价差截面排名",       "CS Spread Rank",             CS,   LIQUIDITY,      RANK,       RANK_NORM, "Φ^{-1}(percentile(spread))",                                "spread在universe中的截面rank→inverse normal") \
  X(cs_tobi_rank,         "失衡截面排名",       "CS TOBI Rank",               CS,   IMBALANCE,      RANK,       RANK_NORM, "Φ^{-1}(percentile(tobi))",                                  "tobi在universe中的截面rank→inverse normal") \
  X(cs_liquidity_ratio,   "流动性比率截面",     "CS Liquidity Ratio",         CS,   LIQUIDITY,      RATIO,      ZSCORE,    "(top_size/median_H)/z-score",                               "当前top-of-book size相对历史中位数的截面z-score") \
  X(next_tick_ret,        "下tick收益",         "Next Tick Return",           LB,   LABEL,          FUTURE_RET, NONE,      "log(mid_{t+1}/mid_t)",                                      "下一个tick的对数收益,作为预测目标") \
  X(next_5tick_ret,       "未来5tick收益",      "Next 5-Tick Return",         LB,   LABEL,          FUTURE_RET, NONE,      "log(mid_{t+5}/mid_t)",                                      "未来5个tick的累计对数收益,中期预测目标") \
  X(asset_valid,          "资产有效标志",       "Asset Valid Flag",           SH,   META,           RAW,        NONE,      "1.0=valid, 0.0=invalid(inactive/suspended)",                "TS/CS共享:标记该asset数据是否有效(停牌/无数据则为0),业务逻辑使用") \
  X(universe_size,        "全域规模",           "Universe Size",              SH,   META,           UNIVERSE,   NONE,      "count(valid_instruments)",                                  "TS/CS共享:当前时刻universe中有效合约数量") \
  X(market_mid_price,     "市场基准价格",       "Market Mid Price",           SH,   META,           BENCHMARK,  NONE,      "benchmark_instrument_mid_price",                            "TS/CS共享:市场基准合约的mid价格") \
  X(_link_to_L1,          "L1时间索引",         "Link to L1 Time Index",      META, META,           RAW,        NONE,      "static_cast<_Float16>(size_t_L1_index)",                    "Backend元数据:L0时刻对应的L1时间索引,存储为_Float16,导出时转为size_t") \
  X(_link_to_L2,          "L2时间索引",         "Link to L2 Time Index",      META, META,           RAW,        NONE,      "static_cast<_Float16>(size_t_L2_index)",                    "Backend元数据:L0时刻对应的L2时间索引,存储为_Float16,导出时转为size_t")

// ============================================================================
// LEVEL 1: Minute-level Features (聚合分钟条, 窗口: 1/5/15/60 minutes)
// ============================================================================

#define LEVEL_1_FIELDS(X) \
  X(min_ret_z,            "分钟收益",           "Minute Return Z-score",      TS, MOMENTUM,       NORMALIZED, WINSOR,    "(r-μ_60m)/σ_60m, r=log(close_t/close_{t-1})",               "一分钟对数收益标准化,rolling 60m") \
  X(rv_5m_norm,           "5分钟波动率",        "Realized Vol 5m Normalized", TS, VOLATILITY,     NORMALIZED, LOG_NORM,  "log(σ_5m) rank-normalize",                                  "5分钟实际波动率标准化,减小偏斜") \
  X(vwap_gap_pct,         "VWAP偏离",           "VWAP Gap Percent",           TS, PRICE,          DEVIATION,  ZSCORE,    "(close-vwap)/vwap rolling z-score",                         "close与vwap相对偏离,表示价格是否偏离当期交易价") \
  X(momentum_15m,         "15分钟动量",         "Momentum 15m",               TS, MOMENTUM,       OSCILLATOR, ZSCORE,    "Σr_{1m}/σ_rolling, 15m累计",                                "15分钟累计动量标准化") \
  X(range_squeeze,        "Range收窄",          "Range Squeeze",              TS, VOLATILITY,     RATIO,      CLIP,      "(high-low)/(σ_30m+ε), clip[-3,3]",                          "range/vol,衡量盘面窄幅,收窄为正") \
  X(cs_min_return_rank,   "分钟收益截面",       "CS Minute Return Rank",      CS, MOMENTUM,       RANK,       RANK_NORM, "Φ^{-1}(percentile(minute_return))",                         "分钟收益在universe中的截面rank→inverse normal") \
  X(cs_min_volume_pct,    "分钟量能百分位",     "CS Minute Volume Percentile",CS, VOLUME,         RANK,       RANK_NORM, "percentile(log(volume)) rank-normalize",                    "分钟volume在universe中的截面百分位排名") \
  X(cs_min_spread_z,      "分钟价差截面",       "CS Minute Spread Z-score",   CS, LIQUIDITY,      NORMALIZED, ZSCORE,    "z-score(spread) cross-sectional",                           "分钟spread的截面z-score,反映相对交易成本") \
  X(next_1m_ret,          "下1分钟收益",        "Next 1-Minute Return",       LB, LABEL,          FUTURE_RET, NONE,      "log(close_{t+1}/close_t)",                                  "下一分钟的对数收益,作为预测目标") \
  X(calmar_score,         "Calmar评分",         "Calmar Score",               LB, LABEL,          SCORE,      NONE,      "annual_return/max_drawdown",                                "Calmar比率,年化收益与最大回撤之比,风险调整收益指标") \
  X(universe_size,        "全域规模",           "Universe Size",              SH, META,           UNIVERSE,   NONE,      "count(valid_instruments)",                                  "TS/CS共享:当前时刻universe中有效合约数量") \
  X(market_return,        "市场收益",           "Market Return",              SH, META,           BENCHMARK,  NONE,      "log(market_close_t/market_close_{t-1})",                    "TS/CS共享:市场基准收益率")

// ============================================================================
// LEVEL 2: Hour-level Features (小时级, 窗口: 1h/3h/6h/24h)
// ============================================================================

#define LEVEL_2_FIELDS(X) \
  X(hour_ret_12h_mom,     "12小时动量",         "Hour Return 12h Momentum",   TS, MOMENTUM,       NORMALIZED, ZSCORE,    "Σr_{1h}^{12}/z-score_{48h}",                                "12小时动量标准化,捕捉中期趋势") \
  X(hour_volatility,      "24小时波动率",       "Hour Volatility 24h",        TS, VOLATILITY,     NORMALIZED, LOG_NORM,  "log(σ_24h) rank-normalize",                                 "24小时realized vol,log后rank标准化减小偏斜") \
  X(pivot_dev,            "Pivot偏差",          "Pivot Deviation",            TS, PRICE,          DEVIATION,  CLIP,      "(close-pivot)/price_range, clip",                           "收盘相对pivot point的偏差,标准化") \
  X(dominant_persist,     "主导持续性",         "Dominant Persistence",       TS, IMBALANCE,      OSCILLATOR, ZSCORE,    "EMA(dominant_side, α) normalized",                          "dominant_side的EMA标准化,表示买卖主导延续性") \
  X(hour_overnight_gap,   "隔夜跳空",           "Hour Overnight Gap",         TS, PRICE,          DEVIATION,  WINSOR,    "(open-prev_close)/σ_intraday, winsorize",                   "当小时起点与前一日收盘gap,捕捉消息型跳空") \
  X(cs_hour_return_beta,  "小时收益残差",       "CS Hour Return Beta",        CS, MOMENTUM,       RANK,       RANK_NORM, "residual(r_t ~ r_market) rank-normalize",                   "小时回报相对市场的回归残差,截面排名") \
  X(cs_hour_liq_adj_ret,  "流动性调整收益",     "CS Hour Liquidity Adj Return",CS, MOMENTUM,      RANK,       RANK_NORM, "hour_ret/sqrt(volume) rank",                                "小时收益按流动性调整后的截面排名") \
  X(cs_hour_range_rank,   "小时Range排名",      "CS Hour Range Rank",         CS, VOLATILITY,     RANK,       RANK_NORM, "Φ^{-1}(percentile(price_range))",                           "price_range在universe中的截面百分位排名") \
  X(next_1h_ret,          "下1小时收益",        "Next 1-Hour Return",         LB, LABEL,          FUTURE_RET, NONE,      "log(close_{t+1h}/close_t)",                                 "下一小时的对数收益,作为预测目标") \
  X(sharpe_score,         "Sharpe评分",         "Sharpe Score",               LB, LABEL,          SCORE,      NONE,      "(mean_return-rf)/std_return",                               "Sharpe比率,超额收益与波动率之比,风险调整收益指标") \
  X(universe_size,        "全域规模",           "Universe Size",              SH, META,           UNIVERSE,   NONE,      "count(valid_instruments)",                                  "TS/CS共享:当前时刻universe中有效合约数量") \
  X(market_volatility,    "市场波动率",         "Market Volatility",          SH, META,           BENCHMARK,  NONE,      "std(market_returns_24h)",                                   "TS/CS共享:市场24小时波动率")

// ============================================================================
// ALL LEVELS REGISTRY
// ============================================================================
// Format: X(level_name, level_index, fields_macro)

#define ALL_LEVELS(X)      \
  X(L0, 0, LEVEL_0_FIELDS) \
  X(L1, 1, LEVEL_1_FIELDS) \
  X(L2, 2, LEVEL_2_FIELDS)

// ============================================================================
// TIME GRANULARITY CONFIGURATION
// ============================================================================

constexpr size_t TRADE_HOURS_PER_DAY = 4;
constexpr size_t TRADE_SECONDS_PER_DAY = TRADE_HOURS_PER_DAY * 3600; // 14400 seconds

// Time unit types
enum class TimeUnit : uint8_t {
  MILLISECOND = 0,
  SECOND = 1,
  MINUTE = 2,
  HOUR = 3
};

// Level time configuration
struct LevelTimeConfig {
  TimeUnit unit;
  size_t interval; // Number of units per time index

  constexpr size_t max_capacity() const {
    switch (unit) {
    case TimeUnit::MILLISECOND:
      return (TRADE_SECONDS_PER_DAY * 1000) / interval + 1;
    case TimeUnit::SECOND:
      return TRADE_SECONDS_PER_DAY / interval + 1;
    case TimeUnit::MINUTE:
      return (TRADE_SECONDS_PER_DAY / 60) / interval + 1;
    case TimeUnit::HOUR:
      return (TRADE_SECONDS_PER_DAY / 3600) / interval + 1;
    }
    return TRADE_SECONDS_PER_DAY + 1;
  }
};

// Predefined level configurations
constexpr LevelTimeConfig LEVEL_CONFIGS[3] = {
    {TimeUnit::SECOND, 1}, // L0: 1s
    {TimeUnit::MINUTE, 1}, // L1: 1min
    {TimeUnit::HOUR, 1}    // L2: 1hour
};

// ============================================================================
// TRADING SESSION MAPPING - High Performance Non-linear Time Conversion
// ============================================================================
// Chinese stock market trading sessions:
//   Morning:   09:30 - 11:30 (2 hours)
//   Lunch:     11:30 - 13:00 (non-trading)
//   Afternoon: 13:00 - 15:00 (2 hours)
// Total trading time: 4 hours = 14400 seconds

// Trading session boundaries (in minutes since midnight)
constexpr uint16_t MORNING_START_MIN = 9 * 60 + 30; // 570 (09:30)
constexpr uint16_t MORNING_END_MIN = 11 * 60 + 30;  // 690 (11:30)
constexpr uint16_t AFTERNOON_START_MIN = 13 * 60;   // 780 (13:00)
constexpr uint16_t AFTERNOON_END_MIN = 15 * 60;     // 900 (15:00)

// Helper: Map clock time to trading seconds (comptime)
// Returns: -1 for pre-market, 0-7199 for morning, 7200-14399 for afternoon, 14399 for post-market (clamped)
constexpr int16_t map_clock_to_trading_seconds(uint8_t hour, uint8_t minute) {
  const uint16_t total_minutes = hour * 60 + minute;

  // Morning session: 09:30-11:30 → 0-7199 seconds
  if (total_minutes >= MORNING_START_MIN && total_minutes < MORNING_END_MIN) {
    return static_cast<int16_t>((total_minutes - MORNING_START_MIN) * 60);
  }

  // Afternoon session: 13:00-15:00 → 7200-14399 seconds
  if (total_minutes >= AFTERNOON_START_MIN && total_minutes < AFTERNOON_END_MIN) {
    return static_cast<int16_t>(7200 + (total_minutes - AFTERNOON_START_MIN) * 60);
  }

  // Lunch break: map to afternoon session start
  if (total_minutes >= MORNING_END_MIN && total_minutes < AFTERNOON_START_MIN) {
    return 7200;
  }

  // Pre-market
  if (total_minutes < MORNING_START_MIN) {
    return -1;
  }

  // Post-market: clamp to last valid index (14399, not 14400)
  return 14399;
}

// Constexpr function to generate lookup table at compile time
constexpr auto generate_trading_offset_table() {
  std::array<int16_t, 24 * 60> table{};
  for (size_t i = 0; i < 24 * 60; ++i) {
    const uint8_t hour = i / 60;
    const uint8_t minute = i % 60;
    table[i] = map_clock_to_trading_seconds(hour, minute);
  }
  return table;
}

// Compile-time generated lookup table (1440 entries x 2 bytes = 2.88 KB)
static constexpr auto TRADING_OFFSET_LUT = generate_trading_offset_table();

// ============================================================================
// TIME CONVERSION - O(1) Branchless Lookup
// ============================================================================

// Convert time to trading seconds (0-14399)
// High-performance branchless implementation using compile-time LUT
inline constexpr size_t time_to_trading_seconds(uint8_t hour, uint8_t minute, uint8_t second) {
  const size_t hm_idx = hour * 60 + minute;
  const int16_t base = TRADING_OFFSET_LUT[hm_idx];
  // Branchless clamp: negative → 0, positive → value
  const size_t clamped_base = base & ~(base >> 15);  // Sign bit mask: if negative, result is 0
  return clamped_base + second;
}

// Convert time to trading milliseconds (0-14399999)
inline constexpr size_t time_to_trading_milliseconds(uint8_t hour, uint8_t minute, uint8_t second, uint8_t millisecond) {
  return time_to_trading_seconds(hour, minute, second) * 1000 + millisecond;
}

// ============================================================================
// ENUM TO STRING MAPPINGS - For metadata query and serialization
// ============================================================================

inline constexpr const char* to_string(FeatureDataType type) {
  switch (type) {
    case FeatureDataType::TS: return "TS";
    case FeatureDataType::CS: return "CS";
    case FeatureDataType::LB: return "LB";
    case FeatureDataType::SH: return "SH";
    case FeatureDataType::META: return "META";
  }
  return "UNKNOWN";
}

inline constexpr const char* to_string(FeatureCategoryL1 cat) {
  switch (cat) {
    case FeatureCategoryL1::PRICE:          return "PRICE";
    case FeatureCategoryL1::VOLUME:         return "VOLUME";
    case FeatureCategoryL1::VOLATILITY:     return "VOLATILITY";
    case FeatureCategoryL1::MOMENTUM:       return "MOMENTUM";
    case FeatureCategoryL1::LIQUIDITY:      return "LIQUIDITY";
    case FeatureCategoryL1::IMBALANCE:      return "IMBALANCE";
    case FeatureCategoryL1::MICROSTRUCTURE: return "MICROSTRUCTURE";
    case FeatureCategoryL1::LABEL:          return "LABEL";
    case FeatureCategoryL1::META:           return "META";
  }
  return "UNKNOWN";
}

inline constexpr const char* to_string(FeatureCategoryL2 cat) {
  switch (cat) {
    case FeatureCategoryL2::RAW:        return "RAW";
    case FeatureCategoryL2::NORMALIZED: return "NORMALIZED";
    case FeatureCategoryL2::OSCILLATOR: return "OSCILLATOR";
    case FeatureCategoryL2::DEVIATION:  return "DEVIATION";
    case FeatureCategoryL2::RATIO:      return "RATIO";
    case FeatureCategoryL2::RANK:       return "RANK";
    case FeatureCategoryL2::FUTURE_RET: return "FUTURE_RET";
    case FeatureCategoryL2::SCORE:      return "SCORE";
    case FeatureCategoryL2::UNIVERSE:   return "UNIVERSE";
    case FeatureCategoryL2::BENCHMARK:  return "BENCHMARK";
  }
  return "UNKNOWN";
}

inline constexpr const char* to_string(NormMethod method) {
  switch (method) {
    case NormMethod::NONE:      return "NONE";
    case NormMethod::ZSCORE:    return "ZSCORE";
    case NormMethod::RANK_NORM: return "RANK_NORM";
    case NormMethod::CLIP:      return "CLIP";
    case NormMethod::TANH:      return "TANH";
    case NormMethod::WINSOR:    return "WINSOR";
    case NormMethod::LOG_NORM:  return "LOG_NORM";
    case NormMethod::PCT_RANK:  return "PCT_RANK";
  }
  return "UNKNOWN";
}
