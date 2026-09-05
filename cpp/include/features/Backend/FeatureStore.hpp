#pragma once

#include "FeatureStoreConfig.hpp"
#include "ZstdHelper.hpp"
#include "misc/logging.hpp"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#ifdef __APPLE__
#include <sys/sysctl.h>
#else
#include <sys/mman.h> // madvise(MADV_HUGEPAGE): 张量池上透明大页
#include <unistd.h>
#endif

// 系统总物理内存 (字节)
inline size_t get_system_memory_bytes() {
#ifdef __APPLE__
  int mib[2] = {CTL_HW, HW_MEMSIZE};
  int64_t size;
  size_t len = sizeof(size);
  sysctl(mib, 2, &size, &len, nullptr, 0);
  return static_cast<size_t>(size);
#else
  return static_cast<size_t>(sysconf(_SC_PHYS_PAGES)) * static_cast<size_t>(sysconf(_SC_PAGE_SIZE));
#endif
}

// 当前实际可分配内存 (字节). 总物理内存会高估: HugePages 预留 / 其他进程 /
// page cache 之外的占用对普通 new[] 都不可用. Linux 用 MemAvailable
// (内核对"可分配而不触发 OOM/换页"的估计), 其他平台退化为总内存.
inline size_t get_available_memory_bytes() {
#ifndef __APPLE__
  std::ifstream f("/proc/meminfo");
  std::string key;
  size_t kb;
  std::string unit;
  while (f >> key >> kb >> unit) {
    if (key == "MemAvailable:")
      return kb * 1024;
  }
#endif
  return get_system_memory_bytes();
}

// 池 slot 数 = 在飞日期上限: TS 当前日 (可超前 1-2 日) + CS 尾随日 + IO flush 中的日.
inline constexpr size_t kDefaultPoolSlots = 4;

// ============================================================================
// FEATURE STORE - 按日门控的张量池
//
// 并发模型 (回测): TS worker 是资产串行的 (一个资产写完一整天才换下一个),
// "秒级流式伴随"在这种调度下没有可用的进度语义 —— 且 label / L1 分钟行都是
// 向过去回填, 行级进度天然罩不住. 所以门控就是按日, 计数粒度是 asset-day:
//
//   TS:  ts_open(date) → 句柄写 (纯指针算术, 无锁无验证) → 每资产 ts_close
//   CS:  cs_open(date) 阻塞至本日 assets_done == num_assets → 整日扫 → cs_close
//   IO:  io_try_flush: 摘 DONE → 落盘 → reset → FREE
//
// 按资产计数 (而非按 worker close 计数) 使资产处置权可在 worker 间转移: 领跑
// worker 可从落后 worker 接手资产并回填旧日期 (旧日 slot 未计满必为 BUSY,
// ts_open 随时可再取句柄), 见 sequential_worker 的负载再平衡. 资产内日序由
// 调度侧的 claim/done 原子保证, 因此 "d+1 日全资产计满 ⇒ d 日已计满" ——
// 日完成天然单调, ts_days_done_ 只增.
//
// slot 生命周期: FREE → BUSY (首个 ts_open) → DONE (cs_close) → FLUSH → FREE.
// 句柄在有效期内被状态机钉住 (TS 未计满不给 CS, CS 未 done 不给 IO), 不可能
// 被回收 —— 不需要 epoch / 每次访问的缓存验证. 数值一致性与调度形态无关, 由
// 输入契约锚定 (见 CoreSequential.hpp / CoreCrosssection.hpp), 按日门控只是
// 回测里的因果保证.
// ============================================================================
class GlobalFeatureStore {
public:
  enum class TensorState : uint8_t {
    FREE = 0,  // 可分配
    BUSY = 1,  // TS 写; TS 全部 close 后 CS 读写
    DONE = 2,  // CS 完成, 待落盘
    FLUSH = 3, // IO 落盘中
  };

  // Per-date slot
  struct Slot {
    std::atomic<TensorState> state{TensorState::FREE};
    std::atomic<uint32_t> assets_done{0};             // == num_assets ⇒ 本日 TS 全部写完 (CS 放行)
    char date[16] = {0};                              // "YYYYMMDD"
    feature_storage_t *data[LEVEL_COUNT] = {nullptr}; // 每层 [T][F][A]

    void allocate(size_t num_assets) {
      // 张量按 2MB 对齐并 madvise(MADV_HUGEPAGE): 透明大页是普通匿名内存,
      // 进程退出/崩溃由内核自动回收, 不需要 nr_hugepages 静态预留 (那种预留
      // 会从 MemAvailable 里整块扣掉, 谁都用不了). 需要系统
      // /sys/kernel/mm/transparent_hugepage/enabled = madvise 或 always.
      // 收益: TS 写行是列间 stride (F 列 × A×2B ≈ 11.8KB), 4K 页下每 tick
      // 踩 F 个不同页, dTLB 压力大; 2M 页覆盖整列邻域, miss 显著减少.
      constexpr size_t kHugePage = 2ull * 1024 * 1024;
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        // aligned_alloc 要求 size 是 alignment 的整数倍; 每层最多多占 <2MB
        const size_t aligned_bytes = ((level_bytes(lvl, num_assets) + kHugePage - 1) / kHugePage) * kHugePage;
        data[lvl] = static_cast<feature_storage_t *>(aligned_alloc(kHugePage, aligned_bytes));
#if defined(__linux__)
        madvise(data[lvl], aligned_bytes, MADV_HUGEPAGE);
#endif
        assert(data[lvl]);
      }
    }

    void reset(size_t num_assets) {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
        std::memset(data[lvl], 0, level_bytes(lvl, num_assets));
      std::memset(date, 0, sizeof(date));
      assets_done.store(0, std::memory_order_relaxed);
    }

    ~Slot() {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
        free(data[lvl]);
    }
  };

  // 日句柄 (TS/CS 同一形态): open 时定址一次, 之后所有读写是纯指针算术 ——
  // 热路径不再有字符串比较 / 原子验证 / slot 查找 (原先每 tick 4-6 次).
  // 空句柄 = 没拿到 (池满 / 本日未就绪), try_open 以此表示失败.
  struct Day {
    Slot *slot = nullptr;
    size_t A = 0;
    explicit operator bool() const { return slot != nullptr; }
  };

private:
  // 某层一天张量字节数 [T][F][A]
  static constexpr size_t level_bytes(size_t lvl, size_t num_assets) {
    return LEVELS[lvl].rows * LEVELS[lvl].width * num_assets * sizeof(feature_storage_t);
  }

  // Config
  const std::uint64_t axis_hash_; // A 轴前缀指纹, 落进每个特征文件头
  const size_t num_assets_;
  const size_t num_ts_workers_;
  size_t pool_size_;
  std::string output_dir_;

  // Pool: 固定 slot 数组. 不需要 date→slot map / freelist / epoch ——
  // slot 数由计算页配置, 线性扫 state 就是查找和空闲表.
  Slot *pool_ = nullptr;
  std::mutex pool_mutex_; // 只串行化 ts_open 的查找/分配; 其余状态转移无锁

  // 计满 num_assets 的日数 (单调, 见 ts_close), 预取门控用
  std::atomic<size_t> ts_days_done_{0};
  std::atomic<size_t> cs_days_done_{0};

  // Per-worker 已完成日数 (worker 自报, release; 领养方 acquire 读 —— 与被
  // 领养资产的 core 构造/状态写入构成 happens-before). 负载再平衡的观测面.
  std::unique_ptr<std::atomic<uint32_t>[]> ts_frontier_;

  // IO worker 专用复用缓冲 (单线程): 列抽取 / 头 + 压缩输出, 稳态零分配
  std::vector<feature_storage_t> io_column_;
  std::vector<uint8_t> io_buf_;

public:
  // axis_hash: AssetAxis::hash_at(num_assets), 写进每个特征文件头锁定列序
  GlobalFeatureStore(size_t num_assets, size_t num_ts_workers,
                     std::uint64_t axis_hash,
                     const std::string &output_dir = "",
                     size_t pool_slots = kDefaultPoolSlots)
      : axis_hash_(axis_hash),
        num_assets_(num_assets), num_ts_workers_(num_ts_workers),
        pool_size_(pool_slots) {
    assert(pool_size_ >= 2 && "Pool size too small, need at least 2 slots");

    ts_frontier_ = std::make_unique<std::atomic<uint32_t>[]>(num_ts_workers_);
    for (size_t w = 0; w < num_ts_workers_; ++w)
      ts_frontier_[w].store(0, std::memory_order_relaxed);

    if (!output_dir.empty()) {
      output_dir_ = output_dir;
      // 只建目录, 不清库: 清库/重算是调用方的显式决定 (回测全量重算见
      // ComputeService), 构造副作用会让实盘复用 store 时误删历史特征.
      std::filesystem::create_directories(output_dir_);
    }

    // Calculate sizes
    size_t bytes_per_day = 0;
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
      bytes_per_day += level_bytes(lvl, num_assets);

    // 限制 pool_size 不超过 40% 当前可用内存 (总内存会高估: HugePages 预留 /
    // 其他进程占用对普通分配不可用, 按总内存分池会被 OOM killer SIGKILL).
    // 留 60% 余量给: 每资产常驻的 LOB 容量 (见 L2::LOB_ORDER_CAPACITY,
    // 随真实逐笔数据写入逐步坐实成 RSS, 整个进程生命周期不释放) + 其他进程.
    constexpr double kPoolMemFraction = 0.4;
    const size_t avail_mem = get_available_memory_bytes();
    const size_t max_pool_bytes = static_cast<size_t>(avail_mem * kPoolMemFraction);
    const size_t max_pool_slots = max_pool_bytes / bytes_per_day;
    if (pool_size_ > max_pool_slots) {
      printf("\n[FeatureStore] Pool size limited: %zu -> %zu (%.0f%% of %.1f GB available, %.1f GB DDR)\n",
             pool_size_, max_pool_slots, kPoolMemFraction * 100.0,
             avail_mem / (1024.0 * 1024.0 * 1024.0),
             get_system_memory_bytes() / (1024.0 * 1024.0 * 1024.0));
      pool_size_ = max_pool_slots;
    }
    assert(pool_size_ >= 2 && "Pool size too small, need at least 2 slots");

    std::cout << "\n=== Feature Store Tensor Pool ===\n";

    // Detailed dimension breakdown
    std::cout << "Dimension Details:\n";
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      const auto &L = LEVELS[lvl];
      printf("  %-6s [T=%6zu][F=%4zu][A=%4zu] = %.2f MB\n", L.level_name, L.rows, L.width, num_assets, level_bytes(lvl, num_assets) / (1024.0 * 1024.0));
    }

    printf("\nPer-day tensor: %.2f MB | Pool size: %zu slots | Total: %.2f GB\n",
           bytes_per_day / (1024.0 * 1024.0),
           pool_size_,
           (bytes_per_day * pool_size_) / (1024.0 * 1024.0 * 1024.0));

    // Allocate pool (Slot array)
    pool_ = new Slot[pool_size_];

    std::cout << "Allocating physical pages...\n";
    auto start = std::chrono::steady_clock::now();

    // Parallel allocation using multiple threads
    const size_t num_threads = std::min<size_t>(pool_size_, std::thread::hardware_concurrency());
    std::vector<std::thread> threads;
    std::atomic<size_t> progress_counter{0};

    auto allocate_slot = [&](size_t i) {
      pool_[i].allocate(num_assets_);
      pool_[i].reset(num_assets_); // memset 清零 = 坐实物理页 + FREE 态不变量: 拿来即写

      const size_t completed = ++progress_counter;
      std::cout << "  " << completed << "/" << pool_size_ << " slots\r" << std::flush;
    };

    for (size_t tid = 0; tid < num_threads; ++tid) {
      threads.emplace_back([&, tid]() {
        for (size_t i = tid; i < pool_size_; i += num_threads) {
          allocate_slot(i);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    std::cout << "\nPhysical allocation complete in " << elapsed << " ms\n";
    std::cout << "=================================\n\n";
  }

  ~GlobalFeatureStore() {
    if (pool_) {
      delete[] pool_; // Slot destructors called automatically
    }
  }

  // ===== TS WORKER API: open 按 (date), close 按 (asset, date) =====

  // 非阻塞: 找到 / 分配本日 slot. 首个到达的 worker 分配 (FREE→BUSY), 其余
  // 直接命中. 回填旧日期也走这里: 未计满的日子必为 BUSY, 直接命中, 不会重
  // 分配. slot 在 flush 时已 reset (FREE 态不变量: 张量清零), 拿来即写.
  // 池满返回空句柄 —— 调用方拿等待时间去干别的 (领跑者在池边领养回填).
  Day ts_try_open(const std::string &date, int worker_id) {
    assert(worker_id >= 0 && worker_id < static_cast<int>(num_ts_workers_));
    std::unique_lock<std::mutex> lock(pool_mutex_);
    // 本日已被其他 worker 打开 (领养回填的旧日期也从这里命中). 只匹配
    // BUSY: 未计满的日子必为 BUSY, 已过 CS/IO 阶段的日子不可能再被打开;
    // FLUSH 中的 slot date 正被 reset, 不可读.
    for (size_t i = 0; i < pool_size_; ++i) {
      Slot &s = pool_[i];
      if (s.state.load(std::memory_order_acquire) == TensorState::BUSY && date == s.date)
        return {&s, num_assets_};
    }
    for (size_t i = 0; i < pool_size_; ++i) {
      Slot &s = pool_[i];
      if (s.state.load(std::memory_order_acquire) == TensorState::FREE) {
        snprintf(s.date, sizeof(s.date), "%s", date.c_str());
        s.state.store(TensorState::BUSY, std::memory_order_release);
        Logger::log("worker_" + std::to_string(worker_id), "ts_open: " + date + " → pool[" + std::to_string(i) + "]");
        return {&s, num_assets_};
      }
    }
    return {};
  }

  // 阻塞版: 池满则等 IO 释放 —— TS 超前于 CS/IO 的唯一背压点.
  // (主循环不用它 —— 等待时间要拿去领养, 见 sequential_worker; 回填与
  // 不需要边等边干活的调用方用这里.)
  Day ts_open(const std::string &date, int worker_id, const std::atomic<bool> &cancel_requested) {
    for (int waited = 0;; ++waited) {
      if (cancel_requested.load(std::memory_order_relaxed))
        return {};
      Day day = ts_try_open(date, worker_id);
      if (day)
        return day;
      if (waited == 0)
        Logger::log("worker_" + std::to_string(worker_id), "Pool exhausted, waiting...");
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  // 一个 (asset, date) 写完. 计数攒齐 num_assets 后 cs_open 放行;
  // release 与 cs_open 的 acquire 配对, 保证张量写入对 CS 可见.
  // 缺 binary 的 asset-day 也要计 (张量保持默认值), 由当时的处置权持有者计.
  void ts_close(const Day &day) {
    assert(day.slot);
    const uint32_t prev = day.slot->assets_done.fetch_add(1, std::memory_order_release);
    assert(prev < num_assets_ && "ts_close: 计数超过资产数 (重复计?)");
    if (prev + 1 == num_assets_) {
      ts_days_done_.fetch_add(1, std::memory_order_relaxed);
      Logger::log("store", "ts_day_done: " + std::string(day.slot->date));
    }
  }

  // Worker 自报已完成日数 (领养观测面, release 兼发布 core 构造/状态)
  void ts_report_frontier(int worker_id, size_t days_done) {
    assert(worker_id >= 0 && worker_id < static_cast<int>(num_ts_workers_));
    ts_frontier_[worker_id].store(static_cast<uint32_t>(days_done), std::memory_order_release);
  }

  size_t ts_frontier(int worker_id) const {
    assert(worker_id >= 0 && worker_id < static_cast<int>(num_ts_workers_));
    return ts_frontier_[worker_id].load(std::memory_order_acquire);
  }

  // ===== CS WORKER API: 每 date 一对 open/close =====

  // 按日门控: 阻塞至本日 slot 存在且 asset-day 计满. 返回后本日三层张量
  // 整体可读 —— CS 内部不需要任何行级等待. 等待发生在日粒度, 轮询开销无所谓.
  //
  // 无锁 (与 io_try_flush 同姿态; 锁只属于 ts_open 的分配互斥):
  //   date 在 ts_open 的 release store(BUSY) 之前写完, acquire 观察到 BUSY 即可读;
  //   BUSY→DONE 只由本 CS 线程自己做 (cs_close), 不存在读期状态漂移.
  Day cs_try_open(const std::string &date) {
    for (size_t i = 0; i < pool_size_; ++i) {
      Slot &s = pool_[i];
      if (s.state.load(std::memory_order_acquire) == TensorState::BUSY && date == s.date &&
          s.assets_done.load(std::memory_order_acquire) == num_assets_)
        return {&s, num_assets_};
    }
    return {};
  }

  Day cs_open(const std::string &date) {
    size_t backoff_us = 100;
    Day day = cs_try_open(date);
    while (!day) {
      std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
      backoff_us = std::min<size_t>(backoff_us * 2, 1000);
      day = cs_try_open(date);
    }
    return day;
  }

  // CS 写完本日全部截面列, slot 交给 IO (BUSY→DONE)
  void cs_close(const Day &day) {
    assert(day.slot);
    Logger::log("store", "cs_close: " + std::string(day.slot->date));
    [[maybe_unused]] const TensorState prev = day.slot->state.exchange(TensorState::DONE, std::memory_order_acq_rel);
    assert(prev == TensorState::BUSY && "cs_close: 状态机乱序");
    cs_days_done_.fetch_add(1, std::memory_order_relaxed);
  }

  // ===== IO WORKER API =====

  // 摘一个 DONE slot 落盘, reset 后归还 (FREE). 返回 false = 暂无可刷.
  // CAS 摘取 + reset 后 release 发布, 与 ts_open 的 acquire 配对 —— 全程无锁.
  bool io_try_flush() {
    Slot *slot = nullptr;
    for (size_t i = 0; i < pool_size_ && !slot; ++i) {
      TensorState expected = TensorState::DONE;
      if (pool_[i].state.compare_exchange_strong(expected, TensorState::FLUSH, std::memory_order_acq_rel, std::memory_order_acquire))
        slot = &pool_[i];
    }
    if (!slot)
      return false;

    const std::string date(slot->date);
    disk_write(date, slot);

    slot->reset(num_assets_); // 清张量 + date + 计数: FREE 态不变量
    slot->state.store(TensorState::FREE, std::memory_order_release);
    Logger::log("store", "io_flush: " + date + " complete");
    return true;
  }

  // ===== QUERY API =====
  size_t query_A() const { return num_assets_; }
  size_t query_slots() const { return pool_size_; }
  size_t query_ts_workers() const { return num_ts_workers_; }
  size_t query_ts_days_done() const { return ts_days_done_.load(std::memory_order_relaxed); } // asset-day 计满的日数
  size_t query_cs_days_done() const { return cs_days_done_.load(std::memory_order_relaxed); }

private:
  // Write file with header + compressed data
  //
  // header[3] = A 轴前缀指纹 (AssetAxis::hash_at(A)): 列 → 资产的映射不存在
  // 文件里, 全靠 A 轴顺序. 存下指纹后, 读文件时 O(1) 就能确认"该文件的列序与
  // 当前注册表前 A 条一致"; 轴 append-only, 所以追加新资产不会动历史文件的
  // 指纹. 热路径 (解压/索引) 不受影响.
  //
  // header[4] = 字段表指纹 (LEVELS[lvl].fingerprint): 字段表
  // 增删改列即变, 读旧文件断言失败而不是静默错位.
  void write_file_with_header(const std::string &filepath, size_t T, size_t F, size_t A,
                              uint64_t table_fp, const void *raw_data, size_t raw_size) {
    const size_t header[FEATURE_FILE_HEADER_WORDS] = {T, F, A, static_cast<size_t>(axis_hash_), static_cast<size_t>(table_fp)};

    // 原子发布: 写 .tmp 再同目录 rename (POSIX 原子) —— 半写文件不可能以正式名
    // 存在. disk_write 按层序落盘, 最后一层的整层文件因此兼任本日 commit 标记
    // (FeatureRead::has_date 的判据): 它在则全日齐备, 中断只留 .tmp 残片.
    const std::string tmp_path = filepath + ".tmp";
    {
      std::ofstream file(tmp_path, std::ios::binary);
      assert(file.is_open());
      file.write(reinterpret_cast<const char *>(header), sizeof(header));

      if constexpr (ZstdHelper::COMPRESSION_LEVEL == 0) {
        // 无压缩: 张量直写, 不过中转缓冲
        file.write(reinterpret_cast<const char *>(raw_data), raw_size);
      } else {
        const size_t compressed_bound = ZstdHelper::compress_bound(raw_size);
        io_buf_.resize(compressed_bound); // 复用容量, 稳态零分配
        const size_t compressed_size = ZstdHelper::compress_to_buffer(raw_data, raw_size, io_buf_.data(), compressed_bound);
        file.write(reinterpret_cast<const char *>(io_buf_.data()), compressed_size);
      }
      assert(file.good());
    }
    std::filesystem::rename(tmp_path, filepath);
  }

  void disk_write(const std::string &date_str, Slot *slot) {
    assert(slot && date_str.size() == 8);

    auto t_start = std::chrono::high_resolution_clock::now();
    Logger::log("store", "disk_write: START " + date_str);

    const std::string out_dir = feature_day_dir(output_dir_, date_str);

    auto t_before_mkdir = std::chrono::high_resolution_clock::now();
    std::filesystem::create_directories(out_dir);
    auto t_after_mkdir = std::chrono::high_resolution_clock::now();

    const size_t A = num_assets_;
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      const auto &L = LEVELS[lvl];
      if (L.columnar) {
        // 逐列: 抽出每列 [T][A] 单独落盘 (Dist 按列选读)
        io_column_.resize(L.rows * A); // 复用容量, 稳态零分配
        for (size_t f = 0; f < L.width; ++f) {
          for (size_t t = 0; t < L.rows; ++t)
            std::memcpy(&io_column_[t * A], &slot->data[lvl][(t * L.width + f) * A], A * sizeof(feature_storage_t));
          write_file_with_header(feature_column_file(out_dir, lvl, f), L.rows, 1, A, L.fingerprint, io_column_.data(), io_column_.size() * sizeof(feature_storage_t));
        }
      } else {
        write_file_with_header(feature_file(out_dir, lvl), L.rows, L.width, A, L.fingerprint, slot->data[lvl], level_bytes(lvl, A));
      }
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    auto mkdir_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_after_mkdir - t_before_mkdir).count();
    auto write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_after_mkdir).count();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();

    Logger::log("store",
                "disk_write: END " + date_str + " [mkdir:" + std::to_string(mkdir_ms) +
                    "ms write:" + std::to_string(write_ms) + "ms total:" + std::to_string(total_ms) + "ms]");
  }
};

// ============================================================================
// 写回 / 截面读写 API (fstore 命名空间): 层是模板参数 (0/1/2 = L0/L1/DEPTH),
// 字段用 <LVL>_Field 枚举; 布局常量一律取 constexpr LEVELS[LVL] (单一事实源),
// 常量下标下定址照样编译期折叠. 数据面走日句柄 (Day, 由 ts_open / cs_open
// 定址一次), 每次调用只剩指针算术.
//   ts_write<L>(day, t, field, a, value)               单值
//   ts_write_range<L>(day, t, f_begin, f_end, a, src)  字段闭区间 (可含宽字段) 按偏移连续写 src
//   ts_write_row<L>(day, t, a, dag)                    按字段表 SRC 列写一行全部 OP 列
//   cs_col<L>(day, t, field)                           某列 t 时刻全部资产的连续段 (CS 读源列 / 就地写目标列)
//   OP(node[, port]) → dag.node.last(port); CS/LABEL/FLAG/META 不在 row 写
// ============================================================================
namespace fstore {

// 唯一真正需要逐层宏展开的东西: 按字段表 SRC 列写一行全部 OP 列
template <size_t LVL>
struct RowWriter;
#define STORE_ROW_WRITER(name, num, fields, rows, psd, columnar)                                                             \
  template <>                                                                                                                \
  struct RowWriter<num> {                                                                                                    \
    template <class DAG>                                                                                                     \
    [[gnu::always_inline]] static inline void write_row(feature_storage_t *row, size_t A, [[maybe_unused]] const DAG &dag) { \
      namespace FO = name##_Field;                                                                                           \
      [[maybe_unused]] constexpr const auto &OFFS = name##_FIELD_OFFSETS;                                                    \
      fields(STORE_ROW_ONE)                                                                                                  \
    }                                                                                                                        \
  };

#define STORE_ROW_OP_PICK(_1, _2, NAME, ...) NAME
#define STORE_ROW_OP_1(node) dag.node.last()
#define STORE_ROW_OP_2(node, port) dag.node.last(dag.node.port)
#define STORE_ROW_OP(code, ...) row[OFFS[FO::code] * A] = STORE_ROW_OP_PICK(__VA_ARGS__, STORE_ROW_OP_2, STORE_ROW_OP_1, )(__VA_ARGS__);
#define STORE_ROW_CS(code, ...)
#define STORE_ROW_LABEL(code)
#define STORE_ROW_FLAG(code)
#define STORE_ROW_META(code, w)
#define STORE_ROW_ONE(code, c1, c2, norm, en, cn, desc, formula, src) SRC_DISPATCH(STORE_ROW, code, src)

ALL_LEVELS(STORE_ROW_WRITER)
#undef STORE_ROW_WRITER

template <size_t LVL>
[[gnu::always_inline]] inline feature_storage_t *ts_row(const GlobalFeatureStore::Day &d, size_t t, size_t a) {
  assert(d.slot && d.slot->data[LVL] && "ts_row: day not open");
  assert(t < LEVELS[LVL].rows && "time index out of bounds");
  assert(a < d.A && "asset index out of bounds");
  return d.slot->data[LVL] + (t * LEVELS[LVL].width) * d.A + a;
}

template <size_t LVL>
[[gnu::always_inline]] inline void ts_write(const GlobalFeatureStore::Day &d, size_t t, size_t field, size_t a, float value) {
  ts_row<LVL>(d, t, a)[LEVELS[LVL].offsets[field] * d.A] = value;
}

// 字段闭区间 [f_begin, f_end] (可含宽字段) 按偏移连续写 src[0 .. span), span = 区间总宽
template <size_t LVL>
[[gnu::always_inline]] inline void ts_write_range(const GlobalFeatureStore::Day &d, size_t t, size_t f_begin, size_t f_end, size_t a, const float *src) {
  assert(f_begin <= f_end && "invalid field range");
  feature_storage_t *row = ts_row<LVL>(d, t, a);
  const size_t o_begin = LEVELS[LVL].offsets[f_begin], o_end = LEVELS[LVL].offsets[f_end] + LEVELS[LVL].fields[f_end].width;
  for (size_t o = o_begin; o < o_end; ++o)
    row[o * d.A] = src[o - o_begin];
}

template <size_t LVL, class DAG>
[[gnu::always_inline]] inline void ts_write_row(const GlobalFeatureStore::Day &d, size_t t, size_t a, const DAG &dag) {
  RowWriter<LVL>::write_row(ts_row<LVL>(d, t, a), d.A, dag);
}

// CS worker: 某层某列在 t 时刻全部资产的连续段 (A 个). 读源列和就地写目标列
// 都是它 —— 与 TS 侧同构, 定址编译期
template <size_t LVL>
[[gnu::always_inline]] inline feature_storage_t *cs_col(const GlobalFeatureStore::Day &d, size_t t, size_t field) {
  assert(d.slot && d.slot->data[LVL] && "cs_col: day not open");
  assert(t < LEVELS[LVL].rows && "time index out of bounds");
  assert(field < LEVELS[LVL].field_count && "field index out of bounds");
  return d.slot->data[LVL] + (t * LEVELS[LVL].width + LEVELS[LVL].offsets[field]) * d.A;
}

} // namespace fstore
