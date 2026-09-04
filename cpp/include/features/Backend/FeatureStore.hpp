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
inline constexpr size_t kPoolSlots = 4;

// ============================================================================
// FEATURE STORE - 按日门控的张量池
//
// 并发模型 (回测): TS worker 是资产串行的 (一个资产写完一整天才换下一个),
// "秒级流式伴随"在这种调度下没有可用的进度语义 —— 且 label / L1 分钟行都是
// 向过去回填, 行级进度天然罩不住. 所以门控就是按日:
//
//   TS:  ts_open(date, w) → 句柄写 (纯指针算术, 无锁无验证) → ts_close
//   CS:  cs_open(date) 阻塞至全部 TS ts_close → 整日扫 → cs_close
//   IO:  io_try_flush_one: 摘 DONE → 落盘 → reset → FREE
//
// slot 生命周期: FREE → BUSY (首个 ts_open) → DONE (cs_close) → FLUSH → FREE.
// 句柄在 open..close 之间被状态机钉住 (TS 未齐 done 不给 CS, CS 未 done 不给
// IO), 有效期内不可能被回收 —— 不需要 epoch / 每次访问的缓存验证. 数值一致性
// 与调度形态无关, 由输入契约锚定 (见 CoreSequential.hpp / CoreCrosssection.hpp),
// 按日门控只是回测里的因果保证.
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
    std::atomic<uint32_t> ts_done_count{0};           // == num_ts_workers ⇒ 本日 TS 全部写完 (CS 放行)
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
      ts_done_count.store(0, std::memory_order_relaxed);
    }

    ~Slot() {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
        free(data[lvl]);
    }
  };

  // 句柄: open 时定址一次, 之后所有读写是纯指针算术 —— 热路径不再有
  // 字符串比较 / 原子验证 / slot 查找 (原先每 tick 4-6 次).
  struct TsDay {
    Slot *slot = nullptr;
    size_t A = 0;
  };
  struct CsDay {
    Slot *slot = nullptr;
    size_t A = 0;
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
  // slot 就 kPoolSlots 个, 线性扫 state 就是查找和空闲表.
  Slot *pool_ = nullptr;
  std::mutex pool_mutex_; // 只串行化 ts_open 的查找/分配; 其余状态转移无锁

  // IO worker 专用复用缓冲 (单线程): 列抽取 / 头 + 压缩输出, 稳态零分配
  std::vector<feature_storage_t> io_column_;
  std::vector<uint8_t> io_buf_;

public:
  // axis_hash: AssetAxis::hash_at(num_assets), 写进每个特征文件头锁定列序
  GlobalFeatureStore(size_t num_assets, size_t num_ts_workers,
                     std::uint64_t axis_hash,
                     const std::string &output_dir = "")
      : axis_hash_(axis_hash),
        num_assets_(num_assets), num_ts_workers_(num_ts_workers),
        pool_size_(kPoolSlots) {

    if (!output_dir.empty()) {
      output_dir_ = output_dir;
      if (std::filesystem::exists(output_dir_)) {
        std::filesystem::remove_all(output_dir_);
      }
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

      // Touch all pages to force physical allocation
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        const size_t total_bytes = level_bytes(lvl, num_assets);
        const size_t page_size = 4096;
        for (size_t offset = 0; offset < total_bytes; offset += page_size) {
          reinterpret_cast<volatile char *>(pool_[i].data[lvl])[offset] = 0;
        }
        reinterpret_cast<volatile char *>(pool_[i].data[lvl])[total_bytes - 1] = 0;
      }

      pool_[i].reset(num_assets_); // FREE 态不变量: 张量已清零, 拿来即写

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

  // ===== TS WORKER API: 每 (worker, date) 一对 open/close =====

  // 找到 / 分配本日 slot. 首个到达的 worker 分配 (FREE→BUSY), 其余直接命中.
  // slot 在 flush 时已 reset (FREE 态不变量: 张量清零), 拿来即写.
  // 池满则等 IO 释放 —— 这是 TS 超前于 CS/IO 的唯一背压点.
  TsDay ts_open(const std::string &date, int worker_id) {
    assert(worker_id >= 0 && worker_id < static_cast<int>(num_ts_workers_));
    std::unique_lock<std::mutex> lock(pool_mutex_);
    for (int waited = 0;; ++waited) {
      // 本日已被其他 worker 打开. 只匹配 BUSY: 日期严格向前推进, TS 不会
      // 重开已过 CS/IO 阶段的日子; FLUSH 中的 slot date 正被 reset, 不可读.
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
          Logger::log("worker_" + std::to_string(worker_id), "ts_open: " + date + " → pool[" + std::to_string(i) + "]" + (waited ? " (waited " + std::to_string(waited * 10) + "ms)" : ""));
          return {&s, num_assets_};
        }
      }
      if (waited == 0)
        Logger::log("worker_" + std::to_string(worker_id), "Pool exhausted, waiting...");
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      lock.lock();
    }
  }

  // 本 worker 本日全部资产写完. 计数攒齐 num_ts_workers 后 cs_open 放行;
  // release 与 cs_open 的 acquire 配对, 保证张量写入对 CS 可见.
  void ts_close(const TsDay &day, int worker_id) {
    assert(day.slot);
    [[maybe_unused]] const uint32_t prev = day.slot->ts_done_count.fetch_add(1, std::memory_order_release);
    assert(prev < num_ts_workers_ && "ts_close: 计数超过 worker 数 (重复 close?)");
    Logger::log("worker_" + std::to_string(worker_id), "ts_close: " + std::string(day.slot->date));
  }

  // ===== CS WORKER API: 每 date 一对 open/close =====

  // 按日门控: 阻塞至本日 slot 存在且全部 TS ts_close. 返回后本日三层张量
  // 整体可读 —— CS 内部不需要任何行级等待. 等待发生在日粒度, 轮询开销无所谓.
  CsDay cs_open(const std::string &date) {
    size_t backoff_us = 100;
    while (true) {
      {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        for (size_t i = 0; i < pool_size_; ++i) {
          Slot &s = pool_[i];
          if (s.state.load(std::memory_order_acquire) == TensorState::BUSY && date == s.date &&
              s.ts_done_count.load(std::memory_order_acquire) == num_ts_workers_)
            return {&s, num_assets_};
        }
      }
      std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
      backoff_us = std::min<size_t>(backoff_us * 2, 1000);
    }
  }

  // CS 写完本日全部截面列, slot 交给 IO (BUSY→DONE)
  void cs_close(const CsDay &day) {
    assert(day.slot);
    Logger::log("store", "cs_close: " + std::string(day.slot->date));
    [[maybe_unused]] const TensorState prev = day.slot->state.exchange(TensorState::DONE, std::memory_order_acq_rel);
    assert(prev == TensorState::BUSY && "cs_close: 状态机乱序");
  }

  // ===== IO WORKER API =====

  // 摘一个 DONE slot 落盘, reset 后归还 (FREE). 返回 false = 暂无可刷.
  // CAS 摘取 + reset 后 release 发布, 与 ts_open 的 acquire 配对 —— 全程无锁.
  bool io_try_flush_one() {
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
    const size_t header_size = FEATURE_FILE_HEADER_WORDS * sizeof(size_t);
    const size_t compressed_bound = ZstdHelper::compress_bound(raw_size);

    io_buf_.resize(header_size + compressed_bound); // 复用容量, 稳态零分配
    size_t *header = reinterpret_cast<size_t *>(io_buf_.data());
    header[0] = T;
    header[1] = F;
    header[2] = A;
    header[3] = static_cast<size_t>(axis_hash_);
    header[4] = static_cast<size_t>(table_fp);

    const size_t compressed_size = ZstdHelper::compress_to_buffer(raw_data, raw_size,
                                                                  io_buf_.data() + header_size,
                                                                  compressed_bound);

    std::ofstream file(filepath, std::ios::binary);
    assert(file.is_open());
    file.write(reinterpret_cast<const char *>(io_buf_.data()), header_size + compressed_size);
    assert(file.good());
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
// 字段用 <LVL>_Field 枚举, 定址全部编译期; 数据面走句柄 (TsDay / CsDay, 由
// ts_open / cs_open 定址一次), 每次调用只剩指针算术.
//   ts_write<L>(day, t, field, a, value)               单值
//   ts_write_range<L>(day, t, f_begin, f_end, a, src)  字段闭区间 (可含宽字段) 按偏移连续写 src
//   ts_write_row<L>(day, t, a, dag)                    按字段表 SRC 列写一行全部 OP 列
//   cs_read<L>(day, t, field) / cs_write<L>(...)       CS worker: 一列全部资产
//   OP(node[, port]) → dag.node.last(port); CS/LABEL/FLAG/META 不在 row 写
// ============================================================================
namespace fstore {

template <size_t LVL>
struct Level;
#define STORE_LEVEL_TRAITS(name, num, fields, rows, psd, columnar)                                                           \
  template <>                                                                                                                \
  struct Level<num> {                                                                                                        \
    static constexpr const auto &OFFS = name##_FIELD_OFFSETS;                                                                \
    static constexpr const auto &INFO = name##_FIELD_INFO;                                                                   \
    static constexpr size_t F = name##_TOTAL_WIDTH;                                                                          \
    static constexpr size_t T = rows;                                                                                        \
    template <class DAG>                                                                                                     \
    [[gnu::always_inline]] static inline void write_row(feature_storage_t *row, size_t A, [[maybe_unused]] const DAG &dag) { \
      namespace FO = name##_Field;                                                                                           \
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

ALL_LEVELS(STORE_LEVEL_TRAITS)
#undef STORE_LEVEL_TRAITS

template <size_t LVL>
[[gnu::always_inline]] inline feature_storage_t *ts_row(const GlobalFeatureStore::TsDay &d, size_t t, size_t a) {
  assert(d.slot && d.slot->data[LVL] && "ts_row: day not open");
  assert(t < Level<LVL>::T && "time index out of bounds");
  assert(a < d.A && "asset index out of bounds");
  return d.slot->data[LVL] + (t * Level<LVL>::F) * d.A + a;
}

template <size_t LVL>
[[gnu::always_inline]] inline void ts_write(const GlobalFeatureStore::TsDay &d, size_t t, size_t field, size_t a, float value) {
  ts_row<LVL>(d, t, a)[Level<LVL>::OFFS[field] * d.A] = value;
}

// 字段闭区间 [f_begin, f_end] (可含宽字段) 按偏移连续写 src[0 .. span), span = 区间总宽
template <size_t LVL>
[[gnu::always_inline]] inline void ts_write_range(const GlobalFeatureStore::TsDay &d, size_t t, size_t f_begin, size_t f_end, size_t a, const float *src) {
  assert(f_begin <= f_end && "invalid field range");
  feature_storage_t *row = ts_row<LVL>(d, t, a);
  const size_t o_begin = Level<LVL>::OFFS[f_begin], o_end = Level<LVL>::OFFS[f_end] + Level<LVL>::INFO[f_end].width;
  for (size_t o = o_begin; o < o_end; ++o)
    row[o * d.A] = src[o - o_begin];
}

template <size_t LVL, class DAG>
[[gnu::always_inline]] inline void ts_write_row(const GlobalFeatureStore::TsDay &d, size_t t, size_t a, const DAG &dag) {
  Level<LVL>::write_row(ts_row<LVL>(d, t, a), d.A, dag);
}

// CS worker: 某层某列在 t 时刻全部资产的连续段 (A 个); 与 TS 侧同构, 定址编译期
template <size_t LVL>
[[gnu::always_inline]] inline feature_storage_t *cs_read(const GlobalFeatureStore::CsDay &d, size_t t, size_t field) {
  assert(d.slot && d.slot->data[LVL] && "cs_read: day not open");
  assert(t < Level<LVL>::T && "time index out of bounds");
  assert(field < std::size(Level<LVL>::INFO) && "field index out of bounds");
  return d.slot->data[LVL] + (t * Level<LVL>::F + Level<LVL>::OFFS[field]) * d.A;
}

template <size_t LVL>
inline void cs_write(const GlobalFeatureStore::CsDay &d, size_t t, size_t field, const feature_storage_t *src, size_t count) {
  assert(count <= d.A && "count exceeds num_assets");
  std::memcpy(cs_read<LVL>(d, t, field), src, count * sizeof(feature_storage_t));
}

} // namespace fstore
