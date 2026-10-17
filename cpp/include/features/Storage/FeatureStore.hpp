#pragma once

#include "FeatureStoreConfig.hpp"
#include "misc/exec.hpp"
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
#include <string>
#include <thread>

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
// FEATURE STORE - 日环张量池 (日流水线的唯一握手面)
//
// 门控按日: TS worker 资产串行 (一个资产写完一整天才换下一个), 且 label / L1 分钟行
// 向过去回填, 行级进度天然罩不住 —— 所以流水线单位是日, TS 计数粒度是 asset-day.
//
// 四段各持一个单调的已完成天数, 段间只靠它们握手 (Disruptor 式):
//   预取 pf_done_ ─→ 时序 ts_done_ ─→ 截面 cs_done_ ─→ 落盘 io_done_
//                      ↑ 背压: 日 d 可开 ⇔ d < io_done_ + P ─────────┘
// 截面 / 落盘按日序推进, 在飞日恒为连续区间 [io_done_, io_done_ + P), 日 d 占 pool_[d % P]
// —— 无状态枚举 / 无锁 / 无查找; slot 由 io_close 清零后经 io_done_ 的 release 交还.
//
// 各段 open / close 同构 (try_open 非阻塞; 等待 / 取消是 worker 骨架的事, 见 feature_workers.hpp):
//   PF: pf_try_open(d)  d ≤ ts_done_ + P + kPrefetchLead → 备源 (不占 slot) → pf_close
//                       软源 (Lob: 暖逐笔 page cache, TS 不等它) / 硬源 (Bar: 解码 bar1m 进源环, TS 等 pf_ready)
//   TS: ts_try_open(d)  d < io_done_ + P → 句柄写 (纯指针算术) → 每 (asset, d) 一次 ts_close, 计满全轴 → ts_done_ + 1
//   CS: cs_try_open(d)  本日计满全轴 → 逐秒扫 → cs_close → cs_done_ = d + 1
//   IO: io_try_open(d)  d < cs_done_ → 落盘 (FeatureDay::write, 池不碰格式) → io_close (清零) → io_done_ = d + 1
//
// TS 按 (asset, d) 计数 (而非按 worker) 使资产处置权可在 worker 间转移: 领跑 worker 可接手
// 落后 worker 的资产并回填旧日 (旧日未计满 ⇒ 仍在飞, ts_try_open 必得), 见 TsSchedule.
// 资产内日序由调度侧的 claim/done 原子保证, 因此 "d+1 日计满 ⇒ d 日已计满" —— ts_done_ 只增.
// 开盘档标签 (T+N) 跨日回填: 旧日 ts_close 推迟到标签结清 (见 CoreTs), 故 P ≥ LabelHold::MIN_POOL_SLOTS.
//
// 句柄在有效期内被握手钉住 (TS 未计满不给 CS, CS 未 close 不给 IO, IO 未 close 不复用),
// 不需要 epoch / 每次访问的验证. 数值一致性与调度形态无关, 由输入契约锚定
// (见 CoreLob.hpp / CoreCs.hpp), 按日门控只是回测里的因果保证.
//
// 布局 [T][F][A] 服务于事件驱动的两个写读姿态: TS 每笔写一行 (t, *, a), CS 每秒
// 读写 (t, f, *) 一段连续 A. 落盘的列主序只在 FeatureDay 里转换, 不反过来改内存布局.
// 性能约束: TS / CS 热路径 (逐笔 / 逐秒) 只许指针算术 —— 无锁、无查找、无分配、无
// 运行时校验 (只 assert); 握手开销全压在每日一次的 open / close 上.
//
// 档 T (lob::Tier / bar::Tier, 见 FeatureStoreConfig) 是模板参数: 布局常量取 T::LEVELS, 档缺的层不分配
// (data[lvl] == nullptr). Slot / Day 独立成模板, fstore:: 的写读 API 从句柄推导档.
// ============================================================================

// 某档某层一天张量字节数 [T][F][A] (档缺的层 F = 0 → 0)
template <class T>
constexpr size_t store_level_bytes(size_t lvl, size_t num_assets) {
  return T::LEVELS[lvl].rows * T::LEVELS[lvl].field_count * num_assets * sizeof(feature_storage_t);
}

// 日 d 的张量 (= pool_[d % P])
template <class T>
struct StoreSlot {
  std::atomic<uint32_t> assets_done{0};             // == num_assets ⇒ 本日 TS 全部写完 (CS 放行)
  feature_storage_t *data[LEVEL_COUNT] = {nullptr}; // 每层 [T][F][A]; 档缺的层 nullptr

  void allocate(size_t num_assets) {
    // 张量按 2MB 对齐并 madvise(MADV_HUGEPAGE): 透明大页是普通匿名内存,
    // 进程退出/崩溃由内核自动回收, 不需要 nr_hugepages 静态预留 (那种预留
    // 会从 MemAvailable 里整块扣掉, 谁都用不了). 需要系统
    // /sys/kernel/mm/transparent_hugepage/enabled = madvise 或 always.
    // 收益: TS 写行是列间 stride (F 列 × A×2B ≈ 11.8KB), 4K 页下每 tick
    // 踩 F 个不同页, dTLB 压力大; 2M 页覆盖整列邻域, miss 显著减少.
    constexpr size_t kHugePage = 2ull * 1024 * 1024;
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      if (!T::has(lvl))
        continue;
      // aligned_alloc 要求 size 是 alignment 的整数倍; 每层最多多占 <2MB
      const size_t aligned_bytes = ((store_level_bytes<T>(lvl, num_assets) + kHugePage - 1) / kHugePage) * kHugePage;
      data[lvl] = static_cast<feature_storage_t *>(aligned_alloc(kHugePage, aligned_bytes));
#if defined(__linux__)
      madvise(data[lvl], aligned_bytes, MADV_HUGEPAGE);
#endif
      assert(data[lvl]);
    }
  }

  void reset(size_t num_assets) {
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
      if (T::has(lvl))
        std::memset(data[lvl], 0, store_level_bytes<T>(lvl, num_assets));
    assets_done.store(0, std::memory_order_relaxed);
  }

  ~StoreSlot() {
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
      free(data[lvl]);
  }
};

// 日句柄 (TS/CS/IO 同一形态): open 时定址一次, 之后所有读写是纯指针算术 ——
// 热路径不再有字符串比较 / 原子验证 / slot 查找 (原先每 tick 4-6 次).
// 空句柄 = 没拿到 (池满 / 本日未就绪), try_open 以此表示失败.
template <class T>
struct StoreDay {
  StoreSlot<T> *slot = nullptr;
  size_t A = 0;
  size_t d = 0; // 档日期轴下标 (StageCtx::dates), close 推进已完成天数用
  explicit operator bool() const { return slot != nullptr; }
};

// 日环握手面 (档无关): 尺寸 + 四段已完成天数 + 预取段 open / close. 仪表盘只读它, 不随档实例化
class StoreRing {
public:
  // 预取领先 TS 的余量 (天): 窗口 = 在飞跨度上限 P + 余量, 保证最快 TS 到达前源已就绪
  static constexpr size_t kPrefetchLead = 2;

protected:
  const size_t num_assets_; // A 维 = 档 A 轴大小 (张量/落盘/门控全按它)
  size_t pool_size_;        // P

  // 各段已完成天数 (单调): 流水线的全部握手面, 进度面也只读它们
  std::atomic<size_t> pf_done_{0};
  std::atomic<size_t> ts_done_{0}; // 计满全轴的日数 (fetch_add, 见 ts_close)
  std::atomic<size_t> cs_done_{0};
  std::atomic<size_t> io_done_{0};

  StoreRing(size_t num_assets, size_t pool_slots) : num_assets_(num_assets), pool_size_(pool_slots) {}

public:
  // ===== PF: 不占 slot, 只有门控与进度 =====

  // 窗口 = TS 在飞跨度上限 P + 余量. 日 d 进窗 ⇒ d − (P + kPrefetchLead + 1) 已全员计满 (源环可复用其格, 见 feed_ring_size)
  bool pf_try_open(size_t d) const { return d <= ts_done_.load(std::memory_order_relaxed) + pool_size_ + kPrefetchLead; }

  // release 与 pf_ready 的 acquire 配对 (硬源: 解码写入先于 TS 读)
  void pf_close(size_t d) {
    assert(d == pf_done_.load(std::memory_order_relaxed) && "pf_close: 日序乱");
    pf_done_.store(d + 1, std::memory_order_release);
  }
  bool pf_ready(size_t d) const { return d < pf_done_.load(std::memory_order_acquire); }

  // 源环格数: 预取窗口跨度 [ts_done_, ts_done_ + P + kPrefetchLead] 全部在格
  size_t feed_ring_size() const { return pool_size_ + kPrefetchLead + 1; }

  // ===== QUERY API =====
  size_t query_A() const { return num_assets_; }
  size_t query_slots() const { return pool_size_; }
  size_t query_pf_done() const { return pf_done_.load(std::memory_order_relaxed); }
  size_t query_ts_done() const { return ts_done_.load(std::memory_order_relaxed); } // 计满全轴的日数 (= 最慢 TS 前沿)
  size_t query_cs_done() const { return cs_done_.load(std::memory_order_relaxed); }
  size_t query_io_done() const { return io_done_.load(std::memory_order_relaxed); }
};

template <class T>
class FeatureStore : public StoreRing {
public:
  using Tier = T;
  using Slot = StoreSlot<T>;
  using Day = StoreDay<T>;

private:
  static constexpr size_t level_bytes(size_t lvl, size_t num_assets) { return store_level_bytes<T>(lvl, num_assets); }

  // Config
  const std::uint64_t axis_hash_; // A 轴指纹 (UniverseAxis::hash), 落进每个特征文件头
  std::string output_dir_;

  // 日环: 日 d 占 pool_[d % P]. 不需要 date→slot map / freelist / epoch / 状态枚举 (见文件头)
  Slot *pool_ = nullptr;

  // 日 d 的句柄. 前置: d 在飞 (io_done_ ≤ d < io_done_ + P), 由各 try_open 的门控保证
  Day day_at(size_t d) { return {&pool_[d % pool_size_], num_assets_, d}; }

public:
  // num_assets: 档 A 轴大小; axis_hash: UniverseAxis::hash, 写进每个
  // 特征文件头锁定"列 → 资产"映射 (读端从同一 FeatureLib 推导同一轴比对)
  FeatureStore(size_t num_assets,
               std::uint64_t axis_hash,
               const std::string &output_dir = "",
               size_t pool_slots = kDefaultPoolSlots)
      : StoreRing(num_assets, pool_slots),
        axis_hash_(axis_hash) {
    assert(pool_size_ >= 2 && "Pool size too small, need at least 2 slots");
    assert(num_assets_ >= 1 && "A 轴为空");

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

    std::cout << "\n=== Feature Store Tensor Pool (" << tier_name(T::kId) << ") ===\n";

    // Detailed dimension breakdown
    std::cout << "Dimension Details:\n";
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      if (!T::has(lvl))
        continue;
      const auto &L = T::LEVELS[lvl];
      printf("  %-6s [T=%6zu][F=%4zu][A=%4zu] = %.2f MB\n", L.level_name, L.rows, L.field_count, num_assets, level_bytes(lvl, num_assets) / (1024.0 * 1024.0));
    }

    printf("\nPer-day tensor: %.2f MB | Pool size: %zu slots | Total: %.2f GB\n",
           bytes_per_day / (1024.0 * 1024.0),
           pool_size_,
           (bytes_per_day * pool_size_) / (1024.0 * 1024.0 * 1024.0));

    // Allocate pool (Slot array)
    pool_ = new Slot[pool_size_];

    std::cout << "Allocating physical pages...\n";
    auto start = std::chrono::steady_clock::now();

    // Parallel allocation: 一槽一任务
    std::atomic<size_t> progress_counter{0};
    misc::exec::ForkJoin ex(static_cast<int>(std::min<size_t>(pool_size_, std::thread::hardware_concurrency())));
    ex.tasks(static_cast<int>(pool_size_), [&](int i, int) {
      pool_[i].allocate(num_assets_);
      pool_[i].reset(num_assets_); // memset 清零 = 坐实物理页 + 空闲态不变量: 拿来即写

      const size_t completed = ++progress_counter;
      std::cout << "  " << completed << "/" << pool_size_ << " slots\r" << std::flush;
    });

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    std::cout << "\nPhysical allocation complete in " << elapsed << " ms\n";
    std::cout << "=================================\n\n";
  }

  ~FeatureStore() {
    if (pool_) {
      delete[] pool_; // Slot destructors called automatically
    }
  }

  // ===== TS: open 按 d, close 按 (asset, d) =====

  // 背压: 日 d 的 slot 须已由 d − P 落盘交还. 回填旧日也走这里 (旧日未计满 ⇒ 仍在飞, 必得).
  // slot 在 io_close 已清零, 拿来即写. 空句柄 = 池满 —— 调用方拿等待时间去干别的 (领跑者在池边领养回填).
  // acquire 与 io_close 的 release 配对 (清零先于复用).
  Day ts_try_open(size_t d) {
    const size_t io = io_done_.load(std::memory_order_acquire);
    assert(d >= io && "ts_try_open: 日 d 已落盘 (调度侧日序乱)");
    return d < io + pool_size_ ? day_at(d) : Day{};
  }

  // 一个 (asset, d) 写完. 计数攒齐全轴 (universe 子轴) 后 CS 放行;
  // release 与 cs_try_open 的 acquire 配对, 保证张量写入对 CS 可见.
  // 缺 binary 的 asset-day 也要计 (张量保持默认值), 由当时的处置权持有者计.
  void ts_close(const Day &day) {
    assert(day);
    const uint32_t prev = day.slot->assets_done.fetch_add(1, std::memory_order_release);
    assert(prev < num_assets_ && "ts_close: 计数超过派活资产数 (重复计?)");
    if (prev + 1 == num_assets_) {
      ts_done_.fetch_add(1, std::memory_order_relaxed);
      Logger::log("store", "ts_day_done: d=" + std::to_string(day.d));
    }
  }

  // ===== CS: 每 d 一对 open/close =====

  // 按日门控: 本日计满全轴才放行, 返回后两层张量整体可读 —— CS 内部不需要任何行级等待.
  // 先验 slot 已轮到日 d (d < io_done_ + P): 否则 pool_[d % P] 还是已计满待落盘的 d − P, 计数会误放行.
  Day cs_try_open(size_t d) {
    assert(d == cs_done_.load(std::memory_order_relaxed) && "cs_try_open: 日序乱");
    if (d >= io_done_.load(std::memory_order_acquire) + pool_size_)
      return {};
    const Day day = day_at(d);
    return day.slot->assets_done.load(std::memory_order_acquire) == num_assets_ ? day : Day{};
  }

  // CS 写完本日全部截面列, 交给 IO. release 与 io_try_open 的 acquire 配对
  void cs_close(const Day &day) {
    assert(day && day.d == cs_done_.load(std::memory_order_relaxed) && "cs_close: 日序乱");
    cs_done_.store(day.d + 1, std::memory_order_release);
    Logger::log("store", "cs_close: d=" + std::to_string(day.d));
  }

  // ===== IO: 每 d 一对 open/close =====

  // 截面已 close 的日才放行; 返回后本日两层张量只归 IO 读 (落盘见 FeatureDay::write)
  Day io_try_open(size_t d) {
    assert(d == io_done_.load(std::memory_order_relaxed) && "io_try_open: 日序乱");
    return d < cs_done_.load(std::memory_order_acquire) ? day_at(d) : Day{};
  }

  // 本日落盘完毕: 清零后交还 (空闲态不变量: 张量 + 计数全零, 拿来即写).
  // release 与 ts_try_open / cs_try_open 的 acquire 配对
  void io_close(const Day &day) {
    assert(day && day.d == io_done_.load(std::memory_order_relaxed) && "io_close: 日序乱");
    day.slot->reset(num_assets_);
    io_done_.store(day.d + 1, std::memory_order_release);
    Logger::log("store", "io_close: d=" + std::to_string(day.d));
  }

  // ===== QUERY API =====
  const std::string &query_dir() const { return output_dir_; } // 特征库目录 (日文件落在其下)
  std::uint64_t query_axis_hash() const { return axis_hash_; } // A 轴指纹 (落进每个日文件头)
};

using LobFeatureStore = FeatureStore<lob::Tier>;
using BarFeatureStore = FeatureStore<bar::Tier>;

// ============================================================================
// 写回 / 截面读写 API (fstore 命名空间): 层是模板参数 (0/1 = L0/L1), 档从日句柄推导,
// 字段用 <tier>::<LVL>_Field 枚举; 布局常量一律取 constexpr T::LEVELS[LVL] (单一事实源),
// 常量下标下定址照样编译期折叠. 数据面走日句柄 (Day, 由 ts_try_open / cs_try_open
// 定址一次), 每次调用只剩指针算术.
//   ts_write<L>(day, t, field, a, value)   单资产单列 (TS: 列间跨步 A)
//   ts_write_row<L>(day, t, a, dag)        按字段表 SRC 列写一行全部 TS 列
//   cs_col<L>(day, t, field)               单列全部资产的连续段 (CS 读源列 / 就地写目标列)
//   TS(node[, port], Tf, Method) → ts::Tf::apply(dag.node.last(port)); CS/LABEL/FLAG/META 不在 row 写
// ============================================================================
namespace fstore {

// 唯一真正需要逐档逐层宏展开的东西: 按字段表 SRC 列写一行全部 TS 列
template <class T, size_t LVL>
struct RowWriter;
#define STORE_ROW_WRITER(T, ns, name, num, fields, psd)                                                                      \
  template <>                                                                                                                \
  struct RowWriter<ns::Tier, num> {                                                                                          \
    template <class DAG>                                                                                                     \
    [[gnu::always_inline]] static inline void write_row(feature_storage_t *row, size_t A, [[maybe_unused]] const DAG &dag) { \
      namespace FO = ns::name##_Field;                                                                                       \
      fields(STORE_ROW_ONE)                                                                                                  \
    }                                                                                                                        \
  };

// TS(node, Tf, Method) / TS(node, port, Tf, Method): 落盘值 = ts::Tf::apply(节点输出). Method 占位未接入 (Method/TS.hpp ★)
#define STORE_ROW_TS_3(node, tf, m) ts::tf::apply(dag.node.last())
#define STORE_ROW_TS_4(node, port, tf, m) ts::tf::apply(dag.node.last(dag.node.port))
#define STORE_ROW_TS(code, ...) row[FO::code * A] = SRC_NODE_PICK(__VA_ARGS__, STORE_ROW_TS_4, STORE_ROW_TS_3, , )(__VA_ARGS__);
#define STORE_ROW_CS(code, ...)
#define STORE_ROW_LABEL(code)
#define STORE_ROW_FLAG(code)
#define STORE_ROW_ONE(code, c1, c2, en, cn, desc, formula, src) SRC_DISPATCH(STORE_ROW, code, src)

TIER_LEVELS(STORE_ROW_WRITER)
#undef STORE_ROW_WRITER

template <size_t LVL, class T>
[[gnu::always_inline]] inline feature_storage_t *ts_row(const StoreDay<T> &d, size_t t, size_t a) {
  static_assert(T::has(LVL), "该档没有这一层");
  assert(d.slot && d.slot->data[LVL] && "ts_row: day not open");
  assert(t < T::LEVELS[LVL].rows && "time index out of bounds");
  assert(a < d.A && "asset index out of bounds");
  return d.slot->data[LVL] + (t * T::LEVELS[LVL].field_count) * d.A + a;
}

template <size_t LVL, class T>
[[gnu::always_inline]] inline void ts_write(const StoreDay<T> &d, size_t t, size_t field, size_t a, float value) {
  assert(field < T::LEVELS[LVL].field_count && "field index out of bounds");
  ts_row<LVL>(d, t, a)[field * d.A] = value;
}

template <size_t LVL, class T, class DAG>
[[gnu::always_inline]] inline void ts_write_row(const StoreDay<T> &d, size_t t, size_t a, const DAG &dag) {
  RowWriter<T, LVL>::write_row(ts_row<LVL>(d, t, a), d.A, dag);
}

// CS worker: 某层某列在 t 时刻全部资产的连续段 (A 个). 读源列和就地写目标列
// 都是它 —— 与 TS 侧同构, 定址编译期
template <size_t LVL, class T>
[[gnu::always_inline]] inline feature_storage_t *cs_col(const StoreDay<T> &d, size_t t, size_t field) {
  static_assert(T::has(LVL), "该档没有这一层");
  assert(d.slot && d.slot->data[LVL] && "cs_col: day not open");
  assert(t < T::LEVELS[LVL].rows && "time index out of bounds");
  assert(field < T::LEVELS[LVL].field_count && "field index out of bounds");
  return d.slot->data[LVL] + (t * T::LEVELS[LVL].field_count + field) * d.A;
}

} // namespace fstore
