#pragma once

#include "FeatureStoreConfig.hpp"
#include "misc/exec.hpp"
#include "misc/logging.hpp"
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
#include <vector>

#ifdef __APPLE__
#include <sys/sysctl.h>
#else
#include <sys/mman.h> // madvise(MADV_HUGEPAGE): 日环上透明大页
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
inline constexpr size_t kDefaultRingSlots = 4;

// ============================================================================
// FEATURE STORE - 日环 (日流水线的唯一握手面)
//
// 流控按日: TS worker 资产串行 (一个资产写完一整天才换下一个), 且 label / L_min 分钟行
// 向过去回填, 行级进度天然罩不住 —— 所以流水线单位是日, TS 计数粒度是 asset-day.
//
// 四段各持一个单调的已完成天数, 段间只靠它们握手 (Disruptor 式):
//   预取 pf_done_ ─→ 时序 ts_done_ ─→ 截面 cs_done_ ─→ 落盘 io_done_
//                      ↑ 背压: 日 d 可开 ⇔ d < io_done_ + P ─────────┘
// io_done_ = 已落盘的连续前缀, 在飞日恒为连续区间 [io_done_, io_done_ + P), 日 d 占 ring_[d % P]
// —— 无状态枚举 / 无锁 / 无查找; slot 由 io_close 清零后经 io_done_ 的推进交还.
// 预取 / 截面 / 落盘各 K 核按日轮转 (日 d 归第 d % K 核, 核内日序递增), close 可乱序 ⇒ 段间握手是逐格的:
//   预取 → 时序: 源环格日标记 pf_mark_[d % R] = d + 1 (只硬源; pf_done_ 只是已备源日数, 仪表盘用)
//   截面 → 落盘: slot.cs_closed (cs_done_ 只是已截面日数, 仪表盘用)
//   落盘 → 时序/截面: slot.io_mark = d + 1, io_close 据此把 io_done_ 推到首个未落盘日
//
// 各段 open / close 同构 (try_open 非阻塞; 等待 / 取消是 worker 骨架的事, 见 feature_workers.hpp):
//   PF: pf_try_open(d)  d ≤ ts_done_ + P + kPrefetchLead → 备源 (不占 slot) → pf_close → pf_mark_
//                       软源 (Lob: 暖逐笔 page cache, TS 不等它) / 硬源 (Bar: 解码 bar1m 进源环, TS 等 pf_ready)
//   TS: ts_try_open(d)  d < io_done_ + P → 句柄写 (纯指针算术) → 每 (asset, d) 一次 ts_close, 计满全轴 → ts_done_ + 1
//   CS: cs_try_open(d)  本日计满全轴 → 逐拍扫 (cs_prev: 跨日只读 d−1 槽位 TS 列) → cs_close → slot.cs_closed
//   IO: io_try_open(d)  d < io_done_ + P ∧ slot.cs_closed ∧ (末日 ∨ CS(d+1) 已 close) → 落盘 (FeatureDay::write, 池不碰格式)
//                       → io_close (清零) → slot.io_mark → 推进 io_done_
//
// TS 按 (asset, d) 计数 (而非按 worker) 使资产处置权可在 worker 间转移: 领跑 worker 可接手
// 落后 worker 的资产并回填旧日 (旧日未计满 ⇒ 仍在飞, ts_try_open 必得), 见 TsSchedule.
// 资产内日序由调度侧的 claim/done 原子保证, 因此 "d+1 日计满 ⇒ d 日已计满" —— ts_done_ 只增.
// 开盘型标签 (T+N) 跨日回填: 旧日 ts_close 推迟到标签结清 (见 CoreTs), 故 P ≥ LabelHold::MIN_RING_SLOTS.
//
// 句柄在有效期内被握手钉住 (TS 未计满不给 CS, CS 未 close 不给 IO, IO 未 close 不复用),
// 不需要 epoch / 每次访问的验证. 数值一致性与调度形态无关, 由输入契约锚定
// (见 CoreLob.hpp / CoreCs.hpp), 按日流控只是回测里的因果保证.
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

// 日 d 的张量 (= ring_[d % P])
template <class T>
struct StoreSlot {
  std::atomic<uint32_t> assets_done{0};             // == num_assets ⇒ 本日 TS 全部写完 (CS 放行)
  std::atomic<bool> cs_closed{false};               // 本日截面写完 (IO 放行)
  std::atomic<size_t> io_mark{0};                   // 本格最近落盘日 d + 1 (0 = 未落过; reset 不动它)
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
    cs_closed.store(false, std::memory_order_relaxed);
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
  const size_t num_assets_; // A 维 = 档 A 轴大小 (张量/落盘/流控全按它)
  size_t ring_size_;        // P

  // 各段已完成天数 (单调): 流水线的全部握手面, 进度面也只读它们
  std::atomic<size_t> pf_done_{0}; // 已备源日数 (fetch_add, 见 pf_close; 预取多核时非连续前缀, 只供仪表盘)
  std::atomic<size_t> ts_done_{0}; // 计满全轴的日数 (fetch_add, 见 ts_close)
  std::atomic<size_t> cs_done_{0}; // 已截面日数 (fetch_add, 见 cs_close; 截面多核时非连续前缀, 只供仪表盘)
  std::atomic<size_t> io_done_{0}; // 已落盘的连续前缀 (CAS 推进, 见 io_close)

  // 源环格日标记 [feed_ring_size()]: 格 d % R 最近备好的日 d + 1 (0 = 未备过). 环长随 ring_size_ 定, 子类收定 P 后 init_pf_marks
  std::vector<std::atomic<size_t>> pf_mark_;

  StoreRing(size_t num_assets, size_t ring_slots) : num_assets_(num_assets), ring_size_(ring_slots) {}
  void init_pf_marks() { pf_mark_ = std::vector<std::atomic<size_t>>(feed_ring_size()); }

public:
  // ===== PF: 不占 slot, 只有流控与进度 =====

  // 窗口 = TS 在飞跨度上限 P + 余量. 日 d 进窗 ⇒ d − (P + kPrefetchLead + 1) 已全员计满 (源环可复用其格, 见 feed_ring_size)
  bool pf_try_open(size_t d) const { return d <= ts_done_.load(std::memory_order_relaxed) + ring_size_ + kPrefetchLead; }

  // 硬源打源环格日标记: release 与 pf_ready 的 acquire 配对 (解码写入先于 TS 读).
  // 软源不打: TS 不等预取, 慢核的旧日可能晚于 d + R 才 close, 标记会倒退 (也没有读者)
  void pf_close(size_t d, bool hard) {
    if (hard) {
      const size_t R = pf_mark_.size();
      assert(R == feed_ring_size() && "pf_close: 源环标记未初始化");
      std::atomic<size_t> &mark = pf_mark_[d % R];
      assert(mark.load(std::memory_order_relaxed) == (d >= R ? d + 1 - R : 0) && "pf_close: 源环格上一日不是 d − R (日序乱)");
      mark.store(d + 1, std::memory_order_release);
    }
    pf_done_.fetch_add(1, std::memory_order_relaxed);
  }
  // 格 d % R 在 d 全员计满前不会被 d + R 复用 (pf_try_open 窗口), 标记相等即日 d 已备好
  bool pf_ready(size_t d) const { return pf_mark_[d % pf_mark_.size()].load(std::memory_order_acquire) == d + 1; }

  // 源环格数: 预取窗口跨度 [ts_done_, ts_done_ + P + kPrefetchLead] 全部在格
  size_t feed_ring_size() const { return ring_size_ + kPrefetchLead + 1; }

  // ===== QUERY API =====
  size_t query_A() const { return num_assets_; }
  size_t query_slots() const { return ring_size_; }
  size_t query_pf_done() const { return pf_done_.load(std::memory_order_relaxed); }
  size_t query_ts_done() const { return ts_done_.load(std::memory_order_relaxed); } // 计满全轴的日数 (= 最慢 TS 前沿)
  size_t query_cs_done() const { return cs_done_.load(std::memory_order_relaxed); } // 已截面日数 (多核时非连续前缀)
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
  const std::uint64_t axis_hash_; // A 轴指纹 (LibAxis::hash), 落进每个特征文件头
  std::string output_dir_;

  // 日环: 日 d 占 ring_[d % P]. 不需要 date→slot map / freelist / epoch / 状态枚举 (见文件头)
  Slot *ring_ = nullptr;

  // 日 d 的句柄. 前置: d 在飞 (io_done_ ≤ d < io_done_ + P), 由各 try_open 的流控保证
  Day day_at(size_t d) { return {&ring_[d % ring_size_], num_assets_, d}; }

public:
  // num_assets: 档 A 轴大小; axis_hash: LibAxis::hash, 写进每个
  // 特征文件头锁定"列 → 资产"映射 (读端从同一 FeatureLib 推导同一轴比对)
  FeatureStore(size_t num_assets,
               std::uint64_t axis_hash,
               const std::string &output_dir = "",
               size_t ring_slots = kDefaultRingSlots)
      : StoreRing(num_assets, ring_slots),
        axis_hash_(axis_hash) {
    assert(ring_size_ >= 2 && "Ring size too small, need at least 2 slots");
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

    // 限制 ring_size 不超过 40% 当前可用内存 (总内存会高估: HugePages 预留 /
    // 其他进程占用对普通分配不可用, 按总内存分池会被 OOM killer SIGKILL).
    // 留 60% 余量给: 每资产常驻的订单簿容量 (见 L2::LOB_ORDER_CAPACITY,
    // 随真实逐笔数据写入逐步坐实成 RSS, 整个进程生命周期不释放) + 其他进程.
    constexpr double kRingMemFraction = 0.4;
    const size_t avail_mem = get_available_memory_bytes();
    const size_t max_ring_bytes = static_cast<size_t>(avail_mem * kRingMemFraction);
    const size_t max_ring_slots = max_ring_bytes / bytes_per_day;
    if (ring_size_ > max_ring_slots) {
      printf("\n[FeatureStore] Ring size limited: %zu -> %zu (%.0f%% of %.1f GB available, %.1f GB DDR)\n",
             ring_size_, max_ring_slots, kRingMemFraction * 100.0,
             avail_mem / (1024.0 * 1024.0 * 1024.0),
             get_system_memory_bytes() / (1024.0 * 1024.0 * 1024.0));
      ring_size_ = max_ring_slots;
    }
    assert(ring_size_ >= 2 && "Ring size too small, need at least 2 slots");
    init_pf_marks();

    std::cout << "\n=== Feature Store Tensor Pool (" << tier_name(T::kId) << ") ===\n";

    // Detailed dimension breakdown
    std::cout << "Dimension Details:\n";
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      if (!T::has(lvl))
        continue;
      const auto &L = T::LEVELS[lvl];
      printf("  %-6s [T=%6zu][F=%4zu][A=%4zu] = %.2f MB\n", L.level_name, L.rows, L.field_count, num_assets, level_bytes(lvl, num_assets) / (1024.0 * 1024.0));
    }

    printf("\nPer-day tensor: %.2f MB | Ring size: %zu slots | Total: %.2f GB\n",
           bytes_per_day / (1024.0 * 1024.0),
           ring_size_,
           (bytes_per_day * ring_size_) / (1024.0 * 1024.0 * 1024.0));

    // Allocate pool (Slot array)
    ring_ = new Slot[ring_size_];

    std::cout << "Allocating physical pages...\n";
    auto start = std::chrono::steady_clock::now();

    // Parallel allocation: 一槽一任务
    std::atomic<size_t> progress_counter{0};
    misc::exec::ForkJoin ex(misc::exec::fit(ring_size_));
    ex.tasks(static_cast<int>(ring_size_), [&](int i, int) {
      ring_[i].allocate(num_assets_);
      ring_[i].reset(num_assets_); // memset 清零 = 坐实物理页 + 空闲态不变量: 拿来即写

      const size_t completed = ++progress_counter;
      std::cout << "  " << completed << "/" << ring_size_ << " slots\r" << std::flush;
    });

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    std::cout << "\nPhysical allocation complete in " << elapsed << " ms\n";
    std::cout << "=================================\n\n";
  }

  ~FeatureStore() {
    if (ring_) {
      delete[] ring_; // Slot destructors called automatically
    }
  }

  // ===== TS: open 按 d, close 按 (asset, d) =====

  // 背压: 日 d 的 slot 须已由 d − P 落盘交还. 回填旧日也走这里 (旧日未计满 ⇒ 仍在飞, 必得).
  // slot 在 io_close 已清零, 拿来即写. 空句柄 = 池满 —— 调用方拿等待时间去干别的 (领跑者在池边领养回填).
  // acquire 与 io_close 的 release 配对 (清零先于复用).
  Day ts_try_open(size_t d) {
    const size_t io = io_done_.load(std::memory_order_acquire);
    assert(d >= io && "ts_try_open: 日 d 已落盘 (调度侧日序乱)");
    return d < io + ring_size_ ? day_at(d) : Day{};
  }

  // 一个 (asset, d) 写完. 计数攒齐全轴 (库轴) 后 CS 放行;
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

  // 按日流控: 本日计满全轴才放行, 返回后两层张量整体可读 —— CS 内部不需要任何行级等待.
  // 先验 slot 已轮到日 d (d < io_done_ + P): 否则 ring_[d % P] 还是已计满待落盘的 d − P, 计数会误放行.
  // 截面多核各开各的日 (d % K), 互不握手
  Day cs_try_open(size_t d) {
    const size_t io = io_done_.load(std::memory_order_acquire);
    assert(d >= io && "cs_try_open: 日 d 已落盘 (日序乱)");
    if (d >= io + ring_size_)
      return {};
    const Day day = day_at(d);
    if (day.slot->assets_done.load(std::memory_order_acquire) != num_assets_)
      return {};
    assert(!day.slot->cs_closed.load(std::memory_order_relaxed) && "cs_try_open: 本日已截面 (日序乱)");
    return day;
  }

  // 截面跨日读 (粗层 as-of 落到前一交易日): 日 d−1 句柄, 只读 TS / 元列 (d−1 的截面 / 标签可能还在写).
  // 槽位由 io_try_open(d−1) 等本日 cs_close 钉住. d = 0 → 空
  Day cs_prev(const Day &day) {
    assert(day);
    if (day.d == 0)
      return {};
    assert(day.d - 1 >= io_done_.load(std::memory_order_acquire) && "cs_prev: d−1 已落盘 (io_try_open 握手失效)");
    return day_at(day.d - 1);
  }

  // CS 写完本日全部截面列, 交给 IO. release 与 io_try_open 的 acquire 配对
  void cs_close(const Day &day) {
    assert(day && !day.slot->cs_closed.load(std::memory_order_relaxed) && "cs_close: 重复 close");
    day.slot->cs_closed.store(true, std::memory_order_release);
    cs_done_.fetch_add(1, std::memory_order_relaxed);
    Logger::log("store", "cs_close: d=" + std::to_string(day.d));
  }

  // ===== IO: 每 d 一对 open/close =====

  // 截面已 close 的日才放行 (逐 slot 查: 截面多核 close 乱序); 返回后本日两层张量只归 IO 读 (落盘见 FeatureDay::write).
  // 落盘多核按日轮转: 先验 slot 已轮到日 d (d < io_done_ + P), 否则 ring_[d % P] 还是已截面待落盘的 d − P, cs_closed 会误放行.
  // 非末日 (last = d 是日期轴末日) 还要 CS(d+1) 已 close: 它经 cs_prev 读本 slot. d+1 的 slot 同样先验已轮到;
  // CS(d+1) close 后 IO(d+1) 可能抢先落盘清零 (cs_closed 复位) → 再认 io_mark == d+2 (粘滞)
  Day io_try_open(size_t d, bool last) {
    const size_t io = io_done_.load(std::memory_order_acquire);
    assert(d >= io && "io_try_open: 日 d 已落盘 (日序乱)");
    if (d >= io + ring_size_)
      return {};
    const Day day = day_at(d);
    if (!day.slot->cs_closed.load(std::memory_order_acquire))
      return {};
    if (last)
      return day;
    if (d + 1 >= io + ring_size_)
      return {};
    const Slot &next = ring_[(d + 1) % ring_size_];
    return next.cs_closed.load(std::memory_order_acquire) || next.io_mark.load(std::memory_order_seq_cst) == d + 2 ? day : Day{};
  }

  // 本日落盘完毕: 清零后打日标记 (空闲态不变量: 张量 + 计数全零, 拿来即写), 再把 io_done_ 推到首个未落盘日.
  // 标记写与 io_done_ 读 / CAS 与标记读构成 Dekker 式交叉, 须 seq_cst: 否则两核各自 close 后都看不到对方, 前缀卡住.
  // io_done_ 的推进 (seq_cst ⊇ release) 与 ts_try_open / cs_try_open 的 acquire 配对 (清零先于复用)
  void io_close(const Day &day) {
    assert(day && day.d >= io_done_.load(std::memory_order_relaxed) && "io_close: 日 d 已落盘 (日序乱)");
    assert(day.slot->io_mark.load(std::memory_order_relaxed) == (day.d >= ring_size_ ? day.d + 1 - ring_size_ : 0) &&
           "io_close: slot 上一日不是 d − P (日序乱)");
    day.slot->reset(num_assets_);
    day.slot->io_mark.store(day.d + 1, std::memory_order_seq_cst);
    size_t io = io_done_.load(std::memory_order_seq_cst);
    while (ring_[io % ring_size_].io_mark.load(std::memory_order_seq_cst) == io + 1)
      if (io_done_.compare_exchange_weak(io, io + 1, std::memory_order_seq_cst))
        ++io;
    Logger::log("store", "io_close: d=" + std::to_string(day.d));
  }

  // ===== QUERY API =====
  const std::string &query_dir() const { return output_dir_; } // 特征库目录 (日文件落在其下)
  std::uint64_t query_axis_hash() const { return axis_hash_; } // A 轴指纹 (落进每个日文件头)
};

using LobFeatureStore = FeatureStore<lob::Tier>;
using BarFeatureStore = FeatureStore<bar::Tier>;

// ============================================================================
// 写回 / 截面读写 API (fstore 命名空间): 层是模板参数 (FeatureLevel), 档从日句柄推导,
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
