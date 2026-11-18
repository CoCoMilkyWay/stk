#pragma once

#include "FeatureDay.hpp" // 续算尾窗读回 (seed)
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureTier.hpp"
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
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <vector>

#if defined(__linux__)
#include <sys/mman.h> // madvise(MADV_HUGEPAGE): 日环上透明大页
#endif

// ============================================================================
// FEATURE STORE - 日环 (日流水线的唯一握手面)
//
// 流控按日: TS worker 资产串行 (一个资产写完一整天才换下一个), 且 label / L_1min 分钟行
// 向过去回填, 行级进度天然罩不住 —— 所以流水线单位是日, TS 计数粒度是 asset-day.
//
// 定长无锁序号环: 张量环 slots_[P], 日 d 恒占 slots_[d % P] (环深 P 启动时按内存预算一次定死,
// 无映射表 / 无扩格); 源格环 feed cells_[F], F = P + kPrefetchLead + 1, 日 d 恒占 cells_[d % F].
// 每格只留单调日序号 (单写者, release/acquire 配对), 四段握手逐格同构:
//   格序号:  free_day (本格下一个可服务的日) / ts_day (本格最近计满全轴的日) / cs_day (本格最近截面完的日)
//   PF(d):   等 slots_[(d−F)%P].ts_day ≥ d−F (源格前主已计满, 再无读者) → 备源 → pf_day_ = d (只硬源)
//   TS(d):   等 free_day == d (硬源另等 pf_day_ == d) → 全轴各资产 ts_close 计数 → 攒满置 ts_day = d
//   CS(d):   等 ts_day == d → 逐拍扫 (cs_prev: 跨日只读 d−1 槽位 TS 列) → 置 cs_day = d
//   IO(d):   等 cs_day == d ∧ (末日 ∨ slots_[(d+1)%P].cs_day ≥ d+1) → 落盘清零 → 置 free_day = d + P
//   (序号单调: IO(d+1) 先落盘也满足 cs_day ≥ d+1; free_day 一格钉死复用点)
//
// io_done = 各格未决日的连续前缀 (min, io_close 里重扫 P 格, 仪表盘/调度观测用);
// pf_done_ / cs_done_ = 单调计数 (仪表盘). 等待 / 取消是 worker 骨架的事 (Stages.hpp wait_open), try_open 非阻塞.
//
// 软源 (Lob: 暖逐笔 page cache, TS 不等它) / 硬源 (Bar: 解码 bar1m 进源格, TS 等 pf_ready).
// 续算: 日期轴前 tail 日 = 库末 tail 日 (尾窗, 标签可能仍悬挂), 启动前 seed 从盘整层读回各自的 slot (IO 的逆);
// 之后四段照常握手 —— 预取 / 截面对尾窗日只 close 不干活, TS 由 CoreTs::resume 接回句柄, IO 照常重落盘.
//
// TS 按 (asset, d) 计数 (而非按 worker) 使资产处置权可在 worker 间转移: 领跑 worker 可接手
// 落后 worker 的资产并回填旧日 (旧日未计满 ⇒ 其格 free_day 仍停在 d, ts_try_open 必得), 见 TsSchedule.
// 资产内日序由调度侧的 claim/done 原子保证.
// 开盘型标签 (T+N) 跨日回填: 旧日 ts_close 推迟到标签结清 (见 CoreTs), 故 P ≥ LabelHold::MIN_RING_SLOTS (或 P = 日期轴天数, 格不复用).
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
// 档 T (lob::Tier / bar::Tier, 见 FeatureTier) 是模板参数: 布局常量取 T::LEVELS, 档缺的层不分配
// (data[lvl] == nullptr). Slot / Day 独立成模板, fstore:: 的写读 API 从句柄推导档.
// ============================================================================

// 某档某层一天张量字节数 [T][F][A] (档缺的层 F = 0 → 0)
template <class T>
constexpr size_t store_level_bytes(size_t lvl, size_t num_assets) {
  return T::LEVELS[lvl].rows * T::LEVELS[lvl].field_count * num_assets * sizeof(feature_storage_t);
}

// 日 d 的张量 (定占 slots_[d % P]): 三个单调日序号 (单写者) + 资产计数; reset 不动序号
template <class T>
struct StoreSlot {
  std::atomic<size_t> free_day{0};                  // 本格下一个可服务的日 (init = 格号; io_close 置 d + P)
  std::atomic<size_t> ts_day{SIZE_MAX};             // 本格最近计满全轴的日
  std::atomic<size_t> cs_day{SIZE_MAX};             // 本格最近截面写完的日
  std::atomic<uint32_t> assets_done{0};             // 本日已写完的资产数 (== num_assets ⇒ 置 ts_day)
  feature_storage_t *data[LEVEL_COUNT] = {nullptr}; // 每层 [T][F][A]; 档缺的层 nullptr

  // 张量按 2MB 对齐并 madvise(MADV_HUGEPAGE): 透明大页是普通匿名内存,
  // 进程退出/崩溃由内核自动回收, 不需要 nr_hugepages 静态预留 (那种预留
  // 会从 MemAvailable 里整块扣掉, 谁都用不了). 需要系统
  // /sys/kernel/mm/transparent_hugepage/enabled = madvise 或 always.
  // 收益: TS 写行是列间 stride (F 列 × A×2B ≈ 11.8KB), 4K 页下每 tick
  // 踩 F 个不同页, dTLB 压力大; 2M 页覆盖整列邻域, miss 显著减少.
  static constexpr size_t kHugePage = 2ull * 1024 * 1024;
  // aligned_alloc 要求 size 是 alignment 的整数倍; 每层最多多占 <2MB
  static size_t level_alloc_bytes(size_t lvl, size_t num_assets) {
    return (store_level_bytes<T>(lvl, num_assets) + kHugePage - 1) / kHugePage * kHugePage;
  }
  // 一格实占字节 (内存预算按它)
  static size_t bytes(size_t num_assets) {
    size_t s = 0;
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
      if (T::has(lvl))
        s += level_alloc_bytes(lvl, num_assets);
    return s;
  }

  void allocate(size_t num_assets) {
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      if (!T::has(lvl))
        continue;
      const size_t aligned_bytes = level_alloc_bytes(lvl, num_assets);
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
// 热路径没有字符串比较 / 原子验证 / slot 查找.
// 空句柄 = 没拿到 (日环满 / 本日未就绪), try_open 以此表示失败.
template <class T>
struct StoreDay {
  StoreSlot<T> *slot = nullptr;
  size_t A = 0;
  size_t d = 0; // 档日期轴下标 (StageCtx::dates), close 推进已完成天数用
  explicit operator bool() const { return slot != nullptr; }
};

// ============================================================================
// StoreRing —— 定长序号环的档无关握手面 (仪表盘只读它, 不随档实例化):
//   张量环 P 格 (FeatureStore<T> 持): 日 d 恒占格 d % P
//   源格环 F = P + kPrefetchLead + 1 (TierFeed 持 cells, 本类持各格日标记): 日 d 恒占格 d % F
// 各格单调日序号, 单写者 release / 读者 acquire; 四段逐格握手见文件头.
// ============================================================================
class StoreRing {
public:
  // 预取领先 TS 的余量 (天): 源格数 = 张量环深 + kPrefetchLead + 1, 保证最快 TS 到达前源已备好
  static constexpr size_t kPrefetchLead = 2;
  static constexpr size_t feed_cells(size_t slots) { return slots + kPrefetchLead + 1; }
  static constexpr size_t kNone = SIZE_MAX; // 序号空值 (未备源 / 未计满 / 未截面)

  StoreRing(size_t num_assets, size_t slots)
      : num_assets_(num_assets), P_(slots), F_(feed_cells(slots)), pf_day_(F_) {
    for (std::atomic<size_t> &m : pf_day_)
      m.store(kNone, std::memory_order_relaxed);
  }
  virtual ~StoreRing() = default;

  const size_t num_assets_;                     // A 维 = 档 A 轴大小 (张量/落盘/流控全按它)
  const size_t P_;                              // 张量环深 (启动一次定死)
  const size_t F_;                              // 源格数
  std::vector<std::atomic<size_t>> pf_day_;     // [F] 各源格最近备好的日 (kNone = 未备过; 单写者 = 日 d % F 归第 d % K_pf 个预取核)
  std::atomic<size_t> pf_done_{0}, cs_done_{0}; // 已备源 / 已截面日数 (多核非连续, 只供仪表盘)
  std::atomic<size_t> io_done_{0};              // 落盘的连续前缀 (io_close 里重扫各格未决日的 min)

  // ===== PF: 不占张量格, 只有流控 / 源格标记 / 进度 =====

  // 日 d 的源格号: 预取在 pf_try_open 之后, 时序在 pf_ready 之后取 (硬源)
  size_t feed_cell(size_t d) const { return d % F_; }
  size_t feed_size() const { return F_; }

  // 硬源打源格日标记: release 与 pf_ready 的 acquire 配对 (解码写入先于 TS 读). 软源不打 (TS 不等预取)
  void pf_close(size_t d, bool hard) {
    if (hard) {
      std::atomic<size_t> &mark = pf_day_[feed_cell(d)];
      const size_t prev = mark.load(std::memory_order_relaxed);
      assert((prev == kNone || prev < d) && "pf_close: 源格已备过日 d 或之后 (日序乱)");
      mark.store(d, std::memory_order_release);
    }
    pf_done_.fetch_add(1, std::memory_order_relaxed);
  }
  // 日 d 的源格备好? (标记相等才认: 读到格的前主标记自然不符)
  bool pf_ready(size_t d) const { return pf_day_[feed_cell(d)].load(std::memory_order_acquire) == d; }

  // ===== QUERY API (仪表盘) =====
  size_t query_A() const { return num_assets_; }
  size_t query_slots() const { return P_; }
  size_t query_pf_done() const { return pf_done_.load(std::memory_order_relaxed); }
  size_t query_cs_done() const { return cs_done_.load(std::memory_order_relaxed); }
  size_t query_io_done() const { return io_done_.load(std::memory_order_relaxed); }
};

// ============================================================================
// FeatureStore<T> —— 档的定长日环: 张量格 slots_[P] + 段 API (open/close 逐格序号)
// ============================================================================
template <class T>
class FeatureStore : public StoreRing {
public:
  using Tier = T;
  using Slot = StoreSlot<T>;
  using Day = StoreDay<T>;

private:
  static constexpr size_t level_bytes(size_t lvl, size_t num_assets) { return store_level_bytes<T>(lvl, num_assets); }

  const std::uint64_t axis_hash_; // A 轴指纹 (PoolAxis::hash), 落进每个特征文件头
  const LibId lib_;

  // 张量环 [P] 定长: 日 d 恒占 slots_[d % P], 格序号见 StoreSlot
  std::vector<std::unique_ptr<Slot>> slots_;

  Slot &slot(size_t d) const { return *slots_[d % P_]; }
  Day day_at(size_t d) const { return {&slot(d), num_assets_, d}; }

  static std::unique_ptr<Slot> make_slot(size_t num_assets) {
    auto s = std::make_unique<Slot>();
    s->allocate(num_assets);
    s->reset(num_assets); // memset 清零 = 坐实物理页 + 空闲态不变量: 拿来即写
    return s;
  }

  // 连续前缀 = 各格未落盘日的 min: 格 i 的 ≡ i (mod P) 且 < free_day 的服务日全部已落盘,
  // 其首个未落盘日 = free_day (free_day − P 是它的最近已落盘日). 前缀只进不退
  // (CAS 取 max; close 可跨格乱序, 扫到中间态只是偏旧, 下一轮补齐)
  static void bump(std::atomic<size_t> &out, size_t v) {
    size_t cur = out.load(std::memory_order_relaxed);
    while (v > cur && !out.compare_exchange_weak(cur, v, std::memory_order_relaxed, std::memory_order_relaxed))
      ;
  }
  void refresh_io_done() {
    size_t m = SIZE_MAX;
    for (size_t i = 0; i < P_; ++i)
      m = std::min(m, slots_[i]->free_day.load(std::memory_order_relaxed));
    bump(io_done_, m);
  }

public:
  // num_assets: 档 A 轴大小; axis_hash: PoolAxis::hash, 写进每个
  // 特征文件头锁定"列 → 资产"映射 (读端从同一 FeatureLib 推导同一轴比对).
  // slots = 环深 P (启动时按内存预算一次定死, 见 worker/compute/Pipeline.hpp)
  FeatureStore(size_t num_assets, std::uint64_t axis_hash, LibId lib, size_t slots)
      : StoreRing(num_assets, slots),
        axis_hash_(axis_hash), lib_(lib), slots_(slots) {
    assert(num_assets_ >= 1 && "A 轴为空");
    // 只建目录, 不清库: 清库/重算是调用方的显式决定 (回测全量重算见
    // ComputeService), 构造副作用会让实盘复用 store 时误删历史特征.
    disk::ensure(disk::Lib, lib_key(lib_));

    const size_t bytes_per_day = Slot::bytes(num_assets);
    std::cout << "\n=== Feature Store Tensor Pool (" << tier_name(T::kId) << ") ===\n";

    // Detailed dimension breakdown
    std::cout << "Dimension Details:\n";
    int name_w = 0;
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
      if (T::has(lvl))
        name_w = std::max(name_w, static_cast<int>(std::strlen(T::LEVELS[lvl].level_name)));
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      if (!T::has(lvl))
        continue;
      const auto &L = T::LEVELS[lvl];
      printf("  %-*s [T=%4zu][F=%4zu][A=%5zu] = %7.2f MB\n", name_w, L.level_name, L.rows, L.field_count, num_assets, level_bytes(lvl, num_assets) / (1024.0 * 1024.0));
    }

    printf("\nPer-day tensor: %.2f MB | %zu slots = %.2f GB\n",
           bytes_per_day / (1024.0 * 1024.0), slots, (bytes_per_day * slots) / (1024.0 * 1024.0 * 1024.0));

    std::cout << "Allocating physical pages...\n";
    auto start = std::chrono::steady_clock::now();

    // Parallel allocation: 一槽一任务
    std::atomic<size_t> progress_counter{0};
    misc::exec::ForkJoin ex(misc::exec::fit(slots));
    ex.tasks(static_cast<int>(slots), [&](int i, int) {
      slots_[static_cast<size_t>(i)] = make_slot(num_assets_);
      slots_[static_cast<size_t>(i)]->free_day.store(static_cast<size_t>(i), std::memory_order_relaxed); // 格 i 先服务日 i
      const size_t completed = ++progress_counter;
      std::cout << "  " << completed << "/" << slots << " slots\r" << std::flush;
    });

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    std::cout << "\nPhysical allocation complete in " << elapsed << " ms\n";
    std::cout << "=================================\n\n";
  }

  // ===== 续算: 尾窗日读回 =====

  // 管线启动前调: 日 d = 0..n−1 的 slot (空闲态) ← 库里 dates[d] 的日文件, 逐层整层读回; 一日一任务.
  // 尾窗日之后照常 ts_close / cs_close / io (见文件头), 读不回 = 库被改过 (清单与日文件不符) → 断言
  void seed(std::span<const misc::Ymd> dates) {
    assert(!dates.empty() && dates.size() < P_ && "seed: 只在管线启动前填尾窗");
    misc::exec::ForkJoin ex(misc::exec::fit(dates.size()));
    ex.tasks(static_cast<int>(dates.size()), [&](int d, int) {
      FeatureDay::Scratch s;
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        if (!T::has(lvl))
          continue;
        const FeatureDay::Status st = FeatureDay::read_layer(lib_day_file(lib_, dates[static_cast<size_t>(d)], lvl), T::LEVELS[lvl], slot(static_cast<size_t>(d)).data[lvl], num_assets_, axis_hash_, s);
        assert(st == FeatureDay::Status::Ok && "续算: 尾窗日文件读不回");
        (void)st;
      }
    });
  }

  // ===== PF: 源格复用流控 =====

  // 日 d 的源格可用? 格的前主日 d − F 已计满 (其 TS 读者散尽). 前 F 日的格天然空闲.
  // kNone = 前主格从未计满过 (首轮 d − F < P 且未算完), 不得复用.
  bool pf_try_open(size_t d) const {
    if (d < F_)
      return true;
    const size_t t = slot(d - F_).ts_day.load(std::memory_order_acquire);
    return t != kNone && t >= d - F_;
  }

  // ===== TS: open 按 d, close 按 (asset, d) =====

  // 背压: free_day == d ⇒ 本格前主 (日 d − P) 已落盘清零, 拿来即写. 回填旧日也走这里
  // (旧日未计满 ⇒ 其格 free_day 还停在 d, 必得). 空句柄 = 日环边 —— 调用方拿等待时间去干别的
  // (领跑者在日环边领养回填). acquire 与 io_close 的 release 配对 (清零先于复用).
  Day ts_try_open(size_t d) {
    Slot &s = slot(d);
    const size_t f = s.free_day.load(std::memory_order_acquire);
    assert(f <= d && "ts_try_open: 日 d 已落盘 (调度侧日序乱)");
    return f == d ? Day{&s, num_assets_, d} : Day{};
  }

  // 一个 (asset, d) 写完. 计数攒齐全轴 (池轴) 后置 ts_day = d: 放行 CS, 并让源格 d % F
  // 可交给日 d + F (源格读完先于复用, 见 pf_try_open); release 与两侧 acquire 配对.
  // 缺 binary 的 asset-day 也要计 (张量保持默认值), 由当时的处置权持有者计.
  void ts_close(const Day &day) {
    assert(day);
    const uint32_t prev = day.slot->assets_done.fetch_add(1, std::memory_order_release);
    assert(prev < num_assets_ && "ts_close: 计数超过派活资产数 (重复计?)");
    if (prev + 1 == num_assets_) {
      day.slot->ts_day.store(day.d, std::memory_order_release);
      Logger::log("store", "ts_day_done: d=" + std::to_string(day.d));
    }
  }

  // ===== CS: 每 d 一对 open/close =====

  // 按日流控: 本日计满全轴 (ts_day == d) 才放行, 返回后本档各层张量整体可读 —— CS 内部不需要任何行级等待.
  // 截面多核各开各的日 (d % K), 互不握手
  Day cs_try_open(size_t d) {
    Slot &s = slot(d);
    const size_t t = s.ts_day.load(std::memory_order_acquire);
    assert((t == kNone || t <= d) && "cs_try_open: 日 d 之后已计满 (日序乱)");
    if (t != d)
      return {};
    assert(s.cs_day.load(std::memory_order_relaxed) != d && "cs_try_open: 本日已截面 (日序乱)");
    return Day{&s, num_assets_, d};
  }

  // 截面跨日读 (粗层 as-of 落到前一交易日): 日 d−1 句柄, 只读 TS / 元列 (d−1 的截面 / 标签可能还在写).
  // 槽位由 io_try_open(d−1) 等本日 cs_close 钉住 (d−1 未落盘 ⇒ free_day 未过 d−1). d = 0 → 空
  Day cs_prev(const Day &day) {
    assert(day);
    if (day.d == 0)
      return {};
    assert(slot(day.d - 1).free_day.load(std::memory_order_acquire) == day.d - 1 && "cs_prev: d−1 已落盘 (io_try_open 握手失效)");
    return day_at(day.d - 1);
  }

  // CS 写完本日全部截面列, 交给 IO. release 与 io_try_open 的 acquire 配对
  void cs_close(const Day &day) {
    assert(day && day.slot->cs_day.load(std::memory_order_relaxed) != day.d && "cs_close: 重复 close");
    day.slot->cs_day.store(day.d, std::memory_order_release);
    cs_done_.fetch_add(1, std::memory_order_relaxed);
    Logger::log("store", "cs_close: d=" + std::to_string(day.d));
  }

  // ===== IO: 每 d 一对 open/close =====

  // 截面已 close 的日才放行 (逐格序号查: 截面多核 close 乱序); 返回后本日各层张量只归 IO 读 (落盘见 FeatureDay::write).
  // 非末日 (last = d 是日期轴末日) 还要 CS(d+1) 已 close: 它经 cs_prev 读本 slot ——
  // kNone = 次日格还没截面过 (首轮新格), 不得放行; 序号单调: IO(d+1) 先落盘时其格 cs_day ≥ d+1, 一并认.
  Day io_try_open(size_t d, bool last) {
    Slot &s = slot(d);
    assert(s.free_day.load(std::memory_order_acquire) == d && "io_try_open: 日 d 未在飞 (日序乱)");
    if (s.cs_day.load(std::memory_order_relaxed) != d)
      return {};
    const size_t next = slot(d + 1).cs_day.load(std::memory_order_acquire);
    if (last || (next != kNone && next >= d + 1))
      return Day{&s, num_assets_, d};
    return {};
  }

  // 本日落盘完毕: 清零后交还格 (空闲态不变量: 张量 + 计数全零, 拿来即写), 置 free_day = d + P.
  // release 与 ts_try_open 的 acquire 配对 (清零先于复用); 本格下一个服务日固定是 d + P.
  void io_close(const Day &day) {
    assert(day && day.slot->free_day.load(std::memory_order_relaxed) == day.d && "io_close: 日序乱");
    day.slot->reset(num_assets_);
    day.slot->free_day.store(day.d + P_, std::memory_order_release);
    refresh_io_done();
    Logger::log("store", "io_close: d=" + std::to_string(day.d));
  }

  // ===== QUERY API =====
  LibId query_lib() const { return lib_; }                     // 特征库 (日文件落在其目录下)
  std::uint64_t query_axis_hash() const { return axis_hash_; } // A 轴指纹 (落进每个日文件头)
};

// ============================================================================
// 写回 / 截面读写 API (fstore 命名空间): 层是模板参数 (FeatureLevel), 档从日句柄推导,
// 字段用 <tier>::<LVL>_Field 枚举; 布局常量一律取 constexpr T::LEVELS[LVL] (单一事实源),
// 常量下标下定址照样编译期折叠. 数据面走日句柄 (Day, 由 ts_try_open / cs_try_open
// 定址一次), 每次调用只剩指针算术.
//   ts_write<L>(day, t, field, a, value)   单资产单列 (TS: 列间跨步 A)
//   ts_write_row<L>(day, t, a, dag)        按字段表 SRC 列写一行全部 TS 列
//   cs_col<L>(day, t, field)               单列全部资产的连续段 (CS 读源列 / 就地写目标列)
//   TS(node, port, Tf) → ts::Tf::apply(dag.node.out(dag.node.port)); CS/LB/META 不在 row 写
// ============================================================================
namespace fstore {

// 唯一真正需要逐档逐层宏展开的东西: 按字段表 SRC 列写一行全部 TS 列
template <class T, size_t LVL>
struct RowWriter;
#define STORE_ROW_WRITER(T, ns, LVL)                                                                                         \
  template <>                                                                                                                \
  struct RowWriter<ns::Tier, LVL> {                                                                                          \
    template <class DAG>                                                                                                     \
    [[gnu::always_inline]] static inline void write_row(feature_storage_t *row, size_t A, [[maybe_unused]] const DAG &dag) { \
      namespace FO = ns::LVL##_Field;                                                                                        \
      TIER_FIELDS(T, LVL)(STORE_ROW_ONE)                                                                                     \
    }                                                                                                                        \
  };

#define STORE_ROW_TS(code, node, port, tf) row[FO::code * A] = ts::tf::apply(dag.node.out(dag.node.port));
#define STORE_ROW_CS(code, ...)
#define STORE_ROW_LB(code, port)
#define STORE_ROW_META(code, port)
#define STORE_ROW_ONE(code, c1, c2, en, cn, desc, formula, src) SRC_DISPATCH(STORE_ROW, code, src)

TIER_LEVELS(STORE_ROW_WRITER)
#undef STORE_ROW_WRITER
#undef STORE_ROW_ONE
#undef STORE_ROW_META
#undef STORE_ROW_LB
#undef STORE_ROW_CS
#undef STORE_ROW_TS

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
