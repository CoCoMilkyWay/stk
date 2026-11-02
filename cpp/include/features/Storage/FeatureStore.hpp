#pragma once

#include "FeatureDay.hpp" // 续算尾窗读回 (seed)
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
#include <iostream>
#include <memory>
#include <mutex>
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
// 两个日窗 (DayWindow) 是段间的全部握手面 (Disruptor 式), 外加两个只供仪表盘的计数 (pf_done_ / cs_done_):
//   张量窗 ring_ [io_done, io_done + P): 在飞日各占一格张量; 背压: 日 d 可开 ⇔ d 在窗
//   源窗   feed_ [ts_done, ts_done + P + kPrefetchLead + 1): 预取的流控 + 硬源的源格
// io_done = 已落盘的连续前缀, ts_done = 计满全轴的日数; 日 → 格经窗口的映射表, 格由 io_close 清零后经张量窗推进交还
// —— 无状态枚举 / 无查找; 窗口推进每日一次 (锁内), 在飞日的格在有效期内不动, 运行中可扩格 (grow, 管线按需调).
// 预取 / 截面 / 落盘各 K 核按日轮转 (日 d 归第 d % K 核, 核内日序递增), close 可乱序 ⇒ 段间握手是逐格的:
//   预取 → 时序: 源格日标记 pf_mark_ = d + 1 (只硬源; pf_done_ 只是已备源日数, 仪表盘用)
//   截面 → 落盘: slot.cs_closed (cs_done_ 只是已截面日数, 仪表盘用)
//   落盘 → 时序/截面: slot.io_mark = d + 1, io_close 据此把张量窗推到首个未落盘日
//
// 各段 open / close 同构 (try_open 非阻塞; 等待 / 取消是 worker 骨架的事, 见 feature_workers.hpp):
//   PF: pf_try_open(d)  d 在源窗 → 备源 (不占 slot) → pf_close → pf_mark_
//                       软源 (Lob: 暖逐笔 page cache, TS 不等它) / 硬源 (Bar: 解码 bar1m 进源格, TS 等 pf_ready)
//   TS: ts_try_open(d)  d 在张量窗 → 句柄写 (纯指针算术) → 每 (asset, d) 一次 ts_close, 计满全轴 → 推进源窗
//   CS: cs_try_open(d)  本日计满全轴 → 逐拍扫 (cs_prev: 跨日只读 d−1 槽位 TS 列) → cs_close → slot.cs_closed
//   IO: io_try_open(d)  d 在张量窗 ∧ slot.cs_closed ∧ (末日 ∨ CS(d+1) 已 close) → 落盘 (FeatureDay::write, 池不碰格式)
//                       → io_close (清零) → slot.io_mark → 推进张量窗
//
// 续算: 日期轴前 tail 日 = 库末 tail 日 (尾窗, 标签可能仍悬挂), 启动前 seed 从盘整层读回各自的 slot (IO 的逆);
// 之后四段照常握手 —— 预取 / 截面对尾窗日只 close 不干活, TS 由 CoreTs::resume 接回句柄, IO 照常重落盘.
//
// TS 按 (asset, d) 计数 (而非按 worker) 使资产处置权可在 worker 间转移: 领跑 worker 可接手
// 落后 worker 的资产并回填旧日 (旧日未计满 ⇒ 仍在飞, ts_try_open 必得), 见 TsSchedule.
// 资产内日序由调度侧的 claim/done 原子保证, 因此 "d+1 日计满 ⇒ d 日已计满" —— 源窗只进.
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

// 日 d 的张量 (张量窗里日 d 所在的格)
template <class T>
struct StoreSlot {
  std::atomic<uint32_t> assets_done{0};             // == num_assets ⇒ 本日 TS 全部写完 (CS 放行)
  std::atomic<bool> cs_closed{false};               // 本日截面写完 (IO 放行)
  std::atomic<size_t> io_mark{0};                   // 本格最近落盘日 d + 1 (0 = 未落过; reset 不动它)
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
    cs_closed.store(false, std::memory_order_relaxed);
  }

  ~StoreSlot() {
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl)
      free(data[lvl]);
  }
};

// 日句柄 (TS/CS/IO 同一形态): open 时定址一次, 之后所有读写是纯指针算术 ——
// 热路径没有字符串比较 / 原子验证 / slot 查找.
// 空句柄 = 没拿到 (池满 / 本日未就绪), try_open 以此表示失败.
template <class T>
struct StoreDay {
  StoreSlot<T> *slot = nullptr;
  size_t A = 0;
  size_t d = 0; // 档日期轴下标 (StageCtx::dates), close 推进已完成天数用
  explicit operator bool() const { return slot != nullptr; }
};

// 日窗: 在飞日 [lo, lo + n) 各占一格, 日 d → 格号 map_[d % cap] (映射表: 扩格不重排在飞日; 格号 0..n−1 恒为现有格).
//   推进: done(lo) 为真即把 lo 推过去, 日 lo 的格交给新进窗的日 lo + n (乱序完结的日由后到者一并推进);
//   扩格: 新格 (号 n) 接给日 lo + n.
// 两者都在 mu_ 下 (每日一次 / 偶发); (lo, n) 打包成一个原子 release 发布, 读端 acquire 一次拿到自洽的窗口, 进窗日的映射随之可见
class DayWindow {
public:
  struct Span {
    size_t lo, n;
    size_t end() const { return lo + n; }
  };

  DayWindow(size_t n, size_t cap) : map_(cap) {
    assert(n >= 1 && n <= cap && cap <= kMaxCells && "日窗: 格数越界");
    for (size_t i = 0; i < n; ++i)
      map_[i].store(i, std::memory_order_relaxed);
    win_.store(pack(0, n), std::memory_order_release);
  }

  Span span() const { return unpack(win_.load(std::memory_order_acquire)); }
  size_t cap() const { return map_.size(); }
  // 前置: 调用方经 span 的 acquire (或与之同步的握手) 看到过 d 进窗
  size_t cell(size_t d) const { return map_[d % map_.size()].load(std::memory_order_relaxed); }

  template <class Done>
  void advance(Done &&done) {
    std::lock_guard lock(mu_);
    const Span s = unpack(win_.load(std::memory_order_relaxed));
    size_t lo = s.lo;
    for (; done(lo); ++lo)
      map_[(lo + s.n) % map_.size()].store(cell(lo), std::memory_order_relaxed);
    if (lo != s.lo)
      win_.store(pack(lo, s.n), std::memory_order_release);
  }

  void grow() {
    std::lock_guard lock(mu_);
    const Span s = unpack(win_.load(std::memory_order_relaxed));
    assert(s.n < map_.size() && "日窗: 已到格数上限");
    map_[s.end() % map_.size()].store(s.n, std::memory_order_relaxed);
    win_.store(pack(s.lo, s.n + 1), std::memory_order_release);
  }

private:
  static constexpr size_t kNBits = 16;
  static constexpr size_t kMaxCells = (size_t{1} << kNBits) - 1;
  static uint64_t pack(size_t lo, size_t n) { return (static_cast<uint64_t>(lo) << kNBits) | n; }
  static Span unpack(uint64_t w) { return {static_cast<size_t>(w >> kNBits), static_cast<size_t>(w & kMaxCells)}; }

  std::vector<std::atomic<size_t>> map_;
  std::atomic<uint64_t> win_{0};
  std::mutex mu_;
};

// 日环握手面 (档无关): 两个日窗 + 段计数 + 预取段 open / close. 仪表盘只读它, 不随档实例化
class StoreRing {
public:
  // 预取领先 TS 的余量 (天): 源窗 = 张量窗格数 P + 余量, 保证最快 TS 到达前源已就绪
  static constexpr size_t kPrefetchLead = 2;
  // 源窗格数: 预取窗口跨度 [ts_done, ts_done + P + kPrefetchLead] 全部在格
  static constexpr size_t feed_cells(size_t slots) { return slots + kPrefetchLead + 1; }

protected:
  const size_t num_assets_; // A 维 = 档 A 轴大小 (张量/落盘/流控全按它)

  // 两个日窗 (流水线的全部握手面, 进度面也只读它们), 同步扩格 (见 FeatureStore::grow):
  //   ring_ 张量窗: lo = io_done (已落盘的连续前缀, 见 io_close), n = P (日环格数)
  //   feed_ 源窗:   lo = ts_done (计满全轴的日数, 见 ts_close), n = feed_cells(P); 硬源的源格号 (软源只用它流控)
  DayWindow ring_;
  DayWindow feed_;
  std::atomic<size_t> pf_done_{0}; // 已备源日数 (fetch_add, 见 pf_close; 预取多核时非连续前缀, 只供仪表盘)
  std::atomic<size_t> cs_done_{0}; // 已截面日数 (fetch_add, 见 cs_close; 截面多核时非连续前缀, 只供仪表盘)

  // 源格日标记 [源窗格数上限]: 格最近备好的日 d + 1 (0 = 未备过)
  std::vector<std::atomic<size_t>> pf_mark_;

  StoreRing(size_t num_assets, size_t slots, size_t cap)
      : num_assets_(num_assets), ring_(slots, cap), feed_(feed_cells(slots), feed_cells(cap)), pf_mark_(feed_cells(cap)) {}

public:
  // ===== PF: 不占 slot, 只有流控与进度 =====

  // 日 d 在源窗 ⇒ 其格上一日已全员计满 (无读者), acquire 与源窗推进配对
  bool pf_try_open(size_t d) const { return d < feed_.span().end(); }

  // 硬源打源格日标记: release 与 pf_ready 的 acquire 配对 (解码写入先于 TS 读).
  // 软源不打: TS 不等预取, 也没有源格
  void pf_close(size_t d, bool hard) {
    if (hard) {
      std::atomic<size_t> &mark = pf_mark_[feed_.cell(d)];
      assert(mark.load(std::memory_order_relaxed) <= d && "pf_close: 源格已备过日 d 或之后 (日序乱)");
      mark.store(d + 1, std::memory_order_release);
    }
    pf_done_.fetch_add(1, std::memory_order_relaxed);
  }
  // 日 d 计满前其源格不交给别的日, 标记相等即已备好 (映射读到旧格 ⇒ 标记不符, 只是晚一轮)
  bool pf_ready(size_t d) const { return pf_mark_[feed_.cell(d)].load(std::memory_order_acquire) == d + 1; }

  // 日 d 的源格号: 预取在 pf_try_open 之后, 时序在 pf_ready 之后取 (硬源)
  size_t feed_cell(size_t d) const { return feed_.cell(d); }
  size_t feed_cap() const { return feed_.cap(); }

  // ===== QUERY API =====
  size_t query_A() const { return num_assets_; }
  size_t query_slots() const { return ring_.span().n; }
  size_t query_cap() const { return ring_.cap(); }
  size_t query_pf_done() const { return pf_done_.load(std::memory_order_relaxed); }
  size_t query_ts_done() const { return feed_.span().lo; }                          // 计满全轴的日数 (标签悬挂结清后才计, 落后最慢 TS 前沿)
  size_t query_cs_done() const { return cs_done_.load(std::memory_order_relaxed); } // 已截面日数 (多核时非连续前缀)
  size_t query_io_done() const { return ring_.span().lo; }
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
  const std::uint64_t axis_hash_; // A 轴指纹 (PoolAxis::hash), 落进每个特征文件头
  std::string output_dir_;

  // 日环格 [cap]: 格号 0..P−1 已分配 (grow 逐格追加), 日 d 占 slots_[ring_.cell(d)]. 不需要 freelist / epoch / 状态枚举 (见文件头)
  std::vector<std::unique_ptr<Slot>> slots_;

  // 前置: d 在飞 (张量窗内), 由各 try_open 的流控保证
  Slot &slot(size_t d) const { return *slots_[ring_.cell(d)]; }
  Day day_at(size_t d) const { return {&slot(d), num_assets_, d}; }

  static std::unique_ptr<Slot> make_slot(size_t num_assets) {
    auto s = std::make_unique<Slot>();
    s->allocate(num_assets);
    s->reset(num_assets); // memset 清零 = 坐实物理页 + 空闲态不变量: 拿来即写
    return s;
  }

public:
  // num_assets: 档 A 轴大小; axis_hash: PoolAxis::hash, 写进每个
  // 特征文件头锁定"列 → 资产"映射 (读端从同一 FeatureLib 推导同一轴比对).
  // slots = 初始格数 P, cap = 格数上限 (映射表长; 格数与内存预算由管线定, 见 feature_pipeline.hpp)
  FeatureStore(size_t num_assets, std::uint64_t axis_hash, const std::string &output_dir, size_t slots, size_t cap)
      : StoreRing(num_assets, slots, cap),
        axis_hash_(axis_hash), output_dir_(output_dir), slots_(cap) {
    assert(num_assets_ >= 1 && !output_dir_.empty() && "A 轴为空 / 无库目录");
    // 只建目录, 不清库: 清库/重算是调用方的显式决定 (回测全量重算见
    // ComputeService), 构造副作用会让实盘复用 store 时误删历史特征.
    std::filesystem::create_directories(output_dir_);

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

    printf("\nPer-day tensor: %.2f MB | Ring: %zu slots (上限 %zu) | Total: %.2f GB\n",
           bytes_per_day / (1024.0 * 1024.0), slots, cap,
           (bytes_per_day * slots) / (1024.0 * 1024.0 * 1024.0));

    std::cout << "Allocating physical pages...\n";
    auto start = std::chrono::steady_clock::now();

    // Parallel allocation: 一槽一任务
    std::atomic<size_t> progress_counter{0};
    misc::exec::ForkJoin ex(misc::exec::fit(slots));
    ex.tasks(static_cast<int>(slots), [&](int i, int) {
      slots_[static_cast<size_t>(i)] = make_slot(num_assets_);
      const size_t completed = ++progress_counter;
      std::cout << "  " << completed << "/" << slots << " slots\r" << std::flush;
    });

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    std::cout << "\nPhysical allocation complete in " << elapsed << " ms\n";
    std::cout << "=================================\n\n";
  }

  // ===== 扩格 (协调线程, worker 照跑) =====

  // 新 slot 先分配清零 (空闲态), 源窗先扩 (预取先看到余量), 再扩张量窗: 两窗各把新格接给各自的下一个进窗日
  void grow() {
    const size_t i = ring_.span().n; // 只有本线程扩格: 新格号 = 现有格数
    slots_[i] = make_slot(num_assets_);
    feed_.grow();
    ring_.grow();
  }

  // ===== 续算: 尾窗日读回 =====

  // 管线启动前调: 日 d = 0..n−1 的 slot (空闲态) ← 库里 dates[d] 的日文件, 逐层整层读回; 一日一任务.
  // 尾窗日之后照常 ts_close / cs_close / io (见文件头), 读不回 = 库被改过 (清单与日文件不符) → 断言
  void seed(std::span<const misc::Ymd> dates) {
    const DayWindow::Span s = ring_.span();
    assert(!dates.empty() && dates.size() < s.n && s.lo == 0 && "seed: 只在管线启动前填尾窗");
    misc::exec::ForkJoin ex(misc::exec::fit(dates.size()));
    ex.tasks(static_cast<int>(dates.size()), [&](int d, int) {
      FeatureDay::Scratch s;
      const std::string day_dir = feature_day_dir(output_dir_, dates[static_cast<size_t>(d)]);
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        if (!T::has(lvl))
          continue;
        const FeatureDay::Status st = FeatureDay::read_layer(feature_file(day_dir, lvl), T::LEVELS[lvl], slot(static_cast<size_t>(d)).data[lvl], num_assets_, axis_hash_, s);
        assert(st == FeatureDay::Status::Ok && "续算: 尾窗日文件读不回");
        (void)st;
      }
    });
  }

  // ===== TS: open 按 d, close 按 (asset, d) =====

  // 背压: 日 d 须在张量窗 (其格已由上一日落盘交还). 回填旧日也走这里 (旧日未计满 ⇒ 仍在飞, 必得).
  // slot 在 io_close 已清零, 拿来即写. 空句柄 = 池满 —— 调用方拿等待时间去干别的 (领跑者在池边领养回填).
  // acquire 与张量窗推进的 release 配对 (清零先于复用).
  Day ts_try_open(size_t d) {
    const DayWindow::Span s = ring_.span();
    assert(d >= s.lo && "ts_try_open: 日 d 已落盘 (调度侧日序乱)");
    return d < s.end() ? day_at(d) : Day{};
  }

  // 一个 (asset, d) 写完. 计数攒齐全轴 (池轴) 后 CS 放行, 并推进源窗;
  // release 与 cs_try_open / 源窗推进的 acquire 配对, 保证张量写入对 CS 可见、源格读完先于复用.
  // 缺 binary 的 asset-day 也要计 (张量保持默认值), 由当时的处置权持有者计.
  // 源窗推进: 已落盘的日必已计满 (计满到推进之间 CS / IO 可能已把本日落盘清零), 其余看计数
  void ts_close(const Day &day) {
    assert(day);
    const uint32_t prev = day.slot->assets_done.fetch_add(1, std::memory_order_release);
    assert(prev < num_assets_ && "ts_close: 计数超过派活资产数 (重复计?)");
    if (prev + 1 == num_assets_) {
      feed_.advance([this](size_t lo) {
        const DayWindow::Span s = ring_.span();
        return lo < s.lo || (lo < s.end() && slot(lo).assets_done.load(std::memory_order_acquire) == num_assets_);
      });
      Logger::log("store", "ts_day_done: d=" + std::to_string(day.d));
    }
  }

  // ===== CS: 每 d 一对 open/close =====

  // 按日流控: 本日计满全轴才放行, 返回后两层张量整体可读 —— CS 内部不需要任何行级等待.
  // 先验日 d 在张量窗: 否则映射还指着上一日 (已计满待落盘) 的格, 计数会误放行.
  // 截面多核各开各的日 (d % K), 互不握手
  Day cs_try_open(size_t d) {
    const DayWindow::Span s = ring_.span();
    assert(d >= s.lo && "cs_try_open: 日 d 已落盘 (日序乱)");
    if (d >= s.end())
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
    assert(day.d - 1 >= ring_.span().lo && "cs_prev: d−1 已落盘 (io_try_open 握手失效)");
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
  // 落盘多核按日轮转: 先验日 d 在张量窗, 否则映射还指着上一日 (已截面待落盘) 的格, cs_closed 会误放行.
  // 非末日 (last = d 是日期轴末日) 还要 CS(d+1) 已 close: 它经 cs_prev 读本 slot. d+1 同样先验在窗;
  // CS(d+1) close 后 IO(d+1) 可能抢先落盘清零 (cs_closed 复位) → 再认 io_mark == d+2 (粘滞; 两者都读漏只是晚一轮)
  Day io_try_open(size_t d, bool last) {
    const DayWindow::Span s = ring_.span();
    assert(d >= s.lo && "io_try_open: 日 d 已落盘 (日序乱)");
    if (d >= s.end())
      return {};
    const Day day = day_at(d);
    if (!day.slot->cs_closed.load(std::memory_order_acquire))
      return {};
    if (last)
      return day;
    if (d + 1 >= s.end())
      return {};
    const Slot &next = slot(d + 1);
    return next.cs_closed.load(std::memory_order_acquire) || next.io_mark.load(std::memory_order_acquire) == d + 2 ? day : Day{};
  }

  // 本日落盘完毕: 清零后打日标记 (空闲态不变量: 张量 + 计数全零, 拿来即写), 再把张量窗推到首个未落盘日.
  // 标记 release 与推进方的 acquire 配对, 张量窗推进的 release 再与 ts_try_open / cs_try_open 的 acquire 配对 (清零先于复用).
  // 每个 close 都在打完自己的标记之后推进 (窗口锁内): 乱序 close 的日由后到者一并推过, 前缀不会卡住
  void io_close(const Day &day) {
    assert(day && day.d >= ring_.span().lo && "io_close: 日 d 已落盘 (日序乱)");
    assert(day.slot->io_mark.load(std::memory_order_relaxed) <= day.d && "io_close: slot 已落过日 d 或之后 (日序乱)");
    day.slot->reset(num_assets_);
    day.slot->io_mark.store(day.d + 1, std::memory_order_release);
    ring_.advance([this](size_t lo) { return slot(lo).io_mark.load(std::memory_order_acquire) == lo + 1; });
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
//   TS(node[, port], Tf, Method) → ts::Tf::apply(dag.node.out(port)); CS/LABEL/FLAG/META 不在 row 写
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
#define STORE_ROW_TS_3(node, tf, m) ts::tf::apply(dag.node.out())
#define STORE_ROW_TS_4(node, port, tf, m) ts::tf::apply(dag.node.out(dag.node.port))
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
