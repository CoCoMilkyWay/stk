#pragma once

#include "features/MetaFlag.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cassert>
#include <memory>
#include <thread>
#include <vector>

// ============================================================================
// 流式分析的两种对偶平面 (对应生产管线的 TS / CS 两段) + 共用的段序 / 读计划 / IO 暂存 / 双缓冲预读
// ============================================================================
//   TsPlane  data[c][a][j][t]  资产主序: 每资产每列一段连续 segs×R (TS 扫描友好), 截面按 asset_stride 跨步取
//   CsPlane  data[j][s][c][a]  切片主序: 每 (段, 切片, 列) 一段连续 A (截面友好), 每段只取 slice_t 指定的 S 行
// 行轴 = 网格层 (ReadPlan::level) 的段 (SegPlan); 列 = ReadPlan 的 ColRef, 跨层跨档的列按 level_asof 投影到网格行.
// 同一个 IO 契约 (每源一个 fmeta::ColumnSet: 读什么、各列怎么门控), 同一套哨兵 (fmeta):
//   门控不过 / 无源 (空日, 区间首日无承接) → kInvalidBits, 门控过但 NaN → kNaNBits, 其余 (含 ±inf) 原样.
//   热循环一次 v != v 两种一并跳过; 要分账 (NaN vs 门控不过) 的消费方按位区分, 分母口径由消费方自己定.
// t = 有效行 (level_valid_rows, 末哨兵行不进平面). 容量准备幂等: 尺寸对得上零分配.
// ============================================================================

// ---- 段: 平面一个 j. 日内层 1 天, L_day 连续 kDaySegDays 个交易日 (日频序列成段); 段行数 = 每日有效行 × 段天数 ----
inline constexpr size_t kDaySegDays = 255;
inline constexpr size_t seg_days(size_t lvl) { return lvl == L_day ? kDaySegDays : 1; }
inline constexpr size_t seg_rows(size_t lvl) { return level_valid_rows(lvl) * seg_days(lvl); }

// 段序: [段][days] 平铺; dates 0 = 尾段空位 (整天作废); prev = 库内前一交易日 (as-of 跨日承接), 0 = 区间首日
struct SegPlan {
  size_t days = 1;
  std::vector<misc::Ymd> dates, prev;
  std::vector<uint16_t> month;  // months 下标
  std::vector<uint8_t> weekday; // Mon=0 .. Sun=6
  size_t size() const { return dates.size() / days; }
};

// 源 = 同 (档, 层) 的一组值列: 一天一次选列读 (cs 末位带该层 ts_valid), 行按 level_asof 投影到网格
// 读端 (平面) 与写端上游读 (CoreCs 旁读 / pool::Panel 票池成员位) 同一份: 跨档跨层一律 level_asof, 不另写特例
struct Source {
  TierId tier = TierId::Lob; // 主档 = 同轴; 旁读档: 平面 a ← 该档轴 Readers::ids[tier][a]
  size_t level = L_min;
  fmeta::ColumnSet cs;        // 值列 (+ 门控列)
  std::vector<uint32_t> slot; // [cs.n()] 值列 → 平面列
  std::vector<int32_t> row;   // [网格每日有效行] → 源行; -1 = 前一交易日末行
  bool cur = false;           // row 含 ≥ 0 (读当日)
  bool carry = false;         // row 含 -1 (读前一交易日)
};
// 源行投影: 网格层 grid 的 rows 个行 → level_asof(源层, grid, t)
inline void project_rows(Source &s, size_t grid, size_t rows) {
  s.row.resize(rows);
  s.cur = s.carry = false;
  for (size_t t = 0; t < rows; ++t) {
    s.row[t] = static_cast<int32_t>(level_asof(s.level, grid, t));
    s.cur |= s.row[t] >= 0;
    s.carry |= s.row[t] < 0;
  }
}
// 读计划: 一组 ColRef → 网格层上的平面列 (GUI 线程构建, 见 analysis::read_plan)
struct ReadPlan {
  TierId main = TierId::Lob; // 主档 (A 轴); 源档 = 主档或其旁读档
  size_t level = L_min;      // 网格层
  std::vector<ColRef> cols;  // [平面列]
  std::vector<Source> srcs;
  size_t n() const { return cols.size(); }
};
// 库集各档 reader + 主轴 → 该档轴映射 (主档 nullptr = 同轴; 库集外档 nullptr)
struct Readers {
  TierId main = TierId::Lob;
  PerTier<const FeatureRead *> rd{};
  PerTier<const uint32_t *> ids{};
  const FeatureRead &of(TierId t) const {
    assert(rd[t] && "档不在库集内");
    return *rd[t];
  }
  const FeatureRead &lib() const { return of(main); }
};

// IO 暂存 (按 io 池 tid): 每源 当日 + 承接日 选列张量 + 选列表 (稳态零分配)
struct IoSlot {
  struct Buf {
    FeatureRead::DayColumns cur, prev;
    bool has_cur = false, has_prev = false;
  };
  std::vector<Buf> src; // [plan.srcs]
  std::vector<size_t> cols;

  // As: 各档 A 轴长 (ReadScope::sizes); max_cols: 单次读的值列上限 (分块读的平面传块宽)
  void prepare(const ReadPlan &plan, const PerTier<size_t> &As, size_t max_cols) {
    src.resize(plan.srcs.size());
    for (size_t i = 0; i < src.size(); ++i) {
      const Source &s = plan.srcs[i];
      const size_t n = std::min(s.cs.n(), max_cols) + s.cs.gated();
      const size_t SA = As[s.tier];
      if (s.cur)
        src[i].cur.preallocate(SA, s.level, n);
      if (s.carry)
        src[i].prev.preallocate(SA, s.level, n);
    }
    cols.reserve(max_cols + 1);
  }
};

// 源一天 (当日 + 承接日) 选列读进暂存; io = 本次读列 (值列子集 [+ 门控列])
inline void load_source(const Readers &rd, const Source &s, misc::Ymd date, misc::Ymd prev,
                        const std::vector<size_t> &io, IoSlot::Buf &b) {
  const FeatureRead &reader = rd.of(s.tier);
  b.has_cur = s.cur && date != 0;
  b.has_prev = s.carry && date != 0 && prev != 0;
  if (b.has_cur)
    reader.load_day_columns(date, io, b.cur);
  if (b.has_prev)
    reader.load_day_columns(prev, io, b.prev);
}
// 网格行 t → 源暂存 + 源行 r (nullptr = 无源)
inline const FeatureRead::DayColumns *source_row(const Source &s, const IoSlot::Buf &b, size_t t, size_t &r) {
  const int32_t x = s.row[t];
  if (x >= 0) {
    r = static_cast<size_t>(x);
    return b.has_cur ? &b.cur : nullptr;
  }
  r = level_valid_rows(s.level) - 1;
  return b.has_prev ? &b.prev : nullptr;
}

// 一格原值 → 折叠哨兵 (g = 同层门控同一格, nullptr = 不门控)
inline feature_storage_t fold_cell(feature_storage_t raw, const feature_storage_t *g, L2::ValidType vt) {
  if (g && !fmeta::valid(static_cast<float>(*g), vt))
    return std::bit_cast<feature_storage_t>(fmeta::kInvalidBits);
  return fmeta::nan(raw) ? std::bit_cast<feature_storage_t>(fmeta::kNaNBits) : raw;
}
// 一行 A 格 (平面 a ← 源 ids[a]; ids nullptr = 同轴)
inline void fold_row(const feature_storage_t *src, const feature_storage_t *g, L2::ValidType vt, size_t A,
                     feature_storage_t *dst, const uint32_t *ids = nullptr) {
  for (size_t a = 0; a < A; ++a) {
    const size_t x = ids ? ids[a] : a;
    dst[a] = fold_cell(src[x], g ? g + x : nullptr, vt);
  }
}

// ============================================================================
// TsPlane: 段批 TS 视图 (Dist / Transform 分批流式, Preview 单段)
// ============================================================================
struct TsPlane {
  static constexpr size_t kTransposeBlock = 64; // 转置按 64 资产一块: 块内写驻留在 64 条 cache line 上

  size_t A = 0, level = 0, R = 0, segs = 0, n_cols = 0;
  std::vector<feature_storage_t> data; // [n_cols][A][segs][R]

  size_t asset_stride() const { return segs * R; }

  // 资产 a 在平面列 c 的批内第 j 段序列 (R 个)
  const feature_storage_t *series(size_t c, size_t a, size_t j) const {
    assert(c < n_cols && a < A && j < segs);
    return data.data() + ((c * A + a) * segs + j) * R;
  }
  // (c, j, t) 截面列首元素; 资产 a 在 + a * asset_stride()
  const feature_storage_t *column(size_t c, size_t j, size_t t) const {
    assert(c < n_cols && j < segs && t < R);
    return data.data() + (c * A * segs + j) * R + t;
  }

  void prepare(size_t A_, size_t level_, size_t segs_, size_t n_cols_) {
    TraceN("TsPlanePrepare");
    assert(A_ > 0 && segs_ > 0 && n_cols_ > 0);
    A = A_;
    level = level_;
    R = seg_rows(level_);
    segs = segs_;
    n_cols = n_cols_;
    data.resize(n_cols * A * segs * R);
  }

  // 第 seg 段 → 批内第 j 段: 逐日逐源读进 sl, 行投影 + 按各列 vt 折叠哨兵 + 转置
  void load_seg(const Readers &rd, const ReadPlan &plan, const SegPlan &sp, size_t seg, size_t j, IoSlot &sl) {
    assert(j < segs && plan.n() == n_cols && plan.level == level && sp.days == seg_days(level));
    const auto invalid = std::bit_cast<feature_storage_t>(fmeta::kInvalidBits);
    const size_t rpd = level_valid_rows(level), stride = asset_stride();
    for (size_t d = 0; d < sp.days; ++d) {
      const size_t i = seg * sp.days + d;
      for (size_t si = 0; si < plan.srcs.size(); ++si) {
        const Source &s = plan.srcs[si];
        IoSlot::Buf &b = sl.src[si];
        {
          TraceN("LoadDay");
          load_source(rd, s, sp.dates[i], sp.prev[i], s.cs.io, b);
        }
        TraceN("TransposeDay");
        const size_t nv = s.cs.n();
        const uint32_t *ids = rd.ids[s.tier];
        for (size_t k = 0; k < nv; ++k) {
          const L2::ValidType vt = s.cs.vts[k];
          feature_storage_t *base = data.data() + (s.slot[k] * A * segs + j) * R + d * rpd; // + a * stride + t
          for (size_t a0 = 0; a0 < A; a0 += kTransposeBlock) {
            const size_t a1 = std::min(a0 + kTransposeBlock, A);
            for (size_t t = 0; t < rpd; ++t) {
              size_t r = 0;
              const FeatureRead::DayColumns *dc = source_row(s, b, t, r);
              if (!dc) {
                for (size_t a = a0; a < a1; ++a)
                  base[a * stride + t] = invalid;
                continue;
              }
              const size_t SA = dc->A;
              const feature_storage_t *val = dc->col(k) + r * SA;
              const feature_storage_t *g = s.cs.gated() ? dc->col(nv) + r * SA : nullptr;
              for (size_t a = a0; a < a1; ++a) {
                const size_t x = ids ? ids[a] : a;
                assert(x < SA);
                base[a * stride + t] = fold_cell(val[x], g ? g + x : nullptr, vt);
              }
            }
          }
        }
      }
    }
  }

  // 一批 [s0, s0 + bs): 单段一任务 (io 池, 暂存按 tid; 只写本平面, 可与另一平面上的计算并发)
  void load_batch(misc::exec::ForkJoin &io, std::vector<IoSlot> &slots, const Readers &rd, const ReadPlan &plan,
                  const SegPlan &sp, size_t s0, size_t bs, const std::atomic<bool> &cancel) {
    assert(bs <= segs && s0 + bs <= sp.size() && slots.size() >= static_cast<size_t>(io.threads()));
    io.tasks(static_cast<int>(bs), [&](int j, int tid) {
      if (!cancel.load(std::memory_order_relaxed))
        load_seg(rd, plan, sp, s0 + static_cast<size_t>(j), static_cast<size_t>(j), slots[tid]);
    });
  }

  void clear() {
    // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存
    data = std::vector<feature_storage_t>{};
    A = level = R = segs = n_cols = 0;
  }
};

// ============================================================================
// CsPlane: 段批 CS 视图 (Corr / CorrLag / CorrPair) —— 每段只抽 slice_t 指定的 S 行
// ============================================================================
// 行号 g = (j·S + s)·n_cols + c  ↔  data + g·A  (切片主序: 同一 (j, s) 的全部列相邻)
struct CsPlane {
  static constexpr size_t kBlockCols = 16; // IO 一任务读的列数 (暂存 16·T·A·2B)

  size_t A = 0, level = 0, R = 0, segs = 0, S = 0, n_cols = 0;
  std::vector<size_t> slice_t;         // [segs][S] 段内行号 (>= R = 越界, 整片作废); 调用方在 load 前填
  std::vector<feature_storage_t> data; // [segs][S][n_cols][A]

  const feature_storage_t *slice(size_t j, size_t s, size_t c) const {
    assert(j < segs && s < S && c < n_cols);
    return data.data() + ((j * S + s) * n_cols + c) * A;
  }

  void prepare(size_t A_, size_t level_, size_t segs_, size_t S_, size_t n_cols_) {
    TraceN("CsPlanePrepare");
    assert(A_ > 0 && segs_ > 0 && S_ > 0 && n_cols_ > 0);
    A = A_;
    level = level_;
    R = seg_rows(level_);
    segs = segs_;
    S = S_;
    n_cols = n_cols_;
    slice_t.assign(segs * S, R);
    data.resize(segs * S * n_cols * A);
  }

  // 一批 [s0, s0 + bs): (段 × 源列块) 一任务 —— 块内逐日读 (只读有切片落点的日, 列块 + 该源门控), 抽行即折叠 (暂存按 io tid)
  void load_batch(misc::exec::ForkJoin &io, std::vector<IoSlot> &slots, const Readers &rd, const ReadPlan &plan,
                  const SegPlan &sp, size_t s0, size_t bs, const std::atomic<bool> &cancel) {
    TraceN("CsPlaneLoad");
    assert(bs <= segs && s0 + bs <= sp.size() && plan.n() == n_cols && plan.level == level && sp.days == seg_days(level));
    assert(slots.size() >= static_cast<size_t>(io.threads()));
    struct Blk {
      size_t si, k0, k1;
    };
    std::vector<Blk> blks;
    for (size_t si = 0; si < plan.srcs.size(); ++si)
      for (size_t k0 = 0; k0 < plan.srcs[si].cs.n(); k0 += kBlockCols)
        blks.push_back({si, k0, std::min(k0 + kBlockCols, plan.srcs[si].cs.n())});
    const auto invalid = std::bit_cast<feature_storage_t>(fmeta::kInvalidBits);
    const size_t rpd = level_valid_rows(level), n_blk = blks.size();
    io.tasks(static_cast<int>(bs * n_blk), [&](int task, int tid) {
      if (cancel.load(std::memory_order_relaxed))
        return;
      const size_t j = static_cast<size_t>(task) / n_blk;
      const Blk &bk = blks[static_cast<size_t>(task) % n_blk];
      const Source &src = plan.srcs[bk.si];
      const size_t *st = slice_t.data() + j * S;
      for (size_t s = 0; s < S; ++s)
        if (st[s] >= R)
          for (size_t k = bk.k0; k < bk.k1; ++k)
            std::fill_n(data.data() + ((j * S + s) * n_cols + src.slot[k]) * A, A, invalid);
      IoSlot &sl = slots[tid];
      IoSlot::Buf &b = sl.src[bk.si];
      sl.cols.assign(src.cs.io.begin() + bk.k0, src.cs.io.begin() + bk.k1);
      if (src.cs.gated())
        sl.cols.push_back(src.cs.gate());
      for (size_t d = 0; d < sp.days; ++d) {
        if (std::none_of(st, st + S, [&](size_t t) { return t < R && t / rpd == d; }))
          continue;
        const size_t i = (s0 + j) * sp.days + d;
        load_source(rd, src, sp.dates[i], sp.prev[i], sl.cols, b);
        for (size_t s = 0; s < S; ++s) {
          if (st[s] >= R || st[s] / rpd != d)
            continue;
          size_t r = 0;
          const FeatureRead::DayColumns *dc = source_row(src, b, st[s] % rpd, r);
          for (size_t k = bk.k0; k < bk.k1; ++k) {
            feature_storage_t *dst = data.data() + ((j * S + s) * n_cols + src.slot[k]) * A;
            if (!dc) {
              std::fill_n(dst, A, invalid);
              continue;
            }
            const size_t SA = dc->A;
            const feature_storage_t *g = src.cs.gated() ? dc->col(bk.k1 - bk.k0) + r * SA : nullptr;
            fold_row(dc->col(k - bk.k0) + r * SA, g, src.cs.vts[k], A, dst, rd.ids[src.tier]);
          }
        }
      }
    });
  }

  void clear() {
    slice_t = std::vector<size_t>{};
    data = std::vector<feature_storage_t>{};
    A = level = R = segs = S = n_cols = 0;
  }
};

// ============================================================================
// Prefetch<Plane>: 双缓冲预读 —— 算 front 的同时后台线程把下一批读进 back
// ============================================================================
// 两个池互不重入: 计算池由 worker 线程 dispatch, io 池由预读线程 dispatch.
// 循环契约 (issue 与 wait 之间不许 return: 预读线程引用着调用方栈):
//   now(load 0);
//   for b: { if (有下一批) issue(load b+1);  compute(front());  wait();  if (cancel) return false;  publish;  flip(); }
template <class Plane>
struct Prefetch {
  Plane buf[2];
  size_t cur = 0;
  std::unique_ptr<misc::exec::ForkJoin> io;
  std::vector<IoSlot> slots; // [io 线程数]
  std::thread th;

  Plane &front() { return buf[cur]; }
  Plane &back() { return buf[cur ^ 1]; }

  // 幂等: 建 io 池 + 暂存 (As / max_cols 见 IoSlot::prepare); 两个平面的尺寸由调用方各自 prepare
  void prepare(size_t n_io, const ReadPlan &plan, const PerTier<size_t> &As, size_t max_cols) {
    assert(!th.joinable());
    misc::exec::ensure(io, static_cast<int>(n_io));
    slots.resize(n_io);
    for (IoSlot &s : slots)
      s.prepare(plan, As, max_cols);
    cur = 0;
  }
  // fn(Plane &, ForkJoin &io, std::vector<IoSlot> &)
  template <class Fn>
  void now(Fn &&fn) {
    assert(!th.joinable());
    fn(front(), *io, slots);
  }
  template <class Fn>
  void issue(Fn &&fn) {
    assert(!th.joinable());
    th = std::thread([this, fn = std::forward<Fn>(fn)]() mutable { fn(back(), *io, slots); });
  }
  void wait() {
    if (th.joinable())
      th.join();
  }
  void flip() {
    assert(!th.joinable());
    cur ^= 1;
  }
  void clear() {
    wait();
    buf[0].clear();
    buf[1].clear();
    io.reset();
    slots = std::vector<IoSlot>{};
    cur = 0;
  }
  ~Prefetch() { wait(); }
};
