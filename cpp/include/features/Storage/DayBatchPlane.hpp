#pragma once

#include "features/Storage/FeatureRead.hpp"
#include "features/MetaFlag.hpp"
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
// 流式分析的两种对偶平面 (对应生产管线的 TS / CS 两段) + 共用的 IO 暂存 / 双缓冲预读
// ============================================================================
//   TsPlane  data[c][a][j][t]  资产主序: 每资产每列一段连续 days×VR (TS 扫描友好), 截面按 asset_stride 跨步取
//   CsPlane  data[j][s][c][a]  切片主序: 每 (天, 切片, 列) 一段连续 A (截面友好), 每天只取 slice_t 指定的 S 行
// 同一个 IO 契约 (fmeta::ColumnSet 描述读什么、各列怎么门控), 同一套哨兵 (fmeta):
//   门控不过 → kInvalidBits, 门控过但 NaN → kNaNBits, 其余 (含 ±inf) 原样.
//   热循环一次 v != v 两种一并跳过; 要分账 (NaN vs 门控不过) 的消费方按位区分, 分母口径由消费方自己定.
// t = 有效行 (level_valid_rows, 末哨兵行不进平面). 容量准备幂等: 尺寸对得上零分配.
// ============================================================================

// IO 暂存 (按 io 池 tid): 单日选列张量 + 选列表 (稳态零分配)
struct IoSlot {
  FeatureRead::DayColumns day;
  std::vector<size_t> cols;

  void prepare(size_t A, size_t level, size_t max_cols) {
    day.preallocate(A, level, max_cols);
    cols.reserve(max_cols);
  }
};

// 一段 A 个原值 → 折叠哨兵 (g = 同层门控同一行, nullptr = 不门控)
inline void fold_row(const feature_storage_t *src, const feature_storage_t *g, L2::ValidType vt, size_t A,
                     feature_storage_t *dst) {
  const auto invalid = std::bit_cast<feature_storage_t>(fmeta::kInvalidBits);
  const auto nan = std::bit_cast<feature_storage_t>(fmeta::kNaNBits);
  for (size_t a = 0; a < A; ++a) {
    const bool ok = !g || fmeta::valid(static_cast<float>(g[a]), vt);
    const feature_storage_t raw = src[a];
    dst[a] = !ok ? invalid : (fmeta::nan(raw) ? nan : raw);
  }
}

// ============================================================================
// TsPlane: 天批 TS 视图 (Dist / Transform 分批流式, Preview 单天)
// ============================================================================
struct TsPlane {
  static constexpr size_t kTransposeBlock = 64; // 转置按 64 资产一块: 块内写驻留在 64 条 cache line 上

  size_t A = 0, level = 0, VR = 0, days = 0, n_cols = 0;
  std::vector<feature_storage_t> data; // [n_cols][A][days][VR]

  size_t asset_stride() const { return days * VR; }

  // 资产 a 在值列 c 的批内第 j 天序列 (VR 个)
  const feature_storage_t *series(size_t c, size_t a, size_t j) const {
    assert(c < n_cols && a < A && j < days);
    return data.data() + ((c * A + a) * days + j) * VR;
  }
  // (c, j, t) 截面列首元素; 资产 a 在 + a * asset_stride()
  const feature_storage_t *column(size_t c, size_t j, size_t t) const {
    assert(c < n_cols && j < days && t < VR);
    return data.data() + (c * A * days + j) * VR + t;
  }

  void prepare(size_t A_, size_t level_, size_t days_, size_t n_cols_) {
    TraceN("TsPlanePrepare");
    assert(A_ > 0 && days_ > 0 && n_cols_ > 0);
    A = A_;
    level = level_;
    VR = level_valid_rows(level_);
    days = days_;
    n_cols = n_cols_;
    data.resize(n_cols * A * days * VR);
  }

  // 一天 → 批内第 j 天: 读 cs.io 进 st, 逐列按自己的 vt 折叠哨兵并转置.
  // 门控行来源: cs 自带 (st 末列) / 调用方外给 gate ([T][A] 同层 ts_valid, 多块共用一次读) / 无 (全 ALL)
  // ids: 平面资产 a ← 源库资产 ids[a] (Bar 档旁读: 源 = 全轴, st 按全轴 preallocate); nullptr = 同轴
  void load_day(const FeatureRead &reader, misc::Ymd date, const fmeta::ColumnSet &cs, size_t j,
                FeatureRead::DayColumns &st, const feature_storage_t *gate = nullptr, const uint32_t *ids = nullptr) {
    assert(j < days && cs.n() == n_cols);
    assert(!(cs.gated() && gate));
    assert(!(ids && gate) && "外给门控行在主轴上");
    assert(ids ? st.A >= A : st.A == A);
    {
      TraceN("LoadDay");
      reader.load_day_columns(date, cs.io, st);
    }
    TraceN("TransposeDay");
    const size_t SA = st.A;
    const feature_storage_t *g = cs.gated() ? st.col(n_cols) : gate;
    if (!g)
      for (const L2::ValidType vt : cs.vts)
        assert(vt == L2::ValidType::ALL && "需门控的列必须带门控来源");
    const auto invalid = std::bit_cast<feature_storage_t>(fmeta::kInvalidBits);
    const auto nan = std::bit_cast<feature_storage_t>(fmeta::kNaNBits);
    const size_t stride = asset_stride();
    for (size_t c = 0; c < n_cols; ++c) {
      const L2::ValidType vt = cs.vts[c];
      feature_storage_t *base = data.data() + (c * A * days + j) * VR; // + a * stride + t
      for (size_t a0 = 0; a0 < A; a0 += kTransposeBlock) {
        const size_t a1 = std::min(a0 + kTransposeBlock, A);
        for (size_t t = 0; t < VR; ++t) {
          const feature_storage_t *val = st.col(c) + t * SA;
          const feature_storage_t *gt = g ? g + t * SA : nullptr;
          for (size_t a = a0; a < a1; ++a) {
            const size_t s = ids ? ids[a] : a;
            assert(s < SA);
            const bool ok = !gt || fmeta::valid(static_cast<float>(gt[s]), vt);
            const feature_storage_t raw = val[s];
            base[a * stride + t] = !ok ? invalid : (fmeta::nan(raw) ? nan : raw);
          }
        }
      }
    }
  }

  // 一批: 单天一任务 (io 池, 暂存按 tid; 只写本平面, 可与另一平面上的计算并发)
  void load_batch(misc::exec::ForkJoin &io, std::vector<IoSlot> &slots, const FeatureRead &reader,
                  const misc::Ymd *dates, size_t bd, const fmeta::ColumnSet &cs, const std::atomic<bool> &cancel,
                  const uint32_t *ids = nullptr) {
    assert(bd <= days && slots.size() >= static_cast<size_t>(io.threads()));
    io.tasks(static_cast<int>(bd), [&](int j, int tid) {
      if (!cancel.load(std::memory_order_relaxed))
        load_day(reader, dates[j], cs, static_cast<size_t>(j), slots[tid].day, nullptr, ids);
    });
  }

  void clear() {
    // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存
    data = std::vector<feature_storage_t>{};
    A = level = VR = days = n_cols = 0;
  }
};

// ============================================================================
// CsPlane: 天批 CS 视图 (Corr / CorrLag / CorrPair) —— 每天只抽 slice_t 指定的 S 行
// ============================================================================
// 行号 g = (j·S + s)·n_cols + c  ↔  data + g·A  (切片主序: 同一 (j, s) 的全部列相邻)
struct CsPlane {
  static constexpr size_t kBlockCols = 16; // IO 一任务读的列数 (暂存 16·T·A·2B)

  size_t A = 0, level = 0, VR = 0, days = 0, S = 0, n_cols = 0;
  std::vector<size_t> slice_t;         // [days][S] 切片行号 (>= VR = 越界, 整片作废); 调用方在 load 前填
  std::vector<feature_storage_t> data; // [days][S][n_cols][A]
  std::vector<feature_storage_t> gate; // [days][S][A] 门控行 (抽好的)

  const feature_storage_t *slice(size_t j, size_t s, size_t c) const {
    assert(j < days && s < S && c < n_cols);
    return data.data() + ((j * S + s) * n_cols + c) * A;
  }

  void prepare(size_t A_, size_t level_, size_t days_, size_t S_, size_t n_cols_) {
    TraceN("CsPlanePrepare");
    assert(A_ > 0 && days_ > 0 && S_ > 0 && n_cols_ > 0);
    A = A_;
    level = level_;
    VR = level_valid_rows(level_);
    days = days_;
    S = S_;
    n_cols = n_cols_;
    slice_t.assign(days * S, VR);
    data.resize(days * S * n_cols * A);
    gate.resize(days * S * A);
  }

  // 一批: 门控列每天单读一次 (天一任务) → 值列 (天 × kBlockCols 列块) 一任务并行读, 抽行即折叠 (暂存按 io tid)
  void load_batch(misc::exec::ForkJoin &io, std::vector<IoSlot> &slots, const FeatureRead &reader,
                  const misc::Ymd *dates, size_t bd, const fmeta::ColumnSet &cs, const std::atomic<bool> &cancel) {
    TraceN("CsPlaneLoad");
    assert(bd <= days && cs.n() == n_cols && slots.size() >= static_cast<size_t>(io.threads()));
    const bool gated = cs.gated();
    if (gated)
      io.tasks(static_cast<int>(bd), [&](int j, int tid) {
        if (cancel.load(std::memory_order_relaxed))
          return;
        IoSlot &sl = slots[tid];
        sl.cols.assign(1, cs.gate());
        reader.load_day_columns(dates[j], sl.cols, sl.day);
        for (size_t s = 0; s < S; ++s) {
          const size_t t = slice_t[j * S + s];
          if (t < VR)
            std::copy_n(sl.day.col(0) + t * A, A, gate.data() + (j * S + s) * A);
        }
      });
    const auto invalid = std::bit_cast<feature_storage_t>(fmeta::kInvalidBits);
    const size_t n_blk = (n_cols + kBlockCols - 1) / kBlockCols;
    io.tasks(static_cast<int>(bd * n_blk), [&](int task, int tid) {
      if (cancel.load(std::memory_order_relaxed))
        return;
      const size_t j = static_cast<size_t>(task) / n_blk, blk = static_cast<size_t>(task) % n_blk;
      const size_t c0 = blk * kBlockCols, c1 = std::min(c0 + kBlockCols, n_cols);
      IoSlot &sl = slots[tid];
      sl.cols.assign(cs.io.begin() + c0, cs.io.begin() + c1);
      reader.load_day_columns(dates[j], sl.cols, sl.day);
      for (size_t s = 0; s < S; ++s) {
        const size_t t = slice_t[j * S + s];
        const feature_storage_t *g = gated ? gate.data() + (j * S + s) * A : nullptr;
        for (size_t c = c0; c < c1; ++c) {
          feature_storage_t *dst = data.data() + ((j * S + s) * n_cols + c) * A;
          if (t >= VR)
            std::fill_n(dst, A, invalid);
          else
            fold_row(sl.day.col(c - c0) + t * A, g, cs.vts[c], A, dst);
        }
      }
    });
  }

  void clear() {
    slice_t = std::vector<size_t>{};
    data = std::vector<feature_storage_t>{};
    gate = std::vector<feature_storage_t>{};
    A = level = VR = days = S = n_cols = 0;
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

  // 幂等: 建 io 池 + 暂存; 两个平面的尺寸由调用方各自 prepare
  void prepare(size_t n_io, size_t A, size_t level, size_t max_cols) {
    assert(!th.joinable());
    misc::exec::ensure(io, static_cast<int>(n_io));
    slots.resize(n_io);
    for (IoSlot &s : slots)
      s.prepare(A, level, max_cols);
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
