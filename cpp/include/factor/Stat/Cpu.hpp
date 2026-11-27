#pragma once

// =============================================================================
// Stat 评估算子的 CPU 后端 (factor::cpu::stat; 语义契约见 factor/Stat/Contract.hpp, 行核见 Stat/Host.hpp)
// =============================================================================
//   定位: 无 GPU 时的评估路径, 与 GPU 后端对拍 (无 golden, 两边必须 match). 不 include Gpu.cuh.
//   【分层】算子 = 行核 (Host.hpp) + 流程 (本文件), 无线程 / 无分配 / 无所有权; 谁出线程由 Exec 定 (misc/exec.hpp).
//   与 Stat/Gpu.cuh 同名同参序 (契约【视图】Plane / Batch / Mode / Labels / Span; 出口逐日 Sum[H][D]):
//     rank_x_rows / rank_y_rows   行区间核 [t0,t1) (单线程, 绝对 t)
//     prep_label    ry[T][A]                                     ← exec.rows(rank_y_rows)
//     rank_x        rx[T][A] (按 Mode 口径 / 方向, 已定向)       ← exec.rows(rank_x_rows)
//     accum         rx × 标签集 → 统计日 [0, D) 的 Row (绝对) 按 Mode 收益口径折进 Sum[H][D]  ← exec.rows(按日切: 每日由一个线程独占, 日内行序累加)
//                   sink.row(i, t, r) 每行一次 (线程内, 按日切: 同日的行同一线程, 行序): DropRows 丢弃 / LandRows 落地 rows[i·T + t] / 调用方自定 (引擎直接给探针组);
//                   sink 带 kCells = true 时另收真持有格 sink.cell(i, t, a, g, pos, y) (行核内一次 walk 同出, 不另走链)
//     run           = rank_x + accum (一个候选的完整评估); HoldStat 由 Fold stat_of
//     run_batch     一批候选: 候选串行, 候选内全核切日 (C 小也能跑满核, 只留一张 [T][A] 的 r16)
//   换手 (账集) 走 Stat/Book.hpp book_rows. 标签秩 ry / r16(x) 平面 rx 由调用方持有; Workspace (Scratch [tid]) 由调用方持有, ensure 幂等.
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Fold.hpp"
#include "factor/Stat/Host.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <type_traits>
#include <vector>

namespace factor::cpu::stat {

using factor::stat::Batch;
using factor::stat::HoldStat;
using factor::stat::Labels;
using factor::stat::Mode;
using factor::stat::Plane;
using factor::stat::Span;
using factor::stat::Sum;

// -----------------------------------------------------------------------------
// 行区间核 (单线程, 绝对 t; 下标同 [T][A] 布局)
// -----------------------------------------------------------------------------

inline void rank_x_rows(Plane x, const uint8_t *g, Frame f, int d, int t0, int t1, int A, uint16_t *rx, Scratch &sc) {
  for (int t = t0; t < t1; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    rank_x_row(KeyX{x.v + base, x.m + base, g + base}, f, d, A, rx + base, sc);
  }
}

inline void rank_y_rows(const Label &lb, int t0, int t1, int A, uint16_t *ry, Scratch &sc) {
  for (int t = t0; t < t1; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    rank_row(KeyY{lb.lv + base, lb.m + base}, A, ry + base, sc);
  }
}

// -----------------------------------------------------------------------------
// 流程 (Exec 出线程, 见 misc/exec.hpp)
// -----------------------------------------------------------------------------

// 调用方持有; ensure 幂等 (形状对得上零分配)
struct Workspace {
  std::vector<Scratch> sc; // [tid]
  int A = 0, threads = 0;
  void ensure(int A_, int threads_) {
    assert(A_ >= 1 && A_ <= kMaxA && threads_ >= 1);
    if (sc.size() < static_cast<size_t>(threads_))
      sc.resize(static_cast<size_t>(threads_));
    for (Scratch &s : sc)
      if (s.a.size() != static_cast<size_t>(A_))
        s.reserve(A_);
    A = A_, threads = threads_;
  }
};

// sink 要不要真持有格 (Sink::kCells; 不声明 = 不要)
template <class S>
inline constexpr bool kSinkCells = [] {
  if constexpr (requires { S::kCells; })
    return S::kCells;
  else
    return false;
}();
// 持有期 i 的格回调转给 sink.cell
template <class S>
struct SinkCells {
  S &s;
  int i;
  static constexpr bool on = kSinkCells<S>;
  void cell(int t, int a, int g, const factor::stat::Pos &p, const float *y) const { s.cell(i, t, a, g, p, y); }
};

// sink 的两个常用件: 丢弃 / 落地 rows[i·T + t] (rows = Row[H][T])
struct DropRows {
  void row(int, int, const Row &) const {}
};
struct LandRows {
  Row *rows = nullptr;
  int T = 0;
  void row(int i, int t, const Row &r) const { rows[static_cast<size_t>(i) * T + t] = r; }
};

// 标签预处理: 一组标签的做多侧 (lb.lv / lb.m) → ry [T][A]. 常驻期算一次 (用 w.sc)
template <class Exec>
inline void prep_label(Exec &ex, const Label &lb, const Span &s, uint16_t *ry, Workspace &w) {
  assert(lb.lv && lb.m && ry && w.A == s.A && w.threads >= ex.threads() && "Workspace 未 ensure 到位");
  ex.rows(s.T, [&](int t0, int t1, int tid) { rank_y_rows(lb, t0, t1, s.A, ry, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 1: r16(x) 按口径 m.f / 方向 m.d → rx [T][A] (块内全部行, 含前后带)
template <class Exec>
inline void rank_x(Exec &ex, Plane x, const uint8_t *g, const Mode &m, const Span &s, uint16_t *rx, Workspace &w) {
  assert(s.T >= 1 && s.A >= 1 && s.A <= kMaxA);
  assert(x.v && x.m && g && rx && w.A == s.A && w.threads >= ex.threads());
  ex.rows(s.T, [&](int t0, int t1, int tid) { rank_x_rows(x, g, m.f, m.d, t0, t1, s.A, rx, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 2: rx × L 组标签 → 统计行 [t_lo, t_hi) 的 Row (绝对), 每行先 sink.row 再按收益口径 m.r 折进 day[i·D + 日] (覆盖; D = s.days(); 一日一线程, 行序累加)
template <class Exec, class Sink>
inline void accum(Exec &ex, const Span &s, const uint16_t *rx, const Labels &L, const Mode &m, Sum *day, Sink &&sink) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, true);
  const int H = L.hd.n, A = s.A, S = s.g.S, D = s.days();
  assert(rx && day);
  ex.rows(D, [&](int d0, int d1, int) {
    for (int dd = d0; dd < d1; ++dd) {
      for (int i = 0; i < H; ++i)
        day[static_cast<size_t>(i) * D + dd] = Sum{};
      for (int t = s.t_lo + dd * S; t < s.t_lo + (dd + 1) * S; ++t) {
        const size_t base = static_cast<size_t>(t) * A;
        for (int i = 0; i < H; ++i) {
          const Hold hd = hold_on(L.hd.k[i], s.g);
          const Row r = row_stat(rx + base, t >= hd.h ? rx + base - static_cast<size_t>(hd.h) * A : nullptr, L.l[i].at(base), L.eo + base,
                                 factor::stat::chain_of(t, s, hd), hd.opens(t), SinkCells<std::remove_reference_t<Sink>>{sink, i});
          sink.row(i, t, r);
          day[static_cast<size_t>(i) * D + dd].add(r, m.r);
        }
      }
    }
  });
}

// 一个候选的完整评估 = rank_x (→ rx) + accum → day[hd.n][D]
template <class Exec, class Sink>
inline void run(Exec &ex, Plane x, const uint8_t *g, const Mode &m, const Span &s, const Labels &L, uint16_t *rx, Workspace &w, Sum *day, Sink &&sink) {
  rank_x(ex, x, g, m, s, rx, w);
  accum(ex, s, rx, L, m, day, sink);
}

// 一批候选 (同口径 m): B.x[c] 各一张 [T][A] (块形) → day[(c·H + i)·D + 日]; rx = r16 工作平面 (候选间复用, 跑完是最后一个候选的秩)
template <class Exec>
inline void run_batch(Exec &ex, const Batch &B, const uint8_t *g, const Mode &m, const Span &s, const Labels &L, uint16_t *rx, Workspace &w, Sum *day) {
  assert(B.C >= 1 && g && rx && day);
  const size_t H = static_cast<size_t>(L.hd.n), D = static_cast<size_t>(s.days());
  for (int c = 0; c < B.C; ++c)
    run(ex, B.x[c], g, m, s, L, rx, w, day + c * H * D, DropRows{});
}

} // namespace factor::cpu::stat
