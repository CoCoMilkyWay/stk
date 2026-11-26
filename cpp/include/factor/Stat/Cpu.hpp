#pragma once

// =============================================================================
// Stat 评估算子的 CPU 后端 (factor::cpu::stat; 语义契约见 factor/Stat/Contract.hpp, 行核见 Stat/Host.hpp)
// =============================================================================
//   定位: 无 GPU 时的评估路径, 与 GPU 后端对拍 (无 golden, 两边必须 match). 不 include Gpu.cuh.
//   【分层】算子 = 行核 (Host.hpp) + 流程 (本文件), 无线程 / 无分配 / 无所有权; 谁出线程由 Exec 定 (misc/exec.hpp).
//   与 Stat/Gpu.cuh 同名同参序 (契约【视图】Plane / Batch / Labels / Span; 出口 Accum[H] + Day[H][D]):
//     rank_x_rows / rank_y_rows   行区间核 [t0,t1) (单线程, 绝对 t)
//     prep_label    ry[T][A]                                     ← exec.rows(rank_y_rows)
//     rank_x        rx[T][A] (已定向)                            ← exec.rows(rank_x_rows)
//     accum_rows    rx × 标签集 → 统计日 [0, D) 的 Row 折进 Accum[H] + Day[H][D]  ← exec.rows(按日切: 每 t 每 h row_stat → sink → Accum[tid]) → merge
//                   sink 每行一次 (线程内, 行不相交): DropRows 丢弃 / LandRows 落地 rows[i·T + t] / 调用方自定
//     run           = rank_x + accum_rows (一个候选的完整评估); HoldStat 由契约 finish_all
//     run_batch     一批候选: 候选串行, 候选内全核切日 (C 小也能跑满核, 只留一张 [T][A] 的 r16)
//     turn_rows     rx × 标签 × C ≥ 1 个金额段 → 因子账集换手 CostAcc[C][H][kLevels]  ← exec.rows(每 t 每 h 一次 factor_slots + turn_row → [tid]) → merge
//   标签秩 ry / r16(x) 平面 rx 由调用方持有; Workspace (Scratch / Accum / CostAcc / TurnWork 各 [tid]) 由调用方持有, ensure 幂等.
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

using factor::stat::Accum;
using factor::stat::Batch;
using factor::stat::CostAcc;
using factor::stat::Day;
using factor::stat::Hold;
using factor::stat::hold_on;
using factor::stat::HoldId;
using factor::stat::Holds;
using factor::stat::HoldStat;
using factor::stat::kAmtCount;
using factor::stat::kHoldCount;
using factor::stat::Labels;
using factor::stat::Plane;
using factor::stat::Span;

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

// 调用方持有; ensure 幂等 (形状对得上零分配). C = 金额段数 (只 turn_rows 用; 不算换手给 0)
struct Workspace {
  std::vector<Scratch> sc;   // [tid]
  std::vector<Accum> acc;    // [tid][H]
  std::vector<CostAcc> cacc; // [tid][C][H][kLevels]
  std::vector<TurnWork> tw;  // [tid]
  int A = 0, H = 0, C = 0, threads = 0;
  void ensure(int A_, int H_, int C_, int threads_) {
    assert(A_ >= 1 && A_ <= kMaxA && H_ >= 1 && H_ <= kHoldCount && C_ >= 0 && threads_ >= 1);
    if (sc.size() < static_cast<size_t>(threads_))
      sc.resize(static_cast<size_t>(threads_));
    for (Scratch &s : sc)
      if (s.a.size() != static_cast<size_t>(A_))
        s.reserve(A_);
    if (acc.size() < static_cast<size_t>(threads_) * H_)
      acc.resize(static_cast<size_t>(threads_) * H_);
    if (cacc.size() < static_cast<size_t>(threads_) * C_ * H_ * kLevels)
      cacc.resize(static_cast<size_t>(threads_) * C_ * H_ * kLevels);
    if (tw.size() < static_cast<size_t>(threads_))
      tw.resize(static_cast<size_t>(threads_));
    A = A_, H = H_, C = C_, threads = threads_;
  }
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

// 阶段 1: r16(x) 按方向 d → rx [T][A] (块内全部行, 含前后带)
template <class Exec>
inline void rank_x(Exec &ex, Plane x, const uint8_t *g, Frame f, int d, const Span &s, uint16_t *rx, Workspace &w) {
  assert(s.T >= 1 && s.A >= 1 && s.A <= kMaxA);
  assert(x.v && x.m && g && rx && w.A == s.A && w.threads >= ex.threads());
  ex.rows(s.T, [&](int t0, int t1, int tid) { rank_x_rows(x, g, f, d, t0, t1, s.A, rx, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 2: rx × L 组标签 → 统计行 [t_lo, t_hi) 的 Row, 每行先 sink.row 再折进 acc[hd.n] (覆盖) 与 day[i·D + 日] (覆盖; D = s.days())
template <class Exec, class Sink>
inline void accum_rows(Exec &ex, const Span &s, const uint16_t *rx, const Labels &L, Workspace &w, Accum *acc, Day *day, const Sink &sink) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, true);
  const int H = L.hd.n, A = s.A, nt = ex.threads(), S = s.g.S, D = s.days();
  assert(rx && acc && day && w.A == A && w.H >= H && w.threads >= nt);
  std::fill(w.acc.begin(), w.acc.begin() + static_cast<size_t>(nt) * H, Accum{});
  ex.rows(D, [&](int d0, int d1, int tid) {
    Accum *a = w.acc.data() + static_cast<size_t>(tid) * H;
    for (int dd = d0; dd < d1; ++dd) {
      for (int i = 0; i < H; ++i)
        day[static_cast<size_t>(i) * D + dd] = Day{};
      for (int t = s.t_lo + dd * S; t < s.t_lo + (dd + 1) * S; ++t) {
        const size_t base = static_cast<size_t>(t) * A;
        for (int i = 0; i < H; ++i) {
          const Hold hd = hold_on(L.hd.k[i], s.g);
          const Row r = row_stat(rx + base, t >= hd.h ? rx + base - static_cast<size_t>(hd.h) * A : nullptr, L.l[i].at(base), factor::stat::chain_of(t, s, hd), L.cap,
                                 hd.opens(s.t0_abs + t));
          sink.row(i, t, r);
          a[i].add(r);
          day[static_cast<size_t>(i) * D + dd].add(r);
        }
      }
    }
  });
  for (int i = 0; i < H; ++i) {
    acc[i] = w.acc[static_cast<size_t>(i)];
    for (int tid = 1; tid < nt; ++tid)
      acc[i].merge(w.acc[static_cast<size_t>(tid) * H + i]);
  }
}

// 一个候选的完整评估 = rank_x (→ rx) + accum_rows → acc[hd.n] + day[hd.n][D]
template <class Exec, class Sink>
inline void run(Exec &ex, Plane x, const uint8_t *g, Frame f, int d, const Span &s, const Labels &L, uint16_t *rx, Workspace &w, Accum *acc, Day *day,
                const Sink &sink) {
  rank_x(ex, x, g, f, d, s, rx, w);
  accum_rows(ex, s, rx, L, w, acc, day, sink);
}

// 一批候选 (同方向 d): B.x[c] 各一张 [T][A] (块形) → acc[c·H + i] + day[(c·H + i)·D + 日]; rx = r16 工作平面 (候选间复用, 跑完是最后一个候选的秩)
template <class Exec>
inline void run_batch(Exec &ex, const Batch &B, const uint8_t *g, Frame f, int d, const Span &s, const Labels &L, uint16_t *rx, Workspace &w, Accum *acc,
                      Day *day) {
  assert(B.C >= 1 && g && rx && acc && day);
  const size_t H = static_cast<size_t>(L.hd.n), D = static_cast<size_t>(s.days());
  for (int c = 0; c < B.C; ++c)
    run(ex, B.x[c], g, f, d, s, L, rx, w, acc + c * H, day + c * H * D, DropRows{});
}

// 因子账集的换手 (契约 Turn.hpp; 本 b = e · kLevels + l): rx (已定向) × L 的标签 / 可成交码 × c[C] (块形平面, 未偏)
//   → out[(k·H + i)·kLevels + l] (CostAcc 两端; 覆盖). 统计行 [t_lo, t_hi), 开仓行 (Hold::opens) 才攒; 交接 Hold::nets(t) → 只交易差额;
//   本批平仓不交接 (nets(t + h) 假) → settle. 满仓 = L.cap · level_frac(l). land 非 NoLand → 另逐行 land(k, i, t, ret[kBooks], row[kBooks])
//   (本行该金额段的本批收益与两侧流水; 线程内, 行不相交)
inline constexpr int kBooks = kEnds * kLevels;
struct NoLand {
  void operator()(int, int, int, const double *, const Turn *) const {}
};
template <class Exec, class Land = NoLand>
inline void turn_rows(Exec &ex, const uint16_t *rx, const Span &s, const Labels &L, const Cost *c, int C, Workspace &w, CostAcc *out, const Land &land = {}) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, false);
  const int H = L.hd.n, A = s.A, nt = ex.threads();
  assert(rx && c && out && C >= 1 && C <= kAmtCount && w.A == A && w.H >= H && w.C >= C && w.threads >= nt);
  const size_t per = static_cast<size_t>(C) * H * kLevels;
  std::fill(w.cacc.begin(), w.cacc.begin() + static_cast<size_t>(nt) * per, CostAcc{});
  double cap[kBooks];
  for (int b = 0; b < kBooks; ++b)
    cap[b] = L.cap * factor::stat::level_frac(b % kLevels);
  ex.rows(s.t_hi() - s.t_lo, [&](int r0, int r1, int tid) {
    CostAcc *acc = w.cacc.data() + static_cast<size_t>(tid) * per;
    TurnWork &tw = w.tw[static_cast<size_t>(tid)];
    for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      const int ta = s.t0_abs + t;
      Cost cr[kAmtCount];
      for (int k = 0; k < C; ++k)
        cr[k] = c[k].at(base);
      for (int i = 0; i < H; ++i) {
        const Hold hd = hold_on(L.hd.k[i], s.g);
        if (!hd.opens(ta))
          continue;
        const Label lb = L.l[i].at(base);
        const factor::stat::Chain ch = factor::stat::chain_of(t, s, hd);
        factor_slots(rx + base, lb, ch, ch.nets(t), tw);
        double ret[kBooks] = {};
        Turn row[static_cast<size_t>(kAmtCount) * kBooks] = {};
        turn_row(tw.cur, tw.pre, !hd.nets(ta + hd.h), lb.eo, cr, C, cap, tw, ret, row);
        for (int k = 0; k < C; ++k) {
          for (int b = 0; b < kBooks; ++b)
            acc[(static_cast<size_t>(k) * H + i) * kLevels + b % kLevels].book[b / kLevels].merge(row[k * kBooks + b]);
          if constexpr (!std::is_same_v<Land, NoLand>)
            land(k, i, t, ret, row + k * kBooks);
        }
      }
    }
  });
  for (size_t j = 0; j < per; ++j) {
    out[j] = w.cacc[j];
    for (int tid = 1; tid < nt; ++tid)
      out[j].merge(w.cacc[static_cast<size_t>(tid) * per + j]);
  }
}

} // namespace factor::cpu::stat
