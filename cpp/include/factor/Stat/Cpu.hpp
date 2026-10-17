#pragma once

// =============================================================================
// Stat 评估算子的 CPU 后端 (factor::cpu::stat; 语义契约见 factor/Stat/Contract.hpp, 行核见 Stat/Host.hpp)
// =============================================================================
//   定位: 挖掘无 GPU 时的评估路径, 与 GPU 后端对拍 (无 golden, 两边必须 match). 不 include Gpu.cuh.
//
//   【分层】算子 = 行核 (Host.hpp) + 流程 (本文件), 无线程 / 无分配 / 无所有权; 谁出线程由 Exec 定 (misc/exec.hpp).
//   与 Stat/Gpu.cuh 同名同参序 (契约【视图】Plane / Batch / Labels / Span; 出口 Accum[H]):
//     rank_x_rows / rank_y_rows   行区间核 [t0,t1) (单线程, 绝对 t)
//     prep_label    ry[T][A]                                     ← exec.rows(rank_y_rows)
//     rank_x        rx[T][A]                                     ← exec.rows(rank_x_rows)
//     accum_rows    rx × 标签集 → 行 [t_lo,T) 的 Row 折进 Accum[H]  ← exec.rows(每 t 每 h row_stat → sink → Accum[tid]) → merge
//                   块语义 (Search 契约【分块流式】): t0_abs 只给 tail_masked; 块内 t − h < 0 → 无 AC; [0,t_lo) 是 carry 只给 lag 行
//                   sink 每行一次 (线程内, 行不相交): ext() 给本行 label_row 的逐格扩展, row(i, t, r, ext) 收行;
//                   DropRows 丢弃 / LandRows 落地 rows[i·T + t] / 调用方自定 (Inspect 尾部分层)
//     run           = rank_x + accum_rows (一个候选的完整评估); HoldStat 由契约 finish_all
//     leg_codes     rx → 腿码 lc[T][A] (契约【换手成本】)
//     cost_rows     lc × C 个金额段 → CostAcc[C][H] + cover[1 + C]  ← exec.rows(每 t 一次 cost_row → [tid]) → merge
//     Search/Cpu.hpp 对每个候选走 rank_x + accum_rows, 不另写流程.
//   r16(x) 平面 rx / 腿码 lc 由调用方持有 (与 GPU 同); Workspace (Scratch[tid] / Accum[tid][H] / CostAcc[tid][C][H] / cover[tid][1 + C])
//   由调用方持有, ensure 幂等 → 稳态零分配; 搜索每 worker 一份, 交互一份常驻.
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Host.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <vector>

namespace factor::cpu::stat {

using factor::stat::Accum;
using factor::stat::Holds;
using factor::stat::HoldStat;
using factor::stat::kMaxCost;
using factor::stat::kMaxHold;
using factor::stat::Labels;
using factor::stat::Plane;
using factor::stat::Span;

// -----------------------------------------------------------------------------
// 行区间核 (单线程, 绝对 t; 下标同 [T][A] 布局)
// -----------------------------------------------------------------------------

inline void rank_x_rows(Plane x, const uint8_t *g, Frame f, int t0, int t1, int A, uint16_t *rx, Scratch &sc) {
  for (int t = t0; t < t1; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    rank_x_row(KeyX{x.v + base, x.m + base, g + base}, f, A, rx + base, sc);
  }
}

inline void rank_y_rows(const Label &lb, int t0, int t1, int A, uint16_t *ry, Scratch &sc) {
  for (int t = t0; t < t1; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    rank_row(KeyY{lb.v[kLong] + base, lb.m + base}, A, ry + base, sc);
  }
}

// -----------------------------------------------------------------------------
// 流程 (Exec 出线程, 见 misc/exec.hpp)
// -----------------------------------------------------------------------------

// 调用方持有; ensure 幂等 (形状对得上零分配). C = 金额段数 (只 cost_rows 用; 不算成本给 0)
struct Workspace {
  std::vector<Scratch> sc;     // [tid]
  std::vector<Accum> acc;      // [tid][H]
  std::vector<CostAcc> cacc;   // [tid][C][H]
  std::vector<long long> ccov; // [tid][1 + C]
  int A = 0, H = 0, C = 0, threads = 0;
  void ensure(int A_, int H_, int C_, int threads_) {
    assert(A_ >= 1 && A_ <= kMaxA && H_ >= 1 && H_ <= kMaxHold && C_ >= 0 && threads_ >= 1);
    if (sc.size() < static_cast<size_t>(threads_))
      sc.resize(static_cast<size_t>(threads_));
    for (Scratch &s : sc)
      if (s.a.size() != static_cast<size_t>(A_))
        s.reserve(A_);
    if (acc.size() < static_cast<size_t>(threads_) * H_)
      acc.resize(static_cast<size_t>(threads_) * H_);
    if (cacc.size() < static_cast<size_t>(threads_) * C_ * H_)
      cacc.resize(static_cast<size_t>(threads_) * C_ * H_);
    if (ccov.size() < static_cast<size_t>(threads_) * (1 + C_))
      ccov.resize(static_cast<size_t>(threads_) * (1 + C_));
    A = A_, H = H_, C = C_, threads = threads_;
  }
};

// sink 的两个常用件 (见文件头 accum_rows): 丢弃 / 落地 rows[i·T + t] (rows = Row[H][T])
struct DropRows {
  using Ext = NoExt;
  Ext ext() const { return {}; }
  void row(int, int, const Row &, const Ext &) const {}
};
struct LandRows {
  Row *rows = nullptr;
  int T = 0;
  using Ext = NoExt;
  Ext ext() const { return {}; }
  void row(int i, int t, const Row &r, const Ext &) const { rows[static_cast<size_t>(i) * T + t] = r; }
};

// 标签预处理: 一组标签的做多腿 (lb.v[kLong] / lb.m) → ry [T][A]. 常驻期算一次 (用 w.sc)
template <class Exec>
inline void prep_label(Exec &ex, const Label &lb, const Span &s, uint16_t *ry, Workspace &w) {
  assert(lb.v[kLong] && lb.m && ry && w.A == s.A && w.threads >= ex.threads() && "Workspace 未 ensure 到位");
  ex.rows(s.T, [&](int t0, int t1, int tid) { rank_y_rows(lb, t0, t1, s.A, ry, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 1: r16(x) → rx [T][A] (块内全部行, 含 carry)
template <class Exec>
inline void rank_x(Exec &ex, Plane x, const uint8_t *g, Frame f, const Span &s, uint16_t *rx, Workspace &w) {
  factor::stat::assert_span(s);
  assert(x.v && x.m && g && rx && w.A == s.A && w.threads >= ex.threads());
  ex.rows(s.T, [&](int t0, int t1, int tid) { rank_x_rows(x, g, f, t0, t1, s.A, rx, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 2: rx × L 组标签 → 行 [t_lo,T) 的 Row, 每行先 sink.row 再折进 acc[hd.n] (覆盖)
template <class Exec, class Sink>
inline void accum_rows(Exec &ex, const Span &s, const uint16_t *rx, const Labels &L, Workspace &w, Accum *acc, const Sink &sink) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, true);
  const int H = L.hd.n, A = s.A, nt = ex.threads();
  assert(rx && acc && w.A == A && w.H >= H && w.threads >= nt);
  std::fill(w.acc.begin(), w.acc.begin() + static_cast<size_t>(nt) * H, Accum{});
  ex.rows(s.T - s.t_lo, [&](int r0, int r1, int tid) {
    Accum *a = w.acc.data() + static_cast<size_t>(tid) * H;
    for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      for (int i = 0; i < H; ++i) {
        const int hold = L.hd.h[i], h = factor::stat::hold_minutes(hold);
        typename Sink::Ext e = sink.ext();
        const Row r = row_stat(rx + base, t >= h ? rx + base - static_cast<size_t>(h) * A : nullptr, L.l[i].at(base), A, L.cap,
                               factor::stat::tail_masked(s.t0_abs + t, hold), e);
        sink.row(i, t, r, e);
        a[i].add(r);
      }
    }
  });
  for (int i = 0; i < H; ++i) {
    acc[i] = w.acc[static_cast<size_t>(i)];
    for (int tid = 1; tid < nt; ++tid)
      acc[i].merge(w.acc[static_cast<size_t>(tid) * H + i]);
  }
}

// 一个候选的完整评估 = rank_x (→ rx) + accum_rows → acc[hd.n]
template <class Exec, class Sink>
inline void run(Exec &ex, Plane x, const uint8_t *g, Frame f, const Span &s, const Labels &L, uint16_t *rx, Workspace &w, Accum *acc,
                const Sink &sink) {
  rank_x(ex, x, g, f, s, rx, w);
  accum_rows(ex, s, rx, L, w, acc, sink);
}

// 腿码: rx → lc [T][A] (契约 leg_code 逐格)
template <class Exec>
inline void leg_codes(Exec &ex, const uint16_t *rx, const Span &s, uint8_t *lc) {
  factor::stat::assert_span(s);
  assert(rx && lc);
  ex.rows(s.T, [&](int t0, int t1, int) {
    for (size_t i = static_cast<size_t>(t0) * s.A, e = static_cast<size_t>(t1) * s.A; i < e; ++i)
      lc[i] = factor::stat::leg_code(rx[i]);
  });
}

// 换手成本 + cover (契约【换手成本】): lc × c[C] (块形平面, 未偏) → out[k·H + i] (覆盖) + cov[1 + C] (覆盖; cov[0] = 有秩格数,
// cov[1 + k] = 其中成本 k 有效). 统计行 [t_lo,T) (同 accum_rows), 每 t 一次 cost_row (H × C 一遍 A 轴); cap = 满仓 (持仓加权)
template <class Exec>
inline void cost_rows(Exec &ex, const uint8_t *lc, const Span &s, const Holds &hd, const Cost *c, int C, double cap, Workspace &w, CostAcc *out,
                      long long *cov) {
  factor::stat::assert_span(s);
  factor::stat::assert_holds(hd);
  const int H = hd.n, A = s.A, nt = ex.threads();
  assert(lc && c && out && cov && C >= 1 && C <= kMaxCost && cap > 0.0 && w.A == A && w.H >= H && w.C >= C && w.threads >= nt);
  const size_t per = static_cast<size_t>(C) * H, pc = static_cast<size_t>(1 + C);
  std::fill(w.cacc.begin(), w.cacc.begin() + static_cast<size_t>(nt) * per, CostAcc{});
  std::fill(w.ccov.begin(), w.ccov.begin() + static_cast<size_t>(nt) * pc, 0LL);
  ex.rows(s.T - s.t_lo, [&](int r0, int r1, int tid) {
    CostAcc *acc = w.cacc.data() + static_cast<size_t>(tid) * per;
    long long *cv = w.ccov.data() + static_cast<size_t>(tid) * pc;
    for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      const uint8_t *lag[kMaxHold];
      for (int i = 0; i < H; ++i) {
        const int h = factor::stat::hold_minutes(hd.h[i]);
        lag[i] = t >= h ? lc + base - static_cast<size_t>(h) * A : nullptr;
      }
      Cost cr[kMaxCost];
      for (int k = 0; k < C; ++k)
        cr[k] = c[k].at(base);
      cost_row(lc + base, lag, H, cr, C, A, cap, acc, cv);
    }
  });
  for (size_t j = 0; j < per; ++j) {
    out[j] = w.cacc[j];
    for (int tid = 1; tid < nt; ++tid)
      out[j].merge(w.cacc[static_cast<size_t>(tid) * per + j]);
  }
  for (size_t j = 0; j < pc; ++j) {
    cov[j] = 0;
    for (int tid = 0; tid < nt; ++tid)
      cov[j] += w.ccov[static_cast<size_t>(tid) * pc + j];
  }
}

} // namespace factor::cpu::stat
