#pragma once

// =============================================================================
// Stat 评估算子的 CPU 后端 (factor::cpu::stat; 语义契约见 factor/Stat/Contract.hpp, 行核见 Stat/Host.hpp)
// =============================================================================
//   定位: 挖掘无 GPU 时的评估路径, 与 GPU 后端对拍 (无 golden, 两边必须 match). 不 include Gpu.cuh.
//
//   【分层】算子 = 行核 (Host.hpp) + 流程 (本文件), 无线程 / 无分配 / 无所有权; 谁出线程由 Exec 定 (factor/Exec.hpp):
//     rank_x_rows / rank_y_rows   行区间核 [t0,t1) (单线程, 绝对 t)
//     prep_label    ry[T][A]                                     ← exec.rows(rank_y_rows)
//     rank_x        rx[T][A]                                     ← exec.rows(rank_x_rows)
//     accum_rows    rx × 标签集 → 行 [t_lo,T) 的 Row 折进 Accum[H]  ← exec.rows(每 t 每 h row_stat → on_row → Accum[tid]) → merge
//                   块语义 (Search 契约【分块流式】): t0_abs 只给 tail_masked; 块内 t − h < 0 → 无 AC; [0,t_lo) 是 carry 只给 lag 行
//                   on_row(i, t, r) 每行回调 (线程内, 行不相交): drop_rows 丢弃 / land_rows 落地 rows[i·T + t] / 调用方自定 (Inspect 顺带尾部)
//     eval_rows     = accum_rows(整张量) + finish → HoldStat[H]
//     run           = rank_x + eval_rows (一个标签集的完整评估)
//     cost_rows     rx × C 个金额档 → CostAcc[C][H]               ← exec.rows(每 t 一次 cost_row → CostAcc[tid]) → merge
//     Search/Cpu.hpp 对每个候选走 rank_x + accum_rows, 不另写流程.
//   r16(x) 平面 rx 由调用方持有 (与 GPU 同); Workspace (Scratch[tid] / Accum[tid][H] / CostAcc[tid][C][H]) 由调用方持有,
//   ensure 幂等 → 稳态零分配; 搜索每 worker 一份, 交互一份常驻.
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

// -----------------------------------------------------------------------------
// 行区间核 (单线程, 绝对 t; 下标同 [T][A] 布局)
// -----------------------------------------------------------------------------

inline void rank_x_rows(const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int t0, int t1, int A, uint16_t *rx, Scratch &sc) {
  for (int t = t0; t < t1; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    rank_x_row(KeyX{xv + base, xm + base, g + base}, f, A, rx + base, sc);
  }
}

inline void rank_y_rows(const uint16_t *lv, const uint8_t *m, int t0, int t1, int A, uint16_t *ry, Scratch &sc) {
  for (int t = t0; t < t1; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    rank_row(KeyY{lv + base, m + base}, A, ry + base, sc);
  }
}

// -----------------------------------------------------------------------------
// 流程 (Exec 出线程, 见 factor/Exec.hpp)
// -----------------------------------------------------------------------------

// 调用方持有; ensure 幂等 (形状对得上零分配). C = 金额档数 (只 cost_rows 用; 不算成本给 0)
struct Workspace {
  std::vector<Scratch> sc;   // [tid]
  std::vector<Accum> acc;    // [tid][H]
  std::vector<CostAcc> cacc; // [tid][C][H]
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
    A = A_, H = H_, C = C_, threads = threads_;
  }
};

// on_row 的两个常用件: 丢弃 / 落地 rows[i·T + t] (rows = Row[H][T])
inline auto drop_rows() {
  return [](int, int, const Row &) {};
}
inline auto land_rows(Row *rows, int T) {
  assert(rows && T >= 1);
  return [rows, T](int i, int t, const Row &r) { rows[static_cast<size_t>(i) * T + t] = r; };
}

// 标签预处理: 做多标签 → ry [T][A]. 常驻期算一次 (用 w.sc)
template <class Exec>
inline void prep_label(Exec &ex, const uint16_t *lv, const uint8_t *m, int T, int A, uint16_t *ry, Workspace &w) {
  assert(w.A == A && w.threads >= ex.threads() && "Workspace 未 ensure 到位");
  ex.rows(T, [&](int t0, int t1, int tid) { rank_y_rows(lv, m, t0, t1, A, ry, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 1: r16(x) → rx [T][A]
template <class Exec>
inline void rank_x(Exec &ex, const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int T, int A, uint16_t *rx, Workspace &w) {
  assert(g && rx && T >= 1 && static_cast<long long>(T) * A < (1LL << 31) && w.A == A && w.threads >= ex.threads());
  ex.rows(T, [&](int t0, int t1, int tid) { rank_x_rows(xv, xm, g, f, t0, t1, A, rx, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 2: rx × hd.n 组标签 → 行 [t_lo,T) 的 Row, 每行先 on_row(i, t, r) 再折进 acc[hd.n] (覆盖)
template <class Exec, class OnRow>
inline void accum_rows(Exec &ex, Frame f, int T, int A, int t0_abs, int t_lo, const uint16_t *rx, const Holds &hd, const Label *lab, Workspace &w,
                       Accum *acc, OnRow &&on_row) {
  assert(rx && w.A == A && w.H >= hd.n && w.threads >= ex.threads() && acc);
  assert(t0_abs >= 0 && t_lo >= 0 && t_lo < T && static_cast<long long>(T) * A < (1LL << 31));
  factor::stat::assert_holds(hd);
  const int H = hd.n;
  for (int i = 0; i < H; ++i)
    assert(lab[i].v[kLong] && lab[i].v[kShort] && lab[i].m && lab[i].ry && "标签未预处理 (prep_label)");
  const int nt = ex.threads();
  std::fill(w.acc.begin(), w.acc.begin() + static_cast<size_t>(nt) * H, Accum{});
  ex.rows(T - t_lo, [&](int r0, int r1, int tid) {
    Accum *a = w.acc.data() + static_cast<size_t>(tid) * H;
    for (int t = t_lo + r0; t < t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      for (int i = 0; i < H; ++i) {
        const int hold = hd.h[i], h = factor::stat::hold_minutes(hold);
        const Row r = row_stat(f, rx + base, t >= h ? rx + static_cast<size_t>(t - h) * A : nullptr, lab[i].at(base), A,
                               factor::stat::tail_masked(t0_abs + t, hold));
        on_row(i, t, r);
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

// 阶段 2 + 二级 (整张量, 绝对 t): out[hd.n]; 行经 on_row
template <class Exec, class OnRow>
inline void eval_rows(Exec &ex, Frame f, int T, int A, const uint16_t *rx, const Holds &hd, const Label *lab, Workspace &w, HoldStat *out,
                      OnRow &&on_row) {
  assert(out);
  Accum acc[kMaxHold];
  accum_rows(ex, f, T, A, 0, 0, rx, hd, lab, w, acc, on_row);
  for (int i = 0; i < hd.n; ++i)
    out[i] = acc[i].finish(hd.h[i]);
}

// 一个标签集的完整评估 = rank_x (→ rx) + eval_rows
template <class Exec, class OnRow>
inline void run(Exec &ex, const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int T, int A, const Holds &hd, const Label *lab,
                uint16_t *rx, Workspace &w, HoldStat *out, OnRow &&on_row) {
  rank_x(ex, xv, xm, g, f, T, A, rx, w);
  eval_rows(ex, f, T, A, rx, hd, lab, w, out, on_row);
}

// 换手成本 (契约【换手成本】): rx × c[C] (整张量平面, 未偏) → out[k·H + i] (覆盖). 每 t 一次 cost_row (H × C 一遍 A 轴)
template <class Exec>
inline void cost_rows(Exec &ex, int T, int A, const uint16_t *rx, const Holds &hd, const Cost *c, int C, Workspace &w, CostAcc *out) {
  assert(rx && c && out && C >= 1 && C <= kMaxCost && w.A == A && w.H >= hd.n && w.C >= C && w.threads >= ex.threads());
  factor::stat::assert_holds(hd);
  const int H = hd.n, nt = ex.threads();
  const size_t per = static_cast<size_t>(C) * H;
  std::fill(w.cacc.begin(), w.cacc.begin() + static_cast<size_t>(nt) * per, CostAcc{});
  ex.rows(T, [&](int t0, int t1, int tid) {
    CostAcc *acc = w.cacc.data() + static_cast<size_t>(tid) * per;
    for (int t = t0; t < t1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      const uint16_t *rlag[kMaxHold];
      for (int i = 0; i < H; ++i) {
        const int h = factor::stat::hold_minutes(hd.h[i]);
        rlag[i] = t >= h ? rx + base - static_cast<size_t>(h) * A : nullptr;
      }
      Cost cr[kMaxCost];
      for (int k = 0; k < C; ++k)
        cr[k] = c[k].at(base);
      cost_row(rx + base, rlag, H, cr, C, A, acc);
    }
  });
  for (size_t j = 0; j < per; ++j) {
    out[j] = w.cacc[j];
    for (int tid = 1; tid < nt; ++tid)
      out[j].merge(w.cacc[static_cast<size_t>(tid) * per + j]);
  }
}

} // namespace factor::cpu::stat
