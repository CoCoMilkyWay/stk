#pragma once

// =============================================================================
// Stat 评估算子的 CPU 后端 (factor::cpu::stat; 语义契约见 factor/Stat/Contract.hpp, 行核见 Stat/Host.hpp)
// =============================================================================
//   定位: 挖掘无 GPU 时的评估路径, 与 GPU 后端对拍 (无 golden, 两边必须 match). 不 include Gpu.cuh.
//
//   【分层】算子 = 行核 (Host.hpp) + 流程 (本文件), 无线程 / 无分配 / 无所有权; 谁出线程由 Exec 定 (factor/Exec.hpp):
//     rank_x_rows / rank_y_rows   行区间核 [t0,t1) (单线程, 绝对 t)
//     prep_label    ry[T][A]                                     ← exec.rows(rank_y_rows)
//     rank_x        ws[T][A]                                     ← exec.rows(rank_x_rows)
//     accum_rows    ws × 标签集 → 行 [t_lo,T) 的 Row (可落地) 折进 Accum[H]  ← exec.rows(每 t 每 h row_stat → Accum[tid]) → merge
//                   块语义 (Search 契约【分块流式】): t0_abs 只给 tail_masked; 块内 t − h < 0 → 无 AC; [0,t_lo) 是 carry 只给 lag 行
//     eval_rows     = accum_rows(整张量) + finish → Row[H][T] + HoldStat[H]
//     run           = rank_x + eval_rows (一个标签集的完整评估)
//     Search/Cpu.hpp 对每个候选走 rank_x + accum_rows (+ 成本), 不另写行核.
//   Workspace 由调用方持有 (ws / Scratch[tid] / Accum[tid][H]), ensure 幂等 → 稳态零分配; 搜索每 worker 一份, 交互一份常驻.
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
using factor::stat::kMaxHold;

// -----------------------------------------------------------------------------
// 行区间核 (单线程, 绝对 t; 下标同 [T][A] 布局)
// -----------------------------------------------------------------------------

inline void rank_x_rows(const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int t0, int t1, int A, uint16_t *ws, Scratch &sc) {
  for (int t = t0; t < t1; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    rank_x_row(xv + base, xm + base, g + base, f, A, ws + base, sc);
  }
}

inline void rank_y_rows(const uint16_t *lv, const uint8_t *m, int t0, int t1, int A, uint16_t *ry, Scratch &sc) {
  for (int t = t0; t < t1; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    rank_y_row(lv + base, m + base, A, ry + base, sc);
  }
}

// -----------------------------------------------------------------------------
// 流程 (Exec 出线程, 见 factor/Exec.hpp)
// -----------------------------------------------------------------------------

// 调用方持有; ensure 幂等 (形状对得上零分配)
struct Workspace {
  std::vector<uint16_t> ws; // [T][A] r16(x)
  std::vector<Scratch> sc;  // [tid]
  std::vector<Accum> acc;   // [tid][H]
  int T = 0, A = 0, H = 0, threads = 0;
  void ensure(int T_, int A_, int H_, int threads_) {
    assert(T_ >= 1 && A_ >= 1 && A_ <= kMaxA && static_cast<long long>(T_) * A_ < (1LL << 31));
    assert(H_ >= 1 && H_ <= kMaxHold && threads_ >= 1);
    if (ws.size() != static_cast<size_t>(T_) * A_)
      ws.resize(static_cast<size_t>(T_) * A_);
    if (sc.size() < static_cast<size_t>(threads_))
      sc.resize(static_cast<size_t>(threads_));
    for (Scratch &s : sc)
      if (s.a.size() != static_cast<size_t>(A_))
        s.reserve(A_);
    if (acc.size() < static_cast<size_t>(threads_) * H_)
      acc.resize(static_cast<size_t>(threads_) * H_);
    T = T_, A = A_, H = H_, threads = threads_;
  }
};

// 标签预处理: 做多标签 → ry [T][A]. 常驻期算一次 (用 w.sc)
template <class Exec>
inline void prep_label(Exec &ex, const uint16_t *lv, const uint8_t *m, int T, int A, uint16_t *ry, Workspace &w) {
  assert(w.A == A && w.threads >= ex.threads() && "Workspace 未 ensure 到位");
  ex.rows(T, [&](int t0, int t1, int tid) { rank_y_rows(lv, m, t0, t1, A, ry, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 1: r16(x) → w.ws
template <class Exec>
inline void rank_x(Exec &ex, const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int T, int A, Workspace &w) {
  assert(g && w.T == T && w.A == A && w.threads >= ex.threads());
  ex.rows(T, [&](int t0, int t1, int tid) { rank_x_rows(xv, xm, g, f, t0, t1, A, w.ws.data(), w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 2: w.ws × hd.n 组标签 → 行 [t_lo,T) 的 Row (rows 非空则落地 rows[i·T + t]) 折进 acc[hd.n] (覆盖)
template <class Exec>
inline void accum_rows(Exec &ex, Frame f, int T, int A, int t0_abs, int t_lo, const Holds &hd, const Label *lab, Workspace &w, Row *rows,
                       Accum *acc) {
  assert(w.T == T && w.A == A && w.H >= hd.n && w.threads >= ex.threads() && acc);
  assert(t0_abs >= 0 && t_lo >= 0 && t_lo < T);
  factor::stat::assert_holds(hd);
  const int H = hd.n;
  for (int i = 0; i < H; ++i)
    assert(lab[i].lv && lab[i].sv && lab[i].m && lab[i].ry && "标签未预处理 (prep_label)");
  const int nt = ex.threads();
  std::fill(w.acc.begin(), w.acc.begin() + static_cast<size_t>(nt) * H, Accum{});
  const uint16_t *ws = w.ws.data();
  ex.rows(T - t_lo, [&](int r0, int r1, int tid) {
    Accum *a = w.acc.data() + static_cast<size_t>(tid) * H;
    for (int t = t_lo + r0; t < t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      const uint16_t *rx = ws + base;
      for (int i = 0; i < H; ++i) {
        const int hold = hd.h[i], h = factor::stat::hold_minutes(hold);
        const Row r = row_stat(f, rx, t >= h ? ws + static_cast<size_t>(t - h) * A : nullptr, lab[i].at(base), A,
                               factor::stat::tail_masked(t0_abs + t, hold));
        if (rows)
          rows[static_cast<size_t>(i) * T + t] = r;
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

// 阶段 2 + 二级 (整张量, 绝对 t): rows[hd.n][T] (nullptr = 不落地) + out[hd.n]
template <class Exec>
inline void eval_rows(Exec &ex, Frame f, int T, int A, const Holds &hd, const Label *lab, Workspace &w, Row *rows, HoldStat *out) {
  assert(out);
  Accum acc[kMaxHold];
  accum_rows(ex, f, T, A, 0, 0, hd, lab, w, rows, acc);
  for (int i = 0; i < hd.n; ++i)
    out[i] = acc[i].finish(hd.h[i]);
}

// 一个标签集的完整评估 = rank_x + eval_rows
template <class Exec>
inline void run(Exec &ex, const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int T, int A, const Holds &hd, const Label *lab,
                Workspace &w, Row *rows, HoldStat *out) {
  rank_x(ex, xv, xm, g, f, T, A, w);
  eval_rows(ex, f, T, A, hd, lab, w, rows, out);
}

} // namespace factor::cpu::stat
