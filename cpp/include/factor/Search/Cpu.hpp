#pragma once

// =============================================================================
// Search 评估算子的 CPU 后端 (factor::cpu::search; 契约 factor/Search/Contract.hpp + Stat/Contract.hpp)
// =============================================================================
//   定位: 无 GPU 时的参数搜索路径. 行核全部复用 Stat CPU (Stat/Host.hpp: rank_x_row / row_stat), 本文件只加:
//     eval_rows   行 [t_lo,T) × H 一遍: row_stat → Accum[tid][h], 末尾 merge. 块语义 t0_abs / t_lo 同 Stat accum_rows; 块内 t − h < 0 → 无 AC
//     run         一批候选: 候选串行, 候选内全核切行 (rank_x → eval_rows). 候选内并行而不是候选间: C 小 (局部网格几个点) 也能跑满核,
//                 且只留一张 [T][A] 的 r16, 不留 ring / 每线程平面.
//   【与 Stat CPU 的关系】同一行核 ⇒ 整数派生量 / 浮点和都逐位同 Stat CPU; 对拍 (Search/Check.hpp) 验的是块语义 / 累加.
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Search/Contract.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>

namespace factor::cpu::search {

using factor::search::Result;
using factor::stat::Accum;
using factor::stat::Frame;
using factor::stat::Holds;
using factor::stat::kGroups;
using factor::stat::kRankNone;
using factor::stat::Label;
using factor::stat::Row;

// 调用方持有: = Stat 的工作区 (r16 平面 + 线程私有 Accum [tid][H]); ensure 幂等
struct Workspace {
  factor::cpu::stat::Workspace st;
  void ensure(int T, int A, int H, int threads) { st.ensure(T, A, H, threads); }
};

// 行 [t_lo,T) × hd.n: w.st.ws (本候选 r16) × 标签集 → out (覆盖)
template <class Exec>
inline void eval_rows(Exec &ex, Frame f, int T, int A, int t0_abs, int t_lo, const Holds &hd, const Label *lab, Workspace &w, Result &out) {
  assert(w.st.T == T && w.st.A == A && w.st.H >= hd.n && w.st.threads >= ex.threads());
  assert(t0_abs >= 0 && t_lo >= 0 && t_lo < T);
  factor::stat::assert_holds(hd);
  const int H = hd.n;
  for (int i = 0; i < H; ++i)
    assert(lab[i].lv && lab[i].sv && lab[i].m && lab[i].ry && "标签未预处理 (prep_label)");
  const int nt = ex.threads();
  std::fill(w.st.acc.begin(), w.st.acc.begin() + static_cast<size_t>(nt) * H, Accum{});
  const uint16_t *ws = w.st.ws.data();
  ex.rows(T - t_lo, [&](int r0, int r1, int tid) {
    Accum *a = w.st.acc.data() + static_cast<size_t>(tid) * H;
    for (int t = t_lo + r0; t < t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      const uint16_t *rx = ws + base;
      for (int i = 0; i < H; ++i) {
        const int hold = hd.h[i], h = factor::stat::hold_minutes(hold);
        const uint16_t *rlag = t >= h ? ws + static_cast<size_t>(t - h) * A : nullptr;
        a[i].add(factor::cpu::stat::row_stat(f, rx, rlag, lab[i].at(base), A, factor::stat::tail_masked(t0_abs + t, hold)));
      }
    }
  });
  out = Result{};
  for (int i = 0; i < H; ++i) {
    out.acc[i] = w.st.acc[static_cast<size_t>(i)];
    for (int tid = 1; tid < nt; ++tid)
      out.acc[i].merge(w.st.acc[static_cast<size_t>(tid) * H + i]);
  }
}

// 一批候选: xv[c] / xm[c] 各一张 [T][A] (块形), 统计 [t_lo,T), 绝对首行 t0_abs (契约【分块流式】) → out[c] (覆盖)
template <class Exec>
inline void run(Exec &ex, int C, const float *const *xv, const uint8_t *const *xm, const uint8_t *g, Frame f, int T, int A, int t0_abs,
                int t_lo, const Holds &hd, const Label *lab, Workspace &w, Result *out) {
  assert(C >= 1 && xv && xm && g && lab && out);
  for (int c = 0; c < C; ++c) {
    factor::cpu::stat::rank_x(ex, xv[c], xm[c], g, f, T, A, w.st);
    eval_rows(ex, f, T, A, t0_abs, t_lo, hd, lab, w, out[c]);
  }
}

} // namespace factor::cpu::search
