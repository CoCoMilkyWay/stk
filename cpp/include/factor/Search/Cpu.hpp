#pragma once

// =============================================================================
// Search 评估算子的 CPU 后端 (factor::cpu::search; 契约 factor/Search/Contract.hpp + Stat/Contract.hpp)
// =============================================================================
//   定位: 无 GPU 时的参数搜索路径. 行核 / 阶段全部复用 Stat CPU (Stat/Host.hpp + Stat/Cpu.hpp), 本文件只加:
//     cost_rows   成本两格 (顶组买入 / 底组卖出; rx 有效 ∧ 成本有效, 不看 ok / 尾部 / t ≥ h) 沿行 [t_lo,T) 池化, Exec 切行 → CostAcc[tid] → merge
//     run         一批候选: 候选串行, 候选内全核切行 (rank_x → accum_rows → cost_rows), 块语义 t0_abs / t_lo 直通 accum_rows.
//                 候选内并行而不是候选间: C 小 (局部网格几个点) 也能跑满核, 且只留一张 [T][A] 的 r16, 不留 ring / 每线程平面.
//   【与 Stat CPU 的关系】同一行核 ⇒ 整数派生量 / 浮点和都逐位同 Stat CPU; 对拍 (Search/Check.hpp) 验的是块语义 / 累加 / 成本.
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Search/Contract.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <vector>

namespace factor::cpu::search {

using factor::search::Cost;
using factor::search::CostAcc;
using factor::search::Result;
using factor::stat::Frame;
using factor::stat::Holds;
using factor::stat::kGroups;
using factor::stat::kRankNone;
using factor::stat::Label;

// 调用方持有: Stat 的工作区 + 成本的线程私有累加器; ensure 幂等
struct Workspace {
  factor::cpu::stat::Workspace st;
  std::vector<CostAcc> cacc; // [tid]
  void ensure(int T, int A, int H, int threads) {
    st.ensure(T, A, H, threads);
    if (cacc.size() < static_cast<size_t>(threads))
      cacc.resize(static_cast<size_t>(threads));
  }
};

// 成本两格一行: rx = 本行 r16, c 已偏到行首
inline void cost_row(const uint16_t *rx, const Cost &c, int A, CostAcc &acc) {
  constexpr int kt = kGroups - 1;
  for (int a = 0; a < A; ++a) {
    const unsigned x = rx[a];
    const bool ok = x != kRankNone && c.m[a] != 0;
    const int g = ok ? factor::stat::grp_of(x) : -1;
    acc.buy_hi += g == kt ? static_cast<double>(factor::cpu::stat::h2f(c.buy[a])) : 0.0;
    acc.sell_lo += g == 0 ? static_cast<double>(factor::cpu::stat::h2f(c.sell[a])) : 0.0;
    acc.n_hi += g == kt;
    acc.n_lo += g == 0;
  }
}

// 成本两格沿行 [t_lo,T) 池化 (w.st.ws 已是本候选的 r16) → out (覆盖)
template <class Exec>
inline void cost_rows(Exec &ex, int T, int A, int t_lo, const Cost &cost, Workspace &w, CostAcc &out) {
  assert(cost.buy && cost.sell && cost.m && w.st.T == T && w.st.A == A && t_lo >= 0 && t_lo < T);
  const int nt = ex.threads();
  assert(w.cacc.size() >= static_cast<size_t>(nt));
  std::fill(w.cacc.begin(), w.cacc.begin() + nt, CostAcc{});
  const uint16_t *ws = w.st.ws.data();
  ex.rows(T - t_lo, [&](int r0, int r1, int tid) {
    CostAcc &a = w.cacc[static_cast<size_t>(tid)];
    for (int t = t_lo + r0; t < t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      cost_row(ws + base, cost.at(base), A, a);
    }
  });
  out = w.cacc[0];
  for (int tid = 1; tid < nt; ++tid)
    out.merge(w.cacc[static_cast<size_t>(tid)]);
}

// 一批候选: xv[c] / xm[c] 各一张 [T][A] (块形), 统计 [t_lo,T), 绝对首行 t0_abs (契约【分块流式】) → out[c] (覆盖)
template <class Exec>
inline void run(Exec &ex, int C, const float *const *xv, const uint8_t *const *xm, const uint8_t *g, Frame f, int T, int A, int t0_abs,
                int t_lo, const Holds &hd, const Label *lab, const Cost &cost, Workspace &w, Result *out) {
  assert(C >= 1 && xv && xm && g && lab && out);
  for (int c = 0; c < C; ++c) {
    factor::cpu::stat::rank_x(ex, xv[c], xm[c], g, f, T, A, w.st);
    out[c] = Result{};
    factor::cpu::stat::accum_rows(ex, f, T, A, t0_abs, t_lo, hd, lab, w.st, nullptr, out[c].acc);
    cost_rows(ex, T, A, t_lo, cost, w, out[c].cost);
  }
}

} // namespace factor::cpu::search
