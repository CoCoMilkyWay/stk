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
//     accum_rows    rx × 标签集 → 统计行 [t_lo,t_hi) 的 Row 折进 Accum[H]  ← exec.rows(每 t 每 h row_stat → sink → Accum[tid]) → merge
//                   块语义 (Search 契约【分块流式】): t0_abs 给 tail_masked / 交接; 块内 t − h < 0 → 无 AC; 前带只给 lag 行 / 交接链, 后带只给续持
//                   sink 每行一次 (线程内, 行不相交): ext() 给本行 label_row 的逐格扩展, row(i, t, r, ext) 收行;
//                   DropRows 丢弃 / LandRows 落地 rows[i·T + t] / 调用方自定 (Inspect 尾部分层)
//     run           = rank_x + accum_rows (一个候选的完整评估); HoldStat 由契约 finish_all
//     end_codes     rx → 端码 ec[T][A] (契约【换手成本】)
//     cost_rows     ec × 标签有效位 × C ≥ 1 个金额段 → CostAcc[C][H]  ← exec.rows(每 t 一次 cost_row → [tid]) → merge
//     Search/Cpu.hpp 对每个候选走 rank_x + accum_rows, 不另写流程.
//   标签秩 ry / r16(x) 平面 rx / 端码 ec 由调用方持有 (ry 随标签一代, 一次 prep_label 多因子共用); Workspace (Scratch[tid] / Accum[tid][H] / CostAcc[tid][C][H])
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

// 标签预处理: 一组标签的做多侧 (lb.v[kLong] / lb.m) → ry [T][A]. 常驻期算一次 (用 w.sc)
template <class Exec>
inline void prep_label(Exec &ex, const Label &lb, const Span &s, uint16_t *ry, Workspace &w) {
  assert(lb.v[kLong] && lb.m && ry && w.A == s.A && w.threads >= ex.threads() && "Workspace 未 ensure 到位");
  ex.rows(s.T, [&](int t0, int t1, int tid) { rank_y_rows(lb, t0, t1, s.A, ry, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 1: r16(x) → rx [T][A] (块内全部行, 含前后带)
template <class Exec>
inline void rank_x(Exec &ex, Plane x, const uint8_t *g, Frame f, const Span &s, uint16_t *rx, Workspace &w) {
  factor::stat::assert_span(s);
  assert(x.v && x.m && g && rx && w.A == s.A && w.threads >= ex.threads());
  ex.rows(s.T, [&](int t0, int t1, int tid) { rank_x_rows(x, g, f, t0, t1, s.A, rx, w.sc[static_cast<size_t>(tid)]); });
}

// 阶段 2: rx × L 组标签 → 统计行 [t_lo, t_hi) 的 Row, 每行先 sink.row 再折进 acc[hd.n] (覆盖)
template <class Exec, class Sink>
inline void accum_rows(Exec &ex, const Span &s, const uint16_t *rx, const Labels &L, Workspace &w, Accum *acc, const Sink &sink) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, true);
  const int H = L.hd.n, A = s.A, nt = ex.threads();
  assert(rx && acc && w.A == A && w.H >= H && w.threads >= nt);
  std::fill(w.acc.begin(), w.acc.begin() + static_cast<size_t>(nt) * H, Accum{});
  ex.rows(s.t_hi() - s.t_lo, [&](int r0, int r1, int tid) {
    Accum *a = w.acc.data() + static_cast<size_t>(tid) * H;
    for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      for (int i = 0; i < H; ++i) {
        const int hold = L.hd.k[i], h = factor::stat::hold_rows(hold, s.g);
        typename Sink::Ext e = sink.ext();
        const Row r = row_stat(rx + base, t >= h ? rx + base - static_cast<size_t>(h) * A : nullptr, L.l[i].at(base),
                               factor::stat::chain_of(t, s, hold), L.cap, factor::stat::tail_masked(s.t0_abs + t, hold, s.g), e);
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

// 端码: rx → ec [T][A] (契约 end_code 逐格; d = 所评方向)
template <class Exec>
inline void end_codes(Exec &ex, const uint16_t *rx, const Span &s, int d, uint8_t *ec) {
  factor::stat::assert_span(s);
  assert(rx && ec);
  ex.rows(s.T, [&](int t0, int t1, int) {
    for (size_t i = static_cast<size_t>(t0) * s.A, e = static_cast<size_t>(t1) * s.A; i < e; ++i)
      ec[i] = factor::stat::end_code(rx[i], d);
  });
}

// 换手成本 (契约【换手成本】): ec × L 的标签有效位 / 可成交码 × c[C] (块形平面, 未偏) → out[k·H + i] (覆盖). 统计行 [t_lo, t_hi) (同 accum_rows), 每 t 一次
// cost_row; 本行尾部 → 不攒; 交接链 chain_of (块内 t ≥ h 且 hold_nets → 前一批 = 行 t−h); 本批平仓不交接 (hold_nets(t + h) 假) → settle;
// cap = 一端满仓 (端码 = 两端组 → L.cap; Inspect 尾部另给). C ≥ 1 (固定段 = 空 Cost). land 非空 → 另落逐行 land[(k·H + i)·T + t]
// (只本行, 尾部行全 0; Portfolio 逐行成本)
template <class Exec>
inline void cost_rows(Exec &ex, const uint8_t *ec, const Span &s, const Labels &L, double cap, const Cost *c, int C, Workspace &w, CostAcc *out,
                      CostAcc *land = nullptr) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, false);
  const int H = L.hd.n, A = s.A, nt = ex.threads();
  assert(ec && c && out && cap > 0.0 && C >= 1 && C <= kMaxCost && w.A == A && w.H >= H && w.C >= C && w.threads >= nt);
  const size_t per = static_cast<size_t>(C) * H;
  std::fill(w.cacc.begin(), w.cacc.begin() + static_cast<size_t>(nt) * per, CostAcc{});
  ex.rows(s.t_hi() - s.t_lo, [&](int r0, int r1, int tid) {
    CostAcc *acc = w.cacc.data() + static_cast<size_t>(tid) * per;
    for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      const int ta = s.t0_abs + t;
      CostHold ch[kMaxHold];
      for (int i = 0; i < H; ++i) {
        const int hold = L.hd.k[i], h = factor::stat::hold_rows(hold, s.g);
        if (factor::stat::tail_masked(ta, hold, s.g))
          continue;
        CostHold &q = ch[i];
        q.m = L.l[i].m + base;
        q.eo = L.l[i].eo + base;
        q.c = factor::stat::chain_of(t, s, hold);
        q.settle = !factor::stat::hold_nets(ta + h, hold, s.g);
      }
      Cost cr[kMaxCost];
      for (int k = 0; k < C; ++k)
        cr[k] = c[k].at(base);
      if (!land) {
        cost_row(ec + base, ch, H, cr, C, A, cap, acc);
        continue;
      }
      CostAcc row[kMaxCost * kMaxHold] = {};
      cost_row(ec + base, ch, H, cr, C, A, cap, row);
      for (size_t j = 0; j < per; ++j) {
        acc[j].merge(row[j]);
        land[j * static_cast<size_t>(s.T) + static_cast<size_t>(t)] = row[j];
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
