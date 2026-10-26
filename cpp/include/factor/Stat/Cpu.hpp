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
//     cost_rows     lc × 标签有效位 × C 个金额段 → CostAcc[C][H]  ← exec.rows(每 t 一次 cost_row → [tid]) → merge
//     StatSession   stat_open (标签 prep_label + 工作区, 一次) / stat_eval (run + leg_codes, 逐因子): 与 gpu::stat_open / stat_eval 同序
//     Search/Cpu.hpp 对每个候选走 rank_x + accum_rows, 不另写流程.
//   r16(x) 平面 rx / 腿码 lc 由调用方持有 (与 GPU 同); Workspace (Scratch[tid] / Accum[tid][H] / CostAcc[tid][C][H])
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
    if (cacc.size() < static_cast<size_t>(threads_) * std::max(C_, 1) * H_)
      cacc.resize(static_cast<size_t>(threads_) * std::max(C_, 1) * H_);
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
  factor::stat::assert_labels(L, s.g.S, true);
  const int H = L.hd.n, A = s.A, nt = ex.threads();
  assert(rx && acc && w.A == A && w.H >= H && w.threads >= nt);
  std::fill(w.acc.begin(), w.acc.begin() + static_cast<size_t>(nt) * H, Accum{});
  ex.rows(s.T - s.t_lo, [&](int r0, int r1, int tid) {
    Accum *a = w.acc.data() + static_cast<size_t>(tid) * H;
    for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      for (int i = 0; i < H; ++i) {
        const int hold = L.hd.h[i], h = factor::stat::hold_rows(hold, s.g);
        typename Sink::Ext e = sink.ext();
        const Row r = row_stat(rx + base, t >= h ? rx + base - static_cast<size_t>(h) * A : nullptr, L.l[i].at(base), A, L.cap,
                               factor::stat::tail_masked(s.t0_abs + t, hold, s.g), e);
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

// 换手成本 (契约【换手成本】): lc × L 的标签有效位 × c[C] (块形平面, 未偏) → out[k·H + i] (覆盖). 统计行 [t_lo,T) (同 accum_rows), 每 t 一次
// cost_row; 本行尾部 → 不攒; 交接 (hold_nets, 块内 t ≥ h) → 前一批 = 行 t−h; 本批平仓不交接 (hold_nets(t + h) 假) → settle; cap = L.cap.
// C = 0 → 只出量 (cost_row), out / land 按一个金额段排. land 非空 → 另落逐行 land[(k·H + i)·T + t] (只本行, 尾部行全 0; Portfolio 逐行成本)
template <class Exec>
inline void cost_rows(Exec &ex, const uint8_t *lc, const Span &s, const Labels &L, const Cost *c, int C, Workspace &w, CostAcc *out,
                      CostAcc *land = nullptr) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, false);
  const int H = L.hd.n, A = s.A, nt = ex.threads(), K = C > 0 ? C : 1;
  assert(lc && (c || C == 0) && out && C >= 0 && C <= kMaxCost && w.A == A && w.H >= H && w.C >= C && w.threads >= nt);
  const size_t per = static_cast<size_t>(K) * H;
  std::fill(w.cacc.begin(), w.cacc.begin() + static_cast<size_t>(nt) * per, CostAcc{});
  ex.rows(s.T - s.t_lo, [&](int r0, int r1, int tid) {
    CostAcc *acc = w.cacc.data() + static_cast<size_t>(tid) * per;
    for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      const int ta = s.t0_abs + t;
      CostHold ch[kMaxHold];
      for (int i = 0; i < H; ++i) {
        const int hold = L.hd.h[i], h = factor::stat::hold_rows(hold, s.g);
        if (factor::stat::tail_masked(ta, hold, s.g))
          continue;
        CostHold &q = ch[i];
        q.m = L.l[i].m + base;
        if (t >= h && factor::stat::hold_nets(ta, hold, s.g)) {
          q.plc = lc + base - static_cast<size_t>(h) * A;
          q.pm = q.m - static_cast<size_t>(h) * A;
        }
        q.settle = !factor::stat::hold_nets(ta + h, hold, s.g);
      }
      Cost cr[kMaxCost];
      for (int k = 0; k < C; ++k)
        cr[k] = c[k].at(base);
      if (!land) {
        cost_row(lc + base, ch, H, cr, C, A, L.cap, acc);
        continue;
      }
      CostAcc row[kMaxCost * kMaxHold] = {};
      cost_row(lc + base, ch, H, cr, C, A, L.cap, row);
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

// -----------------------------------------------------------------------------
// 常驻会话 (与 gpu::StatSession 同名同参序, GpuRun.hpp): 一个作用域多因子 —— open 一次 (标签 rank 预处理 + 工作区), 逐因子 eval
// -----------------------------------------------------------------------------

struct StatSession {
  Span s;                                // 整张量 (t0_abs = t_lo = 0)
  Labels L;                              // 两腿 / 掩码指调用方的宿主标签 (须活过会话), ry 指下面
  std::vector<std::vector<uint16_t>> ry; // [hd.n][T·A]
  Workspace w;                           // ensure 到 (A, hd.n, C, 线程数); 宿主收尾 (cost_rows) 共用
};
// L = 宿主标签 (ry 不看, 同 gpu::stat_open); C = 金额段数 (w 给 cost_rows 用; 不算成本给 0)
template <class Exec>
inline void stat_open(Exec &ex, StatSession &ss, int T, int A, int seg, const Labels &L, int C) {
  ss.s = Span{T, A, geo_of(seg)};
  factor::stat::assert_span(ss.s);
  factor::stat::assert_labels(L, seg, false);
  ss.L = L;
  ss.w.ensure(A, L.hd.n, C, ex.threads());
  ss.ry.resize(static_cast<size_t>(L.hd.n));
  for (int i = 0; i < L.hd.n; ++i) {
    std::vector<uint16_t> &ry = ss.ry[static_cast<size_t>(i)];
    ry.resize(ss.s.n());
    prep_label(ex, L.l[i], ss.s, ry.data(), ss.w);
    ss.L.l[i].ry = ry.data();
  }
}
// 一个因子 (同 gpu::stat_eval 序; 一级 Row 走 sink 而不是 rows): run → rx (调用方持有) + acc[hd.n]; lc 非空再出腿码
template <class Exec, class Sink>
inline void stat_eval(Exec &ex, StatSession &ss, Plane x, const uint8_t *g, Frame f, Accum *acc, const Sink &sink, uint8_t *lc, uint16_t *rx) {
  run(ex, x, g, f, ss.s, ss.L, rx, ss.w, acc, sink);
  if (lc)
    leg_codes(ex, rx, ss.s, lc);
}

} // namespace factor::cpu::stat
