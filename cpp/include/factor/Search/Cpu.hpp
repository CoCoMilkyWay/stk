#pragma once

// =============================================================================
// Search 评估算子的 CPU 后端 (factor::cpu::search; 契约 factor/Search/Contract.hpp + Stat/Contract.hpp)
// =============================================================================
//   定位: 无 GPU 时的参数搜索路径. 行核与流程全部复用 Stat CPU (Stat/Cpu.hpp: rank_x / accum_rows), 本文件只加:
//     run         一批候选: 候选串行, 候选内全核切行 (rank_x → accum_rows, 行丢弃). 候选内并行而不是候选间: C 小 (局部网格几个点) 也能跑满核,
//                 且只留一张 [T][A] 的 r16 (调用方持有), 不留 ring / 每线程平面.
//     SearchSession  search_open / search_load / search_eval / search_ranks: 与 gpu::SearchSession 同名同参序 (rx 归会话, 只留最后一个候选)
//   【与 Stat CPU 的关系】同一流程 ⇒ 整数派生量 / 浮点和都逐位同 Stat CPU; 对拍 (Search/Check.hpp) 验的是块语义 / 累加.
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

using factor::search::Result;
using factor::stat::Batch;
using factor::stat::Frame;
using factor::stat::Holds;
using factor::stat::Label;
using factor::stat::Labels;
using factor::stat::Plane;
using factor::stat::Span;

// 一批候选: B.x[c] 各一张 [T][A] (块形), 统计 [s.t_lo,T), 绝对首行 s.t0_abs (契约【分块流式】) → out[c] (覆盖).
// rx = r16 工作平面 [T][A] (候选间复用, 调用方持有; 跑完是最后一个候选的秩), w = Stat 的工作区
template <class Exec>
inline void run(Exec &ex, const Batch &B, const uint8_t *g, Frame f, const Span &s, const Labels &L, uint16_t *rx,
                factor::cpu::stat::Workspace &w, Result *out) {
  assert(B.C >= 1 && g && rx && out);
  for (int c = 0; c < B.C; ++c) {
    factor::cpu::stat::rank_x(ex, B.x[c], g, f, s, rx, w);
    out[c] = Result{};
    factor::cpu::stat::accum_rows(ex, s, rx, L, w, out[c].acc, factor::cpu::stat::DropRows{});
  }
}

// -----------------------------------------------------------------------------
// 常驻会话 (与 gpu::SearchSession 同名同参序, GpuRun.hpp): open = 块形 + 持有期 + 满仓; load = 一块的标签 (rank 预处理);
// eval = 一批候选; ranks = 刚评的候选的 r16 抽样行
// -----------------------------------------------------------------------------

struct SearchSession {
  int T = 0, A = 0, seg = 0;
  Labels L;                              // hd / cap 随 open; 两腿 / 掩码随 load (调用方宿主标签, 须活过会话), ry 指下面
  std::vector<std::vector<uint16_t>> ry; // [hd.n][T·A]
  factor::cpu::stat::Workspace w;
  std::vector<uint16_t> rx; // r16 [T][A] (候选间复用: 只留本批最后一个候选)
  int last = -1;            // rx 是本批第几个候选 (ranks 只认它)
};
template <class Exec>
inline void search_open(Exec &ex, SearchSession &ss, int T, int A, int seg, const Holds &hd, double cap) {
  factor::stat::assert_span(Span{T, A, geo_of(seg)});
  factor::stat::assert_holds(hd, seg);
  assert(cap > 0.0 && "满仓 cap 未给 (n_full)");
  ss.T = T, ss.A = A, ss.seg = seg;
  ss.L = Labels{};
  ss.L.hd = hd;
  ss.L.cap = cap;
  ss.w.ensure(A, hd.n, 0, ex.threads());
  ss.rx.resize(static_cast<size_t>(T) * A);
  ss.last = -1;
}
// lab[hd.n] = 宿主标签 (ry 不看, 同 gpu::search_load)
template <class Exec>
inline void search_load(Exec &ex, SearchSession &ss, const Label *lab) {
  const Span s{ss.T, ss.A, geo_of(ss.seg)};
  ss.ry.resize(static_cast<size_t>(ss.L.hd.n));
  for (int i = 0; i < ss.L.hd.n; ++i) {
    std::vector<uint16_t> &ry = ss.ry[static_cast<size_t>(i)];
    ry.resize(s.n());
    factor::cpu::stat::prep_label(ex, lab[i], s, ry.data(), ss.w);
    ss.L.l[i] = lab[i];
    ss.L.l[i].ry = ry.data();
  }
  ss.last = -1;
}
// x[C] = 候选平面 (块形); 其余同 run
template <class Exec>
inline void search_eval(Exec &ex, SearchSession &ss, const Plane *x, int C, const uint8_t *g, Frame f, int t0_abs, int t_lo, Result *out) {
  assert(C >= 1 && C <= factor::stat::kMaxBatch);
  Batch B;
  B.C = C;
  std::copy_n(x, C, B.x);
  run(ex, B, g, f, Span{ss.T, ss.A, geo_of(ss.seg), t0_abs, t_lo}, ss.L, ss.rx.data(), ss.w, out);
  ss.last = C - 1;
}
inline void search_ranks(const SearchSession &ss, int c, int t0, int stride, int n_rows, uint16_t *out) {
  assert(c == ss.last && "CPU 会话只留本批最后一个候选的 r16");
  assert(t0 >= 0 && stride >= 1 && n_rows >= 0 && t0 + (n_rows - 1) * stride < ss.T);
  for (int i = 0; i < n_rows; ++i)
    std::copy_n(ss.rx.data() + static_cast<size_t>(t0 + i * stride) * ss.A, ss.A, out + static_cast<size_t>(i) * ss.A);
}

} // namespace factor::cpu::search
