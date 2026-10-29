#pragma once

// =============================================================================
// Search 评估算子的 CPU 后端 (factor::cpu::search; 契约 factor/Search/Contract.hpp + Stat/Contract.hpp)
// =============================================================================
//   定位: 无 GPU 时的参数搜索路径. 行核与流程全部复用 Stat CPU (Stat/Cpu.hpp: rank_x / accum_rows), 本文件只加:
//     run         一批候选: 候选串行, 候选内全核切行 (rank_x → accum_rows, 行丢弃). 候选内并行而不是候选间: C 小 (局部网格几个点) 也能跑满核,
//                 且只留一张 [T][A] 的 r16 (调用方持有), 不留 ring / 每线程平面.
//   GUI 的 CPU 搜索不走会话: 引擎 (FactorsEngine Engine<Cpu>) 逐根 Stat; 本文件只给对拍 (Search/Check.hpp).
//   【与 Stat CPU 的关系】同一流程 ⇒ 整数派生量 / 浮点和都逐位同 Stat CPU; 对拍 (Search/Check.hpp) 验的是块语义 / 累加.
//   出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Search/Contract.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Cpu.hpp"

#include <cassert>
#include <cstdint>

namespace factor::cpu::search {

using factor::search::Result;
using factor::stat::Batch;
using factor::stat::Frame;
using factor::stat::Labels;
using factor::stat::Span;

// 一批候选: B.x[c] 各一张 [T][A] (块形), 统计 [s.t_lo, s.t_hi), 绝对首行 s.t0_abs (契约【分块流式】) → out[c] (覆盖).
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

} // namespace factor::cpu::search
