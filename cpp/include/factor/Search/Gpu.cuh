#pragma once

// =============================================================================
// Search 评估算子的 GPU 后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; nvcc)
// =============================================================================
//   契约 factor/Search/Contract.hpp + Stat/Contract.hpp; 行核 Stat/Dev.cuh (rank_x / warp_row, 与 Stat GPU 同一组). 本文件只加:
//   【stat_batch】grid (⌈C/kWarps⌉, NB, H) × kWB, **一 warp 一候选**, 一块管 kRows 行 × 一个持有期 (grid.z):
//     每行一次 warp_row (全 lane 拿到 Row) → **直接折进 warp 私有的分片累加器** (lane 0 攒 Accum 的标量, lane k < 20 攒组 k 的
//     Σgrp / Σcnt; 运算与 Accum::add 逐字同) → 块末写分片 Slice[c][h][tb] (= 契约的 Accum + CostAcc 原样). 不落 Row.
//     同 block 的 kWarps 个候选同一 (t, h): ry / lv / sv 行一次进 L1 共用, 标签流量被候选数摊薄. 换手成本 (契约【成本】, 随 h 变) 紧跟
//     warp_row 在同一行上攒: 有 lag 行才算, 只有顶 / 底组成员才碰成本平面 (lane 分支跳过读). 核内没有块级同步, 没有原子.
//   【reduce_slices】每 (c, h) 一线程, 沿 tb 升序 merge (契约 merge 原样; 固定序 → GPU 自身逐次确定) → 直接写 Result[c] (设备),
//     主机整块拷回, 无转换.
//   【与 CPU 的差别】整数派生量逐位一致; 浮点和只差求和序 (对拍容差). 成本冲击和: 行内 float 归约, 跨行 double.
//   【禁用】--use_fast_math.
// =============================================================================

#include "factor/Search/Contract.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Dev.cuh"

#include <cassert>
#include <cstdint>
#include <cuda_runtime.h>

namespace factor::gpu::search {

namespace dev = factor::gpu::stat::dev;
using factor::gpu::stat::Batch;
using factor::gpu::stat::kMaxBatch;
using factor::gpu::stat::kWarps;
using factor::gpu::stat::kWB;
using factor::gpu::stat::LabelSet;
using factor::gpu::stat::Slots;
using factor::search::Cost;
using factor::search::CostAcc;
using factor::search::Result;
using factor::stat::Accum;
using factor::stat::Frame;
using factor::stat::Holds;
using factor::stat::kGroups;
using factor::stat::kMaxA;
using factor::stat::kMaxHold;
using factor::stat::kRankNone;
using factor::stat::Label;
using factor::stat::Row;

inline constexpr int kRows = 128; // stat_batch 每 block 的行数 (分片粒度; NB = ⌈(T − t_lo)/kRows⌉)

// 一个 (候选, 持有期, 行块) 的分片 = 契约累加器原样
struct Slice {
  Accum acc;
  CostAcc cost;
};

namespace k {

__global__ void __launch_bounds__(kWB, 2) stat_batch(Frame f, const uint16_t *rx, int C, int T, int A, int t0_abs, int t_lo, Holds hd, LabelSet L,
                                                     Cost cost, int NB, Slice *part) {
  __shared__ Slots sh;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int c = blockIdx.x * kWarps + warp;
  if (c >= C) // warp 一致; 核内没有块级同步, 整 warp 直接退出安全
    return;
  const int hi = blockIdx.z, hold = hd.h[hi], h = factor::stat::hold_minutes(hold);
  const Label lb = L.l[hi];
  const int tb = blockIdx.y, t_beg = t_lo + tb * kRows, t_end = min(T, t_beg + kRows);
  const uint16_t *rxc = rx + static_cast<size_t>(c) * T * A;
  constexpr int kt = kGroups - 1;

  // warp 私有分片 (= Accum::add 的字段拆到 lane: lane 0 标量 + 成本, lane q < kGroups 组 q)
  int an = 0, anpos = 0, anlpos = 0, anac = 0;
  double asic = 0.0, asic2 = 0.0, asic3 = 0.0, asic4 = 0.0, asls = 0.0, asls2 = 0.0, asmk = 0.0, asmk2 = 0.0, aslm = 0.0, asac = 0.0;
  double asg = 0.0;
  long long asc = 0;
  double abuy = 0.0, asell = 0.0;
  long long anh = 0, anl = 0, aeh = 0, ael = 0;

  for (int t = t_beg; t < t_end; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    const uint16_t *xr = rxc + base;
    const uint16_t *xl = t >= h ? xr - static_cast<size_t>(h) * A : nullptr;
    const Row r = dev::warp_row(f, xr, xl, lb.at(base), A, factor::stat::tail_masked(t0_abs + t, hold), sh.gs[warp], sh.gc[warp], lane);
    // ---- 折进分片 (与 Accum::add 逐字同) ----
    if (r.ok) {
      float gq = 0.f; // lane q 取 grp[q] / cnt[q] (编译期展开的 select, Row 留在寄存器)
      int cq = 0;
#pragma unroll
      for (int q = 0; q < kGroups; ++q)
        if (q == lane)
          gq = r.grp[q], cq = r.cnt[q];
      asg += gq;
      asc += cq;
      if (lane == 0) {
        ++an;
        anpos += r.ic > 0.f;
        anlpos += r.ls > 0.f;
        const double ic = r.ic, ic2 = ic * ic, ls = r.ls, mk = r.mkt;
        asic += ic, asic2 += ic2, asic3 += ic2 * ic, asic4 += ic2 * ic2;
        asls += ls, asls2 += ls * ls, asmk += mk, asmk2 += mk * mk, aslm += ls * mk;
      }
    }
    if (r.ok_ac && lane == 0) {
      ++anac;
      asac += r.ac;
    }
    // ---- 换手成本 (有 lag 行才算; 与 Search/Cpu.hpp cost_row 同式: 顶 / 底组成员 (rx 有效 ∧ 成本有效) 计数, t−h 不在该组 = 新进者累冲击) ----
    if (xl) {
      const Cost cr = cost.at(base);
      float cv[2] = {0.f, 0.f}; // Σ买|顶组新进, Σ卖|底组新进
      int cn[4] = {0, 0, 0, 0}; // 顶组数, 底组数, 顶组新进数, 底组新进数
      for (int a = lane; a < A; a += 32) {
        const unsigned x = xr[a];
        const int g = x != kRankNone ? factor::stat::grp_of(x) : -1;
        if ((g == kt || g == 0) && cr.m[a] != 0) { // 只有顶 / 底组成员才读 lag / 成本 (稀疏, 分支跳过读)
          const unsigned y = xl[a];
          const int gl = y != kRankNone ? factor::stat::grp_of(y) : -1;
          const bool hi = g == kt, ent = gl != g;
          cn[0] += hi;
          cn[1] += !hi;
          cn[2] += hi && ent;
          cn[3] += !hi && ent;
          cv[0] += hi && ent ? dev::h2f(cr.buy[a]) : 0.f;
          cv[1] += !hi && ent ? dev::h2f(cr.sell[a]) : 0.f;
        }
      }
      dev::warp_sum(cv);
      dev::warp_sum(cn);
      if (lane == 0) {
        abuy += static_cast<double>(cv[0]), asell += static_cast<double>(cv[1]);
        anh += cn[0], anl += cn[1], aeh += cn[2], ael += cn[3];
      }
    }
  }

  // ---- 写分片 (每字段恰由一个 lane 写) ----
  Slice &P = part[(static_cast<size_t>(c) * hd.n + hi) * NB + tb];
  if (lane < kGroups) {
    P.acc.sg[lane] = asg;
    P.acc.sc[lane] = asc;
  }
  if (lane == 0) {
    P.acc.n = an, P.acc.npos = anpos, P.acc.nlpos = anlpos, P.acc.nac = anac;
    P.acc.sic = asic, P.acc.sic2 = asic2, P.acc.sic3 = asic3, P.acc.sic4 = asic4;
    P.acc.sls = asls, P.acc.sls2 = asls2, P.acc.smk = asmk, P.acc.smk2 = asmk2, P.acc.slm = aslm, P.acc.sac = asac;
    P.cost.buy_hi = abuy, P.cost.sell_lo = asell, P.cost.n_hi = anh, P.cost.n_lo = anl, P.cost.e_hi = aeh, P.cost.e_lo = ael;
  }
}

// reduce_slices: 每 (c, h) 一线程, 沿 tb 升序 merge → out[c].acc[h] / cost[h]
__global__ void __launch_bounds__(kWB) reduce_slices(const Slice *part, int NB, int C, int H, Result *out) {
  const int p = blockIdx.x * kWB + threadIdx.x;
  if (p >= C * H)
    return;
  const int c = p / H, hi = p - c * H;
  const Slice *src = part + static_cast<size_t>(p) * NB;
  Slice s;
  for (int tb = 0; tb < NB; ++tb) {
    s.acc.merge(src[tb].acc);
    s.cost.merge(src[tb].cost);
  }
  out[c].acc[hi] = s.acc;
  out[c].cost[hi] = s.cost;
}

} // namespace k

// =============================================================================
// 宿主接口 (全部设备指针; 拷入拷出由调用方负责 —— 搜索侧候选平面常驻显存)
// =============================================================================

// 一次 eval 的形状与工作区尺寸 (调用方按它分配: rx / part / out)
struct Work {
  int C = 0, T = 0, A = 0, H = 0, t_lo = 0, NB = 0;
  size_t rx_elems() const { return static_cast<size_t>(C) * T * A; }
  size_t part_elems() const { return static_cast<size_t>(C) * H * NB; }
  size_t out_elems() const { return static_cast<size_t>(C); }
};
inline Work plan(int C, int T, int A, int H, int t_lo) {
  assert(C >= 1 && C <= kMaxBatch && T >= 1 && A >= 1 && A <= kMaxA && static_cast<long long>(T) * A < (1LL << 31));
  assert(H >= 1 && H <= kMaxHold && t_lo >= 0 && t_lo < T);
  Work w;
  w.C = C, w.T = T, w.A = A, w.H = H, w.t_lo = t_lo;
  w.NB = (T - t_lo + kRows - 1) / kRows;
  return w;
}

// 一批候选的评估: B (口径 f, 只看池内 g ∧ m) + hd.n 组预处理标签 + 成本 → out[C] (设备 Result). 契约【分块流式】的 t0_abs / t_lo
inline void eval(const Batch &B, const uint8_t *g, Frame f, const Work &w, int t0_abs, const Holds &hd, const Label *lab, const Cost &cost,
                 uint16_t *rx, Slice *part, Result *out, cudaStream_t stream) {
  factor::stat::assert_holds(hd);
  assert(B.C == w.C && hd.n == w.H && g && rx && part && out && t0_abs >= 0);
  assert(cost.buy && cost.sell && cost.m);
  factor::gpu::stat::rank_x(B, g, f, w.T, w.A, rx, stream);
  const dim3 grid(static_cast<unsigned>((w.C + kWarps - 1) / kWarps), static_cast<unsigned>(w.NB), static_cast<unsigned>(w.H));
  k::stat_batch<<<grid, kWB, 0, stream>>>(f, rx, w.C, w.T, w.A, t0_abs, w.t_lo, hd, factor::gpu::stat::label_set(hd, lab), cost, w.NB, part);
  FACTOR_CUDA_OK(cudaGetLastError());
  const int P = w.C * w.H;
  k::reduce_slices<<<(P + kWB - 1) / kWB, kWB, 0, stream>>>(part, w.NB, w.C, w.H, out);
  FACTOR_CUDA_OK(cudaGetLastError());
}

} // namespace factor::gpu::search
