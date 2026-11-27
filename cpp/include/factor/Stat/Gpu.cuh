#pragma once

// =============================================================================
// Stat 评估算子的 GPU 后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; nvcc)
// =============================================================================
//   语义契约见 factor/Stat/Contract.hpp, 行核见 Stat/Dev.cuh (rank_rows / quant_rows / row_stat). 本文件与 Stat/Cpu.hpp 完全独立,
//   流程同名同参序 (出口逐日 Sum[C][H][D]; 三级 = 宿主 Fold stat_of 按日序 merge, 与 CPU 同序). 两种排布, 由候选数定:
//   【单候选 C = 1: 逐行并行】
//     label_rows   grid ⌈(t_hi − t_lo)/kWarps⌉ × kWB, 一 warp 一行 t, 持有期在 warp 内串行 → rows[h][t] (设备)
//     fold_days    一线程一 (h, 日): 日内各行顺序累加 → day[h][d] (求和序同 CPU, 逐位一致)
//   【批 C > 1: 逐候选并行】
//     stat_batch   grid (⌈C/kWarps⌉, NB, H) × kWB, 一 warp 一候选, 一块管整日的 R 行 × 一个持有期:
//                  每行一次 row_stat → 当日 Sum (lane 0 攒头, lane q 攒组 q) → day[c][h][d]. 同 block 的 kWarps 个候选同一 (t, h):
//                  标签行一次进 L1 共用. 不落 Row.
//   换手在宿主算 (成本平面上显存装不下), 回传已定向的 rx 即可 (Stat/Book.hpp book_rows).
//   【禁用】--use_fast_math.
// =============================================================================

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Dev.cuh"
#include "factor/Stat/Fold.hpp"

#include <cassert>
#include <cstdint>
#include <cuda_runtime.h>

namespace factor::gpu::stat {

using factor::stat::Labels;
using factor::stat::Mode;
using factor::stat::Ret;
using factor::stat::Sum;

inline constexpr int kRows = 128; // 批核一块 = 整日的 ≥ kRows / S 日
// 批核一块的行数: 整日对齐的 R = S · max(1, ⌊kRows / S⌋)
inline int batch_rows(int S) { return S * (kRows / S > 0 ? kRows / S : 1); }

// 设备工作区 (调用方按 Span / H / Cmax 分配, 指针归调用方)
struct Workspace {
  Row *rows = nullptr; // [H][T] 一级 (单候选)
  Sum *day = nullptr;  // [Cmax][H][D] 出口
  int H = 0, Cmax = 0, D = 0;
};
inline int n_batch_parts(const Span &s) { return (s.t_hi() - s.t_lo + batch_rows(s.g.S) - 1) / batch_rows(s.g.S); }

namespace k {

__global__ void __launch_bounds__(kWB, 2) label_rows(Span s, const uint16_t *rx, Labels L, Ret ret, Row *rows) {
  __shared__ Slots sh;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int t = s.t_lo + blockIdx.x * kWarps + warp;
  if (t >= s.t_hi()) // warp 一致; 核内没有块级同步, 整 warp 直接退出安全
    return;
  const size_t base = static_cast<size_t>(t) * s.A;
  const uint16_t *xr = rx + base;
  for (int hi = 0; hi < L.hd.n; ++hi) { // warp 一致量, 不发散
    const Hold hd = hold_on(L.hd.k[hi], s.g);
    const Row r = dev::row_stat(xr, t >= hd.h ? xr - static_cast<size_t>(hd.h) * s.A : nullptr, L.l[hi].at(base), factor::stat::chain_of(t, s, hd), ret, hd.opens(t),
                                sh.gs[warp], sh.gc[warp], lane);
    if (lane == 0)
      rows[static_cast<size_t>(hi) * s.T + t] = r;
  }
}

__global__ void __launch_bounds__(kWB) fold_days(Span s, const Row *rows, int H, Sum *day) {
  const int D = s.days(), p = blockIdx.x * kWB + threadIdx.x;
  if (p >= H * D)
    return;
  const int hi = p / D, d = p - hi * D, S = s.g.S;
  const Row *r = rows + static_cast<size_t>(hi) * s.T + s.t_lo + d * S;
  Sum o;
  for (int t = 0; t < S; ++t)
    o.add(r[t]);
  day[p] = o;
}

__global__ void __launch_bounds__(kWB, 2) stat_batch(const uint16_t *rx, int C, Span s, Labels L, Ret ret, Sum *day) {
  __shared__ Slots sh;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int c = blockIdx.x * kWarps + warp;
  if (c >= C) // warp 一致; 核内没有块级同步, 整 warp 直接退出安全
    return;
  const int A = s.A, H = L.hd.n, hi = blockIdx.z, S = s.g.S, R = S * (kRows / S > 0 ? kRows / S : 1), D = s.days();
  const Hold hd = hold_on(L.hd.k[hi], s.g);
  const int h = hd.h;
  const Label lb = L.l[hi];
  const int tb = blockIdx.y, t_beg = s.t_lo + tb * R, t_end = min(s.t_hi(), t_beg + R);
  const uint16_t *rxc = rx + static_cast<size_t>(c) * s.n();
  Sum *dy = day + (static_cast<size_t>(c) * H + hi) * D;

  Sum::Head head;          // 当日: lane 0 攒头
  double gd = 0.0, cd = 0; // 当日: lane q < kGroups 攒组 q
  for (int t = t_beg; t < t_end; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    const uint16_t *xr = rxc + base;
    const uint16_t *xl = t >= h ? xr - static_cast<size_t>(h) * A : nullptr;
    const Row r = dev::row_stat(xr, xl, lb.at(base), factor::stat::chain_of(t, s, hd), ret, hd.opens(t), sh.gs[warp], sh.gc[warp], lane);
    if (lane == 0)
      head.add(r);
    if (r.ok) {
      float gq = 0.f; // lane q 取 grp[q] / cnt[q] (编译期展开的 select, Row 留在寄存器)
      unsigned cq = 0;
#pragma unroll
      for (int q = 0; q < kGroups; ++q)
        if (q == lane)
          gq = r.grp[q], cq = r.cnt[q];
      gd += gq, cd += cq;
    }
    if ((t - s.t_lo + 1) % S == 0) { // 日末: 落当日 (每字段恰由一个 lane 写)
      Sum &o = dy[(t - s.t_lo) / S];
      if (lane == 0)
        o.h = head;
      if (lane < kGroups)
        o.grp[lane] = gd, o.cnt[lane] = cd;
      head = Sum::Head{}, gd = 0.0, cd = 0.0;
    }
  }
}

} // namespace k

// 单候选: rx × L 组预处理标签 (设备, 收益口径 m.r) → 统计行 [t_lo, t_hi) 落 w.rows[h][t] → w.day[hd.n][D] (设备, 覆盖)
inline void accum_rows(const Span &s, const uint16_t *rx, const Labels &L, const Mode &m, const Workspace &w, cudaStream_t stream) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, true);
  const int H = L.hd.n, D = s.days();
  assert(rx && w.rows && w.day && H <= w.H && D <= w.D);
  k::label_rows<<<(s.t_hi() - s.t_lo + kWarps - 1) / kWarps, kWB, 0, stream>>>(s, rx, L, m.r, w.rows);
  FACTOR_CUDA_OK(cudaGetLastError());
  k::fold_days<<<(H * D + kWB - 1) / kWB, kWB, 0, stream>>>(s, w.rows, H, w.day);
  FACTOR_CUDA_OK(cudaGetLastError());
}

// 批: C 张已定向的 rx[c] × L (收益口径 m.r) → w.day[c][h][D] (设备, 覆盖)
inline void accum_batch(const Span &s, const uint16_t *rx, int C, const Labels &L, const Mode &m, const Workspace &w, cudaStream_t stream) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, true);
  const int H = L.hd.n, NB = n_batch_parts(s);
  assert(rx && w.day && C >= 1 && C <= w.Cmax && H <= w.H && s.days() <= w.D);
  const dim3 grid(static_cast<unsigned>((C + kWarps - 1) / kWarps), static_cast<unsigned>(NB), static_cast<unsigned>(H));
  k::stat_batch<<<grid, kWB, 0, stream>>>(rx, C, s, L, m.r, w.day);
  FACTOR_CUDA_OK(cudaGetLastError());
}

} // namespace factor::gpu::stat
