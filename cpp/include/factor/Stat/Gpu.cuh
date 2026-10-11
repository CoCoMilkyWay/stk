#pragma once

// =============================================================================
// Stat 评估算子的 GPU 后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; nvcc)
// =============================================================================
//   语义契约见 factor/Stat/Contract.hpp, 行核见 Stat/Dev.cuh (rank_rows / quant_rows / warp_row). 本文件与 Stat/Cpu.hpp 完全独立.
//   【label_rows】grid ⌈T/kWarps⌉ × kWB, **一 warp 一行 t**, 持有期在 warp 内串行 (xr 行一次进寄存器 / L1, H 组标签各读一遍),
//     每 (t, h) 一次 warp_row → lane 0 落 rows[h][t]. 核内没有块级同步, t ≥ T 的 warp 直接退出.
//   【eval】rank_x (C = 1 的 Batch) → label_rows. 标签 prep_label 常驻期一次 (Dev.cuh).
//   【禁用】--use_fast_math.
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Dev.cuh"

#include <cassert>
#include <cstdint>
#include <cuda_runtime.h>

namespace factor::gpu::stat {

namespace k {

__global__ void __launch_bounds__(kWB, 2) label_rows(Frame f, const uint16_t *ws, LabelSet L, Holds hd, Row *rows, int T, int A) {
  __shared__ Slots sh;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int t = blockIdx.x * kWarps + warp;
  if (t >= T) // warp 一致; 核内没有块级同步, 整 warp 直接退出安全
    return;
  const size_t base = static_cast<size_t>(t) * A;
  const uint16_t *xr = ws + base;
  for (int hi = 0; hi < hd.n; ++hi) { // warp 一致量, 不发散
    const int hold = hd.h[hi], h = factor::stat::hold_minutes(hold);
    const Row r = dev::warp_row(f, xr, t >= h ? ws + static_cast<size_t>(t - h) * A : nullptr, L.l[hi].at(base), A,
                                factor::stat::tail_masked(t, hold), sh.gs[warp], sh.gc[warp], lane);
    if (lane == 0)
      rows[static_cast<size_t>(hi) * T + t] = r;
  }
}

} // namespace k

// 评估: x (口径 f, 只看池内 g ∧ xm 的格) + hd.n 组预处理标签 (设备指针) → rows[hd.n][T] (设备). ws = r16(x) 工作区 [T][A] (跨因子复用)
inline void eval(const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int T, int A, const Holds &hd, const Label *lab, uint16_t *ws,
                 Row *rows, cudaStream_t stream) {
  factor::stat::assert_holds(hd);
  assert(xv && xm && g && ws && rows);
  Batch B;
  B.C = 1, B.v[0] = xv, B.m[0] = xm;
  rank_x(B, g, f, T, A, ws, stream);
  k::label_rows<<<(T + kWarps - 1) / kWarps, kWB, 0, stream>>>(f, ws, label_set(hd, lab), hd, rows, T, A);
  FACTOR_CUDA_OK(cudaGetLastError());
}

} // namespace factor::gpu::stat
