#pragma once

// =============================================================================
// Stat 评估算子的 GPU 后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; nvcc)
// =============================================================================
//   语义契约见 factor/Stat/Contract.hpp, 行核见 Stat/Dev.cuh (rank_rows / quant_rows / row_stat). 本文件与 Stat/Cpu.hpp 完全独立,
//   流程同名同参序 (契约【视图】; 出口 Accum[H], 二级 finish_all 在宿主):
//     prep_label / rank_x (Dev.cuh) · accum_rows (label_rows → fold_rows → reduce_parts) · run = rank_x + accum_rows · leg_codes
//   【label_rows】grid ⌈(T − t_lo)/kWarps⌉ × kWB, **一 warp 一行 t**, 持有期在 warp 内串行 (rx 行一次进寄存器 / L1, H 组标签各读一遍),
//     每 (t, h) 一次 dev::row_stat → lane 0 落 rows[h][t] (设备). 核内没有块级同步, t ≥ T 的 warp 直接退出.
//     行先落设备再折, 不在 warp 内攒: 攒要么每 h 一块重读 rx 行 (H 倍 rx 流量), 要么 H 份累加器压寄存器; 落行的读写远小于前者.
//   【fold_rows】一 warp 一 (h, 行块 tb) (kRows 行): 契约 Accum 的两半, lane 0 攒 Accum::Head, lane q < kGroups 攒 Accum::Grp[q]
//     (调的就是契约的 add) → 分片 part[h][tb]. 【reduce_parts】每 h 一线程沿 tb 升序 merge (固定序 → 逐次确定) → acc[h].
//     宿主只拷回 acc[H] (+ 要时 rows / lc), 不再拷一级回来做二级.
//   【leg_codes】rx → 腿码 lc (契约 leg_code 逐格): 换手成本在宿主算 (成本平面上显存装不下), 只回传 u8 腿码, 不回 u16 rx.
//   【禁用】--use_fast_math.
// =============================================================================

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Dev.cuh"

#include <cassert>
#include <cstdint>
#include <cuda_runtime.h>

namespace factor::gpu::stat {

using factor::stat::Accum;
using factor::stat::Labels;
using factor::stat::Plane;

inline constexpr int kRows = 128; // fold_rows 每 warp 的行块长 (分片粒度)

// 设备工作区 (调用方按 Span / H 分配, 指针归调用方). NB = n_parts(s) 是 t_lo = 0 时的上界
struct Workspace {
  Row *rows = nullptr;   // [H][T] 一级 (fold 的输入; 调用方要就拷回)
  Accum *part = nullptr; // [H][NB] 分片
  Accum *acc = nullptr;  // [H] 出口
  int H = 0, NB = 0;     // 分配时的容量
};
inline int n_parts(const Span &s) { return (s.T - s.t_lo + kRows - 1) / kRows; }

namespace k {

__global__ void __launch_bounds__(kWB, 2) label_rows(Span s, const uint16_t *rx, Labels L, Row *rows) {
  __shared__ Slots sh;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int t = s.t_lo + blockIdx.x * kWarps + warp;
  if (t >= s.T) // warp 一致; 核内没有块级同步, 整 warp 直接退出安全
    return;
  const size_t base = static_cast<size_t>(t) * s.A;
  const uint16_t *xr = rx + base;
  for (int hi = 0; hi < L.hd.n; ++hi) { // warp 一致量, 不发散
    const int hold = L.hd.k[hi], h = factor::stat::hold_rows(hold, s.g);
    const Row r = dev::row_stat(xr, t >= h ? xr - static_cast<size_t>(h) * s.A : nullptr, L.l[hi].at(base),
                                factor::stat::chain_of(t, s.T, s.A, hold, s.g, s.t0_abs), L.cap, factor::stat::tail_masked(s.t0_abs + t, hold, s.g),
                                sh.gs[warp], sh.gc[warp], lane);
    if (lane == 0)
      rows[static_cast<size_t>(hi) * s.T + t] = r;
  }
}

__global__ void __launch_bounds__(kWB) fold_rows(Span s, const Row *rows, int H, int NB, Accum *part) {
  const int lane = threadIdx.x & 31, w = blockIdx.x * kWarps + (threadIdx.x >> 5);
  if (w >= H * NB) // warp 一致
    return;
  const int hi = w / NB, tb = w - hi * NB;
  const int t_beg = s.t_lo + tb * kRows, t_end = min(s.T, t_beg + kRows);
  const Row *r = rows + static_cast<size_t>(hi) * s.T;
  Accum::Head head;
  Accum::Grp grp;
  for (int t = t_beg; t < t_end; ++t) {
    if (lane == 0)
      head.add(r[t]);
    if (lane < kGroups && r[t].ok)
      grp.add(r[t].grp[lane], r[t].cnt[lane]);
  }
  Accum &P = part[static_cast<size_t>(hi) * NB + tb]; // 每字段恰由一个 lane 写
  if (lane < kGroups)
    P.grp[lane] = grp;
  if (lane == 0)
    P.head = head;
}

__global__ void __launch_bounds__(kWB) reduce_parts(const Accum *part, int H, int NB, Accum *acc) {
  const int hi = blockIdx.x * kWB + threadIdx.x;
  if (hi >= H)
    return;
  Accum a;
  for (int tb = 0; tb < NB; ++tb)
    a.merge(part[static_cast<size_t>(hi) * NB + tb]);
  acc[hi] = a;
}

__global__ void __launch_bounds__(kRB) leg_codes(const uint16_t *rx, uint8_t *lc, size_t n) {
  for (size_t i = static_cast<size_t>(blockIdx.x) * kRB + threadIdx.x; i < n; i += static_cast<size_t>(gridDim.x) * kRB)
    lc[i] = factor::stat::leg_code(rx[i]);
}

} // namespace k

// 阶段 2: rx × L 组预处理标签 (设备) → 行 [t_lo,T) 落 w.rows[h][t] → w.acc[hd.n] (设备, 覆盖)
inline void accum_rows(const Span &s, const uint16_t *rx, const Labels &L, const Workspace &w, cudaStream_t stream) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, true);
  const int H = L.hd.n, NB = n_parts(s);
  assert(rx && w.rows && w.part && w.acc && H <= w.H && NB <= w.NB);
  k::label_rows<<<(s.T - s.t_lo + kWarps - 1) / kWarps, kWB, 0, stream>>>(s, rx, L, w.rows);
  FACTOR_CUDA_OK(cudaGetLastError());
  k::fold_rows<<<(H * NB + kWarps - 1) / kWarps, kWB, 0, stream>>>(s, w.rows, H, NB, w.part);
  FACTOR_CUDA_OK(cudaGetLastError());
  k::reduce_parts<<<(H + kWB - 1) / kWB, kWB, 0, stream>>>(w.part, H, NB, w.acc);
  FACTOR_CUDA_OK(cudaGetLastError());
}

// 一个候选的完整评估 = rank_x (→ rx) + accum_rows → w.acc[hd.n] (设备). rx = r16(x) 工作区 [T][A] (跨因子复用)
inline void run(Plane x, const uint8_t *g, Frame f, const Span &s, const Labels &L, uint16_t *rx, const Workspace &w, cudaStream_t stream) {
  Batch B;
  B.C = 1, B.x[0] = x;
  rank_x(B, g, f, s, rx, stream);
  accum_rows(s, rx, L, w, stream);
}

// 腿码: rx → lc [T][A] (设备)
inline void leg_codes(const uint16_t *rx, const Span &s, uint8_t *lc, cudaStream_t stream) {
  factor::stat::assert_span(s);
  assert(rx && lc);
  const size_t n = s.n(), nb = (n + kRB - 1) / kRB;
  k::leg_codes<<<static_cast<unsigned>(nb < 4096 ? nb : 4096), kRB, 0, stream>>>(rx, lc, n);
  FACTOR_CUDA_OK(cudaGetLastError());
}

} // namespace factor::gpu::stat
