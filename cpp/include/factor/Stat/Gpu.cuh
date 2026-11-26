#pragma once

// =============================================================================
// Stat 评估算子的 GPU 后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; nvcc)
// =============================================================================
//   语义契约见 factor/Stat/Contract.hpp, 行核见 Stat/Dev.cuh (rank_rows / quant_rows / row_stat). 本文件与 Stat/Cpu.hpp 完全独立,
//   流程同名同参序 (出口 Accum[C][H] + Day[C][H][D], 三级 finish_all 在宿主). 两种排布, 由候选数定:
//   【单候选 C = 1: 逐行并行】
//     label_rows   grid ⌈(t_hi − t_lo)/kWarps⌉ × kWB, 一 warp 一行 t, 持有期在 warp 内串行 → rows[h][t] (设备)
//     fold_rows    一 warp 一 (h, 行块 tb): lane 0 攒 Accum::Head, lane q < kGroups 攒 Accum::Grp[q] → 分片 part[h][tb]
//     fold_days    一线程一 (h, 日): 日内各行顺序累加 → day[h][d] (求和序同 CPU, 逐位一致)
//     reduce_parts 每 h 一线程沿 tb 升序 merge (固定序 → 逐次确定) → acc[h]
//   【批 C > 1: 逐候选并行】
//     stat_batch   grid (⌈C/kWarps⌉, NB, H) × kWB, 一 warp 一候选, 一块管整日的 R 行 × 一个持有期:
//                  每行一次 row_stat → 折进 warp 私有的分片累加器 (Accum 两半) + lane 0 顺序攒当日 Day → 分片 Slice[c][h][tb] / day[c][h][d].
//                  同 block 的 kWarps 个候选同一 (t, h): 标签行一次进 L1 共用. 不落 Row.
//     reduce_slices 每 (c, h) 一线程沿 tb 升序 merge → acc[c][h]
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

using factor::stat::Accum;
using factor::stat::Day;
using factor::stat::Labels;

inline constexpr int kRows = 128; // 分片粒度 (行); 批核一块 = 整日的 ≥ kRows / S 日
// 批核一块的行数: 整日对齐的 R = S · max(1, ⌊kRows / S⌋)
inline int batch_rows(int S) { return S * (kRows / S > 0 ? kRows / S : 1); }

// 设备工作区 (调用方按 Span / H / Cmax 分配, 指针归调用方). NB / NBB = t_lo = t_back = 0 时的上界
struct Workspace {
  Row *rows = nullptr;   // [H][T] 一级 (单候选)
  Accum *part = nullptr; // [Cmax][H][NBB] 分片 (两种排布共用; 单候选用 [H][NB])
  Accum *acc = nullptr;  // [Cmax][H] 出口
  Day *day = nullptr;    // [Cmax][H][D] 出口
  int H = 0, NB = 0, NBB = 0, Cmax = 0, D = 0;
};
inline int n_parts(const Span &s) { return (s.t_hi() - s.t_lo + kRows - 1) / kRows; }
inline int n_batch_parts(const Span &s) { return (s.t_hi() - s.t_lo + batch_rows(s.g.S) - 1) / batch_rows(s.g.S); }

namespace k {

__global__ void __launch_bounds__(kWB, 2) label_rows(Span s, const uint16_t *rx, Labels L, Row *rows) {
  __shared__ Slots sh;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int t = s.t_lo + blockIdx.x * kWarps + warp;
  if (t >= s.t_hi()) // warp 一致; 核内没有块级同步, 整 warp 直接退出安全
    return;
  const size_t base = static_cast<size_t>(t) * s.A;
  const uint16_t *xr = rx + base;
  for (int hi = 0; hi < L.hd.n; ++hi) { // warp 一致量, 不发散
    const Hold hd = hold_on(L.hd.k[hi], s.g);
    const Row r = dev::row_stat(xr, t >= hd.h ? xr - static_cast<size_t>(hd.h) * s.A : nullptr, L.l[hi].at(base), factor::stat::chain_of(t, s, hd), L.cap,
                                hd.opens(s.t0_abs + t), sh.gs[warp], sh.gc[warp], lane);
    if (lane == 0)
      rows[static_cast<size_t>(hi) * s.T + t] = r;
  }
}

__global__ void __launch_bounds__(kWB) fold_rows(Span s, const Row *rows, int H, int NB, Accum *part) {
  const int lane = threadIdx.x & 31, w = blockIdx.x * kWarps + (threadIdx.x >> 5);
  if (w >= H * NB) // warp 一致
    return;
  const int hi = w / NB, tb = w - hi * NB;
  const int t_beg = s.t_lo + tb * kRows, t_end = min(s.t_hi(), t_beg + kRows);
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

__global__ void __launch_bounds__(kWB) fold_days(Span s, const Row *rows, int H, Day *day) {
  const int D = s.days(), p = blockIdx.x * kWB + threadIdx.x;
  if (p >= H * D)
    return;
  const int hi = p / D, d = p - hi * D, S = s.g.S;
  const Row *r = rows + static_cast<size_t>(hi) * s.T + s.t_lo + d * S;
  Day o;
  for (int t = 0; t < S; ++t)
    o.add(r[t]);
  day[p] = o;
}

__global__ void __launch_bounds__(kWB) reduce_parts(const Accum *part, int P, int NB, Accum *acc) {
  const int p = blockIdx.x * kWB + threadIdx.x;
  if (p >= P)
    return;
  Accum a;
  for (int tb = 0; tb < NB; ++tb)
    a.merge(part[static_cast<size_t>(p) * NB + tb]);
  acc[p] = a;
}

__global__ void __launch_bounds__(kWB, 2) stat_batch(const uint16_t *rx, int C, Span s, Labels L, int NB, Accum *part, Day *day) {
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
  Day *dy = day + (static_cast<size_t>(c) * H + hi) * D;

  Accum::Head head; // warp 私有分片 (lane 0 攒 head, lane q < kGroups 攒 grp = 组 q)
  Accum::Grp grp;
  Day cur;
  for (int t = t_beg; t < t_end; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    const uint16_t *xr = rxc + base;
    const uint16_t *xl = t >= h ? xr - static_cast<size_t>(h) * A : nullptr;
    const Row r = dev::row_stat(xr, xl, lb.at(base), factor::stat::chain_of(t, s, hd), L.cap, hd.opens(s.t0_abs + t), sh.gs[warp], sh.gc[warp], lane);
    if (lane == 0) {
      head.add(r);
      cur.add(r);
      if ((t - s.t_lo + 1) % S == 0) { // 日末: 落当日
        dy[(t - s.t_lo) / S] = cur;
        cur = Day{};
      }
    }
    if (r.ok) {
      float gq = 0.f; // lane q 取 grp[q] / cnt[q] (编译期展开的 select, Row 留在寄存器)
      unsigned cq = 0;
#pragma unroll
      for (int q = 0; q < kGroups; ++q)
        if (q == lane)
          gq = r.grp[q], cq = r.cnt[q];
      grp.add(gq, cq);
    }
  }
  Accum &P = part[(static_cast<size_t>(c) * H + hi) * NB + tb]; // 每字段恰由一个 lane 写
  if (lane < kGroups)
    P.grp[lane] = grp;
  if (lane == 0)
    P.head = head;
}

} // namespace k

// 单候选: rx × L 组预处理标签 (设备) → 统计行 [t_lo, t_hi) 落 w.rows[h][t] → w.acc[hd.n] + w.day[hd.n][D] (设备, 覆盖)
inline void accum_rows(const Span &s, const uint16_t *rx, const Labels &L, const Workspace &w, cudaStream_t stream) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, true);
  const int H = L.hd.n, NB = n_parts(s), D = s.days();
  assert(rx && w.rows && w.part && w.acc && w.day && H <= w.H && NB <= w.NB && D <= w.D);
  k::label_rows<<<(s.t_hi() - s.t_lo + kWarps - 1) / kWarps, kWB, 0, stream>>>(s, rx, L, w.rows);
  FACTOR_CUDA_OK(cudaGetLastError());
  k::fold_rows<<<(H * NB + kWarps - 1) / kWarps, kWB, 0, stream>>>(s, w.rows, H, NB, w.part);
  FACTOR_CUDA_OK(cudaGetLastError());
  k::fold_days<<<(H * D + kWB - 1) / kWB, kWB, 0, stream>>>(s, w.rows, H, w.day);
  FACTOR_CUDA_OK(cudaGetLastError());
  k::reduce_parts<<<(H + kWB - 1) / kWB, kWB, 0, stream>>>(w.part, H, NB, w.acc);
  FACTOR_CUDA_OK(cudaGetLastError());
}

// 批: C 张已定向的 rx[c] × L → w.acc[c][h] + w.day[c][h][D] (设备, 覆盖)
inline void accum_batch(const Span &s, const uint16_t *rx, int C, const Labels &L, const Workspace &w, cudaStream_t stream) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, true);
  const int H = L.hd.n, NB = n_batch_parts(s);
  assert(rx && w.part && w.acc && w.day && C >= 1 && C <= w.Cmax && H <= w.H && NB <= w.NBB && s.days() <= w.D);
  const dim3 grid(static_cast<unsigned>((C + kWarps - 1) / kWarps), static_cast<unsigned>(NB), static_cast<unsigned>(H));
  k::stat_batch<<<grid, kWB, 0, stream>>>(rx, C, s, L, NB, w.part, w.day);
  FACTOR_CUDA_OK(cudaGetLastError());
  k::reduce_parts<<<(C * H + kWB - 1) / kWB, kWB, 0, stream>>>(w.part, C * H, NB, w.acc);
  FACTOR_CUDA_OK(cudaGetLastError());
}

} // namespace factor::gpu::stat
