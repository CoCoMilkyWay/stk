#pragma once

// =============================================================================
// Search 评估算子的 GPU 后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; nvcc)
// =============================================================================
//   契约 factor/Search/Contract.hpp + Stat/Contract.hpp; 行核 Stat/Dev.cuh (rank_x / row_stat, 与 Stat GPU 同一组). 本文件只加:
//   【stat_batch】grid (⌈C/kWarps⌉, NB, H) × kWB, **一 warp 一候选**, 一块管 kRows 行 × 一个持有期 (grid.z):
//     每行一次 row_stat (全 lane 拿到 Row) → **直接折进 warp 私有的分片累加器** (契约 Accum 的两半: lane 0 攒 Accum::Head,
//     lane k < 20 攒 Accum::Grp[k]; 调的就是契约的 add) → 块末写分片 Slice[c][h][tb] (= 契约的 Accum 原样). 不落 Row.
//     同 block 的 kWarps 个候选同一 (t, h): ry / lv / sv 行一次进 L1 共用, 标签流量被候选数摊薄. 核内没有块级同步, 没有原子.
//   【reduce_slices】每 (c, h) 一线程, 沿 tb 升序 merge (契约 merge 原样; 固定序 → GPU 自身逐次确定) → 直接写 Result[c] (设备),
//     主机整块拷回, 无转换.
//   【与 CPU 的差别】整数派生量逐位一致; 浮点和只差求和序 (对拍容差).
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
using factor::gpu::stat::Slots;
using factor::search::Result;
using factor::stat::Accum;
using factor::stat::Frame;
using factor::stat::kGroups;
using factor::stat::kMaxA;
using factor::stat::kMaxHold;
using factor::stat::Label;
using factor::stat::Labels;
using factor::stat::Row;
using factor::stat::Span;

inline constexpr int kRows = 128; // stat_batch 每 block 的行数 (分片粒度; NB = ⌈(T − t_lo)/kRows⌉)

// 一个 (候选, 持有期, 行块) 的分片 = 契约累加器原样
struct Slice {
  Accum acc;
};

namespace k {

__global__ void __launch_bounds__(kWB, 2) stat_batch(const uint16_t *rx, int C, int T, int A, Geo g, int t0_abs, int t_lo, Labels L, int NB,
                                                     Slice *part) {
  const auto &hd = L.hd;
  __shared__ Slots sh;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int c = blockIdx.x * kWarps + warp;
  if (c >= C) // warp 一致; 核内没有块级同步, 整 warp 直接退出安全
    return;
  const int hi = blockIdx.z, hold = hd.h[hi], h = factor::stat::hold_rows(hold, g);
  const Label lb = L.l[hi];
  const int tb = blockIdx.y, t_beg = t_lo + tb * kRows, t_end = min(T, t_beg + kRows);
  const uint16_t *rxc = rx + static_cast<size_t>(c) * T * A;

  // warp 私有分片 (= 契约 Accum 的两半: lane 0 攒 head, lane q < kGroups 攒 grp = 组 q)
  Accum::Head head;
  Accum::Grp grp;

  for (int t = t_beg; t < t_end; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    const uint16_t *xr = rxc + base;
    const uint16_t *xl = t >= h ? xr - static_cast<size_t>(h) * A : nullptr;
    const Row r = dev::row_stat(xr, xl, lb.at(base), A, L.cap, factor::stat::tail_masked(t0_abs + t, hold, g), sh.gs[warp], sh.gc[warp], lane);
    if (lane == 0)
      head.add(r);
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

  // ---- 写分片 (每字段恰由一个 lane 写) ----
  Slice &P = part[(static_cast<size_t>(c) * hd.n + hi) * NB + tb];
  if (lane < kGroups)
    P.acc.grp[lane] = grp;
  if (lane == 0)
    P.acc.head = head;
}

// reduce_slices: 每 (c, h) 一线程, 沿 tb 升序 merge → out[c].acc[h]
__global__ void __launch_bounds__(kWB) reduce_slices(const Slice *part, int NB, int C, int H, Result *out) {
  const int p = blockIdx.x * kWB + threadIdx.x;
  if (p >= C * H)
    return;
  const int c = p / H, hi = p - c * H;
  const Slice *src = part + static_cast<size_t>(p) * NB;
  Slice s;
  for (int tb = 0; tb < NB; ++tb)
    s.acc.merge(src[tb].acc);
  out[c].acc[hi] = s.acc;
}

} // namespace k

// =============================================================================
// 宿主接口 (全部设备指针; 拷入拷出由调用方负责 —— 搜索侧候选平面常驻显存)
// =============================================================================

// 一次 eval 的形状与工作区尺寸 (调用方按它分配: rx / part / out); g = 因子所在层 (seg 定)
struct Work {
  int C = 0, T = 0, A = 0, H = 0, t_lo = 0, NB = 0;
  Geo g;
  size_t rx_elems() const { return static_cast<size_t>(C) * T * A; }
  size_t part_elems() const { return static_cast<size_t>(C) * H * NB; }
  size_t out_elems() const { return static_cast<size_t>(C); }
};
inline Work plan(int C, int T, int A, int seg, int H, int t_lo) {
  assert(C >= 1 && C <= kMaxBatch && T >= 1 && A >= 1 && A <= kMaxA && static_cast<long long>(T) * A < (1LL << 31));
  assert(H >= 1 && H <= kMaxHold && t_lo >= 0 && t_lo < T && is_seg(seg));
  Work w;
  w.C = C, w.T = T, w.A = A, w.H = H, w.t_lo = t_lo, w.g = geo_of(seg);
  w.NB = (T - t_lo + kRows - 1) / kRows;
  return w;
}

// 一批候选的评估: B (口径 f, 只看池内 g ∧ m) + L (L.hd.n 组预处理标签, 设备指针) → out[C] (设备 Result). 契约【分块流式】的 t0_abs / t_lo
inline void eval(const Batch &B, const uint8_t *g, Frame f, const Work &w, int t0_abs, const Labels &L, uint16_t *rx, Slice *part, Result *out,
                 cudaStream_t stream) {
  factor::stat::assert_labels(L, w.g.S, true);
  assert(B.C == w.C && L.hd.n == w.H && g && rx && part && out && t0_abs >= 0);
  factor::gpu::stat::rank_x(B, g, f, Span{w.T, w.A, w.g, t0_abs, w.t_lo}, rx, stream);
  const dim3 grid(static_cast<unsigned>((w.C + kWarps - 1) / kWarps), static_cast<unsigned>(w.NB), static_cast<unsigned>(w.H));
  k::stat_batch<<<grid, kWB, 0, stream>>>(rx, w.C, w.T, w.A, w.g, t0_abs, w.t_lo, L, w.NB, part);
  FACTOR_CUDA_OK(cudaGetLastError());
  const int P = w.C * w.H;
  k::reduce_slices<<<(P + kWB - 1) / kWB, kWB, 0, stream>>>(part, w.NB, w.C, w.H, out);
  FACTOR_CUDA_OK(cudaGetLastError());
}

} // namespace factor::gpu::search
