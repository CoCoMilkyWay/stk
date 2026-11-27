#pragma once

// =============================================================================
// Stat 评估算子的 GPU 后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; nvcc)
// =============================================================================
//   语义契约见 factor/Stat/Contract.hpp, 行核见 Stat/Dev.cuh (rank_rows / quant_rows / row_stat). 本文件与 Stat/Cpu.hpp 完全独立,
//   流程同名同参序 (出口逐日 Sum[C][H][D]; 三级 = 宿主 Fold stat_of 按日序 merge, 与 CPU 同序). 唯一的累加核:
//     stat_days  grid (⌈C · NB / kWarps⌉, H) × kWB, 一 warp 一 (候选, 整日段, 持有期): 段内逐行 row_stat → 当日 Sum (lane 0 攒头, lane q 攒组 q,
//                与 Sum::add 同式) → day[c][h][d]; 段长 = 一日 (C = 1: 按日撒满 warp) / 整日 R ≥ kRows (C > 1: 同块 kWarps 个候选同一 (t, h),
//                标签行一次进 L1 共用). C = 1 另可落一级 rows[h][t]. 日内行序累加, 与 CPU 求和序同
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
using factor::stat::Ret;
using factor::stat::Sum;

inline constexpr int kRows = 128; // 批 (C > 1) 一段 = 整日的 ≥ kRows / S 日
// 一段的行数: 一日 (C = 1) / 整日对齐的 R = S · max(1, ⌊kRows / S⌋) (C > 1)
inline int part_rows(int S, int C) { return C == 1 ? S : S * (kRows / S > 0 ? kRows / S : 1); }
inline int n_parts(const Span &s, int C) { return (s.t_hi() - s.t_lo + part_rows(s.g.S, C) - 1) / part_rows(s.g.S, C); }

// 设备工作区 (调用方按 Span / H / Cmax 分配, 指针归调用方)
struct Workspace {
  Row *rows = nullptr;  // [H][T] 一级 (单候选)
  float *ric = nullptr; // [H][T] 逐行 rIC (单候选; 不 ok = NaN): 只要 rIC 时不搬整行
  Sum *day = nullptr;   // [Cmax][H][D] 出口
  int H = 0, Cmax = 0, D = 0;
};

namespace k {

__global__ void __launch_bounds__(kWB, 2) stat_days(const uint16_t *rx, int C, Span s, Labels L, Ret ret, int R, int NB, Sum *day, Row *rows, float *ric) {
  __shared__ Slots sh;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int w = blockIdx.x * kWarps + warp;
  if (w >= C * NB) // warp 一致; 核内没有块级同步, 整 warp 直接退出安全
    return;
  const int c = w % C, tb = w / C;
  const int A = s.A, H = L.hd.n, hi = blockIdx.y, S = s.g.S, D = s.days();
  const Hold hd = hold_on(L.hd.k[hi], s.g);
  const int h = hd.h;
  const Label lb = L.l[hi];
  const int t_beg = s.t_lo + tb * R, t_end = min(s.t_hi(), t_beg + R);
  const uint16_t *rxc = rx + static_cast<size_t>(c) * s.n();
  Sum *dy = day + (static_cast<size_t>(c) * H + hi) * D;

  Sum::Head head;          // 当日: lane 0 攒头
  double gd = 0.0, cd = 0; // 当日: lane q < kGroups 攒组 q
  for (int t = t_beg; t < t_end; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    const uint16_t *xr = rxc + base;
    const uint16_t *xl = t >= h ? xr - static_cast<size_t>(h) * A : nullptr;
    const Row r = dev::row_stat(xr, xl, lb.at(base), L.eo + base, factor::stat::chain_of(t, s, hd), hd.opens(t), sh.gs[warp], sh.gc[warp], lane);
    if (lane == 0) {
      head.add(r, ret);
      if (rows)
        rows[static_cast<size_t>(hi) * s.T + t] = r;
      if (ric)
        ric[static_cast<size_t>(hi) * s.T + t] = r.ok ? r.ric : __int_as_float(0x7fc00000);
    }
    if (r.ok) {
      float gq = 0.f; // lane q 取 grp[q] / cnt[q] (编译期展开的 select, Row 留在寄存器)
      unsigned cq = 0;
#pragma unroll
      for (int q = 0; q < kGroups; ++q)
        if (q == lane)
          gq = r.grp[q], cq = r.cnt[q];
      gd += static_cast<float>(factor::stat::ret_as(gq, cq, r.mkt, r.cap, ret)), cd += cq;
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

// C 张已定向的 rx[c] × L 组预处理标签 (设备, 收益口径 ret) → 统计行 [t_lo, t_hi) 的 w.day[c][hd.n][D] (设备, 覆盖);
//   rows / ric (只 C = 1) → 另落 w.rows[h][t] 整行 / w.ric[h][t] 逐行 rIC
inline void accum(const Span &s, const uint16_t *rx, int C, const Labels &L, Ret ret, const Workspace &w, bool rows, bool ric, cudaStream_t stream) {
  factor::stat::assert_span(s);
  factor::stat::assert_labels(L, s.g.S, true);
  const int H = L.hd.n, R = part_rows(s.g.S, C), NB = n_parts(s, C);
  assert(rx && w.day && C >= 1 && C <= w.Cmax && H <= w.H && s.days() <= w.D && (!rows || (C == 1 && w.rows)) && (!ric || (C == 1 && w.ric)));
  const dim3 grid(static_cast<unsigned>((C * NB + kWarps - 1) / kWarps), static_cast<unsigned>(H));
  k::stat_days<<<grid, kWB, 0, stream>>>(rx, C, s, L, ret, R, NB, w.day, rows ? w.rows : nullptr, ric ? w.ric : nullptr);
  FACTOR_CUDA_OK(cudaGetLastError());
}

} // namespace factor::gpu::stat
