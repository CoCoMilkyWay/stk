#pragma once

// =============================================================================
// Stat 评估算子的 GPU 后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; nvcc)
// =============================================================================
//   语义契约见 factor/Stat/Contract.hpp. 本文件与 Stat/Cpu.hpp **完全独立**, 只依赖 Contract + Stat/Dev.cuh (契约公式的 __device__ 版) + CUDA/CUB.
//
//   【布局】SoA 行主序 [T][A]; 一行 = 一个截面. rank_rows 一行一 block (grid = T, blockDim = kCB = 512, 每 SM 驻 2 块);
//           label_rows 一行一 warp (grid = ⌈T/kLW⌉, blockDim = kLB = 256).
//   【三个核】
//     rank_rows  (CS) 精确并列均秩: 一行的 (保序键, 资产号) 进 cub::BlockRadixSort (寄存器 + 片上), 排好写回 shared,
//                每个元素二分查 less / eq → r16. 无效键 = kNoKey 排最后, n = 首个 kNoKey 的位置.
//                ITEMS (每线程元素数) 按 A 分三档 2 / 6 / 10 (A ≤ 1024 / 3072 / 5120), shared 6 / 18 / 30 KB.
//                同一个核给因子 (float 平面) 与标签 (fp16 平面) 用, 只换取键器 SrcF / SrcH.
//     quant_rows (TS) 分位 x 直接量化 r16 (r16_quant), 逐格无关, grid-stride.
//     label_rows **一 warp 一行** (kLW = 8 行/block, 核内没有块级同步): 一行只有 A ≤ 5120 格, 一个 512 线程 block
//                  做 9 个持有期 × 4 次 BlockReduce 是 100 多道栅栏, 纯栅栏延迟瓶颈 (实测比 CPU 还慢一倍).
//                  逐持有期一遍 A 轴 (lane 跨步 32), 累加器全在寄存器 + 每 lane **独占**的 smem 分组槽
//                  ([组][lane] 布局, 32 lane 落 32 连续字, 无 bank 冲突; 每格 1 LDS + 1 STS, 不再 20 组展开 select):
//                  整数五和 (IC / rank-AC) 64 位 warp shuffle 归约 → 精确, 与 CPU 逐位一致;
//                  标签浮点和 走 float warp shuffle; 分组槽由 lane q 沿 32 lane 串行加 (旋转起点), 求和序固定 →
//                  GPU 自身逐次确定, 与 CPU 只差求和序 (对拍容差). 没有 shared atomic (float 原子不确定).
//   【禁用】--use_fast_math: pearson_int 的除法 / 开方要与 host 逐位一致.
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Dev.cuh"

#include <cassert>
#include <cstdint>
#include <cub/cub.cuh>
#include <cuda_fp16.h>
#include <cuda_runtime.h>

// =============================================================================
// TS/CS/Stat/Search 四个头共用的设备侧基件 (同一 TU 里多头都 include 时只展开一次)
// =============================================================================
#ifndef FACTOR_GPU_COMMON_CUH
#define FACTOR_GPU_COMMON_CUH

// 无错误处理, 只断言: 越早炸越好
#define FACTOR_CUDA_OK(call)                      \
  do {                                            \
    const cudaError_t e_ = (call);                \
    assert(e_ == cudaSuccess && "CUDA 调用失败"); \
    (void)e_;                                     \
  } while (0)

namespace factor::gpu {

// 设备侧 Val (SoA 两平面读出来的一格)
struct DVal {
  float v;
  uint8_t m;
};

namespace dev {

// ---- 与 Contract.hpp 逐字一致的 __device__ 版 (公式一个字都不能改) ----

// 有限性判据 (branchless): 有限 → v*0 == 0; inf/NaN → NaN != 0. 依赖 -fno-fast-math
__device__ __forceinline__ bool fin(float v) { return v * 0.f == 0.f; }

// 全并列: 有效样本极值 hi > lo (精确)
__device__ __forceinline__ bool spread(float lo, float hi) { return hi > lo; }
// 相消: den 相对于两侧量级 scale
__device__ __forceinline__ bool den_ok(double den, double scale) { return fabs(den) > static_cast<double>(kRelEps) * (scale + 1e-30); }
// 并列均秩的 pct rank: (avg_rank − 1)/(m − 1), m ≤ 1 → 0.5
__device__ __forceinline__ float pct_of(int less, int eq, int m) {
  if (m <= 1)
    return 0.5f;
  const double avg = static_cast<double>(less) + (static_cast<double>(eq) + 1.0) * 0.5;
  return static_cast<float>((avg - 1.0) / static_cast<double>(m - 1));
}
// 分桶 (double 除法, 与 host 逐位一致)
__device__ __forceinline__ int bin_of(float x, float lo, float hi) {
  const int b = static_cast<int>((static_cast<double>(x) - lo) / (static_cast<double>(hi) - lo) * kBuckets);
  return b < 0 ? 0 : (b >= kBuckets ? kBuckets - 1 : b);
}
__device__ __forceinline__ float bin_center(int b, float lo, float hi) {
  return static_cast<float>(lo + (static_cast<double>(hi) - lo) * (b + 0.5) / kBuckets);
}

// bin_of 的热路径版: sm_75 上 fp64 只有 1/32 速率, 每样本一次 double 除法会吃掉 HIST 族全部时间.
// float 快路径算出的 u 与 double 版的绝对偏差 ≤ ~3e-5 (u ∈ [0,256], 两次舍入),
// 故只有落在桶边界 ±1e-3 带内 (≈0.2% 样本) 才回退 double 定夺 → 桶号与 bin_of() 逐位一致.
__device__ __forceinline__ int bin_of_fast(float x, float lo, float hi) {
  const float u = (x - lo) / (hi - lo) * static_cast<float>(kBuckets);
  const float f = floorf(u);
  const float r = u - f;
  if (r < 1e-3f || r > 1.f - 1e-3f)
    return bin_of(x, lo, hi);
  const int b = static_cast<int>(f);
  return b < 0 ? 0 : (b >= kBuckets ? kBuckets - 1 : b);
}

// 出口 = Contract::mk(): 无效或非有限 (溢出) → {0, 0}
__device__ __forceinline__ void store(float *ov, uint8_t *om, int i, float v, bool m) {
  const bool f = m && fin(v);
  ov[i] = f ? v : 0.f;
  om[i] = f ? 1 : 0;
}

} // namespace dev

// 主机侧小工具 (避免依赖 <algorithm>)
inline int hmin(int a, int b) { return a < b ? a : b; }
inline int hmax(int a, int b) { return a > b ? a : b; }

} // namespace factor::gpu
#endif // FACTOR_GPU_COMMON_CUH

namespace factor::gpu::stat {

using factor::stat::Frame;
using factor::stat::Holds;
using factor::stat::kCloseAuction;
using factor::stat::kGroups;
using factor::stat::kMaxA;
using factor::stat::kMaxHold;
using factor::stat::kNoKey;
using factor::stat::kRankMax;
using factor::stat::kRankNone;
using factor::stat::Row;
using factor::stat::Side;

inline constexpr int kCB = 512;      // rank_rows 一行一 block 的线程数 (每 SM 驻 2 块)
inline constexpr int kLB = 256;      // label_rows 的 block 线程数: kLW 个 warp, 一 warp 一行
inline constexpr int kLW = kLB / 32; // label_rows 每 block 的行数

// 一组常驻标签 (全部设备指针, [T][A]); ry 由 prep_label 填
struct Label {
  const uint16_t *lv = nullptr;
  const uint16_t *sv = nullptr;
  const uint8_t *m = nullptr;
  const uint16_t *ry = nullptr;
};
struct LabelSet {
  int n = 0;
  Label l[kMaxHold];
};

namespace k {

// ---- 取键器: 一格 → 保序键 / kNoKey ----
struct SrcF { // 因子平面 (float), 只看池内 g ∧ m 的格
  const float *v;
  const uint8_t *m;
  const uint8_t *g;
  __device__ __forceinline__ bool on(size_t i) const { return (m[i] & g[i]) != 0; }
  __device__ __forceinline__ unsigned key(size_t i) const { return on(i) ? dev::ord(v[i]) : kNoKey; }
};
struct SrcH { // 标签平面 (fp16 位)
  const uint16_t *v;
  const uint8_t *m;
  __device__ __forceinline__ unsigned key(size_t i) const { return m[i] ? dev::ord(dev::h2f(v[i])) : kNoKey; }
};

// =============================================================================
// 1. rank_rows: 一行一 block, 片上基数排序 → 二分 less/eq → r16
// =============================================================================
template <int ITEMS, class Src>
__global__ void __launch_bounds__(kCB) rank_rows(Src src, uint16_t *out, int A) {
  constexpr int N = kCB * ITEMS;
  using Sort = cub::BlockRadixSort<unsigned, kCB, ITEMS, unsigned short>;
  union Sh { // 排序暂存与排好序的数组分时复用同一块 shared
    typename Sort::TempStorage sort;
    struct {
      unsigned key[N];
      unsigned short idx[N];
    } s;
  };
  __shared__ Sh sh;
  const size_t base = static_cast<size_t>(blockIdx.x) * A;
  unsigned keys[ITEMS];
  unsigned short idx[ITEMS];
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    const int a = threadIdx.x * ITEMS + j; // blocked 排布 (cub 要求)
    keys[j] = a < A ? src.key(base + a) : kNoKey;
    idx[j] = static_cast<unsigned short>(a);
  }
  Sort(sh.sort).Sort(keys, idx);
  __syncthreads(); // TempStorage 与排好序的数组是同一块 shared, 复用前必须全员过线
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    const int p = threadIdx.x * ITEMS + j;
    sh.s.key[p] = keys[j];
    sh.s.idx[p] = idx[j];
  }
  __syncthreads();
  const int n = dev::lower_bound(sh.s.key, N, kNoKey);     // 有效键数 (kNoKey 排最后; 有效键绝不等于它)
  const bool ok = n >= 2 && sh.s.key[n - 1] > sh.s.key[0]; // 全并列 = spread 为假
  for (int p = threadIdx.x; p < N; p += kCB) {
    const int a = sh.s.idx[p];
    if (a >= A)
      continue; // 补位
    unsigned short r = kRankNone;
    if (ok && p < n) {
      const unsigned key = sh.s.key[p];
      const int less = dev::lower_bound(sh.s.key, n, key);
      const int eq = dev::upper_bound(sh.s.key, n, key) - less;
      r = dev::r16_of(less, eq, n);
    }
    out[base + a] = r;
  }
}

// =============================================================================
// 1b. quant_rows (TS): 分位直接量化, 逐格
// =============================================================================
__global__ void __launch_bounds__(kCB) quant_rows(SrcF src, uint16_t *out, size_t n) {
  for (size_t i = static_cast<size_t>(blockIdx.x) * kCB + threadIdx.x; i < n; i += static_cast<size_t>(gridDim.x) * kCB)
    out[i] = src.on(i) ? dev::r16_quant(src.v[i]) : kRankNone;
}

// =============================================================================
// 2. label_rows: 每 (t, h) 一行统计
// =============================================================================

// warp 内 D 路求和 (每 lane 一份 v[D]), xor 蝶形 → 全 lane 拿到总和; 求和序固定 → 逐次确定
template <class T, int D>
__device__ __forceinline__ void warp_sum(T (&v)[D]) {
#pragma unroll
  for (int d = 0; d < D; ++d) {
#pragma unroll
    for (int off = 16; off > 0; off >>= 1)
      v[d] += __shfl_xor_sync(0xffffffffu, v[d], off);
  }
}

// 每 lane 独占的分组槽: 组和 float / 组计数 uint16 (≤ kMaxA), 布局 [warp][组][lane]
struct LabSh {
  float gs[kLW][kGroups][32];
  uint16_t gc[kLW][kGroups][32];
};

__global__ void __launch_bounds__(kLB, 2) label_rows(Frame f, const uint16_t *ws, LabelSet L, Holds hd, Row *rows, int T, int A) {
  using ull = unsigned long long;
  __shared__ LabSh sh;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int t = blockIdx.x * kLW + warp;
  if (t >= T) // warp 一致; 核内没有块级同步, 整 warp 直接退出安全
    return;
  const size_t base = static_cast<size_t>(t) * A;
  const uint16_t *rx = ws + base; // 行 r16 (H 个持有期重读, 2B/格, 落 L1/L2)
  float (*gs)[32] = sh.gs[warp];
  uint16_t (*gc)[32] = sh.gc[warp];

  for (int hi = 0; hi < hd.n; ++hi) { // warp 一致量, 不发散
    const int hold = hd.h[hi];
    const int h = dev::hold_minutes(hold);
    const Label &lb = L.l[hi];
    // ---- rank-AC (lag = h) ----
    ull ac[6] = {0, 0, 0, 0, 0, 0}; // n, Σx, Σy, Σx², Σy², Σxy
    if (t >= h) {
      const uint16_t *rp = ws + static_cast<size_t>(t - h) * A;
      for (int a = lane; a < A; a += 32) {
        const unsigned xr = rx[a], yr = rp[a];
        const bool ok = xr != kRankNone && yr != kRankNone;
        ac[0] += ok;
        ac[1] += ok ? xr : 0u;
        ac[2] += ok ? yr : 0u;
        ac[3] += ok ? xr * xr : 0u; // r16 ≤ 65534: 积 < 2³², 32 位乘后再加进 64 位
        ac[4] += ok ? yr * yr : 0u;
        ac[5] += ok ? xr * yr : 0u;
      }
      warp_sum(ac);
    }
    // ---- 标签侧 ----
    const bool tail = dev::tail_masked(t, hold);
    ull s[6] = {0, 0, 0, 0, 0, 0}; // n, Σx, Σy, Σx², Σy², Σxy (IC)
    float fs[3] = {};              // Σlv (mkt), Σsv, Σsv|组0
    if (!tail) {
#pragma unroll
      for (int q = 0; q < kGroups; ++q) { // 清自己的槽
        gs[q][lane] = 0.f;
        gc[q][lane] = 0;
      }
      const uint16_t *ry = lb.ry + base, *lv = lb.lv + base, *sv = lb.sv + base;
      for (int a = lane; a < A; a += 32) {
        const unsigned xr = rx[a], yr = ry[a];
        const bool ok = xr != kRankNone && yr != kRankNone;
        s[0] += ok;
        s[1] += ok ? xr : 0u;
        s[2] += ok ? yr : 0u;
        s[3] += ok ? xr * xr : 0u;
        s[4] += ok ? yr * yr : 0u;
        s[5] += ok ? xr * yr : 0u;
        const float yl = dev::h2f(lv[a]), ys = dev::h2f(sv[a]);
        fs[0] += ok ? yl : 0.f;
        fs[1] += ok ? ys : 0.f;
        const int g = ok ? dev::grp_of(xr) : -1; // 20 组 (顶 / 底侧的 lv 与计数就是组槽, 另记底组的 Σ sv)
        if (ok) {                                // 自己的槽, 无竞争
          gs[g][lane] += yl;
          gc[g][lane] += 1;
        }
        fs[2] += g == 0 ? ys : 0.f;
      }
      warp_sum(s);
      warp_sum(fs);
      __syncwarp();         // 各 lane 的槽写完, 下面跨 lane 读
      if (lane < kGroups) { // lane q 收组 q: 沿 32 lane 串行加, 起点旋转 → 32 个 lane 落 32 个 bank
        float sg = 0.f;
        int cg = 0;
#pragma unroll
        for (int l = 0; l < 32; ++l) {
          const int j = (l + lane) & 31;
          sg += gs[lane][j];
          cg += gc[lane][j];
        }
        gs[lane][0] = sg; // 总和放回 0 号槽, 给 lane 0 收尾
        gc[lane][0] = static_cast<uint16_t>(cg);
      }
      __syncwarp();
    }
    if (lane == 0) {
      Row r;
      if (t >= h) {
        bool ok = false;
        r.ac = dev::pearson_int(ac[0], ac[1], ac[2], ac[3], ac[4], ac[5], ok);
        r.ok_ac = ok ? 1 : 0;
      }
      if (!tail) {
        double gsd[kGroups];
        int gci[kGroups];
#pragma unroll
        for (int q = 0; q < kGroups; ++q) {
          gsd[q] = static_cast<double>(gs[q][0]);
          gci[q] = gc[q][0];
        }
        constexpr int kt = kGroups - 1;
        const Side top{static_cast<double>(gci[kt]), gsd[kt], 0.0}, bot{static_cast<double>(gci[0]), gsd[0], static_cast<double>(fs[2])};
        dev::finish_row(f, s[0], s[1], s[2], s[3], s[4], s[5], static_cast<double>(fs[0]), static_cast<double>(fs[1]), gsd, gci, top, bot, r);
      }
      rows[static_cast<size_t>(hi) * T + t] = r;
    }
    __syncwarp(); // 下一持有期清槽前, lane 0 读完
  }
}

} // namespace k

// =============================================================================
// 宿主接口 (全部设备指针; 拷入拷出由调用方负责 —— 挖掘侧数据常驻显存)
// =============================================================================

// 工作区: rank(x) [T][A] uint16 (跨因子复用)
inline size_t ws_bytes(int T, int A) { return sizeof(uint16_t) * static_cast<size_t>(T) * A; }

template <class Src>
inline void rank_launch(const Src &src, uint16_t *out, int T, int A, cudaStream_t stream) {
  assert(T >= 1 && A >= 1 && A <= kMaxA && static_cast<long long>(T) * A < (1LL << 31));
  if (A <= kCB * 2)
    k::rank_rows<2, Src><<<T, kCB, 0, stream>>>(src, out, A);
  else if (A <= kCB * 6)
    k::rank_rows<6, Src><<<T, kCB, 0, stream>>>(src, out, A);
  else
    k::rank_rows<10, Src><<<T, kCB, 0, stream>>>(src, out, A);
  FACTOR_CUDA_OK(cudaGetLastError());
}

// 标签预处理: 做多标签逐行 r16 → ry. 常驻期算一次
inline void prep_label(const uint16_t *lv, const uint8_t *m, int T, int A, uint16_t *ry, cudaStream_t stream) {
  rank_launch(k::SrcH{lv, m}, ry, T, A, stream);
}

// 评估: x (口径 f, 只看池内 g ∧ xm 的格; 契约【截面池 g】) + L.n 组预处理标签 → rows[L.n][T] (设备)
inline void eval(const float *xv, const uint8_t *xm, const uint8_t *g, Frame f, int T, int A, const Holds &hd, const LabelSet &L, uint16_t *ws,
                 Row *rows, cudaStream_t stream) {
  factor::stat::assert_holds(hd);
  assert(g && L.n == hd.n);
  for (int i = 0; i < L.n; ++i)
    assert(L.l[i].lv && L.l[i].sv && L.l[i].m && L.l[i].ry && "标签未预处理 (prep_label)");
  if (f == Frame::CS) {
    rank_launch(k::SrcF{xv, xm, g}, ws, T, A, stream);
  } else {
    const size_t n = static_cast<size_t>(T) * A;
    const int grid = static_cast<int>(hmin(static_cast<int>((n + kCB - 1) / kCB), 4096));
    k::quant_rows<<<grid, kCB, 0, stream>>>(k::SrcF{xv, xm, g}, ws, n);
    FACTOR_CUDA_OK(cudaGetLastError());
  }
  k::label_rows<<<(T + kLW - 1) / kLW, kLB, 0, stream>>>(f, ws, L, hd, rows, T, A);
  FACTOR_CUDA_OK(cudaGetLastError());
}

} // namespace factor::gpu::stat
