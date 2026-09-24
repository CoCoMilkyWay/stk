#pragma once

// =============================================================================
// Stat 评估算子的 GPU 后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; nvcc)
// =============================================================================
//   语义契约见 factor/Stat/Contract.hpp. 本文件与 Stat/Cpu.hpp **完全独立**, 只依赖 Contract + CUDA/CUB.
//
//   【布局】SoA 行主序 [T][A]; 一行 = 一个截面, 一行一 block (grid = T, blockDim = kCB = 512, 每 SM 驻 2 块).
//   【三个核】
//     rank_rows  (CS) 精确并列均秩: 一行的 (保序键, 资产号) 进 cub::BlockRadixSort (寄存器 + 片上), 排好写回 shared,
//                每个元素二分查 less / eq → r16. 无效键 = kNoKey 排最后, n = 首个 kNoKey 的位置.
//                ITEMS (每线程元素数) 按 A 分三档 2 / 6 / 10 (A ≤ 1024 / 3072 / 5120), shared 6 / 18 / 30 KB.
//                同一个核给因子 (float 平面) 与标签 (fp16 平面) 用, 只换取键器 SrcF / SrcH.
//     quant_rows (TS) 分位 x 直接量化 r16 (r16_quant), 逐格无关, grid-stride.
//     label_rows 每行先把 rx 拉进 shared (H 个持有期共用), 逐持有期一遍 A 轴累加, 收尾 finish_row (按 Frame):
//                  整数五和 (IC / rank-AC) 走 64 位整数 warp shuffle 归约 → 精确, 与 CPU 逐位一致;
//                  标签浮点和 / 分组和 走 float 寄存器 select 累加 (20 组展开) + warp shuffle + 跨 warp double 收尾,
//                  求和序固定 → GPU 自身逐次确定, 与 CPU 只差求和序 (对拍容差).
//                不用 shared atomic (分组落 20 个槽争用严重且不确定).
//   【禁用】--use_fast_math: pearson_int 的除法 / 开方要与 host 逐位一致.
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/Stat/Contract.hpp"

#include <cassert>
#include <cstdint>
#include <cub/cub.cuh>
#include <cuda_fp16.h>
#include <cuda_runtime.h>

// =============================================================================
// TS/CS/Stat 三个头共用的设备侧基件 (同一 TU 里多头都 include 时只展开一次)
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

inline constexpr int kCB = 512; // 一行一 block 的线程数 (每 SM 驻 2 块)
inline constexpr int kWarps = kCB / 32;
inline constexpr int kPart = 24; // block_sum 每次最多归约的分量数

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

namespace dev {

// ---- 与 Stat/Contract.hpp 逐字一致的 __device__ 版 ----
__device__ __forceinline__ unsigned ord(float f) {
  const unsigned u = __float_as_uint(f + 0.f);
  return (u & 0x80000000u) ? ~u : (u | 0x80000000u);
}
__device__ __forceinline__ unsigned short r16_of(int less, int eq, int n) {
  const long long num = (2LL * less + eq - 1) * kRankMax;
  const long long den = 2LL * (n - 1);
  return static_cast<unsigned short>((2 * num + den) / (2 * den));
}
__device__ __forceinline__ unsigned short r16_quant(float x) {
  assert(x >= 0.f && x <= 1.f && "TS 口径的因子值须是分位 ∈ [0,1] (根 TsRankRoll)");
  return static_cast<unsigned short>(static_cast<double>(x) * kRankMax + 0.5);
}
__device__ __forceinline__ int grp_of(unsigned r16) {
  const int g = static_cast<int>(r16 * kGroups / (kRankMax + 1));
  return g < kGroups - 1 ? g : kGroups - 1;
}
__device__ __forceinline__ bool hold_intraday(int hold) { return hold < factor::stat::kHoldDayBase; }
__device__ __forceinline__ int hold_minutes(int hold) {
  if (hold_intraday(hold))
    return hold;
  const int n = hold - factor::stat::kHoldDayBase;
  return kSegLen * (n < 1 ? 1 : n);
}
__device__ __forceinline__ bool tail_masked(int t, int hold) { return hold_intraday(hold) && t % kSegLen >= kSegLen - hold - kCloseAuction; }
__device__ __forceinline__ float pearson_int(unsigned long long n, unsigned long long sx, unsigned long long sy,
                                             unsigned long long sxx, unsigned long long syy, unsigned long long sxy, bool &ok) {
  if (n < 2) {
    ok = false;
    return 0.f;
  }
  const long long vx = static_cast<long long>(n * sxx) - static_cast<long long>(sx * sx);
  const long long vy = static_cast<long long>(n * syy) - static_cast<long long>(sy * sy);
  const long long cxy = static_cast<long long>(n * sxy) - static_cast<long long>(sx * sy);
  ok = vx > 0 && vy > 0;
  return ok ? static_cast<float>(static_cast<double>(cxy) / sqrt(static_cast<double>(vx) * static_cast<double>(vy))) : 0.f;
}
__device__ __forceinline__ float h2f(uint16_t b) { return __half2float(__ushort_as_half(b)); }
__device__ __forceinline__ void finish_row(Frame f, unsigned long long n, unsigned long long sx, unsigned long long sy,
                                           unsigned long long sxx, unsigned long long syy, unsigned long long sxy, double smk, double ssv,
                                           const double *gs, const int *gc, double s0, Row &r) {
  bool icok = false;
  const float ic = pearson_int(n, sx, sy, sxx, syy, sxy, icok);
  bool gok = true;
  if (f == Frame::CS)
    for (int k = 0; k < kGroups; ++k)
      gok = gok && gc[k] >= 1;
  if (!(icok && gok))
    return;
  const double mkt = smk / static_cast<double>(n), mkt_s = ssv / static_cast<double>(n);
  r.ok = 1;
  r.ic = ic;
  r.mkt = static_cast<float>(mkt);
  for (int k = 0; k < kGroups; ++k) {
    r.cnt[k] = static_cast<uint16_t>(gc[k]);
    r.grp[k] = static_cast<float>(gs[k] - gc[k] * mkt);
  }
  const int kt = kGroups - 1;
  const double top = gc[kt] ? (gs[kt] - gc[kt] * mkt) / gc[kt] : 0.0;
  const double bot = gc[0] ? (s0 - gc[0] * mkt_s) / gc[0] : 0.0;
  r.ls = static_cast<float>(top + bot);
}

// shared 有序数组上的二分
__device__ __forceinline__ int lower_bound(const unsigned *k, int n, unsigned key) {
  int lo = 0, hi = n;
  while (lo < hi) {
    const int mid = (lo + hi) >> 1;
    if (k[mid] < key)
      lo = mid + 1;
    else
      hi = mid;
  }
  return lo;
}
__device__ __forceinline__ int upper_bound(const unsigned *k, int n, unsigned key) {
  int lo = 0, hi = n;
  while (lo < hi) {
    const int mid = (lo + hi) >> 1;
    if (k[mid] <= key)
      lo = mid + 1;
    else
      hi = mid;
  }
  return lo;
}

} // namespace dev

namespace k {

// ---- 取键器: 一格 → 保序键 / kNoKey ----
struct SrcF { // 因子平面 (float)
  const float *v;
  const uint8_t *m;
  __device__ __forceinline__ unsigned key(size_t i) const { return m[i] ? dev::ord(v[i]) : kNoKey; }
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
    out[i] = src.m[i] ? dev::r16_quant(src.v[i]) : kRankNone;
}

// =============================================================================
// 2. label_rows: 每 (t, h) 一行统计
// =============================================================================

// 块内 D 路求和 (每线程一份 v[D]): warp shuffle → 各 warp 的 lane0 写 part → 前 D 个线程跨 warp 收尾 → 广播回 v.
// 求和序固定 → 结果逐次确定. 进出各一次 sync, 连续调用可复用同一 part.
template <class T, int D>
__device__ __forceinline__ void block_sum(T (&v)[D], T (*part)[kPart]) {
  static_assert(D <= kPart);
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
#pragma unroll
  for (int d = 0; d < D; ++d) {
#pragma unroll
    for (int off = 16; off > 0; off >>= 1)
      v[d] += __shfl_down_sync(0xffffffffu, v[d], off);
  }
  if (lane == 0) {
#pragma unroll
    for (int d = 0; d < D; ++d)
      part[warp][d] = v[d];
  }
  __syncthreads();
  if (threadIdx.x < D) {
    T s = part[0][threadIdx.x];
#pragma unroll
    for (int w = 1; w < kWarps; ++w)
      s += part[w][threadIdx.x];
    part[0][threadIdx.x] = s; // 只碰自己那一列, 与其余线程无交叠
  }
  __syncthreads();
#pragma unroll
  for (int d = 0; d < D; ++d)
    v[d] = part[0][d];
  __syncthreads(); // 全员读完, 下次调用的 lane0 才能改写 part
}

__global__ void __launch_bounds__(kCB) label_rows(Frame f, const uint16_t *ws, LabelSet L, Holds hd, Row *rows, int T, int A) {
  using ull = unsigned long long;
  __shared__ unsigned short rxs[kMaxA];
  __shared__ ull pu[kWarps][kPart];
  __shared__ float pf[kWarps][kPart];
  __shared__ int pi[kWarps][kPart];
  const int t = blockIdx.x;
  const size_t base = static_cast<size_t>(t) * A;
  for (int a = threadIdx.x; a < A; a += kCB)
    rxs[a] = ws[base + a];
  __syncthreads();

  for (int hi = 0; hi < hd.n; ++hi) { // 块内一致量, 不发散
    const int hold = hd.h[hi];
    const int h = dev::hold_minutes(hold);
    const Label &lb = L.l[hi];
    // ---- rank-AC (lag = h) ----
    ull ac[6] = {0, 0, 0, 0, 0, 0}; // n, Σx, Σy, Σx², Σy², Σxy
    if (t >= h) {
      const uint16_t *rp = ws + static_cast<size_t>(t - h) * A;
      for (int a = threadIdx.x; a < A; a += kCB) {
        const unsigned short xr = rxs[a], yr = rp[a];
        const bool ok = xr != kRankNone && yr != kRankNone;
        const ull x = xr, y = yr;
        ac[0] += ok;
        ac[1] += ok ? x : 0;
        ac[2] += ok ? y : 0;
        ac[3] += ok ? x * x : 0;
        ac[4] += ok ? y * y : 0;
        ac[5] += ok ? x * y : 0;
      }
      block_sum(ac, pu);
    }
    // ---- 标签侧 ----
    const bool tail = dev::tail_masked(t, hold);
    ull s[6] = {0, 0, 0, 0, 0, 0}; // n, Σx, Σy, Σx², Σy², Σxy (IC)
    float fs[3 + kGroups] = {};    // Σlv (mkt), Σsv, Σsv|组0, 各组 Σlv
    int c[kGroups] = {};           // 各组计数
    if (!tail) {
      const uint16_t *ry = lb.ry + base, *lv = lb.lv + base, *sv = lb.sv + base;
      for (int a = threadIdx.x; a < A; a += kCB) {
        const unsigned short xr = rxs[a], yr = ry[a];
        const bool ok = xr != kRankNone && yr != kRankNone;
        const ull x = xr, y = yr;
        s[0] += ok;
        s[1] += ok ? x : 0;
        s[2] += ok ? y : 0;
        s[3] += ok ? x * x : 0;
        s[4] += ok ? y * y : 0;
        s[5] += ok ? x * y : 0;
        const float yl = dev::h2f(lv[a]), ys = dev::h2f(sv[a]);
        const int g = ok ? dev::grp_of(xr) : -1;
        fs[0] += ok ? yl : 0.f;
        fs[1] += ok ? ys : 0.f;
        fs[2] += g == 0 ? ys : 0.f;
#pragma unroll
        for (int q = 0; q < kGroups; ++q) { // 展开 select, 不做寄存器数组的运行期下标
          fs[3 + q] += g == q ? yl : 0.f;
          c[q] += g == q;
        }
      }
      block_sum(s, pu);
      block_sum(fs, pf);
      block_sum(c, pi);
    }
    if (threadIdx.x == 0) {
      Row r;
      if (t >= h) {
        bool ok = false;
        r.ac = dev::pearson_int(ac[0], ac[1], ac[2], ac[3], ac[4], ac[5], ok);
        r.ok_ac = ok ? 1 : 0;
      }
      if (!tail) {
        double gs[kGroups];
#pragma unroll
        for (int q = 0; q < kGroups; ++q)
          gs[q] = static_cast<double>(fs[3 + q]);
        dev::finish_row(f, s[0], s[1], s[2], s[3], s[4], s[5], static_cast<double>(fs[0]), static_cast<double>(fs[1]), gs, c,
                        static_cast<double>(fs[2]), r);
      }
      rows[static_cast<size_t>(hi) * T + t] = r;
    }
    __syncthreads(); // 下一持有期复用 pu / pf / pi 前, 线程 0 读完
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

// 评估: x (口径 f) + L.n 组预处理标签 → rows[L.n][T] (设备)
inline void eval(const float *xv, const uint8_t *xm, Frame f, int T, int A, const Holds &hd, const LabelSet &L, uint16_t *ws, Row *rows,
                 cudaStream_t stream) {
  factor::stat::assert_holds(hd);
  assert(L.n == hd.n);
  for (int i = 0; i < L.n; ++i)
    assert(L.l[i].lv && L.l[i].sv && L.l[i].m && L.l[i].ry && "标签未预处理 (prep_label)");
  if (f == Frame::CS) {
    rank_launch(k::SrcF{xv, xm}, ws, T, A, stream);
  } else {
    const size_t n = static_cast<size_t>(T) * A;
    const int grid = static_cast<int>(hmin(static_cast<int>((n + kCB - 1) / kCB), 4096));
    k::quant_rows<<<grid, kCB, 0, stream>>>(k::SrcF{xv, xm}, ws, n);
    FACTOR_CUDA_OK(cudaGetLastError());
  }
  k::label_rows<<<T, kCB, 0, stream>>>(f, ws, L, hd, rows, T, A);
  FACTOR_CUDA_OK(cudaGetLastError());
}

} // namespace factor::gpu::stat
