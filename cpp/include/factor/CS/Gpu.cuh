#pragma once

// =============================================================================
// CS 轴挖掘后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; clang -x cuda)
// =============================================================================
//   语义契约见 factor/Contract.hpp, 算子真相表见 factor/OpTable.hpp.
//   本文件与 CS/Cpu.hpp / CS/Stream.hpp **完全独立**, 只依赖 Contract + CUDA/CUB.
//
//   【布局】SoA 行主序 [T][A]: 同一时刻的 A 个资产在内存里连续 → 截面归约天然合并访存.
//   【核配置总则】**一行一 block**, grid = T (19200 个 block), blockDim = 512:
//     · sm_75 每 SM 上限就是 1024 线程, 用 512 可以每 SM 驻 2 个 block,
//       让一个 block 卡在 __syncthreads / cub 归约时另一个 block 顶上 (1024 线程/块做不到).
//     · A = 5000 → 每线程 10 轮; 每轮 512×4B = 2 KB 连续读, 完美合并.
//     · 一整行 5000 float = 20 KB, 多趟重扫全部命中 L2 (3 MB), 所以直接重读 global,
//       不把行缓进 shared: 二元/三元算子缓 x+y+掩码就要 50 KB, 放不下 48 KB.
//   【归约】cub::BlockReduce (float) + cub::BlockScan (桶前缀), 自带 warp shfl.
//     计数用 float 归约: cnt ≤ 2²⁴ 内 float 表示精确, 省一套 TempStorage.
//   【两遍】高阶矩一律两遍: 第一趟求 μ, 第二趟在 (x−μ) 上求 M2/Cxy (不用 Σx² 单遍式).
//   【HIST】片上 256 桶直方图 (shared atomicAdd) + BlockScan 前缀, 一行两套桶 (x / y).
//   【GROUP】shared 累加槽 kGrpCap = 1024 组 (行业约 30), 无 global atomic 争用.
//   全程 branchless, 出口 dev::store 保证 ov/om 永不含 NaN/inf.
// =============================================================================

#include "factor/Contract.hpp"

#include <cassert>
#include <cfloat>
#include <climits>
#include <cstdint>
#include <cub/cub.cuh>
#include <cuda_runtime.h>

// =============================================================================
// TS/CS 两个头共用的设备侧基件 (同一 TU 里两头都 include 时只展开一次)
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

namespace factor::gpu::cs {

inline constexpr int kCB = 512;           // 一行一 block 的线程数 (见头注释: 每 SM 驻 2 块)
inline constexpr int kGrpCap = kMaxGroup; // shared 分组槽上限 = 契约的组 id 上限 (三后端同一数). 行业分组约 30;
                                          // 超过即 device assert 失败 —— 退化成 global atomic 需要
                                          // T×G×8B 的 workspace (G=4096 时 629 MB), 不如早炸

namespace k {

using BR = cub::BlockReduce<float, kCB>;
using BS = cub::BlockScan<int, kCB>;

// 自带 min/max 仿函数: cub::Min / cub::Max 在 CCCL 3.x 已移除, 不依赖它
struct MinOp {
  __device__ __forceinline__ float operator()(float a, float b) const { return fminf(a, b); }
};
struct MaxOp {
  __device__ __forceinline__ float operator()(float a, float b) const { return fmaxf(a, b); }
};

// 每块的 shared 家当: 直方图 2 套 (4 KB) + 分组槽 (20 KB) + cub 暂存 ≈ 25 KB
// (512 线程 × 2 块/SM = 50 KB ≤ 64 KB, 不构成占用率瓶颈)
struct Sh {
  typename BR::TempStorage red;
  typename BS::TempStorage scan;
  int cb[kBuckets], pre[kBuckets];   // 主直方图 (桶计数 / 桶的 exclusive 前缀)
  int cb2[kBuckets], pre2[kBuckets]; // 次直方图 (RankDiff 的 y / CondRank 的条件桶)
  float gsx[kGrpCap], gsy[kGrpCap];  // 分组累加槽
  int gcn[kGrpCap];
  int gmn[kGrpCap], gmx[kGrpCap]; // 分组 y 极值 (有序整数编码, 给 GroupResid 的逐组全并列判据)
  float bc;                       // 广播槽
  int ib;
};

// float → 保序 int 编码 (a < b ⟺ ord(a) < ord(b)); ±0 先归一为 +0, 与 host 的 fmin/fmax 判等一致
__device__ __forceinline__ int ord(float f) {
  const int i = __float_as_int(f + 0.f);
  return i >= 0 ? i : i ^ 0x7fffffff;
}

// ---- 块级归约 + 广播 (归约后必须两次 sync: 一次等广播写入, 一次防下轮复用暂存) ----
__device__ inline float bsum(Sh &sh, float v) {
  const float r = BR(sh.red).Sum(v);
  if (threadIdx.x == 0)
    sh.bc = r;
  __syncthreads();
  const float o = sh.bc;
  __syncthreads();
  return o;
}
__device__ inline float bmin(Sh &sh, float v) {
  const float r = BR(sh.red).Reduce(v, MinOp());
  if (threadIdx.x == 0)
    sh.bc = r;
  __syncthreads();
  const float o = sh.bc;
  __syncthreads();
  return o;
}
__device__ inline float bmax(Sh &sh, float v) {
  const float r = BR(sh.red).Reduce(v, MaxOp());
  if (threadIdx.x == 0)
    sh.bc = r;
  __syncthreads();
  const float o = sh.bc;
  __syncthreads();
  return o;
}

// ---- 取值器 (决定"样本集"是什么) ----
struct GetX { // 原值
  const float *v;
  const uint8_t *m;
  int base;
  __device__ __forceinline__ bool get(int a, float &u) const {
    u = v[base + a];
    return m[base + a] != 0;
  }
};
struct GetY2 { // y 值, 但样本集要求 x、y **双有效** (CsCondRank 的条件桶边界)
  const float *v;
  const uint8_t *m;  // ym
  const uint8_t *m2; // xm
  int base;
  __device__ __forceinline__ bool get(int a, float &u) const {
    u = v[base + a];
    return m[base + a] != 0 && m2[base + a] != 0;
  }
};
struct GetWin { // 缩尾后的值 (winsor 族第二轮)
  const float *v;
  const uint8_t *m;
  int base;
  float lo, hi;
  __device__ __forceinline__ bool get(int a, float &u) const {
    u = fminf(fmaxf(v[base + a], lo), hi);
    return m[base + a] != 0;
  }
};
// ---- 样本集的 cnt / lo / hi ----
template <class G>
__device__ inline void span_row(Sh &sh, const G &g, int A, int &cnt, float &lo, float &hi) {
  int c = 0;
  float l = FLT_MAX, h = -FLT_MAX;
  for (int a = threadIdx.x; a < A; a += kCB) {
    float u;
    if (g.get(a, u)) {
      ++c;
      l = fminf(l, u);
      h = fmaxf(h, u);
    }
  }
  cnt = static_cast<int>(bsum(sh, static_cast<float>(c)));
  lo = bmin(sh, l);
  hi = bmax(sh, h);
}

// ---- 片上直方图: cb[b] 计数, pre[b] 为 exclusive 前缀 (桶计数是整数 → 三后端可严格逐位一致) ----
template <class G>
__device__ inline void hist_row(Sh &sh, const G &g, int A, float lo, float hi, int *cb, int *pre) {
  for (int b = threadIdx.x; b < kBuckets; b += kCB)
    cb[b] = 0;
  __syncthreads();
  for (int a = threadIdx.x; a < A; a += kCB) {
    float u;
    if (g.get(a, u))
      atomicAdd(&cb[dev::bin_of_fast(u, lo, hi)], 1); // shared atomic, 5000 样本摊到 256 桶几乎不争用
  }
  __syncthreads();
  const int val = (threadIdx.x < kBuckets) ? cb[threadIdx.x] : 0;
  int ex = 0;
  BS(sh.scan).ExclusiveSum(val, ex);
  if (threadIdx.x < kBuckets)
    pre[threadIdx.x] = ex;
  __syncthreads();
}

// 前缀累计首次 ≥ target 的桶 (前缀单调 → 命中唯一)
__device__ inline int find_bin(Sh &sh, const int *cb, const int *pre, int target) {
  if (threadIdx.x == 0)
    sh.ib = kBuckets - 1;
  __syncthreads();
  if (threadIdx.x < kBuckets && pre[threadIdx.x] < target && pre[threadIdx.x] + cb[threadIdx.x] >= target)
    atomicMin(&sh.ib, static_cast<int>(threadIdx.x));
  __syncthreads();
  const int b = sh.ib;
  __syncthreads();
  return b;
}
// quantile(q): q ≤ 0 → lo, q ≥ 1 → hi, 否则第 ⌈q·cnt⌉ (下限 1) 个样本所在桶的中心
//   q 必须按 **double** 取: 常数分位点要用 double 字面量 (0.01/0.99), 从 Param 来的则是
//   float 值提升成 double —— 两者不同 (0.99 → 0.99×100 = 99 整; 0.99f → 99.00000095 → ⌈⌉ = 100)
__device__ inline float row_quant(Sh &sh, const int *cb, const int *pre, int cnt, float lo, float hi, double q) {
  if (q <= 0.0)
    return lo;
  if (q >= 1.0)
    return hi;
  // ⌈q·cnt⌉ 在 double 下取整: fp32 乘法会把 1.0000000149 舍入成恰好 1.0 → tg 差 1, 整整差一个桶
  // (实测 CsWinsor k=0.05 cnt=20 行 |Δ|=1.32). 一行只算两次, fp64 开销可忽略.
  const int tg = max(1, static_cast<int>(ceil(q * cnt)));
  return dev::bin_center(find_bin(sh, cb, pre, tg), lo, hi);
}
// 某个值在直方图里的 pct rank
__device__ inline float row_pct(const int *cb, const int *pre, int cnt, float lo, float hi, bool rok, float x) {
  if (!rok)
    return 0.5f; // 值域退化 (全并列)
  const int b = dev::bin_of(x, lo, hi);
  return dev::pct_of(pre[b], cb[b], cnt);
}

// ---- 一/二元的截面矩 (两遍: 先 μ 与极值, 再在 (x−μ) 上求 M2 / Cxy); 极值给全并列判据 ----
struct RowStat {
  int cnt;
  float mx, my, M2x, M2y, Cxy;
  float lox, hix, loy, hiy;
  __device__ bool sx() const { return dev::spread(lox, hix); }
  __device__ bool sy() const { return dev::spread(loy, hiy); }
};
template <int NARY>
__device__ inline void row_stats(Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, int base, int A, RowStat &st) {
  int c = 0;
  float sx = 0.f, sy = 0.f, lx = FLT_MAX, hx = -FLT_MAX, ly = FLT_MAX, hy = -FLT_MAX;
  for (int a = threadIdx.x; a < A; a += kCB) {
    const int i = base + a;
    const bool g = (NARY >= 2) ? (xm[i] && ym[i]) : (xm[i] != 0); // 二元要两侧同时有效
    const float x = g ? xv[i] : 0.f;
    const float y = (NARY >= 2 && g) ? yv[i] : 0.f;
    c += g ? 1 : 0;
    sx += x;
    sy += y;
    lx = g ? fminf(lx, x) : lx;
    hx = g ? fmaxf(hx, x) : hx;
    ly = g ? fminf(ly, y) : ly;
    hy = g ? fmaxf(hy, y) : hy;
  }
  st.cnt = static_cast<int>(bsum(sh, static_cast<float>(c)));
  const float tx = bsum(sh, sx);
  const float fn = static_cast<float>(max(st.cnt, 1));
  st.mx = tx / fn;
  st.lox = bmin(sh, lx);
  st.hix = bmax(sh, hx);
  st.my = 0.f;
  st.loy = st.hiy = 0.f;
  if constexpr (NARY >= 2) {
    st.my = bsum(sh, sy) / fn;
    st.loy = bmin(sh, ly);
    st.hiy = bmax(sh, hy);
  }
  float m2x = 0.f, m2y = 0.f, cxy = 0.f;
  for (int a = threadIdx.x; a < A; a += kCB) { // 第二遍: 中心化后累幂和
    const int i = base + a;
    const bool g = (NARY >= 2) ? (xm[i] && ym[i]) : (xm[i] != 0);
    const float dx = g ? (xv[i] - st.mx) : 0.f;
    const float dy = (NARY >= 2 && g) ? (yv[i] - st.my) : 0.f;
    m2x += dx * dx;
    m2y += dy * dy;
    cxy += dx * dy;
  }
  st.M2x = bsum(sh, m2x);
  st.M2y = 0.f;
  st.Cxy = 0.f;
  if constexpr (NARY >= 2) {
    st.M2y = bsum(sh, m2y);
    st.Cxy = bsum(sh, cxy);
  }
}

// ---- 分组 id ----
struct GidCol { // 组 id 直接来自某一列: g = m ? (int)floorf(v) : −1
  const float *v;
  const uint8_t *m;
  int base;
  __device__ __forceinline__ int gid(int a) const { return m[base + a] ? static_cast<int>(floorf(v[base + a])) : -1; }
};
// 组内累加 x (和 y) 到 shared 槽
__device__ inline void grp_clear(Sh &sh) {
  for (int g = threadIdx.x; g < kGrpCap; g += kCB) {
    sh.gsx[g] = 0.f;
    sh.gsy[g] = 0.f;
    sh.gcn[g] = 0;
    sh.gmn[g] = INT_MAX;
    sh.gmx[g] = INT_MIN;
  }
  __syncthreads();
}

// 唯一的 CS 核: 一行一 block, 具体算法由 Op::row 以"块内集体操作"的风格写
template <class Op>
__global__ void row(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int A, Param p) {
  __shared__ Sh sh;
  Op::row(sh, xv, xm, yv, ym, zv, zm, ov, om, blockIdx.x * A, A, p);
}

} // namespace k

// =============================================================================
// 算子: 统一签名 (未用到的指针传 nullptr, 全部设备指针)
// =============================================================================
#define FACTOR_CS_RUN()                                                                                                                                                             \
  static size_t workspace(int, int, const Param &) { return 0; } /* 全部在 shared 里做完, 不要临时显存 */                                                                           \
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p, \
                  void *, cudaStream_t stream) {                                                                                                                                    \
    assert(A >= 1 && static_cast<long long>(T) * A < (1LL << 31));                                                                                                                  \
    k::row<Self><<<T, kCB, 0, stream>>>(xv, xm, yv, ym, zv, zm, ov, om, A, p);                                                                                                      \
    FACTOR_CUDA_OK(cudaGetLastError());                                                                                                                                             \
  }

// ---- REDUCE (7): 沿资产归约, 广播型算子对全行写同一个值 ----
#define FACTOR_CS_RED(Name, NARY, BODY)                                                                                                                                   \
  struct Name {                                                                                                                                                           \
    using Self = Name;                                                                                                                                                    \
    __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *, const uint8_t *, float *ov, uint8_t *om, \
                               int base, int A, const Param &p) {                                                                                                         \
      k::RowStat s;                                                                                                                                                       \
      k::row_stats<NARY>(sh, xv, xm, yv, ym, base, A, s);                                                                                                                 \
      for (int a = threadIdx.x; a < A; a += kCB) {                                                                                                                        \
        const int i = base + a;                                                                                                                                           \
        const DVal x{xv[i], xm[i]};                                                                                                                                       \
        const DVal y{(NARY >= 2) ? yv[i] : 0.f, static_cast<uint8_t>((NARY >= 2) ? ym[i] : 0)};                                                                           \
        float v = 0.f;                                                                                                                                                    \
        bool m = false;                                                                                                                                                   \
        (void)x, (void)y, (void)p;                                                                                                                                        \
        BODY                                                                                                                                                              \
            dev::store(ov, om, i, v, m);                                                                                                                                  \
      }                                                                                                                                                                   \
    }                                                                                                                                                                     \
    FACTOR_CS_RUN()                                                                                                                                                       \
  };

// 截面均值广播
FACTOR_CS_RED(CsMean, 1, {
  v = s.mx;
  m = s.cnt >= 1;
})
// 截面样本标准差广播 (ddof=1)
FACTOR_CS_RED(CsStd, 1, {
  v = sqrtf(fmaxf(s.M2x, 0.f) / static_cast<float>(max(s.cnt - 1, 1)));
  m = s.cnt >= 2 && s.sx();
})
// x − 截面均值 (相对型)
FACTOR_CS_RED(CsDemean, 1, {
  v = x.v - s.mx;
  m = x.m && s.cnt >= 1;
})
// (x − μ)/σ (相对型)
FACTOR_CS_RED(CsZ, 1, {
  const float sd = sqrtf(fmaxf(s.M2x, 0.f) / static_cast<float>(max(s.cnt - 1, 1)));
  v = (x.v - s.mx) / sd;
  m = x.m && s.cnt >= 2 && s.sx();
})
// x 对 y 截面 OLS (含截距) 残差 (相对型)
FACTOR_CS_RED(CsResid, 2, {
  const float b = s.Cxy / s.M2y;
  v = (x.v - s.mx) - b * (y.v - s.my);
  m = x.m && y.m && s.cnt >= 2 && s.sy();
})
// OLS 斜率广播
FACTOR_CS_RED(CsBeta, 2, {
  v = s.Cxy / s.M2y;
  m = s.cnt >= 2 && s.sy();
})
// 截面 Pearson 广播 (按规格不 clamp |ρ| ≤ 1)
FACTOR_CS_RED(CsCorr, 2, {
  // 非全并列 ⇒ 两个方差为正, |ρ| 有柯西–施瓦茨上界; 乘积走 double 防下溢
  v = static_cast<float>(static_cast<double>(s.Cxy) / sqrt(fmax(static_cast<double>(s.M2x) * s.M2y, 0.0)));
  m = s.cnt >= 2 && s.sx() && s.sy();
})

// ---- HIST (9): 片上 256 桶 ----
//   样本集 S = 本行有效值, lo/hi = min/max(S). 全并列 (!spread) 时按契约给中性值.

// pct rank (并列均秩)
struct CsRank {
  using Self = CsRank;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int base, int A,
                             const Param &) {
    const k::GetX g{xv, xm, base};
    int cnt;
    float lo, hi;
    k::span_row(sh, g, A, cnt, lo, hi);
    const bool rok = cnt >= 1 && dev::spread(lo, hi);
    if (rok)
      k::hist_row(sh, g, A, lo, hi, sh.cb, sh.pre);
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a;
      dev::store(ov, om, i, k::row_pct(sh.cb, sh.pre, cnt, lo, hi, rok, xv[i]), xm[i] && cnt >= 1);
    }
  }
  FACTOR_CS_RUN()
};
// Φ⁻¹(clamp(pct, 1/(N+1), N/(N+1)))
//   注: host 侧用 Contract::probit (Wichura AS241), 这里用 CUDA 自带 normcdfinvf, 两者差 ~1e-7
//   → **本算子对拍容差要单独放宽** (Contract.hpp 的注释里也已写明)
struct CsNormRank {
  using Self = CsNormRank;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int base, int A,
                             const Param &) {
    const k::GetX g{xv, xm, base};
    int cnt;
    float lo, hi;
    k::span_row(sh, g, A, cnt, lo, hi);
    const bool rok = cnt >= 1 && dev::spread(lo, hi);
    if (rok)
      k::hist_row(sh, g, A, lo, hi, sh.cb, sh.pre);
    const float fn = static_cast<float>(max(cnt, 1));
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a;
      const float pct = k::row_pct(sh.cb, sh.pre, cnt, lo, hi, rok, xv[i]);
      const float cl = fminf(fmaxf(pct, 1.f / (fn + 1.f)), fn / (fn + 1.f));
      dev::store(ov, om, i, normcdfinvf(cl), xm[i] && cnt >= 1);
    }
  }
  FACTOR_CS_RUN()
};
// 中位数 / k 分位 广播 (桶近似下不做偶数上下平均)
#define FACTOR_CS_QUANT(Name, QEXPR)                                                                                                                                            \
  struct Name {                                                                                                                                                                 \
    using Self = Name;                                                                                                                                                          \
    __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int base, \
                               int A, const Param &p) {                                                                                                                         \
      const k::GetX g{xv, xm, base};                                                                                                                                            \
      int cnt;                                                                                                                                                                  \
      float lo, hi;                                                                                                                                                             \
      k::span_row(sh, g, A, cnt, lo, hi);                                                                                                                                       \
      const bool rok = cnt >= 1 && dev::spread(lo, hi);                                                                                                                         \
      float q = lo; /* 值域退化 → 给 lo */                                                                                                                                      \
      if (rok) {                                                                                                                                                                \
        k::hist_row(sh, g, A, lo, hi, sh.cb, sh.pre);                                                                                                                           \
        q = k::row_quant(sh, sh.cb, sh.pre, cnt, lo, hi, QEXPR);                                                                                                                \
      }                                                                                                                                                                         \
      (void)p;                                                                                                                                                                  \
      for (int a = threadIdx.x; a < A; a += kCB)                                                                                                                                \
        dev::store(ov, om, base + a, q, cnt >= 1);                                                                                                                              \
    }                                                                                                                                                                           \
    FACTOR_CS_RUN()                                                                                                                                                             \
  };
FACTOR_CS_QUANT(CsMedian, 0.5)
FACTOR_CS_QUANT(CsQuantile, p.k)

// 分位缩尾: clamp 到 [q_k, q_{1−k}]
struct CsWinsor {
  using Self = CsWinsor;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int base, int A,
                             const Param &p) {
    const k::GetX g{xv, xm, base};
    int cnt;
    float lo, hi;
    k::span_row(sh, g, A, cnt, lo, hi);
    const bool rok = cnt >= 1 && dev::spread(lo, hi);
    float ql = lo, qh = lo;
    if (rok) {
      k::hist_row(sh, g, A, lo, hi, sh.cb, sh.pre);
      ql = k::row_quant(sh, sh.cb, sh.pre, cnt, lo, hi, p.k);
      // 上分位必须在 double 下算 1−k: 先 float 减再提升会与 cpu / stream 差一个桶
      // (⌈q·cnt⌉ 的取整对 q 的末位极敏感, 见 CsWinsorRank 的 0.99f vs 0.99 之坑)
      qh = k::row_quant(sh, sh.cb, sh.pre, cnt, lo, hi, 1.0 - static_cast<double>(p.k));
    }
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a;
      dev::store(ov, om, i, fminf(fmaxf(xv[i], ql), qh), xm[i] && cnt >= 1);
    }
  }
  FACTOR_CS_RUN()
};
// 先缩尾 (k = 0.01) 再 pct rank: 缩尾后值域变了, 必须按缩尾样本集重新定 lo/hi 与直方图
struct CsWinsorRank {
  using Self = CsWinsorRank;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int base, int A,
                             const Param &) {
    const k::GetX g{xv, xm, base};
    int cnt;
    float lo, hi;
    k::span_row(sh, g, A, cnt, lo, hi);
    bool rok = cnt >= 1 && dev::spread(lo, hi);
    float ql = lo, qh = lo;
    if (rok) {
      k::hist_row(sh, g, A, lo, hi, sh.cb, sh.pre);
      ql = k::row_quant(sh, sh.cb, sh.pre, cnt, lo, hi, 0.01);
      qh = k::row_quant(sh, sh.cb, sh.pre, cnt, lo, hi, 0.99);
    }
    const k::GetWin w{xv, xm, base, ql, qh};
    int c2;
    float lo2, hi2;
    k::span_row(sh, w, A, c2, lo2, hi2);
    const bool rok2 = c2 >= 1 && dev::spread(lo2, hi2);
    if (rok2)
      k::hist_row(sh, w, A, lo2, hi2, sh.cb, sh.pre);
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a;
      const float u = fminf(fmaxf(xv[i], ql), qh);
      dev::store(ov, om, i, k::row_pct(sh.cb, sh.pre, c2, lo2, hi2, rok2, u), xm[i] && cnt >= 1);
    }
  }
  FACTOR_CS_RUN()
};
// 先缩尾 (k = 0.01) 再 z: 均值/方差都在缩尾后的量上两遍求
struct CsWinsorZ {
  using Self = CsWinsorZ;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int base, int A,
                             const Param &) {
    const k::GetX g{xv, xm, base};
    int cnt;
    float lo, hi;
    k::span_row(sh, g, A, cnt, lo, hi);
    const bool rok = cnt >= 1 && dev::spread(lo, hi);
    float ql = lo, qh = lo;
    if (rok) {
      k::hist_row(sh, g, A, lo, hi, sh.cb, sh.pre);
      ql = k::row_quant(sh, sh.cb, sh.pre, cnt, lo, hi, 0.01);
      qh = k::row_quant(sh, sh.cb, sh.pre, cnt, lo, hi, 0.99);
    }
    // 缩尾后样本的极值 = clamp(lo/hi): 全并列判据直接在其上算, 不必再扫一遍
    const bool sp = dev::spread(fminf(fmaxf(lo, ql), qh), fminf(fmaxf(hi, ql), qh));
    float sw = 0.f;
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a;
      sw += xm[i] ? fminf(fmaxf(xv[i], ql), qh) : 0.f;
    }
    const float mu = k::bsum(sh, sw) / static_cast<float>(max(cnt, 1));
    float m2 = 0.f;
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a;
      const float e = xm[i] ? (fminf(fmaxf(xv[i], ql), qh) - mu) : 0.f;
      m2 += e * e;
    }
    const float M2 = k::bsum(sh, m2);
    const float sd = sqrtf(fmaxf(M2, 0.f) / static_cast<float>(max(cnt - 1, 1)));
    const bool ok = cnt >= 2 && sp;
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a;
      const float u = fminf(fmaxf(xv[i], ql), qh);
      dev::store(ov, om, i, (u - mu) / sd, xm[i] && ok);
    }
  }
  FACTOR_CS_RUN()
};
// floor(pct·k) ∈ 0..k−1 (值域退化 → 0)
struct CsBucket {
  using Self = CsBucket;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int base, int A,
                             const Param &p) {
    const int K = max(1, static_cast<int>(p.k));
    const k::GetX g{xv, xm, base};
    int cnt;
    float lo, hi;
    k::span_row(sh, g, A, cnt, lo, hi);
    const bool rok = cnt >= 1 && dev::spread(lo, hi);
    if (rok)
      k::hist_row(sh, g, A, lo, hi, sh.cb, sh.pre);
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a;
      const float pct = rok ? k::row_pct(sh.cb, sh.pre, cnt, lo, hi, true, xv[i]) : 0.f; // 退化 → 桶 0
      const int b = min(K - 1, max(0, static_cast<int>(floorf(pct * static_cast<float>(K)))));
      dev::store(ov, om, i, static_cast<float>(b), xm[i] && cnt >= 1);
    }
  }
  FACTOR_CS_RUN()
};
// pct_rank(x) − pct_rank(y): 两套桶各自定 lo/hi (x 的样本集 = 有效 x, y 的 = 有效 y)
struct CsRankDiff {
  using Self = CsRankDiff;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *, const uint8_t *, float *ov, uint8_t *om, int base,
                             int A, const Param &) {
    const k::GetX gx{xv, xm, base}, gy{yv, ym, base};
    int cx, cy;
    float lox, hix, loy, hiy;
    k::span_row(sh, gx, A, cx, lox, hix);
    k::span_row(sh, gy, A, cy, loy, hiy);
    const bool rx = cx >= 1 && dev::spread(lox, hix);
    const bool ry = cy >= 1 && dev::spread(loy, hiy);
    if (rx)
      k::hist_row(sh, gx, A, lox, hix, sh.cb, sh.pre);
    if (ry)
      k::hist_row(sh, gy, A, loy, hiy, sh.cb2, sh.pre2);
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a;
      const float px = k::row_pct(sh.cb, sh.pre, cx, lox, hix, rx, xv[i]);
      const float py = k::row_pct(sh.cb2, sh.pre2, cy, loy, hiy, ry, yv[i]);
      dev::store(ov, om, i, px - py, xm[i] && ym[i] && cx >= 1 && cy >= 1);
    }
  }
  FACTOR_CS_RUN()
};

// ---- GROUP (4) ----
// 按 y 分组 (整数 id) 的组均值广播
struct CsGroupMean {
  using Self = CsGroupMean;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *, const uint8_t *, float *ov, uint8_t *om, int base,
                             int A, const Param &) {
    const k::GidCol gi{yv, ym, base};
    k::grp_clear(sh);
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a, g = gi.gid(a);
      assert(g < kGrpCap && "组 id 超出 shared 槽上限");
      if (g >= 0 && xm[i]) {
        atomicAdd(&sh.gsx[g], xv[i]); // shared 槽累加, 不碰 global atomic
        atomicAdd(&sh.gcn[g], 1);
      }
    }
    __syncthreads();
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a, g = gi.gid(a);
      const int c = (g >= 0) ? sh.gcn[g] : 0;
      dev::store(ov, om, i, (g >= 0) ? sh.gsx[g] / static_cast<float>(max(c, 1)) : 0.f, xm[i] && g >= 0 && c >= 1);
    }
  }
  FACTOR_CS_RUN()
};

// 组内 pct rank 的公共实现: 逐组重建直方图 (组数 G 约 30 → O(G·A) 每行)
template <class Gid>
__device__ inline void group_rank(k::Sh &sh, const float *xv, const uint8_t *xm, const Gid &gi, float *ov, uint8_t *om, int base, int A) {
  for (int a = threadIdx.x; a < A; a += kCB) // 未归组 / x 无效的资产保持无效
    dev::store(ov, om, base + a, 0.f, false);
  int gmax = 0;
  for (int a = threadIdx.x; a < A; a += kCB)
    gmax = max(gmax, gi.gid(a));
  gmax = static_cast<int>(k::bmax(sh, static_cast<float>(gmax)));
  assert(gmax < kGrpCap && "组 id 超出 shared 槽上限");
  for (int g = 0; g <= gmax; ++g) { // g 是块内一致量, 循环不发散
    struct GetG {
      const float *v;
      const uint8_t *m;
      int base, g;
      const Gid *gi;
      __device__ __forceinline__ bool get(int a, float &u) const {
        u = v[base + a];
        return m[base + a] != 0 && gi->gid(a) == g;
      }
    } sg{xv, xm, base, g, &gi};
    int cnt;
    float lo, hi;
    k::span_row(sh, sg, A, cnt, lo, hi);
    if (cnt < 1)
      continue;
    const bool rok = dev::spread(lo, hi);
    if (rok)
      k::hist_row(sh, sg, A, lo, hi, sh.cb, sh.pre);
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a;
      if (xm[i] && gi.gid(a) == g)
        dev::store(ov, om, i, k::row_pct(sh.cb, sh.pre, cnt, lo, hi, rok, xv[i]), true);
    }
    __syncthreads(); // 下一组要复用 sh.cb / 归约暂存
  }
}

// 按 y 分组的组内 pct rank
struct CsGroupRank {
  using Self = CsGroupRank;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *, const uint8_t *, float *ov, uint8_t *om, int base,
                             int A, const Param &) {
    const k::GidCol gi{yv, ym, base};
    group_rank(sh, xv, xm, gi, ov, om, base, A);
  }
  FACTOR_CS_RUN()
};
// y 分 k 桶 (= CsBucket(k)) 当组 id, x 在桶内 pct rank
struct CsCondRank {
  using Self = CsCondRank;
  struct GidBin { // 条件桶: 由 y 的次直方图现算, 不占额外存储
    const float *v;
    const uint8_t *m;
    const int *cb, *pre;
    int base, cnt, K;
    float lo, hi;
    bool rok;
    __device__ __forceinline__ int gid(int a) const {
      if (!m[base + a])
        return -1;
      const float pct = rok ? k::row_pct(cb, pre, cnt, lo, hi, true, v[base + a]) : 0.f;
      return min(K - 1, max(0, static_cast<int>(floorf(pct * static_cast<float>(K)))));
    }
  };
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *, const uint8_t *, float *ov, uint8_t *om, int base,
                             int A, const Param &p) {
    const int K = max(1, static_cast<int>(p.k));
    assert(K <= kGrpCap);
    // 桶边界只由 xm && ym 双有效的资产决定: y 有效但 x 缺失的资产对任何桶的 x 排名都没有贡献
    const k::GetY2 gy{yv, ym, xm, base};
    int cy;
    float loy, hiy;
    k::span_row(sh, gy, A, cy, loy, hiy);
    const bool ry = cy >= 1 && dev::spread(loy, hiy);
    if (ry)
      k::hist_row(sh, gy, A, loy, hiy, sh.cb2, sh.pre2); // 条件桶用次直方图, 主直方图留给组内 rank
    const GidBin gi{yv, ym, sh.cb2, sh.pre2, base, cy, K, loy, hiy, ry};
    group_rank(sh, xv, xm, gi, ov, om, base, A);
  }
  FACTOR_CS_RUN()
};
// 按 z 分组: x, y 组内 demean 后, 用**全体参与样本**做一个标量回归 (FWL)
struct CsGroupResid {
  using Self = CsGroupResid;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int base,
                             int A, const Param &) {
    const k::GidCol gi{zv, zm, base};
    k::grp_clear(sh);
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a, g = gi.gid(a);
      assert(g < kGrpCap && "组 id 超出 shared 槽上限");
      if (g >= 0 && xm[i] && ym[i]) { // 参与条件 = xm && ym && zm && g ≥ 0
        atomicAdd(&sh.gsx[g], xv[i]);
        atomicAdd(&sh.gsy[g], yv[i]);
        atomicAdd(&sh.gcn[g], 1);
        atomicMin(&sh.gmn[g], k::ord(yv[i]));
        atomicMax(&sh.gmx[g], k::ord(yv[i]));
      }
    }
    __syncthreads();
    // 退化 = 去均值后 ỹ ≡ 0 ⟺ 每组内 y 全并列; 逐组 lo/hi 精确判 (同 host), 不看 Σỹ²
    float sxy = 0.f, syy = 0.f;
    int c = 0;
    bool sp = false;
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a, g = gi.gid(a);
      const bool ok = g >= 0 && xm[i] && ym[i];
      const float fc = ok ? static_cast<float>(max(sh.gcn[g], 1)) : 1.f;
      const float dx = ok ? (xv[i] - sh.gsx[g] / fc) : 0.f; // 组内去均值
      const float dy = ok ? (yv[i] - sh.gsy[g] / fc) : 0.f;
      sxy += dx * dy;
      syy += dy * dy;
      c += ok ? 1 : 0;
      sp |= ok && sh.gmx[g] > sh.gmn[g]; // 每个非空组至少被一个参与资产代表 → 逐资产 OR = 逐组 OR
    }
    const int cnt = static_cast<int>(k::bsum(sh, static_cast<float>(c)));
    const float Sxy = k::bsum(sh, sxy);
    const float Syy = k::bsum(sh, syy);
    const bool ok2 = cnt >= 2 && k::bmax(sh, sp ? 1.f : 0.f) > 0.f;
    const float b = ok2 ? Sxy / Syy : 0.f; // 全体参与样本上的一个标量 β
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a, g = gi.gid(a);
      const bool ok = g >= 0 && xm[i] && ym[i];
      const float fc = ok ? static_cast<float>(max(sh.gcn[g], 1)) : 1.f;
      const float dx = ok ? (xv[i] - sh.gsx[g] / fc) : 0.f;
      const float dy = ok ? (yv[i] - sh.gsy[g] / fc) : 0.f;
      dev::store(ov, om, i, dx - b * dy, ok && ok2);
    }
  }
  FACTOR_CS_RUN()
};

} // namespace factor::gpu::cs
