#pragma once

// =============================================================================
// CS 轴挖掘后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; clang -x cuda)
// =============================================================================
//   语义契约见 factor/Contract.hpp, 算子真相表见 factor/OpTable.hpp.
//   本文件与 CS/Cpu.hpp / CS/Stream.hpp **完全独立**, 只依赖 Contract (含 Stat 的持有期几何) + CUDA/CUB.
//
//   【布局】SoA 行主序 [T][A]: 同一时刻的 A 个资产在内存里连续 → 截面归约天然合并访存.
//   【核配置总则】**一行一 block**, grid = T (19200 个 block), blockDim = 512:
//     · sm_75 每 SM 上限就是 1024 线程, 用 512 可以每 SM 驻 2 个 block,
//       让一个 block 卡在 __syncthreads / cub 归约时另一个 block 顶上 (1024 线程/块做不到).
//     · A = 5000 → 每线程 10 轮; 每轮 512×4B = 2 KB 连续读, 完美合并.
//     · 一整行 5000 float = 20 KB, 多趟重扫全部命中 L2 (3 MB), 所以直接重读 global,
//       不把行缓进 shared: 二元/三元算子缓 x+y+掩码就要 50 KB, 放不下 48 KB.
//   【归约】cub::BlockReduce (float), 自带 warp shfl.
//     计数用 float 归约: cnt ≤ 2²⁴ 内 float 表示精确, 省一套 TempStorage.
//   【两遍】高阶矩一律两遍: 第一趟求 μ, 第二趟在 (x−μ) 上求 M2/Cxy (不用 Σx² 单遍式).
//   【序统计】全族精确 (契约【序统计】), 同一套片上排序: 池内保序键 cub::BlockRadixSort (A ≤ kMaxA) →
//     秩 = 每格二分 less / eq (CsRank / CsNormRank / CsBucket / CsLinRank* 的元), 分位 = 排好的第 q_rank 个键 (CsQuantile / CsWinsor);
//     组秩 = 再排一次 (组 id, 全池名次) 复合键, 组段内二分 (CsGroupRank).
//   【GROUP】shared 累加槽 kGrpCap = 1024 组 (行业约 30), 无 global atomic 争用; 组极值用保序整数编码 atomicMin/Max.
//   【池 gm】样本集 = gm ∧ 有效 (契约【截面池 g】); 输出 = 池内统计量作用于每个有效 x, 不看 gm.
//     GROUP: 组无池成员 → 全池统计量 (CsGroupMean 用 bsum 顺手得全池和; CsGroupRank 用首趟排好的全池名次).
//   退化 → 契约第 6 条中性值 (select 一支); 掩码: 广播型恒真, 相对型 = 该资产输入格有效.
//   全程 branchless, 出口 dev::store 保证 ov/om 永不含 NaN/inf.
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/Stat/Contract.hpp"

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
// 对数比 ln(clamp(x/y, ε, 1/ε)) (与 host 的 log_ratio 同式): 双正 → 夹到 ±kLogCap; x ≤ 0 → −kLogCap; y ≤ 0 → +kLogCap; 双非正 → 0
__device__ __forceinline__ float log_ratio(float x, float y) {
  const bool px = x > 0.f, py = y > 0.f;
  const float l = logf(fmaxf(x, 1e-30f)) - logf(fmaxf(y, 1e-30f));
  return px && py ? fminf(fmaxf(l, -kLogCap), kLogCap) : (py ? -kLogCap : (px ? kLogCap : 0.f));
}
// 并列均秩的 pct rank: (avg_rank − 1)/(m − 1), m ≤ 1 → 0.5
__device__ __forceinline__ float pct_of(int less, int eq, int m) {
  if (m <= 1)
    return 0.5f;
  const double avg = static_cast<double>(less) + (static_cast<double>(eq) + 1.0) * 0.5;
  return static_cast<float>((avg - 1.0) / static_cast<double>(m - 1));
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

// 自带 min/max 仿函数: cub::Min / cub::Max 在 CCCL 3.x 已移除, 不依赖它
struct MinOp {
  __device__ __forceinline__ float operator()(float a, float b) const { return fminf(a, b); }
};
struct MaxOp {
  __device__ __forceinline__ float operator()(float a, float b) const { return fmaxf(a, b); }
};

// 矩族 / 分组族每块的 shared 家当: 分组槽 (20 KB) + cub 暂存 ≈ 21 KB
// (512 线程 × 2 块/SM = 42 KB ≤ 64 KB, 不构成占用率瓶颈)
struct Sh {
  typename BR::TempStorage red;
  float gsx[kGrpCap + 1], gsy[kGrpCap + 1]; // 分组累加槽; 末槽 kGrpCap = 无组 (组 id 有效但为负)
  int gcn[kGrpCap + 1];
  int gmn[kGrpCap + 1], gmx[kGrpCap + 1]; // 分组极值 (有序整数编码: GroupResid 的 y)
  float bc;                               // 广播槽
};

// float → 保序 int 编码 (a < b ⟺ ord(a) < ord(b)); ±0 先归一为 +0, 与 host 的 fmin/fmax 判等一致
__device__ __forceinline__ int ord(float f) {
  const int i = __float_as_int(f + 0.f);
  return i >= 0 ? i : i ^ 0x7fffffff;
}
// 逆编码 (ord 是对合: 负数支的符号位不动, 再异或一次即还原)
__device__ __forceinline__ float unord(int i) { return __int_as_float(i >= 0 ? i : i ^ 0x7fffffff); }

// ---- 块级归约 + 广播 (归约后必须两次 sync: 一次等广播写入, 一次防下轮复用暂存) ----
//   SH = 块的 shared 家当 (须有 red / bc)
template <class SH>
__device__ inline float bsum(SH &sh, float v) {
  const float r = BR(sh.red).Sum(v);
  if (threadIdx.x == 0)
    sh.bc = r;
  __syncthreads();
  const float o = sh.bc;
  __syncthreads();
  return o;
}
template <class SH>
__device__ inline float bmin(SH &sh, float v) {
  const float r = BR(sh.red).Reduce(v, MinOp());
  if (threadIdx.x == 0)
    sh.bc = r;
  __syncthreads();
  const float o = sh.bc;
  __syncthreads();
  return o;
}
template <class SH>
__device__ inline float bmax(SH &sh, float v) {
  const float r = BR(sh.red).Reduce(v, MaxOp());
  if (threadIdx.x == 0)
    sh.bc = r;
  __syncthreads();
  const float o = sh.bc;
  __syncthreads();
  return o;
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
__device__ inline void row_stats(Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const uint8_t *gm, int base, int A, RowStat &st) {
  int c = 0;
  float sx = 0.f, sy = 0.f, lx = FLT_MAX, hx = -FLT_MAX, ly = FLT_MAX, hy = -FLT_MAX;
  for (int a = threadIdx.x; a < A; a += kCB) {
    const int i = base + a;
    const bool g = ((NARY >= 2) ? (xm[i] & ym[i]) : xm[i]) & gm[i]; // 池内, 二元要两侧同时有效
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
    const bool g = ((NARY >= 2) ? (xm[i] & ym[i]) : xm[i]) & gm[i];
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
// 组 id: 格无效 → −1 (不参与); 有效但为负 (未知行业) → kNoGroup (无组, 以全池为组, 槽号 = kGrpCap); 与 host 的 gid / slot 同义
inline constexpr int kNoGroup = -2;
__device__ __forceinline__ int slot(int g) { return g >= 0 ? g : kGrpCap; } // 调用方保证 g != −1
struct GidCol {
  const float *v;
  const uint8_t *m;
  int base;
  __device__ __forceinline__ int gid(int a) const {
    if (!m[base + a])
      return -1;
    const int q = static_cast<int>(floorf(v[base + a]));
    return q < 0 ? kNoGroup : q;
  }
};
// 组内累加 x (和 y) 到 shared 槽
__device__ inline void grp_clear(Sh &sh) {
  for (int g = threadIdx.x; g <= kGrpCap; g += kCB) {
    sh.gsx[g] = 0.f;
    sh.gsy[g] = 0.f;
    sh.gcn[g] = 0;
    sh.gmn[g] = INT_MAX;
    sh.gmx[g] = INT_MIN;
  }
  __syncthreads();
}

// 矩族 / 分组族 (组秩除外) 的 CS 核: 一行一 block, 具体算法由 Op::row 以"块内集体操作"的风格写 (序统计族走下面的排序核)
template <class Op>
__global__ void row(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm, const uint8_t *gm, float *ov, uint8_t *om, int A,
                    Param p) {
  __shared__ Sh sh;
  Op::row(sh, xv, xm, yv, ym, zv, zm, gm, ov, om, blockIdx.x * A, A, p);
}

// ---- 序统计 (契约【序统计】): 一行一 block, 池内样本的保序键片上基数排序 → 秩 = 每格二分 less / eq, 第 r 名 = key[r − 1] ----
//   ITEMS (每线程键数) 按 A 分四级 2 / 6 / 10 / 16 (A ≤ 1024 / 3072 / 5120 / 8192 = kMaxA, 见 tiered); 排序暂存与排好的键分时共用 shared.
inline constexpr uint32_t kPadKey = 0xffffffffu; // 池外 / 无效 / 补位格的键 (有限 float 的键 ≤ 0xff7fffff), 排最后
__device__ __forceinline__ uint32_t ukey(float f) { return static_cast<uint32_t>(ord(f)) ^ 0x80000000u; }
__device__ __forceinline__ float unkey(uint32_t k) { return unord(static_cast<int>(k ^ 0x80000000u)); }
__device__ __forceinline__ int lower_bound(const uint32_t *s, int n, uint32_t key) { // 首个 ≥ key
  int lo = 0, hi = n;
  while (lo < hi) {
    const int mid = (lo + hi) >> 1;
    if (s[mid] < key)
      lo = mid + 1;
    else
      hi = mid;
  }
  return lo;
}
__device__ __forceinline__ int upper_bound(const uint32_t *s, int n, uint32_t key) { // 首个 > key
  int lo = 0, hi = n;
  while (lo < hi) {
    const int mid = (lo + hi) >> 1;
    if (s[mid] <= key)
      lo = mid + 1;
    else
      hi = mid;
  }
  return lo;
}
template <int ITEMS>
struct SortSh {
  using Sort = cub::BlockRadixSort<uint32_t, kCB, ITEMS>;
  static constexpr int N = kCB * ITEMS;
  union {
    typename Sort::TempStorage sort;
    uint32_t key[N]; // 排好的键 (补位键在末尾)
  };
};
// 块内集体: keys (blocked 排布) 排好写进 sh.key, 返回非补位键数. 入口先过线: 上一轮对 sh.key 的读与本轮的排序暂存是同一块 shared
template <int ITEMS>
__device__ inline int sort_keys(SortSh<ITEMS> &sh, uint32_t (&keys)[ITEMS]) {
  __syncthreads();
  typename SortSh<ITEMS>::Sort(sh.sort).Sort(keys);
  __syncthreads();
#pragma unroll
  for (int j = 0; j < ITEMS; ++j)
    sh.key[threadIdx.x * ITEMS + j] = keys[j];
  __syncthreads();
  return lower_bound(sh.key, SortSh<ITEMS>::N, kPadKey);
}
// 块内集体: 一行的池内样本 (该列有效 ∧ 池内) 排好进 sh.key, 返回样本数 n
template <int ITEMS>
__device__ inline int sort_pool(SortSh<ITEMS> &sh, const float *v, const uint8_t *m, const uint8_t *gm, int base, int A) {
  uint32_t keys[ITEMS];
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    const int a = threadIdx.x * ITEMS + j; // blocked 排布 (cub 要求)
    keys[j] = (a < A && (m[base + a] & gm[base + a])) ? ukey(v[base + a]) : kPadKey;
  }
  return sort_keys(sh, keys);
}
// x 在排好的 n 个样本里的 pct rank, 夹到 [0, 1] (池外格可越界); n = 0 → 0.5
__device__ __forceinline__ float sorted_pct(const uint32_t *key, int n, float x) {
  const uint32_t kx = ukey(x);
  const int less = lower_bound(key, n, kx), eq = upper_bound(key, n, kx) - less;
  return fminf(fmaxf(dev::pct_of(less, eq, n), 0.f), 1.f);
}

// 一元序统计核: 排好池内样本后由 Op::emit(key, n, …) 出全行
template <int ITEMS, class Op>
__global__ void __launch_bounds__(kCB) sorted_row(const float *xv, const uint8_t *xm, const uint8_t *gm, float *ov, uint8_t *om, int A, Param p) {
  __shared__ SortSh<ITEMS> sh;
  const int base = blockIdx.x * A;
  const int n = sort_pool(sh, xv, xm, gm, base, A);
  Op::emit(sh.key, n, xv, xm, ov, om, base, A, p);
}

// 组秩核 (CsGroupRank): 首趟排全池 (回退用, 也给每个样本一个全池名次 = 全池里比它小的个数);
//   次趟排复合键 (组 id, 全池名次), 只收有组的池内样本 → 组 g 的段 = [lb(g, 0), lb(g + 1, 0)),
//   段内 < x / ≤ x 的个数 = 复合键 < (g, rl) / < (g, ru) 的个数 (rl / ru = 全池里 < x / ≤ x 的个数; 同值同名次 ⇒ 判等精确)
inline constexpr int kRkBits = 14;
static_assert(factor::stat::kMaxA < (1 << kRkBits) && (static_cast<long long>(kGrpCap) << kRkBits) < static_cast<long long>(kPadKey), "复合键 (组 id, 全池名次) 须放进 32 位且小于补位键");
template <int ITEMS>
__global__ void __launch_bounds__(kCB) group_rank(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const uint8_t *gm, float *ov, uint8_t *om, int A) {
  __shared__ SortSh<ITEMS> sh;
  const int base = blockIdx.x * A;
  const GidCol gi{yv, ym, base};
  const int n = sort_pool(sh, xv, xm, gm, base, A);
  int gq[ITEMS], rl[ITEMS], ru[ITEMS]; // 本线程的资产 (跨步 a = tid + j·kCB): 组, 全池 lower / upper 名次
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    const int a = threadIdx.x + j * kCB;
    gq[j] = a < A ? gi.gid(a) : -1;
    assert(gq[j] < kGrpCap && "组 id 超出上限");
    const uint32_t kx = a < A ? ukey(xv[base + a]) : 0u;
    rl[j] = lower_bound(sh.key, n, kx);
    ru[j] = upper_bound(sh.key, n, kx);
  }
  uint32_t keys[ITEMS];
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) { // 有组的池内样本 (blocked 排布)
    const int a = threadIdx.x * ITEMS + j, g = a < A ? gi.gid(a) : -1;
    const bool in = g >= 0 && xm[base + a] && gm[base + a];
    keys[j] = in ? (static_cast<uint32_t>(g) << kRkBits) | static_cast<uint32_t>(lower_bound(sh.key, n, ukey(xv[base + a]))) : kPadKey;
  }
  const int m = sort_keys(sh, keys);
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    const int a = threadIdx.x + j * kCB;
    if (a >= A)
      continue;
    const int i = base + a;
    const bool ok = gq[j] != -1 && xm[i];
    float v = fminf(fmaxf(dev::pct_of(rl[j], ru[j] - rl[j], n), 0.f), 1.f); // 全池 (无组 / 组无池成员; 全池也空 → 0.5)
    if (ok && gq[j] >= 0) {
      const uint32_t g0 = static_cast<uint32_t>(gq[j]) << kRkBits;
      const int s = lower_bound(sh.key, m, g0), e = lower_bound(sh.key, m, g0 + (1u << kRkBits));
      const int less = lower_bound(sh.key, m, g0 | static_cast<uint32_t>(rl[j])) - s, le = lower_bound(sh.key, m, g0 | static_cast<uint32_t>(ru[j])) - s;
      v = e > s ? fminf(fmaxf(dev::pct_of(less, le - less, e - s), 0.f), 1.f) : v;
    }
    dev::store(ov, om, i, v, ok);
  }
}

// 按 A 选每线程键数 ITEMS 启动: L::go<ITEMS>(args…)
static_assert(kCB * 16 >= factor::stat::kMaxA, "末级 ITEMS 须盖住 kMaxA");
template <class L, class... Args>
inline void tiered(int A, const Args &...args) {
  assert(A >= 1 && A <= factor::stat::kMaxA && "片上排序容量 kMaxA");
  if (A <= kCB * 2)
    L::template go<2>(args...);
  else if (A <= kCB * 6)
    L::template go<6>(args...);
  else if (A <= kCB * 10)
    L::template go<10>(args...);
  else
    L::template go<16>(args...);
  FACTOR_CUDA_OK(cudaGetLastError());
}
template <class Op>
struct SortedL {
  template <int I>
  static void go(const Call &c, cudaStream_t s) { sorted_row<I, Op><<<c.T, kCB, 0, s>>>(c.x.v, c.x.m, c.g, c.o.v, c.o.m, c.A, c.p); }
};
struct GroupRankL {
  template <int I>
  static void go(const Call &c, cudaStream_t s) { group_rank<I><<<c.T, kCB, 0, s>>>(c.x.v, c.x.m, c.y.v, c.y.m, c.g, c.o.v, c.o.m, c.A); }
};

} // namespace k

// =============================================================================
// 算子: Contract【调用形状】(元数不到的槽为空, 池掩码 c.g 必填, 全部设备指针)
// =============================================================================
#define FACTOR_CS_RUN()                                                                                           \
  static size_t scratch(const Call &) { return 0; } /* 全部在 shared 里做完, 不要临时显存 */                      \
  static void run(const Call &c, void *, cudaStream_t stream) {                                                   \
    assert(c.g != nullptr && c.o.v && c.o.m && c.A >= 1 && static_cast<long long>(c.T) * c.A < (1LL << 31));      \
    k::row<Self><<<c.T, kCB, 0, stream>>>(c.x.v, c.x.m, c.y.v, c.y.m, c.z.v, c.z.m, c.g, c.o.v, c.o.m, c.A, c.p); \
    FACTOR_CUDA_OK(cudaGetLastError());                                                                           \
  }

// ---- REDUCE (7): 沿资产归约, 广播型算子对全行写同一个值 ----
#define FACTOR_CS_RED(Name, NARY, BODY)                                                                                                                              \
  struct Name {                                                                                                                                                      \
    using Self = Name;                                                                                                                                               \
    __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *, const uint8_t *, const uint8_t *gm, \
                               float *ov, uint8_t *om, int base, int A, const Param &p) {                                                                            \
      k::RowStat s;                                                                                                                                                  \
      k::row_stats<NARY>(sh, xv, xm, yv, ym, gm, base, A, s);                                                                                                        \
      for (int a = threadIdx.x; a < A; a += kCB) {                                                                                                                   \
        const int i = base + a;                                                                                                                                      \
        const DVal x{xv[i], xm[i]};                                                                                                                                  \
        const DVal y{(NARY >= 2) ? yv[i] : 0.f, static_cast<uint8_t>((NARY >= 2) ? ym[i] : 0)};                                                                      \
        float v = 0.f;                                                                                                                                               \
        bool m = false;                                                                                                                                              \
        (void)x, (void)y, (void)p;                                                                                                                                   \
        BODY                                                                                                                                                         \
            dev::store(ov, om, i, v, m);                                                                                                                             \
      }                                                                                                                                                              \
    }                                                                                                                                                                \
    FACTOR_CS_RUN()                                                                                                                                                  \
  };

// 截面均值广播; 空池 → 0 (mx = 0/1)
FACTOR_CS_RED(CsMean, 1, {
  v = s.mx;
  m = true;
})
// 截面样本标准差广播 (ddof=1); n < 2 / 全并列 → 0
FACTOR_CS_RED(CsStd, 1, {
  v = (s.cnt >= 2 && s.sx()) ? sqrtf(fmaxf(s.M2x, 0.f) / static_cast<float>(max(s.cnt - 1, 1))) : 0.f;
  m = true;
})
// (x − μ)/σ (相对型); σ 退化 → 0
FACTOR_CS_RED(CsZ, 1, {
  const float sd = sqrtf(fmaxf(s.M2x, 0.f) / static_cast<float>(max(s.cnt - 1, 1)));
  v = (s.cnt >= 2 && s.sx()) ? (x.v - s.mx) / sd : 0.f;
  m = x.m;
})
// x 对 y 截面 OLS (含截距) 残差 (相对型); y 无离散度 → β = 0, 即 x − μ^x
FACTOR_CS_RED(CsResid, 2, {
  const float b = (s.cnt >= 2 && s.sy()) ? s.Cxy / s.M2y : 0.f;
  v = (x.v - s.mx) - b * (y.v - s.my);
  m = x.m && y.m;
})
// OLS 斜率广播; y 无离散度 → 0
FACTOR_CS_RED(CsBeta, 2, {
  v = (s.cnt >= 2 && s.sy()) ? s.Cxy / s.M2y : 0.f;
  m = true;
})
// 截面 Pearson 广播 (按规格不 clamp |ρ| ≤ 1); 任一侧无离散度 → 0
FACTOR_CS_RED(CsCorr, 2, {
  // 非全并列 ⇒ 两个方差为正, |ρ| 有柯西–施瓦茨上界; 乘积走 double 防下溢
  v = (s.cnt >= 2 && s.sx() && s.sy())
          ? static_cast<float>(static_cast<double>(s.Cxy) / sqrt(fmax(static_cast<double>(s.M2x) * s.M2y, 0.0)))
          : 0.f;
  m = true;
})

// ---- 序统计 (5): 全走 k::sorted_row (契约【序统计】; 样本集 = 本行池内有效值, 排好的键 key[0..n)) ----
//   Op::emit(key, n, …) 在排好的键上出全行: 秩族每格二分, 分位族取第 q_rank 个键
#define FACTOR_CS_SORTED_RUN(Self)                                                               \
  static size_t scratch(const Call &) { return 0; } /* 全部在 shared 里做完, 不要临时显存 */     \
  static void run(const Call &c, void *, cudaStream_t stream) {                                  \
    assert(c.g != nullptr && c.o.v && c.o.m && static_cast<long long>(c.T) * c.A < (1LL << 31)); \
    k::tiered<k::SortedL<Self>>(c.A, c, stream);                                                 \
  }
#define FACTOR_CS_EMIT \
  __device__ static void emit(const uint32_t *key, int n, const float *xv, const uint8_t *xm, float *ov, uint8_t *om, int base, int A, const Param &p)

// pct rank (并列均秩); 空池 / 池内全并列 → 0.5
struct CsRank {
  FACTOR_CS_EMIT {
    (void)p;
    for (int a = threadIdx.x; a < A; a += kCB)
      dev::store(ov, om, base + a, k::sorted_pct(key, n, xv[base + a]), xm[base + a]);
  }
  FACTOR_CS_SORTED_RUN(CsRank)
};
// Φ⁻¹(clamp(pct, 1/(N+1), N/(N+1))); 空池 → pct 0.5 夹到 [1/2, 1/2] → Φ⁻¹(0.5) = 0
//   注: host 侧用 Contract::probit (Wichura AS241), 这里用 CUDA 自带 normcdfinvf, 两者差 ~1e-7
//   → **本算子对拍容差要单独放宽** (Contract.hpp 的注释里也已写明)
struct CsNormRank {
  FACTOR_CS_EMIT {
    (void)p;
    const float fn = static_cast<float>(max(n, 1));
    for (int a = threadIdx.x; a < A; a += kCB) {
      const float pct = k::sorted_pct(key, n, xv[base + a]);
      dev::store(ov, om, base + a, normcdfinvf(fminf(fmaxf(pct, 1.f / (fn + 1.f)), fn / (fn + 1.f))), xm[base + a]);
    }
  }
  FACTOR_CS_SORTED_RUN(CsNormRank)
};
// k 分位广播 (k = 0.5 即中位; 第 q_rank(k, n) 个样本, 不做偶数上下平均); 空池 → 0
struct CsQuantile {
  FACTOR_CS_EMIT {
    (void)xm;
    const float q = n >= 1 ? k::unkey(key[q_rank(p.k, n) - 1]) : 0.f;
    for (int a = threadIdx.x; a < A; a += kCB)
      dev::store(ov, om, base + a, q, true);
  }
  FACTOR_CS_SORTED_RUN(CsQuantile)
};
// 分位缩尾: clamp 到 [Q_k, Q_{1−k}] (上分位的 1 − k 在 double 下减, 见 q_rank); 空池 → 不缩尾, x 原值
struct CsWinsor {
  FACTOR_CS_EMIT {
    const float ql = n >= 1 ? k::unkey(key[q_rank(p.k, n) - 1]) : -FLT_MAX;
    const float qh = n >= 1 ? k::unkey(key[q_rank(1.0 - static_cast<double>(p.k), n) - 1]) : FLT_MAX;
    for (int a = threadIdx.x; a < A; a += kCB)
      dev::store(ov, om, base + a, fminf(fmaxf(xv[base + a], ql), qh), xm[base + a]);
  }
  FACTOR_CS_SORTED_RUN(CsWinsor)
};
// bucket_of(pct, k) ∈ 0..k−1 (空池 / 全并列 → pct = 0.5 → 中间桶)
struct CsBucket {
  FACTOR_CS_EMIT {
    const int K = max(1, static_cast<int>(p.k));
    for (int a = threadIdx.x; a < A; a += kCB)
      dev::store(ov, om, base + a, static_cast<float>(bucket_of(k::sorted_pct(key, n, xv[base + a]), K)), xm[base + a]);
  }
  FACTOR_CS_SORTED_RUN(CsBucket)
};
// ---- GROUP (3) ----
// 按 y 分组 (整数 id) 的组均值广播 (无组 / 组无池成员 → 全池均值; 组 id 格无效才无效)
struct CsGroupMean {
  using Self = CsGroupMean;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *, const uint8_t *, const uint8_t *gm, float *ov,
                             uint8_t *om, int base, int A, const Param &) {
    const k::GidCol gi{yv, ym, base};
    k::grp_clear(sh);
    float sa = 0.f; // 全池 (回退; 含无组的池成员), 顺手归约
    int na = 0;
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a, g = gi.gid(a);
      assert(g < kGrpCap && "组 id 超出 shared 槽上限");
      if (g != -1 && xm[i] && gm[i]) {
        atomicAdd(&sh.gsx[k::slot(g)], xv[i]); // shared 槽累加, 不碰 global atomic (无组槽的和不用, 只为无分支)
        atomicAdd(&sh.gcn[k::slot(g)], 1);
        sa += xv[i];
        ++na;
      }
    }
    __syncthreads();
    const int cnta = static_cast<int>(k::bsum(sh, static_cast<float>(na)));
    const float ma = k::bsum(sh, sa) / static_cast<float>(max(cnta, 1)); // 全池也空 → 0
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a, g = gi.gid(a);
      const int c = (g >= 0) ? sh.gcn[g] : 0; // 无组 → c = 0 → 全池
      dev::store(ov, om, i, c >= 1 ? sh.gsx[g] / static_cast<float>(c) : ma, xm[i] && g != -1);
    }
  }
  FACTOR_CS_RUN()
};

// 按 y 分组的组内 pct rank (样本 = 本组池成员; 无组 / 组无池成员 → 全池; 组 id 格无效才无效). 走 k::group_rank
struct CsGroupRank {
  static size_t scratch(const Call &) { return 0; } /* 全部在 shared 里做完, 不要临时显存 */
  static void run(const Call &c, void *, cudaStream_t stream) {
    assert(c.g != nullptr && c.o.v && c.o.m && static_cast<long long>(c.T) * c.A < (1LL << 31));
    k::tiered<k::GroupRankL>(c.A, c, stream);
  }
};
// 按 z 分组: x, y 组内 demean 后, 用**池内参与样本**做一个标量回归 (FWL); 输出对象 = 全部参与者,
// 无组 (组 id 有效但为负, 占末槽) / 组无池成员 → 用全池均值去均值; 组 id 格无效才无效
struct CsGroupResid {
  using Self = CsGroupResid;
  __device__ static void row(k::Sh &sh, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm, const uint8_t *gm, float *ov,
                             uint8_t *om, int base, int A, const Param &) {
    const k::GidCol gi{zv, zm, base};
    k::grp_clear(sh);
    float sxa = 0.f, sya = 0.f; // 全池 (回退; 含无组槽), 顺手归约
    int na = 0;
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a, g = gi.gid(a);
      assert(g < kGrpCap && "组 id 超出 shared 槽上限");
      if (g != -1 && xm[i] && ym[i] && gm[i]) { // 拟合样本 = 参与 (xm && ym && zm) ∧ 池内; 无组进末槽 (只用其 y 极值做并列判据)
        const int s = k::slot(g);
        atomicAdd(&sh.gsx[s], xv[i]);
        atomicAdd(&sh.gsy[s], yv[i]);
        atomicAdd(&sh.gcn[s], 1);
        atomicMin(&sh.gmn[s], k::ord(yv[i]));
        atomicMax(&sh.gmx[s], k::ord(yv[i]));
        sxa += xv[i];
        sya += yv[i];
        ++na;
      }
    }
    __syncthreads();
    const float fna = static_cast<float>(max(static_cast<int>(k::bsum(sh, static_cast<float>(na))), 1));
    const float mxa = k::bsum(sh, sxa) / fna, mya = k::bsum(sh, sya) / fna;
    // 退化 = 去均值后 ỹ ≡ 0 ⟺ 每组内 y 全并列; 逐组 lo/hi 精确判 (同 host, 无组槽同此判), 不看 Σỹ²
    float sxy = 0.f, syy = 0.f;
    int c = 0;
    bool sp = false;
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a, g = gi.gid(a);
      const bool ok = g != -1 && xm[i] && ym[i];
      const bool pl = ok && gm[i];
      const int gc = (ok && g >= 0) ? sh.gcn[g] : 0; // 无组 → gc = 0 → 全池均值
      const float fc = static_cast<float>(max(gc, 1));
      const float dx = !ok ? 0.f : (gc >= 1 ? xv[i] - sh.gsx[g] / fc : xv[i] - mxa); // 组内去均值 (无组 / 组空 → 全池)
      const float dy = !ok ? 0.f : (gc >= 1 ? yv[i] - sh.gsy[g] / fc : yv[i] - mya);
      sxy += pl ? dx * dy : 0.f;
      syy += pl ? dy * dy : 0.f;
      c += pl ? 1 : 0;
      sp |= pl && sh.gmx[k::slot(g)] > sh.gmn[k::slot(g)]; // 每个非空槽至少被一个池内参与资产代表 → 逐资产 OR = 逐槽 OR
    }
    const int cnt = static_cast<int>(k::bsum(sh, static_cast<float>(c)));
    const float Sxy = k::bsum(sh, sxy);
    const float Syy = k::bsum(sh, syy);
    const bool ok2 = cnt >= 2 && k::bmax(sh, sp ? 1.f : 0.f) > 0.f;
    const float b = ok2 ? Sxy / Syy : 0.f; // 池内参与样本上的一个标量 β; 退化 → β = 0, 输出 x̃
    for (int a = threadIdx.x; a < A; a += kCB) {
      const int i = base + a, g = gi.gid(a);
      const bool ok = g != -1 && xm[i] && ym[i];
      const int gc = (ok && g >= 0) ? sh.gcn[g] : 0;
      const float fc = static_cast<float>(max(gc, 1));
      const float dx = !ok ? 0.f : (gc >= 1 ? xv[i] - sh.gsx[g] / fc : xv[i] - mxa);
      const float dy = !ok ? 0.f : (gc >= 1 ? yv[i] - sh.gsy[g] / fc : yv[i] - mya);
      dev::store(ov, om, i, dx - b * dy, ok);
    }
  }
  FACTOR_CS_RUN()
};

// ---- N 元 (2) ----
//   各元 x̃ = 池内精确 pct − 1/2 (契约【N 元】【序统计】): 逐元排一次 (k::sort_pool), 每格二分; 线程只管自己的跨步资产 (a = tid + j·kCB),
//   各元结果留在线程内 (ITEMS 个资产 × 元数), 不落 shared / 显存.
//   定权: 一核一行一 block, 逐元累 Σ p.w_i x̃_i (double, 按元序).
//   回归: xs[0..n) 特征元, xs[n] = h 持有期做多标签 (Contract【N 元回归】). 两核, 工作区 = 每标签行一个矩向量 [T][M] double:
//     核 1 一行一 block: 行 s 的 n+1 元逐个排序 → 逐资产 x̃ 累矩 (线程内 double) → 逐矩块归约 (cub 固定序, 确定);
//     核 2 一行一 block: 线程 0 求 u(t) → 窗和 (G = kCB / M 组, 组 g 按 s ≡ g 跨步累, 再按组序合并, 确定) → 线程 0 岭解 →
//     行 t 各元逐个排序 → 输出拟合值
namespace k {

using BRD = cub::BlockReduce<double, kCB>;

struct LrArgs { // kernel 参数区
  In x[kMaxIn];
  int n;
};

template <int ITEMS>
__global__ void __launch_bounds__(kCB) lr_wsum(const LrArgs a, const uint8_t *gm, float *ov, uint8_t *om, int A, Param p) {
  __shared__ SortSh<ITEMS> sh;
  const int n = a.n, base = blockIdx.x * A;
  double v[ITEMS];
  bool ok[ITEMS];
#pragma unroll
  for (int j = 0; j < ITEMS; ++j)
    v[j] = 0.0, ok[j] = true;
  for (int e = 0; e < n; ++e) {
    const int ne = sort_pool(sh, a.x[e].v, a.x[e].m, gm, base, A);
#pragma unroll
    for (int j = 0; j < ITEMS; ++j) {
      const int c = threadIdx.x + j * kCB;
      if (c >= A)
        continue;
      const int i = base + c;
      ok[j] = ok[j] && a.x[e].m[i];
      v[j] += static_cast<double>(p.w[e]) * (static_cast<double>(sorted_pct(sh.key, ne, a.x[e].v[i])) - 0.5);
    }
  }
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    const int c = threadIdx.x + j * kCB;
    if (c < A)
      dev::store(ov, om, base + c, static_cast<float>(v[j]), ok[j]);
  }
}

template <int ITEMS>
__global__ void __launch_bounds__(kCB) lr_mom(const LrArgs a, const uint8_t *gm, int A, Geo geo, int hold, double *mom) {
  __shared__ SortSh<ITEMS> sh;
  __shared__ typename BRD::TempStorage redd;
  const int s = blockIdx.x, n = a.n, M = lr_moms(n), base = s * A;
  double *out = mom + static_cast<size_t>(s) * M;
  if (!hold_on(static_cast<HoldId>(hold), geo).opens(s)) { // 段末尾部行恒 0 (块内一致)
    for (int m = threadIdx.x; m < M; m += kCB)
      out[m] = 0.0;
    return;
  }
  float pct[ITEMS][kMaxIn]; // 本线程各资产的各元 pct
  bool ok[ITEMS];           // 样本 = 池内 ∧ 全部元与标签有效
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    const int c = threadIdx.x + j * kCB;
    ok[j] = c < A && gm[base + c];
  }
  for (int e = 0; e <= n; ++e) {
    const int ne = sort_pool(sh, a.x[e].v, a.x[e].m, gm, base, A);
#pragma unroll
    for (int j = 0; j < ITEMS; ++j) {
      const int c = threadIdx.x + j * kCB;
      if (c >= A)
        continue;
      ok[j] = ok[j] && a.x[e].m[base + c];
      pct[j][e] = sorted_pct(sh.key, ne, a.x[e].v[base + c]);
    }
  }
  double acc[kLrMoms];
  for (int m = 0; m < M; ++m)
    acc[m] = 0.0;
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    if (!ok[j])
      continue;
    double x[kMaxIn];
    for (int e = 0; e <= n; ++e)
      x[e] = static_cast<double>(pct[j][e]) - 0.5;
    for (int p = 0; p < n; ++p) {
      for (int q = p; q < n; ++q)
        acc[lr_ix(p, q, n)] += x[p] * x[q];
      acc[n * (n + 1) / 2 + p] += x[p] * x[n];
    }
  }
  for (int m = 0; m < M; ++m) {
    const double r = BRD(redd).Sum(acc[m]);
    if (threadIdx.x == 0)
      out[m] = r;
    __syncthreads();
  }
}

template <int ITEMS>
__global__ void __launch_bounds__(kCB) lr_fit(const LrArgs a, const uint8_t *gm, float *ov, uint8_t *om, int A, Geo geo, Param p, const double *mom) {
  __shared__ SortSh<ITEMS> sh;
  __shared__ double part[kCB]; // 窗和的组部分和 [g][m]
  __shared__ double acc[kLrMoms], w[kLrN];
  __shared__ int su;
  const int t = blockIdx.x, n = a.n, M = lr_moms(n), base = t * A, d = p.d;
  if (threadIdx.x == 0)
    su = hold_on(p.h, geo).last(t);
  __syncthreads();
  const int u = su, s0 = max(0, u - d + 1), G = kCB / M;
  if (threadIdx.x < G * M) {
    const int g = threadIdx.x / M, m = threadIdx.x % M;
    double v = 0.0;
    for (int s = s0 + g; s <= u; s += G)
      v += mom[static_cast<size_t>(s) * M + m];
    part[g * M + m] = v;
  }
  __syncthreads();
  if (threadIdx.x < M) {
    double v = 0.0;
    for (int g = 0; g < G; ++g)
      v += part[g * M + threadIdx.x];
    acc[threadIdx.x] = v;
  }
  __syncthreads();
  if (threadIdx.x == 0)
    lr_solve(acc, n, p.k, w);
  __syncthreads();
  double v[ITEMS];
  bool ok[ITEMS];
#pragma unroll
  for (int j = 0; j < ITEMS; ++j)
    v[j] = 0.0, ok[j] = true;
  for (int e = 0; e < n; ++e) {
    const int ne = sort_pool(sh, a.x[e].v, a.x[e].m, gm, base, A);
#pragma unroll
    for (int j = 0; j < ITEMS; ++j) {
      const int c = threadIdx.x + j * kCB;
      if (c >= A)
        continue;
      const int i = base + c;
      ok[j] = ok[j] && a.x[e].m[i];
      v[j] += w[e] * (static_cast<double>(sorted_pct(sh.key, ne, a.x[e].v[i])) - 0.5);
    }
  }
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    const int c = threadIdx.x + j * kCB;
    if (c < A)
      dev::store(ov, om, base + c, static_cast<float>(v[j]), ok[j]);
  }
}

struct LrWsumL {
  template <int I>
  static void go(const LrArgs &a, const Call &c, cudaStream_t s) { lr_wsum<I><<<c.T, kCB, 0, s>>>(a, c.g, c.o.v, c.o.m, c.A, c.p); }
};
struct LrMomL {
  template <int I>
  static void go(const LrArgs &a, const Call &c, double *mom, cudaStream_t s) { lr_mom<I><<<c.T, kCB, 0, s>>>(a, c.g, c.A, c.geo, c.p.h, mom); }
};
struct LrFitL {
  template <int I>
  static void go(const LrArgs &a, const Call &c, const double *mom, cudaStream_t s) { lr_fit<I><<<c.T, kCB, 0, s>>>(a, c.g, c.o.v, c.o.m, c.A, c.geo, c.p, mom); }
};

} // namespace k

struct CsLinRankFix {
  static size_t scratch(const Call &) { return 0; }
  static void run(const Call &c, void *, cudaStream_t stream) {
    assert(c.g != nullptr && c.o.v && c.o.m && static_cast<long long>(c.T) * c.A < (1LL << 31));
    assert(c.n >= 2 && c.n <= kLrN && "至少 2 个特征元");
    k::LrArgs a;
    for (int i = 0; i < c.n; ++i)
      a.x[i] = c.xs[i];
    a.n = c.n;
    k::tiered<k::LrWsumL>(c.A, a, c, stream);
  }
};

struct CsLinRankRoll {
  static size_t scratch(const Call &c) { return static_cast<size_t>(c.T) * lr_moms(c.n - 1) * sizeof(double); }
  static void run(const Call &c, void *ws, cudaStream_t stream) {
    assert(c.g != nullptr && c.o.v && c.o.m && static_cast<long long>(c.T) * c.A < (1LL << 31));
    assert(c.n >= 3 && c.n <= kMaxIn && ws && "至少 2 个特征元 + 目标");
    k::LrArgs a;
    for (int i = 0; i < c.n; ++i)
      a.x[i] = c.xs[i];
    a.n = c.n - 1;
    double *mom = static_cast<double *>(ws);
    k::tiered<k::LrMomL>(c.A, a, c, mom, stream);
    k::tiered<k::LrFitL>(c.A, a, c, static_cast<const double *>(mom), stream);
  }
};

} // namespace factor::gpu::cs
