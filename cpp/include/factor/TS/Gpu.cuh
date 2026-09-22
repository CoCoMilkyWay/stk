#pragma once

// =============================================================================
// TS 轴挖掘后端 (CUDA, sm_75 / RTX  / 336 GB·s⁻¹; clang -x cuda)
// =============================================================================
//   语义契约见 factor/Contract.hpp, 算子真相表见 factor/OpTable.hpp.
//   本文件与 TS/Naive.hpp / TS/Stream.hpp **完全独立** (对拍才有意义), 只依赖 Contract + CUDA/CUB.
//
//   【布局】SoA 行主序 [T][A], 下标 t*A + a. 同一时刻的资产在内存里连续
//     → TS 方向"一线程一资产 + 沿 t 串行"天然合并访存 (相邻线程读相邻地址, 128B/warp).
//     典型规模 T = 19200 (80 段 × kSegLen 240), A = 5000, 单平面 fp32 = 384 MB.
//
//   【核配置总则】
//     POINT : 2D 网格逐格融合, grid(⌈A/256⌉, min(T,512)) × 256 线程, 纯带宽.
//     SCAN  : 一线程一资产. EXPAND 按段 (240 步寄存器串行, 段天然独立);
//             ROLL 按块 (块长 C = 240·⌈d/240⌉) 预热 d−1 步后滑窗 add/sub,
//             每块重建累加器 = 契约要求的"分块", 误差不跨块累积.
//             **不用全局前缀和平面**: 五和 (Σx,Σy,Σx²,Σy²,Σxy) 各需 384 MB,
//             再加 pre/suf 两级就是 3.8 GB, 6 GB 卡上放不下 (详见回执). 滑窗差分等价且 ws = 0.
//     EXTREME: 滑窗极值 van Herk / Gil-Werman: 以 d 为块长, 块内前缀极值 + 前一块后缀极值合并.
//             一线程一资产串行, 与 d 无关的常数趟; 工作区只存前一块的后缀 (环形 [d][A]), 不再有 log₂d 趟全平面.
//     HIST  : 一线程一资产 + 窗内多趟. 每线程只留 16 个计数器 (粗 16 组 × 细 16 桶 = 256 桶),
//             桶号与单趟 256 桶直方图逐位一致; blockDim = 64 让 d·64·4B 的窗切片留在 L1.
//     RECUR : 仿射复合 (a,b)∘(c,d) = (a·c, b·c+d) 的分块 scan (块内 → 块间 → 回填).
//
//   【数值】幂和 fp64, 按段/按块重建; 高阶矩两遍: 第一遍扫出块内 (μ,σ) 作参考,
//     第二遍在 z = (x−μ)/σ 上累幂和 (中心矩对平移不变, 偏度峰度对缩放不变 → 代数等价, 条件数好得多).
//     全并列判据 (契约第 2 条) 不看累加器: ROLL 的 add/sub 残留会让常值窗的 M2 ≠ 0, 阈值判据在这里必然与
//     CPU 不一致. 改用 Chg 追踪 "最近一次相邻有效样本不等的位置" 与 "窗内首个有效位置" (都是整数, 无减法) → 精确.
//     全程 branchless (select, 不用分支), 出口 dev::store 保证 ov/om 永不含 NaN/inf.
//
//   【禁用】--use_fast_math / __logf / __fdividef: 逐点算子要与 host 侧位级一致.
// =============================================================================

#include "factor/Contract.hpp"

#include <cassert>
#include <cfloat>
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

namespace factor::gpu::ts {

// 线程块尺寸: 逐点 / 一线程一资产 都用 256 (20 个 block 覆盖 A=5000, 合并访存 1KB/warp-row);
// HIST 用 64 → 窗切片 d·64·4B (d=240 时 61 KB) 落在 96 KB 的 L1 里, 多趟重扫全部命中
inline constexpr int kPB = 256;
inline constexpr int kTB = 256;
inline constexpr int kHB = 64;

namespace k {

// =============================================================================
// 1. POINT: 逐格融合
// =============================================================================
template <int NARY, class Op>
__global__ void point(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, Param p) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  for (int t = blockIdx.y; t < T; t += gridDim.y) { // grid-stride over t: 网格封顶 512 行
    const int i = t * A + a;
    DVal x{0.f, 0}, y{0.f, 0}, z{0.f, 0};
    if constexpr (NARY >= 1)
      x = DVal{xv[i], xm[i]};
    if constexpr (NARY >= 2)
      y = DVal{yv[i], ym[i]};
    if constexpr (NARY >= 3)
      z = DVal{zv[i], zm[i]};
    float v = 0.f;
    bool m = false;
    Op::apply(x, y, z, t % kSegLen, p, v, m);
    dev::store(ov, om, i, v, m);
  }
}

// GATHER: 取 t−d 行 (d 只改地址, 不改算法)
template <bool DELTA>
__global__ void gather(const float *xv, const uint8_t *xm, float *ov, uint8_t *om, int T, int A, int d) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  for (int t = blockIdx.y; t < T; t += gridDim.y) {
    const int i = t * A + a;
    const int j = (t - d) * A + a;
    const bool ok = t >= d;
    const float xo = ok ? xv[j] : 0.f; // 越界不读
    const bool mo = ok && xm[j] != 0;
    dev::store(ov, om, i, DELTA ? (xv[i] - xo) : xo, DELTA ? (mo && xm[i] != 0) : mo);
  }
}

// =============================================================================
// 2. 累加器 / 两遍参考量 (EXPAND 与 ROLL 共用同一批 functor)
// =============================================================================
struct RefNone {};
struct Ref1 {
  float mu, sg;
};
struct Ref2 {
  float mx, sx, my, sy;
};

// 第一遍: 扫 [from,to) 的 (μ,σ) 作为标准化参考. 参考量只影响条件数, 不影响代数结果
__device__ inline void ref_scan(const float *v, const uint8_t *mk, int a, int A, int from, int to, float &mu, float &sg) {
  int n = 0;
  float s1 = 0.f, s2 = 0.f;
  for (int s = from; s < to; ++s) {
    const int i = s * A + a;
    const float u = v[i];
    const float g = mk[i] ? 1.f : 0.f;
    n += mk[i] ? 1 : 0;
    s1 += g * u;
    s2 += g * u * u;
  }
  mu = n > 0 ? s1 / static_cast<float>(n) : 0.f;
  const float var = n > 1 ? (s2 - s1 * mu) / static_cast<float>(n - 1) : 0.f;
  sg = (var > 0.f && dev::fin(var)) ? sqrtf(var) : 1.f; // σ 恒为有限正数
}

struct StRaw {
  int n;
  double s1, s2, sa;
}; // n, Σx, Σx², Σ|x|
// 幂和累加器一律 fp64. 理由 (实测): ROLL 是 add/sub 滑窗, 一个大值进窗再出窗后,
// 累加器里会永久留下 ~eps·max(z²) 的绝对残差; 重尾数据 (student-t df=2.5) 下 z 可达 1e2,
// 残差 ~1e-3, 而后续窗口自身的 m2 可能只有 1e-2 → 相对误差 10%, 四阶矩再被 m2² 放大.
// fp64 把这一残差压到 1e-12, 代价是 sm_75 上 1/32 的 fp64 吞吐 (这几个算子仍在 10 ms 量级).
struct StZ {
  int n;
  double z1, z2, z3, z4;
}; // n, Σz..Σz⁴
struct StZ2 {
  int n;
  double zx, zy, zxx, zyy, zxy;
};

__device__ __forceinline__ void upd_z(StZ &s, DVal x, int w, const Ref1 &r) {
  const int g = x.m ? w : 0;
  const double fg = static_cast<double>(g);
  const double z = (static_cast<double>(x.v) - r.mu) / r.sg;
  s.n += g;
  s.z1 += fg * z;
  s.z2 += fg * z * z;
  s.z3 += fg * z * z * z;
  s.z4 += fg * z * z * z * z;
}
__device__ __forceinline__ void upd_z2(StZ2 &s, DVal x, DVal y, int w, const Ref2 &r) {
  const int g = (x.m && y.m) ? w : 0; // 二元: 两侧同时有效
  const double fg = static_cast<double>(g);
  const double zx = (static_cast<double>(x.v) - r.mx) / r.sx, zy = (static_cast<double>(y.v) - r.my) / r.sy;
  s.n += g;
  s.zx += fg * zx;
  s.zy += fg * zy;
  s.zxx += fg * zx * zx;
  s.zyy += fg * zy * zy;
  s.zxy += fg * zx * zy;
}

// ---- 全并列判据的滑窗实现 (契约第 2 条; 不能看累加器, 见文件头) ----
//   窗内有效样本全并列 ⟺ 不存在窗内位置 j > f 使 x_j ≠ x_{j 之前最近的有效样本} (f = 窗内首个有效位置):
//   若 j > f 且 j 处"变动", 其前一有效样本 ≥ f 也在窗内, 二者不等 → 有离散度;
//   反之窗内有效序列从 f 起若非常值, 必有相邻一对不等, 该位置 > f.
//   last: 最近一次变动位置, 只前进; f: 由核维护, 摊还前进 (每行掩码至多读一次). 都无减法.
struct Chg {
  int lastx, lasty; // 最近变动位置 (块内坐标); 远负 = 从未变动
  float lvx, lvy;   // 上一有效 (对) 样本
  bool seen;
  __device__ __forceinline__ void init() {
    lastx = lasty = -(1 << 29);
    lvx = lvy = 0.f;
    seen = false;
  }
  __device__ __forceinline__ void push(bool g, float x, float y, int li) {
    const bool cx = g && seen && x != lvx, cy = g && seen && y != lvy;
    lastx = cx ? li : lastx;
    lasty = cy ? li : lasty;
    lvx = g ? x : lvx;
    lvy = g ? y : lvy;
    seen = seen || g;
  }
};

// z 尺度总体中心矩 → **一律还原回原尺度**: m_k = σ^k · m_k(z) (与 host 同一量纲, 对拍容差按原尺度算)
struct Mom {
  double mean, M2, m2, m3, m4;
};
__device__ inline void mom_of(const StZ &s, const Ref1 &r, Mom &o) {
  const double inv = 1.0 / static_cast<double>(max(s.n, 1));
  const double b1 = s.z1 * inv, b2 = s.z2 * inv, b3 = s.z3 * inv, b4 = s.z4 * inv;
  const double g = r.sg, g2 = g * g;
  o.mean = r.mu + g * b1;
  o.M2 = g2 * (s.z2 - s.z1 * b1); // Σ(x−x̄)²
  o.m2 = g2 * (b2 - b1 * b1);
  o.m3 = g2 * g * (b3 - 3.0 * b1 * b2 + 2.0 * b1 * b1 * b1);
  o.m4 = g2 * g2 * (b4 - 4.0 * b1 * b3 + 6.0 * b1 * b1 * b2 - 3.0 * b1 * b1 * b1 * b1);
}
constexpr int kNoBase = -(1 << 29); // 窗内下标重基用的哨兵 (还没有元素进过窗)

struct Mom2 {
  double mx, my, cxx, cyy, cxy;
};
__device__ inline void mom2_of(const StZ2 &s, const Ref2 &r, Mom2 &o) {
  const double inv = 1.0 / static_cast<double>(max(s.n, 1));
  const double bx = s.zx * inv, by = s.zy * inv;
  o.mx = r.mx + r.sx * bx;
  o.my = r.my + r.sy * by;
  o.cxx = static_cast<double>(r.sx) * r.sx * (s.zxx - s.zx * bx);
  o.cyy = static_cast<double>(r.sy) * r.sy * (s.zyy - s.zy * by);
  o.cxy = static_cast<double>(r.sx) * r.sy * (s.zxy - s.zx * by);
}

// =============================================================================
// 3. SCAN 骨架: EXPAND (段内 expanding) / ROLL (滑窗 add-sub)
//    Op 接口: kArity / kLagY / kSpread / Ref / St / pre / init / upd(w=±1) / emit(…, sx, sy, …)
//    kSpread = true 的算子由核维护 Chg + f, emit 拿到 sx/sy = x/y 在窗内非全并列 (精确)
// =============================================================================

// 取样点 (s 为块内坐标, 基址 base 为块内 0 对应的全局行) 的有效位: 一元看 x; 二元看 x 且 y (含 y 的 lag)
template <class Op>
__device__ __forceinline__ bool valid_at(const uint8_t *xm, const uint8_t *ym, int base, int s, int lag, int A, int a) {
  bool g = xm[(base + s) * A + a] != 0;
  if constexpr (Op::kArity >= 2) {
    const bool ok = Op::kLagY ? (s >= lag) : true;
    g = g && ok && ym[(base + s - (Op::kLagY ? lag : 0)) * A + a] != 0;
  }
  return g;
}

template <class Op>
__global__ void expand(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, float *ov, uint8_t *om, int T, int A, Param p) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  const int s0 = blockIdx.y * kSegLen; // 段起点 (块起点对齐段起点)
  const int len = min(kSegLen, T - s0);
  const int lag = Op::kLagY ? static_cast<int>(p.k) : 0; // TsCorrLagCum: K = (int)p.k
  assert(lag >= 0);
  typename Op::Ref r;
  Op::pre(r, xv, xm, yv, ym, a, A, s0, s0 + len, p);
  typename Op::St st;
  Op::init(st, p);
  Chg ch;
  ch.init();
  int f = 0;                      // 段内首个有效位置 (只前进)
  for (int s = 0; s < len; ++s) { // 段内 240 步寄存器串行, 段间完全独立
    const int i = (s0 + s) * A + a;
    DVal x{xv[i], xm[i]}, y{0.f, 0};
    if constexpr (Op::kArity >= 2) {
      const int j = Op::kLagY ? (s0 + s - lag) * A + a : i; // y 延迟 k 期, 段内取
      const bool ok = Op::kLagY ? (s >= lag) : true;        // s−k < 0 → 该样本对不成立
      y = DVal{ok ? yv[j] : 0.f, static_cast<uint8_t>(ok && ym[j] ? 1 : 0)};
    }
    Op::upd(st, x, y, s, 1, r, p);
    bool sx = false, sy = false;
    if constexpr (Op::kSpread) {
      ch.push(Op::kArity >= 2 ? (x.m && y.m) : (x.m != 0), x.v, y.v, s);
      while (f <= s && !valid_at<Op>(xm, ym, s0, f, lag, A, a))
        ++f;
      sx = ch.lastx > f;
      sy = ch.lasty > f;
    }
    float v = 0.f;
    bool m = false;
    Op::emit(st, x, y, s, true, sx, sy, r, p, v, m);
    dev::store(ov, om, i, v, m);
  }
}

template <class Op>
__global__ void roll(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, float *ov, uint8_t *om, int T, int A, int d, int C, Param p) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  const int c0 = blockIdx.y * C; // 本块负责的输出行区间 [c0, cend), C = 240·⌈d/240⌉
  if (c0 >= T)
    return;
  const int cend = min(c0 + C, T);
  const int w0 = max(0, c0 - d + 1); // 本块所有窗口的并集左端 (ROLL 跨段不 reset)
  typename Op::Ref r;
  Op::pre(r, xv, xm, yv, ym, a, A, w0, cend, p);
  typename Op::St st;
  Op::init(st, p);
  Chg ch;
  ch.init();
  int f = w0 - c0;                // 窗内首个有效位置 (块内坐标, 预热区起点; 只前进)
  for (int s = w0; s < c0; ++s) { // 预热: 把 [c0−d+1, c0−1] 装进窗口
    const int i = s * A + a;
    DVal x{xv[i], xm[i]}, y{0.f, 0};
    if constexpr (Op::kArity >= 2)
      y = DVal{yv[i], ym[i]};
    Op::upd(st, x, y, s - c0, 1, r, p);
    if constexpr (Op::kSpread)
      ch.push(Op::kArity >= 2 ? (x.m && y.m) : (x.m != 0), x.v, y.v, s - c0);
  }
  for (int t = c0; t < cend; ++t) {
    const int i = t * A + a, li = t - c0;
    DVal x{xv[i], xm[i]}, y{0.f, 0};
    if constexpr (Op::kArity >= 2)
      y = DVal{yv[i], ym[i]};
    Op::upd(st, x, y, li, 1, r, p); // 进窗
    const int o = t - d;
    // 出窗判据必须是 o ≥ w0 而不是 o ≥ 0: 累加器里只装过 [w0, t] 的行, 块首几步的 t−d < w0,
    // 那些行从没进过窗, 减了就等于凭空扣掉一份 (d=1 时连 n 都会被减成 0 → 整块掩码假)
    if (o >= w0) { // 出窗 (地址仍是合并访存)
      const int io = o * A + a;
      DVal xo{xv[io], xm[io]}, yo{0.f, 0};
      if constexpr (Op::kArity >= 2)
        yo = DVal{yv[io], ym[io]};
      Op::upd(st, xo, yo, o - c0, -1, r, p);
    }
    bool sx = false, sy = false;
    if constexpr (Op::kSpread) {
      ch.push(Op::kArity >= 2 ? (x.m && y.m) : (x.m != 0), x.v, y.v, li);
      f = max(f, li - d + 1); // 窗左端之前的行不必读
      while (f <= li && !valid_at<Op>(xm, ym, c0, f, 0, A, a))
        ++f;
      sx = ch.lastx > f;
      sy = ch.lasty > f;
    }
    float v = 0.f;
    bool m = false;
    Op::emit(st, x, y, li, t >= d - 1, sx, sy, r, p, v, m); // 窗未满 (t < d−1) 一律无效
    dev::store(ov, om, i, v, m);
  }
}

// ---- 无参考量的 pre (单遍算子) ----
#define FACTOR_TS_PRE_NONE \
  __device__ static void pre(RefNone &, const float *, const uint8_t *, const float *, const uint8_t *, int, int, int, int, const Param &) {}
// ---- 一元两遍 pre ----
#define FACTOR_TS_PRE_X                                                                                                                                    \
  __device__ static void pre(Ref1 &r, const float *xv, const uint8_t *xm, const float *, const uint8_t *, int a, int A, int from, int to, const Param &) { \
    ref_scan(xv, xm, a, A, from, to, r.mu, r.sg);                                                                                                          \
  }
// ---- 二元两遍 pre ----
#define FACTOR_TS_PRE_XY                                                                                                                                       \
  __device__ static void pre(Ref2 &r, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, int a, int A, int from, int to, const Param &) { \
    ref_scan(xv, xm, a, A, from, to, r.mx, r.sx);                                                                                                              \
    ref_scan(yv, ym, a, A, from, to, r.my, r.sy);                                                                                                              \
  }

// =============================================================================
// 4. SCAN functor 集 (Ts<核>Cum 与 Ts<核>Roll 共用一个 functor: 只差"窗满"条件与所在窗形)
// =============================================================================

// ---- Σx / 均值 (单遍, 段/块内 ≤ C 项, 无需标准化) ----
struct FnSum {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  using St = StRaw;
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = StRaw{0, 0.0, 0.0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal, int, int w, const Ref &, const Param &) {
    const int g = x.m ? w : 0;
    const double fg = static_cast<double>(g);
    s.n += g;
    s.s1 += fg * x.v;
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = static_cast<float>(s.s1);
    m = full && s.n >= 1;
  }
};
struct FnMean : FnSum {
  __device__ static void emit(const StRaw &s, DVal, DVal, int, bool full, bool, bool, const RefNone &, const Param &, float &v, bool &m) {
    v = static_cast<float>(s.s1 / static_cast<double>(max(s.n, 1))); // Σx / n
    m = full && s.n >= 1;
  }
};

// ---- 方差族 (两遍: z = (x−μ)/σ 上累幂和; ddof=1; 全并列由核给的 sx 判) ----
struct FnVar {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = true;
  using Ref = Ref1;
  using St = StZ;
  FACTOR_TS_PRE_X
  __device__ static void init(St &s, const Param &) { s = StZ{0, 0.0, 0.0, 0.0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal, int, int w, const Ref &r, const Param &) { upd_z(s, x, w, r); }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool sx, bool, const Ref &r, const Param &, float &v, bool &m) {
    Mom q;
    mom_of(s, r, q);
    v = static_cast<float>(q.M2 / static_cast<double>(max(s.n - 1, 1))); // M2/(n−1)
    m = full && s.n >= 2 && sx;
  }
};
struct FnStd : FnVar {
  __device__ static void emit(const StZ &s, DVal, DVal, int, bool full, bool sx, bool, const Ref1 &r, const Param &, float &v, bool &m) {
    Mom q;
    mom_of(s, r, q);
    v = static_cast<float>(sqrt(fmax(q.M2, 0.0) / static_cast<double>(max(s.n - 1, 1))));
    m = full && s.n >= 2 && sx;
  }
};
struct FnSkew : FnVar {
  __device__ static void emit(const StZ &s, DVal, DVal, int, bool full, bool sx, bool, const Ref1 &r, const Param &, float &v, bool &m) {
    Mom q;
    mom_of(s, r, q);
    // m3/m2^1.5: 非全并列 ⇒ m2 > 0; |m3|/m2^1.5 ≤ √n 有柯西–施瓦茨上界, 恒为有限值
    const double m2 = fmax(q.m2, 0.0);
    v = static_cast<float>(q.m3 / pow(m2, 1.5));
    m = full && s.n >= 3 && sx;
  }
};
struct FnKurt : FnVar {
  __device__ static void emit(const StZ &s, DVal, DVal, int, bool full, bool sx, bool, const Ref1 &r, const Param &, float &v, bool &m) {
    Mom q;
    mom_of(s, r, q);
    // m4/m2² − 3: m4/m2² ≤ n 有界
    const double m2 = fmax(q.m2, 0.0);
    v = static_cast<float>(q.m4 / (m2 * m2) - 3.0);
    m = full && s.n >= 4 && sx;
  }
};
// 相对型: 描述 x_t 自身, x_t 无效则无效
struct FnZ : FnVar {
  __device__ static void emit(const StZ &s, DVal x, DVal, int, bool full, bool sx, bool, const Ref1 &r, const Param &, float &v, bool &m) {
    Mom q;
    mom_of(s, r, q);
    const double sd = sqrt(fmax(q.M2, 0.0) / static_cast<double>(max(s.n - 1, 1)));
    v = static_cast<float>((static_cast<double>(x.v) - q.mean) / sd);
    m = full && s.n >= 2 && sx && x.m;
  }
};

// ---- 段内 cummax / cummin (max 是结合算子, 直接 scan; 只在 EXPAND 用, w 恒 +1) ----
template <bool MAXOP>
struct FnExtCum {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  struct St {
    int n;
    float best;
  };
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0, MAXOP ? -FLT_MAX : FLT_MAX}; }
  __device__ static void upd(St &s, DVal x, DVal, int, int w, const Ref &, const Param &) {
    const bool g = x.m && w > 0;
    const bool hit = g && (MAXOP ? (x.v > s.best) : (x.v < s.best));
    s.best = hit ? x.v : s.best; // select, 无分支
    s.n += g ? 1 : 0;
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = s.best;
    m = full && s.n >= 1; // n = 0 时 best = ±FLT_MAX (有限), 但掩码假 → 出口写 0
  }
};
// 首个 (最早) 极值**距今的期数** li − pos: 严格比较 → 并列保留较早位置 (与 Roll 版同口径)
template <bool MAXOP>
struct FnArgCum : FnExtCum<MAXOP> {
  struct St {
    int n;
    float best;
    int pos;
  };
  __device__ static void init(St &s, const Param &) { s = St{0, MAXOP ? -FLT_MAX : FLT_MAX, 0}; }
  __device__ static void upd(St &s, DVal x, DVal, int li, int w, const RefNone &, const Param &) {
    const bool g = x.m && w > 0;
    const bool hit = g && (MAXOP ? (x.v > s.best) : (x.v < s.best));
    s.best = hit ? x.v : s.best;
    s.pos = hit ? li : s.pos;
    s.n += g ? 1 : 0;
  }
  __device__ static void emit(const St &s, DVal, DVal, int li, bool full, bool, bool, const RefNone &, const Param &, float &v, bool &m) {
    v = static_cast<float>(li - s.pos);
    m = full && s.n >= 1;
  }
};

// ---- TsHhiCum: Σx²/(Σx)², Σx 相消退化 ----
struct FnHhi {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  using St = StRaw;
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = StRaw{0, 0.0, 0.0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal, int, int w, const Ref &, const Param &) {
    const int g = x.m ? w : 0;
    const double fg = static_cast<double>(g);
    s.n += g;
    s.s1 += fg * x.v;
    s.s2 += fg * static_cast<double>(x.v) * x.v;
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = static_cast<float>(s.s2 / (s.s1 * s.s1));
    m = full && s.n >= 1 && dev::den_ok(s.s1 * s.s1, static_cast<double>(s.n) * s.s2);
  }
};

// ---- TsEntropyCum: ln S − Σ(x·ln x)/S, 只计 x > 0; 有正样本 ⇒ S > 0 ----
struct FnEntropy {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  struct St {
    int np;
    double S, sxlx;
  };
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0, 0.0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal, int, int w, const Ref &, const Param &) {
    const bool g = x.m && x.v > 0.f;
    const double fg = g ? static_cast<double>(w) : 0.0;
    s.np += g ? w : 0;
    s.S += fg * x.v;
    s.sxlx += fg * x.v * log(static_cast<double>(g ? x.v : 1.f)); // x ≤ 0 的分支权 0, 参数换 1 防 log(≤0) 污染
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = static_cast<float>(log(s.S) - s.sxlx / s.S);
    m = full && s.np >= 1;
  }
};

// ---- CountGt: 精确计数 (整数, 不走桶) ----
struct FnCountGt {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  struct St {
    int n, c;
  };
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0, 0}; }
  __device__ static void upd(St &s, DVal x, DVal, int, int w, const Ref &, const Param &p) {
    const int g = x.m ? w : 0;
    s.n += g;
    s.c += (x.m && x.v > p.k) ? w : 0;
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = static_cast<float>(s.c);
    m = full && s.n >= 1;
  }
};

// ---- TsPeaksCum: 峰在 s 成立需 s−1,s,s+1 同段且都有效, 且 x_s > k·mean_{≤s}; 峰在 s+1 时刻确认 ----
//   Σx 用 double: x_s > k·mean 是硬阈值比较, fp32 累加的舍入会让峰计数与 host 差 1
struct FnPeaks {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  struct St {
    int n, cnt;
    double s1;      // 有效数 / 已确认峰数 / Σx
    float v1, v2;   // x[s−1], x[s−2]
    uint8_t m1, m2; // 对应有效位
    double meanp;   // mean_{≤s−1} (含 s−1 的段内 expanding 均值)
  };
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0, 0, 0.0, 0.f, 0.f, 0, 0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal, int li, int w, const Ref &, const Param &p) {
    if (w < 0)
      return; // 只在 EXPAND 用
    // 先判 j = li−1 处的峰 (此刻 x[j+1] = x 已知, 用的是含 j 的 expanding 均值)
    const bool inseg = li >= 2;
    const bool pk = inseg && s.m2 && s.m1 && x.m && s.v2 < s.v1 && s.v1 > x.v && static_cast<double>(s.v1) > static_cast<double>(p.k) * s.meanp;
    s.cnt += pk ? 1 : 0;
    // 再把 x 并入均值与历史
    const int g = x.m ? 1 : 0;
    s.n += g;
    s.s1 += g ? static_cast<double>(x.v) : 0.0;
    s.meanp = s.s1 / static_cast<double>(max(s.n, 1));
    s.v2 = s.v1;
    s.m2 = s.m1;
    s.v1 = x.v;
    s.m1 = x.m;
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = static_cast<float>(s.cnt);
    m = full && s.n >= 1;
  }
};

// ---- 协方差族 (两遍; Corr/Beta/Resid 的全并列由核给的 sx/sy 判) ----
struct FnCov {
  static constexpr int kArity = 2;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = Ref2;
  using St = StZ2;
  FACTOR_TS_PRE_XY
  __device__ static void init(St &s, const Param &) { s = StZ2{0, 0.0, 0.0, 0.0, 0.0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal y, int, int w, const Ref &r, const Param &) { upd_z2(s, x, y, w, r); }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &r, const Param &, float &v, bool &m) {
    Mom2 q;
    mom2_of(s, r, q);
    v = static_cast<float>(q.cxy / static_cast<double>(max(s.n - 1, 1)));
    m = full && s.n >= 2;
  }
};
struct FnCorr : FnCov {
  static constexpr bool kSpread = true;
  __device__ static void emit(const StZ2 &s, DVal, DVal, int, bool full, bool sx, bool sy, const Ref2 &r, const Param &, float &v, bool &m) {
    Mom2 q;
    mom2_of(s, r, q);
    // 按规格不 clamp |ρ| ≤ 1: 非全并列 ⇒ cxx、cyy > 0, 且 |cxy| ≤ √(cxx·cyy) → 比值有界. 乘积在 double 下算, 避免下溢成 0
    v = static_cast<float>(q.cxy / sqrt(fmax(q.cxx * q.cyy, 0.0)));
    m = full && s.n >= 2 && sx && sy;
  }
};
struct FnCorrLag : FnCorr { // 样本对 (x_s, y_{s−k}), 取样由 expand 核负责
  static constexpr bool kLagY = true;
};
struct FnBeta : FnCov {
  static constexpr bool kSpread = true;
  __device__ static void emit(const StZ2 &s, DVal, DVal, int, bool full, bool, bool sy, const Ref2 &r, const Param &, float &v, bool &m) {
    Mom2 q;
    mom2_of(s, r, q);
    v = static_cast<float>(q.cxy / q.cyy);
    m = full && s.n >= 2 && sy;
  }
};
struct FnResid : FnCov { // 相对型
  static constexpr bool kSpread = true;
  __device__ static void emit(const StZ2 &s, DVal x, DVal y, int, bool full, bool, bool sy, const Ref2 &r, const Param &, float &v, bool &m) {
    Mom2 q;
    mom2_of(s, r, q);
    v = static_cast<float>((static_cast<double>(x.v) - q.mx) - q.cxy / q.cyy * (static_cast<double>(y.v) - q.my));
    m = full && s.n >= 2 && sy && x.m && y.m;
  }
};
// ---- 加权均值: Σ(y·x)/Σy, Σy 相消退化 ----
struct FnWMean {
  static constexpr int kArity = 2;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  struct St {
    int n;
    double swx, sw, saw;
  };
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0, 0.0, 0.0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal y, int, int w, const Ref &, const Param &) {
    const int g = (x.m && y.m) ? w : 0;
    const double fg = static_cast<double>(g);
    s.n += g;
    s.swx += fg * static_cast<double>(y.v) * x.v;
    s.sw += fg * y.v;
    s.saw += fg * fabsf(y.v);
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = static_cast<float>(s.swx / s.sw);
    m = full && s.n >= 1 && dev::den_ok(s.sw, s.saw);
  }
};

// ---- TsWmaRoll: w = i+1 (窗内下标, 最旧 = 0) → Σ(w·x) = Σ(s·x) − (t−d)·Σx, Σw = Σs − (t−d)·n ----
//      s 用**块内**行号 (|s| ≤ C+d): 用全局行号会让 Σ(s·x) ~ 4.6e6 与 (t−d)Σx 抵消掉 2 位有效数字
struct FnWma {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  struct St {
    int n, lb;
    double sx, ss, ssx;
  }; // n, 上次进窗的块内坐标, Σx, Σi, Σ(i·x); i 是**窗内**下标 (随窗滑动重基)
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0, kNoBase, 0.f, 0.f, 0.f}; }
  // 下标绝不能用块内坐标 li (最大 239) 再在 emit 里减掉窗左端: Σ(li·x) 与 o·Σx 同阶相消,
  // d=1 时就是 239x−238x, fp32 有效位直接掉 2~3 位. 改为每次进窗把已有成员的下标整体下移,
  // 使"刚进窗的元素" 恒为 i = d−1, "正在出窗的元素" 恒为 i = −1 → 所有下标幅度 O(d).
  __device__ static void upd(St &s, DVal x, DVal, int li, int w, const Ref &, const Param &p) {
    float fi = -1.f; // 出窗元素的窗内下标恒为 −1
    if (w > 0) {
      const int sh = (s.lb == kNoBase) ? 0 : (li - s.lb); // 恒为 1 (进窗按 li 递增)
      const float fs = static_cast<float>(sh);
      s.ss -= fs * static_cast<float>(s.n); // Σi → Σ(i−sh)
      s.ssx -= fs * s.sx;
      s.lb = li;
      fi = static_cast<float>(p.d - 1); // 进窗元素的窗内下标恒为 d−1
    }
    const int g = x.m ? w : 0;
    const double fg = static_cast<double>(g);
    s.n += g;
    s.sx += fg * x.v;
    s.ss += fg * fi;
    s.ssx += fg * fi * x.v;
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    const double wx = s.ssx + s.sx;                    // 权 w = i+1 → Σ(w·x) = Σ(i·x) + Σx
    const double sw = s.ss + static_cast<double>(s.n); // Σw (w ≥ 1 → n ≥ 1 时 Σw ≥ 1, 无需退化判据)
    v = static_cast<float>(wx / sw);
    m = full && s.n >= 1;
  }
};

// ---- TsSlopeRoll: x 对窗内下标 i = 0..d−1 的 OLS 斜率 (下标矩同样由 Σs/Σs²/Σ(s·x) 线性组合出来) ----
//      下标两两不同 ⇒ n ≥ 2 时 Σ(i−ī)² ≥ 1/2, 无需退化判据
struct FnSlope {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  struct St {
    int n, lb;
    double sx, ss, sss, ssx;
  }; // Σi / Σi² / Σ(i·x) 同样用随窗滑动的窗内下标 i, 避免块内坐标的同阶相消
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0, kNoBase, 0.f, 0.f, 0.f, 0.f}; }
  __device__ static void upd(St &s, DVal x, DVal, int li, int w, const Ref &, const Param &p) {
    float fi = -1.f;
    if (w > 0) {
      const int sh = (s.lb == kNoBase) ? 0 : (li - s.lb);
      const float fs = static_cast<float>(sh), fn = static_cast<float>(s.n);
      s.sss += fs * fs * fn - 2.f * fs * s.ss; // Σ(i−sh)² = Σi² − 2sh·Σi + sh²n (先用旧 Σi)
      s.ss -= fs * fn;
      s.ssx -= fs * s.sx;
      s.lb = li;
      fi = static_cast<float>(p.d - 1);
    }
    const int g = x.m ? w : 0;
    const double fg = static_cast<double>(g);
    s.n += g;
    s.sx += fg * x.v;
    s.ss += fg * fi;
    s.sss += fg * fi * fi;
    s.ssx += fg * fi * x.v;
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    const double fn = static_cast<double>(max(s.n, 1));
    const double sxx = s.sss - s.ss * s.ss / fn; // Σ(i−ī)²
    v = static_cast<float>((s.ssx - s.ss * s.sx / fn) / sxx);
    m = full && s.n >= 2;
  }
};

// ---- TsProductRoll: Π(1+x) − 1 = expm1(Σ log1p x); "任一 x ≤ −1" 用可加减的违例计数表达 ----
struct FnProduct {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  struct St {
    int n, bad;
    double sl;
  };
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0, 0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal, int, int w, const Ref &, const Param &) {
    const int g = x.m ? w : 0;
    const double fg = static_cast<double>(g);
    const bool ok = x.v > -1.f;
    s.n += g;
    s.bad += (x.m && !ok) ? w : 0;
    s.sl += fg * log1p(static_cast<double>(ok ? x.v : 0.f)); // 违例点参数换 0 (log1p 0 = 0), 不让 −inf 进滑窗累加器
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool full, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = static_cast<float>(expm1(s.sl));
    m = full && s.n >= 1 && s.bad == 0;
  }
};

// ---- TsAgeRoll: 距上次变动 (相邻有效样本精确不等) 的期数 ----
//   "最近一次变动位置" = 前向扫描里最后一次变动的位置 (它若落在窗外, 窗内必然无变动) → 只前进, 无需减法
struct FnAge {
  static constexpr int kArity = 1;
  static constexpr bool kLagY = false, kSpread = false;
  using Ref = RefNone;
  struct St {
    float pv;
    uint8_t pm;
    int last; // 上一格的值/有效位, 最近变动的块内位置
  };
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0.f, 0, -(1 << 29)}; } // last 取远负: 从未变动
  __device__ static void upd(St &s, DVal x, DVal, int li, int w, const Ref &, const Param &) {
    if (w < 0)
      return; // 出窗不改状态
    const bool chg = s.pm && x.m && x.v != s.pv;
    s.last = chg ? li : s.last;
    s.pv = x.v;
    s.pm = x.m;
  }
  __device__ static void emit(const St &s, DVal x, DVal, int li, bool full, bool, bool, const Ref &, const Param &p, float &v, bool &m) {
    const bool inw = s.last >= li - p.d + 2; // 变动点 j 需 j−1 也在窗内 → j ≥ t−d+2
    v = inw ? static_cast<float>(li - s.last) : static_cast<float>(p.d);
    m = full && x.m;
  }
};

// =============================================================================
// 5. EXTREME: 滑窗极值 van Herk / Gil-Werman
//    以 d 为块长切 t 轴 (块界对齐全局 0): 窗 [t−d+1, t] 恰好跨相邻两块 (或 t 为块末时 = 整块),
//    ext[t−d+1, t] = cmb(S[t−d+1], P[t]), P = 所在块内前缀极值, S = 所在块内后缀极值.
//    一线程一资产: 每块先前向算 P 并输出 (用前一块的 S), 再后向算 S 写进环形区 [d][A] 供下一块;
//    读 x 两遍 + 环形区读写各一遍, 与 d 无关. 时间上按 C (d 的倍数) 分块, 块首多做一次前块的后缀预热.
//    工作区 = gridY · d · A · sizeof(E) ≤ T·A·sizeof(E) 的 1/4 量级 (C ≈ 4·240), 不再是 log₂d 趟全平面.
// =============================================================================
struct FI {
  float v;
  int i;
}; // (值, 全局行号) pair

template <bool MAXOP>
__device__ __forceinline__ float cmb(float a, float b) { return MAXOP ? fmaxf(a, b) : fminf(a, b); }
template <bool MAXOP>
__device__ __forceinline__ FI cmb(FI a, FI b) {
  const bool tie = a.v == b.v;
  const bool win = MAXOP ? (a.v > b.v) : (a.v < b.v);
  return (win || (tie && a.i <= b.i)) ? a : b; // 并列保留较旧 (行号小) 的位置
}
// 单位元 / 取样 (无效点取单位元: 有限值, 不引入 inf)
template <bool MAXOP, class E>
struct Ext;
template <bool MAXOP>
struct Ext<MAXOP, float> {
  __device__ static float ident() { return MAXOP ? -FLT_MAX : FLT_MAX; }
  __device__ static float at(const float *xv, const uint8_t *xm, int i, int) { return xm[i] ? xv[i] : ident(); }
  __device__ static float out(float e, int) { return e; } // 极值
};
template <bool MAXOP>
struct Ext<MAXOP, FI> {
  __device__ static FI ident() { return FI{MAXOP ? -FLT_MAX : FLT_MAX, 0}; }
  __device__ static FI at(const float *xv, const uint8_t *xm, int i, int t) { return FI{xm[i] ? xv[i] : (MAXOP ? -FLT_MAX : FLT_MAX), t}; }
  __device__ static float out(FI e, int t) { return static_cast<float>(t - e.i); } // (d−1)−i = t−pos
};

template <bool MAXOP, class E>
__global__ void extreme(const float *xv, const uint8_t *xm, E *ws, float *ov, uint8_t *om, int T, int A, int d, int C) {
  using X = Ext<MAXOP, E>;
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  const int c0 = blockIdx.y * C; // C 是 d 的倍数 → 时间块界对齐 d-块界
  if (c0 >= T)
    return;
  const int cend = min(c0 + C, T);
  E *S = ws + static_cast<size_t>(blockIdx.y) * d * A; // 本线程块的环形后缀区 [d][A], 下标 (s % d)·A + a
  // 预热 1: 前一 d-块 [c0−d, c0) 的后缀极值 (c0 = 0 时没有前块; 那时首块内的窗也未满或 = 整块)
  if (c0 >= d) {
    E run = X::ident();
    for (int s = c0 - 1; s >= c0 - d; --s) {
      run = cmb<MAXOP>(X::at(xv, xm, s * A + a, s), run);
      S[(s % d) * A + a] = run;
    }
  }
  // 预热 2: 滑窗有效计数 (整数, add/sub 精确), 同 roll(): 只减预热时真加过的行 (t−d ≥ w0)
  const int w0 = max(0, c0 - d + 1);
  int n = 0;
  for (int s = w0; s < c0; ++s)
    n += xm[s * A + a] ? 1 : 0;
  for (int b0 = c0; b0 < cend; b0 += d) {
    const int bend = min(b0 + d, T);
    E P = X::ident();
    for (int t = b0; t < bend; ++t) { // 前向: 块内前缀极值 + 输出
      const int i = t * A + a;
      n += xm[i] ? 1 : 0;
      if (t - d >= w0)
        n -= xm[(t - d) * A + a] ? 1 : 0;
      P = cmb<MAXOP>(P, X::at(xv, xm, i, t));
      const bool full = t >= d - 1;
      const int l = t - d + 1;                                   // 窗左端
      const bool cross = full && l < b0;                         // 窗跨前块 (l 在前块) ; 否则 t 为块末, 窗 = 整块 = P
      const E e = cross ? cmb<MAXOP>(S[(l % d) * A + a], P) : P; // S 更旧, 放前面 → 并列保留最旧
      dev::store(ov, om, i, X::out(e, t), full && n >= 1);
    }
    E run = X::ident();
    for (int t = bend - 1; t >= b0; --t) { // 后向: 块内后缀极值 → 环形区 (前向已用完前块的 S, 可覆盖)
      run = cmb<MAXOP>(X::at(xv, xm, t * A + a, t), run);
      S[(t % d) * A + a] = run;
    }
  }
}

// =============================================================================
// 6. HIST: 序统计族 (窗内 lo/hi 随窗漂移 → 桶边界不固定, 只能窗内多趟; 见回执"规格冲突")
//    每线程 16 个寄存器计数器, 粗 16 组 × 细 16 桶 = kBuckets 256, 桶号与单趟直方图逐位一致
// =============================================================================
struct Src { // 样本取值器: mode 0 = x, mode 1 = |x − shift| (TsMadRoll 第二轮)
  const float *v;
  const uint8_t *mk;
  int a, A, mode;
  float shift;
  __device__ __forceinline__ bool get(int s, float &u) const {
    const int i = s * A + a;
    u = v[i];
    u = (mode == 1) ? fabsf(u - shift) : u;
    return mk[i] != 0;
  }
};

// 一趟: cnt / lo / hi / Σx / Σ|x| (pos_only = 只取 x > 0, 给 Gini)
__device__ inline void span_stat(const Src &src, int s0, int t, bool pos_only, int &cnt, float &lo, float &hi, float &sum, float &asum) {
  int c = 0;
  float l = FLT_MAX, h = -FLT_MAX, sm = 0.f, sa = 0.f;
  for (int s = s0; s <= t; ++s) {
    float u;
    const bool g = src.get(s, u) && (!pos_only || u > 0.f);
    c += g ? 1 : 0;
    l = (g && u < l) ? u : l;
    h = (g && u > h) ? u : h;
    sm += g ? u : 0.f;
    sa += g ? fabsf(u) : 0.f;
  }
  cnt = c;
  lo = l;
  hi = h;
  sum = sm;
  asum = sa;
}

// 升序累计到 target 的最小桶 (两趟: 粗 16 组 → 细 16 桶). target ≥ 1
__device__ inline int hier_lower(const Src &src, int s0, int t, float lo, float hi, bool pos_only, int target) {
  int c[16] = {0};
  for (int s = s0; s <= t; ++s) {
    float u;
    const bool g = src.get(s, u) && (!pos_only || u > 0.f);
    const int gb = g ? (dev::bin_of_fast(u, lo, hi) >> 4) : -1;
#pragma unroll
    for (int j = 0; j < 16; ++j)
      c[j] += (gb == j) ? 1 : 0; // 谓词加, 全留寄存器 (动态下标会掉进 local memory)
  }
  int acc = 0, g0 = 15;
#pragma unroll
  for (int j = 0; j < 16; ++j) {
    const bool hit = (acc < target) && (acc + c[j] >= target);
    g0 = hit ? j : g0;
    acc += c[j];
  }
  int base = 0;
#pragma unroll
  for (int j = 0; j < 16; ++j)
    base += (j < g0) ? c[j] : 0;
  int f[16] = {0};
  for (int s = s0; s <= t; ++s) {
    float u;
    const bool g = src.get(s, u) && (!pos_only || u > 0.f);
    const int b = g ? dev::bin_of_fast(u, lo, hi) : -1;
    const int fb = (b >> 4) == g0 ? (b & 15) : -1;
#pragma unroll
    for (int j = 0; j < 16; ++j)
      f[j] += (fb == j) ? 1 : 0;
  }
  int acc2 = base, b0 = 15;
#pragma unroll
  for (int j = 0; j < 16; ++j) {
    const bool hit = (acc2 < target) && (acc2 + f[j] >= target);
    b0 = hit ? j : b0;
    acc2 += f[j];
  }
  return (g0 << 4) | b0;
}

// 降序 (从最高桶往低) 累计首次 ≥ K 的桶 b*
__device__ inline int hier_upper(const Src &src, int s0, int t, float lo, float hi, int K) {
  int c[16] = {0};
  for (int s = s0; s <= t; ++s) {
    float u;
    const bool g = src.get(s, u);
    const int gb = g ? (dev::bin_of_fast(u, lo, hi) >> 4) : -1;
#pragma unroll
    for (int j = 0; j < 16; ++j)
      c[j] += (gb == j) ? 1 : 0;
  }
  int acc = 0, g0 = 0;
#pragma unroll
  for (int j = 15; j >= 0; --j) {
    const bool hit = (acc < K) && (acc + c[j] >= K);
    g0 = hit ? j : g0;
    acc += c[j];
  }
  int above = 0;
#pragma unroll
  for (int j = 0; j < 16; ++j)
    above += (j > g0) ? c[j] : 0;
  int f[16] = {0};
  for (int s = s0; s <= t; ++s) {
    float u;
    const bool g = src.get(s, u);
    const int b = g ? dev::bin_of_fast(u, lo, hi) : -1;
    const int fb = (b >> 4) == g0 ? (b & 15) : -1;
#pragma unroll
    for (int j = 0; j < 16; ++j)
      f[j] += (fb == j) ? 1 : 0;
  }
  int acc2 = above, b0 = 0;
#pragma unroll
  for (int j = 15; j >= 0; --j) {
    const bool hit = (acc2 < K) && (acc2 + f[j] >= K);
    b0 = hit ? j : b0;
    acc2 += f[j];
  }
  return (g0 << 4) | b0;
}

// MODE 0 = EXPAND (样本 = 段内 s ≤ t_seg), 1 = ROLL (样本 = [t−d+1, t])
template <int MODE, class Op>
__global__ void hist(const float *xv, const uint8_t *xm, float *ov, uint8_t *om, int T, int A, int d, Param p) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  const int c0 = blockIdx.y * kSegLen; // 一个块吃一段的 t, 窗切片留在 L1 供多趟重扫
  const int cend = min(c0 + kSegLen, T);
  for (int t = c0; t < cend; ++t) {
    const int i = t * A + a;
    if (MODE == 1 && t < d - 1) { // ROLL 窗未满
      dev::store(ov, om, i, 0.f, false);
      continue;
    }
    const int s0 = (MODE == 0) ? c0 : (t - d + 1);
    Src src{xv, xm, a, A, 0, 0.f};
    float v = 0.f;
    bool m = false;
    Op::eval(src, s0, t, xv[i], xm[i] != 0, p, v, m);
    dev::store(ov, om, i, v, m);
  }
}

// pct rank: less/eq 只要两个计数器, 一趟搞定
struct HRank {
  __device__ static void eval(const Src &src, int s0, int t, float xt, bool xtm, const Param &, float &v, bool &m) {
    int cnt;
    float lo, hi, sm, sa;
    span_stat(src, s0, t, false, cnt, lo, hi, sm, sa);
    m = cnt >= 1 && xtm;
    if (!m)
      return;
    if (!dev::spread(lo, hi)) { // 值域退化 → 全并列
      v = 0.5f;
      return;
    }
    const int b = dev::bin_of(xt, lo, hi);
    int less = 0, eq = 0;
    for (int s = s0; s <= t; ++s) {
      float u;
      const bool g = src.get(s, u);
      const int bb = g ? dev::bin_of_fast(u, lo, hi) : -1;
      less += (g && bb < b) ? 1 : 0;
      eq += (g && bb == b) ? 1 : 0;
    }
    v = dev::pct_of(less, eq, cnt);
  }
};
// 中位数 = quantile(0.5): 前缀累计首次 ≥ ⌈0.5·cnt⌉ 的桶中心
struct HMedian {
  __device__ static void eval(const Src &src, int s0, int t, float, bool, const Param &, float &v, bool &m) {
    int cnt;
    float lo, hi, sm, sa;
    span_stat(src, s0, t, false, cnt, lo, hi, sm, sa);
    m = cnt >= 1;
    if (!m)
      return;
    if (!dev::spread(lo, hi)) {
      v = lo;
      return;
    }
    const int tg = max(1, static_cast<int>(ceilf(0.5f * static_cast<float>(cnt))));
    v = dev::bin_center(hier_lower(src, s0, t, lo, hi, false, tg), lo, hi);
  }
};
// MAD: 两轮直方图 (先窗内 median, 再对 |x − med| 重新定 lo/hi 求 median)
struct HMad {
  __device__ static void eval(const Src &src, int s0, int t, float, bool, const Param &, float &v, bool &m) {
    int cnt;
    float lo, hi, sm, sa;
    span_stat(src, s0, t, false, cnt, lo, hi, sm, sa);
    m = cnt >= 1;
    if (!m)
      return;
    float med = lo;
    if (dev::spread(lo, hi)) {
      const int tg = max(1, static_cast<int>(ceilf(0.5f * static_cast<float>(cnt))));
      med = dev::bin_center(hier_lower(src, s0, t, lo, hi, false, tg), lo, hi);
    }
    Src d2 = src;
    d2.mode = 1;
    d2.shift = med;
    int c2;
    float lo2, hi2, sm2, sa2;
    span_stat(d2, s0, t, false, c2, lo2, hi2, sm2, sa2);
    if (!dev::spread(lo2, hi2)) {
      v = lo2;
      return;
    }
    const int tg2 = max(1, static_cast<int>(ceilf(0.5f * static_cast<float>(c2))));
    v = dev::bin_center(hier_lower(d2, s0, t, lo2, hi2, false, tg2), lo2, hi2);
  }
};
// TopK: 从高桶累计到 K, 边界桶按缺口线性补
struct HTopK {
  __device__ static void eval(const Src &src, int s0, int t, float, bool, const Param &p, float &v, bool &m) {
    int cnt;
    float lo, hi, sm, sa;
    span_stat(src, s0, t, false, cnt, lo, hi, sm, sa);
    const int K = static_cast<int>(p.k);
    assert(K >= 1); // K ≤ 0 无定义
    m = cnt >= K && dev::den_ok(sm, sa);
    if (!m)
      return;
    if (!dev::spread(lo, hi)) {
      v = static_cast<float>(min(K, cnt)) / static_cast<float>(max(cnt, 1));
      return;
    }
    const int bs = hier_upper(src, s0, t, lo, hi, K);
    float top = 0.f;
    int above = 0;
    for (int s = s0; s <= t; ++s) { // Σ_{b>b*} center(b)·cnt_b 等价于逐样本累 center(bin)
      float u;
      const bool g = src.get(s, u);
      const int b = g ? dev::bin_of_fast(u, lo, hi) : -1;
      top += (g && b > bs) ? dev::bin_center(b, lo, hi) : 0.f;
      above += (g && b > bs) ? 1 : 0;
    }
    top += dev::bin_center(bs, lo, hi) * static_cast<float>(K - above);
    v = top / sm; // 掩码已保证 Σx 未相消
  }
};
// Gini: 按桶升序走 Lorenz 曲线. 16 个粗组逐组细化 (空粗组整组跳过, 其贡献恒为 0)
struct HGini {
  __device__ static void eval(const Src &src, int s0, int t, float, bool, const Param &, float &v, bool &m) {
    int cnt;
    float lo, hi, sm, sa;
    span_stat(src, s0, t, true, cnt, lo, hi, sm, sa); // 只计 x > 0
    m = cnt >= 2;
    if (!m)
      return;
    if (!dev::spread(lo, hi)) {
      v = 0.f;
      return;
    }
    int cg[16] = {0};
    float V = 0.f;
    for (int s = s0; s <= t; ++s) { // 粗组计数 + 总价值 V = Σ_b center(b)·cnt_b
      float u;
      const bool g = src.get(s, u) && u > 0.f;
      const int b = g ? dev::bin_of_fast(u, lo, hi) : -1;
      const int gb = b >> 4;
      V += g ? dev::bin_center(b, lo, hi) : 0.f;
#pragma unroll
      for (int j = 0; j < 16; ++j)
        cg[j] += (gb == j && g) ? 1 : 0;
    }
    const float N = static_cast<float>(cnt);
    const float Vg = V; // 正样本 ⇒ 桶心 > 0 ⇒ V > 0
    float pp = 0.f, LL = 0.f, acc = 0.f, cw = 0.f;
    int cc = 0;
    for (int g0 = 0; g0 < 16; ++g0) {
      if (cg[g0] == 0)
        continue; // 空桶 (p_i − p_{i−1}) = 0, 跳过与否逐位等价
      int f[16] = {0};
      for (int s = s0; s <= t; ++s) {
        float u;
        const bool g = src.get(s, u) && u > 0.f;
        const int b = g ? dev::bin_of_fast(u, lo, hi) : -1;
        const int fb = (b >> 4) == g0 ? (b & 15) : -1;
#pragma unroll
        for (int j = 0; j < 16; ++j)
          f[j] += (fb == j) ? 1 : 0;
      }
      for (int j = 0; j < 16; ++j) {
        if (f[j] == 0)
          continue;
        cc += f[j];
        cw += dev::bin_center((g0 << 4) | j, lo, hi) * static_cast<float>(f[j]);
        const float pi = static_cast<float>(cc) / N, Li = cw / Vg;
        acc += (pi - pp) * (Li + LL); // G = 1 − Σ (p_i − p_{i−1})(L_i + L_{i−1})
        pp = pi;
        LL = Li;
      }
    }
    v = 1.f - acc;
  }
};

// =============================================================================
// 7. RECUR: TsMeanEma 的仿射分块 scan
//    元素 (a,b): y ← a·y + b. 复合 (a,b)∘(c,d) = (a·c, b·c+d)
//      无效点            → (1, 0) 恒等 (y 与掩码保持)
//      本资产首个有效点  → (0, x)     **首值播种**: y ← x (不是 k·x)
//      其余有效点        → (1−k, k·x)
//    "是否首个" 是个 OR 前缀, 所以每块出**两份**复合: 假设块前已有有效点的 (a, b_seen),
//    和假设本块含首个有效点的 b_seed (该变体 a ≡ 0, 入口 y 被首元素抹掉, 不必存).
//    块间 scan 时按 OR 前缀二选一 → 仍是一趟结合扫描, 没有退化成串行.
//    分块: 块内串行复合 → 块间 scan → 回填. (1−k)^len 下溢到 0 是良性的.
// =============================================================================
__global__ void ema_local(const float *xv, const uint8_t *xm, float *ca, float *cb, float *cd, uint8_t *cs, int T, int A, float k) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  const int c = blockIdx.y;
  const int s0 = c * kSegLen, e = min(s0 + kSegLen, T);
  float pa = 1.f, pb = 0.f; // 变体 1: 块前已见过有效点
  float qb = 0.f;           // 变体 2: 本块的首个有效点做播种 (a 恒为 0)
  uint8_t sn = 0, sn2 = 0;
  for (int s = s0; s < e; ++s) {
    const int i = s * A + a;
    const bool g = xm[i] != 0;
    const float x = xv[i];
    const float ea = g ? (1.f - k) : 1.f, eb = g ? k * x : 0.f;
    pa = pa * ea;
    pb = pb * ea + eb;
    const bool seed = g && !sn2; // 本块内的首个有效点
    qb = seed ? x : (g ? (qb * (1.f - k) + k * x) : qb);
    sn2 = static_cast<uint8_t>(sn2 | (g ? 1 : 0));
    sn = static_cast<uint8_t>(sn | (g ? 1 : 0));
  }
  ca[c * A + a] = pa;
  cb[c * A + a] = pb;
  cd[c * A + a] = qb;
  cs[c * A + a] = sn;
}
// 块间 exclusive scan: y₀ = 0 是已知常数, 故只需沿块传一个标量 y 与 seen 位 (nc = 80, 一线程串行足够)
__global__ void ema_carry(float *ca, float *cb, const float *cd, uint8_t *cs, int A, int nc) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  float y = 0.f;
  uint8_t sn = 0;
  for (int c = 0; c < nc; ++c) {
    const int i = c * A + a;
    const float na = ca[i], nb = cb[i], nd = cd[i];
    const uint8_t ns = cs[i];
    ca[i] = y; // 本块入口的 y
    cs[i] = sn;
    y = sn ? (na * y + nb) : (ns ? nd : y); // 块前没见过有效点且本块有 → 走播种变体
    sn = static_cast<uint8_t>(sn | ns);
    (void)nb;
  }
}
__global__ void ema_apply(const float *xv, const uint8_t *xm, const float *ca, const uint8_t *cs, float *ov, uint8_t *om, int T, int A, float k) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  const int c = blockIdx.y;
  const int s0 = c * kSegLen, e = min(s0 + kSegLen, T);
  float y = ca[c * A + a]; // 块入口的 y
  bool seen = cs[c * A + a] != 0;
  for (int s = s0; s < e; ++s) {
    const int i = s * A + a;
    const bool g = xm[i] != 0;
    const float x = xv[i];
    y = g ? (seen ? (k * x + (1.f - k) * y) : x) : y; // 首个有效点播种, 无效点保持
    seen = seen || g;
    dev::store(ov, om, i, y, seen);
  }
}

// ---- 启动配置 ----
inline dim3 g_pt(int T, int A) { return dim3(static_cast<unsigned>((A + kPB - 1) / kPB), static_cast<unsigned>(hmin(T, 512))); }
inline dim3 g_seg(int T, int A) { return dim3(static_cast<unsigned>((A + kTB - 1) / kTB), static_cast<unsigned>((T + kSegLen - 1) / kSegLen)); }
inline dim3 g_hist(int T, int A) { return dim3(static_cast<unsigned>((A + kHB - 1) / kHB), static_cast<unsigned>((T + kSegLen - 1) / kSegLen)); }
inline int roll_chunk(int d) { return kSegLen * ((d + kSegLen - 1) / kSegLen); } // 块长 ≥ d: 预热开销 ≤ 一趟
inline int ext_chunk(int d) { return d * hmax(1, (4 * kSegLen) / d); }           // d 的倍数 (块界对齐 d-块界), ≈ 4 段长
inline dim3 g_roll(int T, int A, int C) { return dim3(static_cast<unsigned>((A + kTB - 1) / kTB), static_cast<unsigned>((T + C - 1) / C)); }

} // namespace k

// =============================================================================
// 算子: 统一签名 (未用到的指针传 nullptr, 全部设备指针); kStrat 对 OpTable 的 GPU 策略列
// =============================================================================

// ---- POINT (22) ----
#define FACTOR_TS_POINT(Name, NARY, BODY)                                                                                                                                             \
  struct Name {                                                                                                                                                                       \
    static constexpr Strat kStrat = Strat::POINT;                                                                                                                                     \
    struct F {                                                                                                                                                                        \
      __device__ static void apply(DVal x, DVal y, DVal z, int t_seg, const Param &p, float &v, bool &m) {                                                                            \
        (void)x, (void)y, (void)z, (void)t_seg, (void)p;                                                                                                                              \
        BODY                                                                                                                                                                          \
      }                                                                                                                                                                               \
    };                                                                                                                                                                                \
    static size_t workspace(int, int, const Param &) { return 0; }                                                                                                                    \
    static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, const Param &p, \
                    void *, cudaStream_t stream) {                                                                                                                                    \
      assert(static_cast<long long>(T) * A < (1LL << 31));                                                                                                                            \
      k::point<NARY, F><<<k::g_pt(T, A), kPB, 0, stream>>>(xv, xm, yv, ym, zv, zm, ov, om, T, A, p);                                                                                  \
      FACTOR_CUDA_OK(cudaGetLastError());                                                                                                                                             \
    }                                                                                                                                                                                 \
  };

FACTOR_TS_POINT(TsAbs, 1, { v = fabsf(x.v); m = x.m; })
FACTOR_TS_POINT(TsSign, 1, { v = x.v > 0.f ? 1.f : (x.v < 0.f ? -1.f : 0.f); m = x.m; }) // select, 无分支
FACTOR_TS_POINT(TsLog, 1, { v = copysignf(log1pf(fabsf(x.v)), x.v); m = x.m; })  // sign(x)·log1p(|x|)
FACTOR_TS_POINT(TsAsinh, 1, { v = asinhf(x.v); m = x.m; })
FACTOR_TS_POINT(TsTanh, 1, { v = tanhf(x.v); m = x.m; })
FACTOR_TS_POINT(TsSqrt, 1, { v = copysignf(sqrtf(fabsf(x.v)), x.v); m = x.m; })
FACTOR_TS_POINT(TsRelu, 1, { v = fmaxf(0.f, x.v); m = x.m; })
FACTOR_TS_POINT(TsRecip, 1, { v = 1.f / x.v; m = x.m && x.v != 0.f; })     // x = 0 退化; 溢出 → 出口转无效
FACTOR_TS_POINT(TsSignedPow, 1, { v = copysignf(powf(fabsf(x.v), p.k), x.v); m = x.m; }) // 溢出 → 出口判非有限转无效
FACTOR_TS_POINT(TsClip, 1, { assert(p.k >= 0.f); v = fminf(fmaxf(x.v, -p.k), p.k); m = x.m; })
FACTOR_TS_POINT(TsTodMask, 0, { v = (t_seg >= static_cast<int>(p.k) && t_seg < static_cast<int>(p.k2)) ? 1.f : 0.f; m = true; }) // 元数 0: 不读输入, 纯 t 坐标
FACTOR_TS_POINT(TsAdd, 2, { v = x.v + y.v; m = x.m && y.m; })
FACTOR_TS_POINT(TsSub, 2, { v = x.v - y.v; m = x.m && y.m; })
FACTOR_TS_POINT(TsMul, 2, { v = x.v * y.v; m = x.m && y.m; })
FACTOR_TS_POINT(TsDiv, 2, { v = x.v / y.v; m = x.m && y.m && y.v != 0.f; }) // y = 0 退化; 溢出 → 出口转无效
FACTOR_TS_POINT(TsMax, 2, { v = fmaxf(x.v, y.v); m = x.m && y.m; })
FACTOR_TS_POINT(TsMin, 2, { v = fminf(x.v, y.v); m = x.m && y.m; })
FACTOR_TS_POINT(TsImb, 2, { v = (x.v - y.v) / (x.v + y.v); m = x.m && y.m && dev::den_ok(x.v + y.v, fabsf(x.v) + fabsf(y.v)); })
FACTOR_TS_POINT(TsShare, 2, { v = x.v / (x.v + y.v); m = x.m && y.m && dev::den_ok(x.v + y.v, fabsf(x.v) + fabsf(y.v)); })
FACTOR_TS_POINT(TsLogRatio, 2, { v = logf(x.v) - logf(y.v); m = x.m && y.m && x.v > 0.f && y.v > 0.f; }) // 两个正有限数各取对数, 不溢出
FACTOR_TS_POINT(TsWhere, 3, { v = x.v > 0.f ? y.v : z.v; m = x.m && (x.v > 0.f ? y.m : z.m); })
FACTOR_TS_POINT(TsClip3, 3, { v = fminf(fmaxf(x.v, y.v), z.v); m = x.m && y.m && z.m && (y.v <= z.v); })

// ---- GATHER (2): d 只改地址 ----
#define FACTOR_TS_GATHER(Name, DELTA)                                                                                                                                                 \
  struct Name {                                                                                                                                                                       \
    static constexpr Strat kStrat = Strat::GATHER;                                                                                                                                    \
    static size_t workspace(int, int, const Param &) { return 0; }                                                                                                                    \
    static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p, void *, \
                    cudaStream_t stream) {                                                                                                                                            \
      assert(p.d >= 1 && p.d <= T);                                                                                                                                                   \
      k::gather<DELTA><<<k::g_pt(T, A), kPB, 0, stream>>>(xv, xm, ov, om, T, A, p.d);                                                                                                 \
      FACTOR_CUDA_OK(cudaGetLastError());                                                                                                                                             \
    }                                                                                                                                                                                 \
  };
FACTOR_TS_GATHER(TsDelayRoll, false) // x_{t−d}, m = (t≥d) && xm[t−d]
FACTOR_TS_GATHER(TsDeltaRoll, true)  // x_t − x_{t−d}

// ---- EXPAND (SCAN 族): 段内 expanding, 段界 reset ----
#define FACTOR_TS_EXPAND(Name, Fn)                                                                                                                                                \
  struct Name {                                                                                                                                                                   \
    static constexpr Strat kStrat = Strat::SCAN;                                                                                                                                  \
    static size_t workspace(int, int, const Param &) { return 0; }                                                                                                                \
    static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p, \
                    void *, cudaStream_t stream) {                                                                                                                                \
      assert(static_cast<long long>(T) * A < (1LL << 31));                                                                                                                        \
      k::expand<k::Fn><<<k::g_seg(T, A), kTB, 0, stream>>>(xv, xm, yv, ym, ov, om, T, A, p);                                                                                      \
      FACTOR_CUDA_OK(cudaGetLastError());                                                                                                                                         \
    }                                                                                                                                                                             \
  };
FACTOR_TS_EXPAND(TsSumCum, FnSum)   // Σ_{s≤t} x_s
FACTOR_TS_EXPAND(TsMeanCum, FnMean) // Σx/n
FACTOR_TS_EXPAND(TsVarCum, FnVar)   // M2/(n−1)
FACTOR_TS_EXPAND(TsStdCum, FnStd)   // √TsVarCum
FACTOR_TS_EXPAND(TsSkewCum, FnSkew) // m3/m2^1.5
FACTOR_TS_EXPAND(TsKurtCum, FnKurt) // m4/m2²−3
FACTOR_TS_EXPAND(TsMaxCum, FnExtCum<true>)
FACTOR_TS_EXPAND(TsMinCum, FnExtCum<false>)
FACTOR_TS_EXPAND(TsArgMaxCum, FnArgCum<true>) // 首个最大值距今期数
FACTOR_TS_EXPAND(TsArgMinCum, FnArgCum<false>)
FACTOR_TS_EXPAND(TsHhiCum, FnHhi)
FACTOR_TS_EXPAND(TsEntropyCum, FnEntropy)
FACTOR_TS_EXPAND(TsPeaksCum, FnPeaks)
FACTOR_TS_EXPAND(TsCountGtCum, FnCountGt)
FACTOR_TS_EXPAND(TsCovCum, FnCov)
FACTOR_TS_EXPAND(TsCorrCum, FnCorr)
FACTOR_TS_EXPAND(TsBetaCum, FnBeta)
FACTOR_TS_EXPAND(TsResidCum, FnResid)
FACTOR_TS_EXPAND(TsWMeanCum, FnWMean)
FACTOR_TS_EXPAND(TsCorrLagCum, FnCorrLag) // y 延迟 k 期, 取样在核内完成

// ---- ROLL (SCAN 族): 块内预热 + 滑窗 add/sub, 跨段不 reset ----
#define FACTOR_TS_ROLL(Name, Fn)                                                                                                                                                  \
  struct Name {                                                                                                                                                                   \
    static constexpr Strat kStrat = Strat::SCAN;                                                                                                                                  \
    static size_t workspace(int, int, const Param &) { return 0; }                                                                                                                \
    static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p, \
                    void *, cudaStream_t stream) {                                                                                                                                \
      assert(p.d >= 1 && p.d <= T && static_cast<long long>(T) * A < (1LL << 31));                                                                                                \
      const int C = k::roll_chunk(p.d);                                                                                                                                           \
      k::roll<k::Fn><<<k::g_roll(T, A, C), kTB, 0, stream>>>(xv, xm, yv, ym, ov, om, T, A, p.d, C, p);                                                                            \
      FACTOR_CUDA_OK(cudaGetLastError());                                                                                                                                         \
    }                                                                                                                                                                             \
  };
FACTOR_TS_ROLL(TsSumRoll, FnSum)
FACTOR_TS_ROLL(TsMeanRoll, FnMean)
FACTOR_TS_ROLL(TsVarRoll, FnVar)
FACTOR_TS_ROLL(TsStdRoll, FnStd)
FACTOR_TS_ROLL(TsSkewRoll, FnSkew)
FACTOR_TS_ROLL(TsKurtRoll, FnKurt)
FACTOR_TS_ROLL(TsZRoll, FnZ)
FACTOR_TS_ROLL(TsWmaRoll, FnWma)
FACTOR_TS_ROLL(TsProductRoll, FnProduct)
FACTOR_TS_ROLL(TsSlopeRoll, FnSlope)
FACTOR_TS_ROLL(TsCountGtRoll, FnCountGt)
FACTOR_TS_ROLL(TsAgeRoll, FnAge)
FACTOR_TS_ROLL(TsCovRoll, FnCov)
FACTOR_TS_ROLL(TsCorrRoll, FnCorr)
FACTOR_TS_ROLL(TsBetaRoll, FnBeta)
FACTOR_TS_ROLL(TsResidRoll, FnResid)
FACTOR_TS_ROLL(TsWMeanRoll, FnWMean)

// ---- EXTREME (4): 滑窗极值 / 距今期数 ----
//   workspace = 每时间块一份环形后缀区 [d][A]: gridY·d·A·sizeof(E). T=19200 A=5000 d=240: 20×240×5000×8B = 192 MB (pair 版)
#define FACTOR_TS_EXTREME(Name, MAXOP, ELEM)                                                                                                                                  \
  struct Name {                                                                                                                                                               \
    static constexpr Strat kStrat = Strat::EXTREME;                                                                                                                           \
    static size_t workspace(int T, int A, const Param &p) {                                                                                                                   \
      const int C = k::ext_chunk(p.d);                                                                                                                                        \
      return static_cast<size_t>((T + C - 1) / C) * p.d * A * sizeof(ELEM);                                                                                                   \
    }                                                                                                                                                                         \
    static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p, \
                    void *ws, cudaStream_t stream) {                                                                                                                          \
      assert(p.d >= 1 && p.d <= T && ws != nullptr && static_cast<long long>(T) * A < (1LL << 31));                                                                           \
      const int C = k::ext_chunk(p.d);                                                                                                                                        \
      k::extreme<MAXOP, ELEM><<<k::g_roll(T, A, C), kTB, 0, stream>>>(xv, xm, static_cast<ELEM *>(ws), ov, om, T, A, p.d, C);                                                 \
      FACTOR_CUDA_OK(cudaGetLastError());                                                                                                                                     \
    }                                                                                                                                                                         \
  };
FACTOR_TS_EXTREME(TsMaxRoll, true, float)
FACTOR_TS_EXTREME(TsMinRoll, false, float)
FACTOR_TS_EXTREME(TsArgMaxRoll, true, k::FI) // (d−1)−i, 并列保留最旧
FACTOR_TS_EXTREME(TsArgMinRoll, false, k::FI)

// ---- HIST (6) ----
#define FACTOR_TS_HIST(Name, MODE, H)                                                                                                                                                 \
  struct Name {                                                                                                                                                                       \
    static constexpr Strat kStrat = Strat::HIST;                                                                                                                                      \
    static size_t workspace(int, int, const Param &) { return 0; }                                                                                                                    \
    static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p, void *, \
                    cudaStream_t stream) {                                                                                                                                            \
      assert(static_cast<long long>(T) * A < (1LL << 31));                                                                                                                            \
      if (MODE == 1)                                                                                                                                                                  \
        assert(p.d >= 1 && p.d <= T);                                                                                                                                                 \
      k::hist<MODE, k::H><<<k::g_hist(T, A), kHB, 0, stream>>>(xv, xm, ov, om, T, A, p.d, p);                                                                                         \
      FACTOR_CUDA_OK(cudaGetLastError());                                                                                                                                             \
    }                                                                                                                                                                                 \
  };
FACTOR_TS_HIST(TsRankCum, 0, HRank)
FACTOR_TS_HIST(TsTopKCum, 0, HTopK)
FACTOR_TS_HIST(TsGiniCum, 0, HGini)
FACTOR_TS_HIST(TsRankRoll, 1, HRank)
FACTOR_TS_HIST(TsMedianRoll, 1, HMedian)
FACTOR_TS_HIST(TsMadRoll, 1, HMad)

// ---- EXPO (1) ----
struct TsMeanEma {
  static constexpr Strat kStrat = Strat::RECUR;
  // 块前缀 (a, b_seen, b_seed, seen) × nc 块: 80×5000×(4+4+4+1) = 5.2 MB
  static size_t workspace(int T, int A, const Param &) {
    const size_t nc = static_cast<size_t>((T + kSegLen - 1) / kSegLen);
    return nc * A * (3 * sizeof(float) + sizeof(uint8_t));
  }
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p, void *ws,
                  cudaStream_t stream) {
    assert(ws != nullptr && static_cast<long long>(T) * A < (1LL << 31));
    const int nc = (T + kSegLen - 1) / kSegLen;
    float *ca = static_cast<float *>(ws);
    float *cb = ca + static_cast<size_t>(nc) * A;
    float *cd = cb + static_cast<size_t>(nc) * A;
    uint8_t *cs = reinterpret_cast<uint8_t *>(cd + static_cast<size_t>(nc) * A);
    const dim3 g = k::g_seg(T, A);
    k::ema_local<<<g, kTB, 0, stream>>>(xv, xm, ca, cb, cd, cs, T, A, p.k);
    FACTOR_CUDA_OK(cudaGetLastError());
    k::ema_carry<<<(A + kTB - 1) / kTB, kTB, 0, stream>>>(ca, cb, cd, cs, A, nc);
    FACTOR_CUDA_OK(cudaGetLastError());
    k::ema_apply<<<g, kTB, 0, stream>>>(xv, xm, ca, cs, ov, om, T, A, p.k);
    FACTOR_CUDA_OK(cudaGetLastError());
  }
};

} // namespace factor::gpu::ts
