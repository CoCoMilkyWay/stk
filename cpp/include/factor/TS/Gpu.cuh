#pragma once

// =============================================================================
// TS 轴挖掘后端 (CUDA, sm_75 / RTX  / 336 GB·s⁻¹; clang -x cuda)
// =============================================================================
//   语义契约见 factor/Contract.hpp, 算子真相表见 factor/OpTable.hpp.
//   本文件与 TS/Cpu.hpp / TS/Stream.hpp **完全独立** (对拍才有意义), 只依赖 Contract + CUDA/CUB.
//
//   【布局】SoA 行主序 [T][A], 下标 t*A + a. 同一时刻的资产在内存里连续
//     → TS 方向"一线程一资产 + 沿 t 串行"天然合并访存 (相邻线程读相邻地址, 128B/warp).
//     典型规模 T = 19200 (80 段 × kSegLen 240), A = 5000, 单平面 fp32 = 384 MB.
//
//   【核配置总则】
//     POINT : 2D 网格逐格融合, grid(⌈A/256⌉, min(T,512)) × 256 线程, 纯带宽.
//     SCAN  : 一线程一资产. EXPAND 按段 (240 步寄存器串行, 段天然独立);
//             ROLL 按块 (块长 C = kSegLen·⌈d/kSegLen⌉) 预热 d−1 步后滑窗 add/sub,
//             每块重建累加器 = 契约要求的"分块", 误差不跨块累积.
//             **不用全局前缀和平面**: 五和 (Σx,Σy,Σx²,Σy²,Σxy) 各需 384 MB,
//             再加 pre/suf 两级就是 3.8 GB, 6 GB 卡上放不下 (详见回执). 滑窗差分等价且 ws = 0.
//     EXTREME: 滑窗极值 van Herk / Gil-Werman: 以 d 为块长, 块内前缀极值 + 前一块后缀极值合并.
//             一线程一资产串行, 与 d 无关的常数趟; 工作区只存前一块的后缀 (环形 [d][A]), 不再有 log₂d 趟全平面.
//     HIST  : **一 block 一 (资产, 时间块)**: 块内样本按值排序 + 在时间序上建 wavelet matrix, 每个输出是
//             O(log N) 次 smem 位运算, 无数据相关分支, 代价与 d 无关. (逐 lane 的"增量直方图 + 偶尔重建"在
//             SIMT 上退化: 任一 lane 重建整个 warp 都等, P(warp 重建) = 1−(1−p)³² ≈ 85%, 实测 d = 1275 跑 1 s.)
//     RECUR : 仿射复合 (a,b)∘(c,d) = (a·c, b·c+d) 的分块 scan (块内 → 块间 → 回填).
//
//   【数值】幂和 fp64, 按段/按块重建; 高阶矩两遍: 第一遍扫出块内 (μ,σ) 作参考,
//     第二遍在 z = (x−μ)/σ 上累幂和 (中心矩对平移不变, 偏度峰度对缩放不变 → 代数等价, 条件数好得多).
//     全并列判据 (契约第 2 条) 不看累加器: ROLL 的 add/sub 残留会让常值窗的 M2 ≠ 0, 阈值判据在这里必然与
//     CPU 不一致. 改用 Chg 追踪 "最近一次相邻有效样本不等的位置" 与 "窗内首个有效位置" (都是整数, 无减法) → 精确.
//     全程 branchless (select, 不用分支), 出口 dev::store 保证 ov/om 永不含 NaN/inf.
//     退化按契约第 6 条给中性值 (select 一支, 掩码不动); 掩码只随输入格 (第 7 条).
//     ROLL 未满窗 = 已有前缀: 预热区 [w0, c0) 本来就只装前缀, 不再有 "t ≥ d−1" 判据.
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
// HIST 的 block 尺寸随容量走, 见第 6 节
inline constexpr int kPB = 256;
inline constexpr int kTB = 256;

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

// GATHER: 取 max(t−d, 0) 行 (滞后夹到序列头; d 只改地址, 不改算法)
template <bool DELTA>
__global__ void gather(const float *xv, const uint8_t *xm, float *ov, uint8_t *om, int T, int A, int d) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  for (int t = blockIdx.y; t < T; t += gridDim.y) {
    const int i = t * A + a;
    const int j = max(t - d, 0) * A + a;
    const float xo = xv[j];
    const bool mo = xm[j] != 0;
    dev::store(ov, om, i, DELTA ? (xv[i] - xo) : xo, DELTA ? (mo && xm[i] != 0) : mo);
  }
}

// =============================================================================
// 2. 累加器 / 两遍参考量 (EXPAND 与 ROLL 共用同一批 functor)
// =============================================================================
struct RefNone {};
struct Ref1 {
  float mu, sg;
  double isg; // 1/σ: 每样本 z = (x−μ)·isg 用乘法, 不做 fp64 除法 (sm_75 的 fp64 除法是十几条 1/32 速率的指令)
};
struct Ref2 {
  float mx, sx, my, sy;
  double isx, isy;
};

// 1/n (n ≥ 1, ≤ 2²⁴): fp32 正确舍入倒数 + 一步牛顿 → 相对误差 ~1e-14, 两条 fp64 指令代替 fp64 除法序列.
// 参考量 / 均值化这类 "除以样本数" 对 1e-14 不敏感 (对拍容差 1e-3); 分母可任意小的除法 (Z / Corr / Beta …) 仍走 fp64 除法
__device__ __forceinline__ double rcp_int(int n) {
  const double f = static_cast<double>(__frcp_rn(static_cast<float>(n)));
  return f * (2.0 - static_cast<double>(n) * f);
}

// 第一遍: 扫 [from,to) 的 (μ,σ) 作为标准化参考. 参考量只影响条件数, 不影响代数结果
__device__ inline void ref_scan(const float *v, const uint8_t *mk, int a, int A, int from, int to, float &mu, float &sg, double &isg) {
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
  isg = 1.0 / static_cast<double>(sg);                  // 每线程每块一次
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
  const double z = (static_cast<double>(x.v) - r.mu) * r.isg;
  const double gz = fg * z, z2 = z * z;
  s.n += g;
  s.z1 += gz;
  s.z2 += gz * z;
  s.z3 += gz * z2;
  s.z4 += gz * z2 * z;
}
__device__ __forceinline__ void upd_z2(StZ2 &s, DVal x, DVal y, int w, const Ref2 &r) {
  const int g = (x.m && y.m) ? w : 0; // 二元: 两侧同时有效
  const double fg = static_cast<double>(g);
  const double zx = (static_cast<double>(x.v) - r.mx) * r.isx, zy = (static_cast<double>(y.v) - r.my) * r.isy;
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
  const double inv = rcp_int(max(s.n, 1));
  const double b1 = s.z1 * inv, b2 = s.z2 * inv, b3 = s.z3 * inv, b4 = s.z4 * inv;
  const double g = r.sg, g2 = g * g, b11 = b1 * b1;
  o.mean = r.mu + g * b1;
  o.M2 = g2 * (s.z2 - s.z1 * b1); // Σ(x−x̄)²
  o.m2 = g2 * (b2 - b11);
  o.m3 = g2 * g * (b3 - 3.0 * b1 * b2 + 2.0 * b11 * b1);
  o.m4 = g2 * g2 * (b4 - 4.0 * b1 * b3 + 6.0 * b11 * b2 - 3.0 * b11 * b11);
}
constexpr int kNoBase = -(1 << 29); // 窗内下标重基用的哨兵 (还没有元素进过窗)

struct Mom2 {
  double mx, my, cxx, cyy, cxy;
};
__device__ inline void mom2_of(const StZ2 &s, const Ref2 &r, Mom2 &o) {
  const double inv = rcp_int(max(s.n, 1));
  const double bx = s.zx * inv, by = s.zy * inv;
  o.mx = r.mx + r.sx * bx;
  o.my = r.my + r.sy * by;
  o.cxx = static_cast<double>(r.sx) * r.sx * (s.zxx - s.zx * bx);
  o.cyy = static_cast<double>(r.sy) * r.sy * (s.zyy - s.zy * by);
  o.cxy = static_cast<double>(r.sx) * r.sy * (s.zxy - s.zx * by);
}

// =============================================================================
// 3. SCAN 骨架: EXPAND (段内 expanding) / ROLL (滑窗 add-sub)
//    Op 接口: kArity / kSpread / Ref / St / pre / init / upd(w=±1) / emit(…, sx, sy, …)
//    kSpread = true 的算子由核维护 Chg + f, emit 拿到 sx/sy = x/y 在窗内非全并列 (精确)
// =============================================================================

// 取样点 (s 为块内坐标, 基址 base 为块内 0 对应的全局行) 的有效位: 一元看 x; 二元看 x 且 y
template <class Op>
__device__ __forceinline__ bool valid_at(const uint8_t *xm, const uint8_t *ym, int base, int s, int A, int a) {
  bool g = xm[(base + s) * A + a] != 0;
  if constexpr (Op::kArity >= 2)
    g = g && ym[(base + s) * A + a] != 0;
  return g;
}

template <class Op>
__global__ void expand(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, float *ov, uint8_t *om, int T, int A, Param p) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  const int s0 = blockIdx.y * kSegLen; // 段起点 (块起点对齐段起点)
  const int len = min(kSegLen, T - s0);
  typename Op::Ref r;
  Op::pre(r, xv, xm, yv, ym, a, A, s0, s0 + len, p);
  typename Op::St st;
  Op::init(st, p);
  Chg ch;
  ch.init();
  int f = 0;                      // 段内首个有效位置 (只前进)
  for (int s = 0; s < len; ++s) { // 段内 kSegLen 步寄存器串行, 段间完全独立
    const int i = (s0 + s) * A + a;
    DVal x{xv[i], xm[i]}, y{0.f, 0};
    if constexpr (Op::kArity >= 2)
      y = DVal{yv[i], ym[i]};
    Op::upd(st, x, y, s, 1, r, p);
    bool sx = false, sy = false;
    if constexpr (Op::kSpread) {
      ch.push(Op::kArity >= 2 ? (x.m && y.m) : (x.m != 0), x.v, y.v, s);
      while (f <= s && !valid_at<Op>(xm, ym, s0, f, A, a))
        ++f;
      sx = ch.lastx > f;
      sy = ch.lasty > f;
    }
    float v = 0.f;
    bool m = false;
    Op::emit(st, x, y, s, sx, sy, r, p, v, m);
    dev::store(ov, om, i, v, m);
  }
}

template <class Op>
__global__ void roll(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, float *ov, uint8_t *om, int T, int A, int d, int C, Param p) {
  const int a = blockIdx.x * blockDim.x + threadIdx.x;
  if (a >= A)
    return;
  const int c0 = blockIdx.y * C; // 本块负责的输出行区间 [c0, cend), C = kSegLen·⌈d/kSegLen⌉
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
      while (f <= li && !valid_at<Op>(xm, ym, c0, f, A, a))
        ++f;
      sx = ch.lastx > f;
      sy = ch.lasty > f;
    }
    float v = 0.f;
    bool m = false;
    Op::emit(st, x, y, li, sx, sy, r, p, v, m); // 未满窗: 累加器只装了前缀, 即契约的 "窗 = 已有前缀"
    dev::store(ov, om, i, v, m);
  }
}

// ---- 无参考量的 pre (单遍算子) ----
#define FACTOR_TS_PRE_NONE \
  __device__ static void pre(RefNone &, const float *, const uint8_t *, const float *, const uint8_t *, int, int, int, int, const Param &) {}
// ---- 一元两遍 pre ----
#define FACTOR_TS_PRE_X                                                                                                                                    \
  __device__ static void pre(Ref1 &r, const float *xv, const uint8_t *xm, const float *, const uint8_t *, int a, int A, int from, int to, const Param &) { \
    ref_scan(xv, xm, a, A, from, to, r.mu, r.sg, r.isg);                                                                                                   \
  }
// ---- 二元两遍 pre ----
#define FACTOR_TS_PRE_XY                                                                                                                                       \
  __device__ static void pre(Ref2 &r, const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, int a, int A, int from, int to, const Param &) { \
    ref_scan(xv, xm, a, A, from, to, r.mx, r.sx, r.isx);                                                                                                       \
    ref_scan(yv, ym, a, A, from, to, r.my, r.sy, r.isy);                                                                                                       \
  }

// =============================================================================
// 4. SCAN functor 集 (Ts<核>Cum 与 Ts<核>Roll 共用一个 functor: 只差所在窗形)
//    emit: 退化走 select 给契约第 6 条的中性值; m 只随输入格 (相对型 Z / Resid 看 x_t / y_t, 其余恒真)
// =============================================================================

// ---- Σx / 均值 (单遍, 段/块内 ≤ C 项, 无需标准化); 空 → 0 ----
struct FnSum {
  static constexpr int kArity = 1;
  static constexpr bool kSpread = false;
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
  __device__ static void emit(const St &s, DVal, DVal, int, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = static_cast<float>(s.s1);
    m = true;
  }
};
struct FnMean : FnSum {
  __device__ static void emit(const StRaw &s, DVal, DVal, int, bool, bool, const RefNone &, const Param &, float &v, bool &m) {
    v = static_cast<float>(s.s1 * rcp_int(max(s.n, 1))); // Σx / n; n = 0 时 Σx = 0 → 0
    m = true;
  }
};

// ---- 方差族 (两遍: z = (x−μ)/σ 上累幂和; ddof=1; 全并列由核给的 sx 判); 样本不足 / 全并列 → 0 ----
//      emit 的末步 (开方 / 有界比值) 在 fp32 做: 相消已在 mom_of 的 fp64 里完成, 末步条件数 ~1,
//      fp32 的 1e-7 相对误差远在对拍容差 (1e-3 / 1e-2) 之内; sm_75 fp64 sqrt / div / pow 各是几十条 1/32 速率指令
struct FnVar {
  static constexpr int kArity = 1;
  static constexpr bool kSpread = true;
  using Ref = Ref1;
  using St = StZ;
  FACTOR_TS_PRE_X
  __device__ static void init(St &s, const Param &) { s = StZ{0, 0.0, 0.0, 0.0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal, int, int w, const Ref &r, const Param &) { upd_z(s, x, w, r); }
  __device__ static void emit(const St &s, DVal, DVal, int, bool sx, bool, const Ref &r, const Param &, float &v, bool &m) {
    Mom q;
    mom_of(s, r, q);
    const bool ok = s.n >= 2 && sx;
    v = ok ? static_cast<float>(q.M2 * rcp_int(max(s.n - 1, 1))) : 0.f; // M2/(n−1)
    m = true;
  }
};
struct FnStd : FnVar {
  __device__ static void emit(const StZ &s, DVal, DVal, int, bool sx, bool, const Ref1 &r, const Param &, float &v, bool &m) {
    Mom q;
    mom_of(s, r, q);
    const bool ok = s.n >= 2 && sx;
    v = ok ? sqrtf(fmaxf(static_cast<float>(q.M2 * rcp_int(max(s.n - 1, 1))), 0.f)) : 0.f;
    m = true;
  }
};
struct FnSkew : FnVar {
  __device__ static void emit(const StZ &s, DVal, DVal, int, bool sx, bool, const Ref1 &r, const Param &, float &v, bool &m) {
    Mom q;
    mom_of(s, r, q);
    // m3/m2^1.5: 非全并列 ⇒ m2 > 0; |m3|/m2^1.5 ≤ √n 有柯西–施瓦茨上界, 恒为有限值. m2^1.5 = m2·√m2 (无 pow)
    const float m2 = fmaxf(static_cast<float>(q.m2), 0.f);
    const bool ok = s.n >= 3 && sx;
    v = ok ? static_cast<float>(q.m3) / (m2 * sqrtf(m2)) : 0.f;
    m = true;
  }
};
struct FnKurt : FnVar {
  __device__ static void emit(const StZ &s, DVal, DVal, int, bool sx, bool, const Ref1 &r, const Param &, float &v, bool &m) {
    Mom q;
    mom_of(s, r, q);
    // m4/m2² − 3: m4/m2² ≤ n 有界
    const float m2 = fmaxf(static_cast<float>(q.m2), 0.f);
    const bool ok = s.n >= 4 && sx;
    v = ok ? static_cast<float>(q.m4) / (m2 * m2) - 3.f : 0.f;
    m = true;
  }
};
// 相对型: 描述 x_t 自身, x_t 无效则无效; σ 退化 → 0 (x = μ)
struct FnZ : FnVar {
  __device__ static void emit(const StZ &s, DVal x, DVal, int, bool sx, bool, const Ref1 &r, const Param &, float &v, bool &m) {
    Mom q;
    mom_of(s, r, q);
    const float sd = sqrtf(fmaxf(static_cast<float>(q.M2 * rcp_int(max(s.n - 1, 1))), 0.f));
    const bool ok = s.n >= 2 && sx;
    v = ok ? static_cast<float>(static_cast<double>(x.v) - q.mean) / sd : 0.f; // 分子的相消留在 fp64
    m = x.m;
  }
};

// ---- 段内 cummax / cummin (max 是结合算子, 直接 scan; 只在 EXPAND 用, w 恒 +1) ----
template <bool MAXOP>
struct FnExtCum {
  static constexpr int kArity = 1;
  static constexpr bool kSpread = false;
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
  __device__ static void emit(const St &s, DVal, DVal, int, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = s.n >= 1 ? s.best : 0.f; // n = 0 时 best = ±FLT_MAX 哨兵, 换成中性值 0
    m = true;
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
  __device__ static void emit(const St &s, DVal, DVal, int li, bool, bool, const RefNone &, const Param &, float &v, bool &m) {
    v = s.n >= 1 ? static_cast<float>(li - s.pos) : 0.f;
    m = true;
  }
};

// ---- TsHhiCum: Σx²/(Σx)²; Σx 相消 (非负域下即全 0) → 1/n (全体相等的极限); 空 → 0 ----
struct FnHhi {
  static constexpr int kArity = 1;
  static constexpr bool kSpread = false;
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
  __device__ static void emit(const St &s, DVal, DVal, int, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    const double s11 = s.s1 * s.s1; // 平方在 fp64 做 (防下溢), 比值 ∈ [1/n, 1] 在 fp32 除
    const bool ok = dev::den_ok(s11, static_cast<double>(s.n) * s.s2);
    v = s.n < 1 ? 0.f : (ok ? static_cast<float>(s.s2) / static_cast<float>(s11) : 1.f / static_cast<float>(s.n));
    m = true;
  }
};

// ---- TsEntropyCum: ln S − Σ(x·ln x)/S, 只计 x > 0; 有正样本 ⇒ S > 0; 无正样本 → 0 (单样本熵 = 0 的极限) ----
struct FnEntropy {
  static constexpr int kArity = 1;
  static constexpr bool kSpread = false;
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
    // x ≤ 0 的分支权 0, 参数换 1 防 log(≤0) 污染. logf (1 ulp) 代替 fp64 log: 每项误差 1e-7·|x ln x|,
    // 且进窗 / 出窗算的是同一个值, 滑窗加减精确相消, 只剩 fp64 累加的舍入
    s.sxlx += fg * x.v * static_cast<double>(logf(g ? x.v : 1.f));
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    v = s.np >= 1 ? logf(static_cast<float>(s.S)) - static_cast<float>(s.sxlx) / static_cast<float>(s.S) : 0.f;
    m = true;
  }
};

// ---- 协方差族 (两遍; Corr/Beta/Resid 的全并列由核给的 sx/sy 判); 退化 → 0 (Resid → β = 0 即 x − μ^x) ----
struct FnCov {
  static constexpr int kArity = 2;
  static constexpr bool kSpread = false;
  using Ref = Ref2;
  using St = StZ2;
  FACTOR_TS_PRE_XY
  __device__ static void init(St &s, const Param &) { s = StZ2{0, 0.0, 0.0, 0.0, 0.0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal y, int, int w, const Ref &r, const Param &) { upd_z2(s, x, y, w, r); }
  __device__ static void emit(const St &s, DVal, DVal, int, bool, bool, const Ref &r, const Param &, float &v, bool &m) {
    Mom2 q;
    mom2_of(s, r, q);
    v = s.n >= 2 ? static_cast<float>(q.cxy * rcp_int(max(s.n - 1, 1))) : 0.f;
    m = true;
  }
};
struct FnCorr : FnCov {
  static constexpr bool kSpread = true;
  __device__ static void emit(const StZ2 &s, DVal, DVal, int, bool sx, bool sy, const Ref2 &r, const Param &, float &v, bool &m) {
    Mom2 q;
    mom2_of(s, r, q);
    // 按规格不 clamp |ρ| ≤ 1: 非全并列 ⇒ cxx、cyy > 0, 且 |cxy| ≤ √(cxx·cyy) → 比值有界.
    // √cxx·√cyy 各自开方再相乘 (不先乘积: fp32 下乘积会下溢成 0), 末步全 fp32
    const bool ok = s.n >= 2 && sx && sy;
    const float sdx = sqrtf(fmaxf(static_cast<float>(q.cxx), 0.f)), sdy = sqrtf(fmaxf(static_cast<float>(q.cyy), 0.f));
    v = ok ? static_cast<float>(q.cxy) / (sdx * sdy) : 0.f;
    m = true;
  }
};
struct FnBeta : FnCov {
  static constexpr bool kSpread = true;
  __device__ static void emit(const StZ2 &s, DVal, DVal, int, bool, bool sy, const Ref2 &r, const Param &, float &v, bool &m) {
    Mom2 q;
    mom2_of(s, r, q);
    const bool ok = s.n >= 2 && sy;
    v = ok ? static_cast<float>(q.cxy) / static_cast<float>(q.cyy) : 0.f;
    m = true;
  }
};
struct FnResid : FnCov { // 相对型: 当前对有效 ⇒ n ≥ 1, 均值有定义
  static constexpr bool kSpread = true;
  __device__ static void emit(const StZ2 &s, DVal x, DVal y, int, bool, bool sy, const Ref2 &r, const Param &, float &v, bool &m) {
    Mom2 q;
    mom2_of(s, r, q);
    const double b = (s.n >= 2 && sy) ? static_cast<double>(static_cast<float>(q.cxy) / static_cast<float>(q.cyy)) : 0.0;
    v = static_cast<float>((static_cast<double>(x.v) - q.mx) - b * (static_cast<double>(y.v) - q.my));
    m = x.m && y.m;
  }
};
// ---- 加权均值: Σ(y·x)/Σy; Σy 相消 → 等权均值 Σx/n (权全相等的极限); 空 → 0 ----
struct FnWMean {
  static constexpr int kArity = 2;
  static constexpr bool kSpread = false;
  using Ref = RefNone;
  struct St {
    int n;
    double sx, swx, sw, saw;
  };
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0, 0.0, 0.0, 0.0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal y, int, int w, const Ref &, const Param &) {
    const int g = (x.m && y.m) ? w : 0;
    const double fg = static_cast<double>(g);
    s.n += g;
    s.sx += fg * x.v;
    s.swx += fg * static_cast<double>(y.v) * x.v;
    s.sw += fg * y.v;
    s.saw += fg * fabsf(y.v);
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    const bool ok = dev::den_ok(s.sw, s.saw);
    v = s.n < 1 ? 0.f : (ok ? static_cast<float>(s.swx) / static_cast<float>(s.sw) : static_cast<float>(s.sx * rcp_int(s.n)));
    m = true;
  }
};

// ---- TsWmaRoll: w = i+1 (窗内下标, 最旧 = 0) → Σ(w·x) = Σ(s·x) − (t−d)·Σx, Σw = Σs − (t−d)·n ----
//      s 用**块内**行号 (|s| ≤ C+d): 用全局行号会让 Σ(s·x) ~ 4.6e6 与 (t−d)Σx 抵消掉 2 位有效数字
struct FnWma {
  static constexpr int kArity = 1;
  static constexpr bool kSpread = false;
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
  __device__ static void emit(const St &s, DVal, DVal, int, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    const double wx = s.ssx + s.sx;                    // 权 w = i+1 → Σ(w·x) = Σ(i·x) + Σx
    const double sw = s.ss + static_cast<double>(s.n); // Σw (w ≥ 1 → n ≥ 1 时 Σw ≥ 1); 空 → 0
    v = s.n >= 1 ? static_cast<float>(wx) / static_cast<float>(sw) : 0.f;
    m = true;
  }
};

// ---- TsSlopeRoll: x 对窗内下标 i = 0..d−1 的 OLS 斜率 (下标矩同样由 Σs/Σs²/Σ(s·x) 线性组合出来) ----
//      下标两两不同 ⇒ n ≥ 2 时 Σ(i−ī)² ≥ 1/2, 无需退化判据
struct FnSlope {
  static constexpr int kArity = 1;
  static constexpr bool kSpread = false;
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
  __device__ static void emit(const St &s, DVal, DVal, int, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    const double inv = rcp_int(max(s.n, 1));
    const double sxx = s.sss - s.ss * s.ss * inv;                                                 // Σ(i−ī)² (相消在 fp64)
    v = s.n >= 2 ? static_cast<float>(s.ssx - s.ss * s.sx * inv) / static_cast<float>(sxx) : 0.f; // n < 2 → 0 (无趋势); sxx ≥ 1/2
    m = true;
  }
};

// ---- TsProductRoll: Π(1+x) − 1 精确. 累 Σ ln|1+x| (x > −1 用 log1p, x < −1 用 log(−1−x), 与 host 同式),
//      零因子 (x = −1) 与负因子 (x < −1) 用可加减的计数表达; 空窗 = 空积 − 1 = 0 ----
struct FnProduct {
  static constexpr int kArity = 1;
  static constexpr bool kSpread = false;
  using Ref = RefNone;
  struct St {
    int zero, neg;
    double sl;
  };
  FACTOR_TS_PRE_NONE
  __device__ static void init(St &s, const Param &) { s = St{0, 0, 0.0}; }
  __device__ static void upd(St &s, DVal x, DVal, int, int w, const Ref &, const Param &) {
    const int g = x.m ? w : 0;
    const double fg = static_cast<double>(g);
    const bool gt = x.v > -1.f, lt = x.v < -1.f;
    s.zero += (x.m && x.v == -1.f) ? w : 0;
    s.neg += (x.m && lt) ? w : 0;
    // 两支都算成有限值再 select (零因子参数换 0 → log1p 0 = 0), 不让 −inf / NaN 进滑窗累加器
    // fp32 对数 (1 ulp): 进窗 / 出窗同值精确相消, 累加器仍 fp64
    const float l = gt ? log1pf(x.v) : (lt ? logf(-1.f - x.v) : 0.f);
    s.sl += fg * static_cast<double>(l);
  }
  __device__ static void emit(const St &s, DVal, DVal, int, bool, bool, const Ref &, const Param &, float &v, bool &m) {
    const float sl = static_cast<float>(s.sl);
    v = s.zero > 0 ? -1.f : ((s.neg & 1) ? -expf(sl) - 1.f : expm1f(sl));
    m = true;
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
      const int l = t - d + 1;                                   // 窗左端; l < 0 = 未满窗, 窗 = 前缀 [0, t] ⊂ 首块 = P
      const bool cross = l >= 0 && l < b0;                       // 窗跨前块 (l 在前块) ; 否则 t 为块末, 窗 = 整块 = P
      const E e = cross ? cmb<MAXOP>(S[(l % d) * A + a], P) : P; // S 更旧, 放前面 → 并列保留最旧
      dev::store(ov, om, i, n >= 1 ? X::out(e, t) : 0.f, true);  // 空窗 → 0 (e 是哨兵)
    }
    E run = X::ident();
    for (int t = bend - 1; t >= b0; --t) { // 后向: 块内后缀极值 → 环形区 (前向已用完前块的 S, 可覆盖)
      run = cmb<MAXOP>(X::at(xv, xm, t * A + a, t), run);
      S[(t % d) * A + a] = run;
    }
  }
}

// =============================================================================
// 6. HIST: 序统计族 (TsRankCum / TsRankRoll / TsQuantileRoll) —— 块内排序 + wavelet matrix, 桶号与 CPU 逐位一致
//    **一 block 一 (资产 a, 时间块 [c0, cend))**. 块内所有查询窗 ⊂ [base0, cend), N = cend − base0 ≤ NCAP:
//      EXPAND: 块 = 一段, base0 = 段首, N ≤ kSegLen;  ROLL: 块长 C = NCAP − d + 1, base0 = max(0, c0−d+1).
//    建 (每块一次, 摊到 C 个查询上):
//      1. 读 N 格 → 保序 uint32 键 (无效 = 全 1, 排到最后), cub::BlockRadixSort (键, 时间位) →
//         sorted[] (升序键) 与 rk[i] (时间位 i 的名次); nvalid = 首个无效键的位置.
//      2. 在序列 rk[0..N) 上建 wavelet matrix (L = log₂NCAP 层位向量, 每字带 popcount 前缀):
//         区间 [l,r) 内 "名次 < v 的个数" (count_less) 与 "第 k 小的名次" (quantile) 都是 L 步 × 2 次 rank1.
//      3. 有效位向量 vb: 窗内有效数 n = rank1(r) − rank1(l).
//    查 (一线程一 t, 没有数据相关的循环):
//      lo / hi = sorted[quantile(1)] / sorted[quantile(n)];  b = bin_of_fast(x_t, lo, hi).
//      bin_of 对 x 单调不减 ⇒ {x : bin(x) < b} = {x : x < θ_b}, θ_b = 使 bin ≥ b 的最小 float (bin_edge: 解析解
//      收紧区间后在保序键空间二分, 精确) ⇒ less = count_less(l, r, lower_bound(sorted, key(θ_b))), 与 CPU 直方图
//      前缀逐个相同. 分位: 第 ⌈k·n⌉ 小的值所在桶 = CPU 的前缀首达桶 (bin 单调 ⇒ 等价), 一次 quantile.
//    每查询 ≈ 5 次 L 步遍历 + 2 次二分 ≈ 200 次 smem 读, 与 d 无关; 无 warp 发散.
//    容量: EXPAND NCAP = 256 (256 线程 × 1); ROLL d ≤ 1024 → 2048 (512 × 4, ≈ 21 KB smem, 每 SM 2 块),
//          d ≤ kMaxD = 1275 → 4096 (512 × 8, ≈ 43 KB, 每 SM 1 块). 键把 ±0 归一为 +0 (与 bin_of / fmin 判等一致).
// =============================================================================
constexpr int ilog2(int n) { return n <= 1 ? 0 : 1 + ilog2(n >> 1); }
inline constexpr uint32_t kBadKey = 0xffffffffu; // 无效样本的键 (有限 float 的键 ≤ 0xff800000)

// float → 保序 uint32 (a < b ⟺ fkey(a) < fkey(b)); ±0 归一为 +0
__device__ __forceinline__ uint32_t fkey(float f) {
  const uint32_t u = __float_as_uint(f + 0.f);
  return (u & 0x80000000u) ? ~u : (u | 0x80000000u);
}
__device__ __forceinline__ float funkey(uint32_t k) { return __uint_as_float((k & 0x80000000u) ? (k & 0x7fffffffu) : ~k); }

// 首个 ≥ key 的下标 ∈ [0, n]
__device__ __forceinline__ int lower_bound(const uint32_t *s, int n, uint32_t key) {
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

// 使 bin_of(x, lo, hi) ≥ b 的最小 float (1 ≤ b ≤ kBuckets−1). bin_of 对 x 单调不减 (减 / 除正数 / 乘 2⁸ / 截断都单调),
// 在保序键空间二分 (不变量 bin(kl) < b ≤ bin(kh)), 区间先用解析解 ±1 ulp 收紧 → 常见情形 2 次 bin_of 命中.
// **不能从解析解逐 ulp 走**: 桶边落在 0 附近时 (对数收益 lo ≈ −hi, 中桶边 ≈ 0), 解析解离真边虽只 ~1e-18,
// 中间却隔着上亿个 float (0 附近 float 极密, 含全部 denormal), nextafterf 走不完 —— 实测整核卡死
__device__ __forceinline__ float bin_edge(int b, float lo, float hi) {
  const float xa = static_cast<float>(lo + (static_cast<double>(hi) - lo) * b / kBuckets);
  uint32_t kl, kh;
  if (dev::bin_of(xa, lo, hi) >= b) {
    kh = fkey(xa);
    const float xd = nextafterf(xa, -FLT_MAX);
    kl = dev::bin_of(xd, lo, hi) < b ? fkey(xd) : fkey(lo); // bin(lo) = 0 < b
  } else {
    kl = fkey(xa);
    const float xu = nextafterf(xa, FLT_MAX);
    kh = dev::bin_of(xu, lo, hi) >= b ? fkey(xu) : fkey(hi); // bin(hi) = 255 ≥ b
  }
  while (kh - kl > 1u) { // ≤ 32 步; [key(lo), key(hi)] 内每个键都是有限 float (含 −0 → 与 +0 同桶)
    const uint32_t km = kl + ((kh - kl) >> 1);
    if (dev::bin_of(funkey(km), lo, hi) >= b)
      kh = km;
    else
      kl = km;
  }
  return funkey(kh);
}

// ---- 位向量 + 每字前缀: rank1(i) = [0, i) 内 1 的个数 ----
template <int NCAP>
struct BitRank {
  static constexpr int W = NCAP / 32;
  uint32_t w[W + 1];   // 末字恒 0 (i = N 且 32 | N 时落到字 W)
  uint16_t pre[W + 1]; // 字前 1 的个数 (≤ NCAP)
  __device__ __forceinline__ int rank1(int i) const { return pre[i >> 5] + __popc(w[i >> 5] & ((1u << (i & 31)) - 1u)); }
};

// ---- wavelet matrix: 名次序列 (值 < NCAP) 的 L 层位向量, 自高位起逐层稳定划分 (0 在前) ----
template <int NCAP>
struct WMat {
  static constexpr int L = ilog2(NCAP);
  BitRank<NCAP> b[L];
  int z[L]; // 每层 0 的个数
  // [l,r) 内名次 < v 的个数 (v < NCAP)
  __device__ int count_less(int l, int r, int v) const {
    int cnt = 0;
#pragma unroll
    for (int lv = L - 1; lv >= 0; --lv) {
      const int l1 = b[lv].rank1(l), r1 = b[lv].rank1(r);
      if ((v >> lv) & 1) { // 本层为 0 的都小于 v
        cnt += (r - l) - (r1 - l1);
        l = z[lv] + l1;
        r = z[lv] + r1;
      } else {
        l -= l1;
        r -= r1;
      }
    }
    return cnt;
  }
  // [l,r) 内第 k 小 (1 ≤ k ≤ r−l) 的名次
  __device__ int quantile(int l, int r, int k) const {
    int res = 0;
#pragma unroll
    for (int lv = L - 1; lv >= 0; --lv) {
      const int l1 = b[lv].rank1(l), r1 = b[lv].rank1(r);
      const int zs = (r - l) - (r1 - l1);
      if (k <= zs) {
        l -= l1;
        r -= r1;
      } else {
        k -= zs;
        res |= 1 << lv;
        l = z[lv] + l1;
        r = z[lv] + r1;
      }
    }
    return res;
  }
};

// 块内集体: 由 seq[0..N) 的第 lv 位 (lv < 0: 取 seq[i] < nvalid) 建位向量 + 前缀, 返回 1 的总数
template <int NCAP, int NT>
__device__ inline int build_bits(BitRank<NCAP> &br, const uint16_t *seq, int N, int lv, int nvalid, typename cub::BlockScan<int, NT>::TempStorage &scan) {
  constexpr int W = NCAP / 32, NW = NT / 32;
  static_assert(NT >= W + 1, "前缀扫描要求线程数 ≥ 字数 + 1");
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  for (int w = warp; w < W; w += NW) {
    const int i = w * 32 + lane;
    const int s = i < N ? seq[i] : 0;
    const bool bit = i < N && (lv >= 0 ? (((s >> lv) & 1) != 0) : (s < nvalid));
    const uint32_t word = __ballot_sync(0xffffffffu, bit);
    if (lane == 0)
      br.w[w] = word;
  }
  __syncthreads();
  const int cnt = static_cast<int>(threadIdx.x) < W ? __popc(br.w[threadIdx.x]) : 0;
  int ex = 0, total = 0;
  cub::BlockScan<int, NT>(scan).ExclusiveSum(cnt, ex, total);
  if (static_cast<int>(threadIdx.x) < W)
    br.pre[threadIdx.x] = static_cast<uint16_t>(ex);
  if (threadIdx.x == 0) {
    br.pre[W] = static_cast<uint16_t>(total);
    br.w[W] = 0u;
  }
  __syncthreads();
  return total;
}

// 块内集体: 在 seq (原地划分, 用后即毁) 上建 wavelet matrix
template <int NCAP, int NT>
__device__ inline void build_wm(WMat<NCAP> &wm, uint16_t *seq, int N, typename cub::BlockScan<int, NT>::TempStorage &scan) {
  constexpr int IPT = NCAP / NT;
  for (int lv = WMat<NCAP>::L - 1; lv >= 0; --lv) {
    BitRank<NCAP> &br = wm.b[lv];
    const int z = N - build_bits<NCAP, NT>(br, seq, N, lv, 0, scan);
    if (threadIdx.x == 0)
      wm.z[lv] = z;
    int pos[IPT], val[IPT];
#pragma unroll
    for (int j = 0; j < IPT; ++j) { // 稳定划分: 0 → 前面第 rank0(i) 位, 1 → z + rank1(i)
      const int i = threadIdx.x * IPT + j;
      const int v = i < N ? seq[i] : 0;
      const int r1 = i < N ? br.rank1(i) : 0;
      val[j] = v;
      pos[j] = ((v >> lv) & 1) ? z + r1 : i - r1;
    }
    __syncthreads(); // 全部读完再原地散写
#pragma unroll
    for (int j = 0; j < IPT; ++j) {
      const int i = threadIdx.x * IPT + j;
      if (i < N)
        seq[pos[j]] = static_cast<uint16_t>(val[j]);
    }
    __syncthreads();
  }
}

// 窗 [l,r) 内 "值 < th" 的有效样本数 (无效样本名次 ≥ nvalid, 二分只在有效段)
template <int NCAP>
__device__ __forceinline__ int count_below(const WMat<NCAP> &wm, const uint32_t *sorted, int nvalid, int l, int r, int n, float th) {
  const int v = lower_bound(sorted, nvalid, fkey(th));
  return v >= nvalid ? n : wm.count_less(l, r, v);
}

// pct rank (并列均秩). 相对型: x_t 无效则无效 (x_t 有效 ⇒ n ≥ 1); 全并列 → 0.5
struct HRank {
  template <int NCAP>
  __device__ static void emit(const WMat<NCAP> &wm, const uint32_t *sorted, int nvalid, int l, int r, int n, float x, bool xin, const Param &, float &v, bool &m) {
    m = xin;
    if (!m)
      return;
    const float lo = funkey(sorted[wm.quantile(l, r, 1)]), hi = funkey(sorted[wm.quantile(l, r, n)]);
    if (!(hi > lo)) {
      v = 0.5f;
      return;
    }
    const int b = dev::bin_of_fast(x, lo, hi); // 与 bin_of 逐位一致 (见 dev::bin_of_fast)
    const int less = b == 0 ? 0 : count_below(wm, sorted, nvalid, l, r, n, bin_edge(b, lo, hi));
    const int le = b == kBuckets - 1 ? n : count_below(wm, sorted, nvalid, l, r, n, bin_edge(b + 1, lo, hi));
    v = dev::pct_of(less, le - less, n);
  }
};
// k 分位 (k = 0.5 即中位): 第 ⌈k·n⌉ 小的值所在桶的中心; 空窗 → 0; 全并列 / k ≤ 0 → lo; k ≥ 1 → hi (同 CPU hist_quantile)
struct HQuantile {
  template <int NCAP>
  __device__ static void emit(const WMat<NCAP> &wm, const uint32_t *sorted, int, int l, int r, int n, float, bool, const Param &p, float &v, bool &m) {
    m = true;
    if (n < 1) {
      v = 0.f;
      return;
    }
    const float lo = funkey(sorted[wm.quantile(l, r, 1)]), hi = funkey(sorted[wm.quantile(l, r, n)]);
    if (!(hi > lo) || p.k <= 0.f) {
      v = lo;
      return;
    }
    if (p.k >= 1.f) {
      v = hi;
      return;
    }
    // ⌈k·n⌉ 必须在 double 下取整: float 的末位会把边界样本推到邻桶, 与 host 差一个桶
    const int tg = max(1, static_cast<int>(ceil(static_cast<double>(p.k) * n)));
    const float q = funkey(sorted[wm.quantile(l, r, tg)]);
    v = dev::bin_center(dev::bin_of_fast(q, lo, hi), lo, hi);
  }
};

template <int NCAP>
struct HistSh {       // 建完后的常驻结构 (与排序暂存共用一段 smem: 排完才建)
  uint16_t seq[NCAP]; // 名次序列 (建树时原地划分)
  uint16_t rk[NCAP];  // 时间位 → 名次
  WMat<NCAP> wm;
  BitRank<NCAP> vb; // 有效位
};

// MODE 0 = EXPAND (块 = 一段, 窗 = [段首, t]), 1 = ROLL (块长 C = NCAP − d + 1; 窗 = [max(0, t−d+1), t], 未满窗即前缀)
template <int MODE, int NCAP, int NT, class Op>
__global__ void __launch_bounds__(NT, (NCAP <= 256 ? 4 : (NCAP <= 2048 ? 2 : 1))) hist(const float *xv, const uint8_t *xm, float *ov, uint8_t *om, int T, int A, int d, int C, Param p) {
  constexpr int IPT = NCAP / NT;
  static_assert(NCAP % NT == 0 && NCAP <= 65536, "块容量须为线程数整数倍, 且名次放得进 uint16");
  using Sort = cub::BlockRadixSort<uint32_t, NT, IPT, uint16_t>;
  using Scan = cub::BlockScan<int, NT>;
  __shared__ union {
    typename Sort::TempStorage sort;
    HistSh<NCAP> h;
  } sh;
  __shared__ typename Scan::TempStorage scan;
  __shared__ uint32_t sorted[NCAP];
  __shared__ int s_nvalid;

  const int a = blockIdx.x;
  const int c0 = blockIdx.y * C;
  if (c0 >= T) // 块一致
    return;
  const int cend = min(c0 + C, T);
  const int base0 = (MODE == 0) ? c0 : max(0, c0 - d + 1);
  const int N = cend - base0; // ≤ NCAP (启动方保证)
  const int tid = threadIdx.x;

  // ---- 1. 读 + 排序 (键, 时间位); 读不合并 (一列), 相邻 block 是相邻资产 → 同一行的 128B 线在 L2 里共享 ----
  uint32_t key[IPT];
  uint16_t idx[IPT];
#pragma unroll
  for (int j = 0; j < IPT; ++j) {
    const int i = tid * IPT + j;
    uint32_t kk = kBadKey;
    if (i < N) {
      const int g = (base0 + i) * A + a;
      if (xm[g])
        kk = fkey(xv[g]);
    }
    key[j] = kk;
    idx[j] = static_cast<uint16_t>(i);
  }
  Sort(sh.sort).Sort(key, idx); // 输出仍是 blocked 排布: 线程 tid 持有位置 tid·IPT + j
  __syncthreads();              // 排序暂存让位给 h
  if (tid == 0)
    s_nvalid = N;
#pragma unroll
  for (int j = 0; j < IPT; ++j) {
    const int pos = tid * IPT + j;
    sorted[pos] = key[j];
    if (idx[j] < N) { // 补位项 (i ≥ N) 不进序列; 无效样本名次 ≥ nvalid 即可, 不必 < N
      sh.h.rk[idx[j]] = static_cast<uint16_t>(pos);
      sh.h.seq[idx[j]] = static_cast<uint16_t>(pos);
    }
  }
  __syncthreads();
#pragma unroll
  for (int j = 0; j < IPT; ++j) { // nvalid = 首个无效键的位置 (边界唯一); 全有效 → N
    const int pos = tid * IPT + j;
    if (key[j] == kBadKey && (pos == 0 || sorted[pos - 1] != kBadKey))
      s_nvalid = pos;
  }
  __syncthreads();
  const int nvalid = s_nvalid;

  // ---- 2. wavelet matrix + 有效位向量 ----
  build_wm<NCAP, NT>(sh.h.wm, sh.h.seq, N, scan);
  build_bits<NCAP, NT>(sh.h.vb, sh.h.rk, N, -1, nvalid, scan);

  // ---- 3. 查询: 一线程一 t ----
  for (int t = c0 + tid; t < cend; t += NT) {
    const int i = t - base0, r = i + 1;
    const int l = (MODE == 0) ? 0 : max(0, i - d + 1);
    const int n = sh.h.vb.rank1(r) - sh.h.vb.rank1(l);
    const int ri = sh.h.rk[i];
    const bool xin = ri < nvalid;
    const float x = funkey(sorted[ri]);
    float v = 0.f;
    bool m = false;
    Op::emit(sh.h.wm, sorted, nvalid, l, r, n, x, xin, p, v, m);
    dev::store(ov, om, t * A + a, v, m);
  }
}

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
    y = g ? (seen ? (k * x + (1.f - k) * y) : x) : y; // 首个有效点播种, 无效点保持; 尚无有效点 → y = 0
    seen = seen || g;
    dev::store(ov, om, i, y, true);
  }
}

// ---- 启动配置 ----
inline dim3 g_pt(int T, int A) { return dim3(static_cast<unsigned>((A + kPB - 1) / kPB), static_cast<unsigned>(hmin(T, 512))); }
inline dim3 g_seg(int T, int A) { return dim3(static_cast<unsigned>((A + kTB - 1) / kTB), static_cast<unsigned>((T + kSegLen - 1) / kSegLen)); }
inline dim3 g_hist(int T, int A, int C) { return dim3(static_cast<unsigned>(A), static_cast<unsigned>((T + C - 1) / C)); } // 一 block 一 (资产, 块)
// HIST 启动: 模板函数里 if constexpr 才会丢弃另一支 (宏展开的非模板 run 里不丢弃, nvcc 会去找没实例化的核)
template <int MODE, class H>
inline void launch_hist(const float *xv, const uint8_t *xm, float *ov, uint8_t *om, int T, int A, const Param &p, cudaStream_t stream) {
  static_assert(kSegLen <= 256, "EXPAND 块容量 256 须盖住一段");
  if constexpr (MODE == 0) {
    hist<0, 256, 256, H><<<g_hist(T, A, kSegLen), 256, 0, stream>>>(xv, xm, ov, om, T, A, 0, kSegLen, p);
  } else {
    assert(p.d >= 1 && p.d <= T && p.d <= 4096 - 1024 && "ROLL 块容量 4096 下 d 过大");
    if (p.d <= 1024) {
      const int C = 2048 - p.d + 1;
      hist<1, 2048, 512, H><<<g_hist(T, A, C), 512, 0, stream>>>(xv, xm, ov, om, T, A, p.d, C, p);
    } else {
      const int C = 4096 - p.d + 1;
      hist<1, 4096, 512, H><<<g_hist(T, A, C), 512, 0, stream>>>(xv, xm, ov, om, T, A, p.d, C, p);
    }
  }
}
inline int roll_chunk(int d) { return kSegLen * ((d + kSegLen - 1) / kSegLen); } // 块长 ≥ d: 预热开销 ≤ 一趟
inline int ext_chunk(int d) { return d * hmax(1, (4 * kSegLen) / d); }           // d 的倍数 (块界对齐 d-块界), ≈ 4 段长
inline dim3 g_roll(int T, int A, int C) { return dim3(static_cast<unsigned>((A + kTB - 1) / kTB), static_cast<unsigned>((T + C - 1) / C)); }

} // namespace k

// =============================================================================
// 算子: 统一签名 (未用到的指针传 nullptr, 全部设备指针); 怎么算是本后端私事, 不进 OpTable
// =============================================================================

// ---- POINT (21) ----
#define FACTOR_TS_POINT(Name, NARY, BODY)                                                                                                                                             \
  struct Name {                                                                                                                                                                       \
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
FACTOR_TS_POINT(TsSqrt, 1, { v = copysignf(sqrtf(fabsf(x.v)), x.v); m = x.m; })
FACTOR_TS_POINT(TsRelu, 1, { v = fmaxf(0.f, x.v); m = x.m; })
FACTOR_TS_POINT(TsNeg, 1, { v = -x.v; m = x.m; })
FACTOR_TS_POINT(TsRecip, 1, { v = x.v != 0.f ? 1.f / x.v : 0.f; m = x.m; }) // x = 0 → 0; 溢出 → 出口转无效
FACTOR_TS_POINT(TsGt, 1, { v = x.v > p.k ? 1.f : 0.f; m = x.m; })    // 指示; 套 Sum 窗 = 计数, 套 Mean 窗 = 占比
FACTOR_TS_POINT(TsClip, 1, { assert(p.k >= 0.f); v = fminf(fmaxf(x.v, -p.k), p.k); m = x.m; })
FACTOR_TS_POINT(TsTodMask, 0, { v = (t_seg >= static_cast<int>(p.k) && t_seg < static_cast<int>(p.k2)) ? 1.f : 0.f; m = true; }) // 元数 0: 不读输入, 纯 t 坐标
FACTOR_TS_POINT(TsAdd, 2, { v = x.v + y.v; m = x.m && y.m; })
FACTOR_TS_POINT(TsSub, 2, { v = x.v - y.v; m = x.m && y.m; })
FACTOR_TS_POINT(TsMul, 2, { v = x.v * y.v; m = x.m && y.m; })
FACTOR_TS_POINT(TsDiv, 2, { v = y.v != 0.f ? x.v / y.v : 0.f; m = x.m && y.m; }) // y = 0 → 0; 溢出 → 出口转无效
FACTOR_TS_POINT(TsMax, 2, { v = fmaxf(x.v, y.v); m = x.m && y.m; })
FACTOR_TS_POINT(TsMin, 2, { v = fminf(x.v, y.v); m = x.m && y.m; })
// Imb / Share: 和相消 (双边皆 0 / x ≈ −y) → 0 / 0.5
FACTOR_TS_POINT(TsImb, 2, { v = dev::den_ok(x.v + y.v, fabsf(x.v) + fabsf(y.v)) ? (x.v - y.v) / (x.v + y.v) : 0.f; m = x.m && y.m; })
FACTOR_TS_POINT(TsShare, 2, { v = dev::den_ok(x.v + y.v, fabsf(x.v) + fabsf(y.v)) ? x.v / (x.v + y.v) : 0.5f; m = x.m && y.m; })
FACTOR_TS_POINT(TsLogRatio, 2, { v = dev::log_ratio(x.v, y.v); m = x.m && y.m; }) // 非正侧饱和到 ∓kLogCap
FACTOR_TS_POINT(TsMask, 2, { v = x.v; m = x.m && y.m && y.v > 0.f; })     // y 当掩码, y ≤ 0 → 无效 (不补 0)
FACTOR_TS_POINT(TsWhere, 3, { v = x.v > 0.f ? y.v : z.v; m = x.m && (x.v > 0.f ? y.m : z.m); })

// ---- GATHER (2): d 只改地址 ----
#define FACTOR_TS_GATHER(Name, DELTA)                                                                                                                                                 \
  struct Name {                                                                                                                                                                       \
    static size_t workspace(int, int, const Param &) { return 0; }                                                                                                                    \
    static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p, void *, \
                    cudaStream_t stream) {                                                                                                                                            \
      assert(p.d >= 1 && p.d <= T);                                                                                                                                                   \
      k::gather<DELTA><<<k::g_pt(T, A), kPB, 0, stream>>>(xv, xm, ov, om, T, A, p.d);                                                                                                 \
      FACTOR_CUDA_OK(cudaGetLastError());                                                                                                                                             \
    }                                                                                                                                                                                 \
  };
FACTOR_TS_GATHER(TsDelayRoll, false) // x_{max(t−d,0)}, m = xm[max(t−d,0)]
FACTOR_TS_GATHER(TsDeltaRoll, true)  // x_t − x_{max(t−d,0)}

// ---- EXPAND (SCAN 族): 段内 expanding, 段界 reset ----
#define FACTOR_TS_EXPAND(Name, Fn)                                                                                                                                                \
  struct Name {                                                                                                                                                                   \
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
FACTOR_TS_EXPAND(TsCovCum, FnCov)
FACTOR_TS_EXPAND(TsCorrCum, FnCorr)
FACTOR_TS_EXPAND(TsBetaCum, FnBeta)
FACTOR_TS_EXPAND(TsResidCum, FnResid)
FACTOR_TS_EXPAND(TsWMeanCum, FnWMean)

// ---- ROLL (SCAN 族): 块内预热 + 滑窗 add/sub, 跨段不 reset ----
#define FACTOR_TS_ROLL(Name, Fn)                                                                                                                                                  \
  struct Name {                                                                                                                                                                   \
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
FACTOR_TS_ROLL(TsCovRoll, FnCov)
FACTOR_TS_ROLL(TsCorrRoll, FnCorr)
FACTOR_TS_ROLL(TsBetaRoll, FnBeta)
FACTOR_TS_ROLL(TsResidRoll, FnResid)
FACTOR_TS_ROLL(TsWMeanRoll, FnWMean)

// ---- EXTREME (4): 滑窗极值 / 距今期数 ----
//   workspace = 每时间块一份环形后缀区 [d][A]: gridY·d·A·sizeof(E). T=19200 A=5000 d=240: 20×240×5000×8B = 192 MB (pair 版)
#define FACTOR_TS_EXTREME(Name, MAXOP, ELEM)                                                                                                                                  \
  struct Name {                                                                                                                                                               \
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

// ---- HIST (3): EXPAND 一段一块 (容量 256); ROLL 块长 C = NCAP − d + 1, 容量按 d 选 2048 / 4096 (见第 6 节) ----
#define FACTOR_TS_HIST(Name, MODE, H)                                                                                                                                                 \
  struct Name {                                                                                                                                                                       \
    static size_t workspace(int, int, const Param &) { return 0; }                                                                                                                    \
    static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p, void *, \
                    cudaStream_t stream) {                                                                                                                                            \
      assert(static_cast<long long>(T) * A < (1LL << 31));                                                                                                                            \
      k::launch_hist<MODE, k::H>(xv, xm, ov, om, T, A, p, stream);                                                                                                                    \
      FACTOR_CUDA_OK(cudaGetLastError());                                                                                                                                             \
    }                                                                                                                                                                                 \
  };
FACTOR_TS_HIST(TsRankCum, 0, HRank)
FACTOR_TS_HIST(TsRankRoll, 1, HRank)
FACTOR_TS_HIST(TsQuantileRoll, 1, HQuantile)

// ---- EXPO (1) ----
struct TsMeanEma {
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
