#pragma once

// =============================================================================
// Search 评估算子的 GPU 后端 (CUDA, sm_75 / RTX 2060 6GB / 336 GB·s⁻¹; nvcc)
// =============================================================================
//   契约 factor/Search/Contract.hpp + Stat/Contract.hpp; 设备侧公式用 Stat/Dev.cuh (Stat/Contract 的 __device__ 逐字版).
//   与 Stat/Gpu.cuh 的核互不可见, 两边各自极致: Stat 一因子一次要 Row; Search 一批候选 C 张平面只要 Accum, 同 (t, h) 的标签行被 C 个候选复用.
//
//   【布局】候选 c 的 r16 平面 rx[c][T][A] (u16, 工作区); 标签 / 成本 / 池 [T][A] 一份; 候选的 (值, 掩码) 平面是 DAG 根槽, 只传指针 (Batch).
//   【三个核】
//     rank_batch   grid (T, C) 一行一 block: 片上 cub 基数排序 → 二分 less/eq → r16 (CS). ITEMS 按 A 分 2 / 6 / 10 三档.
//                  同一个核给标签预处理用 (SrcH, C = 1). TS 走 quant_batch 逐格量化 (grid.y = c).
//     stat_batch   grid (⌈C/kSW⌉, NB, H), block kSB = kSW 个 warp, **一 warp 一候选**, 一块管 kRows 行 × 一个持有期 (grid.z):
//                  每行: rank-AC (lag = h, 整数五和 64 位 warp 归约) + 标签侧 (每 lane 独占 smem 分组槽 [组][lane], 32 lane 落 32 bank;
//                  整数五和 / 浮点和 warp 蝶形归约; 组槽由 lane q 沿 32 lane 串行加) → lane 0 finish_row → Row → **直接折进 warp 私有的
//                  分片累加器** (lane 0 攒标量, lane k < 20 攒组 k 的 Σgrp / Σcnt; 运算与 Accum::add 逐字同) → 块末写 Part[c][h][tb].
//                  同 block 的 kSW 个候选同一 (t, h): ry / lv / sv 行一次进 L1 共用. 成本两格只在 h 序 0 的块里攒 (与持有期无关).
//                  核内没有块级同步, 没有原子.
//     reduce_parts 每 (c, h) 一 warp, 沿 tb 升序串行加 (固定序 → GPU 自身逐次确定) → Part[c][h]; 主机 to_result 搬成 Result.
//   【与 CPU 的差别】整数派生量逐位一致; 浮点和只差求和序 (对拍容差). 成本两格: 行内 float 归约, 跨行 double.
//   【禁用】--use_fast_math (pearson_int 的除法 / 开方须与 host 逐位一致).
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/Search/Contract.hpp"
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

namespace factor::gpu::search {

namespace dev = factor::gpu::stat::dev; // Stat/Dev.cuh: 契约公式的 __device__ 版

using factor::search::CostAcc;
using factor::search::Result;
using factor::stat::Accum;
using factor::stat::Frame;
using factor::stat::Holds;
using factor::stat::kGroups;
using factor::stat::kMaxA;
using factor::stat::kMaxHold;
using factor::stat::kNoKey;
using factor::stat::kRankMax;
using factor::stat::kRankNone;
using factor::stat::Row;
using factor::stat::Side;

inline constexpr int kMaxBatch = 128; // 一批候选上限 (Batch 走 kernel 参数区: 128 × 16 B = 2 KB)
inline constexpr int kRB = 512;       // rank_batch 一行一 block 的线程数 (每 SM 驻 2 块)
inline constexpr int kSB = 256;       // stat_batch 的 block 线程数: kSW 个 warp, 一 warp 一候选
inline constexpr int kSW = kSB / 32;  // stat_batch 每 block 的候选数
inline constexpr int kRows = 32;      // stat_batch 每 block 的行数 (分片粒度; NB = ⌈(T − t_lo)/kRows⌉)

// 候选批 (设备指针, 每张 [T][A])
struct Batch {
  int C = 0;
  const float *v[kMaxBatch];
  const uint8_t *m[kMaxBatch];
};
// 一组标签 (设备指针, [T][A]); ry 由 prep_label 填
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
// 单一金额档的建仓冲击 (设备指针, [T][A])
struct Cost {
  const uint16_t *buy = nullptr;
  const uint16_t *sell = nullptr;
  const uint8_t *m = nullptr;
};

// 分片 / 归约后的累加器 (设备格式): 全部 8 字节字, reduce_parts 按字类型 (整数 / double) 沿 tb 顺序加.
//   前 54 字 = Accum 的字段 (n.. 用 64 位), 后 4 字 = 成本两格 (只有 h 序 0 的分片有意义)
struct Part {
  long long n, npos, nlpos, nac;
  double sic, sic2, sic3, sic4, sls, sls2, smk, smk2, slm, sac;
  double sg[kGroups];
  long long sc[kGroups];
  double buy_hi, sell_lo;
  long long n_hi, n_lo;
};
inline constexpr int kPartWords = 4 + 10 + kGroups + kGroups + 2 + 2;
static_assert(sizeof(Part) == static_cast<size_t>(kPartWords) * 8, "Part 必须是紧凑的 8 字节字数组");
__host__ __device__ constexpr bool part_word_is_int(int w) {
  return w < 4 || (w >= 14 + kGroups && w < 14 + 2 * kGroups) || w >= kPartWords - 2;
}

namespace k {

// ---- 取键器: (候选, 格) → 保序键 / kNoKey ----
struct SrcB { // 候选批, 只看池内 g ∧ m 的格
  Batch B;
  const uint8_t *g;
  __device__ __forceinline__ bool on(int c, size_t i) const { return (B.m[c][i] & g[i]) != 0; }
  __device__ __forceinline__ unsigned key(int c, size_t i) const { return on(c, i) ? dev::ord(B.v[c][i]) : kNoKey; }
};
struct SrcH { // 标签平面 (fp16 位), 单张
  const uint16_t *v;
  const uint8_t *m;
  __device__ __forceinline__ unsigned key(int, size_t i) const { return m[i] ? dev::ord(dev::h2f(v[i])) : kNoKey; }
};

// =============================================================================
// 1. rank_batch: grid (T, C) 一行一 block, 片上基数排序 → 二分 less/eq → r16 → out[c][t][·]
// =============================================================================
template <int ITEMS, class Src>
__global__ void __launch_bounds__(kRB) rank_batch(Src src, uint16_t *out, int T, int A) {
  constexpr int N = kRB * ITEMS;
  using Sort = cub::BlockRadixSort<unsigned, kRB, ITEMS, unsigned short>;
  union Sh { // 排序暂存与排好序的数组分时复用同一块 shared
    typename Sort::TempStorage sort;
    struct {
      unsigned key[N];
      unsigned short idx[N];
    } s;
  };
  __shared__ Sh sh;
  const int t = blockIdx.x, c = blockIdx.y;
  const size_t base = static_cast<size_t>(t) * A;
  const size_t obase = (static_cast<size_t>(c) * T + t) * A;
  unsigned keys[ITEMS];
  unsigned short idx[ITEMS];
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    const int a = threadIdx.x * ITEMS + j; // blocked 排布 (cub 要求)
    keys[j] = a < A ? src.key(c, base + a) : kNoKey;
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
  for (int p = threadIdx.x; p < N; p += kRB) {
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
    out[obase + a] = r;
  }
}

// =============================================================================
// 1b. quant_batch (TS): 分位直接量化, 逐格; grid.y = 候选
// =============================================================================
__global__ void __launch_bounds__(kRB) quant_batch(SrcB src, uint16_t *out, size_t plane) {
  const int c = blockIdx.y;
  uint16_t *o = out + static_cast<size_t>(c) * plane;
  const float *v = src.B.v[c];
  for (size_t i = static_cast<size_t>(blockIdx.x) * kRB + threadIdx.x; i < plane; i += static_cast<size_t>(gridDim.x) * kRB)
    o[i] = src.on(c, i) ? dev::r16_quant(v[i]) : kRankNone;
}

// =============================================================================
// 2. stat_batch: 一 warp 一候选, 一块 kRows 行 × 一个持有期 → 分片 Part[c][h][tb]
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

// 每 lane 独占的分组槽: 组和 float / 组计数 uint16 (≤ kMaxA), 布局 [warp][组][lane]; 槽 0 兼作 lane 0 ↔ lane k 的信箱
struct Slots {
  float gs[kSW][kGroups][32];
  uint16_t gc[kSW][kGroups][32];
};

__global__ void __launch_bounds__(kSB, 2) stat_batch(Frame f, const uint16_t *rx, int C, int T, int A, int t0_abs, int t_lo, Holds hd, LabelSet L,
                                                     Cost cost, int NB, Part *part) {
  using ull = unsigned long long;
  __shared__ Slots sh;
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int c = blockIdx.x * kSW + warp;
  if (c >= C) // warp 一致; 核内没有块级同步, 整 warp 直接退出安全
    return;
  const int hi = blockIdx.z, hold = hd.h[hi], h = dev::hold_minutes(hold);
  const Label lb = L.l[hi];
  const bool with_cost = hi == 0;
  const int tb = blockIdx.y, t_beg = t_lo + tb * kRows, t_end = min(T, t_beg + kRows);
  const size_t plane = static_cast<size_t>(T) * A;
  const uint16_t *rxc = rx + static_cast<size_t>(c) * plane;
  float (*gs)[32] = sh.gs[warp];
  uint16_t (*gc)[32] = sh.gc[warp];
  constexpr int kt = kGroups - 1;

  // warp 私有分片累加器 (= Accum::add 的字段): lane 0 攒标量 + 成本, lane k < kGroups 攒组 k
  long long an = 0, anpos = 0, anlpos = 0, anac = 0;
  double asic = 0.0, asic2 = 0.0, asic3 = 0.0, asic4 = 0.0, asls = 0.0, asls2 = 0.0, asmk = 0.0, asmk2 = 0.0, aslm = 0.0, asac = 0.0;
  double asg = 0.0;
  long long asc = 0;
  double abuy = 0.0, asell = 0.0;
  long long anh = 0, anl = 0;

  for (int t = t_beg; t < t_end; ++t) {
    const size_t base = static_cast<size_t>(t) * A;
    const uint16_t *xr = rxc + base;
    Row r; // lane 0 收尾用
    // ---- rank-AC (lag = h) ----
    if (t >= h) {
      const uint16_t *xl = xr - static_cast<size_t>(h) * A;
      ull ac[6] = {0, 0, 0, 0, 0, 0}; // n, Σx, Σy, Σx², Σy², Σxy
      for (int a = lane; a < A; a += 32) {
        const unsigned x = xr[a], y = xl[a];
        const bool ok = x != kRankNone && y != kRankNone;
        ac[0] += ok;
        ac[1] += ok ? x : 0u;
        ac[2] += ok ? y : 0u;
        ac[3] += ok ? x * x : 0u; // r16 ≤ 65534: 积 < 2³², 32 位乘后再加进 64 位
        ac[4] += ok ? y * y : 0u;
        ac[5] += ok ? x * y : 0u;
      }
      warp_sum(ac);
      if (lane == 0) {
        bool ok = false;
        r.ac = dev::pearson_int(ac[0], ac[1], ac[2], ac[3], ac[4], ac[5], ok);
        r.ok_ac = ok ? 1 : 0;
      }
    }
    // ---- 标签侧 ----
    const bool tail = dev::tail_masked(t0_abs + t, hold);
    if (!tail) {
#pragma unroll
      for (int q = 0; q < kGroups; ++q) { // 清自己的槽
        gs[q][lane] = 0.f;
        gc[q][lane] = 0;
      }
      const uint16_t *ry = lb.ry + base, *lv = lb.lv + base, *sv = lb.sv + base;
      ull s[6] = {0, 0, 0, 0, 0, 0}; // n, Σx, Σy, Σx², Σy², Σxy (IC)
      float fs[3] = {};              // Σlv (mkt), Σsv, Σsv|组0
      for (int a = lane; a < A; a += 32) {
        const unsigned x = xr[a], y = ry[a];
        const bool ok = x != kRankNone && y != kRankNone;
        s[0] += ok;
        s[1] += ok ? x : 0u;
        s[2] += ok ? y : 0u;
        s[3] += ok ? x * x : 0u;
        s[4] += ok ? y * y : 0u;
        s[5] += ok ? x * y : 0u;
        const float yl = dev::h2f(lv[a]), ys = dev::h2f(sv[a]);
        fs[0] += ok ? yl : 0.f;
        fs[1] += ok ? ys : 0.f;
        const int g = ok ? dev::grp_of(x) : -1;
        if (ok) { // 自己的槽, 无竞争
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
      if (lane == 0) {
        double gsd[kGroups];
        int gci[kGroups];
#pragma unroll
        for (int q = 0; q < kGroups; ++q) {
          gsd[q] = static_cast<double>(gs[q][0]);
          gci[q] = gc[q][0];
        }
        const Side top{static_cast<double>(gci[kt]), gsd[kt], 0.0}, bot{static_cast<double>(gci[0]), gsd[0], static_cast<double>(fs[2])};
        dev::finish_row(f, s[0], s[1], s[2], s[3], s[4], s[5], static_cast<double>(fs[0]), static_cast<double>(fs[1]), gsd, gci, top, bot, r);
        if (r.ok) { // 组超额和回填槽 0 (计数不变 = r.cnt), lane q 取走攒进分片
#pragma unroll
          for (int q = 0; q < kGroups; ++q)
            gs[q][0] = r.grp[q];
        }
      }
      __syncwarp();
    }
    // ---- 折进分片 (与 Accum::add 逐字同) ----
    const bool ok = __shfl_sync(0xffffffffu, static_cast<int>(r.ok), 0) != 0;
    if (ok && lane < kGroups) {
      asg += static_cast<double>(gs[lane][0]);
      asc += gc[lane][0];
    }
    if (lane == 0) {
      if (r.ok) {
        ++an;
        anpos += r.ic > 0.f;
        anlpos += r.ls > 0.f;
        const double ic = r.ic, ic2 = ic * ic, ls = r.ls, mk = r.mkt;
        asic += ic, asic2 += ic2, asic3 += ic2 * ic, asic4 += ic2 * ic2;
        asls += ls, asls2 += ls * ls, asmk += mk, asmk2 += mk * mk, aslm += ls * mk;
      }
      if (r.ok_ac) {
        ++anac;
        asac += r.ac;
      }
    }
    // ---- 成本两格 (只 h 序 0; rx 有效 ∧ 成本有效, 不看 ok / 尾部) ----
    if (with_cost) {
      const uint8_t *cm = cost.m + base;
      const uint16_t *cb = cost.buy + base, *cs = cost.sell + base;
      float cv[2] = {0.f, 0.f}; // Σ买|顶组, Σ卖|底组
      int cn[2] = {0, 0};
      for (int a = lane; a < A; a += 32) {
        const unsigned x = xr[a];
        const bool okc = x != kRankNone && cm[a] != 0;
        const int g = okc ? dev::grp_of(x) : -1;
        cv[0] += g == kt ? dev::h2f(cb[a]) : 0.f;
        cv[1] += g == 0 ? dev::h2f(cs[a]) : 0.f;
        cn[0] += g == kt;
        cn[1] += g == 0;
      }
      warp_sum(cv);
      warp_sum(cn);
      if (lane == 0) {
        abuy += static_cast<double>(cv[0]), asell += static_cast<double>(cv[1]);
        anh += cn[0], anl += cn[1];
      }
    }
    __syncwarp(); // 下一行清槽前, lane 0 / lane k 读完
  }

  // ---- 写分片 (每字段恰由一个 lane 写满) ----
  Part &P = part[(static_cast<size_t>(c) * hd.n + hi) * NB + tb];
  if (lane < kGroups) {
    P.sg[lane] = asg;
    P.sc[lane] = asc;
  }
  if (lane == 0) {
    P.n = an, P.npos = anpos, P.nlpos = anlpos, P.nac = anac;
    P.sic = asic, P.sic2 = asic2, P.sic3 = asic3, P.sic4 = asic4;
    P.sls = asls, P.sls2 = asls2, P.smk = asmk, P.smk2 = asmk2, P.slm = aslm, P.sac = asac;
    P.buy_hi = abuy, P.sell_lo = asell, P.n_hi = anh, P.n_lo = anl;
  }
}

// =============================================================================
// 3. reduce_parts: 每 (c, h) 一 warp, lane 管字 w = lane, lane + 32, 沿 tb 升序串行加 → out[c·H + h]
// =============================================================================
__global__ void __launch_bounds__(kSB) reduce_parts(const Part *part, int NB, int P, Part *out) {
  const int lane = threadIdx.x & 31, warp = threadIdx.x >> 5;
  const int p = blockIdx.x * kSW + warp;
  if (p >= P)
    return;
  const long long *src = reinterpret_cast<const long long *>(part + static_cast<size_t>(p) * NB);
  long long *dst = reinterpret_cast<long long *>(out + p);
  for (int w = lane; w < kPartWords; w += 32) {
    if (part_word_is_int(w)) {
      long long s = 0;
      for (int tb = 0; tb < NB; ++tb)
        s += src[static_cast<size_t>(tb) * kPartWords + w];
      dst[w] = s;
    } else {
      double s = 0.0;
      for (int tb = 0; tb < NB; ++tb)
        s += __longlong_as_double(src[static_cast<size_t>(tb) * kPartWords + w]);
      dst[w] = __double_as_longlong(s);
    }
  }
}

} // namespace k

// =============================================================================
// 宿主接口 (全部设备指针; 拷入拷出由调用方负责 —— 搜索侧候选平面常驻显存)
// =============================================================================

// 一次 eval 的形状与工作区尺寸 (调用方按它分配: rx / part / out)
struct Work {
  int C = 0, T = 0, A = 0, H = 0, t_lo = 0, NB = 0;
  size_t plane() const { return static_cast<size_t>(T) * A; }
  size_t rx_elems() const { return static_cast<size_t>(C) * plane(); }
  size_t part_elems() const { return static_cast<size_t>(C) * H * NB; }
  size_t out_elems() const { return static_cast<size_t>(C) * H; }
};
inline Work plan(int C, int T, int A, int H, int t_lo) {
  assert(C >= 1 && C <= kMaxBatch && T >= 1 && A >= 1 && A <= kMaxA && static_cast<long long>(T) * A < (1LL << 31));
  assert(H >= 1 && H <= kMaxHold && t_lo >= 0 && t_lo < T);
  Work w;
  w.C = C, w.T = T, w.A = A, w.H = H, w.t_lo = t_lo;
  w.NB = (T - t_lo + kRows - 1) / kRows;
  return w;
}

template <class Src>
inline void rank_launch(const Src &src, uint16_t *out, int T, int A, int C, cudaStream_t stream) {
  assert(T >= 1 && A >= 1 && A <= kMaxA && C >= 1 && C <= kMaxBatch);
  const dim3 grid(static_cast<unsigned>(T), static_cast<unsigned>(C));
  if (A <= kRB * 2)
    k::rank_batch<2, Src><<<grid, kRB, 0, stream>>>(src, out, T, A);
  else if (A <= kRB * 6)
    k::rank_batch<6, Src><<<grid, kRB, 0, stream>>>(src, out, T, A);
  else
    k::rank_batch<10, Src><<<grid, kRB, 0, stream>>>(src, out, T, A);
  FACTOR_CUDA_OK(cudaGetLastError());
}

// 标签预处理: 做多标签逐行 r16 → ry. 块内一次, 候选共用
inline void prep_label(const uint16_t *lv, const uint8_t *m, int T, int A, uint16_t *ry, cudaStream_t stream) {
  rank_launch(k::SrcH{lv, m}, ry, T, A, 1, stream);
}

// 一批候选的评估: B (口径 f, 只看池内 g ∧ m) + 标签集 L + 成本 → out[c·H + h] (设备 Part; 成本在 h = 0 那份). 契约【分块流式】的 t0_abs / t_lo
inline void eval(const Batch &B, const uint8_t *g, Frame f, const Work &w, int t0_abs, const Holds &hd, const LabelSet &L, const Cost &cost,
                 uint16_t *rx, Part *part, Part *out, cudaStream_t stream) {
  factor::stat::assert_holds(hd);
  assert(B.C == w.C && hd.n == w.H && L.n == hd.n && g && rx && part && out && t0_abs >= 0);
  assert(cost.buy && cost.sell && cost.m);
  for (int c = 0; c < B.C; ++c)
    assert(B.v[c] && B.m[c]);
  for (int i = 0; i < L.n; ++i)
    assert(L.l[i].lv && L.l[i].sv && L.l[i].m && L.l[i].ry && "标签未预处理 (prep_label)");
  const k::SrcB src{B, g};
  if (f == Frame::CS) {
    rank_launch(src, rx, w.T, w.A, w.C, stream);
  } else {
    const size_t n = w.plane();
    const dim3 grid(static_cast<unsigned>(hmin(static_cast<int>((n + kRB - 1) / kRB), 4096)), static_cast<unsigned>(w.C));
    k::quant_batch<<<grid, kRB, 0, stream>>>(src, rx, n);
    FACTOR_CUDA_OK(cudaGetLastError());
  }
  const dim3 grid(static_cast<unsigned>((w.C + kSW - 1) / kSW), static_cast<unsigned>(w.NB), static_cast<unsigned>(w.H));
  k::stat_batch<<<grid, kSB, 0, stream>>>(f, rx, w.C, w.T, w.A, t0_abs, w.t_lo, hd, L, cost, w.NB, part);
  FACTOR_CUDA_OK(cudaGetLastError());
  const int P = w.C * w.H;
  k::reduce_parts<<<(P + kSW - 1) / kSW, kSB, 0, stream>>>(part, w.NB, P, out);
  FACTOR_CUDA_OK(cudaGetLastError());
}

// 归约后的 H 份 Part (已拷回宿主, 候选 c 的 out + c·H) → Result
inline void to_result(const Part *o, int H, Result &r) {
  assert(o && H >= 1 && H <= kMaxHold);
  r = Result{};
  for (int i = 0; i < H; ++i) {
    const Part &p = o[i];
    Accum &a = r.acc[i];
    a.n = static_cast<int>(p.n), a.npos = static_cast<int>(p.npos), a.nlpos = static_cast<int>(p.nlpos), a.nac = static_cast<int>(p.nac);
    a.sic = p.sic, a.sic2 = p.sic2, a.sic3 = p.sic3, a.sic4 = p.sic4;
    a.sls = p.sls, a.sls2 = p.sls2, a.smk = p.smk, a.smk2 = p.smk2, a.slm = p.slm, a.sac = p.sac;
    for (int q = 0; q < kGroups; ++q)
      a.sg[q] = p.sg[q], a.sc[q] = p.sc[q];
  }
  r.cost.buy_hi = o[0].buy_hi, r.cost.sell_lo = o[0].sell_lo;
  r.cost.n_hi = o[0].n_hi, r.cost.n_lo = o[0].n_lo;
}

} // namespace factor::gpu::search
