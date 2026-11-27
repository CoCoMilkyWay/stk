#pragma once

// =============================================================================
// Stat 行核的设备版 (CUDA, sm_75; 契约 factor/Stat/Contract.hpp): 一行的 rank 核 / 一 warp 一 (t, hold) 的 Row
// =============================================================================
//   Stat/Gpu.cuh 与 Search/Gpu.cuh 两个 GPU 算子共用, 与 Stat/Host.hpp 对仗 (同一组行核同名同参序, 一边宿主一边设备).
//   契约公式 (ord / r16 / grp_of / tail / pair_add / pearson_int / finish_row) 是 FACTOR_HD, 设备直接调主机那一份, 这里不复刻;
//   视图 (Plane / Batch / Labels / Span) 也是契约那一份.
//
//   【布局】SoA 行主序 [T][A]; 一行 = 一个截面. 候选批 Batch = C 张 Plane (kernel 参数区, 128 × 16 B = 2 KB);
//           r16 工作区 rx[c][T][A] (u16). Stat 是 C = 1 的特例.
//   【rank_rows】grid (T, C) 一行一 block (kRB = 512 线程, 前三级每 SM 驻 2 块): 一行的 (保序键, 资产号) 进 cub::BlockRadixSort (寄存器 + 片上),
//     排好写回 shared, 每个元素二分查 less / eq → r16. 无效键 = kNoKey 排最后, n = 首个 kNoKey 的位置.
//     ITEMS (每线程元素数) 按 A 分四级 2 / 6 / 10 / 16 (A ≤ 1024 / 3072 / 5120 / 8192), shared 6 / 18 / 30 / 48 KB;
//     末级 key + idx 正好顶满静态 shared 上限 48 KB (cub 排序暂存 ≈ 34 KB 与之 union), sm_75 每 SM 只驻 1 块.
//     同一个核给因子批 (SrcX: 池内 g ∧ m) 与标签 (SrcY: fp16 平面, C = 1) 用, 只换行源; 行源 row(c, 行首) 给出取键器
//     KeyX / KeyY (行视图, 与 Host.hpp 同名同形).
//   【quant_rows】TS: 分位直接量化 (r16_quant), 逐格无关, grid (·, C). 两核出口按方向 d 定向 (契约【方向 Dir】).
//   【row_stat】= 换手侧 lag_row + 标签侧 label_row (与 Host.hpp 同构; 尾部和在寄存器里攒, 下标编译期定). 一 warp 算一 (t, hold) 行 → Row, **全 lane 同值**
//     (收尾 finish_row 在 32 lane 冗余算: SIMT 下与 lane 0 独算同耗时,
//     省掉 lane 间搬运; 消费端 lane 0 落 Row 或 lane k 取 grp[k] 都直接从寄存器拿). 一行只有 A ≤ kMaxA 格, 块级归约是纯栅栏延迟瓶颈,
//     所以一遍 A 轴 lane 跨步 32, 累加器全在寄存器 + 每 lane **独占**的 smem 分组槽 (Slots, [warp][组][lane] 布局, 32 lane 落 32 连续字,
//     无 bank 冲突; 每格 1 LDS + 1 STS, 不展开 20 组 select): 整数五和 (rIC / rAC) 64 位 warp 蝶形归约 → 精确, 与 CPU 逐位一致;
//     标签浮点和 float 蝶形; 分组槽由 lane q 沿 32 lane 串行加 (旋转起点), 求和序固定 → GPU 自身逐次确定, 与 CPU 只差求和序 (对拍容差).
//     无 shared atomic, 无块级同步 (block 内各 warp 互不相干, 消费核可整 warp 提前退出).
//   【禁用】--use_fast_math: pearson_int 的除法 / 开方要与 host 逐位一致.
// =============================================================================

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Pos.hpp"

#include <cassert>
#include <cstdint>
#include <cub/cub.cuh>
#include <cuda_fp16.h>
#include <cuda_runtime.h>

// 无错误处理, 只断言: 越早炸越好 (与 TS/CS Gpu.cuh 的定义逐字同; 同一 TU 多头 include 只认一份)
#ifndef FACTOR_CUDA_OK
#define FACTOR_CUDA_OK(call)                      \
  do {                                            \
    const cudaError_t e_ = (call);                \
    assert(e_ == cudaSuccess && "CUDA 调用失败"); \
    (void)e_;                                     \
  } while (0)
#endif

namespace factor::gpu::stat {

using factor::stat::Batch;
using factor::stat::Frame;
using factor::stat::kEnds;
using factor::stat::kGroups;
using factor::stat::kMaxA;
using factor::stat::kMaxBatch;
using factor::stat::kNoKey;
using factor::stat::kRankNone;
using factor::stat::kSums;
using factor::stat::kTails;
using factor::stat::Label;
using factor::stat::Row;
using factor::stat::Span;

inline constexpr int kRB = 512; // rank_rows 一行一 block 的线程数 (ITEMS ≤ 10 每 SM 驻 2 块)
static_assert(kRB * 16 == kMaxA, "rank_rows 末级 (ITEMS = 16) 须恰好盖住 kMaxA");
inline constexpr int kWB = 256; // 行核 block 的线程数: kWarps 个 warp, 一 warp 一行
inline constexpr int kWarps = kWB / 32;

// 每 lane 独占的分组槽: 组和 float / 组计数 uint16 (≤ kMaxA), 布局 [warp][组][lane]
struct Slots {
  float gs[kWarps][kGroups][32];
  uint16_t gc[kWarps][kGroups][32];
};

namespace dev {

__device__ __forceinline__ float h2f(uint16_t b) { return __half2float(__ushort_as_half(b)); }
struct H2F {
  __device__ __forceinline__ float operator()(uint16_t b) const { return h2f(b); }
};

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

// 换手侧 (t−h → t): rx = 本行 r16, rlag = t−h 行 r16 → r.rac / r.ok_rac (全 lane 同值). 整数五和, 64 位 warp 蝶形 → 精确
__device__ __forceinline__ void lag_row(const uint16_t *rx, const uint16_t *rlag, int A, Row &r, int lane) {
  unsigned long long s[kSums] = {};
  for (int a = lane; a < A; a += 32) {
    const unsigned x = rx[a], y = rlag[a];
    factor::stat::pair_add(s, x, y, x != kRankNone && y != kRankNone);
  }
  warp_sum(s);
  bool ok = false;
  r.rac = factor::stat::pearson_int(s, ok);
  r.ok_rac = ok ? 1 : 0;
}

// 标签侧 (t → t+h): rx = 本行 r16, lb 已偏到行首 (交接链回看 / 前看同块其他行), c = 本行交接链, cap = 满仓, ret = 收益口径, mkt = 本行池基准
// → r 的 ric / mkt / grp / cnt / ok (全 lane 同值; 组与尾部按 Pos.hpp 一次 walk 出层级, 与 Host.hpp 同式). gs / gc = 本 warp 的分组槽,
// 连续调用间的槽复用由函数内首个 __syncwarp 保护. 调用方已排除段末尾部
__device__ __forceinline__ void label_row(const uint16_t *rx, const Label &lb, const factor::stat::Chain &c, double cap, factor::stat::Ret ret, double mkt, Row &r,
                                          float (*gs)[32], uint16_t (*gc)[32], int lane) {
  __syncwarp(); // 上一行的广播读 (gs[q][0]) 全部结束, 才能清槽
#pragma unroll
  for (int q = 0; q < kGroups; ++q) {
    gs[q][lane] = 0.f;
    gc[q][lane] = 0;
  }
  unsigned long long s[kSums] = {}; // rIC
  float ts[kEnds * kTails] = {};    // 尾部和 (下标编译期定, 留寄存器)
  int tc[kEnds * kTails] = {};      // 尾部只数
  for (int a = lane; a < c.A; a += 32) {
    const unsigned x = rx[a], y = lb.ry[a];
    const bool ok = x != kRankNone && y != kRankNone;
    factor::stat::pair_add(s, x, y, ok);
    if (!ok)
      continue;
    const int g = factor::stat::grp_of(x);
    float yv[2];
    const factor::stat::Pos p = factor::stat::cell(c, lb, a, factor::stat::InRank{rx + a, g}, H2F{}, yv);
    if (p.held < 0)
      continue;
    gs[g][lane] += factor::stat::ret_at(p, 0, yv); // 自己的槽, 无竞争
    gc[g][lane] += 1;
#pragma unroll
    for (int e = 0; e < kEnds; ++e) {
      if (g != factor::stat::end_group(e))
        continue;
#pragma unroll
      for (int j = 0; j < kTails; ++j)
        if (j < p.held) {
          ts[e * kTails + j] += factor::stat::ret_at(p, 1 + j, yv);
          tc[e * kTails + j] += 1;
        }
    }
  }
  warp_sum(s);
  warp_sum(ts);
  warp_sum(tc);
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
    gs[lane][0] = sg; // 总和放回 0 号槽
    gc[lane][0] = static_cast<uint16_t>(cg);
  }
  __syncwarp();
  double gsd[kGroups], tsd[kEnds][kTails];
  int gci[kGroups], tci[kEnds][kTails];
#pragma unroll
  for (int q = 0; q < kGroups; ++q) { // 广播读 (全 lane 同址, 无冲突)
    gsd[q] = static_cast<double>(gs[q][0]);
    gci[q] = gc[q][0];
  }
#pragma unroll
  for (int e = 0; e < kEnds; ++e)
#pragma unroll
    for (int j = 0; j < kTails; ++j)
      tsd[e][j] = static_cast<double>(ts[e * kTails + j]), tci[e][j] = tc[e * kTails + j];
  factor::stat::finish_row(s, mkt, gsd, gci, tsd, tci, cap, ret, r);
}

// 一 (t, hold) 完整行 (全 lane 同值): rlag = t−h 行 r16 (没有 → nullptr, 无 rAC); lb 已偏到行首; c = 本行交接链; cap = 满仓; ret = 收益口径;
// opens = 开仓行 (Hold::opens)
__device__ __forceinline__ Row row_stat(const uint16_t *rx, const uint16_t *rlag, const Label &lb, const factor::stat::Chain &c, double cap,
                                        factor::stat::Ret ret, bool opens, float (*gs)[32], uint16_t (*gc)[32], int lane) {
  Row r;
  if (rlag)
    lag_row(rx, rlag, c.A, r, lane);
  if (opens)
    label_row(rx, lb, c, cap, ret, lb.mk[c.t], r, gs, gc, lane);
  return r;
}

} // namespace dev

namespace k {

// ---- 取键器: 格 a (指针已偏到行首) → 保序键 / kNoKey (与 Host.hpp 同名同形) ----
struct KeyX { // 因子一行, 只看池内 g ∧ m 的格 (契约【截面池 g】); g = m 即不看池
  const float *v;
  const uint8_t *m, *g;
  __device__ __forceinline__ bool on(int a) const { return (m[a] & g[a]) != 0; }
  __device__ __forceinline__ unsigned key(int a) const { return on(a) ? factor::stat::ord(v[a]) : kNoKey; }
};
struct KeyY { // 做多标签一行 (fp16 位)
  const uint16_t *v;
  const uint8_t *m;
  __device__ __forceinline__ unsigned key(int a) const { return m[a] ? factor::stat::ord(dev::h2f(v[a])) : kNoKey; }
};
// ---- 行源: (候选, 行首格) → 取键器 ----
struct SrcX { // 候选批 + 池
  Batch B;
  const uint8_t *g;
  __device__ __forceinline__ KeyX row(int c, size_t base) const { return {B.x[c].v + base, B.x[c].m + base, g + base}; }
};
struct SrcY { // 做多标签平面, 单张
  const uint16_t *v;
  const uint8_t *m;
  __device__ __forceinline__ KeyY row(int, size_t base) const { return {v + base, m + base}; }
};

// rank_rows: grid (T, C) 一行一 block, 片上基数排序 → 二分 less/eq → r16 → out[c][t][·]
template <int ITEMS, class Src>
__global__ void __launch_bounds__(kRB) rank_rows(Src src, uint16_t *out, int T, int A, int d) {
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
  const auto key = src.row(c, base);
  unsigned keys[ITEMS];
  unsigned short idx[ITEMS];
#pragma unroll
  for (int j = 0; j < ITEMS; ++j) {
    const int a = threadIdx.x * ITEMS + j; // blocked 排布 (cub 要求)
    keys[j] = a < A ? key.key(a) : kNoKey;
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
      r = factor::stat::r16_of(less, eq, n);
    }
    out[obase + a] = factor::stat::oriented(r, d);
  }
}

// quant_rows (TS): 分位直接量化, 逐格 (整平面当一行: plane < 2^31); grid.y = 候选
__global__ void __launch_bounds__(kRB) quant_rows(SrcX src, uint16_t *out, size_t plane, int d) {
  const int c = blockIdx.y;
  uint16_t *o = out + static_cast<size_t>(c) * plane;
  const KeyX key = src.row(c, 0);
  for (size_t i = static_cast<size_t>(blockIdx.x) * kRB + threadIdx.x; i < plane; i += static_cast<size_t>(gridDim.x) * kRB) {
    const int a = static_cast<int>(i);
    o[i] = factor::stat::oriented(key.on(a) ? factor::stat::r16_quant(key.v[a]) : kRankNone, d);
  }
}

} // namespace k

// -----------------------------------------------------------------------------
// 宿主侧发射 (全部设备指针)
// -----------------------------------------------------------------------------

template <class Src>
inline void rank_launch(const Src &src, uint16_t *out, int T, int A, int C, int d, cudaStream_t stream) {
  assert(T >= 1 && A >= 1 && A <= kMaxA && C >= 1 && C <= kMaxBatch && static_cast<long long>(T) * A < (1LL << 31));
  const dim3 grid(static_cast<unsigned>(T), static_cast<unsigned>(C));
  if (A <= kRB * 2)
    k::rank_rows<2, Src><<<grid, kRB, 0, stream>>>(src, out, T, A, d);
  else if (A <= kRB * 6)
    k::rank_rows<6, Src><<<grid, kRB, 0, stream>>>(src, out, T, A, d);
  else if (A <= kRB * 10)
    k::rank_rows<10, Src><<<grid, kRB, 0, stream>>>(src, out, T, A, d);
  else
    k::rank_rows<16, Src><<<grid, kRB, 0, stream>>>(src, out, T, A, d);
  FACTOR_CUDA_OK(cudaGetLastError());
}

// 阶段 1: 候选批 B (口径 m.f, 方向 m.d, 只看池内 g ∧ 有效位) → 已定向的 rx[c][T][A] (块内全部行, 含前后带). 口径分叉 / 定向只在这里
inline void rank_x(const Batch &B, const uint8_t *g, const factor::stat::Mode &m, const Span &s, uint16_t *rx, cudaStream_t stream) {
  assert(s.T >= 1 && s.A >= 1 && s.A <= kMaxA);
  assert(B.C >= 1 && B.C <= kMaxBatch && g && rx);
  for (int c = 0; c < B.C; ++c)
    assert(B.x[c].v && B.x[c].m);
  const k::SrcX src{B, g};
  if (m.f == Frame::CS) {
    rank_launch(src, rx, s.T, s.A, B.C, m.d, stream);
    return;
  }
  const size_t plane = s.n();
  const size_t nb = (plane + kRB - 1) / kRB;
  const dim3 grid(static_cast<unsigned>(nb < 4096 ? nb : 4096), static_cast<unsigned>(B.C));
  k::quant_rows<<<grid, kRB, 0, stream>>>(src, rx, plane, m.d);
  FACTOR_CUDA_OK(cudaGetLastError());
}

// 标签预处理: 一组标签的做多侧 (lb.lv / lb.m) 逐行 r16 → ry. 常驻期 (块内) 算一次, 因子 / 候选共用
inline void prep_label(const Label &lb, const Span &s, uint16_t *ry, cudaStream_t stream) {
  assert(s.T >= 1 && s.A >= 1 && s.A <= kMaxA);
  assert(lb.lv && lb.m && ry);
  rank_launch(k::SrcY{lb.lv, lb.m}, ry, s.T, s.A, 1, factor::stat::kFwd, stream);
}

} // namespace factor::gpu::stat
