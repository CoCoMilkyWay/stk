#pragma once

// =============================================================================
// Stat/Contract.hpp 的设备侧逐字版 (__device__): Stat/Gpu.cuh 与 Search/Gpu.cuh 共用
// =============================================================================
//   这里只有"契约公式"的 __device__ 复刻 (ord / r16 / grp_of / ls_of / hold / tail / pearson_int / finish_row) 与两个 shared 二分;
//   不含任何核与布局. 公式一个字都不能改: 两后端对拍靠的就是它与主机版逐位一致 (整数派生量) / 只差求和序 (浮点和).
//   Stat 与 Search 两个 GPU 算子的核互不可见, 只经本头 + Stat/Contract.hpp 相见.
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/Stat/Contract.hpp"

#include <cassert>
#include <cstdint>
#include <cuda_fp16.h>
#include <cuda_runtime.h>

namespace factor::gpu::stat {

using factor::stat::Frame;
using factor::stat::kCloseAuction;
using factor::stat::kGroups;
using factor::stat::kRankMax;
using factor::stat::Row;
using factor::stat::Side;

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
__device__ __forceinline__ double ls_of(const Side &hi, const Side &lo, double mkt, double mkt_s) {
  return (hi.w > 0.0 ? hi.lv / hi.w - mkt : 0.0) + (lo.w > 0.0 ? lo.sv / lo.w - mkt_s : 0.0);
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
                                           const double *gs, const int *gc, const Side &top, const Side &bot, Row &r) {
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
  r.ls = static_cast<float>(dev::ls_of(top, bot, mkt, mkt_s)); // 显式限定: Side 在 factor::stat, ADL 会把主机版 ls_of 一起拉进来
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
} // namespace factor::gpu::stat
