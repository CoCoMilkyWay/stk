#pragma once

// =============================================================================
// 分桶直方图 —— **流式后端专用** (TS/Stream.hpp 与 CS/Stream.hpp 共用; 另两个后端各写各的)
// =============================================================================
//   实现的是 Contract.hpp 里"序统计族"的分桶规则: 样本集定 lo/hi, kBuckets 等宽桶,
//   rank / quantile 全由**整数桶计数**决定 → 三后端算法各异仍能逐位一致,
//   近似只相对于"真序统计" (分辨率 = 桶宽).
//   样本量 ≤ 段长 240 (EXPAND) / 窗长 d (ROLL) / 截面宽 A (CS), 直接缓存后建桶.
//   build 时顺带算 exclusive 前缀 → rank 是 O(1) (截面 A 次查询不必每次扫 256 桶).
// =============================================================================

#include "factor/Contract.hpp"

#include <algorithm>
#include <cmath>
#include <vector>

namespace factor::stream {

struct Hist {
  int cnt[kBuckets] = {};
  int pre[kBuckets + 1] = {}; // pre[b] = Σ_{j<b} cnt[j]; pre[kBuckets] = n
  float lo = 0, hi = 0;
  int n = 0;
  bool ok = false; // spread(lo, hi): 非全并列

  void build(const std::vector<float> &s) {
    n = static_cast<int>(s.size());
    if (n == 0)
      return;
    const auto [a, b] = std::minmax_element(s.begin(), s.end());
    lo = *a, hi = *b;
    ok = spread(lo, hi);
    if (!ok)
      return;
    for (float v : s)
      ++cnt[bin_of(v, lo, hi)];
    for (int b2 = 0; b2 < kBuckets; ++b2)
      pre[b2 + 1] = pre[b2] + cnt[b2];
  }
  float rank(float x) const { // 并列均秩的 pct rank
    if (!ok)
      return 0.5f;
    const int b = bin_of(x, lo, hi);
    return pct_of(pre[b], cnt[b], n);
  }
  float quantile(double q) const { // 最小的桶使前缀累计 ≥ ⌈q·n⌉
    if (!ok || q <= 0.0)
      return lo;
    if (q >= 1.0)
      return hi;
    const int need = std::max(1, static_cast<int>(std::ceil(q * n)));
    for (int b = 0; b < kBuckets; ++b)
      if (pre[b + 1] >= need)
        return bin_center(b, lo, hi);
    return hi;
  }
};

} // namespace factor::stream
