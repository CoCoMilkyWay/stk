#pragma once

// =============================================================================
// 分桶直方图 —— **流式后端专用** (TS/Stream.hpp 与 CS/Stream.hpp 共用; 另两个后端各写各的)
// =============================================================================
//   实现的是 Contract.hpp 里"序统计族"的分桶规则: 样本集定 lo/hi, kBuckets 等宽桶,
//   rank / quantile 全由**整数桶计数**决定 → 三后端算法各异仍能逐位一致,
//   近似只相对于"真序统计" (分辨率 = 桶宽).
//   样本量 ≤ 段长 240 (EXPAND) / 窗长 d (ROLL) / 截面宽 A (CS), 直接缓存后建桶.
// =============================================================================

#include "factor/Contract.hpp"

#include <algorithm>
#include <cmath>
#include <vector>

namespace factor::stream {

struct Hist {
  int cnt[kBuckets] = {};
  float lo = 0, hi = 0;
  int n = 0;
  bool spread = false; // 值域非退化 (非全并列)

  void build(const std::vector<float> &s) {
    n = static_cast<int>(s.size());
    if (n == 0)
      return;
    const auto [a, b] = std::minmax_element(s.begin(), s.end());
    lo = *a, hi = *b;
    spread = range_ok(lo, hi);
    if (!spread)
      return;
    for (float v : s)
      ++cnt[bin_of(v, lo, hi)];
  }
  float rank(float x) const { // 并列均秩的 pct rank
    if (!spread)
      return 0.5f;
    const int b = bin_of(x, lo, hi);
    int less = 0;
    for (int j = 0; j < b; ++j)
      less += cnt[j];
    return pct_of(less, cnt[b], n);
  }
  float quantile(double q) const { // 最小的桶使前缀累计 ≥ ⌈q·n⌉
    if (!spread || q <= 0.0)
      return lo;
    if (q >= 1.0)
      return hi;
    const int need = std::max(1, static_cast<int>(std::ceil(q * n)));
    int cum = 0;
    for (int b = 0; b < kBuckets; ++b)
      if ((cum += cnt[b]) >= need)
        return bin_center(b, lo, hi);
    return hi;
  }
};

} // namespace factor::stream
