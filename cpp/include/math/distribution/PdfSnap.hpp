#pragma once

#include "math/distribution/KLLcache.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>

// PDF 发布快照: KLL 导出成品 (矩 + R−1 点折线), UI 零计算只画
template <size_t R>
struct PdfSnap {
  uint64_t n = 0; // 累积样本数
  float mean = 0.0f, var = 0.0f, skew = 0.0f, kurt = 0.0f;
  uint32_t n_pts = 0; // 折线点数; 0 = 样本不足, 不画
  std::array<float, R - 1> x{}, y{};

  float sd() const { return std::sqrt(std::max(0.0f, var)); }

  // n > 0 → 矩; n ≥ min_samples → PDF 折线; 否则 n_pts = 0
  void fill(const KLLcache &kll, size_t min_samples) {
    n = kll.totalCount();
    n_pts = 0;
    if (n == 0)
      return;
    mean = static_cast<float>(kll.mean());
    var = static_cast<float>(kll.var());
    skew = static_cast<float>(kll.skew());
    kurt = static_cast<float>(kll.kurt());
    if (n < min_samples)
      return;
    const auto pdf = kll.exportPDF();
    assert(pdf.n <= x.size());
    n_pts = static_cast<uint32_t>(pdf.n);
    std::copy_n(pdf.x, pdf.n, x.data());
    std::copy_n(pdf.y, pdf.n, y.data());
  }
};

// 聚合 sketch 档位 (全局 / 聚合槽; 小面板 ~400px, 255 点 PDF 足够)
inline constexpr size_t kAggKllCapacity = 512;
inline constexpr size_t kAggKllResolution = 256;
using AggPdf = PdfSnap<kAggKllResolution>;
