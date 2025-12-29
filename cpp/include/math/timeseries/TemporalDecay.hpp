#pragma once

#include <algorithm>
#include <cassert>
#include <cmath>
#include <numeric>
#include <span>
#include <vector>

// ============================================================================
// Temporal Decay Metrics
// ============================================================================
//
// 评估截面结构的时间稳定性:
//   - Gini 系数: 衡量特征值分布的不均匀性
//   - HHI (Herfindahl-Hirschman Index): 衡量集中度
//   - Rank Correlation: 相邻时间点的排名相关性
//
// ============================================================================

namespace math::timeseries {

// ============================================================================
// Gini 系数
// ============================================================================

inline float compute_gini(std::span<const float> x) {
  const size_t n = x.size();
  if (n < 2) return 0.0f;
  
  // 复制并取绝对值排序
  std::vector<float> sorted(n);
  for (size_t i = 0; i < n; ++i) {
    sorted[i] = std::abs(x[i]);
  }
  std::sort(sorted.begin(), sorted.end());
  
  double sum_xi = 0.0;
  double weighted_sum = 0.0;
  
  for (size_t i = 0; i < n; ++i) {
    sum_xi += sorted[i];
    weighted_sum += static_cast<double>(i + 1) * sorted[i];
  }
  
  if (sum_xi <= 0.0) return 0.0f;
  
  double nn = static_cast<double>(n);
  double gini = (2.0 * weighted_sum) / (nn * sum_xi) - (nn + 1.0) / nn;
  
  return static_cast<float>(std::clamp(gini, 0.0, 1.0));
}

// ============================================================================
// HHI (Herfindahl-Hirschman Index)
// ============================================================================

inline float compute_hhi(std::span<const float> x) {
  const size_t n = x.size();
  if (n < 2) return 1.0f;
  
  // 计算市场份额 (使用绝对值的平方)
  double sum_abs = 0.0;
  for (size_t i = 0; i < n; ++i) {
    sum_abs += std::abs(x[i]);
  }
  
  if (sum_abs <= 0.0) return 1.0f / static_cast<float>(n);
  
  double hhi = 0.0;
  for (size_t i = 0; i < n; ++i) {
    double share = std::abs(x[i]) / sum_abs;
    hhi += share * share;
  }
  
  return static_cast<float>(hhi);
}

// ============================================================================
// Spearman Rank Correlation
// ============================================================================

namespace detail {

inline void compute_ranks(std::span<const float> x, std::vector<double> &ranks) {
  const size_t n = x.size();
  ranks.resize(n);
  
  // 创建索引数组
  std::vector<size_t> indices(n);
  std::iota(indices.begin(), indices.end(), 0);
  
  // 按值排序索引
  std::sort(indices.begin(), indices.end(),
            [&x](size_t a, size_t b) { return x[a] < x[b]; });
  
  // 分配排名 (处理并列)
  size_t i = 0;
  while (i < n) {
    size_t j = i;
    // 找出所有相同值的范围
    while (j < n && x[indices[j]] == x[indices[i]]) {
      ++j;
    }
    // 计算平均排名
    double avg_rank = static_cast<double>(i + j + 1) / 2.0;
    for (size_t k = i; k < j; ++k) {
      ranks[indices[k]] = avg_rank;
    }
    i = j;
  }
}

}  // namespace detail

inline float spearman_rank_correlation(std::span<const float> x,
                                       std::span<const float> y) {
  const size_t n = x.size();
  assert(n == y.size());
  if (n < 3) return 0.0f;
  
  std::vector<double> rank_x, rank_y;
  detail::compute_ranks(x, rank_x);
  detail::compute_ranks(y, rank_y);
  
  // 计算 Pearson correlation on ranks
  double mean_rx = 0.0, mean_ry = 0.0;
  for (size_t i = 0; i < n; ++i) {
    mean_rx += rank_x[i];
    mean_ry += rank_y[i];
  }
  mean_rx /= static_cast<double>(n);
  mean_ry /= static_cast<double>(n);
  
  double cov = 0.0, var_x = 0.0, var_y = 0.0;
  for (size_t i = 0; i < n; ++i) {
    double dx = rank_x[i] - mean_rx;
    double dy = rank_y[i] - mean_ry;
    cov += dx * dy;
    var_x += dx * dx;
    var_y += dy * dy;
  }
  
  if (var_x <= 0.0 || var_y <= 0.0) return 0.0f;
  
  double rho = cov / std::sqrt(var_x * var_y);
  return static_cast<float>(std::clamp(rho, -1.0, 1.0));
}

// ============================================================================
// Temporal Stability (变异系数的倒数)
// ============================================================================

inline float compute_stability(std::span<const float> series) {
  const size_t n = series.size();
  if (n < 2) return 0.0f;
  
  double mean = 0.0;
  for (size_t i = 0; i < n; ++i) mean += series[i];
  mean /= static_cast<double>(n);
  
  if (std::abs(mean) < 1e-10) return 0.0f;
  
  double var = 0.0;
  for (size_t i = 0; i < n; ++i) {
    double d = series[i] - mean;
    var += d * d;
  }
  var /= static_cast<double>(n - 1);
  
  double cv = std::sqrt(var) / std::abs(mean);  // 变异系数
  
  // 稳定性 = 1 / (1 + CV)，范围 (0, 1]
  return static_cast<float>(1.0 / (1.0 + cv));
}

}  // namespace math::timeseries

