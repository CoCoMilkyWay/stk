#pragma once

#include "features/FeaturesDefine.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <span>
#include <vector>

// ============================================================================
// Normalization Methods (归一化方法)
// ============================================================================
//
// 将特征值映射到标准化范围，便于模型训练和特征比较
//
// 方法分类:
//   - 线性缩放: ZSCORE, ROBUST_ZSCORE, IQR_ZSCORE
//   - 排序变换: RANK, RANK_ZSCORE
//   - 边界限制: CLIP, WINSOR
//   - 非线性变换: LOG, POWER, ASINH, TANH
//   - 编码: SINCOS
//   - 复合: LOG_ZSCORE, POWER_ZSCORE, etc.
//
// ============================================================================

namespace math::normalize {

// 参数结构
struct NormParams {
  float clip_k = 3.0f;          // CLIP: 截断到 [-k*std, k*std]
  float winsor_pct = 0.05f;     // WINSOR: 截断百分位
  float power_alpha = 0.5f;     // POWER: x^alpha
  float eps = 1e-8f;            // 数值稳定性
};

// 统计量结构
struct Stats {
  float mean = 0.0f;
  float std = 1.0f;
  float median = 0.0f;
  float mad = 1.0f;             // Median Absolute Deviation
  float q1 = 0.0f;
  float q2 = 0.0f;              // median
  float q3 = 0.0f;
  float min = 0.0f;
  float max = 1.0f;
};

// ============================================================================
// 统计量计算
// ============================================================================

inline Stats compute_stats(std::span<const float> x) {
  Stats s;
  const size_t n = x.size();
  if (n == 0) return s;

  // Mean
  double sum = 0.0;
  for (size_t i = 0; i < n; ++i) {
    sum += x[i];
  }
  s.mean = static_cast<float>(sum / n);

  // Std
  double var_sum = 0.0;
  for (size_t i = 0; i < n; ++i) {
    double d = x[i] - s.mean;
    var_sum += d * d;
  }
  s.std = static_cast<float>(std::sqrt(var_sum / n));
  if (s.std < 1e-10f) s.std = 1.0f;

  // 排序 (用于分位数)
  std::vector<float> sorted(x.begin(), x.end());
  std::sort(sorted.begin(), sorted.end());

  s.min = sorted[0];
  s.max = sorted[n - 1];

  // 分位数
  auto quantile = [&sorted, n](float p) -> float {
    float idx = p * (n - 1);
    size_t lo = static_cast<size_t>(idx);
    size_t hi = std::min(lo + 1, n - 1);
    float frac = idx - lo;
    return sorted[lo] * (1 - frac) + sorted[hi] * frac;
  };

  s.q1 = quantile(0.25f);
  s.q2 = quantile(0.50f);
  s.q3 = quantile(0.75f);
  s.median = s.q2;

  // MAD
  std::vector<float> abs_devs(n);
  for (size_t i = 0; i < n; ++i) {
    abs_devs[i] = std::abs(x[i] - s.median);
  }
  std::sort(abs_devs.begin(), abs_devs.end());
  s.mad = abs_devs[n / 2];
  if (s.mad < 1e-10f) s.mad = 1.0f;

  return s;
}

// ============================================================================
// 各归一化方法实现
// ============================================================================

// Z-Score: (x - mean) / std
inline void zscore(std::span<const float> in, std::span<float> out, const Stats &s) {
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = (in[i] - s.mean) / s.std;
  }
}

// Robust Z-Score: (x - median) / MAD
inline void robust_zscore(std::span<const float> in, std::span<float> out, const Stats &s) {
  const float scale = 1.4826f; // MAD to std conversion for normal distribution
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = (in[i] - s.median) / (s.mad * scale);
  }
}

// IQR Z-Score: (x - Q2) / (Q3 - Q1)
inline void iqr_zscore(std::span<const float> in, std::span<float> out, const Stats &s) {
  float iqr = s.q3 - s.q1;
  if (iqr < 1e-10f) iqr = 1.0f;
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = (in[i] - s.q2) / iqr;
  }
}

// Rank: rank / N (0 to 1)
inline void rank_transform(std::span<const float> in, std::span<float> out) {
  const size_t n = in.size();
  if (n == 0) return;

  // 创建索引排序
  std::vector<size_t> indices(n);
  for (size_t i = 0; i < n; ++i) indices[i] = i;
  std::sort(indices.begin(), indices.end(),
            [&in](size_t a, size_t b) { return in[a] < in[b]; });

  // 分配排名
  const float inv_n = 1.0f / n;
  for (size_t r = 0; r < n; ++r) {
    out[indices[r]] = (r + 0.5f) * inv_n;
  }
}

// Rank Z-Score: rank → inverse normal (Φ^{-1}(rank))
inline void rank_zscore(std::span<const float> in, std::span<float> out) {
  rank_transform(in, out);
  
  // 简化版inverse normal CDF (rational approximation)
  auto inv_normal = [](float p) -> float {
    // Abramowitz and Stegun approximation
    if (p <= 0.0f) return -6.0f;
    if (p >= 1.0f) return 6.0f;
    
    float sign = (p < 0.5f) ? -1.0f : 1.0f;
    if (p > 0.5f) p = 1.0f - p;
    
    float t = std::sqrt(-2.0f * std::log(p));
    float c0 = 2.515517f, c1 = 0.802853f, c2 = 0.010328f;
    float d1 = 1.432788f, d2 = 0.189269f, d3 = 0.001308f;
    float num = c0 + c1 * t + c2 * t * t;
    float den = 1.0f + d1 * t + d2 * t * t + d3 * t * t * t;
    return sign * (t - num / den);
  };

  for (size_t i = 0; i < out.size(); ++i) {
    out[i] = inv_normal(out[i]);
  }
}

// Clip: clip(x, [-k*std, k*std]) after zscore
inline void clip_transform(std::span<const float> in, std::span<float> out, 
                           const Stats &s, float k) {
  float lo = s.mean - k * s.std;
  float hi = s.mean + k * s.std;
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = std::clamp(in[i], lo, hi);
  }
}

// Winsorize: 截断到 [pct, 1-pct] 分位数
inline void winsorize(std::span<const float> in, std::span<float> out, float pct) {
  const size_t n = in.size();
  if (n == 0) return;

  std::vector<float> sorted(in.begin(), in.end());
  std::sort(sorted.begin(), sorted.end());

  size_t lo_idx = static_cast<size_t>(pct * n);
  size_t hi_idx = static_cast<size_t>((1.0f - pct) * n);
  if (hi_idx >= n) hi_idx = n - 1;

  float lo = sorted[lo_idx];
  float hi = sorted[hi_idx];

  for (size_t i = 0; i < n; ++i) {
    out[i] = std::clamp(in[i], lo, hi);
  }
}

// Log: log(x + eps) or log1p
inline void log_transform(std::span<const float> in, std::span<float> out, float eps) {
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = std::log(std::abs(in[i]) + eps);
    if (in[i] < 0) out[i] = -out[i];
  }
}

// Power: sign(x) * |x|^alpha
inline void power_transform(std::span<const float> in, std::span<float> out, float alpha) {
  for (size_t i = 0; i < in.size(); ++i) {
    float sign = (in[i] >= 0) ? 1.0f : -1.0f;
    out[i] = sign * std::pow(std::abs(in[i]), alpha);
  }
}

// Asinh: asinh(x)
inline void asinh_transform(std::span<const float> in, std::span<float> out) {
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = std::asinh(in[i]);
  }
}

// Tanh: tanh(x)
inline void tanh_transform(std::span<const float> in, std::span<float> out) {
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = std::tanh(in[i]);
  }
}

// SinCos: 输出两个通道 (sin(x), cos(x)) - 这里简化只输出sin
inline void sincos_transform(std::span<const float> in, std::span<float> out) {
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = std::sin(in[i]);
  }
}

// ============================================================================
// 统一入口
// ============================================================================

inline void normalize(std::span<const float> in, std::span<float> out,
                      NormMethod method, const NormParams &p) {
  assert(in.size() == out.size());
  
  if (in.empty()) return;

  // 预计算统计量
  Stats s = compute_stats(in);
  std::vector<float> tmp(in.size());

  switch (method) {
  case NormMethod::NONE:
    std::copy(in.begin(), in.end(), out.begin());
    break;

  case NormMethod::ZSCORE:
    zscore(in, out, s);
    break;

  case NormMethod::ROBUST_ZSCORE:
    robust_zscore(in, out, s);
    break;

  case NormMethod::IQR_ZSCORE:
    iqr_zscore(in, out, s);
    break;

  case NormMethod::RANK:
    rank_transform(in, out);
    break;

  case NormMethod::RANK_ZSCORE:
    rank_zscore(in, out);
    break;

  case NormMethod::CLIP:
    clip_transform(in, out, s, p.clip_k);
    break;

  case NormMethod::WINSOR:
    winsorize(in, out, p.winsor_pct);
    break;

  case NormMethod::LOG:
    log_transform(in, out, p.eps);
    break;

  case NormMethod::POWER:
    power_transform(in, out, p.power_alpha);
    break;

  case NormMethod::ASINH:
    asinh_transform(in, out);
    break;

  case NormMethod::TANH:
    tanh_transform(in, out);
    break;

  case NormMethod::SINCOS:
    sincos_transform(in, out);
    break;

  // 复合方法
  case NormMethod::LOG_ZSCORE:
    log_transform(in, {tmp.data(), tmp.size()}, p.eps);
    s = compute_stats({tmp.data(), tmp.size()});
    zscore({tmp.data(), tmp.size()}, out, s);
    break;

  case NormMethod::POWER_ZSCORE:
    power_transform(in, {tmp.data(), tmp.size()}, p.power_alpha);
    s = compute_stats({tmp.data(), tmp.size()});
    zscore({tmp.data(), tmp.size()}, out, s);
    break;

  case NormMethod::ASINH_ZSCORE:
    asinh_transform(in, {tmp.data(), tmp.size()});
    s = compute_stats({tmp.data(), tmp.size()});
    zscore({tmp.data(), tmp.size()}, out, s);
    break;

  case NormMethod::CLIP_ZSCORE:
    zscore(in, {tmp.data(), tmp.size()}, s);
    clip_transform({tmp.data(), tmp.size()}, out, s, p.clip_k);
    break;

  case NormMethod::WINSOR_ZSCORE:
    winsorize(in, {tmp.data(), tmp.size()}, p.winsor_pct);
    s = compute_stats({tmp.data(), tmp.size()});
    zscore({tmp.data(), tmp.size()}, out, s);
    break;

  case NormMethod::CLIP_LOG_ZSCORE:
    clip_transform(in, {tmp.data(), tmp.size()}, s, p.clip_k);
    log_transform({tmp.data(), tmp.size()}, {tmp.data(), tmp.size()}, p.eps);
    s = compute_stats({tmp.data(), tmp.size()});
    zscore({tmp.data(), tmp.size()}, out, s);
    break;

  default:
    std::copy(in.begin(), in.end(), out.begin());
    break;
  }
}

} // namespace math::normalize
