#pragma once

#include "features/FeaturesDefine.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <span>
#include <vector>

// ============================================================================
// Normalization
// ============================================================================
//
// 张量维度: [asset, time, features]
//
// 方向:
//   - ts (TimeSeries):     单个 asset 时间序列，expanding 统计量
//   - cs (CrossSectional): 某时刻所有 asset，batch 统计量
//
// ============================================================================

namespace math::normalize {

// ============================================================================
// 参数
// ============================================================================

struct ClipParams {
  float k = 3.0f;
};

struct WinsorParams {
  float pct = 0.05f;
};

struct PowerParams {
  float alpha = 0.5f;
};

struct LogParams {
  float eps = 1e-8f;
  float base = 0.0f; // 0=ln, 2=log2, 10=log10
};

struct AsinhParams {
  float scale = 1.0f;
};

struct TanhParams {
  float scale = 1.0f;
};

struct SinParams {
  float period = 1.0f;
};

struct RobustParams {
  float mad_scale = 1.4826f; // MAD -> std 转换常数 (正态假设)
};

struct IQRParams {
  float q_lo = 0.25f;
  float q_hi = 0.75f;
};

struct Params {
  ClipParams clip;
  WinsorParams winsor;
  PowerParams power;
  LogParams log;
  AsinhParams asinh;
  TanhParams tanh;
  SinParams sin;
  RobustParams robust;
  IQRParams iqr;
  size_t min_samples = 2; // 仅 TS 方向使用
};

// ============================================================================
// 辅助
// ============================================================================

// Abramowitz-Stegun 近似，输出范围 [-6, 6]
inline float inv_normal_cdf(float p) {
  if (p <= 0.0f)
    return -6.0f;
  if (p >= 1.0f)
    return 6.0f;
  float sign = (p < 0.5f) ? -1.0f : 1.0f;
  if (p > 0.5f)
    p = 1.0f - p;
  float t = std::sqrt(-2.0f * std::log(p));
  constexpr float c0 = 2.515517f, c1 = 0.802853f, c2 = 0.010328f;
  constexpr float d1 = 1.432788f, d2 = 0.189269f, d3 = 0.001308f;
  float r = sign * (t - (c0 + c1 * t + c2 * t * t) / (1.0f + d1 * t + d2 * t * t + d3 * t * t * t));
  return std::clamp(r, -6.0f, 6.0f);
}

// ============================================================================
// Order Statistics Tree (AVL + subtree size)
// O(log n) insert, select, rank
// ============================================================================

class OSTree {
  struct Node {
    float val;
    int size = 1, height = 1;
    int left = -1, right = -1;
  };

  std::vector<Node> pool_;
  int root_ = -1;

  int sz(int i) const { return i < 0 ? 0 : pool_[i].size; }
  int ht(int i) const { return i < 0 ? 0 : pool_[i].height; }
  int bal(int i) const { return i < 0 ? 0 : ht(pool_[i].left) - ht(pool_[i].right); }

  void upd(int i) {
    pool_[i].size = 1 + sz(pool_[i].left) + sz(pool_[i].right);
    pool_[i].height = 1 + std::max(ht(pool_[i].left), ht(pool_[i].right));
  }

  int rot_r(int y) {
    int x = pool_[y].left;
    pool_[y].left = pool_[x].right;
    pool_[x].right = y;
    upd(y);
    upd(x);
    return x;
  }

  int rot_l(int x) {
    int y = pool_[x].right;
    pool_[x].right = pool_[y].left;
    pool_[y].left = x;
    upd(x);
    upd(y);
    return y;
  }

  int rebal(int i) {
    upd(i);
    int b = bal(i);
    if (b > 1) {
      if (bal(pool_[i].left) < 0)
        pool_[i].left = rot_l(pool_[i].left);
      return rot_r(i);
    }
    if (b < -1) {
      if (bal(pool_[i].right) > 0)
        pool_[i].right = rot_r(pool_[i].right);
      return rot_l(i);
    }
    return i;
  }

  int insert(int i, float x) {
    if (i < 0) {
      int idx = static_cast<int>(pool_.size());
      pool_.push_back({x, 1, 1, -1, -1});
      return idx;
    }
    if (x <= pool_[i].val)
      pool_[i].left = insert(pool_[i].left, x);
    else
      pool_[i].right = insert(pool_[i].right, x);
    return rebal(i);
  }

  float select(int i, int k) const {
    int left_sz = sz(pool_[i].left);
    if (k < left_sz)
      return select(pool_[i].left, k);
    if (k == left_sz)
      return pool_[i].val;
    return select(pool_[i].right, k - left_sz - 1);
  }

  int rank_lt(int i, float x) const {
    if (i < 0)
      return 0;
    if (x <= pool_[i].val)
      return rank_lt(pool_[i].left, x);
    return sz(pool_[i].left) + 1 + rank_lt(pool_[i].right, x);
  }

  int rank_le(int i, float x) const {
    if (i < 0)
      return 0;
    if (x < pool_[i].val)
      return rank_le(pool_[i].left, x);
    return sz(pool_[i].left) + 1 + rank_le(pool_[i].right, x);
  }

public:
  OSTree() { pool_.reserve(16384); }

  void add(float x) { root_ = insert(root_, x); }

  // k-th smallest (0-indexed)
  float kth(int k) const {
    assert(k >= 0 && k < sz(root_));
    return select(root_, k);
  }

  // quantile p in [0,1]
  float get(float p) const {
    int n = sz(root_);
    if (n == 0)
      return 0.0f;
    int k = static_cast<int>(p * (n - 1));
    return kth(k);
  }

  // fraction of elements < x, 范围 [0, 1]
  float rank(float x) const {
    int n = sz(root_);
    if (n <= 1)
      return 0.5f;
    int lt = rank_lt(root_, x);
    int le = rank_le(root_, x);
    // 平均排名映射到 [0, 1]，clamp 防止极端值
    float r = (lt + le - 1) * 0.5f / (n - 1);
    return std::clamp(r, 0.0f, 1.0f);
  }

  float median() const { return get(0.5f); }
  float iqr() const {
    float v = get(0.75f) - get(0.25f);
    return v < 1e-10f ? 1.0f : v;
  }
  int size() const { return sz(root_); }
  void clear() {
    pool_.clear();
    root_ = -1;
  }
};

// ============================================================================
// Point-wise 变换 (无方向，直接对元素操作)
// ============================================================================

inline void pw_log(std::span<const float> in, std::span<float> out, const LogParams &p) {
  float inv = (p.base > 1.0f) ? (1.0f / std::log(p.base)) : 1.0f;
  for (size_t i = 0; i < in.size(); ++i) {
    float v = std::log(std::abs(in[i]) + p.eps) * inv;
    out[i] = (in[i] >= 0) ? v : -v;
  }
}

inline void pw_power(std::span<const float> in, std::span<float> out, float alpha) {
  for (size_t i = 0; i < in.size(); ++i) {
    float a = std::abs(in[i]);
    float s = (in[i] >= 0) ? 1.0f : -1.0f;
    // 防止 pow(0, <=0) 产生 inf/nan
    float v = (a < 1e-12f && alpha <= 0.0f) ? 0.0f : std::pow(a, alpha);
    out[i] = std::isfinite(v) ? s * v : 0.0f;
  }
}

inline void pw_asinh(std::span<const float> in, std::span<float> out, float scale) {
  float inv = (std::abs(scale) < 1e-12f) ? 1.0f : (1.0f / scale);
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = std::asinh(in[i] * inv);
  }
}

inline void pw_tanh(std::span<const float> in, std::span<float> out, float scale) {
  float inv = (std::abs(scale) < 1e-12f) ? 1.0f : (1.0f / scale);
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = std::tanh(in[i] * inv);
  }
}

inline void pw_sin(std::span<const float> in, std::span<float> out, float period) {
  constexpr float two_pi = 6.283185307179586f;
  float s = (std::abs(period) < 1e-12f) ? two_pi : (two_pi / period);
  for (size_t i = 0; i < in.size(); ++i) {
    out[i] = std::sin(in[i] * s);
  }
}

// ============================================================================
// TimeSeries (TS方向): expanding 统计量
// ============================================================================
// 输入: x[t] (单个 asset 时间序列)
// 规则: 只用历史数据

namespace ts {

// Expanding Mean/Std (Welford) - O(1) per update
struct EMeanStd {
  double mean = 0.0, M2 = 0.0;
  size_t n = 0;

  void add(float x) {
    ++n;
    double d = x - mean;
    mean += d / n;
    M2 += d * (x - mean);
  }

  float get_mean() const { return static_cast<float>(mean); }
  float get_std() const {
    if (n < 2)
      return 1.0f;
    float s = static_cast<float>(std::sqrt(M2 / n));
    return (s < 1e-10f) ? 1.0f : s;
  }
};

// Expanding Quantile - O(log n) per update via OSTree
struct EQuantile {
  OSTree tree;

  void add(float x) { tree.add(x); }
  float get(float p) const { return tree.get(p); }
  float median() const { return tree.median(); }
  float iqr() const { return tree.iqr(); }
  size_t size() const { return static_cast<size_t>(tree.size()); }
};

// Expanding MAD - O(n) per query
// 由于 median 变化，每次需要重新计算
struct EMAD {
  std::vector<float> data;
  float mad_scale = 1.4826f;

  void add(float x) { data.push_back(x); }

  float get(float median) const {
    size_t n = data.size();
    if (n == 0)
      return 1.0f;
    std::vector<float> ad(n);
    for (size_t i = 0; i < n; ++i)
      ad[i] = std::abs(data[i] - median);
    std::nth_element(ad.begin(), ad.begin() + n / 2, ad.end());
    float m = ad[n / 2] * mad_scale;
    return (m < 1e-10f) ? 1.0f : m;
  }
};

// Expanding Rank - O(log n) per update via OSTree
struct ERank {
  OSTree tree;

  void add(float x) { tree.add(x); }
  float get(float x) const { return tree.rank(x); }
  size_t size() const { return static_cast<size_t>(tree.size()); }
};

// --- 方法 ---

inline void zscore(std::span<const float> x, std::span<float> out, size_t min_n) {
  EMeanStd e;
  for (size_t i = 0; i < x.size(); ++i) {
    e.add(x[i]);
    out[i] = (e.n < min_n) ? 0.0f : (x[i] - e.get_mean()) / e.get_std();
  }
}

inline void robust_zscore(std::span<const float> x, std::span<float> out, size_t min_n, float mad_scale) {
  EQuantile eq;
  EMAD em;
  em.mad_scale = mad_scale;
  for (size_t i = 0; i < x.size(); ++i) {
    eq.add(x[i]);
    em.add(x[i]);
    if (eq.size() < min_n) {
      out[i] = 0.0f;
    } else {
      float med = eq.median();
      out[i] = (x[i] - med) / em.get(med);
    }
  }
}

inline void iqr_zscore(std::span<const float> x, std::span<float> out, size_t min_n, float q_lo, float q_hi) {
  EQuantile eq;
  for (size_t i = 0; i < x.size(); ++i) {
    eq.add(x[i]);
    if (eq.size() < min_n) {
      out[i] = 0.0f;
    } else {
      float iqr = eq.get(q_hi) - eq.get(q_lo);
      if (iqr < 1e-10f)
        iqr = 1.0f;
      out[i] = (x[i] - eq.median()) / iqr;
    }
  }
}

inline void rank(std::span<const float> x, std::span<float> out, size_t min_n) {
  ERank er;
  for (size_t i = 0; i < x.size(); ++i) {
    out[i] = (er.size() < min_n) ? 0.5f : er.get(x[i]);
    er.add(x[i]);
  }
}

inline void rank_zscore(std::span<const float> x, std::span<float> out, size_t min_n) {
  rank(x, out, min_n);
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = inv_normal_cdf(out[i]);
}

inline void clip(std::span<const float> x, std::span<float> out, float k, size_t min_n) {
  EMeanStd e;
  for (size_t i = 0; i < x.size(); ++i) {
    e.add(x[i]);
    if (e.n < min_n) {
      out[i] = x[i];
    } else {
      float lo = e.get_mean() - k * e.get_std();
      float hi = e.get_mean() + k * e.get_std();
      out[i] = std::clamp(x[i], lo, hi);
    }
  }
}

inline void winsor(std::span<const float> x, std::span<float> out, float pct, size_t min_n) {
  EQuantile eq;
  for (size_t i = 0; i < x.size(); ++i) {
    eq.add(x[i]);
    if (eq.size() < min_n) {
      out[i] = x[i];
    } else {
      out[i] = std::clamp(x[i], eq.get(pct), eq.get(1.0f - pct));
    }
  }
}

// 复合

inline void clip_zscore(std::span<const float> x, std::span<float> out, float k, size_t min_n) {
  EMeanStd e;
  for (size_t i = 0; i < x.size(); ++i) {
    e.add(x[i]);
    if (e.n < min_n) {
      out[i] = 0.0f;
    } else {
      float z = (x[i] - e.get_mean()) / e.get_std();
      out[i] = std::clamp(z, -k, k);
    }
  }
}

inline void log_zscore(std::span<const float> x, std::span<float> out,
                       const LogParams &lp, size_t min_n) {
  float inv = (lp.base > 1.0f) ? (1.0f / std::log(lp.base)) : 1.0f;
  EMeanStd e;
  for (size_t i = 0; i < x.size(); ++i) {
    float v = std::log(std::abs(x[i]) + lp.eps) * inv;
    if (x[i] < 0)
      v = -v;
    e.add(v);
    out[i] = (e.n < min_n) ? 0.0f : (v - e.get_mean()) / e.get_std();
  }
}

inline void power_zscore(std::span<const float> x, std::span<float> out,
                         float alpha, size_t min_n) {
  EMeanStd e;
  for (size_t i = 0; i < x.size(); ++i) {
    float s = (x[i] >= 0) ? 1.0f : -1.0f;
    float v = s * std::pow(std::abs(x[i]), alpha);
    e.add(v);
    out[i] = (e.n < min_n) ? 0.0f : (v - e.get_mean()) / e.get_std();
  }
}

inline void asinh_zscore(std::span<const float> x, std::span<float> out,
                         float scale, size_t min_n) {
  float inv = 1.0f / scale;
  EMeanStd e;
  for (size_t i = 0; i < x.size(); ++i) {
    float v = std::asinh(x[i] * inv);
    e.add(v);
    out[i] = (e.n < min_n) ? 0.0f : (v - e.get_mean()) / e.get_std();
  }
}

inline void winsor_zscore(std::span<const float> x, std::span<float> out,
                          float pct, size_t min_n) {
  EQuantile eq;
  EMeanStd ew;
  for (size_t i = 0; i < x.size(); ++i) {
    eq.add(x[i]);
    float v = (eq.size() < min_n) ? x[i] : std::clamp(x[i], eq.get(pct), eq.get(1.0f - pct));
    ew.add(v);
    out[i] = (ew.n < min_n) ? 0.0f : (v - ew.get_mean()) / ew.get_std();
  }
}

inline void clip_log_zscore(std::span<const float> x, std::span<float> out,
                            float k, const LogParams &lp, size_t min_n) {
  float inv = (lp.base > 1.0f) ? (1.0f / std::log(lp.base)) : 1.0f;
  EMeanStd ec, ef;
  for (size_t i = 0; i < x.size(); ++i) {
    ec.add(x[i]);
    float v = x[i];
    if (ec.n >= min_n) {
      v = std::clamp(v, ec.get_mean() - k * ec.get_std(), ec.get_mean() + k * ec.get_std());
    }
    float lv = std::log(std::abs(v) + lp.eps) * inv;
    if (v < 0)
      lv = -lv;
    ef.add(lv);
    out[i] = (ef.n < min_n) ? 0.0f : (lv - ef.get_mean()) / ef.get_std();
  }
}

} // namespace ts

// ============================================================================
// CrossSectional (CS方向): batch 统计量
// ============================================================================
// 输入: x[a] (某时刻所有 asset)
// 规则: 可用当前截面全部数据

namespace cs {

// 按需计算，避免不必要的开销
struct MeanStd {
  float mean = 0.0f, std = 1.0f;
};

inline MeanStd mean_std(std::span<const float> x) {
  MeanStd s;
  const size_t n = x.size();
  if (n == 0)
    return s;
  double sum = 0.0;
  for (size_t i = 0; i < n; ++i)
    sum += x[i];
  s.mean = static_cast<float>(sum / n);
  double var = 0.0;
  for (size_t i = 0; i < n; ++i) {
    double d = x[i] - s.mean;
    var += d * d;
  }
  s.std = static_cast<float>(std::sqrt(var / n));
  if (s.std < 1e-10f)
    s.std = 1.0f;
  return s;
}

struct MedianIQR {
  float median = 0.0f, iqr = 1.0f;
};

inline MedianIQR median_iqr(std::span<const float> x, std::vector<float> &buf, float q_lo = 0.25f, float q_hi = 0.75f) {
  MedianIQR s;
  const size_t n = x.size();
  if (n == 0)
    return s;
  buf.assign(x.begin(), x.end());
  // nth_element for Q1, median, Q3 - O(n) each
  size_t i_med = n / 2;
  std::nth_element(buf.begin(), buf.begin() + i_med, buf.end());
  s.median = buf[i_med];
  size_t i_q1 = static_cast<size_t>(q_lo * (n - 1));
  size_t i_q3 = static_cast<size_t>(q_hi * (n - 1));
  std::nth_element(buf.begin(), buf.begin() + i_q1, buf.end());
  std::nth_element(buf.begin(), buf.begin() + i_q3, buf.end());
  s.iqr = buf[i_q3] - buf[i_q1];
  if (s.iqr < 1e-10f)
    s.iqr = 1.0f;
  return s;
}

inline float compute_mad(std::span<const float> x, float median, std::vector<float> &buf, float mad_scale = 1.4826f) {
  const size_t n = x.size();
  if (n == 0)
    return 1.0f;
  buf.resize(n);
  for (size_t i = 0; i < n; ++i)
    buf[i] = std::abs(x[i] - median);
  std::nth_element(buf.begin(), buf.begin() + n / 2, buf.end());
  float m = buf[n / 2] * mad_scale;
  return (m < 1e-10f) ? 1.0f : m;
}

// 兼容旧接口
struct Stats {
  float mean = 0.0f, std = 1.0f;
  float median = 0.0f, mad = 1.0f, iqr = 1.0f;
};

inline Stats stats(std::span<const float> x) {
  Stats s;
  const size_t n = x.size();
  if (n == 0)
    return s;
  auto ms = mean_std(x);
  s.mean = ms.mean;
  s.std = ms.std;
  std::vector<float> buf;
  auto mi = median_iqr(x, buf);
  s.median = mi.median;
  s.iqr = mi.iqr;
  s.mad = compute_mad(x, s.median, buf);
  return s;
}

// --- 方法 ---

inline void zscore(std::span<const float> x, std::span<float> out) {
  auto s = mean_std(x);
  float inv = 1.0f / s.std;
  for (size_t i = 0; i < x.size(); ++i)
    out[i] = (x[i] - s.mean) * inv;
}

inline void robust_zscore(std::span<const float> x, std::span<float> out, float mad_scale = 1.4826f) {
  std::vector<float> buf;
  auto mi = median_iqr(x, buf);
  float mad = compute_mad(x, mi.median, buf, mad_scale);
  float inv = 1.0f / mad;
  for (size_t i = 0; i < x.size(); ++i)
    out[i] = (x[i] - mi.median) * inv;
}

inline void iqr_zscore(std::span<const float> x, std::span<float> out, float q_lo = 0.25f, float q_hi = 0.75f) {
  std::vector<float> buf;
  auto mi = median_iqr(x, buf, q_lo, q_hi);
  float inv = 1.0f / mi.iqr;
  for (size_t i = 0; i < x.size(); ++i)
    out[i] = (x[i] - mi.median) * inv;
}

inline void rank(std::span<const float> x, std::span<float> out) {
  const size_t n = x.size();
  if (n == 0)
    return;
  if (n == 1) {
    out[0] = 0.5f;
    return;
  }

  std::vector<size_t> idx(n);
  for (size_t i = 0; i < n; ++i)
    idx[i] = i;
  std::sort(idx.begin(), idx.end(), [&](size_t a, size_t b) { return x[a] < x[b]; });

  float inv = 1.0f / (n - 1);
  size_t i = 0;
  while (i < n) {
    size_t j = i + 1;
    while (j < n && x[idx[j]] == x[idx[i]])
      ++j;
    // 平均排名映射到 [0, 1]
    float r = (static_cast<float>(i + j - 1) * 0.5f) * inv;
    for (size_t k = i; k < j; ++k)
      out[idx[k]] = r;
    i = j;
  }
}

inline void rank_zscore(std::span<const float> x, std::span<float> out) {
  rank(x, out);
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = inv_normal_cdf(out[i]);
}

inline void clip(std::span<const float> x, std::span<float> out, float k) {
  auto s = mean_std(x);
  float lo = s.mean - k * s.std, hi = s.mean + k * s.std;
  for (size_t i = 0; i < x.size(); ++i)
    out[i] = std::clamp(x[i], lo, hi);
}

inline void winsor(std::span<const float> x, std::span<float> out, float pct) {
  const size_t n = x.size();
  if (n == 0)
    return;
  std::vector<float> buf(x.begin(), x.end());
  size_t lo_idx = static_cast<size_t>(pct * (n - 1));
  size_t hi_idx = static_cast<size_t>((1.0f - pct) * (n - 1));
  std::nth_element(buf.begin(), buf.begin() + lo_idx, buf.end());
  float lo = buf[lo_idx];
  std::nth_element(buf.begin(), buf.begin() + hi_idx, buf.end());
  float hi = buf[hi_idx];
  for (size_t i = 0; i < n; ++i)
    out[i] = std::clamp(x[i], lo, hi);
}

// 复合

inline void clip_zscore(std::span<const float> x, std::span<float> out, float k) {
  zscore(x, out);
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = std::clamp(out[i], -k, k);
}

inline void log_zscore(std::span<const float> x, std::span<float> out, const LogParams &lp) {
  pw_log(x, out, lp);
  auto s = mean_std(out);
  float inv = 1.0f / s.std;
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = (out[i] - s.mean) * inv;
}

inline void power_zscore(std::span<const float> x, std::span<float> out, float alpha) {
  pw_power(x, out, alpha);
  auto s = mean_std(out);
  float inv = 1.0f / s.std;
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = (out[i] - s.mean) * inv;
}

inline void asinh_zscore(std::span<const float> x, std::span<float> out, float scale) {
  pw_asinh(x, out, scale);
  auto s = mean_std(out);
  float inv = 1.0f / s.std;
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = (out[i] - s.mean) * inv;
}

inline void winsor_zscore(std::span<const float> x, std::span<float> out, float pct) {
  winsor(x, out, pct);
  auto s = mean_std(out);
  float inv = 1.0f / s.std;
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = (out[i] - s.mean) * inv;
}

inline void clip_log_zscore(std::span<const float> x, std::span<float> out,
                            float k, const LogParams &lp) {
  clip(x, out, k);
  pw_log(out, out, lp);
  auto s = mean_std(out);
  float inv = 1.0f / s.std;
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = (out[i] - s.mean) * inv;
}

} // namespace cs

// ============================================================================
// 统一入口
// ============================================================================

// TS方向: 对单个 asset 时间序列
inline void apply_ts(std::span<const float> x, std::span<float> out, NormMethod m, const Params &p) {
  assert(x.size() == out.size());
  if (x.empty())
    return;
  const size_t min_n = p.min_samples;

  switch (m) {
  case NormMethod::NONE:
    std::copy(x.begin(), x.end(), out.begin());
    break;
  case NormMethod::ZSCORE:
    ts::zscore(x, out, min_n);
    break;
  case NormMethod::ROBUST_ZSCORE:
    ts::robust_zscore(x, out, min_n, p.robust.mad_scale);
    break;
  case NormMethod::IQR_ZSCORE:
    ts::iqr_zscore(x, out, min_n, p.iqr.q_lo, p.iqr.q_hi);
    break;
  case NormMethod::RANK:
    ts::rank(x, out, min_n);
    break;
  case NormMethod::RANK_ZSCORE:
    ts::rank_zscore(x, out, min_n);
    break;
  case NormMethod::CLIP:
    ts::clip(x, out, p.clip.k, min_n);
    break;
  case NormMethod::WINSOR:
    ts::winsor(x, out, p.winsor.pct, min_n);
    break;
  case NormMethod::LOG:
    pw_log(x, out, p.log);
    break;
  case NormMethod::POWER:
    pw_power(x, out, p.power.alpha);
    break;
  case NormMethod::ASINH:
    pw_asinh(x, out, p.asinh.scale);
    break;
  case NormMethod::TANH:
    pw_tanh(x, out, p.tanh.scale);
    break;
  case NormMethod::SINCOS:
    pw_sin(x, out, p.sin.period);
    break;
  case NormMethod::LOG_ZSCORE:
    ts::log_zscore(x, out, p.log, min_n);
    break;
  case NormMethod::POWER_ZSCORE:
    ts::power_zscore(x, out, p.power.alpha, min_n);
    break;
  case NormMethod::ASINH_ZSCORE:
    ts::asinh_zscore(x, out, p.asinh.scale, min_n);
    break;
  case NormMethod::CLIP_ZSCORE:
    ts::clip_zscore(x, out, p.clip.k, min_n);
    break;
  case NormMethod::WINSOR_ZSCORE:
    ts::winsor_zscore(x, out, p.winsor.pct, min_n);
    break;
  case NormMethod::CLIP_LOG_ZSCORE:
    ts::clip_log_zscore(x, out, p.clip.k, p.log, min_n);
    break;
  default:
    std::copy(x.begin(), x.end(), out.begin());
    break;
  }
}

// CS方向: 对某时刻所有 asset
inline void apply_cs(std::span<const float> x, std::span<float> out, NormMethod m, const Params &p) {
  assert(x.size() == out.size());
  if (x.empty())
    return;

  switch (m) {
  case NormMethod::NONE:
    std::copy(x.begin(), x.end(), out.begin());
    break;
  case NormMethod::ZSCORE:
    cs::zscore(x, out);
    break;
  case NormMethod::ROBUST_ZSCORE:
    cs::robust_zscore(x, out, p.robust.mad_scale);
    break;
  case NormMethod::IQR_ZSCORE:
    cs::iqr_zscore(x, out, p.iqr.q_lo, p.iqr.q_hi);
    break;
  case NormMethod::RANK:
    cs::rank(x, out);
    break;
  case NormMethod::RANK_ZSCORE:
    cs::rank_zscore(x, out);
    break;
  case NormMethod::CLIP:
    cs::clip(x, out, p.clip.k);
    break;
  case NormMethod::WINSOR:
    cs::winsor(x, out, p.winsor.pct);
    break;
  case NormMethod::LOG:
    pw_log(x, out, p.log);
    break;
  case NormMethod::POWER:
    pw_power(x, out, p.power.alpha);
    break;
  case NormMethod::ASINH:
    pw_asinh(x, out, p.asinh.scale);
    break;
  case NormMethod::TANH:
    pw_tanh(x, out, p.tanh.scale);
    break;
  case NormMethod::SINCOS:
    pw_sin(x, out, p.sin.period);
    break;
  case NormMethod::LOG_ZSCORE:
    cs::log_zscore(x, out, p.log);
    break;
  case NormMethod::POWER_ZSCORE:
    cs::power_zscore(x, out, p.power.alpha);
    break;
  case NormMethod::ASINH_ZSCORE:
    cs::asinh_zscore(x, out, p.asinh.scale);
    break;
  case NormMethod::CLIP_ZSCORE:
    cs::clip_zscore(x, out, p.clip.k);
    break;
  case NormMethod::WINSOR_ZSCORE:
    cs::winsor_zscore(x, out, p.winsor.pct);
    break;
  case NormMethod::CLIP_LOG_ZSCORE:
    cs::clip_log_zscore(x, out, p.clip.k, p.log);
    break;
  default:
    std::copy(x.begin(), x.end(), out.begin());
    break;
  }
}

} // namespace math::normalize
