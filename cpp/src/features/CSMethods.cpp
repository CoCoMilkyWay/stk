// CSMethods 实现 — qmt/cpp/src/feature/cs.cpp 截面 kernel 逐步复刻.
//
// 忠实性契约 (决定 qmt 因子效果能否复现, 改动前先对 qmt 源码逐行核对):
//   median_in_place: nth_element 取上中位, 偶数长度与下中位平均
//   winsor_mad:      k=3, mad==0 → 不动; 样本 <2 → 不动
//   winsorize_quantile: lo_k=floor(lo·(n-1)), hi_k=ceil(hi·(n-1)), 样本 <2 → 不动
//   z:               double 累加, cnt<2 或 var<=0 → 不动
//   pct_rank:        并列取平均秩, pct=(avg_rank-1)/(m-1), m==1 → 0.5
//   neutralize:      行业组内 demean + demean 后对 log(mcap) 标量回归取残差
//                    (FWL 等价 full OLS); 样本无效 → 残差 NaN; 行业 id 0 为独立组
//   均值填充:        NaN → finite 均值; 全缺失 → 全 0
//
// 与 qmt 的唯一口径差: 参与集. qmt 用在市全集 (mask_offmarket), 本项目热路径
//   上游已按 _data_valid 收成 dense 活跃子集 (停牌/退市自然缺席), 无需再 mask.
//
// 注意: 本文件依赖 NaN 语义, 必须在 CMake PRECISE_MATH 列表里 (-fno-fast-math).
#include "features/Misc/CSMethods.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <utility>
#include <vector>

namespace cs {

namespace {

constexpr int SW2021_L1_COUNT = 32; // 0=未知 (独立组), 1..31

inline float nanf_() { return std::nanf(""); }

// 逆正态 CDF (Beasley-Springer-Moro 简化近似), NormRank 用; p 越界钳到 ±6
inline float inverse_normal_cdf(float p) {
  constexpr float a0 = 2.50662823884f;
  constexpr float a1 = -18.61500062529f;
  constexpr float a2 = 41.39119773534f;
  constexpr float a3 = -25.44106049637f;
  constexpr float b1 = -8.47351093090f;
  constexpr float b2 = 23.08336743743f;
  constexpr float b3 = -21.06224101826f;
  constexpr float b4 = 3.13082909833f;

  if (p <= 0.0f)
    return -6.0f;
  if (p >= 1.0f)
    return 6.0f;

  float t = (p < 0.5f) ? p : (1.0f - p);
  t = std::sqrt(-2.0f * std::log(t));
  float num = a0 + t * (a1 + t * (a2 + t * a3));
  float denom = 1.0f + t * (b1 + t * (b2 + t * (b3 + t * b4)));
  float result = t - num / denom;
  return (p < 0.5f) ? -result : result;
}

float median_in_place(std::vector<float> &tmp) {
  assert(!tmp.empty());
  std::size_t n = tmp.size();
  std::nth_element(tmp.begin(), tmp.begin() + n / 2, tmp.end());
  float m = tmp[n / 2];
  if (n % 2 == 0) {
    auto max_lo = std::max_element(tmp.begin(), tmp.begin() + n / 2);
    m = (m + *max_lo) * 0.5f;
  }
  return m;
}

// thread_local scratch: 避免每特征每分钟反复 malloc
std::vector<float> &scratch_f(std::size_t reserve_n) {
  thread_local std::vector<float> v;
  v.clear();
  v.reserve(reserve_n);
  return v;
}
std::vector<std::size_t> &scratch_idx(std::size_t reserve_n) {
  thread_local std::vector<std::size_t> v;
  v.clear();
  v.reserve(reserve_n);
  return v;
}

void winsor_mad(float *x, std::size_t n, float k) {
  std::vector<float> &tmp = scratch_f(n);
  for (std::size_t i = 0; i < n; ++i) {
    if (std::isfinite(x[i]))
      tmp.push_back(x[i]);
  }
  if (tmp.size() < 2)
    return;

  float med = median_in_place(tmp);

  thread_local std::vector<float> dev;
  dev.clear();
  dev.reserve(tmp.size());
  for (float v : tmp)
    dev.push_back(std::fabs(v - med));
  float mad = median_in_place(dev);
  if (mad == 0.0f)
    return;

  float lo = med - k * mad;
  float hi = med + k * mad;
  for (std::size_t i = 0; i < n; ++i) {
    float &v = x[i];
    if (!std::isfinite(v))
      continue;
    if (v < lo)
      v = lo;
    else if (v > hi)
      v = hi;
  }
}

void winsorize_quantile(float *x, std::size_t n, float lo_pct, float hi_pct) {
  std::vector<float> &tmp = scratch_f(n);
  for (std::size_t i = 0; i < n; ++i)
    if (std::isfinite(x[i]))
      tmp.push_back(x[i]);
  std::size_t m = tmp.size();
  if (m < 2)
    return;
  std::sort(tmp.begin(), tmp.end());
  std::size_t lo_k = static_cast<std::size_t>(std::floor(lo_pct * static_cast<double>(m - 1)));
  std::size_t hi_k = static_cast<std::size_t>(std::ceil(hi_pct * static_cast<double>(m - 1)));
  if (lo_k > m - 1)
    lo_k = m - 1;
  if (hi_k > m - 1)
    hi_k = m - 1;
  float lo = tmp[lo_k], hi = tmp[hi_k];
  for (std::size_t i = 0; i < n; ++i) {
    float &v = x[i];
    if (!std::isfinite(v))
      continue;
    if (v < lo)
      v = lo;
    else if (v > hi)
      v = hi;
  }
}

void z(float *x, std::size_t n) {
  double sum = 0.0, sum2 = 0.0;
  std::size_t cnt = 0;
  for (std::size_t i = 0; i < n; ++i) {
    float v = x[i];
    if (!std::isfinite(v))
      continue;
    sum += v;
    sum2 += static_cast<double>(v) * v;
    ++cnt;
  }
  if (cnt < 2)
    return;
  double mean = sum / cnt;
  double var = sum2 / cnt - mean * mean;
  if (var <= 0.0)
    return;
  double sd = std::sqrt(var);
  for (std::size_t i = 0; i < n; ++i) {
    if (!std::isfinite(x[i]))
      continue;
    x[i] = static_cast<float>((static_cast<double>(x[i]) - mean) / sd);
  }
}

void pct_rank(float *x, std::size_t n) {
  std::vector<std::size_t> &idx = scratch_idx(n);
  for (std::size_t i = 0; i < n; ++i) {
    if (std::isfinite(x[i]))
      idx.push_back(i);
  }
  if (idx.empty())
    return;

  std::sort(idx.begin(), idx.end(),
            [&](std::size_t a, std::size_t b) { return x[a] < x[b]; });

  std::size_t m = idx.size();
  std::size_t i = 0;
  while (i < m) {
    std::size_t j = i + 1;
    while (j < m && x[idx[j]] == x[idx[i]])
      ++j;
    float avg_rank = static_cast<float>(i + 1 + j) * 0.5f;
    float pct = (m > 1) ? (avg_rank - 1.0f) / static_cast<float>(m - 1)
                        : 0.5f;
    for (std::size_t k = i; k < j; ++k)
      x[idx[k]] = pct;
    i = j;
  }
}

// 行业 + log(mcap) 截面中性化 (Frisch-Waugh-Lovell 等价):
//   y ~ 1 + log(mcap) + 行业 dummy 的残差 == 先行业内 demean(y, logmc),
//   再 demean 后 y 对 demean 后 logmc 做标量回归取残差. O(n) 且残差恒等.
//   y/logmc 任一 NaN, 或 industry 越界 → 该样本不参与, 残差留 NaN (下游均值填充).
void neutralize(float *y, const float *logmc, const float *industry,
                std::size_t n) {
  constexpr int K = SW2021_L1_COUNT;
  std::array<double, SW2021_L1_COUNT> sy{}, sl{}, cnt{};
  for (std::size_t a = 0; a < n; ++a) {
    if (!std::isfinite(y[a]) || !std::isfinite(logmc[a]))
      continue;
    int id = static_cast<int>(industry[a]);
    if (id < 0 || id >= K)
      continue;
    sy[static_cast<std::size_t>(id)] += static_cast<double>(y[a]);
    sl[static_cast<std::size_t>(id)] += static_cast<double>(logmc[a]);
    cnt[static_cast<std::size_t>(id)] += 1.0;
  }
  std::array<float, SW2021_L1_COUNT> my{}, ml{};
  for (int i = 0; i < K; ++i) {
    if (cnt[static_cast<std::size_t>(i)] > 0.0) {
      my[static_cast<std::size_t>(i)] = static_cast<float>(sy[static_cast<std::size_t>(i)] / cnt[static_cast<std::size_t>(i)]);
      ml[static_cast<std::size_t>(i)] = static_cast<float>(sl[static_cast<std::size_t>(i)] / cnt[static_cast<std::size_t>(i)]);
    }
  }
  // demean 后标量回归: b = Σ(y_dm·lm_dm) / Σ(lm_dm²)
  double num = 0.0, den = 0.0;
  for (std::size_t a = 0; a < n; ++a) {
    if (!std::isfinite(y[a]) || !std::isfinite(logmc[a]))
      continue;
    int id = static_cast<int>(industry[a]);
    if (id < 0 || id >= K)
      continue;
    if (cnt[static_cast<std::size_t>(id)] <= 0.0)
      continue;
    double ydm = static_cast<double>(y[a]) - my[static_cast<std::size_t>(id)];
    double ldm = static_cast<double>(logmc[a]) - ml[static_cast<std::size_t>(id)];
    num += ydm * ldm;
    den += ldm * ldm;
  }
  float b = (den > 0.0) ? static_cast<float>(num / den) : 0.0f;
  for (std::size_t a = 0; a < n; ++a) {
    if (!std::isfinite(y[a]) || !std::isfinite(logmc[a])) {
      y[a] = nanf_();
      continue;
    }
    int id = static_cast<int>(industry[a]);
    if (id < 0 || id >= K || cnt[static_cast<std::size_t>(id)] <= 0.0) {
      y[a] = nanf_();
      continue;
    }
    float ydm = y[a] - my[static_cast<std::size_t>(id)];
    float ldm = logmc[a] - ml[static_cast<std::size_t>(id)];
    y[a] = ydm - b * ldm;
  }
}

// NaN → finite 均值; 全缺失 → 全 0 (只表达"无横截面信息", 不制造排序差异)
void mean_fill(float *y, std::size_t n) {
  double sum = 0.0;
  std::size_t cnt = 0;
  for (std::size_t i = 0; i < n; ++i) {
    if (!std::isfinite(y[i]))
      continue;
    sum += y[i];
    ++cnt;
  }
  if (cnt == 0) {
    std::fill(y, y + n, 0.0f);
    return;
  }
  float mean = static_cast<float>(sum / static_cast<double>(cnt));
  for (std::size_t i = 0; i < n; ++i)
    if (!std::isfinite(y[i]))
      y[i] = mean;
}

// 普通方法: rank → inverse normal, 缺失 → 0
void rank_inverse_normal(float *y, std::size_t n) {
  thread_local std::vector<std::pair<float, std::size_t>> sv;
  sv.clear();
  sv.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    if (std::isfinite(y[i]))
      sv.emplace_back(y[i], i);
  }
  std::fill(y, y + n, 0.0f);
  const std::size_t N = sv.size();
  if (N == 0)
    return;
  std::sort(sv.begin(), sv.end());
  const float scale = 1.0f / (static_cast<float>(N) + 1.0f);
  for (std::size_t rank = 0; rank < N; ++rank)
    y[sv[rank].second] = inverse_normal_cdf((static_cast<float>(rank) + 1.0f) * scale);
}

} // anonymous namespace

void prepare_logmc(float *mcap, std::size_t n) {
  for (std::size_t i = 0; i < n; ++i) {
    float v = mcap[i];
    mcap[i] = (std::isfinite(v) && v > 0.0f) ? std::log(v) : nanf_();
  }
}

void apply(Method m, Transform tf, float *y, std::size_t n,
           const float *logmc, const float *industry) {
  if (tf == Transform::Reciprocal) {
    // qmt ep/bp/sp/cp 口径: 非 finite 或 0 → NaN, 否则 1/x
    for (std::size_t i = 0; i < n; ++i) {
      float v = y[i];
      y[i] = (std::isfinite(v) && v != 0.0f) ? 1.0f / v : nanf_();
    }
  }

  switch (m) {
  case Method::NormRank:
    rank_inverse_normal(y, n);
    break;
  case Method::WinsorRank:
    winsor_mad(y, n, 3.0f);
    z(y, n);
    pct_rank(y, n);
    mean_fill(y, n);
    break;
  case Method::NeutralRank:
    assert(logmc != nullptr && industry != nullptr);
    winsorize_quantile(y, n, 0.01f, 0.99f);
    neutralize(y, logmc, industry, n);
    z(y, n);
    pct_rank(y, n);
    mean_fill(y, n);
    break;
  }
}

} // namespace cs
