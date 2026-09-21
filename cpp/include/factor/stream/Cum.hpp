#pragma once

// =============================================================================
// CUM 日内 expanding 算子 (契约见 Kernel.hpp; 表见 OpTable.hpp OP_CUM1/2)
// =============================================================================
//   push(x) 每分钟一次, 返回 "开盘至今" 的统计量; 日界 reset(). t = 本日已 push 次数 (含 NaN, 从 0 起).
//   集合型 (Sum/Mean/Std/…/Hhi/Entropy/Peaks/Count) 在 x 为 NaN 时返回当前集合的值 (跳过不计);
//   相对型 (Rank/Resid) 描述 x_t 自身 → x_t 为 NaN 时返回 NaN.
//   标 * (Rank/TopK) 需存当日全部有效值 (≤ 240 点), 其余 O(1) 状态.
// =============================================================================

#include "factor/stream/Kernel.hpp"

namespace factor {

// ---- 一元 ----
struct CumSum {
  explicit CumSum(const Param &) {}
  void reset() { s_ = 0, n_ = 0; }
  float push(float x) {
    if (ok(x))
      s_ += x, ++n_;
    return n_ ? static_cast<float>(s_) : kNaN;
  }
  double s_ = 0;
  size_t n_ = 0;
};

struct CumMean {
  explicit CumMean(const Param &) {}
  void reset() { s_ = 0, n_ = 0; }
  float push(float x) {
    if (ok(x))
      s_ += x, ++n_;
    return n_ ? static_cast<float>(s_ / static_cast<double>(n_)) : kNaN;
  }
  double s_ = 0;
  size_t n_ = 0;
};

// Std / Var / Skew / Kurt 共用 Moments, 只差取值
#define FACTOR_CUM_MOMENT(Name, getter) \
  struct Name {                         \
    explicit Name(const Param &) {}     \
    void reset() { m_.reset(); }        \
    float push(float x) {               \
      if (ok(x))                        \
        m_.add(x);                      \
      return m_.getter();               \
    }                                   \
    Moments m_;                         \
  };
FACTOR_CUM_MOMENT(CumStd, std)
FACTOR_CUM_MOMENT(CumVar, var)
FACTOR_CUM_MOMENT(CumSkew, skew)
FACTOR_CUM_MOMENT(CumKurt, kurt)
#undef FACTOR_CUM_MOMENT

// Max / Min / ArgMax / ArgMin 共用极值追踪: 首个 (最早) 极值, 严格比较
template <bool IS_MAX, bool ARG>
struct CumExtreme {
  explicit CumExtreme(const Param &) {}
  void reset() { v_ = kNaN, pos_ = 0, t_ = 0; }
  float push(float x) {
    if (ok(x) && (!ok(v_) || (IS_MAX ? x > v_ : x < v_)))
      v_ = x, pos_ = t_;
    ++t_;
    if constexpr (ARG)
      return ok(v_) ? static_cast<float>(pos_) : kNaN;
    else
      return v_;
  }
  float v_ = kNaN;
  size_t pos_ = 0, t_ = 0;
};
using CumMax = CumExtreme<true, false>;
using CumMin = CumExtreme<false, false>;
using CumArgMax = CumExtreme<true, true>;
using CumArgMin = CumExtreme<false, true>;

struct CumRank { // *
  explicit CumRank(const Param &) { v_.reserve(256); }
  void reset() { v_.clear(); }
  float push(float x) {
    if (!ok(x))
      return kNaN;
    v_.push_back(x);
    return stat::pct_rank(v_, x);
  }
  std::vector<float> v_;
};

struct CumHhi {
  explicit CumHhi(const Param &) {}
  void reset() { s1_ = 0, s2_ = 0; }
  float push(float x) {
    if (ok(x))
      s1_ += x, s2_ += static_cast<double>(x) * x;
    return s1_ != 0 ? static_cast<float>(s2_ / (s1_ * s1_)) : kNaN;
  }
  double s1_ = 0, s2_ = 0;
};

struct CumEntropy { // 只计 x > 0; H = ln Σx − Σ x ln x / Σx
  explicit CumEntropy(const Param &) {}
  void reset() { s_ = 0, sl_ = 0; }
  float push(float x) {
    if (ok(x) && x > 0.f)
      s_ += x, sl_ += static_cast<double>(x) * std::log(static_cast<double>(x));
    return s_ > 0 ? static_cast<float>(std::log(s_) - sl_ / s_) : kNaN;
  }
  double s_ = 0, sl_ = 0;
};

struct CumTopK { // * 前 k 大之和 / Σx
  explicit CumTopK(const Param &p) : k_(static_cast<size_t>(p.k)) { v_.reserve(256); }
  void reset() { v_.clear(), s_ = 0; }
  float push(float x) {
    if (ok(x))
      v_.push_back(x), s_ += x;
    if (v_.size() < k_ || s_ == 0)
      return kNaN;
    std::vector<float> tmp(v_);
    std::nth_element(tmp.begin(), tmp.begin() + static_cast<std::ptrdiff_t>(k_), tmp.end(), std::greater<float>());
    double top = 0;
    for (size_t i = 0; i < k_; ++i)
      top += tmp[i];
    return static_cast<float>(top / s_);
  }
  size_t k_;
  std::vector<float> v_;
  double s_ = 0;
};

struct CumPeaks { // 峰 s 在 s+1 时确认: x_{s−1} < x_s > x_{s+1} 且 x_s > k · CumMean_s (含 s), 三点皆有效
  explicit CumPeaks(const Param &p) : k_(p.k) {}
  void reset() { p2_ = p1_ = kNaN, mean1_ = 0, s_ = 0, n_ = 0, cnt_ = 0; }
  float push(float x) {
    if (ok(p2_) && ok(p1_) && ok(x) && p2_ < p1_ && p1_ > x && p1_ > k_ * mean1_)
      ++cnt_;
    if (ok(x))
      s_ += x, ++n_;
    p2_ = p1_, p1_ = x;
    mean1_ = n_ ? s_ / static_cast<double>(n_) : 0; // = CumMean 到当前 (下一步的 s)
    return static_cast<float>(cnt_);
  }
  float k_;
  float p2_ = kNaN, p1_ = kNaN;
  double mean1_ = 0, s_ = 0;
  size_t n_ = 0, cnt_ = 0;
};

struct CumCountGt {
  explicit CumCountGt(const Param &p) : k_(p.k) {}
  void reset() { cnt_ = 0; }
  float push(float x) {
    if (ok(x) && x > k_)
      ++cnt_;
    return static_cast<float>(cnt_);
  }
  float k_;
  size_t cnt_ = 0;
};

struct TodMask { // 1[k ≤ t < k2], 与 x 无关
  explicit TodMask(const Param &p) : lo_(static_cast<size_t>(p.k)), hi_(static_cast<size_t>(p.k2)) {}
  void reset() { t_ = 0; }
  float push(float) {
    const float r = (t_ >= lo_ && t_ < hi_) ? 1.f : 0.f;
    ++t_;
    return r;
  }
  size_t lo_, hi_, t_ = 0;
};

// ---- 二元 (CoStats 只吃两侧皆有效的样本) ----
#define FACTOR_CUM_CO(Name, expr)       \
  struct Name {                         \
    explicit Name(const Param &) {}     \
    void reset() { c_.reset(); }        \
    float push(float x, float y) {      \
      const bool both = ok(x) && ok(y); \
      if (both)                         \
        c_.add(x, y);                   \
      return expr;                      \
    }                                   \
    CoStats c_;                         \
  };
FACTOR_CUM_CO(CumCov, c_.cov())
FACTOR_CUM_CO(CumCorr, c_.corr())
FACTOR_CUM_CO(CumBeta, c_.beta())
FACTOR_CUM_CO(CumResid, both ? c_.resid(x, y) : kNaN)
#undef FACTOR_CUM_CO

struct CumWMean { // Σ y·x / Σ y
  explicit CumWMean(const Param &) {}
  void reset() { sxw_ = 0, sw_ = 0; }
  float push(float x, float y) {
    if (ok(x) && ok(y))
      sxw_ += static_cast<double>(x) * y, sw_ += y;
    return sw_ != 0 ? static_cast<float>(sxw_ / sw_) : kNaN;
  }
  double sxw_ = 0, sw_ = 0;
};

} // namespace factor
