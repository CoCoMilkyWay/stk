#pragma once

// =============================================================================
// ROLL 跨日滚动算子 (D 窗; 契约见 Kernel.hpp; 表见 OpTable.hpp OP_ROLL1/2)
// =============================================================================
//   push(x) 每期一次 (日频序列 = 每交易日一次), ring 存最近 d 期原值 (含 NaN); 窗未满 → NaN.
//   满窗后在窗上重算 (O(d), d ≤ 250): 与向量式 unfold 逐窗重算严格同义, 无递推漂移, 窗内 NaN 自然跳过.
//   集合型在 x_t 为 NaN 时仍返回窗统计; 相对型 (Rank/Z/Resid) x_t 为 NaN → NaN.
//   TsEma 无窗 (k 系数), 首个有效值起递推, NaN 保持.
// =============================================================================

#include "factor/stream/Kernel.hpp"

namespace factor {

namespace win {

// 窗内有效值 (double), 按时间序 (最旧 → 最新); idx = 对应期序 i ∈ 0..d−1
struct Valid {
  std::vector<double> v;
  std::vector<size_t> idx;
  void collect(const Ring &r) {
    v.clear(), idx.clear();
    for (size_t i = 0; i < r.size(); ++i)
      if (ok(r.at(i)))
        v.push_back(r.at(i)), idx.push_back(i);
  }
  size_t n() const { return v.size(); }
  double sum() const {
    double s = 0;
    for (double x : v)
      s += x;
    return s;
  }
  double mean() const { return sum() / static_cast<double>(v.size()); }
  double central(int k, double mu) const { // Σ(x−μ)^k
    double s = 0;
    for (double x : v)
      s += std::pow(x - mu, k);
    return s;
  }
};

} // namespace win

// 一元窗算子骨架: 派生只写 value(Valid&, Ring&)
template <class Derived>
struct Roll1 {
  explicit Roll1(int d) : r_(d) {}
  void reset() { r_.reset(); }
  float push(float x) {
    r_.push(x);
    if (!r_.full())
      return kNaN;
    w_.collect(r_);
    return static_cast<Derived *>(this)->value(w_, r_);
  }
  Ring r_;
  win::Valid w_;
};

// ---- 一元 ----
struct TsDelay : Roll1<TsDelay> {
  explicit TsDelay(const Param &p) : Roll1(p.d + 1) {}
  float value(const win::Valid &, const Ring &r) const { return r.oldest(); }
};
struct TsDelta : Roll1<TsDelta> {
  explicit TsDelta(const Param &p) : Roll1(p.d + 1) {}
  float value(const win::Valid &, const Ring &r) const { return r.newest() - r.oldest(); }
};
struct TsSum : Roll1<TsSum> {
  explicit TsSum(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) const { return w.n() ? static_cast<float>(w.sum()) : kNaN; }
};
struct TsMean : Roll1<TsMean> {
  explicit TsMean(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) const { return w.n() ? static_cast<float>(w.mean()) : kNaN; }
};
struct TsVar : Roll1<TsVar> {
  explicit TsVar(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) const { return w.n() < 2 ? kNaN : static_cast<float>(w.central(2, w.mean()) / static_cast<double>(w.n() - 1)); }
};
struct TsStd : Roll1<TsStd> {
  explicit TsStd(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) const { return w.n() < 2 ? kNaN : static_cast<float>(std::sqrt(w.central(2, w.mean()) / static_cast<double>(w.n() - 1))); }
};
struct TsSkew : Roll1<TsSkew> {
  explicit TsSkew(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) const {
    if (w.n() < 3)
      return kNaN;
    const double mu = w.mean(), n = static_cast<double>(w.n()), m2 = w.central(2, mu) / n, m3 = w.central(3, mu) / n;
    return m2 <= 0 ? kNaN : static_cast<float>(m3 / std::pow(m2, 1.5));
  }
};
struct TsKurt : Roll1<TsKurt> {
  explicit TsKurt(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) const {
    if (w.n() < 4)
      return kNaN;
    const double mu = w.mean(), n = static_cast<double>(w.n()), m2 = w.central(2, mu) / n, m4 = w.central(4, mu) / n;
    return m2 <= 0 ? kNaN : static_cast<float>(m4 / (m2 * m2) - 3.0);
  }
};

// Max / Min / ArgMax / ArgMin: 首个 (最旧) 极值, 严格比较; Arg = 距今期数 d−1−i
template <bool IS_MAX, bool ARG>
struct TsExtreme : Roll1<TsExtreme<IS_MAX, ARG>> {
  explicit TsExtreme(const Param &p) : Roll1<TsExtreme>(p.d) {}
  float value(const win::Valid &w, const Ring &r) const {
    if (!w.n())
      return kNaN;
    size_t best = 0;
    for (size_t j = 1; j < w.n(); ++j)
      if (IS_MAX ? w.v[j] > w.v[best] : w.v[j] < w.v[best])
        best = j;
    return ARG ? static_cast<float>(r.size() - 1 - w.idx[best]) : static_cast<float>(w.v[best]);
  }
};
using TsMax = TsExtreme<true, false>;
using TsMin = TsExtreme<false, false>;
using TsArgMax = TsExtreme<true, true>;
using TsArgMin = TsExtreme<false, true>;

struct TsMed : Roll1<TsMed> {
  explicit TsMed(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) {
    tmp_.assign(w.v.begin(), w.v.end());
    return stat::median(tmp_);
  }
  std::vector<float> tmp_;
};
struct TsMad : Roll1<TsMad> {
  explicit TsMad(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) {
    tmp_.assign(w.v.begin(), w.v.end());
    const float med = stat::median(tmp_);
    for (float &x : tmp_)
      x = std::fabs(x - med);
    return stat::median(tmp_);
  }
  std::vector<float> tmp_;
};
struct TsRank : Roll1<TsRank> {
  explicit TsRank(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &r) {
    if (!ok(r.newest()))
      return kNaN;
    tmp_.assign(w.v.begin(), w.v.end());
    return stat::pct_rank(tmp_, r.newest());
  }
  std::vector<float> tmp_;
};
struct TsZ : Roll1<TsZ> {
  explicit TsZ(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &r) const {
    if (!ok(r.newest()) || w.n() < 2)
      return kNaN;
    const double mu = w.mean(), var = w.central(2, mu) / static_cast<double>(w.n() - 1);
    return var <= 0 ? kNaN : static_cast<float>((r.newest() - mu) / std::sqrt(var));
  }
};
struct TsWma : Roll1<TsWma> { // w_i = i + 1 (最旧 1 … 最新 d), 只在有效值上归一
  explicit TsWma(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) const {
    double sxw = 0, sw = 0;
    for (size_t j = 0; j < w.n(); ++j) {
      const double wt = static_cast<double>(w.idx[j] + 1);
      sxw += wt * w.v[j], sw += wt;
    }
    return sw > 0 ? static_cast<float>(sxw / sw) : kNaN;
  }
};
struct TsProduct : Roll1<TsProduct> {
  explicit TsProduct(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) const {
    if (!w.n())
      return kNaN;
    double p = 1;
    for (double x : w.v)
      p *= 1.0 + x;
    return static_cast<float>(p - 1.0);
  }
};
struct TsSlope : Roll1<TsSlope> { // x 对期序 i 的 OLS 斜率
  explicit TsSlope(const Param &p) : Roll1(p.d) {}
  float value(const win::Valid &w, const Ring &) const {
    if (w.n() < 2)
      return kNaN;
    double mi = 0;
    for (size_t i : w.idx)
      mi += static_cast<double>(i);
    mi /= static_cast<double>(w.n());
    const double mx = w.mean();
    double num = 0, den = 0;
    for (size_t j = 0; j < w.n(); ++j) {
      const double di = static_cast<double>(w.idx[j]) - mi;
      num += di * (w.v[j] - mx), den += di * di;
    }
    return den > 0 ? static_cast<float>(num / den) : kNaN;
  }
};
struct TsCountGt : Roll1<TsCountGt> {
  explicit TsCountGt(const Param &p) : Roll1(p.d), k_(p.k) {}
  float value(const win::Valid &w, const Ring &) const {
    size_t c = 0;
    for (double x : w.v)
      c += x > k_;
    return static_cast<float>(c);
  }
  float k_;
};

struct TsEma { // 无窗: y = k·x + (1−k)·y, 首个有效 x 起; x 为 NaN → 保持 y
  explicit TsEma(const Param &p) : k_(p.k) {}
  void reset() { y_ = kNaN; }
  float push(float x) {
    if (ok(x))
      y_ = ok(y_) ? k_ * x + (1.f - k_) * y_ : x;
    return y_;
  }
  float k_;
  float y_ = kNaN;
};

// ---- 二元: 两 ring 同步, 只吃两侧皆有效的样本 (CoStats 在窗上重算) ----
template <class Derived>
struct Roll2 {
  explicit Roll2(int d) : rx_(d), ry_(d) {}
  void reset() { rx_.reset(), ry_.reset(); }
  float push(float x, float y) {
    rx_.push(x), ry_.push(y);
    if (!rx_.full())
      return kNaN;
    c_.reset();
    for (size_t i = 0; i < rx_.size(); ++i)
      if (ok(rx_.at(i)) && ok(ry_.at(i)))
        c_.add(rx_.at(i), ry_.at(i));
    return static_cast<Derived *>(this)->value(c_, rx_, ry_);
  }
  Ring rx_, ry_;
  CoStats c_;
};

struct TsCov : Roll2<TsCov> {
  explicit TsCov(const Param &p) : Roll2(p.d) {}
  float value(const CoStats &c, const Ring &, const Ring &) const { return c.cov(); }
};
struct TsCorr : Roll2<TsCorr> {
  explicit TsCorr(const Param &p) : Roll2(p.d) {}
  float value(const CoStats &c, const Ring &, const Ring &) const { return c.corr(); }
};
struct TsBeta : Roll2<TsBeta> {
  explicit TsBeta(const Param &p) : Roll2(p.d) {}
  float value(const CoStats &c, const Ring &, const Ring &) const { return c.beta(); }
};
struct TsResid : Roll2<TsResid> {
  explicit TsResid(const Param &p) : Roll2(p.d) {}
  float value(const CoStats &c, const Ring &rx, const Ring &ry) const {
    return (ok(rx.newest()) && ok(ry.newest())) ? c.resid(rx.newest(), ry.newest()) : kNaN;
  }
};
struct TsWMean : Roll2<TsWMean> { // Σ y·x / Σ y
  explicit TsWMean(const Param &p) : Roll2(p.d) {}
  float value(const CoStats &c, const Ring &rx, const Ring &ry) const {
    double sxw = 0, sw = 0;
    for (size_t i = 0; i < rx.size(); ++i)
      if (ok(rx.at(i)) && ok(ry.at(i)))
        sxw += static_cast<double>(rx.at(i)) * ry.at(i), sw += ry.at(i);
    (void)c;
    return sw != 0 ? static_cast<float>(sxw / sw) : kNaN;
  }
};

} // namespace factor
