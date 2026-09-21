#pragma once

// =============================================================================
// CS 截面算子 (契约见 Kernel.hpp; 表见 OpTable.hpp OP_CS1/2/3)
// =============================================================================
//   apply(x[, y[, z]], out, n, p): 一个时刻的截面 (n 资产), out 与输入不重叠.
//   前六个一元 = Method/CS.hpp 的 cs:: 方法原样套用 (含它们的 均值填充 / 零填充 口径, qmt 忠实性不动);
//   其余为因子层新算子, NaN 保持 (无效资产输出 NaN, 不填充).
//   分组 (GroupMean / GroupRank / GroupResid): 分组列取整为 id, 非 finite → 该资产不参与, 输出 NaN.
// =============================================================================

#include "factor/stream/Kernel.hpp"
#include "features/Method/CS.hpp"

namespace factor {

namespace cs_util {

// 有效子集上的 pct rank (并列均秩), 无效 → NaN
inline void pct_rank(const float *x, float *out, size_t n) {
  thread_local std::vector<size_t> idx;
  idx.clear();
  for (size_t i = 0; i < n; ++i)
    if (ok(x[i]))
      idx.push_back(i);
    else
      out[i] = kNaN;
  const size_t m = idx.size();
  std::sort(idx.begin(), idx.end(), [&](size_t a, size_t b) { return x[a] < x[b]; });
  for (size_t i = 0; i < m;) {
    size_t j = i + 1;
    while (j < m && x[idx[j]] == x[idx[i]])
      ++j;
    const double avg_rank = static_cast<double>(i + 1 + j) * 0.5; // 1-based
    const float pct = m > 1 ? static_cast<float>((avg_rank - 1.0) / static_cast<double>(m - 1)) : 0.5f;
    for (size_t k = i; k < j; ++k)
      out[idx[k]] = pct;
    i = j;
  }
}

// 分组 id: floor(by), 非 finite → −1
inline int gid(float by) { return ok(by) ? static_cast<int>(std::floor(by)) : -1; }

// 组内 pct rank: 按 (组, 值) 排序后逐组走并列
inline void group_rank(const float *x, const float *by, float *out, size_t n) {
  thread_local std::vector<size_t> idx;
  idx.clear();
  for (size_t i = 0; i < n; ++i)
    if (ok(x[i]) && gid(by[i]) >= 0)
      idx.push_back(i);
    else
      out[i] = kNaN;
  std::sort(idx.begin(), idx.end(), [&](size_t a, size_t b) {
    const int ga = gid(by[a]), gb = gid(by[b]);
    return ga != gb ? ga < gb : x[a] < x[b];
  });
  for (size_t g0 = 0; g0 < idx.size();) {
    size_t g1 = g0 + 1;
    while (g1 < idx.size() && gid(by[idx[g1]]) == gid(by[idx[g0]]))
      ++g1;
    const size_t m = g1 - g0;
    for (size_t i = g0; i < g1;) {
      size_t j = i + 1;
      while (j < g1 && x[idx[j]] == x[idx[i]])
        ++j;
      const double avg_rank = static_cast<double>((i - g0) + 1 + (j - g0)) * 0.5;
      const float pct = m > 1 ? static_cast<float>((avg_rank - 1.0) / static_cast<double>(m - 1)) : 0.5f;
      for (size_t k = i; k < j; ++k)
        out[idx[k]] = pct;
      i = j;
    }
    g0 = g1;
  }
}

// 两列皆有效样本的协方差统计
inline CoStats costats(const float *x, const float *y, size_t n) {
  CoStats c;
  for (size_t i = 0; i < n; ++i)
    if (ok(x[i]) && ok(y[i]))
      c.add(x[i], y[i]);
  return c;
}

inline void fill(float *out, size_t n, float v) { std::fill(out, out + n, v); }

} // namespace cs_util

// ---- 一元: cs:: 方法套用 ----
#define FACTOR_CS_WRAP(Name, Method)                                         \
  struct Name {                                                              \
    static void apply(const float *x, float *out, size_t n, const Param &) { \
      std::copy(x, x + n, out);                                              \
      cs::Method::apply(out, n);                                             \
    }                                                                        \
  };
FACTOR_CS_WRAP(CsRank, Rank)
FACTOR_CS_WRAP(CsNormRank, NormRank)
FACTOR_CS_WRAP(CsWinsorRank, WinsorRank)
FACTOR_CS_WRAP(CsDemean, Demean)
FACTOR_CS_WRAP(CsZ, Z)
FACTOR_CS_WRAP(CsWinsorZ, WinsorZ)
#undef FACTOR_CS_WRAP

// ---- 一元: 聚合广播 ----
struct CsMean {
  static void apply(const float *x, float *out, size_t n, const Param &) {
    double s = 0;
    size_t m = 0;
    for (size_t i = 0; i < n; ++i)
      if (ok(x[i]))
        s += x[i], ++m;
    cs_util::fill(out, n, m ? static_cast<float>(s / static_cast<double>(m)) : kNaN);
  }
};
struct CsMedian {
  static void apply(const float *x, float *out, size_t n, const Param &) {
    thread_local std::vector<float> v;
    v.clear();
    for (size_t i = 0; i < n; ++i)
      if (ok(x[i]))
        v.push_back(x[i]);
    cs_util::fill(out, n, stat::median(v));
  }
};
struct CsStd {
  static void apply(const float *x, float *out, size_t n, const Param &) {
    Moments m;
    for (size_t i = 0; i < n; ++i)
      if (ok(x[i]))
        m.add(x[i]);
    cs_util::fill(out, n, m.std());
  }
};
struct CsBucket {
  static void apply(const float *x, float *out, size_t n, const Param &p) {
    cs_util::pct_rank(x, out, n);
    const float k = p.k;
    for (size_t i = 0; i < n; ++i)
      if (ok(out[i]))
        out[i] = std::min(std::floor(out[i] * k), k - 1.f);
  }
};

// ---- 二元 ----
struct CsResid { // x 对 y 含截距 OLS 残差
  static void apply(const float *x, const float *y, float *out, size_t n, const Param &) {
    const CoStats c = cs_util::costats(x, y, n);
    for (size_t i = 0; i < n; ++i)
      out[i] = (ok(x[i]) && ok(y[i])) ? c.resid(x[i], y[i]) : kNaN;
  }
};
struct CsBeta {
  static void apply(const float *x, const float *y, float *out, size_t n, const Param &) {
    cs_util::fill(out, n, cs_util::costats(x, y, n).beta());
  }
};
struct CsCorr {
  static void apply(const float *x, const float *y, float *out, size_t n, const Param &) {
    cs_util::fill(out, n, cs_util::costats(x, y, n).corr());
  }
};
struct CsRankDiff {
  static void apply(const float *x, const float *y, float *out, size_t n, const Param &) {
    thread_local std::vector<float> ry;
    ry.resize(n);
    cs_util::pct_rank(x, out, n);
    cs_util::pct_rank(y, ry.data(), n);
    for (size_t i = 0; i < n; ++i)
      out[i] -= ry[i]; // 任一 NaN 自然传播
  }
};
struct CsGroupMean { // y = 分组列
  static void apply(const float *x, const float *y, float *out, size_t n, const Param &) {
    int gmax = -1;
    for (size_t i = 0; i < n; ++i)
      gmax = std::max(gmax, cs_util::gid(y[i]));
    thread_local std::vector<double> s;
    thread_local std::vector<size_t> c;
    s.assign(static_cast<size_t>(gmax + 1), 0.0), c.assign(static_cast<size_t>(gmax + 1), 0);
    for (size_t i = 0; i < n; ++i) {
      const int g = cs_util::gid(y[i]);
      if (g >= 0 && ok(x[i]))
        s[static_cast<size_t>(g)] += x[i], ++c[static_cast<size_t>(g)];
    }
    for (size_t i = 0; i < n; ++i) {
      const int g = cs_util::gid(y[i]);
      out[i] = (g >= 0 && ok(x[i]) && c[static_cast<size_t>(g)]) ? static_cast<float>(s[static_cast<size_t>(g)] / static_cast<double>(c[static_cast<size_t>(g)])) : kNaN;
    }
  }
};
struct CsGroupRank { // y = 分组列
  static void apply(const float *x, const float *y, float *out, size_t n, const Param &) { cs_util::group_rank(x, y, out, n); }
};
struct CsCondRank { // y 分 k 桶, x 在桶内 rank
  static void apply(const float *x, const float *y, float *out, size_t n, const Param &p) {
    thread_local std::vector<float> b;
    b.resize(n);
    CsBucket::apply(y, b.data(), n, p);
    cs_util::group_rank(x, b.data(), out, n);
  }
};

// ---- 三元 ----
struct CsGroupResid { // z = 分组列: x, y 组内 demean → x 对 y 标量回归残差 (与 cs::neutralize 同式: den ≤ 0 → β = 0)
  static void apply(const float *x, const float *y, const float *z, float *out, size_t n, const Param &) {
    int gmax = -1;
    for (size_t i = 0; i < n; ++i)
      gmax = std::max(gmax, cs_util::gid(z[i]));
    const size_t G = static_cast<size_t>(gmax + 1);
    thread_local std::vector<double> sx, sy;
    thread_local std::vector<size_t> c;
    sx.assign(G, 0.0), sy.assign(G, 0.0), c.assign(G, 0);
    auto in = [&](size_t i) { return ok(x[i]) && ok(y[i]) && cs_util::gid(z[i]) >= 0; };
    for (size_t i = 0; i < n; ++i)
      if (in(i)) {
        const size_t g = static_cast<size_t>(cs_util::gid(z[i]));
        sx[g] += x[i], sy[g] += y[i], ++c[g];
      }
    double num = 0, den = 0;
    for (size_t i = 0; i < n; ++i)
      if (in(i)) {
        const size_t g = static_cast<size_t>(cs_util::gid(z[i]));
        const double xd = x[i] - sx[g] / static_cast<double>(c[g]), yd = y[i] - sy[g] / static_cast<double>(c[g]);
        num += xd * yd, den += yd * yd;
      }
    const double b = den > 0 ? num / den : 0.0;
    for (size_t i = 0; i < n; ++i) {
      if (!in(i)) {
        out[i] = kNaN;
        continue;
      }
      const size_t g = static_cast<size_t>(cs_util::gid(z[i]));
      const double xd = x[i] - sx[g] / static_cast<double>(c[g]), yd = y[i] - sy[g] / static_cast<double>(c[g]);
      out[i] = static_cast<float>(xd - b * yd);
    }
  }
};

} // namespace factor
