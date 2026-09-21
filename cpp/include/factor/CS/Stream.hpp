#pragma once

// =============================================================================
// CS 流式实现 (实盘: 每分钟一个截面, 一次 apply 处理全市场 A 个资产)
// =============================================================================
//   struct Op { static void apply(x 值/掩码, y 值/掩码, z 值/掩码, out 值/掩码, int A, const Param &); }
//   未用到的输入传 nullptr. 输入输出不重叠.
//   CS 没有"窗"这一属性 (截面本身就是全部样本), 所以行格式与 TS 不同, 见 OpTable.hpp.
//
//   广播型 (CsMean/CsStd/CsMedian/CsQuantile/CsBeta/CsCorr) 对全行写同一个值与同一个掩码,
//   哪怕该资产自己的 x 是缺失的 —— 它描述的是截面而不是资产.
//   相对型 (Demean/Z/Rank/Winsor/Bucket/Resid/RankDiff/Group*) 描述资产自身, x 缺失即无效.
//
//   【precise-math】依赖受控浮点, 编进 -fno-fast-math TU.
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/StreamHist.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

namespace factor::cs {

namespace detail {

inline Val at(const float *v, const uint8_t *m, int i) { return Val{v[i], m[i] != 0}; }
inline void put(float *v, uint8_t *m, int i, Val a) { v[i] = a.v, m[i] = a.m ? 1 : 0; }
inline void broadcast(float *v, uint8_t *m, int A, Val a) {
  for (int i = 0; i < A; ++i)
    put(v, m, i, a);
}

inline std::vector<float> gather(const float *v, const uint8_t *m, int A) {
  std::vector<float> s;
  s.reserve(static_cast<size_t>(A));
  for (int i = 0; i < A; ++i)
    if (m[i])
      s.push_back(v[i]);
  return s;
}

// 一元矩 (两遍中心化, 不用 Σx²−nμ²)
struct M1 {
  int n = 0;
  double mean = 0, m2 = 0, sx2 = 0;
  bool disp() const { return disp_ok(m2, sx2); }
};
inline M1 moments(const float *v, const uint8_t *m, int A) {
  M1 r;
  double s = 0;
  for (int i = 0; i < A; ++i)
    if (m[i])
      s += v[i], r.sx2 += static_cast<double>(v[i]) * v[i], ++r.n;
  if (r.n == 0)
    return r;
  r.mean = s / r.n;
  for (int i = 0; i < A; ++i)
    if (m[i]) {
      const double d = v[i] - r.mean;
      r.m2 += d * d;
    }
  return r;
}

// 二元共矩 (两遍)
struct M2 {
  int n = 0;
  double mx = 0, my = 0, cxx = 0, cyy = 0, cxy = 0, sx2 = 0, sy2 = 0;
  bool dx_ok() const { return disp_ok(cxx, sx2); }
  bool dy_ok() const { return disp_ok(cyy, sy2); }
};
inline M2 comoments(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, int A) {
  M2 r;
  double sx = 0, sy = 0;
  for (int i = 0; i < A; ++i)
    if (xm[i] && ym[i]) {
      sx += xv[i], sy += yv[i];
      r.sx2 += static_cast<double>(xv[i]) * xv[i], r.sy2 += static_cast<double>(yv[i]) * yv[i];
      ++r.n;
    }
  if (r.n == 0)
    return r;
  r.mx = sx / r.n, r.my = sy / r.n;
  for (int i = 0; i < A; ++i)
    if (xm[i] && ym[i]) {
      const double dx = xv[i] - r.mx, dy = yv[i] - r.my;
      r.cxx += dx * dx, r.cyy += dy * dy, r.cxy += dx * dy;
    }
  return r;
}

inline stream::Hist hist_of(const float *v, const uint8_t *m, int A) {
  stream::Hist h;
  h.build(gather(v, m, A));
  return h;
}

// 分位缩尾: 返回逐资产 clamp 后的值 (无效位置原样, 由掩码屏蔽)
inline std::vector<float> winsorize(const float *v, int A, const stream::Hist &h, double q) {
  const float a = h.quantile(q), b = h.quantile(1.0 - q);
  std::vector<float> w(static_cast<size_t>(A));
  for (int i = 0; i < A; ++i)
    w[i] = std::clamp(v[i], std::fmin(a, b), std::fmax(a, b));
  return w;
}

inline constexpr double kWinsorQ = 0.01; // CsWinsorRank / CsWinsorZ 的固定缩尾分位

// 组 id: y 无效或为负 → 不参与
inline int gid(const float *v, const uint8_t *m, int i) {
  return m[i] ? static_cast<int>(std::floor(v[i])) : -1;
}
inline int max_gid(const float *v, const uint8_t *m, int A) {
  int g = -1;
  for (int i = 0; i < A; ++i)
    g = std::max(g, gid(v, m, i));
  assert(g < (1 << 16)); // 组 id 当下标用, 防止 y 传进来的不是分组列
  return g;
}

} // namespace detail

// =========================== 矩族 (REDUCE) ===========================

struct CsMean {
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto s = detail::moments(xv, xm, A);
    detail::broadcast(ov, om, A, mk(s.mean, s.n >= 1));
  }
};

struct CsStd {
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto s = detail::moments(xv, xm, A);
    detail::broadcast(ov, om, A, mk(std::sqrt(s.m2 / (s.n - 1)), s.n >= 2 && s.disp()));
  }
};

struct CsDemean {
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto s = detail::moments(xv, xm, A);
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk(xv[i] - s.mean, xm[i] && s.n >= 1));
  }
};

struct CsZ {
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto s = detail::moments(xv, xm, A);
    const float sd = guard(static_cast<float>(std::sqrt(s.m2 / std::max(1, s.n - 1))));
    const bool ok = s.n >= 2 && s.disp();
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk((xv[i] - s.mean) / sd, xm[i] && ok));
  }
};

struct CsResid { // x 对 y 的截面 OLS (含截距) 残差
  static void apply(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto s = detail::comoments(xv, xm, yv, ym, A);
    const double b = s.cxy / guard(static_cast<float>(s.cyy));
    const bool ok = s.n >= 2 && s.dy_ok();
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk((xv[i] - s.mx) - b * (yv[i] - s.my), xm[i] && ym[i] && ok));
  }
};

struct CsBeta {
  static void apply(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto s = detail::comoments(xv, xm, yv, ym, A);
    detail::broadcast(ov, om, A, mk(s.cxy / guard(static_cast<float>(s.cyy)), s.n >= 2 && s.dy_ok()));
  }
};

struct CsCorr {
  static void apply(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto s = detail::comoments(xv, xm, yv, ym, A);
    detail::broadcast(ov, om, A,
                      mk(s.cxy / std::sqrt(s.cxx * s.cyy), s.n >= 2 && s.dx_ok() && s.dy_ok()));
  }
};

// =========================== 序统计族 (HIST) ===========================

struct CsRank {
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto h = detail::hist_of(xv, xm, A);
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk(h.rank(xv[i]), xm[i] && h.n >= 1));
  }
};

struct CsNormRank { // pct 先夹到 (0,1) 开区间再取正态分位, 否则 ±inf
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto h = detail::hist_of(xv, xm, A);
    const double lo = 1.0 / (h.n + 1.0), hi = static_cast<double>(h.n) / (h.n + 1.0);
    for (int i = 0; i < A; ++i) {
      // probit 只在掩码成立时求值: 截面全缺失 (n = 0) 时 lo/hi 倒挂, clamp 的前置条件不成立
      const bool ok = xm[i] && h.n >= 1;
      detail::put(ov, om, i, mk(ok ? probit(std::clamp<double>(h.rank(xv[i]), lo, hi)) : 0.f, ok));
    }
  }
};

struct CsMedian {
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto h = detail::hist_of(xv, xm, A);
    detail::broadcast(ov, om, A, mk(h.quantile(0.5), h.n >= 1));
  }
};

struct CsQuantile {
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &p) {
    const auto h = detail::hist_of(xv, xm, A);
    detail::broadcast(ov, om, A, mk(h.quantile(p.k), h.n >= 1));
  }
};

struct CsWinsor {
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &p) {
    const auto h = detail::hist_of(xv, xm, A);
    const auto w = detail::winsorize(xv, A, h, p.k);
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk(w[i], xm[i] && h.n >= 1));
  }
};

struct CsWinsorRank { // 先固定分位缩尾再排名: 极端值不再独占秩尾
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto w = detail::winsorize(xv, A, detail::hist_of(xv, xm, A), detail::kWinsorQ);
    const auto h = detail::hist_of(w.data(), xm, A);
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk(h.rank(w[i]), xm[i] && h.n >= 1));
  }
};

struct CsWinsorZ {
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto w = detail::winsorize(xv, A, detail::hist_of(xv, xm, A), detail::kWinsorQ);
    const auto s = detail::moments(w.data(), xm, A);
    const float sd = guard(static_cast<float>(std::sqrt(s.m2 / std::max(1, s.n - 1))));
    const bool ok = s.n >= 2 && s.disp();
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk((w[i] - s.mean) / sd, xm[i] && ok));
  }
};

struct CsBucket { // 等频分 k 组: floor(pct·k) ∈ 0..k−1
  static void apply(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &p) {
    const auto h = detail::hist_of(xv, xm, A);
    const int k = static_cast<int>(p.k);
    assert(k >= 1);
    for (int i = 0; i < A; ++i) {
      const int b = std::clamp(static_cast<int>(std::floor(h.rank(xv[i]) * k)), 0, k - 1);
      detail::put(ov, om, i, mk(h.spread ? b : 0, xm[i] && h.n >= 1));
    }
  }
};

struct CsRankDiff {
  static void apply(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const auto hx = detail::hist_of(xv, xm, A);
    const auto hy = detail::hist_of(yv, ym, A);
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i,
                  mk(hx.rank(xv[i]) - hy.rank(yv[i]), xm[i] && ym[i] && hx.n >= 1 && hy.n >= 1));
  }
};

// =========================== 分组族 (GROUP) ===========================

struct CsGroupMean { // y = 组 id (行业等)
  static void apply(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const int G = detail::max_gid(yv, ym, A) + 1;
    std::vector<double> s(static_cast<size_t>(std::max(G, 1)), 0.0);
    std::vector<int> c(static_cast<size_t>(std::max(G, 1)), 0);
    for (int i = 0; i < A; ++i) {
      const int g = detail::gid(yv, ym, i);
      if (g >= 0 && xm[i])
        s[g] += xv[i], ++c[g];
    }
    for (int i = 0; i < A; ++i) {
      const int g = detail::gid(yv, ym, i);
      const bool ok = g >= 0 && xm[i] && c[g] >= 1;
      detail::put(ov, om, i, mk(ok ? s[g] / c[g] : 0.0, ok));
    }
  }
};

struct CsGroupRank { // 组内 pct rank
  static void apply(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &) {
    const int G = detail::max_gid(yv, ym, A) + 1;
    std::vector<std::vector<float>> bucket(static_cast<size_t>(std::max(G, 1)));
    for (int i = 0; i < A; ++i) {
      const int g = detail::gid(yv, ym, i);
      if (g >= 0 && xm[i])
        bucket[g].push_back(xv[i]);
    }
    std::vector<stream::Hist> h(static_cast<size_t>(std::max(G, 1)));
    for (int g = 0; g < G; ++g)
      h[g].build(bucket[g]);
    for (int i = 0; i < A; ++i) {
      const int g = detail::gid(yv, ym, i);
      const bool ok = g >= 0 && xm[i];
      detail::put(ov, om, i, mk(ok ? h[g].rank(xv[i]) : 0.f, ok));
    }
  }
};

struct CsCondRank { // 先按 y 等频分 k 桶, 再在桶内对 x 排名 (条件排序, 去掉 y 的一阶影响)
  static void apply(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *,
                    const uint8_t *, float *ov, uint8_t *om, int A, const Param &p) {
    const int k = static_cast<int>(p.k);
    assert(k >= 1);
    // 桶边界只由**双有效**的资产定: y 有效而 x 缺失的资产对任何桶的 x 排名都没有贡献,
    // 让它参与决定 y 的分位边界只会引入与 x 无关的抖动
    std::vector<uint8_t> both(static_cast<size_t>(A));
    for (int i = 0; i < A; ++i)
      both[i] = (xm[i] && ym[i]) ? 1 : 0;
    const auto hy = detail::hist_of(yv, both.data(), A);
    std::vector<std::vector<float>> bucket(static_cast<size_t>(k));
    std::vector<int> bof(static_cast<size_t>(A), -1);
    for (int i = 0; i < A; ++i) {
      if (!both[i])
        continue;
      bof[i] = hy.spread ? std::clamp(static_cast<int>(std::floor(hy.rank(yv[i]) * k)), 0, k - 1) : 0;
      bucket[bof[i]].push_back(xv[i]);
    }
    std::vector<stream::Hist> h(static_cast<size_t>(k));
    for (int b = 0; b < k; ++b)
      h[b].build(bucket[b]);
    for (int i = 0; i < A; ++i) {
      const bool ok = bof[i] >= 0 && hy.n >= 1;
      detail::put(ov, om, i, mk(ok ? h[bof[i]].rank(xv[i]) : 0.f, ok));
    }
  }
};

struct CsGroupResid { // FWL: 组内去均值后再做一次全局回归, 等价于"组固定效应 + y" 的残差
  static void apply(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv,
                    const uint8_t *zm, float *ov, uint8_t *om, int A, const Param &) {
    const int G = detail::max_gid(zv, zm, A) + 1;
    const size_t GS = static_cast<size_t>(std::max(G, 1));
    std::vector<double> sx(GS, 0.0), sy(GS, 0.0);
    std::vector<int> c(GS, 0);
    std::vector<int> g(static_cast<size_t>(A), -1);
    for (int i = 0; i < A; ++i) {
      const int gi = detail::gid(zv, zm, i);
      if (gi < 0 || !xm[i] || !ym[i])
        continue;
      g[i] = gi, sx[gi] += xv[i], sy[gi] += yv[i], ++c[gi];
    }
    double sxy = 0, syy = 0, sy2 = 0;
    int n = 0;
    for (int i = 0; i < A; ++i) {
      if (g[i] < 0)
        continue;
      const double xt = xv[i] - sx[g[i]] / c[g[i]], yt = yv[i] - sy[g[i]] / c[g[i]];
      sxy += xt * yt, syy += yt * yt, sy2 += static_cast<double>(yv[i]) * yv[i], ++n;
    }
    const double b = sxy / guard(static_cast<float>(syy));
    const bool ok = n >= 2 && disp_ok(syy, sy2);
    for (int i = 0; i < A; ++i) {
      if (g[i] < 0) {
        detail::put(ov, om, i, Val{});
        continue;
      }
      const double xt = xv[i] - sx[g[i]] / c[g[i]], yt = yv[i] - sy[g[i]] / c[g[i]];
      detail::put(ov, om, i, mk(xt - b * yt, ok));
    }
  }
};

} // namespace factor::cs
