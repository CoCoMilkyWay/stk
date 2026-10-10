#pragma once

// =============================================================================
// CS 流式实现 (实盘: 每分钟一个截面, 一次 apply 处理全市场 A 个资产)
// =============================================================================
//   struct Op { static void apply(x 值/掩码, y 值/掩码, z 值/掩码, g 池掩码, out 值/掩码, int A, const Param &); }
//   未用到的输入传 nullptr; g 必填 (当日池成员位 [A], 契约【截面池 g】). 输入输出不重叠.
//   CS 没有"窗"这一属性 (截面本身就是全部样本), 所以行格式与 TS 不同, 见 OpTable.hpp.
//
//   样本集 = 池内 ∧ 有效 (统计量只由池内算出); 输出对每个 x 有效的资产写 "池内统计量作用于自己的 x".
//   广播型 (CsMean/CsStd/CsQuantile/CsBeta/CsCorr) 对全行写同一个值, 掩码恒真 (空池行 → 中性值),
//   哪怕该资产自己的 x 是缺失的 —— 它描述的是截面而不是资产.
//   相对型 (Demean/Z/Rank/NormRank/Winsor/Bucket/Resid/Group*) 描述资产自身, x 缺失即无效; 统计量退化 → 契约第 6 条中性值.
//   GROUP: 组内池成员统计量; 该组无池内成员 → 回退全池统计量.
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

// 每分钟一次 apply, 中间量用 thread_local 缓存复用容量 (gather 与 winsorize 会同时存活, 两个独立缓冲;
// 分组族另有一份全池直方图的 gather, 与组内 gather 同时存活 → gather2)
template <int Slot = 0>
inline const std::vector<float> &gather(const float *v, const uint8_t *m, const uint8_t *g, int A) {
  static thread_local std::vector<float> s;
  s.clear();
  for (int i = 0; i < A; ++i)
    if (m[i] && g[i])
      s.push_back(v[i]);
  return s;
}

// 一元矩 (两遍中心化, 不用 Σx²−nμ²); 样本 = m ∧ g; lo/hi 给全并列判据
struct M1 {
  int n = 0;
  double mean = 0, m2 = 0;
  float lo = 0, hi = 0;
  bool disp() const { return spread(lo, hi); }
};
inline M1 moments(const float *v, const uint8_t *m, const uint8_t *g, int A) {
  M1 r;
  double s = 0;
  for (int i = 0; i < A; ++i)
    if (m[i] && g[i]) {
      r.lo = r.n == 0 ? v[i] : std::fmin(r.lo, v[i]);
      r.hi = r.n == 0 ? v[i] : std::fmax(r.hi, v[i]);
      s += v[i], ++r.n;
    }
  if (r.n == 0)
    return r;
  r.mean = s / r.n;
  for (int i = 0; i < A; ++i)
    if (m[i] && g[i]) {
      const double d = v[i] - r.mean;
      r.m2 += d * d;
    }
  return r;
}

// 二元共矩 (两遍)
struct M2 {
  int n = 0;
  double mx = 0, my = 0, cxx = 0, cyy = 0, cxy = 0;
  float lox = 0, hix = 0, loy = 0, hiy = 0;
  bool dx_ok() const { return spread(lox, hix); }
  bool dy_ok() const { return spread(loy, hiy); }
};
inline M2 comoments(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const uint8_t *g, int A) {
  M2 r;
  double sx = 0, sy = 0;
  for (int i = 0; i < A; ++i)
    if (xm[i] && ym[i] && g[i]) {
      const bool first = r.n == 0;
      r.lox = first ? xv[i] : std::fmin(r.lox, xv[i]), r.hix = first ? xv[i] : std::fmax(r.hix, xv[i]);
      r.loy = first ? yv[i] : std::fmin(r.loy, yv[i]), r.hiy = first ? yv[i] : std::fmax(r.hiy, yv[i]);
      sx += xv[i], sy += yv[i];
      ++r.n;
    }
  if (r.n == 0)
    return r;
  r.mx = sx / r.n, r.my = sy / r.n;
  for (int i = 0; i < A; ++i)
    if (xm[i] && ym[i] && g[i]) {
      const double dx = xv[i] - r.mx, dy = yv[i] - r.my;
      r.cxx += dx * dx, r.cyy += dy * dy, r.cxy += dx * dy;
    }
  return r;
}

template <int Slot = 0>
inline stream::Hist hist_of(const float *v, const uint8_t *m, const uint8_t *g, int A) {
  stream::Hist h;
  h.build(gather<Slot>(v, m, g, A));
  return h;
}

// 分位缩尾: 返回逐资产 clamp 后的值 (无效位置原样, 由掩码屏蔽)
inline const std::vector<float> &winsorize(const float *v, int A, const stream::Hist &h, double q) {
  static thread_local std::vector<float> w;
  const float a = h.quantile(q), b = h.quantile(1.0 - q);
  const float lo = std::fmin(a, b), hi = std::fmax(a, b);
  w.resize(static_cast<size_t>(A));
  for (int i = 0; i < A; ++i)
    w[i] = std::fmin(std::fmax(v[i], lo), hi);
  return w;
}

// 组 id: y 无效或为负 → 不参与
inline int gid(const float *v, const uint8_t *m, int i) {
  return m[i] ? static_cast<int>(std::floor(v[i])) : -1;
}
inline int max_gid(const float *v, const uint8_t *m, int A) {
  int g = -1;
  for (int i = 0; i < A; ++i)
    g = std::max(g, gid(v, m, i));
  assert(g < kMaxGroup); // 组 id 当下标用 (三后端同一上限), 防止 y 传进来的不是分组列
  return g;
}

} // namespace detail

// =========================== 矩族 (REDUCE) ===========================

// 统一签名 (g = 当日池成员位 [A], 必填)
#define CS_SIG                                                                                                                  \
  static void apply(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, const uint8_t *zm, \
                    const uint8_t *g, float *ov, uint8_t *om, int A, const Param &p)
#define CS_NO23 (void)yv, (void)ym, (void)zv, (void)zm
#define CS_NO3 (void)zv, (void)zm

struct CsMean { // 空池 → 0 (moments 的 mean 初值)
  CS_SIG {
    CS_NO23, (void)p;
    const auto s = detail::moments(xv, xm, g, A);
    detail::broadcast(ov, om, A, mk(s.mean, true));
  }
};

struct CsStd { // n < 2 / 全并列 → 0
  CS_SIG {
    CS_NO23, (void)p;
    const auto s = detail::moments(xv, xm, g, A);
    detail::broadcast(ov, om, A, mk(s.n >= 2 && s.disp() ? std::sqrt(s.m2 / (s.n - 1)) : 0.0, true));
  }
};

struct CsDemean { // 空池 → μ = 0, 即 x 原值
  CS_SIG {
    CS_NO23, (void)p;
    const auto s = detail::moments(xv, xm, g, A);
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk(xv[i] - s.mean, xm[i]));
  }
};

struct CsZ { // σ 退化 → 0 (无尺度可比)
  CS_SIG {
    CS_NO23, (void)p;
    const auto s = detail::moments(xv, xm, g, A);
    const bool ok = s.n >= 2 && s.disp();
    const double sd = ok ? std::sqrt(s.m2 / (s.n - 1)) : 1.0;
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk(ok ? (xv[i] - s.mean) / sd : 0.0, xm[i]));
  }
};

struct CsResid { // x 对 y 的截面 OLS (含截距) 残差; y 无离散度 → β = 0, 即 x − μ^x
  CS_SIG {
    CS_NO3, (void)p;
    const auto s = detail::comoments(xv, xm, yv, ym, g, A);
    const bool ok = s.n >= 2 && s.dy_ok();
    const double b = ok ? s.cxy / s.cyy : 0.0;
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk((xv[i] - s.mx) - b * (yv[i] - s.my), xm[i] && ym[i]));
  }
};

struct CsBeta { // y 无离散度 → 0
  CS_SIG {
    CS_NO3, (void)p;
    const auto s = detail::comoments(xv, xm, yv, ym, g, A);
    detail::broadcast(ov, om, A, mk(s.n >= 2 && s.dy_ok() ? s.cxy / s.cyy : 0.0, true));
  }
};

struct CsCorr { // 任一侧无离散度 → 0
  CS_SIG {
    CS_NO3, (void)p;
    const auto s = detail::comoments(xv, xm, yv, ym, g, A);
    detail::broadcast(ov, om, A,
                      mk(s.n >= 2 && s.dx_ok() && s.dy_ok() ? s.cxy / std::sqrt(s.cxx * s.cyy) : 0.0, true));
  }
};

// =========================== 序统计族 (HIST) ===========================

struct CsRank { // 空池 / 全并列 → 0.5 (Hist 内)
  CS_SIG {
    CS_NO23, (void)p;
    const auto h = detail::hist_of(xv, xm, g, A);
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk(h.rank(xv[i]), xm[i]));
  }
};

struct CsNormRank { // pct 先夹到 (0,1) 开区间再取正态分位, 否则 ±inf; 空池 → Φ⁻¹(0.5) = 0
  CS_SIG {
    CS_NO23, (void)p;
    const auto h = detail::hist_of(xv, xm, g, A);
    const double lo = 1.0 / (h.n + 1.0), hi = static_cast<double>(h.n) / (h.n + 1.0);
    for (int i = 0; i < A; ++i) {
      // probit 只在池非空时求值: n = 0 时 lo/hi 倒挂, clamp 的前置条件不成立
      const double v = h.n >= 1 ? probit(std::clamp<double>(h.rank(xv[i]), lo, hi)) : 0.0;
      detail::put(ov, om, i, mk(v, xm[i]));
    }
  }
};

struct CsQuantile { // k 分位 (k = 0.5 即中位); 空池 → 0
  CS_SIG {
    CS_NO23;
    const auto h = detail::hist_of(xv, xm, g, A);
    detail::broadcast(ov, om, A, mk(h.n >= 1 ? h.quantile(p.k) : 0.f, true));
  }
};

struct CsWinsor { // 空池 → 不缩尾, x 原值
  CS_SIG {
    CS_NO23;
    const auto h = detail::hist_of(xv, xm, g, A);
    const auto &w = detail::winsorize(xv, A, h, p.k);
    for (int i = 0; i < A; ++i)
      detail::put(ov, om, i, mk(h.n >= 1 ? w[i] : xv[i], xm[i]));
  }
};

struct CsBucket { // 等频分 k 组: floor(pct·k) ∈ 0..k−1; 值域退化 / 空池 → pct = 0.5 → 中间桶
  CS_SIG {
    CS_NO23;
    const auto h = detail::hist_of(xv, xm, g, A);
    const int k = static_cast<int>(p.k);
    assert(k >= 1);
    for (int i = 0; i < A; ++i) {
      const int b = std::clamp(static_cast<int>(std::floor(h.rank(xv[i]) * k)), 0, k - 1);
      detail::put(ov, om, i, mk(b, xm[i]));
    }
  }
};

// =========================== 分组族 (GROUP) ===========================

// 分组族: 组统计量只由组内池成员算; 该组无池内成员的资产回退全池统计量 (契约【截面池 g】)

struct CsGroupMean { // y = 组 id (行业等)
  CS_SIG {
    CS_NO3, (void)p;
    const int G = detail::max_gid(yv, ym, A) + 1;
    std::vector<double> s(static_cast<size_t>(std::max(G, 1)), 0.0);
    std::vector<int> c(static_cast<size_t>(std::max(G, 1)), 0);
    double sa = 0.0; // 全池
    int na = 0;
    for (int i = 0; i < A; ++i) {
      const int q = detail::gid(yv, ym, i);
      if (q >= 0 && xm[i] && g[i])
        s[q] += xv[i], ++c[q], sa += xv[i], ++na;
    }
    for (int i = 0; i < A; ++i) { // 组空 → 全池; 全池也空 → 0
      const int q = detail::gid(yv, ym, i);
      const bool ok = q >= 0 && xm[i];
      const double v = !ok ? 0.0 : (c[q] >= 1 ? s[q] / c[q] : (na >= 1 ? sa / na : 0.0));
      detail::put(ov, om, i, mk(v, ok));
    }
  }
};

struct CsGroupRank { // 组内 pct rank
  CS_SIG {
    CS_NO3, (void)p;
    const int G = detail::max_gid(yv, ym, A) + 1;
    std::vector<std::vector<float>> bucket(static_cast<size_t>(std::max(G, 1)));
    for (int i = 0; i < A; ++i) {
      const int q = detail::gid(yv, ym, i);
      if (q >= 0 && xm[i] && g[i])
        bucket[q].push_back(xv[i]);
    }
    std::vector<stream::Hist> h(static_cast<size_t>(std::max(G, 1)));
    for (int q = 0; q < G; ++q)
      h[q].build(bucket[q]);
    const auto ha = detail::hist_of<1>(xv, xm, g, A); // 全池 (回退)
    for (int i = 0; i < A; ++i) {                     // 组空 → 全池直方图; 全池也空 → Hist 给 0.5
      const int q = detail::gid(yv, ym, i);
      const bool ok = q >= 0 && xm[i];
      const float v = !ok ? 0.f : (h[q].n >= 1 ? h[q].rank(xv[i]) : ha.rank(xv[i]));
      detail::put(ov, om, i, mk(v, ok));
    }
  }
};

struct CsGroupResid { // FWL: 组内去均值后再做一次全局回归, 等价于"组固定效应 + y" 的残差
  // 退化 = 去均值后的 ỹ 全为 0 ⟺ 每组内 y 全并列 (逐组 lo/hi 精确判, 不看 Σỹ²) → β = 0, 输出 x̃
  // 拟合样本 = 池内参与者; 输出对象 = 全部参与者 (组无池成员 → 用全池均值去均值); 非参与者 (x/y/组 id 缺) 无效
  CS_SIG {
    (void)p;
    const int G = detail::max_gid(zv, zm, A) + 1;
    const size_t GS = static_cast<size_t>(std::max(G, 1));
    std::vector<double> sx(GS, 0.0), sy(GS, 0.0);
    std::vector<float> lo(GS, 0.f), hi(GS, 0.f);
    std::vector<int> c(GS, 0);
    std::vector<int> q(static_cast<size_t>(A), -1); // 参与者的组 id (xm ∧ ym ∧ 组有效), 否则 −1
    double sxa = 0.0, sya = 0.0;
    int na = 0;
    for (int i = 0; i < A; ++i) {
      const int gi = detail::gid(zv, zm, i);
      if (gi < 0 || !xm[i] || !ym[i])
        continue;
      q[i] = gi;
      if (!g[i])
        continue;
      lo[gi] = c[gi] == 0 ? yv[i] : std::fmin(lo[gi], yv[i]);
      hi[gi] = c[gi] == 0 ? yv[i] : std::fmax(hi[gi], yv[i]);
      sx[gi] += xv[i], sy[gi] += yv[i], ++c[gi];
      sxa += xv[i], sya += yv[i], ++na;
    }
    bool any_spread = false;
    for (int gi = 0; gi < G; ++gi)
      any_spread |= c[gi] >= 1 && spread(lo[gi], hi[gi]);
    const auto center = [&](int i, double &xt, double &yt) {
      const int gi = q[i];
      const double mx = c[gi] >= 1 ? sx[gi] / c[gi] : sxa / std::max(na, 1);
      const double my = c[gi] >= 1 ? sy[gi] / c[gi] : sya / std::max(na, 1);
      xt = xv[i] - mx, yt = yv[i] - my;
    };
    double sxy = 0, syy = 0;
    int n = 0;
    for (int i = 0; i < A; ++i) {
      if (q[i] < 0 || !g[i])
        continue;
      double xt, yt;
      center(i, xt, yt);
      sxy += xt * yt, syy += yt * yt, ++n;
    }
    const bool ok = n >= 2 && any_spread;
    const double b = ok ? sxy / syy : 0.0;
    for (int i = 0; i < A; ++i) {
      if (q[i] < 0) {
        detail::put(ov, om, i, Val{});
        continue;
      }
      double xt, yt;
      center(i, xt, yt);
      detail::put(ov, om, i, mk(xt - b * yt, true));
    }
  }
};

#undef CS_NO3
#undef CS_NO23
#undef CS_SIG

} // namespace factor::cs
