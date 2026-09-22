#pragma once

// =============================================================================
// TS 算子的**对拍参考实现** (factor::naive::ts, 72 个, 对应 OpTable 的 OP_TS_POINT / OP_TS_WIN)
// =============================================================================
//   定位: 按定义整段重算, double 累加, O(T·d) 双重循环, 不要任何性能.
//   它的唯一价值是"独立第二实现", 所以这里**不得** include 任何 Stream/Gpu 头,
//   也不复用它们的一行算法代码.
//
//   布局: SoA 行主序 [T][A], 下标 t*A + a. TS = 每资产 a 独立、沿 t 推进.
//   窗:  EXPAND 按段 (只看本段内 s ≤ t_seg); ROLL 跨段不重置 (全局行 t−d+1..t); EXPO 全程.
//   输出: 一律走 Contract 的 mk(), om == 0 时 ov 必为 0, 全程不产 NaN / inf.
//   有效样本: 一元看 xm; 二元要 xm && ym 同时成立.
//   出错策略: 不做错误处理, 只 assert (参数非法立刻死在最早处).
// =============================================================================

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <vector>

#include "factor/Contract.hpp"

namespace factor::naive::ts {

// -----------------------------------------------------------------------------
// detail: 样本收集 / 统计量 / 桶法. 全部按定义直白重算, 不做任何增量复用.
// -----------------------------------------------------------------------------
namespace detail {

inline void put(float *ov, uint8_t *om, int i, double v, bool m) {
  const Val r = mk(v, m);
  ov[i] = r.v;
  om[i] = r.m ? 1u : 0u;
}

// ---- 一元统计量: 幂和 + 中心矩和 + 极值 ----
//   m2/m3/m4 是**中心矩和** Σ(x−μ)^k (未除 n); 方差取 ddof=1, 偏峰取总体矩 (再除 n).
//   全并列判据 = 极值比较 (契约: 精确, 无阈值)
struct S1 {
  int n = 0;
  double sum = 0, sumsq = 0, sumabs = 0; // Σx, Σx², Σ|x|
  double mean = 0, m2 = 0, m3 = 0, m4 = 0;
  double vmin = 0, vmax = 0;
  bool spread() const { return vmax > vmin; }
  double var() const { return m2 / (n - 1); } // ddof=1
  double skew() const {
    const double p2 = m2 / n;
    return (m3 / n) / std::pow(p2, 1.5);
  }
  double kurt() const {
    const double p2 = m2 / n;
    return (m4 / n) / (p2 * p2) - 3.0;
  }
};

inline S1 stat1(const std::vector<double> &b) {
  S1 r;
  r.n = static_cast<int>(b.size());
  if (r.n == 0)
    return r;
  r.vmin = r.vmax = b[0];
  for (const double x : b) {
    r.sum += x;
    r.sumsq += x * x;
    r.sumabs += std::fabs(x);
    r.vmin = std::fmin(r.vmin, x);
    r.vmax = std::fmax(r.vmax, x);
  }
  r.mean = r.sum / r.n;
  for (const double x : b) { // 第二遍: 中心矩 (参考实现取精度优先, 不用幂和展开)
    const double dx = x - r.mean, q = dx * dx;
    r.m2 += q;
    r.m3 += q * dx;
    r.m4 += q * q;
  }
  return r;
}

// ---- 二元统计量: cxx/cyy/cxy 为中心化平方和 (未除 n); 极值给全并列判据 ----
struct S2 {
  int n = 0;
  double sx = 0, sy = 0, sxy = 0, syabs = 0; // Σx, Σy, Σxy, Σ|y|
  double ax = 0, ay = 0, cxx = 0, cyy = 0, cxy = 0;
  double xmin = 0, xmax = 0, ymin = 0, ymax = 0;
  bool spread_x() const { return xmax > xmin; }
  bool spread_y() const { return ymax > ymin; }
  double cov() const { return cxy / (n - 1); } // ddof=1
  double corr() const { return cxy / std::sqrt(cxx * cyy); }
  double beta() const { return cxy / cyy; }
};

inline S2 stat2(const std::vector<double> &bx, const std::vector<double> &by) {
  assert(bx.size() == by.size());
  S2 r;
  r.n = static_cast<int>(bx.size());
  if (r.n == 0)
    return r;
  r.xmin = r.xmax = bx[0];
  r.ymin = r.ymax = by[0];
  for (int i = 0; i < r.n; ++i) {
    r.sx += bx[i];
    r.sy += by[i];
    r.sxy += bx[i] * by[i];
    r.syabs += std::fabs(by[i]);
    r.xmin = std::fmin(r.xmin, bx[i]);
    r.xmax = std::fmax(r.xmax, bx[i]);
    r.ymin = std::fmin(r.ymin, by[i]);
    r.ymax = std::fmax(r.ymax, by[i]);
  }
  r.ax = r.sx / r.n;
  r.ay = r.sy / r.n;
  for (int i = 0; i < r.n; ++i) {
    const double dx = bx[i] - r.ax, dy = by[i] - r.ay;
    r.cxx += dx * dx;
    r.cyy += dy * dy;
    r.cxy += dx * dy;
  }
  return r;
}

// ---- 样本收集 ----
// EXPAND: 本段内 s ≤ t_seg 的有效点 (段起点 = t − t%kSegLen)
inline void gather_exp1(const float *xv, const uint8_t *xm, int t, int A, int a, std::vector<double> &b) {
  b.clear();
  for (int s = t - t % kSegLen; s <= t; ++s)
    if (xm[s * A + a])
      b.push_back(xv[s * A + a]);
}

// ROLL: 窗 [t−d+1, t] 内的有效点, 跨段不重置 (窗未满时结果由调用方丢弃)
inline void gather_roll1(const float *xv, const uint8_t *xm, int t, int A, int a, int d,
                         std::vector<double> &b) {
  b.clear();
  for (int s = std::max(0, t - d + 1); s <= t; ++s)
    if (xm[s * A + a])
      b.push_back(xv[s * A + a]);
}

inline void gather_exp2(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, int t,
                        int A, int a, std::vector<double> &bx, std::vector<double> &by) {
  bx.clear();
  by.clear();
  for (int s = t - t % kSegLen; s <= t; ++s) {
    const int i = s * A + a;
    if (xm[i] && ym[i]) {
      bx.push_back(xv[i]);
      by.push_back(yv[i]);
    }
  }
}

inline void gather_roll2(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, int t,
                         int A, int a, int d, std::vector<double> &bx, std::vector<double> &by) {
  bx.clear();
  by.clear();
  for (int s = std::max(0, t - d + 1); s <= t; ++s) {
    const int i = s * A + a;
    if (xm[i] && ym[i]) {
      bx.push_back(xv[i]);
      by.push_back(yv[i]);
    }
  }
}

// TsCorrLagCum 专用: 样本对 (x_s, y_{s−K}), 要求 s−K 仍在**本段内**且两值都有效
inline void gather_exp2_lag(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
                            int t, int A, int a, int K, std::vector<double> &bx,
                            std::vector<double> &by) {
  bx.clear();
  by.clear();
  const int s0 = t - t % kSegLen;
  for (int s = s0; s <= t; ++s) {
    const int sl = s - K;
    if (sl < 0 || sl < s0)
      continue;
    if (xm[s * A + a] && ym[sl * A + a]) {
      bx.push_back(xv[s * A + a]);
      by.push_back(yv[sl * A + a]);
    }
  }
}

// ---- 计数 / 只取正样本 ----
inline int count_gt(const std::vector<double> &b, float k) {
  int c = 0;
  for (const double x : b)
    if (x > static_cast<double>(k))
      ++c;
  return c;
}
inline int pos_n(const std::vector<double> &b) {
  int c = 0;
  for (const double x : b)
    if (x > 0.0)
      ++c;
  return c;
}
inline double pos_sum(const std::vector<double> &b) {
  double s = 0;
  for (const double x : b)
    if (x > 0.0)
      s += x;
  return s;
}

// ---- 熵: 只计 x>0 的样本, S = Σx, 值 = ln S − Σ(x·ln x)/S ----
inline double entropy_of(const std::vector<double> &b) {
  double S = 0, sxl = 0;
  for (const double x : b)
    if (x > 0.0) {
      S += x;
      sxl += x * std::log(x);
    }
  assert(S > 0.0); // 掩码已保证有正样本
  return std::log(S) - sxl / S;
}

// ---- 复利: Π(1+x) − 1, 走 Σlog1p → expm1; 任一 x ≤ −1 (1+x ≤ 0) 则退化 ----
inline bool prod_ok(const std::vector<double> &b) {
  for (const double x : b)
    if (!(x > -1.0))
      return false;
  return true;
}
inline double prod_of(const std::vector<double> &b) {
  double s = 0;
  for (const double x : b)
    s += std::log1p(x);
  return std::expm1(s);
}

// ---- 桶法 (序统计族): lo/hi = 样本 min/max, 桶数 kBuckets, 桶计数是整数故三后端可严格一致 ----
struct Hist {
  int cnt = 0;
  float lo = 0.f, hi = 0.f;
  bool ok = false; // spread(lo, hi): false = 全并列
  int c[kBuckets];
};

inline Hist hist_of(const std::vector<double> &b) {
  Hist h;
  for (int i = 0; i < kBuckets; ++i)
    h.c[i] = 0;
  h.cnt = static_cast<int>(b.size());
  if (h.cnt == 0)
    return h;
  double lo = b[0], hi = b[0];
  for (const double x : b) {
    lo = std::fmin(lo, x);
    hi = std::fmax(hi, x);
  }
  h.lo = static_cast<float>(lo);
  h.hi = static_cast<float>(hi);
  h.ok = spread(h.lo, h.hi);
  if (h.ok)
    for (const double x : b)
      ++h.c[bin_of(static_cast<float>(x), h.lo, h.hi)];
  return h;
}

// 分位: 最小的 b 使前缀累计 ≥ max(1, ceil(q·cnt)), 取桶中心; 值域退化 → lo; q≤0 → lo, q≥1 → hi
inline double quantile_of(const std::vector<double> &b, double q) {
  const Hist h = hist_of(b);
  assert(h.cnt >= 1);
  if (!h.ok)
    return h.lo;
  if (q <= 0.0)
    return h.lo;
  if (q >= 1.0)
    return h.hi;
  const int need = std::max(1, static_cast<int>(std::ceil(q * h.cnt)));
  int acc = 0;
  for (int i = 0; i < kBuckets; ++i) {
    acc += h.c[i];
    if (acc >= need)
      return bin_center(i, h.lo, h.hi);
  }
  return h.hi;
}

// pct rank: less = 低桶总数, eq = 本桶数; 值域退化 → 0.5
inline double rank_of(const std::vector<double> &b, float x) {
  const Hist h = hist_of(b);
  assert(h.cnt >= 1);
  if (!h.ok)
    return 0.5;
  const int bb = bin_of(x, h.lo, h.hi);
  int less = 0;
  for (int i = 0; i < bb; ++i)
    less += h.c[i];
  return static_cast<double>(pct_of(less, h.c[bb], h.cnt));
}

// MAD: 两轮 —— 先窗内 median, 再对 |x − med| 这组值**重新**定 lo/hi/直方图求 median
inline double mad_of(const std::vector<double> &b) {
  const double med = quantile_of(b, 0.5);
  std::vector<double> d;
  d.reserve(b.size());
  for (const double x : b)
    d.push_back(std::fabs(x - med));
  return quantile_of(d, 0.5);
}

// topk: 从最高桶往低累计, 在 b* 处首次 ≥ K, 和 = Σ_{b>b*} ctr·cnt + ctr(b*)·(K − 已累计); 输出 和/Σx
inline double topk_of(const std::vector<double> &b, int K) {
  const Hist h = hist_of(b);
  assert(K >= 1 && h.cnt >= K);
  if (!h.ok)
    return static_cast<double>(std::min(K, h.cnt)) / static_cast<double>(h.cnt); // 全并列 → K/cnt
  double tot = 0;
  for (const double x : b)
    tot += x;
  int acc = 0;
  double s = 0;
  for (int i = kBuckets - 1; i >= 0; --i) {
    if (acc + h.c[i] >= K) {
      s += bin_center(i, h.lo, h.hi) * static_cast<double>(K - acc);
      break;
    }
    acc += h.c[i];
    s += bin_center(i, h.lo, h.hi) * static_cast<double>(h.c[i]);
  }
  return s / tot; // 掩码已保证 Σx 未相消
}

// gini: 只用 x>0 的样本 (重新定 lo/hi/直方图), 按桶升序累计人口比 p 与价值比 L (价值 = ctr·cnt),
//       G = 1 − Σ (p_i − p_{i−1})(L_i + L_{i−1}), p_0 = L_0 = 0; 值域退化 → 0
inline double gini_of(const std::vector<double> &b) {
  std::vector<double> q;
  for (const double x : b)
    if (x > 0.0)
      q.push_back(x);
  const Hist h = hist_of(q);
  assert(h.cnt >= 2); // 掩码已保证
  if (!h.ok)
    return 0.0;
  double tot = 0;
  for (int i = 0; i < kBuckets; ++i)
    tot += bin_center(i, h.lo, h.hi) * static_cast<double>(h.c[i]);
  assert(tot > 0.0); // 全为正样本 ⇒ lo > 0 ⇒ 所有桶中心 > 0
  double pp = 0, lp = 0, g = 1.0;
  for (int i = 0; i < kBuckets; ++i) {
    if (h.c[i] == 0)
      continue;
    const double pc = pp + static_cast<double>(h.c[i]) / h.cnt;
    const double lc = lp + bin_center(i, h.lo, h.hi) * static_cast<double>(h.c[i]) / tot;
    g -= (pc - pp) * (lc + lp);
    pp = pc;
    lp = lc;
  }
  return g;
}

} // namespace detail

// -----------------------------------------------------------------------------
// 统一签名 (72 个算子同形; 元数不足时调用方传 nullptr, 故未用到的指针一律不许读)
// -----------------------------------------------------------------------------
#define NV_SIG                                                                              \
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,   \
                  const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, \
                  const Param &p)

#define NV_NO23 (void)yv, (void)ym, (void)zv, (void)zm
#define NV_NO3 (void)zv, (void)zm

// 样板一律写成 `m ? (值式) : 0.0`: 掩码为假时值不参与对拍, 顺带杜绝 log(负) 之类的 NaN.

// ---- 逐点样板 (V 可用 x/y/z 与 p, M 可用 mx/my/mz) ----
#define NV_P1(Name, V, M)                                            \
  struct Name {                                                      \
    NV_SIG {                                                         \
      NV_NO23;                                                       \
      (void)p;                                                       \
      for (int i = 0; i < T * A; ++i) {                              \
        const float x = xv[i];                                       \
        const bool mx = xm[i] != 0;                                  \
        const bool m = (M);                                          \
        detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m); \
      }                                                              \
    }                                                                \
  };

#define NV_P2(Name, V, M)                                            \
  struct Name {                                                      \
    NV_SIG {                                                         \
      NV_NO3;                                                        \
      (void)p;                                                       \
      for (int i = 0; i < T * A; ++i) {                              \
        const float x = xv[i], y = yv[i];                            \
        const bool mx = xm[i] != 0, my = ym[i] != 0;                 \
        const bool m = (M);                                          \
        detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m); \
      }                                                              \
    }                                                                \
  };

#define NV_P3(Name, V, M)                                             \
  struct Name {                                                       \
    NV_SIG {                                                          \
      (void)p;                                                        \
      for (int i = 0; i < T * A; ++i) {                               \
        const float x = xv[i], y = yv[i], z = zv[i];                  \
        const bool mx = xm[i] != 0, my = ym[i] != 0, mz = zm[i] != 0; \
        const bool m = (M);                                           \
        detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m);  \
      }                                                               \
    }                                                                 \
  };

// ---- EXPAND 样板 (b = 段内有效样本, s = 其统计量; 可用 xv/xm/yv/ym 与 t/A/a/p) ----
#define NV_EXP1(Name, V, M)                                                    \
  struct Name {                                                                \
    NV_SIG {                                                                   \
      NV_NO23;                                                                 \
      (void)p;                                                                 \
      std::vector<double> b;                                                   \
      for (int a = 0; a < A; ++a)                                              \
        for (int t = 0; t < T; ++t) {                                          \
          detail::gather_exp1(xv, xm, t, A, a, b);                             \
          const detail::S1 s = detail::stat1(b);                               \
          (void)s;                                                             \
          const bool m = (M);                                                  \
          detail::put(ov, om, t * A + a, m ? static_cast<double>(V) : 0.0, m); \
        }                                                                      \
    }                                                                          \
  };

#define NV_EXP2(Name, V, M)                                                    \
  struct Name {                                                                \
    NV_SIG {                                                                   \
      NV_NO3;                                                                  \
      (void)p;                                                                 \
      std::vector<double> bx, by;                                              \
      for (int a = 0; a < A; ++a)                                              \
        for (int t = 0; t < T; ++t) {                                          \
          detail::gather_exp2(xv, xm, yv, ym, t, A, a, bx, by);                \
          const detail::S2 s = detail::stat2(bx, by);                          \
          const bool m = (M);                                                  \
          detail::put(ov, om, t * A + a, m ? static_cast<double>(V) : 0.0, m); \
        }                                                                      \
    }                                                                          \
  };

// ---- ROLL 样板 (窗满 t ≥ d−1 是所有 ROLL 算子的前置条件, 已并入掩码) ----
#define NV_ROLL1(Name, V, M)                                                   \
  struct Name {                                                                \
    NV_SIG {                                                                   \
      NV_NO23;                                                                 \
      assert(p.d >= 1);                                                        \
      std::vector<double> b;                                                   \
      for (int a = 0; a < A; ++a)                                              \
        for (int t = 0; t < T; ++t) {                                          \
          const bool full = t >= p.d - 1;                                      \
          detail::gather_roll1(xv, xm, t, A, a, p.d, b);                       \
          const detail::S1 s = detail::stat1(b);                               \
          (void)s;                                                             \
          const bool m = full && (M);                                          \
          detail::put(ov, om, t * A + a, m ? static_cast<double>(V) : 0.0, m); \
        }                                                                      \
    }                                                                          \
  };

#define NV_ROLL2(Name, V, M)                                                   \
  struct Name {                                                                \
    NV_SIG {                                                                   \
      NV_NO3;                                                                  \
      assert(p.d >= 1);                                                        \
      std::vector<double> bx, by;                                              \
      for (int a = 0; a < A; ++a)                                              \
        for (int t = 0; t < T; ++t) {                                          \
          const bool full = t >= p.d - 1;                                      \
          detail::gather_roll2(xv, xm, yv, ym, t, A, a, p.d, bx, by);          \
          const detail::S2 s = detail::stat2(bx, by);                          \
          const bool m = full && (M);                                          \
          detail::put(ov, om, t * A + a, m ? static_cast<double>(V) : 0.0, m); \
        }                                                                      \
    }                                                                          \
  };

// =============================================================================
// OP_TS_POINT (22): 逐点无状态. 表达式严格照写, 不改等价形式 (三后端要位级一致).
// =============================================================================
NV_P1(TsAbs, std::fabs(x), mx)
NV_P1(TsSign, x > 0.f ? 1.f : (x < 0.f ? -1.f : 0.f), mx)
NV_P1(TsLog, std::copysign(std::log1p(std::fabs(x)), x), mx)
NV_P1(TsAsinh, std::asinh(x), mx)
NV_P1(TsTanh, std::tanh(x), mx)
NV_P1(TsSqrt, std::copysign(std::sqrt(std::fabs(x)), x), mx)
NV_P1(TsRelu, std::fmax(0.f, x), mx)
NV_P1(TsRecip, 1.f / x, mx &&x != 0.f) // x = 0 退化, 溢出由 mk 转无效
NV_P1(TsSignedPow, std::copysign(std::pow(std::fabs(x), p.k), x), mx)

// TsClip: clamp 要求 lo ≤ hi, 即 k ≥ 0 —— 参数级约束, 早死在 assert 上
struct TsClip {
  NV_SIG {
    NV_NO23;
    assert(p.k >= 0.f);
    for (int i = 0; i < T * A; ++i) {
      const bool m = xm[i] != 0;
      detail::put(ov, om, i, m ? static_cast<double>(std::clamp(xv[i], -p.k, p.k)) : 0.0, m);
    }
  }
};

// TsTodMask: 元数 0, 完全不读输入指针 (调用方传 nullptr)
struct TsTodMask {
  NV_SIG {
    (void)xv, (void)xm;
    NV_NO23;
    for (int t = 0; t < T; ++t) {
      const int t_seg = t % kSegLen;
      const float v = (t_seg >= static_cast<int>(p.k) && t_seg < static_cast<int>(p.k2)) ? 1.f : 0.f;
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, t * A + a, static_cast<double>(v), true);
    }
  }
};

NV_P2(TsAdd, x + y, mx &&my)
NV_P2(TsSub, x - y, mx &&my)
NV_P2(TsMul, x *y, mx &&my)
NV_P2(TsDiv, x / y, mx && my && y != 0.f) // y = 0 退化, 溢出由 mk 转无效
NV_P2(TsMax, std::fmax(x, y), mx &&my)
NV_P2(TsMin, std::fmin(x, y), mx &&my)
// TsImb / TsShare: 和相消退化 (scale = |x| + |y|), 分母不钳位
NV_P2(TsImb, (x - y) / (x + y), mx && my && den_ok(static_cast<double>(x) + y, std::fabs(x) + std::fabs(y)))
NV_P2(TsShare, x / (x + y), mx && my && den_ok(static_cast<double>(x) + y, std::fabs(x) + std::fabs(y)))
NV_P2(TsLogRatio, std::log(x) - std::log(y), mx && my && x > 0.f && y > 0.f)
// TsWhere: 未被选中的那支不要求有效
NV_P3(TsWhere, x > 0.f ? y : z, mx && (x > 0.f ? my : mz))
// TsClip3: y > z 退化; 掩码含 y ≤ z, 故 clamp 只在前置条件成立时求值
NV_P3(TsClip3, std::clamp(x, y, z), mx && my && mz && (y <= z))

// =============================================================================
// OP_TS_WIN / EXPAND (23): 样本 = 本段内 s ≤ t_seg 的有效点
// =============================================================================
NV_EXP1(TsSumCum, s.sum, s.n >= 1)
NV_EXP1(TsMeanCum, s.sum / s.n, s.n >= 1)
NV_EXP1(TsVarCum, s.var(), s.n >= 2 && s.spread())
NV_EXP1(TsStdCum, std::sqrt(s.var()), s.n >= 2 && s.spread())
NV_EXP1(TsSkewCum, s.skew(), s.n >= 3 && s.spread())
NV_EXP1(TsKurtCum, s.kurt(), s.n >= 4 && s.spread())
NV_EXP1(TsMaxCum, s.vmax, s.n >= 1)
NV_EXP1(TsMinCum, s.vmin, s.n >= 1)
// TsRankCum: 相对型, x_t 无效则无效
NV_EXP1(TsRankCum, detail::rank_of(b, xv[t * A + a]), s.n >= 1 && xm[t * A + a])
NV_EXP1(TsHhiCum, s.sumsq / (s.sum * s.sum), s.n >= 1 && den_ok(s.sum * s.sum, s.n *s.sumsq))
NV_EXP1(TsEntropyCum, detail::entropy_of(b), detail::pos_n(b) >= 1)
NV_EXP1(TsTopKCum, detail::topk_of(b, static_cast<int>(p.k)),
        s.n >= static_cast<int>(p.k) && static_cast<int>(p.k) >= 1 && den_ok(s.sum, s.sumabs))
NV_EXP1(TsGiniCum, detail::gini_of(b), detail::pos_n(b) >= 2)
NV_EXP1(TsCountGtCum, static_cast<double>(detail::count_gt(b, p.k)), s.n >= 1)

// TsArgMaxCum / TsArgMinCum: 首个 (最早) 极值**距今的期数** t_seg − s, 严格比较故取最早
//   与 Roll 版同口径 (0 = 极值就在当前格)
struct TsArgMaxCum {
  NV_SIG {
    NV_NO23;
    (void)p;
    for (int a = 0; a < A; ++a)
      for (int t = 0; t < T; ++t) {
        const int s0 = t - t % kSegLen;
        int n = 0, best = -1;
        double bv = 0;
        for (int s = s0; s <= t; ++s)
          if (xm[s * A + a]) {
            const double x = xv[s * A + a];
            ++n;
            if (best < 0 || x > bv) {
              bv = x;
              best = s - s0;
            }
          }
        detail::put(ov, om, t * A + a, static_cast<double>((t - s0) - best), n >= 1);
      }
  }
};

struct TsArgMinCum {
  NV_SIG {
    NV_NO23;
    (void)p;
    for (int a = 0; a < A; ++a)
      for (int t = 0; t < T; ++t) {
        const int s0 = t - t % kSegLen;
        int n = 0, best = -1;
        double bv = 0;
        for (int s = s0; s <= t; ++s)
          if (xm[s * A + a]) {
            const double x = xv[s * A + a];
            ++n;
            if (best < 0 || x < bv) {
              bv = x;
              best = s - s0;
            }
          }
        detail::put(ov, om, t * A + a, static_cast<double>((t - s0) - best), n >= 1);
      }
  }
};

// TsPeaksCum: 峰在段内位置 s 要求 s−1/s/s+1 三点同段且都有效, 且 x_{s−1} < x_s > x_{s+1}
//             且 x_s > k·mean_{≤s} (含 s 的段内 expanding 均值). 峰在 s+1 时刻才确认,
//             故输出 t 只统计 s ≤ t−1 的峰.
struct TsPeaksCum {
  NV_SIG {
    NV_NO23;
    for (int a = 0; a < A; ++a)
      for (int t = 0; t < T; ++t) {
        const int s0 = t - t % kSegLen;
        int n = 0, peaks = 0;
        double sum = 0;
        for (int s = s0; s <= t; ++s) {
          if (xm[s * A + a]) {
            ++n;
            sum += xv[s * A + a];
          }
          if (s > s0 && s < t && xm[(s - 1) * A + a] && xm[s * A + a] && xm[(s + 1) * A + a]) {
            const double xs = xv[s * A + a];
            if (xv[(s - 1) * A + a] < xs && xs > xv[(s + 1) * A + a] &&
                xs > static_cast<double>(p.k) * (sum / n))
              ++peaks;
          }
        }
        detail::put(ov, om, t * A + a, static_cast<double>(peaks), n >= 1);
      }
  }
};

NV_EXP2(TsCovCum, s.cov(), s.n >= 2)
NV_EXP2(TsCorrCum, s.corr(), s.n >= 2 && s.spread_x() && s.spread_y())
NV_EXP2(TsBetaCum, s.beta(), s.n >= 2 && s.spread_y())
// TsResidCum: 相对型, 需 x_t、y_t 都有效
NV_EXP2(TsResidCum, (xv[t * A + a] - s.ax) - s.beta() * (yv[t * A + a] - s.ay),
        s.n >= 2 && s.spread_y() && xm[t * A + a] && ym[t * A + a])
NV_EXP2(TsWMeanCum, s.sxy / s.sy, s.n >= 1 && den_ok(s.sy, s.syabs))

// TsCorrLagCum: 样本对 (x_s, y_{s−K}), 其余口径同 TsCorrCum
struct TsCorrLagCum {
  NV_SIG {
    NV_NO3;
    const int K = static_cast<int>(p.k);
    assert(K >= 0);
    std::vector<double> bx, by;
    for (int a = 0; a < A; ++a)
      for (int t = 0; t < T; ++t) {
        detail::gather_exp2_lag(xv, xm, yv, ym, t, A, a, K, bx, by);
        const detail::S2 s = detail::stat2(bx, by);
        const bool m = s.n >= 2 && s.spread_x() && s.spread_y();
        detail::put(ov, om, t * A + a, m ? s.corr() : 0.0, m);
      }
  }
};

// =============================================================================
// OP_TS_WIN / ROLL (26): 样本 = 窗 [t−d+1, t] 内的有效点; 窗未满 (t < d−1) 一律无效
// =============================================================================

// TsDelayRoll / TsDeltaRoll: 窗实际跨 d+1 格, 前置条件是 t ≥ d (不是 t ≥ d−1)
struct TsDelayRoll {
  NV_SIG {
    NV_NO23;
    assert(p.d >= 1);
    for (int t = 0; t < T; ++t)
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        const bool m = t >= p.d && xm[(t - p.d) * A + a];
        detail::put(ov, om, i, m ? static_cast<double>(xv[(t - p.d) * A + a]) : 0.0, m);
      }
  }
};

struct TsDeltaRoll {
  NV_SIG {
    NV_NO23;
    assert(p.d >= 1);
    for (int t = 0; t < T; ++t)
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        const bool m = t >= p.d && xm[i] && xm[(t - p.d) * A + a];
        detail::put(ov, om, i, m ? static_cast<double>(xv[i]) - xv[(t - p.d) * A + a] : 0.0, m);
      }
  }
};

NV_ROLL1(TsSumRoll, s.sum, s.n >= 1)
NV_ROLL1(TsMeanRoll, s.sum / s.n, s.n >= 1)
NV_ROLL1(TsVarRoll, s.var(), s.n >= 2 && s.spread())
NV_ROLL1(TsStdRoll, std::sqrt(s.var()), s.n >= 2 && s.spread())
NV_ROLL1(TsSkewRoll, s.skew(), s.n >= 3 && s.spread())
NV_ROLL1(TsKurtRoll, s.kurt(), s.n >= 4 && s.spread())
NV_ROLL1(TsMaxRoll, s.vmax, s.n >= 1)
NV_ROLL1(TsMinRoll, s.vmin, s.n >= 1)
NV_ROLL1(TsMedianRoll, detail::quantile_of(b, 0.5), s.n >= 1)
NV_ROLL1(TsMadRoll, detail::mad_of(b), s.n >= 1)
NV_ROLL1(TsRankRoll, detail::rank_of(b, xv[t * A + a]), s.n >= 1 && xm[t * A + a])
NV_ROLL1(TsZRoll, (xv[t * A + a] - s.mean) / std::sqrt(s.var()), s.n >= 2 && s.spread() && xm[t * A + a])
NV_ROLL1(TsProductRoll, detail::prod_of(b), s.n >= 1 && detail::prod_ok(b))
NV_ROLL1(TsCountGtRoll, static_cast<double>(detail::count_gt(b, p.k)), s.n >= 1)

// TsArgMaxRoll / TsArgMinRoll: 距今期数 (d−1) − i, i = 窗内首个 (最旧) 极值的窗内下标
struct TsArgMaxRoll {
  NV_SIG {
    NV_NO23;
    assert(p.d >= 1);
    for (int a = 0; a < A; ++a)
      for (int t = 0; t < T; ++t) {
        int n = 0, bi = -1;
        double bv = 0;
        for (int i = 0; i < p.d; ++i) {
          const int s = t - p.d + 1 + i;
          if (s < 0 || !xm[s * A + a])
            continue;
          const double x = xv[s * A + a];
          ++n;
          if (bi < 0 || x > bv) {
            bv = x;
            bi = i;
          }
        }
        const bool m = t >= p.d - 1 && n >= 1;
        detail::put(ov, om, t * A + a, m ? static_cast<double>(p.d - 1 - bi) : 0.0, m);
      }
  }
};

struct TsArgMinRoll {
  NV_SIG {
    NV_NO23;
    assert(p.d >= 1);
    for (int a = 0; a < A; ++a)
      for (int t = 0; t < T; ++t) {
        int n = 0, bi = -1;
        double bv = 0;
        for (int i = 0; i < p.d; ++i) {
          const int s = t - p.d + 1 + i;
          if (s < 0 || !xm[s * A + a])
            continue;
          const double x = xv[s * A + a];
          ++n;
          if (bi < 0 || x < bv) {
            bv = x;
            bi = i;
          }
        }
        const bool m = t >= p.d - 1 && n >= 1;
        detail::put(ov, om, t * A + a, m ? static_cast<double>(p.d - 1 - bi) : 0.0, m);
      }
  }
};

// TsWmaRoll: 权 w = i+1 (窗内下标 i, 最旧 = 0), 只对有效点累加权与乘积, 值 = Σ(w·x)/Σw
struct TsWmaRoll {
  NV_SIG {
    NV_NO23;
    assert(p.d >= 1);
    for (int a = 0; a < A; ++a)
      for (int t = 0; t < T; ++t) {
        int n = 0;
        double sw = 0, swx = 0;
        for (int i = 0; i < p.d; ++i) {
          const int s = t - p.d + 1 + i;
          if (s < 0 || !xm[s * A + a])
            continue;
          const double w = i + 1;
          ++n;
          sw += w;
          swx += w * xv[s * A + a];
        }
        const bool m = t >= p.d - 1 && n >= 1;
        detail::put(ov, om, t * A + a, m ? swx / sw : 0.0, m); // n ≥ 1 ⇒ Σw ≥ 1
      }
  }
};

// TsSlopeRoll: x 对窗内下标 i 的 OLS 斜率 (Σix − Σi·Σx/n)/(Σi² − (Σi)²/n), 只用有效点; 下标两两不同 ⇒ n ≥ 2 时分母 > 0
struct TsSlopeRoll {
  NV_SIG {
    NV_NO23;
    assert(p.d >= 1);
    for (int a = 0; a < A; ++a)
      for (int t = 0; t < T; ++t) {
        int n = 0;
        double si = 0, sii = 0, sx = 0, six = 0;
        for (int i = 0; i < p.d; ++i) {
          const int s = t - p.d + 1 + i;
          if (s < 0 || !xm[s * A + a])
            continue;
          const double fi = i, x = xv[s * A + a];
          ++n;
          si += fi;
          sii += fi * fi;
          sx += x;
          six += fi * x;
        }
        const bool m = t >= p.d - 1 && n >= 2;
        const double den = m ? sii - si * si / n : 1.0;
        const double num = m ? six - si * sx / n : 0.0;
        detail::put(ov, om, t * A + a, m ? num / den : 0.0, m);
      }
  }
};

// TsAgeRoll: 窗内相邻 j−1/j (都在窗内且都有效) 若 x_j ≠ x_{j−1} (精确) 则 j 处变动;
//            取最近一次变动的全局行号 j*, 输出 t − j*; 窗内无变动则输出 d
struct TsAgeRoll {
  NV_SIG {
    NV_NO23;
    assert(p.d >= 1);
    for (int a = 0; a < A; ++a)
      for (int t = 0; t < T; ++t) {
        int last = -1;
        for (int j = std::max(1, t - p.d + 2); j <= t; ++j) {
          if (!xm[j * A + a] || !xm[(j - 1) * A + a])
            continue;
          const double xj = xv[j * A + a], xp = xv[(j - 1) * A + a];
          if (xj != xp)
            last = j;
        }
        const bool m = t >= p.d - 1 && xm[t * A + a];
        detail::put(ov, om, t * A + a,
                    m ? (last >= 0 ? static_cast<double>(t - last) : static_cast<double>(p.d)) : 0.0,
                    m);
      }
  }
};

NV_ROLL2(TsCovRoll, s.cov(), s.n >= 2)
NV_ROLL2(TsCorrRoll, s.corr(), s.n >= 2 && s.spread_x() && s.spread_y())
NV_ROLL2(TsBetaRoll, s.beta(), s.n >= 2 && s.spread_y())
NV_ROLL2(TsResidRoll, (xv[t * A + a] - s.ax) - s.beta() * (yv[t * A + a] - s.ay),
         s.n >= 2 && s.spread_y() && xm[t * A + a] && ym[t * A + a])
NV_ROLL2(TsWMeanRoll, s.sxy / s.sy, s.n >= 1 && den_ok(s.sy, s.syabs))

// =============================================================================
// OP_TS_WIN / EXPO (1): 全程递推, 不按段重置
// =============================================================================
// TsMeanEma: x 有效时 y = 已出现过有效值 ? k·x + (1−k)·y : x; x 无效时 y 与掩码都不动
struct TsMeanEma {
  NV_SIG {
    NV_NO23;
    for (int a = 0; a < A; ++a) {
      double y = 0;
      bool has = false;
      for (int t = 0; t < T; ++t) {
        const int i = t * A + a;
        if (xm[i]) {
          y = has ? static_cast<double>(p.k) * xv[i] + (1.0 - static_cast<double>(p.k)) * y
                  : static_cast<double>(xv[i]);
          has = true;
        }
        detail::put(ov, om, i, y, has);
      }
    }
  }
};

#undef NV_ROLL2
#undef NV_ROLL1
#undef NV_EXP2
#undef NV_EXP1
#undef NV_P3
#undef NV_P2
#undef NV_P1
#undef NV_NO3
#undef NV_NO23
#undef NV_SIG

} // namespace factor::naive::ts
