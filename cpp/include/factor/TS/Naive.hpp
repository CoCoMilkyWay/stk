#pragma once

// =============================================================================
// TS 算子的**对拍参考实现** (factor::naive::ts, 72 个, 对应 OpTable 的
// OP_TS_POINT / OP_TS_EXPAND / OP_TS_ROLL / OP_TS_EXPO)
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
struct S1 {
  int n = 0;
  double sum = 0, sumsq = 0, sumabs = 0; // Σx, Σx², Σ|x|
  double mean = 0, m2 = 0, m3 = 0, m4 = 0;
  double vmin = 0, vmax = 0;
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

// ---- 二元统计量: cxx/cyy/cxy 为中心化平方和 (未除 n) ----
struct S2 {
  int n = 0;
  double sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0, syabs = 0; // Σx, Σy, Σx², Σy², Σxy, Σ|y|
  double ax = 0, ay = 0, cxx = 0, cyy = 0, cxy = 0;
  double cov() const { return cxy / (n - 1); } // ddof=1
  double corr() const { return cxy / std::sqrt(cxx * cyy); }
  double beta() const { return cxy / static_cast<double>(guard(static_cast<float>(cyy))); }
};

inline S2 stat2(const std::vector<double> &bx, const std::vector<double> &by) {
  assert(bx.size() == by.size());
  S2 r;
  r.n = static_cast<int>(bx.size());
  if (r.n == 0)
    return r;
  for (int i = 0; i < r.n; ++i) {
    r.sx += bx[i];
    r.sy += by[i];
    r.sxx += bx[i] * bx[i];
    r.syy += by[i] * by[i];
    r.sxy += bx[i] * by[i];
    r.syabs += std::fabs(by[i]);
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

// CumCorrLag 专用: 样本对 (x_s, y_{s−K}), 要求 s−K 仍在**本段内**且两值都有效
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
  assert(S > 0.0); // 掩码已保证 S > kEps
  return std::log(S) - sxl / S;
}

// ---- 复利: Π(1+x) − 1, 走 Σlog1p → expm1; 任一 1+x ≤ kEps 则退化 ----
inline bool prod_ok(const std::vector<double> &b) {
  for (const double x : b)
    if (!(1.0 + x > static_cast<double>(kEps)))
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
  bool ok = false; // range_ok: false = 值域退化 (全并列)
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
  h.ok = range_ok(h.lo, h.hi);
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
  return s / static_cast<double>(guard(static_cast<float>(tot)));
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
NV_P1(Abs, std::fabs(x), mx)
NV_P1(Sign, x > 0.f ? 1.f : (x < 0.f ? -1.f : 0.f), mx)
NV_P1(Log, std::copysign(std::log1p(std::fabs(x)), x), mx)
NV_P1(Asinh, std::asinh(x), mx)
NV_P1(Tanh, std::tanh(x), mx)
NV_P1(Sqrt, std::copysign(std::sqrt(std::fabs(x)), x), mx)
NV_P1(Relu, std::fmax(0.f, x), mx)
NV_P1(Recip, 1.f / guard(x), mx &&abs_den_ok(x)) // |x| < kEps 退化
NV_P1(SignedPow, std::copysign(std::pow(std::fabs(x), p.k), x), mx)

// Clip: clamp 要求 lo ≤ hi, 即 k ≥ 0 —— 参数级约束, 早死在 assert 上
struct Clip {
  NV_SIG {
    NV_NO23;
    assert(p.k >= 0.f);
    for (int i = 0; i < T * A; ++i) {
      const bool m = xm[i] != 0;
      detail::put(ov, om, i, m ? static_cast<double>(std::clamp(xv[i], -p.k, p.k)) : 0.0, m);
    }
  }
};

// TodMask: 元数 0, 完全不读输入指针 (调用方传 nullptr)
struct TodMask {
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

NV_P2(Add, x + y, mx &&my)
NV_P2(Sub, x - y, mx &&my)
NV_P2(Mul, x *y, mx &&my)
NV_P2(Div, x / guard(y), mx && my && abs_den_ok(y))
NV_P2(Max, std::fmax(x, y), mx &&my)
NV_P2(Min, std::fmin(x, y), mx &&my)
// Imb / Share: 和相对退化 (scale = |x| + |y|)
NV_P2(Imb, (x - y) / guard(x + y), mx && my && den_ok(static_cast<double>(x) + y, std::fabs(x) + std::fabs(y)))
NV_P2(Share, x / guard(x + y), mx && my && den_ok(static_cast<double>(x) + y, std::fabs(x) + std::fabs(y)))
NV_P2(LogRatio, std::log(x / guard(y)), mx && my && x > 0.f && y > 0.f)
// Where: 未被选中的那支不要求有效
NV_P3(Where, x > 0.f ? y : z, mx && (x > 0.f ? my : mz))
// Clip3: y > z 退化; 掩码含 y ≤ z, 故 clamp 只在前置条件成立时求值
NV_P3(Clip3, std::clamp(x, y, z), mx && my && mz && (y <= z))

// =============================================================================
// OP_TS_EXPAND (23): 样本 = 本段内 s ≤ t_seg 的有效点
// =============================================================================
NV_EXP1(CumSum, s.sum, s.n >= 1)
NV_EXP1(CumMean, s.sum / s.n, s.n >= 1)
NV_EXP1(CumVar, s.var(), s.n >= 2 && disp_ok(s.m2, s.sumsq))
NV_EXP1(CumStd, std::sqrt(s.var()), s.n >= 2 && disp_ok(s.m2, s.sumsq))
NV_EXP1(CumSkew, s.skew(), s.n >= 3 && disp_ok(s.m2, s.sumsq))
NV_EXP1(CumKurt, s.kurt(), s.n >= 4 && disp_ok(s.m2, s.sumsq))
NV_EXP1(CumMax, s.vmax, s.n >= 1)
NV_EXP1(CumMin, s.vmin, s.n >= 1)
// CumRank: 相对型, x_t 无效则无效
NV_EXP1(CumRank, detail::rank_of(b, xv[t * A + a]), s.n >= 1 && xm[t * A + a])
NV_EXP1(CumHhi, s.sumsq / static_cast<double>(guard(static_cast<float>(s.sum * s.sum))),
        s.n >= 1 && den_ok(s.sum * s.sum, s.n *s.sumsq))
NV_EXP1(CumEntropy, detail::entropy_of(b),
        detail::pos_n(b) >= 1 && detail::pos_sum(b) > static_cast<double>(kEps))
NV_EXP1(CumTopK, detail::topk_of(b, static_cast<int>(p.k)),
        s.n >= static_cast<int>(p.k) && static_cast<int>(p.k) >= 1 && den_ok(s.sum, s.sumabs))
NV_EXP1(CumGini, detail::gini_of(b), detail::pos_n(b) >= 2)
NV_EXP1(CumCountGt, static_cast<double>(detail::count_gt(b, p.k)), s.n >= 1)

// CumArgMax / CumArgMin: 首个 (最早) 极值的**段内位置 s**, 严格比较故取最早
struct CumArgMax {
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
        detail::put(ov, om, t * A + a, static_cast<double>(best), n >= 1);
      }
  }
};

struct CumArgMin {
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
        detail::put(ov, om, t * A + a, static_cast<double>(best), n >= 1);
      }
  }
};

// CumPeaks: 峰在段内位置 s 要求 s−1/s/s+1 三点同段且都有效, 且 x_{s−1} < x_s > x_{s+1}
//           且 x_s > k·mean_{≤s} (含 s 的段内 expanding 均值). 峰在 s+1 时刻才确认,
//           故输出 t 只统计 s ≤ t−1 的峰.
struct CumPeaks {
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

NV_EXP2(CumCov, s.cov(), s.n >= 2)
NV_EXP2(CumCorr, s.corr(), s.n >= 2 && disp_ok(s.cxx, s.sxx) && disp_ok(s.cyy, s.syy))
NV_EXP2(CumBeta, s.beta(), s.n >= 2 && disp_ok(s.cyy, s.syy))
// CumResid: 相对型, 需 x_t、y_t 都有效
NV_EXP2(CumResid, (xv[t * A + a] - s.ax) - s.beta() * (yv[t * A + a] - s.ay),
        s.n >= 2 && disp_ok(s.cyy, s.syy) && xm[t * A + a] && ym[t * A + a])
NV_EXP2(CumWMean, s.sxy / static_cast<double>(guard(static_cast<float>(s.sy))),
        s.n >= 1 && den_ok(s.sy, s.syabs))

// CumCorrLag: 样本对 (x_s, y_{s−K}), 其余口径同 CumCorr
struct CumCorrLag {
  NV_SIG {
    NV_NO3;
    const int K = static_cast<int>(p.k);
    assert(K >= 0);
    std::vector<double> bx, by;
    for (int a = 0; a < A; ++a)
      for (int t = 0; t < T; ++t) {
        detail::gather_exp2_lag(xv, xm, yv, ym, t, A, a, K, bx, by);
        const detail::S2 s = detail::stat2(bx, by);
        const bool m = s.n >= 2 && disp_ok(s.cxx, s.sxx) && disp_ok(s.cyy, s.syy);
        detail::put(ov, om, t * A + a, m ? s.corr() : 0.0, m);
      }
  }
};

// =============================================================================
// OP_TS_ROLL (26): 样本 = 窗 [t−d+1, t] 内的有效点; 窗未满 (t < d−1) 一律无效
// =============================================================================

// TsDelay / TsDelta: 窗实际跨 d+1 格, 前置条件是 t ≥ d (不是 t ≥ d−1)
struct TsDelay {
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

struct TsDelta {
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

NV_ROLL1(TsSum, s.sum, s.n >= 1)
NV_ROLL1(TsMean, s.sum / s.n, s.n >= 1)
NV_ROLL1(TsVar, s.var(), s.n >= 2 && disp_ok(s.m2, s.sumsq))
NV_ROLL1(TsStd, std::sqrt(s.var()), s.n >= 2 && disp_ok(s.m2, s.sumsq))
NV_ROLL1(TsSkew, s.skew(), s.n >= 3 && disp_ok(s.m2, s.sumsq))
NV_ROLL1(TsKurt, s.kurt(), s.n >= 4 && disp_ok(s.m2, s.sumsq))
NV_ROLL1(TsMax, s.vmax, s.n >= 1)
NV_ROLL1(TsMin, s.vmin, s.n >= 1)
NV_ROLL1(TsMed, detail::quantile_of(b, 0.5), s.n >= 1)
NV_ROLL1(TsMad, detail::mad_of(b), s.n >= 1)
NV_ROLL1(TsRank, detail::rank_of(b, xv[t * A + a]), s.n >= 1 && xm[t * A + a])
NV_ROLL1(TsZ, (xv[t * A + a] - s.mean) / static_cast<double>(guard(static_cast<float>(std::sqrt(s.var())))),
         s.n >= 2 && disp_ok(s.m2, s.sumsq) && xm[t * A + a])
NV_ROLL1(TsProduct, detail::prod_of(b), s.n >= 1 && detail::prod_ok(b))
NV_ROLL1(TsCountGt, static_cast<double>(detail::count_gt(b, p.k)), s.n >= 1)

// TsArgMax / TsArgMin: 距今期数 (d−1) − i, i = 窗内首个 (最旧) 极值的窗内下标
struct TsArgMax {
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

struct TsArgMin {
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

// TsWma: 权 w = i+1 (窗内下标 i, 最旧 = 0), 只对有效点累加权与乘积, 值 = Σ(w·x)/Σw
struct TsWma {
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

// TsSlope: x 对窗内下标 i 的 OLS 斜率 (Σix − Σi·Σx/n)/(Σi² − (Σi)²/n), 只用有效点
struct TsSlope {
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
        const double den = n >= 1 ? sii - si * si / n : 0.0;
        const bool m = t >= p.d - 1 && n >= 2 && disp_ok(den, sii);
        const double num = six - si * sx / (n >= 1 ? n : 1);
        detail::put(ov, om, t * A + a,
                    m ? num / static_cast<double>(guard(static_cast<float>(den))) : 0.0, m);
      }
  }
};

// EventAge: 窗内相邻 j−1/j (都在窗内且都有效) 若 |Δ| > kRelEps(|x_j| + |x_{j−1}|) 则 j 处变动;
//           取最近一次变动的全局行号 j*, 输出 t − j*; 窗内无变动则输出 d
struct EventAge {
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
          if (std::fabs(xj - xp) > static_cast<double>(kRelEps) * (std::fabs(xj) + std::fabs(xp)))
            last = j;
        }
        const bool m = t >= p.d - 1 && xm[t * A + a];
        detail::put(ov, om, t * A + a,
                    m ? (last >= 0 ? static_cast<double>(t - last) : static_cast<double>(p.d)) : 0.0,
                    m);
      }
  }
};

NV_ROLL2(TsCov, s.cov(), s.n >= 2)
NV_ROLL2(TsCorr, s.corr(), s.n >= 2 && disp_ok(s.cxx, s.sxx) && disp_ok(s.cyy, s.syy))
NV_ROLL2(TsBeta, s.beta(), s.n >= 2 && disp_ok(s.cyy, s.syy))
NV_ROLL2(TsResid, (xv[t * A + a] - s.ax) - s.beta() * (yv[t * A + a] - s.ay),
         s.n >= 2 && disp_ok(s.cyy, s.syy) && xm[t * A + a] && ym[t * A + a])
NV_ROLL2(TsWMean, s.sxy / static_cast<double>(guard(static_cast<float>(s.sy))),
         s.n >= 1 && den_ok(s.sy, s.syabs))

// =============================================================================
// OP_TS_EXPO (1): 全程递推, 不按段重置
// =============================================================================
// TsEma: x 有效时 y = 已出现过有效值 ? k·x + (1−k)·y : x; x 无效时 y 与掩码都不动
struct TsEma {
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
