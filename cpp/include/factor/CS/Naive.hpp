#pragma once

// =============================================================================
// CS 算子对拍参考实现 (OP_CS 全 20 个; 语义契约见 factor/Contract.hpp)
// =============================================================================
//   按定义最直白地算, double 累加, 不求性能 —— 唯一价值是独立第二实现,
//   故除 Contract.hpp (共享数学件: mk/spread/den_ok/pct_of/probit/分桶规则) 外不依赖任何后端.
//
//   CS = 每个时刻 t 一个截面, 沿资产 a 归约, 各 t 之间完全独立 (外层 for t, 内层一行 A 个资产).
//   数组 SoA 行主序 [T][A], 下标 t*A + a. 未用到的输入指针调用方传 nullptr.
//
//   广播型 (Mean/Std/Median/Quantile/Beta/Corr): 对该行**所有** A 个资产写同值同掩码,
//     哪怕该资产自己的 x 缺失 —— 它描述的是截面, 不是资产.
//   相对型 (其余): 描述资产自身, 该资产 x 缺失 (二元还要 ym) 即输出无效.
//   统计口径: 方差 ddof=1; 有效样本 = 一元看 xm, 二元看 xm && ym.
// =============================================================================

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <vector>

#include "factor/Contract.hpp"

namespace factor::naive::cs {

namespace detail {

// ---- 输出: 一律经 mk (无效或非有限 → 0/false) ----
inline void put(float *ov, uint8_t *om, int i, double v, bool m) {
  const Val r = mk(v, m);
  ov[i] = r.v;
  om[i] = r.m ? 1 : 0;
}

// ---- 序统计族的直方图 (分桶规则取自 Contract.hpp, 三后端共用 → 桶计数是整数, 对拍可严格一致) ----
struct Hist {
  int cnt = 0;
  float lo = 0.f, hi = 0.f;
  bool ok = false; // spread(lo, hi): false = 全并列
  int cnt_b[kBuckets] = {};

  explicit Hist(const std::vector<float> &s) {
    cnt = static_cast<int>(s.size());
    if (cnt == 0)
      return;
    lo = hi = s[0];
    for (const float x : s) {
      lo = std::min(lo, x);
      hi = std::max(hi, x);
    }
    ok = spread(lo, hi);
    if (!ok)
      return;
    for (const float x : s)
      ++cnt_b[bin_of(x, lo, hi)];
  }

  // pct rank = pct_of(less, eq, cnt); 值域退化 → 全并列, 给 0.5
  float rank(float x) const {
    if (!ok)
      return 0.5f;
    const int b = bin_of(x, lo, hi);
    int less = 0;
    for (int i = 0; i < b; ++i)
      less += cnt_b[i];
    return pct_of(less, cnt_b[b], cnt);
  }

  // 取最小的 b 使前缀计数 ≥ max(1, ceil(q·cnt)), 值 = 桶心; q ≤ 0 → lo, q ≥ 1 → hi, 退化 → lo
  float quantile(double q) const {
    if (!ok || q <= 0.0)
      return lo;
    if (q >= 1.0)
      return hi;
    const int need = std::max(1, static_cast<int>(std::ceil(q * cnt)));
    int acc = 0;
    for (int b = 0; b < kBuckets; ++b) {
      acc += cnt_b[b];
      if (acc >= need)
        return bin_center(b, lo, hi);
    }
    return hi;
  }
};

// ---- 一元矩 (两遍: 先 μ, 再 M2 = Σ(x−μ)²; lo/hi 给全并列判据) ----
struct Stat {
  int cnt = 0;
  double mean = 0.0, m2 = 0.0;
  float lo = 0.f, hi = 0.f;

  explicit Stat(const std::vector<float> &s) {
    cnt = static_cast<int>(s.size());
    if (cnt == 0)
      return;
    lo = hi = s[0];
    double sum = 0.0;
    for (const float x : s) {
      sum += x;
      lo = std::min(lo, x);
      hi = std::max(hi, x);
    }
    mean = sum / cnt;
    for (const float x : s) {
      const double d = static_cast<double>(x) - mean;
      m2 += d * d;
    }
  }
  bool spread() const { return factor::spread(lo, hi); }
  // ddof=1 标准差; cnt < 2 时无定义, 给 0 (掩码此时必为 false)
  double sd() const { return cnt >= 2 ? std::sqrt(m2 / (cnt - 1)) : 0.0; }
};

// ---- 二元共矩 (配对样本, 两遍) ----
struct Stat2 {
  int cnt = 0;
  double mx = 0.0, my = 0.0;
  double cxx = 0.0, cyy = 0.0, cxy = 0.0; // 中心化平方和 / 交叉积和
  float lox = 0.f, hix = 0.f, loy = 0.f, hiy = 0.f;

  Stat2(const std::vector<float> &xs, const std::vector<float> &ys) {
    assert(xs.size() == ys.size());
    cnt = static_cast<int>(xs.size());
    if (cnt == 0)
      return;
    lox = hix = xs[0];
    loy = hiy = ys[0];
    double sx = 0.0, sy = 0.0;
    for (int i = 0; i < cnt; ++i) {
      sx += xs[i];
      sy += ys[i];
      lox = std::min(lox, xs[i]);
      hix = std::max(hix, xs[i]);
      loy = std::min(loy, ys[i]);
      hiy = std::max(hiy, ys[i]);
    }
    mx = sx / cnt;
    my = sy / cnt;
    for (int i = 0; i < cnt; ++i) {
      const double dx = static_cast<double>(xs[i]) - mx, dy = static_cast<double>(ys[i]) - my;
      cxx += dx * dx;
      cyy += dy * dy;
      cxy += dx * dy;
    }
  }
  bool spread_x() const { return factor::spread(lox, hix); }
  bool spread_y() const { return factor::spread(loy, hiy); }
  double beta() const { return cxy / cyy; } // OLS 斜率 (只在 spread_y 时求值)
};

// ---- 组 id: 分组列无效 → −1 (不参与); id 要当数组下标, 过大说明传进来的不是分组列 ----
inline int gid_of(const float *gv, const uint8_t *gm, int i) {
  if (!gm[i])
    return -1;
  const int g = static_cast<int>(std::floor(gv[i]));
  assert(g < kMaxGroup);
  return g;
}

} // namespace detail

using detail::gid_of;
using detail::Hist;
using detail::put;
using detail::Stat;
using detail::Stat2;

// ===== 一元: 矩 =====

// Σx/cnt 广播; m = cnt ≥ 1
struct CsMean {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &) {
    std::vector<float> s;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Stat st(s);
      for (int a = 0; a < A; ++a)
        put(ov, om, t * A + a, st.mean, st.cnt >= 1);
    }
  }
};

// sqrt(M2/(cnt−1)) 广播; m = cnt ≥ 2 && 非全并列
struct CsStd {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &) {
    std::vector<float> s;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Stat st(s);
      const bool m = st.cnt >= 2 && st.spread();
      for (int a = 0; a < A; ++a)
        put(ov, om, t * A + a, st.sd(), m);
    }
  }
};

// x − μ; m = xm && cnt ≥ 1
struct CsDemean {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &) {
    std::vector<float> s;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Stat st(s);
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        put(ov, om, i, static_cast<double>(xv[i]) - st.mean, xm[i] && st.cnt >= 1);
      }
    }
  }
};

// (x − μ)/σ, σ ddof=1; m = xm && cnt ≥ 2 && 非全并列
struct CsZ {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &) {
    std::vector<float> s;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Stat st(s);
      const bool ok = st.cnt >= 2 && st.spread();
      const double den = ok ? st.sd() : 1.0;
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        put(ov, om, i, (static_cast<double>(xv[i]) - st.mean) / den, xm[i] && ok);
      }
    }
  }
};

// ===== 一元: 序统计 =====

// 桶法 pct rank; m = xm && cnt ≥ 1
struct CsRank {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &) {
    std::vector<float> s;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Hist h(s);
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        put(ov, om, i, h.rank(xv[i]), xm[i] && h.cnt >= 1);
      }
    }
  }
};

// probit(clamp(pct, 1/(cnt+1), cnt/(cnt+1))); m = xm && cnt ≥ 1
struct CsNormRank {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &) {
    std::vector<float> s;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Hist h(s);
      const double n = h.cnt;
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        const bool m = xm[i] && h.cnt >= 1;
        // cnt = 0 时 probit 的入参无定义, 不求值 (probit assert p ∈ (0,1))
        const double v =
            m ? probit(std::clamp<double>(h.rank(xv[i]), 1.0 / (n + 1.0), n / (n + 1.0))) : 0.0;
        put(ov, om, i, v, m);
      }
    }
  }
};

// 桶法 quantile(0.5) 广播; m = cnt ≥ 1
struct CsMedian {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &) {
    std::vector<float> s;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Hist h(s);
      const double q = h.quantile(0.5);
      for (int a = 0; a < A; ++a)
        put(ov, om, t * A + a, q, h.cnt >= 1);
    }
  }
};

// 桶法 quantile(p.k) 广播; m = cnt ≥ 1
struct CsQuantile {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p) {
    std::vector<float> s;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Hist h(s);
      const double q = h.quantile(p.k);
      for (int a = 0; a < A; ++a)
        put(ov, om, t * A + a, q, h.cnt >= 1);
    }
  }
};

// clamp(x, q(p.k), q(1−p.k)) (两端按 min/max 摆正); m = xm && cnt ≥ 1
struct CsWinsor {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p) {
    std::vector<float> s;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Hist h(s);
      const float qa = h.quantile(p.k), qb = h.quantile(1.0 - static_cast<double>(p.k));
      const float w_lo = std::min(qa, qb), w_hi = std::max(qa, qb);
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        put(ov, om, i, std::clamp(xv[i], w_lo, w_hi), xm[i] && h.cnt >= 1);
      }
    }
  }
};

// 固定 q = 0.01 缩尾后**重建**直方图再算 pct rank; m = xm && cnt ≥ 1
struct CsWinsorRank {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &) {
    constexpr double kQ = 0.01;
    std::vector<float> s, w;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Hist h0(s);
      const float qa = h0.quantile(kQ), qb = h0.quantile(1.0 - kQ);
      const float w_lo = std::min(qa, qb), w_hi = std::max(qa, qb);
      w.clear();
      for (const float x : s)
        w.push_back(std::clamp(x, w_lo, w_hi));
      const Hist h(w);
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        put(ov, om, i, h.rank(std::clamp(xv[i], w_lo, w_hi)), xm[i] && h.cnt >= 1);
      }
    }
  }
};

// 固定 q = 0.01 缩尾后**重算** μ/σ/M2 再做 z; m = xm && cnt ≥ 2 && 缩尾后非全并列
struct CsWinsorZ {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &) {
    constexpr double kQ = 0.01;
    std::vector<float> s, w;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Hist h0(s);
      const float qa = h0.quantile(kQ), qb = h0.quantile(1.0 - kQ);
      const float w_lo = std::min(qa, qb), w_hi = std::max(qa, qb);
      w.clear();
      for (const float x : s)
        w.push_back(std::clamp(x, w_lo, w_hi));
      const Stat st(w);
      const bool ok = st.cnt >= 2 && st.spread();
      const double den = ok ? st.sd() : 1.0;
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        const double x = std::clamp(xv[i], w_lo, w_hi);
        put(ov, om, i, (x - st.mean) / den, xm[i] && ok);
      }
    }
  }
};

// clamp(floor(pct·k), 0, k−1); 值域退化一律给 0; m = xm && cnt ≥ 1
struct CsBucket {
  static void run(const float *xv, const uint8_t *xm, const float *, const uint8_t *, const float *,
                  const uint8_t *, float *ov, uint8_t *om, int T, int A, const Param &p) {
    const int k = static_cast<int>(p.k);
    assert(k >= 1);
    std::vector<float> s;
    for (int t = 0; t < T; ++t) {
      s.clear();
      for (int a = 0; a < A; ++a)
        if (xm[t * A + a])
          s.push_back(xv[t * A + a]);
      const Hist h(s);
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        const int b =
            h.ok ? std::clamp(static_cast<int>(std::floor(h.rank(xv[i]) * k)), 0, k - 1) : 0;
        put(ov, om, i, b, xm[i] && h.cnt >= 1);
      }
    }
  }
};

// ===== 二元: 回归 / 相关 =====

// 相对型 (x−x̄) − β(y−ȳ), β = cxy/cyy; m = xm && ym && cnt ≥ 2 && y 非全并列
struct CsResid {
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
                  const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A,
                  const Param &) {
    std::vector<float> xs, ys;
    for (int t = 0; t < T; ++t) {
      xs.clear();
      ys.clear();
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        if (xm[i] && ym[i]) {
          xs.push_back(xv[i]);
          ys.push_back(yv[i]);
        }
      }
      const Stat2 st(xs, ys);
      const bool ok = st.cnt >= 2 && st.spread_y();
      const double b = ok ? st.beta() : 0.0;
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        const double v = (static_cast<double>(xv[i]) - st.mx) - b * (static_cast<double>(yv[i]) - st.my);
        put(ov, om, i, v, xm[i] && ym[i] && ok);
      }
    }
  }
};

// β 广播; m = cnt ≥ 2 && y 非全并列
struct CsBeta {
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
                  const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A,
                  const Param &) {
    std::vector<float> xs, ys;
    for (int t = 0; t < T; ++t) {
      xs.clear();
      ys.clear();
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        if (xm[i] && ym[i]) {
          xs.push_back(xv[i]);
          ys.push_back(yv[i]);
        }
      }
      const Stat2 st(xs, ys);
      const bool m = st.cnt >= 2 && st.spread_y();
      for (int a = 0; a < A; ++a)
        put(ov, om, t * A + a, m ? st.beta() : 0.0, m);
    }
  }
};

// cxy/sqrt(cxx·cyy) 广播; m = cnt ≥ 2 && x、y 都非全并列
struct CsCorr {
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
                  const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A,
                  const Param &) {
    std::vector<float> xs, ys;
    for (int t = 0; t < T; ++t) {
      xs.clear();
      ys.clear();
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        if (xm[i] && ym[i]) {
          xs.push_back(xv[i]);
          ys.push_back(yv[i]);
        }
      }
      const Stat2 st(xs, ys);
      const bool m = st.cnt >= 2 && st.spread_x() && st.spread_y();
      // 非全并列 ⇒ cxx、cyy > 0; 柯西–施瓦茨保证 |r| ≤ 1 不会发散
      const double v = m ? st.cxy / std::sqrt(st.cxx * st.cyy) : 0.0;
      for (int a = 0; a < A; ++a)
        put(ov, om, t * A + a, v, m);
    }
  }
};

// pct_rank(x) − pct_rank(y), 两列各自独立建直方图; m = xm && ym && cnt_x ≥ 1 && cnt_y ≥ 1
struct CsRankDiff {
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
                  const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A,
                  const Param &) {
    std::vector<float> xs, ys;
    for (int t = 0; t < T; ++t) {
      xs.clear();
      ys.clear();
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        if (xm[i])
          xs.push_back(xv[i]);
        if (ym[i])
          ys.push_back(yv[i]);
      }
      const Hist hx(xs), hy(ys);
      const bool ok = hx.cnt >= 1 && hy.cnt >= 1;
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        put(ov, om, i, static_cast<double>(hx.rank(xv[i])) - hy.rank(yv[i]),
            xm[i] && ym[i] && ok);
      }
    }
  }
};

// ===== 二元 / 三元: 分组 =====

// 组 id g = ym ? floor(y) : −1 (g < 0 不参与); 输出所属组的组均值 (组内只统计 xm 有效);
// m = xm && g ≥ 0 && 组内有效数 ≥ 1
struct CsGroupMean {
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
                  const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A,
                  const Param &) {
    std::vector<int> gid(A), gcnt;
    std::vector<double> gsum;
    for (int t = 0; t < T; ++t) {
      int gmax = -1;
      for (int a = 0; a < A; ++a) {
        gid[a] = gid_of(yv, ym, t * A + a);
        gmax = std::max(gmax, gid[a]);
      }
      gsum.assign(gmax + 1, 0.0);
      gcnt.assign(gmax + 1, 0);
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        if (xm[i] && gid[a] >= 0) {
          gsum[gid[a]] += xv[i];
          ++gcnt[gid[a]];
        }
      }
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a, g = gid[a];
        const bool m = xm[i] && g >= 0 && gcnt[g] >= 1;
        put(ov, om, i, m ? gsum[g] / gcnt[g] : 0.0, m);
      }
    }
  }
};

// 组内 pct rank (lo/hi 与直方图都在**该组的样本集**上独立建); m = xm && g ≥ 0
struct CsGroupRank {
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
                  const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A,
                  const Param &) {
    std::vector<int> gid(A);
    for (int t = 0; t < T; ++t) {
      int gmax = -1;
      for (int a = 0; a < A; ++a) {
        gid[a] = gid_of(yv, ym, t * A + a);
        gmax = std::max(gmax, gid[a]);
      }
      std::vector<std::vector<float>> gs(gmax + 1);
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        if (xm[i] && gid[a] >= 0)
          gs[gid[a]].push_back(xv[i]);
      }
      std::vector<Hist> gh;
      gh.reserve(gmax + 1);
      for (int g = 0; g <= gmax; ++g)
        gh.emplace_back(gs[g]);
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a, g = gid[a];
        const bool m = xm[i] && g >= 0;
        put(ov, om, i, m ? gh[g].rank(xv[i]) : 0.0, m);
      }
    }
  }
};

// 先对 y 桶法算 pct, bucket = clamp(floor(pct_y·k), 0, k−1) 作组 id (y 值域退化 → 一律 0 桶),
// 只有 xm && ym 的资产参与分桶; 再在桶内对 x 算 pct rank; m = 参与分桶 && cnt_y ≥ 1
struct CsCondRank {
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
                  const float *, const uint8_t *, float *ov, uint8_t *om, int T, int A,
                  const Param &p) {
    const int k = static_cast<int>(p.k);
    assert(k >= 1);
    std::vector<int> bid(A);
    std::vector<float> ys;
    for (int t = 0; t < T; ++t) {
      ys.clear();
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        if (xm[i] && ym[i])
          ys.push_back(yv[i]);
      }
      const Hist hy(ys);
      std::vector<std::vector<float>> bs(k);
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        bid[a] = -1;
        if (!(xm[i] && ym[i]))
          continue;
        bid[a] = hy.ok ? std::clamp(static_cast<int>(std::floor(hy.rank(yv[i]) * k)), 0, k - 1) : 0;
        bs[bid[a]].push_back(xv[i]);
      }
      std::vector<Hist> bh;
      bh.reserve(k);
      for (int b = 0; b < k; ++b)
        bh.emplace_back(bs[b]);
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a;
        const bool m = bid[a] >= 0 && hy.cnt >= 1;
        put(ov, om, i, m ? bh[bid[a]].rank(xv[i]) : 0.0, m);
      }
    }
  }
};

// z 为分组列: g = zm ? floor(z) : −1, 参与 = xm && ym && g ≥ 0.
// 组内 demean 得 x̃, ỹ, 再用**全体参与样本**算标量 β = Σ(x̃·ỹ)/Σỹ², 输出 x̃ − β·ỹ (FWL);
// m = 参与 && 全体参与数 ≥ 2 && 至少一组内 y 非全并列 (⟺ ỹ 不全为 0, 逐组 lo/hi 精确判)
struct CsGroupResid {
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,
                  const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A,
                  const Param &) {
    std::vector<int> gid(A), gcnt;
    std::vector<double> gx, gy;
    std::vector<float> glo, ghi;
    for (int t = 0; t < T; ++t) {
      int gmax = -1;
      for (int a = 0; a < A; ++a) {
        gid[a] = gid_of(zv, zm, t * A + a);
        gmax = std::max(gmax, gid[a]);
      }
      gx.assign(gmax + 1, 0.0);
      gy.assign(gmax + 1, 0.0);
      gcnt.assign(gmax + 1, 0);
      glo.assign(gmax + 1, 0.f);
      ghi.assign(gmax + 1, 0.f);
      auto join = [&](int a) {
        const int i = t * A + a;
        return xm[i] && ym[i] && gid[a] >= 0;
      };
      for (int a = 0; a < A; ++a)
        if (join(a)) {
          const int i = t * A + a, g = gid[a];
          glo[g] = gcnt[g] == 0 ? yv[i] : std::min(glo[g], yv[i]);
          ghi[g] = gcnt[g] == 0 ? yv[i] : std::max(ghi[g], yv[i]);
          gx[g] += xv[i];
          gy[g] += yv[i];
          ++gcnt[g];
        }
      bool any_spread = false;
      for (int g = 0; g <= gmax; ++g)
        any_spread = any_spread || (gcnt[g] >= 1 && spread(glo[g], ghi[g]));
      // 组内均值 → 组内 demean 后的全体幂和
      int n = 0;
      double syy_t = 0.0, sxy_t = 0.0;
      for (int a = 0; a < A; ++a)
        if (join(a)) {
          const int i = t * A + a, g = gid[a];
          const double dx = xv[i] - gx[g] / gcnt[g], dy = yv[i] - gy[g] / gcnt[g];
          syy_t += dy * dy;
          sxy_t += dx * dy;
          ++n;
        }
      const bool ok = n >= 2 && any_spread;
      const double b = ok ? sxy_t / syy_t : 0.0;
      for (int a = 0; a < A; ++a) {
        const int i = t * A + a, g = gid[a];
        const bool m = join(a) && ok;
        double v = 0.0;
        if (join(a)) {
          const double dx = xv[i] - gx[g] / gcnt[g], dy = yv[i] - gy[g] / gcnt[g];
          v = dx - b * dy;
        }
        put(ov, om, i, v, m);
      }
    }
  }
};

} // namespace factor::naive::cs
