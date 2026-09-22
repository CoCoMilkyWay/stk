#pragma once

// =============================================================================
// CS 算子的挖掘 CPU 后端 (factor::cpu::cs, OP_CS 全 20 个; 语义契约见 factor/Contract.hpp)
// =============================================================================
//   定位: 因子挖掘无 GPU 时的 fallback —— 整张量批量直算, 吞吐必须 ≥ stream.
//   独立第二实现: 除 Contract.hpp (共享数学件: mk/spread/den_ok/pct_of/probit/分桶规则) 外
//   不依赖任何后端 (否则 cpu↔stream 对拍空转).
//
//   CS = 每个时刻 t 一个截面 (行主序 [T][A] 的一行, 内存连续), 各 t 之间完全独立.
//   【性能】不 gather 拷贝 (直接在掩码上 branchless 归约); 直方图带 exclusive 前缀 → rank O(1) 查询;
//   分组族用扁平数组 (组 id < kMaxGroup), 缓冲跨 t 复用容量, 不逐 t 建 vector<vector>.
//
//   广播型 (Mean/Std/Median/Quantile/Beta/Corr): 对该行所有 A 个资产写同值同掩码,
//     哪怕该资产自己的 x 缺失 —— 它描述的是截面, 不是资产.
//   相对型 (其余): 描述资产自身, 该资产 x 缺失 (二元还要 ym) 即输出无效.
//   统计口径: 方差 ddof=1; 有效样本 = 一元看 xm, 二元看 xm && ym.
//   出错策略: 只 assert. 【precise-math】依赖受控浮点, 编进 -fno-fast-math TU.
// =============================================================================

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <vector>

#include "factor/Contract.hpp"

namespace factor::cpu::cs {

namespace detail {

// ---- 输出: 一律经 mk (无效或非有限 → 0/false) ----
inline void put(float *ov, uint8_t *om, size_t i, double v, bool m) {
  const Val r = mk(v, m);
  ov[i] = r.v;
  om[i] = r.m ? 1u : 0u;
}

// 广播: mk 一次, 全行同值同掩码
inline void bcast(float *ov, uint8_t *om, size_t r, int A, double v, bool m) {
  const Val x = mk(v, m);
  const uint8_t mm = x.m ? 1u : 0u;
  for (int a = 0; a < A; ++a) {
    ov[r + a] = x.v;
    om[r + a] = mm;
  }
}

// ---- 一元矩 (两遍中心化, 不用 Σx²−nμ²; lo/hi 给全并列判据) ----
struct M1 {
  int n = 0;
  double mean = 0.0, m2 = 0.0;
  float lo = 0.f, hi = 0.f;
  bool disp() const { return spread(lo, hi); }
  double sd() const { return std::sqrt(m2 / (n - 1)); } // 调用方保证 n ≥ 2
};
inline M1 moments(const float *v, const uint8_t *m, int A) {
  M1 r;
  double s = 0.0;
  float lo = 0.f, hi = 0.f;
  int n = 0;
  for (int i = 0; i < A; ++i) {
    const bool b = m[i] != 0;
    lo = b ? (n == 0 ? v[i] : std::fmin(lo, v[i])) : lo;
    hi = b ? (n == 0 ? v[i] : std::fmax(hi, v[i])) : hi;
    s += b ? static_cast<double>(v[i]) : 0.0;
    n += b;
  }
  r.n = n;
  r.lo = lo;
  r.hi = hi;
  if (n == 0)
    return r;
  r.mean = s / n;
  for (int i = 0; i < A; ++i) {
    const double d = v[i] - r.mean;
    r.m2 += m[i] ? d * d : 0.0;
  }
  return r;
}

// ---- 二元共矩 (配对样本, 两遍) ----
struct M2 {
  int n = 0;
  double mx = 0.0, my = 0.0, cxx = 0.0, cyy = 0.0, cxy = 0.0;
  float lox = 0.f, hix = 0.f, loy = 0.f, hiy = 0.f;
  bool dx_ok() const { return spread(lox, hix); }
  bool dy_ok() const { return spread(loy, hiy); }
  double beta() const { return cxy / cyy; } // 只在 dy_ok 时求值
};
inline M2 comoments(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, int A) {
  M2 r;
  double sx = 0.0, sy = 0.0;
  int n = 0;
  for (int i = 0; i < A; ++i) {
    const bool b = xm[i] && ym[i];
    r.lox = b ? (n == 0 ? xv[i] : std::fmin(r.lox, xv[i])) : r.lox;
    r.hix = b ? (n == 0 ? xv[i] : std::fmax(r.hix, xv[i])) : r.hix;
    r.loy = b ? (n == 0 ? yv[i] : std::fmin(r.loy, yv[i])) : r.loy;
    r.hiy = b ? (n == 0 ? yv[i] : std::fmax(r.hiy, yv[i])) : r.hiy;
    sx += b ? static_cast<double>(xv[i]) : 0.0;
    sy += b ? static_cast<double>(yv[i]) : 0.0;
    n += b;
  }
  r.n = n;
  if (n == 0)
    return r;
  r.mx = sx / n;
  r.my = sy / n;
  for (int i = 0; i < A; ++i) {
    const bool b = xm[i] && ym[i];
    const double dx = xv[i] - r.mx, dy = yv[i] - r.my;
    r.cxx += b ? dx * dx : 0.0;
    r.cyy += b ? dy * dy : 0.0;
    r.cxy += b ? dx * dy : 0.0;
  }
  return r;
}

// ---- 序统计族直方图 (契约分桶规则 + exclusive 前缀 → rank O(1)) ----
struct Hist {
  int cnt[kBuckets];
  int pre[kBuckets + 1]; // pre[b] = Σ_{j<b} cnt[j]
  float lo = 0.f, hi = 0.f;
  int n = 0;
  bool ok = false; // n ≥ 1 且非全并列

  void build(const float *v, const uint8_t *m, int A) {
    n = 0;
    ok = false;
    float l = 0.f, h = 0.f;
    for (int i = 0; i < A; ++i) {
      const bool b = m[i] != 0;
      l = b ? (n == 0 ? v[i] : std::fmin(l, v[i])) : l;
      h = b ? (n == 0 ? v[i] : std::fmax(h, v[i])) : h;
      n += b;
    }
    lo = l;
    hi = h;
    ok = n >= 1 && spread(lo, hi);
    if (!ok)
      return;
    std::fill(cnt, cnt + kBuckets, 0);
    for (int i = 0; i < A; ++i)
      if (m[i])
        ++cnt[bin_of(v[i], lo, hi)];
    pre[0] = 0;
    for (int b = 0; b < kBuckets; ++b)
      pre[b + 1] = pre[b] + cnt[b];
  }
  float rank(float x) const { // 并列均秩的 pct rank; 全并列 → 0.5
    if (!ok)
      return 0.5f;
    const int b = bin_of(x, lo, hi);
    return pct_of(pre[b], cnt[b], n);
  }
  float quantile(double q) const { // 最小的桶使前缀累计 ≥ ⌈q·n⌉
    if (!ok || q <= 0.0)
      return lo;
    if (q >= 1.0)
      return hi;
    const int need = std::max(1, static_cast<int>(std::ceil(q * n)));
    for (int b = 0; b < kBuckets; ++b)
      if (pre[b + 1] >= need)
        return bin_center(b, lo, hi);
    return hi;
  }
};

inline constexpr double kWinsorQ = 0.01; // CsWinsorRank / CsWinsorZ 的固定缩尾分位

// ---- 组 id: 分组列无效 → −1 (不参与); 上限断言在 max_gid ----
inline int gid(const float *v, const uint8_t *m, size_t i) {
  return m[i] ? static_cast<int>(std::floor(v[i])) : -1;
}

} // namespace detail

// -----------------------------------------------------------------------------
// 统一签名 (与 TS 后端同形; 元数不足时调用方传 nullptr)
// -----------------------------------------------------------------------------
#define CP_SIG                                                                              \
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,   \
                  const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, \
                  const Param &p)

#define CP_NO23 (void)yv, (void)ym, (void)zv, (void)zm
#define CP_NO3 (void)zv, (void)zm

// ===== 一元: 矩 (REDUCE) =====

struct CsMean {
  CP_SIG {
    CP_NO23;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M1 s = detail::moments(xv + r, xm + r, A);
      detail::bcast(ov, om, r, A, s.mean, s.n >= 1);
    }
  }
};

struct CsStd {
  CP_SIG {
    CP_NO23;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M1 s = detail::moments(xv + r, xm + r, A);
      const bool ok = s.n >= 2 && s.disp();
      detail::bcast(ov, om, r, A, ok ? s.sd() : 0.0, ok);
    }
  }
};

struct CsDemean {
  CP_SIG {
    CP_NO23;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M1 s = detail::moments(xv + r, xm + r, A);
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a, static_cast<double>(xv[r + a]) - s.mean, xm[r + a] && s.n >= 1);
    }
  }
};

struct CsZ {
  CP_SIG {
    CP_NO23;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M1 s = detail::moments(xv + r, xm + r, A);
      const bool ok = s.n >= 2 && s.disp();
      const double sd = ok ? s.sd() : 1.0;
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a, (static_cast<double>(xv[r + a]) - s.mean) / sd, xm[r + a] && ok);
    }
  }
};

// ===== 一元: 序统计 (HIST) =====

struct CsRank {
  CP_SIG {
    CP_NO23;
    (void)p;
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, A);
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a, h.rank(xv[r + a]), xm[r + a] && h.n >= 1);
    }
  }
};

struct CsNormRank { // pct 先夹到 (0,1) 开区间再取正态分位, 否则 ±inf
  CP_SIG {
    CP_NO23;
    (void)p;
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, A);
      const double lo = 1.0 / (h.n + 1.0), hi = static_cast<double>(h.n) / (h.n + 1.0);
      for (int a = 0; a < A; ++a) {
        // probit 只在掩码成立时求值: 截面全缺失 (n = 0) 时 lo/hi 倒挂, clamp 的前置条件不成立
        const bool ok = xm[r + a] && h.n >= 1;
        const double v = ok ? probit(std::clamp<double>(h.rank(xv[r + a]), lo, hi)) : 0.0;
        detail::put(ov, om, r + a, v, ok);
      }
    }
  }
};

struct CsMedian {
  CP_SIG {
    CP_NO23;
    (void)p;
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, A);
      detail::bcast(ov, om, r, A, h.quantile(0.5), h.n >= 1);
    }
  }
};

struct CsQuantile {
  CP_SIG {
    CP_NO23;
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, A);
      detail::bcast(ov, om, r, A, h.quantile(p.k), h.n >= 1);
    }
  }
};

struct CsWinsor {
  CP_SIG {
    CP_NO23;
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, A);
      const float qa = h.quantile(p.k), qb = h.quantile(1.0 - static_cast<double>(p.k));
      const float w_lo = std::fmin(qa, qb), w_hi = std::fmax(qa, qb);
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a,
                    static_cast<double>(std::fmin(std::fmax(xv[r + a], w_lo), w_hi)),
                    xm[r + a] && h.n >= 1);
    }
  }
};

struct CsWinsorRank { // 固定分位缩尾后重建直方图再排名: 极端值不再独占秩尾
  CP_SIG {
    CP_NO23;
    (void)p;
    detail::Hist h0, h;
    std::vector<float> w;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h0.build(xv + r, xm + r, A);
      const float qa = h0.quantile(detail::kWinsorQ), qb = h0.quantile(1.0 - detail::kWinsorQ);
      const float w_lo = std::fmin(qa, qb), w_hi = std::fmax(qa, qb);
      w.resize(static_cast<size_t>(A));
      for (int a = 0; a < A; ++a)
        w[a] = std::fmin(std::fmax(xv[r + a], w_lo), w_hi);
      h.build(w.data(), xm + r, A);
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a, h.rank(w[a]), xm[r + a] && h.n >= 1);
    }
  }
};

struct CsWinsorZ {
  CP_SIG {
    CP_NO23;
    (void)p;
    detail::Hist h0;
    std::vector<float> w;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h0.build(xv + r, xm + r, A);
      const float qa = h0.quantile(detail::kWinsorQ), qb = h0.quantile(1.0 - detail::kWinsorQ);
      const float w_lo = std::fmin(qa, qb), w_hi = std::fmax(qa, qb);
      w.resize(static_cast<size_t>(A));
      for (int a = 0; a < A; ++a)
        w[a] = std::fmin(std::fmax(xv[r + a], w_lo), w_hi);
      const detail::M1 s = detail::moments(w.data(), xm + r, A);
      const bool ok = s.n >= 2 && s.disp();
      const double sd = ok ? s.sd() : 1.0;
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a, (static_cast<double>(w[a]) - s.mean) / sd, xm[r + a] && ok);
    }
  }
};

struct CsBucket { // 等频分 k 组: floor(pct·k) ∈ 0..k−1; 值域退化一律给 0
  CP_SIG {
    CP_NO23;
    const int k = static_cast<int>(p.k);
    assert(k >= 1);
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, A);
      for (int a = 0; a < A; ++a) {
        const int b =
            h.ok ? std::clamp(static_cast<int>(std::floor(h.rank(xv[r + a]) * k)), 0, k - 1) : 0;
        detail::put(ov, om, r + a, static_cast<double>(b), xm[r + a] && h.n >= 1);
      }
    }
  }
};

// ===== 二元: 回归 / 相关 (REDUCE) =====

struct CsResid { // x 对 y 的截面 OLS (含截距) 残差
  CP_SIG {
    CP_NO3;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M2 s = detail::comoments(xv + r, xm + r, yv + r, ym + r, A);
      const bool ok = s.n >= 2 && s.dy_ok();
      const double b = ok ? s.beta() : 0.0;
      for (int a = 0; a < A; ++a) {
        const double v =
            (static_cast<double>(xv[r + a]) - s.mx) - b * (static_cast<double>(yv[r + a]) - s.my);
        detail::put(ov, om, r + a, v, xm[r + a] && ym[r + a] && ok);
      }
    }
  }
};

struct CsBeta {
  CP_SIG {
    CP_NO3;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M2 s = detail::comoments(xv + r, xm + r, yv + r, ym + r, A);
      const bool ok = s.n >= 2 && s.dy_ok();
      detail::bcast(ov, om, r, A, ok ? s.beta() : 0.0, ok);
    }
  }
};

struct CsCorr {
  CP_SIG {
    CP_NO3;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M2 s = detail::comoments(xv + r, xm + r, yv + r, ym + r, A);
      const bool ok = s.n >= 2 && s.dx_ok() && s.dy_ok();
      // 非全并列 ⇒ cxx、cyy > 0; 柯西–施瓦茨保证 |r| ≤ 1 不会发散
      detail::bcast(ov, om, r, A, ok ? s.cxy / std::sqrt(s.cxx * s.cyy) : 0.0, ok);
    }
  }
};

struct CsRankDiff { // 两列各自独立建直方图
  CP_SIG {
    CP_NO3;
    (void)p;
    detail::Hist hx, hy;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      hx.build(xv + r, xm + r, A);
      hy.build(yv + r, ym + r, A);
      const bool ok = hx.n >= 1 && hy.n >= 1;
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a,
                    static_cast<double>(hx.rank(xv[r + a])) - hy.rank(yv[r + a]),
                    xm[r + a] && ym[r + a] && ok);
    }
  }
};

// ===== 二元 / 三元: 分组 (GROUP) =====

namespace detail {

// 扫组 id 并返回组数 G = max gid + 1 (上限断言; 全无效 → 0)
inline int scan_gid(const float *gv, const uint8_t *gm, int A, std::vector<int> &g) {
  g.resize(static_cast<size_t>(A));
  int gmax = -1;
  for (int i = 0; i < A; ++i) {
    g[i] = gid(gv, gm, static_cast<size_t>(i));
    gmax = std::max(gmax, g[i]);
  }
  assert(gmax < kMaxGroup); // 组 id 当下标用, 过大说明传进来的不是分组列
  return gmax + 1;
}

} // namespace detail

struct CsGroupMean { // y = 组 id (行业等); 组均值广播到组员
  CP_SIG {
    CP_NO3;
    (void)p;
    std::vector<int> g, c;
    std::vector<double> s;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const int G = detail::scan_gid(yv + r, ym + r, A, g);
      const size_t GS = static_cast<size_t>(std::max(G, 1));
      s.assign(GS, 0.0);
      c.assign(GS, 0);
      for (int a = 0; a < A; ++a)
        if (g[a] >= 0 && xm[r + a]) {
          s[g[a]] += xv[r + a];
          ++c[g[a]];
        }
      for (int a = 0; a < A; ++a) {
        const bool ok = g[a] >= 0 && xm[r + a] && c[g[a]] >= 1;
        detail::put(ov, om, r + a, ok ? s[g[a]] / c[g[a]] : 0.0, ok);
      }
    }
  }
};

struct CsGroupRank { // 组内 pct rank (每组独立定 lo/hi 与直方图)
  CP_SIG {
    CP_NO3;
    (void)p;
    std::vector<int> g, gn, gh, gpre;
    std::vector<float> glo, ghi;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const int G = detail::scan_gid(yv + r, ym + r, A, g);
      const size_t GS = static_cast<size_t>(std::max(G, 1));
      gn.assign(GS, 0);
      glo.assign(GS, 0.f);
      ghi.assign(GS, 0.f);
      for (int a = 0; a < A; ++a) // 逐组 lo/hi/计数
        if (g[a] >= 0 && xm[r + a]) {
          const int q = g[a];
          glo[q] = gn[q] == 0 ? xv[r + a] : std::fmin(glo[q], xv[r + a]);
          ghi[q] = gn[q] == 0 ? xv[r + a] : std::fmax(ghi[q], xv[r + a]);
          ++gn[q];
        }
      gh.assign(GS * kBuckets, 0); // 逐组直方图 + exclusive 前缀
      for (int a = 0; a < A; ++a)
        if (g[a] >= 0 && xm[r + a] && spread(glo[g[a]], ghi[g[a]]))
          ++gh[static_cast<size_t>(g[a]) * kBuckets + bin_of(xv[r + a], glo[g[a]], ghi[g[a]])];
      gpre.assign(GS * (kBuckets + 1), 0);
      for (size_t q = 0; q < GS; ++q)
        for (int b = 0; b < kBuckets; ++b)
          gpre[q * (kBuckets + 1) + b + 1] = gpre[q * (kBuckets + 1) + b] + gh[q * kBuckets + b];
      for (int a = 0; a < A; ++a) {
        const bool ok = g[a] >= 0 && xm[r + a];
        double v = 0.0;
        if (ok) {
          const int q = g[a];
          if (!spread(glo[q], ghi[q]))
            v = 0.5; // 组内全并列
          else {
            const int b = bin_of(xv[r + a], glo[q], ghi[q]);
            v = pct_of(gpre[static_cast<size_t>(q) * (kBuckets + 1) + b],
                       gh[static_cast<size_t>(q) * kBuckets + b], gn[q]);
          }
        }
        detail::put(ov, om, r + a, v, ok);
      }
    }
  }
};

struct CsCondRank { // 先按 y 等频分 k 桶 (桶界只由双有效资产定), 再在桶内对 x 排名
  CP_SIG {
    CP_NO3;
    const int k = static_cast<int>(p.k);
    assert(k >= 1);
    detail::Hist hy;
    std::vector<uint8_t> both;
    std::vector<int> bof, bn, bh, bpre;
    std::vector<float> blo, bhi;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      both.resize(static_cast<size_t>(A));
      for (int a = 0; a < A; ++a)
        both[a] = (xm[r + a] && ym[r + a]) ? 1 : 0;
      hy.build(yv + r, both.data(), A);
      bof.assign(static_cast<size_t>(A), -1);
      const size_t KS = static_cast<size_t>(k);
      bn.assign(KS, 0);
      blo.assign(KS, 0.f);
      bhi.assign(KS, 0.f);
      for (int a = 0; a < A; ++a) { // 分桶 + 桶内 lo/hi/计数
        if (!both[a])
          continue;
        const int b =
            hy.ok ? std::clamp(static_cast<int>(std::floor(hy.rank(yv[r + a]) * k)), 0, k - 1) : 0;
        bof[a] = b;
        blo[b] = bn[b] == 0 ? xv[r + a] : std::fmin(blo[b], xv[r + a]);
        bhi[b] = bn[b] == 0 ? xv[r + a] : std::fmax(bhi[b], xv[r + a]);
        ++bn[b];
      }
      bh.assign(KS * kBuckets, 0); // 桶内直方图 + exclusive 前缀
      for (int a = 0; a < A; ++a)
        if (bof[a] >= 0 && spread(blo[bof[a]], bhi[bof[a]]))
          ++bh[static_cast<size_t>(bof[a]) * kBuckets + bin_of(xv[r + a], blo[bof[a]], bhi[bof[a]])];
      bpre.assign(KS * (kBuckets + 1), 0);
      for (size_t q = 0; q < KS; ++q)
        for (int b = 0; b < kBuckets; ++b)
          bpre[q * (kBuckets + 1) + b + 1] = bpre[q * (kBuckets + 1) + b] + bh[q * kBuckets + b];
      for (int a = 0; a < A; ++a) {
        const bool ok = bof[a] >= 0 && hy.n >= 1;
        double v = 0.0;
        if (ok) {
          const int q = bof[a];
          if (!spread(blo[q], bhi[q]))
            v = 0.5;
          else {
            const int b = bin_of(xv[r + a], blo[q], bhi[q]);
            v = pct_of(bpre[static_cast<size_t>(q) * (kBuckets + 1) + b],
                       bh[static_cast<size_t>(q) * kBuckets + b], bn[q]);
          }
        }
        detail::put(ov, om, r + a, v, ok);
      }
    }
  }
};

struct CsGroupResid { // FWL: 按 z 分组, x/y 组内 demean 后 x 对 y 回归残差
  // 退化 = 去均值后的 ỹ 全为 0 ⟺ 每组内 y 全并列 (逐组 lo/hi 精确判, 不看 Σỹ²)
  CP_SIG {
    (void)p;
    std::vector<int> g, c;
    std::vector<double> sx, sy;
    std::vector<float> lo, hi;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const int G = detail::scan_gid(zv + r, zm + r, A, g);
      const size_t GS = static_cast<size_t>(std::max(G, 1));
      sx.assign(GS, 0.0);
      sy.assign(GS, 0.0);
      c.assign(GS, 0);
      lo.assign(GS, 0.f);
      hi.assign(GS, 0.f);
      for (int a = 0; a < A; ++a) { // 参与 = xm && ym && 组有效; 不参与的组 id 置 −1
        if (g[a] < 0 || !xm[r + a] || !ym[r + a]) {
          g[a] = -1;
          continue;
        }
        const int q = g[a];
        lo[q] = c[q] == 0 ? yv[r + a] : std::fmin(lo[q], yv[r + a]);
        hi[q] = c[q] == 0 ? yv[r + a] : std::fmax(hi[q], yv[r + a]);
        sx[q] += xv[r + a];
        sy[q] += yv[r + a];
        ++c[q];
      }
      bool any_spread = false;
      for (size_t q = 0; q < GS; ++q)
        any_spread = any_spread || (c[q] >= 1 && spread(lo[q], hi[q]));
      double sxy = 0.0, syy = 0.0;
      int n = 0;
      for (int a = 0; a < A; ++a) {
        if (g[a] < 0)
          continue;
        const int q = g[a];
        const double xt = xv[r + a] - sx[q] / c[q], yt = yv[r + a] - sy[q] / c[q];
        sxy += xt * yt;
        syy += yt * yt;
        ++n;
      }
      const bool ok = n >= 2 && any_spread;
      const double b = ok ? sxy / syy : 0.0;
      for (int a = 0; a < A; ++a) {
        if (g[a] < 0) {
          detail::put(ov, om, r + a, 0.0, false);
          continue;
        }
        const int q = g[a];
        const double xt = xv[r + a] - sx[q] / c[q], yt = yv[r + a] - sy[q] / c[q];
        detail::put(ov, om, r + a, xt - b * yt, ok);
      }
    }
  }
};

#undef CP_NO3
#undef CP_NO23
#undef CP_SIG

} // namespace factor::cpu::cs
