#pragma once

// =============================================================================
// CS 算子的挖掘 CPU 后端 (factor::cpu::cs, OP_CS 全 15 个; 语义契约见 factor/Contract.hpp)
// =============================================================================
//   定位: 因子挖掘无 GPU 时的 fallback —— 整张量批量直算, 吞吐必须 ≥ stream.
//   独立第二实现: 除 Contract.hpp (共享数学件: mk/spread/den_ok/pct_of/probit/分桶规则) 外
//   不依赖任何后端 (否则 cpu↔stream 对拍空转).
//
//   CS = 每个时刻 t 一个截面 (行主序 [T][A] 的一行, 内存连续), 各 t 之间完全独立.
//   【性能】不 gather 拷贝 (直接在掩码上 branchless 归约); 直方图带 exclusive 前缀 → rank O(1) 查询;
//   分组族用扁平数组 (组 id < kMaxGroup), 缓冲跨 t 复用容量, 不逐 t 建 vector<vector>.
//
//   广播型 (Mean/Std/Median/Quantile/Beta/Corr): 对该行所有 A 个资产写同值, 掩码恒真 (空池行 → 中性值),
//     哪怕该资产自己的 x 缺失 —— 它描述的是截面, 不是资产.
//   相对型 (其余): 描述资产自身, 该资产 x 缺失 (二元还要 ym) 即输出无效; 统计量退化 → 契约第 6 条中性值.
//   统计口径: 方差 ddof=1; 样本 = 池内 g ∧ 有效 (一元 xm, 二元 xm && ym; 契约【截面池 g】); 输出 = 池内统计量作用于每个有效 x.
//   GROUP: 组内池成员统计量, 该组无池内成员 → 全池统计量 (回退表也按组算 G 次, 不按资产算).
//   出错策略: 只 assert. 【precise-math】依赖受控浮点, 编进 -fno-fast-math -fno-math-errno TU.
//
//   【横向归约的向量化】CS 的热点是整行 Σ (A 个资产归一个数), 与 TS "跨资产状态数组" 不同, 是真归约:
//     -fno-fast-math 下浮点加法不可重结合, 自动向量化必然失败 (逐元素 4 cycle 依赖链, 与 stream 同速).
//     行内 Σ 用 `#pragma clang fp reassociate(on)` 只放开重结合 (NaN / inf / 除法语义不动, 不是 fast-math):
//     求和顺序由编译器定, 同一构建下确定; GPU 本就是分块归约, 契约容差早已覆盖顺序差.
//     极值 LLVM 不认 fmin/fmax 归约, 走单调整数键 (fkey) 的 min/max: 位级精确, 不是近似.
//     每元素一次除法 (组均值 / 组内 demean) 一律先按组算好再 gather, 除法只做 G 次.
// =============================================================================

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
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

// ---- 浮点极值的单调整数键: 位型按符号翻转后整数序 = 浮点序 (有限值; 契约保证无 NaN) ----
//   fmin/fmax 归约 LLVM 不向量化, smin/smax 归约可以. 键与值互逆 (fval ∘ fkey = id), 位级精确;
//   唯一差别 ±0 键不同 (−0 < +0), 但下游 spread / bin_of 只做算术比较, −0 与 +0 等价.
//   无有效样本时哨兵解回来是 NaN 位型, 调用方按 n == 0 置 lo = hi = 0 (与原语义同).
inline int32_t fkey(float x) {
  int32_t u;
  std::memcpy(&u, &x, sizeof u);
  return u ^ ((u >> 31) & 0x7fffffff);
}
inline float fval(int32_t k) {
  const int32_t u = k ^ ((k >> 31) & 0x7fffffff);
  float x;
  std::memcpy(&x, &u, sizeof x);
  return x;
}
inline constexpr int32_t kKeyMax = std::numeric_limits<int32_t>::max(); // lo 哨兵 (min 单位元)
inline constexpr int32_t kKeyMin = std::numeric_limits<int32_t>::min(); // hi 哨兵 (max 单位元)
inline constexpr float kInfF = std::numeric_limits<float>::infinity();  // 空池不缩尾的 clamp 边界

// ---- 一元矩 (两遍中心化, 不用 Σx²−nμ²; lo/hi 给全并列判据) ----
//   need_m2 = false: 只要 n / mean / lo / hi (Mean / Demean), 省第二遍
struct M1 {
  int n = 0;
  double mean = 0.0, m2 = 0.0;
  float lo = 0.f, hi = 0.f;
  bool disp() const { return spread(lo, hi); }
  double sd() const { return std::sqrt(m2 / (n - 1)); } // 调用方保证 n ≥ 2
};
inline M1 moments(const float *v, const uint8_t *m, const uint8_t *g, int A, bool need_m2 = true) {
  M1 r;
  double s = 0.0;
  int32_t klo = kKeyMax, khi = kKeyMin;
  int n = 0;
  {
#pragma clang fp reassociate(on)
    for (int i = 0; i < A; ++i) {
      const bool b = (m[i] & g[i]) != 0;
      const int32_t k = fkey(v[i]);
      klo = std::min(klo, b ? k : kKeyMax);
      khi = std::max(khi, b ? k : kKeyMin);
      s += b ? static_cast<double>(v[i]) : 0.0;
      n += b;
    }
  }
  r.n = n;
  if (n == 0)
    return r;
  r.lo = fval(klo);
  r.hi = fval(khi);
  r.mean = s / n;
  if (!need_m2)
    return r;
  double m2 = 0.0;
  {
#pragma clang fp reassociate(on)
    for (int i = 0; i < A; ++i) {
      const double d = static_cast<double>(v[i]) - r.mean;
      m2 += (m[i] & g[i]) ? d * d : 0.0;
    }
  }
  r.m2 = m2;
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
inline M2 comoments(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const uint8_t *g, int A) {
  M2 r;
  double sx = 0.0, sy = 0.0;
  int32_t klox = kKeyMax, khix = kKeyMin, kloy = kKeyMax, khiy = kKeyMin;
  int n = 0;
  {
#pragma clang fp reassociate(on)
    for (int i = 0; i < A; ++i) {
      const bool b = (xm[i] & ym[i] & g[i]) != 0;
      const int32_t kx = fkey(xv[i]), ky = fkey(yv[i]);
      klox = std::min(klox, b ? kx : kKeyMax);
      khix = std::max(khix, b ? kx : kKeyMin);
      kloy = std::min(kloy, b ? ky : kKeyMax);
      khiy = std::max(khiy, b ? ky : kKeyMin);
      sx += b ? static_cast<double>(xv[i]) : 0.0;
      sy += b ? static_cast<double>(yv[i]) : 0.0;
      n += b;
    }
  }
  r.n = n;
  if (n == 0)
    return r;
  r.lox = fval(klox), r.hix = fval(khix);
  r.loy = fval(kloy), r.hiy = fval(khiy);
  r.mx = sx / n;
  r.my = sy / n;
  double cxx = 0.0, cyy = 0.0, cxy = 0.0;
  {
#pragma clang fp reassociate(on)
    for (int i = 0; i < A; ++i) {
      const bool b = (xm[i] & ym[i] & g[i]) != 0;
      const double dx = static_cast<double>(xv[i]) - r.mx, dy = static_cast<double>(yv[i]) - r.my;
      cxx += b ? dx * dx : 0.0;
      cyy += b ? dy * dy : 0.0;
      cxy += b ? dx * dy : 0.0;
    }
  }
  r.cxx = cxx, r.cyy = cyy, r.cxy = cxy;
  return r;
}

// ---- 序统计族直方图 (契约分桶规则 + exclusive 前缀 → rank O(1)) ----
struct Hist {
  int cnt[kBuckets];
  int pre[kBuckets + 1]; // pre[b] = Σ_{j<b} cnt[j]
  float lo = 0.f, hi = 0.f;
  int n = 0;
  bool ok = false; // n ≥ 1 且非全并列

  void build(const float *v, const uint8_t *m, const uint8_t *g, int A) {
    n = 0;
    ok = false;
    int32_t klo = kKeyMax, khi = kKeyMin;
    for (int i = 0; i < A; ++i) {
      const bool b = (m[i] & g[i]) != 0;
      const int32_t k = fkey(v[i]);
      klo = std::min(klo, b ? k : kKeyMax);
      khi = std::max(khi, b ? k : kKeyMin);
      n += b;
    }
    lo = n >= 1 ? fval(klo) : 0.f;
    hi = n >= 1 ? fval(khi) : 0.f;
    ok = n >= 1 && spread(lo, hi);
    if (!ok)
      return;
    std::fill(cnt, cnt + kBuckets, 0);
    for (int i = 0; i < A; ++i)
      if (m[i] & g[i])
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

// ---- 组 id: 分组列无效 → −1 (不参与); 上限断言在 max_gid ----
inline int gid(const float *v, const uint8_t *m, size_t i) {
  return m[i] ? static_cast<int>(std::floor(v[i])) : -1;
}

} // namespace detail

// -----------------------------------------------------------------------------
// 统一签名 (TS 后端同形 + 池掩码 g [T][A] 必填; 元数不足时调用方传 nullptr)
// -----------------------------------------------------------------------------
#define CP_SIG                                                                                             \
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym, const float *zv, \
                  const uint8_t *zm, const uint8_t *g, float *ov, uint8_t *om, int T, int A, const Param &p)

#define CP_NO23 (void)yv, (void)ym, (void)zv, (void)zm
#define CP_NO3 (void)zv, (void)zm

// ===== 一元: 矩 (REDUCE) =====

struct CsMean {
  CP_SIG {
    CP_NO23;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M1 s = detail::moments(xv + r, xm + r, g + r, A, /*need_m2=*/false);
      detail::bcast(ov, om, r, A, s.mean, true); // 空池 → 0 (mean 初值)
    }
  }
};

struct CsStd { // n < 2 / 全并列 → 0
  CP_SIG {
    CP_NO23;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M1 s = detail::moments(xv + r, xm + r, g + r, A);
      const bool ok = s.n >= 2 && s.disp();
      detail::bcast(ov, om, r, A, ok ? s.sd() : 0.0, true);
    }
  }
};

struct CsDemean { // 空池 → μ = 0, 即 x 原值
  CP_SIG {
    CP_NO23;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M1 s = detail::moments(xv + r, xm + r, g + r, A, /*need_m2=*/false);
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a, static_cast<double>(xv[r + a]) - s.mean, xm[r + a]);
    }
  }
};

struct CsZ { // σ 退化 → 0 (无尺度可比)
  CP_SIG {
    CP_NO23;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M1 s = detail::moments(xv + r, xm + r, g + r, A);
      const bool ok = s.n >= 2 && s.disp();
      const double sd = ok ? s.sd() : 1.0;
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a, ok ? (static_cast<double>(xv[r + a]) - s.mean) / sd : 0.0, xm[r + a]);
    }
  }
};

// ===== 一元: 序统计 (HIST) =====

struct CsRank { // 空池 / 全并列 → 0.5 (Hist 内)
  CP_SIG {
    CP_NO23;
    (void)p;
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, g + r, A);
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a, h.rank(xv[r + a]), xm[r + a]);
    }
  }
};

struct CsNormRank { // pct 先夹到 (0,1) 开区间再取正态分位, 否则 ±inf; 空池 → Φ⁻¹(0.5) = 0
  CP_SIG {
    CP_NO23;
    (void)p;
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, g + r, A);
      const double lo = 1.0 / (h.n + 1.0), hi = static_cast<double>(h.n) / (h.n + 1.0);
      for (int a = 0; a < A; ++a) {
        // probit 只在池非空时求值: n = 0 时 lo/hi 倒挂, clamp 的前置条件不成立
        const double v = h.n >= 1 ? probit(std::clamp<double>(h.rank(xv[r + a]), lo, hi)) : 0.0;
        detail::put(ov, om, r + a, v, xm[r + a]);
      }
    }
  }
};

struct CsQuantile { // k 分位 (k = 0.5 即中位); 空池 → 0
  CP_SIG {
    CP_NO23;
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, g + r, A);
      detail::bcast(ov, om, r, A, h.n >= 1 ? h.quantile(p.k) : 0.f, true);
    }
  }
};

struct CsWinsor { // 空池 → 不缩尾, x 原值
  CP_SIG {
    CP_NO23;
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, g + r, A);
      const float qa = h.quantile(p.k), qb = h.quantile(1.0 - static_cast<double>(p.k));
      const float w_lo = h.n >= 1 ? std::fmin(qa, qb) : -detail::kInfF, w_hi = h.n >= 1 ? std::fmax(qa, qb) : detail::kInfF;
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a,
                    static_cast<double>(std::fmin(std::fmax(xv[r + a], w_lo), w_hi)),
                    xm[r + a]);
    }
  }
};

struct CsBucket { // 等频分 k 组: floor(pct·k) ∈ 0..k−1; 值域退化 / 空池 → pct = 0.5 → 中间桶
  CP_SIG {
    CP_NO23;
    const int k = static_cast<int>(p.k);
    assert(k >= 1);
    detail::Hist h;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      h.build(xv + r, xm + r, g + r, A);
      for (int a = 0; a < A; ++a) {
        const int b = std::clamp(static_cast<int>(std::floor(h.rank(xv[r + a]) * k)), 0, k - 1);
        detail::put(ov, om, r + a, static_cast<double>(b), xm[r + a]);
      }
    }
  }
};

// ===== 二元: 回归 / 相关 (REDUCE) =====

struct CsResid { // x 对 y 的截面 OLS (含截距) 残差; y 无离散度 → β = 0, 即 x − μ^x
  CP_SIG {
    CP_NO3;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M2 s = detail::comoments(xv + r, xm + r, yv + r, ym + r, g + r, A);
      const bool ok = s.n >= 2 && s.dy_ok();
      const double b = ok ? s.beta() : 0.0;
      for (int a = 0; a < A; ++a) {
        const double v =
            (static_cast<double>(xv[r + a]) - s.mx) - b * (static_cast<double>(yv[r + a]) - s.my);
        detail::put(ov, om, r + a, v, xm[r + a] && ym[r + a]);
      }
    }
  }
};

struct CsBeta { // y 无离散度 → 0
  CP_SIG {
    CP_NO3;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M2 s = detail::comoments(xv + r, xm + r, yv + r, ym + r, g + r, A);
      const bool ok = s.n >= 2 && s.dy_ok();
      detail::bcast(ov, om, r, A, ok ? s.beta() : 0.0, true);
    }
  }
};

struct CsCorr { // 任一侧无离散度 → 0
  CP_SIG {
    CP_NO3;
    (void)p;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const detail::M2 s = detail::comoments(xv + r, xm + r, yv + r, ym + r, g + r, A);
      const bool ok = s.n >= 2 && s.dx_ok() && s.dy_ok();
      // 非全并列 ⇒ cxx、cyy > 0; 柯西–施瓦茨保证 |r| ≤ 1 不会发散
      detail::bcast(ov, om, r, A, ok ? s.cxy / std::sqrt(s.cxx * s.cyy) : 0.0, true);
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

struct CsGroupMean { // y = 组 id (行业等); 组均值广播到组员 (组无池成员 → 全池均值)
  // gather 回写遍单拎出来: 组表 gq / mean 是本地缓冲, 与输出平面不同块, 标 __restrict 告诉编译器 ov/om 的存储
  // 改不了它们 —— 否则 "任意下标 gather + 顺序存储" 做不了别名判定, 循环不向量化 (GROUP 族都这么写)
  static void gather(const int *__restrict gq, const double *__restrict mean, const uint8_t *xm, float *ov,
                     uint8_t *om, int A) {
    for (int a = 0; a < A; ++a) {
      const bool ok = gq[a] >= 0 && xm[a];
      const double v = mean[ok ? gq[a] : 0];
      detail::put(ov, om, a, ok ? v : 0.0, ok);
    }
  }
  CP_SIG {
    CP_NO3;
    (void)p;
    std::vector<int> gq, c;
    std::vector<double> s, mean;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const int G = detail::scan_gid(yv + r, ym + r, A, gq);
      const size_t GS = static_cast<size_t>(std::max(G, 1));
      s.assign(GS + 1, 0.0); // 末位 = 不参与者的汇集槽, 让散加无分支 (散加本身天然标量)
      c.assign(GS + 1, 0);
      for (int a = 0; a < A; ++a) {
        const bool ok = gq[a] >= 0 && xm[r + a] && g[r + a];
        const int q = ok ? gq[a] : static_cast<int>(GS);
        s[q] += ok ? static_cast<double>(xv[r + a]) : 0.0;
        c[q] += ok;
      }
      double sa = 0.0; // 全池 (回退)
      int na = 0;
      for (size_t q = 0; q < GS; ++q)
        sa += s[q], na += c[q];
      const double ma = sa / std::max(na, 1); // 全池也空 → 0
      mean.resize(GS);                        // 除法按组做 G 次, 不按资产做 A 次
      for (size_t q = 0; q < GS; ++q)
        mean[q] = c[q] >= 1 ? s[q] / c[q] : ma;
      gather(gq.data(), mean.data(), xm + r, ov + r, om + r, A);
    }
  }
};

struct CsGroupRank { // 组内 pct rank (每组独立定 lo/hi 与直方图; 组无池成员 → 全池直方图)
  CP_SIG {
    CP_NO3;
    (void)p;
    std::vector<int> gq, gn, gh, gpre;
    std::vector<float> glo, ghi;
    detail::Hist ha;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const int G = detail::scan_gid(yv + r, ym + r, A, gq);
      const size_t GS = static_cast<size_t>(std::max(G, 1));
      gn.assign(GS, 0);
      glo.assign(GS, 0.f);
      ghi.assign(GS, 0.f);
      for (int a = 0; a < A; ++a) // 逐组 lo/hi/计数 (池内)
        if (gq[a] >= 0 && xm[r + a] && g[r + a]) {
          const int q = gq[a];
          glo[q] = gn[q] == 0 ? xv[r + a] : std::fmin(glo[q], xv[r + a]);
          ghi[q] = gn[q] == 0 ? xv[r + a] : std::fmax(ghi[q], xv[r + a]);
          ++gn[q];
        }
      gh.assign(GS * kBuckets, 0); // 逐组直方图 + exclusive 前缀; 顺手查有没有资产落在空组
      bool need = false;
      for (int a = 0; a < A; ++a) {
        if (gq[a] < 0 || !xm[r + a])
          continue;
        need = need || gn[gq[a]] < 1;
        if (g[r + a] && spread(glo[gq[a]], ghi[gq[a]]))
          ++gh[static_cast<size_t>(gq[a]) * kBuckets + bin_of(xv[r + a], glo[gq[a]], ghi[gq[a]])];
      }
      gpre.assign(GS * (kBuckets + 1), 0);
      for (size_t q = 0; q < GS; ++q)
        for (int b = 0; b < kBuckets; ++b)
          gpre[q * (kBuckets + 1) + b + 1] = gpre[q * (kBuckets + 1) + b] + gh[q * kBuckets + b];
      if (need) // 全池 (回退), 只在有资产落在空组时建 (正常日子行业全在池里, 不走)
        ha.build(xv + r, xm + r, g + r, A);
      for (int a = 0; a < A; ++a) {
        const int q = gq[a];
        const bool ok = q >= 0 && xm[r + a];
        double v = 0.0;
        if (ok) {
          if (gn[q] < 1)
            v = ha.rank(xv[r + a]); // 组无池成员 → 全池; 全池也空 → Hist 给 0.5
          else if (!spread(glo[q], ghi[q]))
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

struct CsGroupResid { // FWL: 按 z 分组, x/y 组内 demean 后 x 对 y 回归残差
  // 退化 = 去均值后的 ỹ 全为 0 ⟺ 每组内 y 全并列 (逐组 lo/hi 精确判, 不看 Σỹ²) → β = 0, 输出 x̃
  // 拟合样本 = 池内参与者; 输出对象 = 全部参与者 (xm ∧ ym ∧ 组有效), 组无池成员的用全池均值去均值.
  // 组内 demean 遍: x̃ / ỹ 落到行缓冲 (gather 组均值只做这一次), 顺手归约池内的 Σx̃ỹ / Σỹ² / n.
  // 组表与行缓冲都是本地块, 标 __restrict 的理由同 CsGroupMean::gather
  static void center(const int *__restrict gq, const double *__restrict mx, const double *__restrict my,
                     const float *xv, const float *yv, const uint8_t *g, double *__restrict xt, double *__restrict yt, int A,
                     double &sxy, double &syy, int &n) {
#pragma clang fp reassociate(on) // 只准放在复合语句开头, 故作用于整个函数体
    double axy = 0.0, ayy = 0.0;
    int an = 0;
    for (int a = 0; a < A; ++a) {
      const bool in = gq[a] >= 0;
      const bool pl = in && g[a];
      const int q = in ? gq[a] : 0;
      const double x = in ? static_cast<double>(xv[a]) - mx[q] : 0.0;
      const double y = in ? static_cast<double>(yv[a]) - my[q] : 0.0;
      xt[a] = x, yt[a] = y;
      axy += pl ? x * y : 0.0;
      ayy += pl ? y * y : 0.0;
      an += pl;
    }
    sxy = axy, syy = ayy, n = an;
  }
  CP_SIG {
    (void)p;
    std::vector<int> gq, c;
    std::vector<double> sx, sy, mx, my, xt, yt;
    std::vector<float> lo, hi;
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      const int G = detail::scan_gid(zv + r, zm + r, A, gq);
      const size_t GS = static_cast<size_t>(std::max(G, 1));
      sx.assign(GS, 0.0);
      sy.assign(GS, 0.0);
      c.assign(GS, 0);
      lo.assign(GS, 0.f);
      hi.assign(GS, 0.f);
      double sxa = 0.0, sya = 0.0; // 全池 (回退)
      int na = 0;
      for (int a = 0; a < A; ++a) { // 参与 = xm && ym && 组有效; 不参与的组 id 置 −1; 只有池内参与者进组统计 (散加, 天然标量)
        if (gq[a] < 0 || !xm[r + a] || !ym[r + a]) {
          gq[a] = -1;
          continue;
        }
        if (!g[r + a])
          continue;
        const int q = gq[a];
        lo[q] = c[q] == 0 ? yv[r + a] : std::fmin(lo[q], yv[r + a]);
        hi[q] = c[q] == 0 ? yv[r + a] : std::fmax(hi[q], yv[r + a]);
        sx[q] += xv[r + a];
        sy[q] += yv[r + a];
        ++c[q];
        sxa += xv[r + a];
        sya += yv[r + a];
        ++na;
      }
      bool any_spread = false;
      const double mxa = sxa / std::max(na, 1), mya = sya / std::max(na, 1);
      mx.resize(GS), my.resize(GS); // 组均值按组算 G 次, 每元素不再除
      for (size_t q = 0; q < GS; ++q) {
        any_spread = any_spread || (c[q] >= 1 && spread(lo[q], hi[q]));
        mx[q] = c[q] >= 1 ? sx[q] / c[q] : mxa;
        my[q] = c[q] >= 1 ? sy[q] / c[q] : mya;
      }
      xt.resize(static_cast<size_t>(A)), yt.resize(static_cast<size_t>(A));
      double sxy, syy;
      int n;
      center(gq.data(), mx.data(), my.data(), xv + r, yv + r, g + r, xt.data(), yt.data(), A, sxy, syy, n);
      const bool ok = n >= 2 && any_spread; // 退化 → β = 0, 输出 x̃
      const double b = ok ? sxy / syy : 0.0;
      for (int a = 0; a < A; ++a) { // 回写: 行缓冲顺序读, 无 gather
        const bool in = gq[a] >= 0;
        detail::put(ov, om, r + a, in ? xt[a] - b * yt[a] : 0.0, in);
      }
    }
  }
};

#undef CP_NO3
#undef CP_NO23
#undef CP_SIG

} // namespace factor::cpu::cs
