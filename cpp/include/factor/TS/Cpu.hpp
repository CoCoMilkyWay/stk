#pragma once

// =============================================================================
// TS 算子的挖掘 CPU 后端 (factor::cpu::ts, 72 个, 对应 OpTable 的 OP_TS = OP_TS0..3)
// =============================================================================
//   定位: 因子挖掘无 GPU 时的 fallback —— 整张量批量直算, 不逐点因果推进, 吞吐必须 ≥ stream.
//   语义与流式一致 (掩码逐位相等, 值在对拍容差内); 独立第二实现, **不得** include 任何
//   Stream/Gpu 头, 不复用其算法代码 (否则 cpu↔stream 对拍空转).
//
//   【布局与向量化】SoA 行主序 [T][A]: 内层 A 连续, 一切按"行推进 + 跨资产状态数组"组织,
//   内层循环 branchless select 交给自动向量化; 序统计族按资产分块 (kBlockA, 直方图驻留 L2).
//
//   【算法】(旧 naive 参考实现按定义 O(T·d·A) 重算, 本文件全部摊到 O(T·A) 或 O(T·A·B)):
//     POINT   单遍逐元素
//     EXPAND  沿 t 递推累加 (double 幂和), 段界 reset; 序统计 = 增量直方图 (段内 lo/hi 只扩不缩, 扩时重建)
//     ROLL    滑动加减幂和 (double); spread 判据要求精确 lo/hi → van Herk 分块前后缀滑窗极值;
//             序统计 = 环形窗缓存 + 直方图 (lo/hi 未变则增量改桶, 变了整窗重建)
//     EXPO    沿 t 递推
//
//   【数值】滑动加减幂和相对整窗重算有 O(ε_double) 漂移; 近 ulp 简并窗 (真中心矩 ≈ double 舍入噪声)
//   下值甚至掩码可能与流式不一致 —— 与 GPU SCAN 后端同一风险等级, 契约的造数与容差刻意避开该带.
//   中心矩用 fmax(·, 0) 钳掉相消出负; 溢出由 mk 出口转无效 (契约: 溢出是数据属性, 不是 bug).
//
//   出错策略: 不做错误处理, 只 assert (参数非法立刻死在最早处).
//   【precise-math】依赖受控浮点, 编进 -fno-fast-math TU.
// =============================================================================

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <vector>

#include "factor/Contract.hpp"

namespace factor::cpu::ts {

// -----------------------------------------------------------------------------
// detail: 输出 / 幂和→中心矩 / 滑窗极值 (van Herk) / 桶分位
// -----------------------------------------------------------------------------
namespace detail {

inline constexpr float kInf = std::numeric_limits<float>::infinity();
inline constexpr int kBlockA = 128; // 序统计族的资产分块宽 (直方图 + 环形缓存驻留 L2)

inline void put(float *ov, uint8_t *om, size_t i, double v, bool m) {
  const Val r = mk(v, m);
  ov[i] = r.v;
  om[i] = r.m ? 1u : 0u;
}

// ---- 幂和 → 中心矩和 Σ(x−μ)^k (未除 n; 相消出负由调用方 fmax 钳 0) ----
inline double c2_of(double s1, double s2, int n) { return s2 - s1 * s1 / n; }
inline double c3_of(double s1, double s2, double s3, int n) {
  const double nn = n;
  return s3 - 3.0 * s2 * s1 / nn + 2.0 * s1 * s1 * s1 / (nn * nn);
}
inline double c4_of(double s1, double s2, double s3, double s4, int n) {
  const double nn = n;
  return s4 - 4.0 * s3 * s1 / nn + 6.0 * s2 * s1 * s1 / (nn * nn) -
         3.0 * s1 * s1 * s1 * s1 / (nn * nn * nn);
}

// ---- 滑窗极值 (van Herk/Gil–Werman 分块前后缀): 每行均摊 O(1) 产出窗内有效样本的**精确** min/max ----
//   spread 判据要求 lo/hi 与逐样本 fmin/fmax 完全一致, 本法给的就是精确极值 (不是近似).
//   块长 = 窗长 w; 进入新块时回读前一块 w 行建后缀极值 (读放大 2×), 前缀在线递推.
//   xm2 非空时有效样本 = xm && xm2 (二元算子的双有效窗). 无有效样本 → lo = +inf, hi = −inf (spread 恒假).
class RollMinMax {
public:
  RollMinMax(int w, int width) : w_(w), n_(width) {
    assert(w >= 1 && width >= 1);
    sl_.assign(static_cast<size_t>(w + 1) * width, kInf);
    sh_.assign(static_cast<size_t>(w + 1) * width, -kInf);
    pl_.assign(static_cast<size_t>(width), kInf);
    ph_.assign(static_cast<size_t>(width), -kInf);
    lo.assign(static_cast<size_t>(width), kInf);
    hi.assign(static_cast<size_t>(width), -kInf);
  }
  // 按 t 递增喂行 (xv/xm 为整平面, 行距 stride, 列 [a0, a0+width)); 返回后 lo/hi = 窗 [t−w+1, t] 极值
  void step(const float *xv, const uint8_t *xm, const uint8_t *xm2, int stride, int a0, int t) {
    const int pos = t % w_;
    if (pos == 0)
      rebuild(xv, xm, xm2, stride, a0, t);
    const size_t r = static_cast<size_t>(t) * stride + a0;
    const float *sl = sl_.data() + static_cast<size_t>(pos + 1) * n_;
    const float *sh = sh_.data() + static_cast<size_t>(pos + 1) * n_;
    for (int a = 0; a < n_; ++a) {
      const bool m = xm[r + a] && (!xm2 || xm2[r + a]);
      const float vl = m ? xv[r + a] : kInf, vh = m ? xv[r + a] : -kInf;
      pl_[a] = pos == 0 ? vl : std::fmin(pl_[a], vl);
      ph_[a] = pos == 0 ? vh : std::fmax(ph_[a], vh);
      lo[a] = std::fmin(sl[a], pl_[a]);
      hi[a] = std::fmax(sh[a], ph_[a]);
    }
  }
  std::vector<float> lo, hi;

private:
  void rebuild(const float *xv, const uint8_t *xm, const uint8_t *xm2, int stride, int a0, int t) {
    std::fill_n(sl_.begin() + static_cast<size_t>(w_) * n_, n_, kInf); // 下标 w = 空后缀哨兵
    std::fill_n(sh_.begin() + static_cast<size_t>(w_) * n_, n_, -kInf);
    for (int i = w_ - 1; i >= 0; --i) {
      float *cl = sl_.data() + static_cast<size_t>(i) * n_;
      float *ch = sh_.data() + static_cast<size_t>(i) * n_;
      const float *nl = cl + n_;
      const float *nh = ch + n_;
      const int row = t - w_ + i;
      if (row < 0) { // 平面开头: 空行
        std::copy_n(nl, n_, cl);
        std::copy_n(nh, n_, ch);
        continue;
      }
      const size_t r = static_cast<size_t>(row) * stride + a0;
      for (int a = 0; a < n_; ++a) {
        const bool m = xm[r + a] && (!xm2 || xm2[r + a]);
        cl[a] = std::fmin(m ? xv[r + a] : kInf, nl[a]);
        ch[a] = std::fmax(m ? xv[r + a] : -kInf, nh[a]);
      }
    }
  }
  int w_, n_;
  std::vector<float> sl_, sh_, pl_, ph_;
};

// ---- 滑窗极值 + 最旧并列位置 (契约: 严格比较 → 并列保留最旧) ----
//   后缀 (窗的旧半) 反向扫取 ≥/≤ (旧优先), 前缀正向扫取 >/< (新不夺旧), 合并时后缀平局优先.
template <bool IsMax>
class RollArgExt {
public:
  RollArgExt(int w, int width) : w_(w), n_(width) {
    assert(w >= 1 && width >= 1);
    sv_.assign(static_cast<size_t>(w + 1) * width, kSent);
    sj_.assign(static_cast<size_t>(w + 1) * width, -1);
    pv_.assign(static_cast<size_t>(width), kSent);
    pj_.assign(static_cast<size_t>(width), -1);
    best.assign(static_cast<size_t>(width), kSent);
    arg.assign(static_cast<size_t>(width), -1);
  }
  void step(const float *xv, const uint8_t *xm, int stride, int a0, int t) {
    const int pos = t % w_;
    if (pos == 0)
      rebuild(xv, xm, stride, a0, t);
    const size_t r = static_cast<size_t>(t) * stride + a0;
    const float *sv = sv_.data() + static_cast<size_t>(pos + 1) * n_;
    const int *sj = sj_.data() + static_cast<size_t>(pos + 1) * n_;
    for (int a = 0; a < n_; ++a) {
      const bool m = xm[r + a] != 0;
      const float x = xv[r + a];
      if (pos == 0) {
        pv_[a] = m ? x : kSent;
        pj_[a] = m ? t : -1;
      } else if (m && (IsMax ? x > pv_[a] : x < pv_[a])) {
        pv_[a] = x;
        pj_[a] = t;
      }
      const bool s_wins = IsMax ? sv[a] >= pv_[a] : sv[a] <= pv_[a];
      best[a] = s_wins ? sv[a] : pv_[a];
      arg[a] = s_wins ? sj[a] : pj_[a];
    }
  }
  std::vector<float> best; // 窗内极值 (无有效样本 → 哨兵, 由调用方的 n 屏蔽)
  std::vector<int> arg;    // 最旧极值的全局行号

private:
  static constexpr float kSent = IsMax ? -kInf : kInf;
  void rebuild(const float *xv, const uint8_t *xm, int stride, int a0, int t) {
    std::fill_n(sv_.begin() + static_cast<size_t>(w_) * n_, n_, kSent);
    std::fill_n(sj_.begin() + static_cast<size_t>(w_) * n_, n_, -1);
    for (int i = w_ - 1; i >= 0; --i) {
      float *cv = sv_.data() + static_cast<size_t>(i) * n_;
      int *cj = sj_.data() + static_cast<size_t>(i) * n_;
      const float *nv = cv + n_;
      const int *nj = cj + n_;
      const int row = t - w_ + i;
      if (row < 0) {
        std::copy_n(nv, n_, cv);
        std::copy_n(nj, n_, cj);
        continue;
      }
      const size_t r = static_cast<size_t>(row) * stride + a0;
      for (int a = 0; a < n_; ++a) {
        const bool take = xm[r + a] && (IsMax ? xv[r + a] >= nv[a] : xv[r + a] <= nv[a]);
        cv[a] = take ? xv[r + a] : nv[a];
        cj[a] = take ? row : nj[a];
      }
    }
  }
  int w_, n_;
  std::vector<float> sv_, pv_;
  std::vector<int> sj_, pj_;
};

// ---- 桶分位 (契约分桶规则): 全并列 (hi ≤ lo) → lo; 其余取首个前缀累计 ≥ ⌈q·n⌉ 的桶心 ----
inline float hist_quantile(const int *h, int n, float lo, float hi, double q) {
  if (!(hi > lo) || q <= 0.0)
    return lo;
  if (q >= 1.0)
    return hi;
  const int need = std::max(1, static_cast<int>(std::ceil(q * n)));
  int acc = 0;
  for (int b = 0; b < kBuckets; ++b) {
    acc += h[b];
    if (acc >= need)
      return bin_center(b, lo, hi);
  }
  return hi;
}

// ---- 一元矩核的掩码与求值 (EXPAND 与 ROLL 共用公式, 窗形只是状态推进方式不同) ----
enum class Mom { VAR,
                 STD,
                 SKEW,
                 KURT,
                 Z };

template <Mom K>
inline bool mom_ok(int n, bool sp) {
  if constexpr (K == Mom::SKEW)
    return n >= 3 && sp;
  else if constexpr (K == Mom::KURT)
    return n >= 4 && sp;
  else
    return n >= 2 && sp; // VAR / STD / Z
}

template <Mom K>
inline double mom_val(double s1, double s2, double s3, double s4, int n, float x) {
  (void)s3, (void)s4, (void)x;
  const double m2 = std::fmax(c2_of(s1, s2, n), 0.0);
  if constexpr (K == Mom::VAR)
    return m2 / (n - 1);
  else if constexpr (K == Mom::STD)
    return std::sqrt(m2 / (n - 1));
  else if constexpr (K == Mom::Z)
    return (x - s1 / n) / std::sqrt(m2 / (n - 1));
  else if constexpr (K == Mom::SKEW)
    return (c3_of(s1, s2, s3, n) / n) / std::pow(m2 / n, 1.5);
  else
    return (c4_of(s1, s2, s3, s4, n) / n) / ((m2 / n) * (m2 / n)) - 3.0;
}

// ---- 二元核的掩码与求值 ----
enum class Pair { COV,
                  CORR,
                  BETA,
                  RESID };

template <Pair K>
inline void pair_put(float *ov, uint8_t *om, size_t i, bool pre, int n, bool spx, bool spy, double sx,
                     double sy, double sxy, double sxx, double syy, float x, float y, bool mxy) {
  (void)spx, (void)spy, (void)sxx, (void)x, (void)y, (void)mxy;
  bool ok = pre && n >= 2;
  if constexpr (K == Pair::CORR)
    ok = ok && spx && spy;
  else if constexpr (K == Pair::BETA)
    ok = ok && spy;
  else if constexpr (K == Pair::RESID)
    ok = ok && spy && mxy;
  double v = 0.0;
  if (ok) {
    const double cxy = sxy - sx * sy / n;
    if constexpr (K == Pair::COV)
      v = cxy / (n - 1);
    else if constexpr (K == Pair::CORR)
      v = cxy / std::sqrt((sxx - sx * sx / n) * (syy - sy * sy / n));
    else if constexpr (K == Pair::BETA)
      v = cxy / (syy - sy * sy / n);
    else
      v = (x - sx / n) - cxy / (syy - sy * sy / n) * (y - sy / n);
  }
  put(ov, om, i, v, ok);
}

} // namespace detail

// -----------------------------------------------------------------------------
// 统一签名 (72 个算子同形; 元数不足时调用方传 nullptr, 故未用到的指针一律不许读)
// -----------------------------------------------------------------------------
#define CP_SIG                                                                              \
  static void run(const float *xv, const uint8_t *xm, const float *yv, const uint8_t *ym,   \
                  const float *zv, const uint8_t *zm, float *ov, uint8_t *om, int T, int A, \
                  const Param &p)

#define CP_NO23 (void)yv, (void)ym, (void)zv, (void)zm
#define CP_NO3 (void)zv, (void)zm

// =============================================================================
// POINT (22): 逐点无状态, 单遍. 表达式与流式严格同式 (三后端要位级一致).
// =============================================================================
#define CP_P1(Name, V, M)                                            \
  struct Name {                                                      \
    CP_SIG {                                                         \
      CP_NO23;                                                       \
      (void)p;                                                       \
      const size_t N = static_cast<size_t>(T) * A;                   \
      for (size_t i = 0; i < N; ++i) {                               \
        const float x = xv[i];                                       \
        const bool mx = xm[i] != 0;                                  \
        const bool m = (M);                                          \
        detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m); \
      }                                                              \
    }                                                                \
  };

#define CP_P2(Name, V, M)                                            \
  struct Name {                                                      \
    CP_SIG {                                                         \
      CP_NO3;                                                        \
      (void)p;                                                       \
      const size_t N = static_cast<size_t>(T) * A;                   \
      for (size_t i = 0; i < N; ++i) {                               \
        const float x = xv[i], y = yv[i];                            \
        const bool mx = xm[i] != 0, my = ym[i] != 0;                 \
        const bool m = (M);                                          \
        detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m); \
      }                                                              \
    }                                                                \
  };

#define CP_P3(Name, V, M)                                             \
  struct Name {                                                       \
    CP_SIG {                                                          \
      (void)p;                                                        \
      const size_t N = static_cast<size_t>(T) * A;                    \
      for (size_t i = 0; i < N; ++i) {                                \
        const float x = xv[i], y = yv[i], z = zv[i];                  \
        const bool mx = xm[i] != 0, my = ym[i] != 0, mz = zm[i] != 0; \
        const bool m = (M);                                           \
        detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m);  \
      }                                                               \
    }                                                                 \
  };

CP_P1(TsAbs, std::fabs(x), mx)
CP_P1(TsSign, x > 0.f ? 1.f : (x < 0.f ? -1.f : 0.f), mx)
CP_P1(TsLog, std::copysign(std::log1p(std::fabs(x)), x), mx)
CP_P1(TsAsinh, std::asinh(x), mx)
CP_P1(TsTanh, std::tanh(x), mx)
CP_P1(TsSqrt, std::copysign(std::sqrt(std::fabs(x)), x), mx)
CP_P1(TsRelu, std::fmax(0.f, x), mx)
CP_P1(TsRecip, 1.f / x, mx &&x != 0.f) // x = 0 退化, 溢出由 mk 转无效
CP_P1(TsSignedPow, std::copysign(std::pow(std::fabs(x), p.k), x), mx)

// TsClip: clamp 要求 lo ≤ hi, 即 k ≥ 0 —— 参数级约束, 早死在 assert 上
struct TsClip {
  CP_SIG {
    CP_NO23;
    assert(p.k >= 0.f);
    const size_t N = static_cast<size_t>(T) * A;
    for (size_t i = 0; i < N; ++i) {
      const bool m = xm[i] != 0;
      detail::put(ov, om, i, m ? static_cast<double>(std::clamp(xv[i], -p.k, p.k)) : 0.0, m);
    }
  }
};

// TsTodMask: 元数 0, 完全不读输入指针 (调用方传 nullptr)
struct TsTodMask {
  CP_SIG {
    (void)xv, (void)xm;
    CP_NO23;
    for (int t = 0; t < T; ++t) {
      const int t_seg = t % kSegLen;
      const float v = (t_seg >= static_cast<int>(p.k) && t_seg < static_cast<int>(p.k2)) ? 1.f : 0.f;
      const size_t r = static_cast<size_t>(t) * A;
      for (int a = 0; a < A; ++a)
        detail::put(ov, om, r + a, static_cast<double>(v), true);
    }
  }
};

CP_P2(TsAdd, x + y, mx &&my)
CP_P2(TsSub, x - y, mx &&my)
CP_P2(TsMul, x *y, mx &&my)
CP_P2(TsDiv, x / y, mx && my && y != 0.f) // y = 0 退化, 溢出由 mk 转无效
CP_P2(TsMax, std::fmax(x, y), mx &&my)
CP_P2(TsMin, std::fmin(x, y), mx &&my)
// TsImb / TsShare: 和相消退化 (scale = |x| + |y|), 分母不钳位
CP_P2(TsImb, (x - y) / (x + y), mx && my && den_ok(static_cast<double>(x) + y, std::fabs(x) + std::fabs(y)))
CP_P2(TsShare, x / (x + y), mx && my && den_ok(static_cast<double>(x) + y, std::fabs(x) + std::fabs(y)))
CP_P2(TsLogRatio, std::log(x) - std::log(y), mx && my && x > 0.f && y > 0.f)
// TsWhere: 未被选中的那支不要求有效
CP_P3(TsWhere, x > 0.f ? y : z, mx && (x > 0.f ? my : mz))
// TsClip3: y > z 退化; 掩码含 y ≤ z, 故 clamp 只在前置条件成立时求值
CP_P3(TsClip3, std::clamp(x, y, z), mx && my && mz && (y <= z))

// =============================================================================
// EXPAND (23): 沿 t 递推, 段界 reset. 状态 = 跨资产数组.
// =============================================================================

// ---- Sum / Mean ----
template <bool Mean>
struct ExpSum {
  CP_SIG {
    CP_NO23;
    (void)p;
    std::vector<int> n(static_cast<size_t>(A));
    std::vector<double> s(static_cast<size_t>(A));
    for (int seg = 0; seg < T; seg += kSegLen) {
      std::fill(n.begin(), n.end(), 0);
      std::fill(s.begin(), s.end(), 0.0);
      const int e = std::min(seg + kSegLen, T);
      for (int t = seg; t < e; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        for (int a = 0; a < A; ++a) {
          const bool m = xm[r + a] != 0;
          s[a] += m ? static_cast<double>(xv[r + a]) : 0.0;
          n[a] += m;
          const bool ok = n[a] >= 1;
          detail::put(ov, om, r + a, ok ? (Mean ? s[a] / n[a] : s[a]) : 0.0, ok);
        }
      }
    }
  }
};

// ---- Var / Std / Skew / Kurt: 幂和递推, lo/hi 段内只扩不缩 (精确 spread) ----
template <detail::Mom K>
struct ExpMom {
  static constexpr bool kS3 = K == detail::Mom::SKEW || K == detail::Mom::KURT;
  static constexpr bool kS4 = K == detail::Mom::KURT;
  CP_SIG {
    CP_NO23;
    (void)p;
    const size_t W = static_cast<size_t>(A);
    std::vector<int> n(W);
    std::vector<double> s1(W), s2(W), s3(kS3 ? W : 0), s4(kS4 ? W : 0);
    std::vector<float> lo(W), hi(W);
    for (int seg = 0; seg < T; seg += kSegLen) {
      std::fill(n.begin(), n.end(), 0);
      std::fill(s1.begin(), s1.end(), 0.0);
      std::fill(s2.begin(), s2.end(), 0.0);
      if constexpr (kS3)
        std::fill(s3.begin(), s3.end(), 0.0);
      if constexpr (kS4)
        std::fill(s4.begin(), s4.end(), 0.0);
      std::fill(lo.begin(), lo.end(), detail::kInf);
      std::fill(hi.begin(), hi.end(), -detail::kInf);
      const int e = std::min(seg + kSegLen, T);
      for (int t = seg; t < e; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        for (int a = 0; a < A; ++a) {
          const float x = xv[r + a];
          const bool m = xm[r + a] != 0;
          const double dx = m ? static_cast<double>(x) : 0.0;
          const double q = dx * dx;
          s1[a] += dx;
          s2[a] += q;
          if constexpr (kS3)
            s3[a] += q * dx;
          if constexpr (kS4)
            s4[a] += q * q;
          n[a] += m;
          lo[a] = m ? std::fmin(lo[a], x) : lo[a];
          hi[a] = m ? std::fmax(hi[a], x) : hi[a];
          const bool ok = detail::mom_ok<K>(n[a], hi[a] > lo[a]);
          detail::put(ov, om, r + a,
                      ok ? detail::mom_val<K>(s1[a], s2[a], kS3 ? s3[a] : 0.0, kS4 ? s4[a] : 0.0,
                                              n[a], x)
                         : 0.0,
                      ok);
        }
      }
    }
  }
};

// ---- Max / Min / ArgMax / ArgMin: 递推极值 (严格比较 → 并列保留最旧) ----
template <bool IsMax, bool IsArg>
struct ExpExt {
  CP_SIG {
    CP_NO23;
    (void)p;
    std::vector<int> n(static_cast<size_t>(A)), bi(static_cast<size_t>(A));
    std::vector<float> best(static_cast<size_t>(A));
    for (int seg = 0; seg < T; seg += kSegLen) {
      std::fill(n.begin(), n.end(), 0);
      std::fill(bi.begin(), bi.end(), 0);
      std::fill(best.begin(), best.end(), 0.f);
      const int e = std::min(seg + kSegLen, T);
      for (int t = seg; t < e; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        const int ts = t - seg;
        for (int a = 0; a < A; ++a) {
          const float x = xv[r + a];
          const bool m = xm[r + a] != 0;
          const bool c = m && (n[a] == 0 || (IsMax ? x > best[a] : x < best[a]));
          best[a] = c ? x : best[a];
          bi[a] = c ? ts : bi[a];
          n[a] += m;
          const bool ok = n[a] >= 1;
          const double v = IsArg ? static_cast<double>(ts - bi[a]) : static_cast<double>(best[a]);
          detail::put(ov, om, r + a, ok ? v : 0.0, ok);
        }
      }
    }
  }
};

// ---- RankCum: 增量直方图 (段内 lo/hi 只扩不缩; 扩时用段内样本缓存重建) ----
struct TsRankCum {
  CP_SIG {
    CP_NO23;
    (void)p;
    std::vector<float> buf, lo, hi;
    std::vector<int> hist, cnt;
    for (int a0 = 0; a0 < A; a0 += detail::kBlockA) {
      const int W = std::min(detail::kBlockA, A - a0);
      buf.resize(static_cast<size_t>(W) * kSegLen);
      hist.assign(static_cast<size_t>(W) * kBuckets, 0);
      cnt.assign(static_cast<size_t>(W), 0);
      lo.assign(static_cast<size_t>(W), detail::kInf);
      hi.assign(static_cast<size_t>(W), -detail::kInf);
      for (int seg = 0; seg < T; seg += kSegLen) {
        std::fill(cnt.begin(), cnt.end(), 0); // hist 不清: 重建时清
        std::fill(lo.begin(), lo.end(), detail::kInf);
        std::fill(hi.begin(), hi.end(), -detail::kInf);
        const int e = std::min(seg + kSegLen, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A + a0;
          for (int j = 0; j < W; ++j) {
            const float x = xv[r + j];
            const bool m = xm[r + j] != 0;
            int *h = hist.data() + static_cast<size_t>(j) * kBuckets;
            if (m) {
              buf[static_cast<size_t>(j) * kSegLen + cnt[j]] = x;
              ++cnt[j];
              const float pl = lo[j], ph = hi[j];
              const float nl = std::fmin(pl, x), nh = std::fmax(ph, x);
              lo[j] = nl;
              hi[j] = nh;
              if (nh > nl) {
                if (nl != pl || nh != ph) { // 值域扩了 (含首次进入 spread): 重建
                  std::fill(h, h + kBuckets, 0);
                  const float *b = buf.data() + static_cast<size_t>(j) * kSegLen;
                  for (int s = 0; s < cnt[j]; ++s)
                    ++h[bin_of(b[s], nl, nh)];
                } else {
                  ++h[bin_of(x, nl, nh)];
                }
              }
            }
            const bool ok = cnt[j] >= 1 && m; // 相对型: x_t 无效则无效
            double v = 0.0;
            if (ok) {
              if (!(hi[j] > lo[j]))
                v = 0.5;
              else {
                const int b = bin_of(x, lo[j], hi[j]);
                int less = 0;
                for (int q = 0; q < b; ++q)
                  less += h[q];
                v = pct_of(less, h[b], cnt[j]);
              }
            }
            detail::put(ov, om, r + j, v, ok);
          }
        }
      }
    }
  }
};

// ---- HhiCum ----
struct TsHhiCum {
  CP_SIG {
    CP_NO23;
    (void)p;
    std::vector<int> n(static_cast<size_t>(A));
    std::vector<double> s(static_cast<size_t>(A)), s2(static_cast<size_t>(A));
    for (int seg = 0; seg < T; seg += kSegLen) {
      std::fill(n.begin(), n.end(), 0);
      std::fill(s.begin(), s.end(), 0.0);
      std::fill(s2.begin(), s2.end(), 0.0);
      const int e = std::min(seg + kSegLen, T);
      for (int t = seg; t < e; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        for (int a = 0; a < A; ++a) {
          const float x = xv[r + a];
          const bool m = xm[r + a] != 0;
          s[a] += m ? static_cast<double>(x) : 0.0;
          s2[a] += m ? static_cast<double>(x) * x : 0.0;
          n[a] += m;
          const bool ok = n[a] >= 1 && den_ok(s[a] * s[a], n[a] * s2[a]);
          detail::put(ov, om, r + a, ok ? s2[a] / (s[a] * s[a]) : 0.0, ok);
        }
      }
    }
  }
};

// ---- EntropyCum: 只计 x > 0 的样本 ----
struct TsEntropyCum {
  CP_SIG {
    CP_NO23;
    (void)p;
    std::vector<int> np(static_cast<size_t>(A));
    std::vector<double> s(static_cast<size_t>(A)), sxl(static_cast<size_t>(A));
    for (int seg = 0; seg < T; seg += kSegLen) {
      std::fill(np.begin(), np.end(), 0);
      std::fill(s.begin(), s.end(), 0.0);
      std::fill(sxl.begin(), sxl.end(), 0.0);
      const int e = std::min(seg + kSegLen, T);
      for (int t = seg; t < e; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        for (int a = 0; a < A; ++a) {
          const float x = xv[r + a];
          const bool pos = xm[r + a] && x > 0.f;
          s[a] += pos ? static_cast<double>(x) : 0.0;
          sxl[a] += pos ? static_cast<double>(x) * std::log(static_cast<double>(x)) : 0.0;
          np[a] += pos;
          const bool ok = np[a] >= 1;
          detail::put(ov, om, r + a, ok ? std::log(s[a]) - sxl[a] / s[a] : 0.0, ok);
        }
      }
    }
  }
};

// ---- TopKCum: 增量直方图 + 幂和 (输出从最高桶向下取 K 个) ----
struct TsTopKCum {
  CP_SIG {
    CP_NO23;
    const int K = static_cast<int>(p.k);
    std::vector<float> buf, lo, hi;
    std::vector<int> hist, cnt;
    std::vector<double> s, sabs;
    for (int a0 = 0; a0 < A; a0 += detail::kBlockA) {
      const int W = std::min(detail::kBlockA, A - a0);
      buf.resize(static_cast<size_t>(W) * kSegLen);
      hist.assign(static_cast<size_t>(W) * kBuckets, 0);
      cnt.assign(static_cast<size_t>(W), 0);
      lo.assign(static_cast<size_t>(W), detail::kInf);
      hi.assign(static_cast<size_t>(W), -detail::kInf);
      s.assign(static_cast<size_t>(W), 0.0);
      sabs.assign(static_cast<size_t>(W), 0.0);
      for (int seg = 0; seg < T; seg += kSegLen) {
        std::fill(cnt.begin(), cnt.end(), 0);
        std::fill(lo.begin(), lo.end(), detail::kInf);
        std::fill(hi.begin(), hi.end(), -detail::kInf);
        std::fill(s.begin(), s.end(), 0.0);
        std::fill(sabs.begin(), sabs.end(), 0.0);
        const int e = std::min(seg + kSegLen, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A + a0;
          for (int j = 0; j < W; ++j) {
            const float x = xv[r + j];
            const bool m = xm[r + j] != 0;
            int *h = hist.data() + static_cast<size_t>(j) * kBuckets;
            if (m) {
              buf[static_cast<size_t>(j) * kSegLen + cnt[j]] = x;
              ++cnt[j];
              s[j] += x;
              sabs[j] += std::fabs(x);
              const float pl = lo[j], ph = hi[j];
              const float nl = std::fmin(pl, x), nh = std::fmax(ph, x);
              lo[j] = nl;
              hi[j] = nh;
              if (nh > nl) {
                if (nl != pl || nh != ph) {
                  std::fill(h, h + kBuckets, 0);
                  const float *b = buf.data() + static_cast<size_t>(j) * kSegLen;
                  for (int q = 0; q < cnt[j]; ++q)
                    ++h[bin_of(b[q], nl, nh)];
                } else {
                  ++h[bin_of(x, nl, nh)];
                }
              }
            }
            const bool ok = cnt[j] >= K && K >= 1 && den_ok(s[j], sabs[j]);
            double v = 0.0;
            if (ok) {
              if (!(hi[j] > lo[j]))
                v = cnt[j] >= 1 ? static_cast<double>(std::min(K, cnt[j])) / cnt[j] : 0.0;
              else {
                double top = 0.0;
                int taken = 0;
                for (int b = kBuckets - 1; b >= 0 && taken < K; --b) {
                  const int take = std::min(h[b], K - taken);
                  top += static_cast<double>(bin_center(b, lo[j], hi[j])) * take;
                  taken += take;
                }
                v = top / s[j];
              }
            }
            detail::put(ov, om, r + j, v, ok);
          }
        }
      }
    }
  }
};

// ---- GiniCum: 只计 x > 0 的样本 (正样本独立定桶); 输出两遍桶扫 (总值 + Lorenz) ----
struct TsGiniCum {
  CP_SIG {
    CP_NO23;
    (void)p;
    std::vector<float> buf, lo, hi;
    std::vector<int> hist, np;
    for (int a0 = 0; a0 < A; a0 += detail::kBlockA) {
      const int W = std::min(detail::kBlockA, A - a0);
      buf.resize(static_cast<size_t>(W) * kSegLen);
      hist.assign(static_cast<size_t>(W) * kBuckets, 0);
      np.assign(static_cast<size_t>(W), 0);
      lo.assign(static_cast<size_t>(W), detail::kInf);
      hi.assign(static_cast<size_t>(W), -detail::kInf);
      for (int seg = 0; seg < T; seg += kSegLen) {
        std::fill(np.begin(), np.end(), 0);
        std::fill(lo.begin(), lo.end(), detail::kInf);
        std::fill(hi.begin(), hi.end(), -detail::kInf);
        const int e = std::min(seg + kSegLen, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A + a0;
          for (int j = 0; j < W; ++j) {
            const float x = xv[r + j];
            const bool pos = xm[r + j] && x > 0.f;
            int *h = hist.data() + static_cast<size_t>(j) * kBuckets;
            if (pos) {
              buf[static_cast<size_t>(j) * kSegLen + np[j]] = x;
              ++np[j];
              const float pl = lo[j], ph = hi[j];
              const float nl = std::fmin(pl, x), nh = std::fmax(ph, x);
              lo[j] = nl;
              hi[j] = nh;
              if (nh > nl) {
                if (nl != pl || nh != ph) {
                  std::fill(h, h + kBuckets, 0);
                  const float *b = buf.data() + static_cast<size_t>(j) * kSegLen;
                  for (int q = 0; q < np[j]; ++q)
                    ++h[bin_of(b[q], nl, nh)];
                } else {
                  ++h[bin_of(x, nl, nh)];
                }
              }
            }
            const bool ok = np[j] >= 2;
            double v = 0.0;
            if (ok && hi[j] > lo[j]) {
              double tot = 0.0;
              for (int b = 0; b < kBuckets; ++b)
                tot += static_cast<double>(bin_center(b, lo[j], hi[j])) * h[b];
              double cp = 0.0, cl = 0.0, g = 1.0;
              for (int b = 0; b < kBuckets; ++b) {
                if (h[b] == 0)
                  continue;
                const double pp = cp + static_cast<double>(h[b]) / np[j];
                const double ll = cl + static_cast<double>(bin_center(b, lo[j], hi[j])) * h[b] / tot;
                g -= (pp - cp) * (ll + cl);
                cp = pp;
                cl = ll;
              }
              v = g;
            }
            detail::put(ov, om, r + j, v, ok);
          }
        }
      }
    }
  }
};

// ---- CountGtCum ----
struct TsCountGtCum {
  CP_SIG {
    CP_NO23;
    std::vector<int> n(static_cast<size_t>(A)), c(static_cast<size_t>(A));
    for (int seg = 0; seg < T; seg += kSegLen) {
      std::fill(n.begin(), n.end(), 0);
      std::fill(c.begin(), c.end(), 0);
      const int e = std::min(seg + kSegLen, T);
      for (int t = seg; t < e; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        for (int a = 0; a < A; ++a) {
          const bool m = xm[r + a] != 0;
          c[a] += m && (xv[r + a] > p.k);
          n[a] += m;
          const bool ok = n[a] >= 1;
          detail::put(ov, om, r + a, ok ? static_cast<double>(c[a]) : 0.0, ok);
        }
      }
    }
  }
};

// ---- PeaksCum: 峰在 s 处成立需三点相邻有效且 x_s > k·mean_{≤s}; 在 s+1 时刻确认计入 ----
struct TsPeaksCum {
  CP_SIG {
    CP_NO23;
    const size_t W = static_cast<size_t>(A);
    std::vector<int> n(W), c(W);
    std::vector<double> s(W), mean1(W);
    std::vector<float> p2v(W), p1v(W);
    std::vector<uint8_t> p2m(W), p1m(W);
    for (int seg = 0; seg < T; seg += kSegLen) {
      std::fill(n.begin(), n.end(), 0);
      std::fill(c.begin(), c.end(), 0);
      std::fill(s.begin(), s.end(), 0.0);
      std::fill(mean1.begin(), mean1.end(), 0.0);
      std::fill(p2v.begin(), p2v.end(), 0.f);
      std::fill(p1v.begin(), p1v.end(), 0.f);
      std::fill(p2m.begin(), p2m.end(), 0);
      std::fill(p1m.begin(), p1m.end(), 0);
      const int e = std::min(seg + kSegLen, T);
      for (int t = seg; t < e; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        for (int a = 0; a < A; ++a) {
          const float x = xv[r + a];
          const bool m = xm[r + a] != 0;
          s[a] += m ? static_cast<double>(x) : 0.0;
          n[a] += m;
          const double mc = n[a] >= 1 ? s[a] / n[a] : 0.0;
          c[a] += (p2m[a] && p1m[a] && m && p2v[a] < p1v[a] && p1v[a] > x &&
                   p1v[a] > static_cast<double>(p.k) * mean1[a]);
          detail::put(ov, om, r + a, static_cast<double>(c[a]), n[a] >= 1);
          p2v[a] = p1v[a];
          p2m[a] = p1m[a];
          p1v[a] = x;
          p1m[a] = m ? 1 : 0;
          mean1[a] = mc;
        }
      }
    }
  }
};

// ---- Cov / Corr / Beta / Resid (Cum): 二元幂和递推 + 双有效样本的段内极值 ----
template <detail::Pair K>
struct ExpPair {
  static constexpr bool kSpx = K == detail::Pair::CORR;
  static constexpr bool kSpy = K != detail::Pair::COV;
  CP_SIG {
    CP_NO3;
    (void)p;
    const size_t W = static_cast<size_t>(A);
    std::vector<int> n(W);
    std::vector<double> sx(W), sy(W), sxy(W), sxx(kSpx ? W : 0), syy(kSpy ? W : 0);
    std::vector<float> lox(kSpx ? W : 0), hix(kSpx ? W : 0), loy(kSpy ? W : 0), hiy(kSpy ? W : 0);
    for (int seg = 0; seg < T; seg += kSegLen) {
      std::fill(n.begin(), n.end(), 0);
      std::fill(sx.begin(), sx.end(), 0.0);
      std::fill(sy.begin(), sy.end(), 0.0);
      std::fill(sxy.begin(), sxy.end(), 0.0);
      if constexpr (kSpx) {
        std::fill(sxx.begin(), sxx.end(), 0.0);
        std::fill(lox.begin(), lox.end(), detail::kInf);
        std::fill(hix.begin(), hix.end(), -detail::kInf);
      }
      if constexpr (kSpy) {
        std::fill(syy.begin(), syy.end(), 0.0);
        std::fill(loy.begin(), loy.end(), detail::kInf);
        std::fill(hiy.begin(), hiy.end(), -detail::kInf);
      }
      const int e = std::min(seg + kSegLen, T);
      for (int t = seg; t < e; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        for (int a = 0; a < A; ++a) {
          const float x = xv[r + a], y = yv[r + a];
          const bool mm = xm[r + a] && ym[r + a];
          const double dx = mm ? static_cast<double>(x) : 0.0;
          const double dy = mm ? static_cast<double>(y) : 0.0;
          sx[a] += dx;
          sy[a] += dy;
          sxy[a] += dx * dy;
          if constexpr (kSpx) {
            sxx[a] += dx * dx;
            lox[a] = mm ? std::fmin(lox[a], x) : lox[a];
            hix[a] = mm ? std::fmax(hix[a], x) : hix[a];
          }
          if constexpr (kSpy) {
            syy[a] += dy * dy;
            loy[a] = mm ? std::fmin(loy[a], y) : loy[a];
            hiy[a] = mm ? std::fmax(hiy[a], y) : hiy[a];
          }
          n[a] += mm;
          detail::pair_put<K>(ov, om, r + a, true, n[a], kSpx ? hix[a] > lox[a] : true,
                              kSpy ? hiy[a] > loy[a] : true, sx[a], sy[a], sxy[a],
                              kSpx ? sxx[a] : 0.0, kSpy ? syy[a] : 0.0, x, y, mm);
        }
      }
    }
  }
};

// ---- WMeanCum: y 为权 ----
struct TsWMeanCum {
  CP_SIG {
    CP_NO3;
    (void)p;
    const size_t W = static_cast<size_t>(A);
    std::vector<int> n(W);
    std::vector<double> sy(W), syx(W), say(W);
    for (int seg = 0; seg < T; seg += kSegLen) {
      std::fill(n.begin(), n.end(), 0);
      std::fill(sy.begin(), sy.end(), 0.0);
      std::fill(syx.begin(), syx.end(), 0.0);
      std::fill(say.begin(), say.end(), 0.0);
      const int e = std::min(seg + kSegLen, T);
      for (int t = seg; t < e; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        for (int a = 0; a < A; ++a) {
          const float x = xv[r + a], y = yv[r + a];
          const bool mm = xm[r + a] && ym[r + a];
          sy[a] += mm ? static_cast<double>(y) : 0.0;
          syx[a] += mm ? static_cast<double>(y) * x : 0.0;
          say[a] += mm ? static_cast<double>(std::fabs(y)) : 0.0;
          n[a] += mm;
          const bool ok = n[a] >= 1 && den_ok(sy[a], say[a]);
          detail::put(ov, om, r + a, ok ? syx[a] / sy[a] : 0.0, ok);
        }
      }
    }
  }
};

// ---- CorrLagCum: 样本对 (x_s, y_{s−K}), 两值都要在本段内且有效; 逐 t 增量配对 ----
struct TsCorrLagCum {
  CP_SIG {
    CP_NO3;
    const int K = static_cast<int>(p.k);
    assert(K >= 0);
    const size_t W = static_cast<size_t>(A);
    std::vector<int> n(W);
    std::vector<double> sx(W), sy(W), sxy(W), sxx(W), syy(W);
    std::vector<float> lox(W), hix(W), loy(W), hiy(W);
    for (int seg = 0; seg < T; seg += kSegLen) {
      std::fill(n.begin(), n.end(), 0);
      std::fill(sx.begin(), sx.end(), 0.0);
      std::fill(sy.begin(), sy.end(), 0.0);
      std::fill(sxy.begin(), sxy.end(), 0.0);
      std::fill(sxx.begin(), sxx.end(), 0.0);
      std::fill(syy.begin(), syy.end(), 0.0);
      std::fill(lox.begin(), lox.end(), detail::kInf);
      std::fill(hix.begin(), hix.end(), -detail::kInf);
      std::fill(loy.begin(), loy.end(), detail::kInf);
      std::fill(hiy.begin(), hiy.end(), -detail::kInf);
      const int e = std::min(seg + kSegLen, T);
      for (int t = seg; t < e; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        if (t - K >= seg) {
          const size_t rl = static_cast<size_t>(t - K) * A;
          for (int a = 0; a < A; ++a) {
            const float x = xv[r + a], y = yv[rl + a];
            const bool mm = xm[r + a] && ym[rl + a];
            const double dx = mm ? static_cast<double>(x) : 0.0;
            const double dy = mm ? static_cast<double>(y) : 0.0;
            sx[a] += dx;
            sy[a] += dy;
            sxy[a] += dx * dy;
            sxx[a] += dx * dx;
            syy[a] += dy * dy;
            lox[a] = mm ? std::fmin(lox[a], x) : lox[a];
            hix[a] = mm ? std::fmax(hix[a], x) : hix[a];
            loy[a] = mm ? std::fmin(loy[a], y) : loy[a];
            hiy[a] = mm ? std::fmax(hiy[a], y) : hiy[a];
            n[a] += mm;
          }
        }
        for (int a = 0; a < A; ++a) {
          const bool ok = n[a] >= 2 && hix[a] > lox[a] && hiy[a] > loy[a];
          double v = 0.0;
          if (ok) {
            const double cxy = sxy[a] - sx[a] * sy[a] / n[a];
            v = cxy / std::sqrt((sxx[a] - sx[a] * sx[a] / n[a]) * (syy[a] - sy[a] * sy[a] / n[a]));
          }
          detail::put(ov, om, r + a, v, ok);
        }
      }
    }
  }
};

// TsArgMaxCum / TsArgMinCum / TsMaxCum / TsMinCum 由 ExpExt 实例化 (见文件末尾)

// =============================================================================
// ROLL (26): 滑动加减 + van Herk 极值; 窗未满 (t < d−1) 一律无效
// =============================================================================

// ---- Delay / Delta: 纯移位取值, 窗实际跨 d+1 格 (t < d 无效) ----
struct TsDelayRoll {
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const size_t N = static_cast<size_t>(T) * A;
    const size_t off = static_cast<size_t>(p.d) * A;
    const size_t head = std::min(off, N);
    for (size_t i = 0; i < head; ++i)
      detail::put(ov, om, i, 0.0, false);
    for (size_t i = head; i < N; ++i) {
      const bool m = xm[i - off] != 0;
      detail::put(ov, om, i, m ? static_cast<double>(xv[i - off]) : 0.0, m);
    }
  }
};

struct TsDeltaRoll {
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const size_t N = static_cast<size_t>(T) * A;
    const size_t off = static_cast<size_t>(p.d) * A;
    const size_t head = std::min(off, N);
    for (size_t i = 0; i < head; ++i)
      detail::put(ov, om, i, 0.0, false);
    for (size_t i = head; i < N; ++i) {
      const bool m = xm[i] && xm[i - off];
      detail::put(ov, om, i, m ? static_cast<double>(xv[i]) - xv[i - off] : 0.0, m);
    }
  }
};

// ---- Sum / Mean (Roll): 滑动加减 ----
template <bool Mean>
struct RollSum {
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const int d = p.d;
    std::vector<int> n(static_cast<size_t>(A), 0);
    std::vector<double> s(static_cast<size_t>(A), 0.0);
    for (int t = 0; t < T; ++t) {
      if (t >= d) {
        const size_t q = static_cast<size_t>(t - d) * A;
        for (int a = 0; a < A; ++a) {
          const bool m = xm[q + a] != 0;
          s[a] -= m ? static_cast<double>(xv[q + a]) : 0.0;
          n[a] -= m;
        }
      }
      const size_t r = static_cast<size_t>(t) * A;
      const bool full = t >= d - 1;
      for (int a = 0; a < A; ++a) {
        const bool m = xm[r + a] != 0;
        s[a] += m ? static_cast<double>(xv[r + a]) : 0.0;
        n[a] += m;
        const bool ok = full && n[a] >= 1;
        detail::put(ov, om, r + a, ok ? (Mean ? s[a] / n[a] : s[a]) : 0.0, ok);
      }
    }
  }
};

// ---- Var / Std / Skew / Kurt / Z (Roll): 滑动加减幂和 + van Herk 精确 lo/hi ----
template <detail::Mom K>
struct RollMom {
  static constexpr bool kS3 = K == detail::Mom::SKEW || K == detail::Mom::KURT;
  static constexpr bool kS4 = K == detail::Mom::KURT;
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const int d = p.d;
    const size_t W = static_cast<size_t>(A);
    std::vector<int> n(W, 0);
    std::vector<double> s1(W, 0.0), s2(W, 0.0), s3(kS3 ? W : 0, 0.0), s4(kS4 ? W : 0, 0.0);
    detail::RollMinMax mm(d, A);
    for (int t = 0; t < T; ++t) {
      mm.step(xv, xm, nullptr, A, 0, t);
      if (t >= d) {
        const size_t q0 = static_cast<size_t>(t - d) * A;
        for (int a = 0; a < A; ++a) {
          const bool m = xm[q0 + a] != 0;
          const double dx = m ? static_cast<double>(xv[q0 + a]) : 0.0;
          const double q = dx * dx;
          s1[a] -= dx;
          s2[a] -= q;
          if constexpr (kS3)
            s3[a] -= q * dx;
          if constexpr (kS4)
            s4[a] -= q * q;
          n[a] -= m;
        }
      }
      const size_t r = static_cast<size_t>(t) * A;
      const bool full = t >= d - 1;
      for (int a = 0; a < A; ++a) {
        const float x = xv[r + a];
        const bool m = xm[r + a] != 0;
        const double dx = m ? static_cast<double>(x) : 0.0;
        const double q = dx * dx;
        s1[a] += dx;
        s2[a] += q;
        if constexpr (kS3)
          s3[a] += q * dx;
        if constexpr (kS4)
          s4[a] += q * q;
        n[a] += m;
        bool ok = full && detail::mom_ok<K>(n[a], mm.hi[a] > mm.lo[a]);
        if constexpr (K == detail::Mom::Z)
          ok = ok && m; // 相对型: x_t 无效则无效
        detail::put(ov, om, r + a,
                    ok ? detail::mom_val<K>(s1[a], s2[a], kS3 ? s3[a] : 0.0, kS4 ? s4[a] : 0.0,
                                            n[a], x)
                       : 0.0,
                    ok);
      }
    }
  }
};

// ---- Max / Min / ArgMax / ArgMin (Roll): van Herk 极值 + 最旧并列位置 ----
template <bool IsMax, bool IsArg>
struct RollExt {
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const int d = p.d;
    detail::RollArgExt<IsMax> mm(d, A);
    std::vector<int> n(static_cast<size_t>(A), 0);
    for (int t = 0; t < T; ++t) {
      mm.step(xv, xm, A, 0, t);
      if (t >= d) {
        const size_t q = static_cast<size_t>(t - d) * A;
        for (int a = 0; a < A; ++a)
          n[a] -= xm[q + a] != 0;
      }
      const size_t r = static_cast<size_t>(t) * A;
      const bool full = t >= d - 1;
      for (int a = 0; a < A; ++a) {
        n[a] += xm[r + a] != 0;
        const bool ok = full && n[a] >= 1;
        const double v =
            IsArg ? static_cast<double>(t - mm.arg[a]) : static_cast<double>(mm.best[a]);
        detail::put(ov, om, r + a, ok ? v : 0.0, ok);
      }
    }
  }
};

// ---- Rank / Median / Mad (Roll): 环形窗缓存 + 直方图 (lo/hi 未变增量改桶, 变了整窗重建) ----
enum class HistK { RANK,
                   MEDIAN,
                   MAD };

template <HistK K>
struct RollHist {
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const int d = p.d;
    std::vector<float> rv, plo, phi;
    std::vector<uint8_t> rm;
    std::vector<int> hist, n;
    std::vector<float> ad(K == HistK::MAD ? static_cast<size_t>(d) : 0); // MAD: |x−med| 暂存
    for (int a0 = 0; a0 < A; a0 += detail::kBlockA) {
      const int W = std::min(detail::kBlockA, A - a0);
      detail::RollMinMax mm(d, W);
      rv.assign(static_cast<size_t>(d) * W, 0.f);
      rm.assign(static_cast<size_t>(d) * W, 0);
      hist.assign(static_cast<size_t>(W) * kBuckets, 0);
      n.assign(static_cast<size_t>(W), 0);
      plo.assign(static_cast<size_t>(W), detail::kInf);
      phi.assign(static_cast<size_t>(W), -detail::kInf);
      for (int t = 0; t < T; ++t) {
        mm.step(xv, xm, nullptr, A, a0, t);
        const int slot = t % d;
        const size_t r = static_cast<size_t>(t) * A + a0;
        const bool full = t >= d - 1;
        for (int j = 0; j < W; ++j) {
          const float x = xv[r + j];
          const bool m = xm[r + j] != 0;
          const size_t rs = static_cast<size_t>(slot) * W + j;
          const float xo = rv[rs];
          const bool mo = rm[rs] != 0;
          rv[rs] = x;
          rm[rs] = m ? 1 : 0;
          n[j] += static_cast<int>(m) - static_cast<int>(mo);
          const float nl = mm.lo[j], nh = mm.hi[j];
          int *h = hist.data() + static_cast<size_t>(j) * kBuckets;
          if (nh > nl) {
            if (nl != plo[j] || nh != phi[j]) { // 值域变了: 整窗重建
              std::fill(h, h + kBuckets, 0);
              for (int s = 0; s < d; ++s)
                if (rm[static_cast<size_t>(s) * W + j])
                  ++h[bin_of(rv[static_cast<size_t>(s) * W + j], nl, nh)];
            } else { // 值域没变: 增量出旧入新
              if (mo)
                --h[bin_of(xo, nl, nh)];
              if (m)
                ++h[bin_of(x, nl, nh)];
            }
          }
          plo[j] = nl;
          phi[j] = nh;
          if constexpr (K == HistK::RANK) {
            const bool ok = full && n[j] >= 1 && m; // 相对型
            double v = 0.0;
            if (ok) {
              if (!(nh > nl))
                v = 0.5;
              else {
                const int b = bin_of(x, nl, nh);
                int less = 0;
                for (int q = 0; q < b; ++q)
                  less += h[q];
                v = pct_of(less, h[b], n[j]);
              }
            }
            detail::put(ov, om, r + j, v, ok);
          } else {
            const bool ok = full && n[j] >= 1;
            double v = 0.0;
            if (ok) {
              const float med = detail::hist_quantile(h, n[j], nl, nh, 0.5);
              if constexpr (K == HistK::MEDIAN)
                v = med;
              else { // MAD: 对 |x − med| 这组值重新定桶再取中位
                int cnt2 = 0;
                float l2 = 0.f, u2 = 0.f;
                for (int s = 0; s < d; ++s)
                  if (rm[static_cast<size_t>(s) * W + j]) {
                    const float w2 = std::fabs(rv[static_cast<size_t>(s) * W + j] - med);
                    ad[cnt2] = w2;
                    l2 = cnt2 == 0 ? w2 : std::fmin(l2, w2);
                    u2 = cnt2 == 0 ? w2 : std::fmax(u2, w2);
                    ++cnt2;
                  }
                if (!(u2 > l2))
                  v = l2;
                else {
                  int h2[kBuckets];
                  std::fill(h2, h2 + kBuckets, 0);
                  for (int s = 0; s < cnt2; ++s)
                    ++h2[bin_of(ad[s], l2, u2)];
                  v = detail::hist_quantile(h2, cnt2, l2, u2, 0.5);
                }
              }
            }
            detail::put(ov, om, r + j, v, ok);
          }
        }
      }
    }
  }
};

// ---- WmaRoll: 线性权 (最旧 1 … 最新 d); 滑动时全体权重 −1 (平移), 新样本以权 d 入窗 ----
struct TsWmaRoll {
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const int d = p.d;
    const size_t W = static_cast<size_t>(A);
    std::vector<int> n(W, 0);
    std::vector<double> sx(W, 0.0), sw(W, 0.0), swx(W, 0.0);
    for (int t = 0; t < T; ++t) {
      for (int a = 0; a < A; ++a) { // 平移: Σw −= n, Σwx −= Σx
        sw[a] -= n[a];
        swx[a] -= sx[a];
      }
      if (t >= d) { // 滑出样本权重已平移到 0, 只影响 n / Σx
        const size_t q = static_cast<size_t>(t - d) * A;
        for (int a = 0; a < A; ++a) {
          const bool mo = xm[q + a] != 0;
          n[a] -= mo;
          sx[a] -= mo ? static_cast<double>(xv[q + a]) : 0.0;
        }
      }
      const size_t r = static_cast<size_t>(t) * A;
      const bool full = t >= d - 1;
      for (int a = 0; a < A; ++a) {
        const float x = xv[r + a];
        const bool m = xm[r + a] != 0;
        n[a] += m;
        sx[a] += m ? static_cast<double>(x) : 0.0;
        sw[a] += m ? static_cast<double>(d) : 0.0;
        swx[a] += m ? static_cast<double>(d) * x : 0.0;
        const bool ok = full && n[a] >= 1;
        detail::put(ov, om, r + a, ok ? swx[a] / sw[a] : 0.0, ok); // n ≥ 1 ⇒ Σw ≥ 1
      }
    }
  }
};

// ---- SlopeRoll: OLS 斜率; 滑动时窗内下标全体 −1 (平移), 下标和/平方和是整数值 double, 平移精确 ----
struct TsSlopeRoll {
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const int d = p.d;
    const size_t W = static_cast<size_t>(A);
    std::vector<int> n(W, 0);
    std::vector<double> si(W, 0.0), sii(W, 0.0), sx(W, 0.0), six(W, 0.0);
    for (int t = 0; t < T; ++t) {
      for (int a = 0; a < A; ++a) { // 平移: Σ(i−1)² = Σi² − 2Σi + n
        sii[a] -= 2.0 * si[a] - n[a];
        si[a] -= n[a];
        six[a] -= sx[a];
      }
      if (t >= d) { // 滑出样本平移后下标 = −1
        const size_t q = static_cast<size_t>(t - d) * A;
        for (int a = 0; a < A; ++a) {
          const bool mo = xm[q + a] != 0;
          const double xo = mo ? static_cast<double>(xv[q + a]) : 0.0;
          si[a] += mo;
          sii[a] -= mo;
          sx[a] -= xo;
          six[a] += xo;
          n[a] -= mo;
        }
      }
      const size_t r = static_cast<size_t>(t) * A;
      const bool full = t >= d - 1;
      const double fi = d - 1; // 新样本的窗内下标
      for (int a = 0; a < A; ++a) {
        const bool m = xm[r + a] != 0;
        const double dx = m ? static_cast<double>(xv[r + a]) : 0.0;
        si[a] += m ? fi : 0.0;
        sii[a] += m ? fi * fi : 0.0;
        sx[a] += dx;
        six[a] += fi * dx;
        n[a] += m;
        const bool ok = full && n[a] >= 2;
        double v = 0.0;
        if (ok)
          v = (six[a] - si[a] * sx[a] / n[a]) / (sii[a] - si[a] * si[a] / n[a]);
        detail::put(ov, om, r + a, v, ok);
      }
    }
  }
};

// ---- CountGtRoll ----
struct TsCountGtRoll {
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const int d = p.d;
    std::vector<int> n(static_cast<size_t>(A), 0), c(static_cast<size_t>(A), 0);
    for (int t = 0; t < T; ++t) {
      if (t >= d) {
        const size_t q = static_cast<size_t>(t - d) * A;
        for (int a = 0; a < A; ++a) {
          const bool mo = xm[q + a] != 0;
          c[a] -= mo && (xv[q + a] > p.k);
          n[a] -= mo;
        }
      }
      const size_t r = static_cast<size_t>(t) * A;
      const bool full = t >= d - 1;
      for (int a = 0; a < A; ++a) {
        const bool m = xm[r + a] != 0;
        c[a] += m && (xv[r + a] > p.k);
        n[a] += m;
        const bool ok = full && n[a] >= 1;
        detail::put(ov, om, r + a, ok ? static_cast<double>(c[a]) : 0.0, ok);
      }
    }
  }
};

// ---- ProductRoll: 滑动加减 Σlog1p (log1p 确定性 ⇒ 出窗减去的正是入窗加上的) ----
struct TsProductRoll {
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const int d = p.d;
    std::vector<int> n(static_cast<size_t>(A), 0), bad(static_cast<size_t>(A), 0);
    std::vector<double> sl(static_cast<size_t>(A), 0.0);
    for (int t = 0; t < T; ++t) {
      if (t >= d) {
        const size_t q = static_cast<size_t>(t - d) * A;
        for (int a = 0; a < A; ++a) {
          const float xo = xv[q + a];
          const bool mo = xm[q + a] != 0;
          const bool good = mo && xo > -1.f;
          sl[a] -= good ? std::log1p(static_cast<double>(xo)) : 0.0;
          bad[a] -= mo && !(xo > -1.f);
          n[a] -= mo;
        }
      }
      const size_t r = static_cast<size_t>(t) * A;
      const bool full = t >= d - 1;
      for (int a = 0; a < A; ++a) {
        const float x = xv[r + a];
        const bool m = xm[r + a] != 0;
        const bool good = m && x > -1.f;
        sl[a] += good ? std::log1p(static_cast<double>(x)) : 0.0;
        bad[a] += m && !(x > -1.f);
        n[a] += m;
        const bool ok = full && n[a] >= 1 && bad[a] == 0;
        detail::put(ov, om, r + a, ok ? std::expm1(sl[a]) : 0.0, ok);
      }
    }
  }
};

// ---- AgeRoll: 变动位置单调递增 ⇒ 只需记"最近一次变动的全局行号", 与窗起点比较即可 ----
struct TsAgeRoll {
  CP_SIG {
    CP_NO23;
    assert(p.d >= 1);
    const int d = p.d;
    std::vector<int> last(static_cast<size_t>(A), -1); // 相邻两格都有效且值精确不等的最近行号
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      if (t >= 1) {
        const size_t q = r - A;
        for (int a = 0; a < A; ++a) {
          const bool chg = xm[r + a] && xm[q + a] && xv[r + a] != xv[q + a];
          last[a] = chg ? t : last[a];
        }
      }
      for (int a = 0; a < A; ++a) {
        const bool ok = t >= d - 1 && xm[r + a];
        const double v =
            last[a] >= t - d + 2 ? static_cast<double>(t - last[a]) : static_cast<double>(d);
        detail::put(ov, om, r + a, ok ? v : 0.0, ok);
      }
    }
  }
};

// ---- Cov / Corr / Beta / Resid (Roll): 滑动加减二元幂和 + van Herk 双有效极值 ----
template <detail::Pair K>
struct RollPair {
  static constexpr bool kSpx = K == detail::Pair::CORR;
  static constexpr bool kSpy = K != detail::Pair::COV;
  CP_SIG {
    CP_NO3;
    assert(p.d >= 1);
    const int d = p.d;
    const size_t W = static_cast<size_t>(A);
    std::vector<int> n(W, 0);
    std::vector<double> sx(W, 0.0), sy(W, 0.0), sxy(W, 0.0), sxx(kSpx ? W : 0, 0.0),
        syy(kSpy ? W : 0, 0.0);
    detail::RollMinMax mmx(kSpx ? d : 1, kSpx ? A : 1); // 未用的实例给最小形状, 不 step
    detail::RollMinMax mmy(kSpy ? d : 1, kSpy ? A : 1);
    for (int t = 0; t < T; ++t) {
      if constexpr (kSpx)
        mmx.step(xv, xm, ym, A, 0, t);
      if constexpr (kSpy)
        mmy.step(yv, ym, xm, A, 0, t);
      if (t >= d) {
        const size_t q = static_cast<size_t>(t - d) * A;
        for (int a = 0; a < A; ++a) {
          const bool mm = xm[q + a] && ym[q + a];
          const double dx = mm ? static_cast<double>(xv[q + a]) : 0.0;
          const double dy = mm ? static_cast<double>(yv[q + a]) : 0.0;
          sx[a] -= dx;
          sy[a] -= dy;
          sxy[a] -= dx * dy;
          if constexpr (kSpx)
            sxx[a] -= dx * dx;
          if constexpr (kSpy)
            syy[a] -= dy * dy;
          n[a] -= mm;
        }
      }
      const size_t r = static_cast<size_t>(t) * A;
      const bool full = t >= d - 1;
      for (int a = 0; a < A; ++a) {
        const float x = xv[r + a], y = yv[r + a];
        const bool mm = xm[r + a] && ym[r + a];
        const double dx = mm ? static_cast<double>(x) : 0.0;
        const double dy = mm ? static_cast<double>(y) : 0.0;
        sx[a] += dx;
        sy[a] += dy;
        sxy[a] += dx * dy;
        if constexpr (kSpx)
          sxx[a] += dx * dx;
        if constexpr (kSpy)
          syy[a] += dy * dy;
        n[a] += mm;
        detail::pair_put<K>(ov, om, r + a, full, n[a], kSpx ? mmx.hi[a] > mmx.lo[a] : true,
                            kSpy ? mmy.hi[a] > mmy.lo[a] : true, sx[a], sy[a], sxy[a],
                            kSpx ? sxx[a] : 0.0, kSpy ? syy[a] : 0.0, x, y, mm);
      }
    }
  }
};

// ---- WMeanRoll ----
struct TsWMeanRoll {
  CP_SIG {
    CP_NO3;
    assert(p.d >= 1);
    const int d = p.d;
    const size_t W = static_cast<size_t>(A);
    std::vector<int> n(W, 0);
    std::vector<double> sy(W, 0.0), syx(W, 0.0), say(W, 0.0);
    for (int t = 0; t < T; ++t) {
      if (t >= d) {
        const size_t q = static_cast<size_t>(t - d) * A;
        for (int a = 0; a < A; ++a) {
          const float x = xv[q + a], y = yv[q + a];
          const bool mm = xm[q + a] && ym[q + a];
          sy[a] -= mm ? static_cast<double>(y) : 0.0;
          syx[a] -= mm ? static_cast<double>(y) * x : 0.0;
          say[a] -= mm ? static_cast<double>(std::fabs(y)) : 0.0;
          n[a] -= mm;
        }
      }
      const size_t r = static_cast<size_t>(t) * A;
      const bool full = t >= d - 1;
      for (int a = 0; a < A; ++a) {
        const float x = xv[r + a], y = yv[r + a];
        const bool mm = xm[r + a] && ym[r + a];
        sy[a] += mm ? static_cast<double>(y) : 0.0;
        syx[a] += mm ? static_cast<double>(y) * x : 0.0;
        say[a] += mm ? static_cast<double>(std::fabs(y)) : 0.0;
        n[a] += mm;
        const bool ok = full && n[a] >= 1 && den_ok(sy[a], say[a]);
        detail::put(ov, om, r + a, ok ? syx[a] / sy[a] : 0.0, ok);
      }
    }
  }
};

// =============================================================================
// EXPO (1): 全程递推, 不按段重置
// =============================================================================
// TsMeanEma: x 有效时 y = 已出现过有效值 ? k·x + (1−k)·y : x; x 无效时 y 与掩码都不动
struct TsMeanEma {
  CP_SIG {
    CP_NO23;
    assert(p.k > 0.f && p.k <= 1.f);
    const double k = p.k;
    std::vector<double> y(static_cast<size_t>(A), 0.0);
    std::vector<uint8_t> on(static_cast<size_t>(A), 0);
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A;
      for (int a = 0; a < A; ++a) {
        if (xm[r + a]) {
          y[a] = on[a] ? k * xv[r + a] + (1.0 - k) * y[a] : static_cast<double>(xv[r + a]);
          on[a] = 1;
        }
        detail::put(ov, om, r + a, y[a], on[a] != 0);
      }
    }
  }
};

// =============================================================================
// 实例化: OpTable 同名 (模板核 × 窗形; 单独 struct 的已在上文直接以表名定义)
// =============================================================================
using TsSumCum = ExpSum<false>;
using TsMeanCum = ExpSum<true>;
using TsSumRoll = RollSum<false>;
using TsMeanRoll = RollSum<true>;
using TsVarCum = ExpMom<detail::Mom::VAR>;
using TsVarRoll = RollMom<detail::Mom::VAR>;
using TsStdCum = ExpMom<detail::Mom::STD>;
using TsStdRoll = RollMom<detail::Mom::STD>;
using TsSkewCum = ExpMom<detail::Mom::SKEW>;
using TsSkewRoll = RollMom<detail::Mom::SKEW>;
using TsKurtCum = ExpMom<detail::Mom::KURT>;
using TsKurtRoll = RollMom<detail::Mom::KURT>;
using TsZRoll = RollMom<detail::Mom::Z>;
using TsMaxCum = ExpExt<true, false>;
using TsMaxRoll = RollExt<true, false>;
using TsMinCum = ExpExt<false, false>;
using TsMinRoll = RollExt<false, false>;
using TsArgMaxCum = ExpExt<true, true>;
using TsArgMaxRoll = RollExt<true, true>;
using TsArgMinCum = ExpExt<false, true>;
using TsArgMinRoll = RollExt<false, true>;
using TsRankRoll = RollHist<HistK::RANK>;
using TsMedianRoll = RollHist<HistK::MEDIAN>;
using TsMadRoll = RollHist<HistK::MAD>;
using TsCovCum = ExpPair<detail::Pair::COV>;
using TsCovRoll = RollPair<detail::Pair::COV>;
using TsCorrCum = ExpPair<detail::Pair::CORR>;
using TsCorrRoll = RollPair<detail::Pair::CORR>;
using TsBetaCum = ExpPair<detail::Pair::BETA>;
using TsBetaRoll = RollPair<detail::Pair::BETA>;
using TsResidCum = ExpPair<detail::Pair::RESID>;
using TsResidRoll = RollPair<detail::Pair::RESID>;

#undef CP_P3
#undef CP_P2
#undef CP_P1
#undef CP_NO3
#undef CP_NO23
#undef CP_SIG

} // namespace factor::cpu::ts
