#pragma once

// =============================================================================
// TS 算子的挖掘 CPU 后端 (factor::cpu::ts, 62 个, 对应 OpTable 的 OP_TS = OP_TS0..3)
// =============================================================================
//   定位: 因子挖掘无 GPU 时的 fallback —— 整张量批量直算, 不逐点因果推进, 吞吐必须 ≥ stream.
//   语义与流式一致 (掩码逐位相等, 值在对拍容差内); 独立第二实现, **不得** include 任何
//   Stream/Gpu 头, 不复用其算法代码 (否则 cpu↔stream 对拍空转).
//
//   【布局与向量化】SoA 行主序 [T][A]: 内层 A 连续, 一切按"行推进 + 跨资产状态数组"组织,
//   内层循环 branchless select 交给自动向量化. 有状态算子按资产列块 (kTileA 列) 推进: 状态数组按块容量
//   从调用方暂存切出 (scratch 声明, 驻留 L1/L2), 算子内部不分配.
//
//   【算法】(旧 naive 参考实现按定义 O(T·d·A) 重算, 本文件全部摊到 O(T·A) 或 O(T·A·B)):
//     POINT   单遍逐元素
//     EXPAND  沿 t 递推累加 (double 幂和), 段界 reset; 序统计 = 增量直方图 (段内 lo/hi 只扩不缩, 扩时重建)
//     ROLL    滑动加减幂和 (double); spread 判据要求精确 lo/hi → van Herk 分块前后缀滑窗极值;
//             序统计 = 环形窗缓存 + 直方图 (lo/hi 未变则增量改桶, 变了整窗重建)
//     EXPO    沿 t 递推
//
//   【数值】滑动加减幂和相对整窗重算有 O(ε_double) 漂移; 近 ulp 简并窗 (真中心矩 ≈ double 舍入噪声)
//   下值可能与流式不一致 —— 与 GPU SCAN 后端同一风险等级, 契约的造数与容差刻意避开该带.
//   中心矩用 fmax(·, 0) 钳掉相消出负; 溢出由 mk 出口转无效 (契约: 溢出是数据属性, 不是 bug).
//   退化按契约第 6 条给中性值 (select 一支, 掩码不动); 掩码只随输入格 (第 7 条). ROLL 未满窗 = 已有前缀, 无 full 判据.
//
//   出错策略: 不做错误处理, 只 assert (参数非法立刻死在最早处).
//   【precise-math】依赖受控浮点, 编进 -fno-fast-math -fno-math-errno TU (后者让 sqrt 保持 intrinsic).
//   【向量化红线】内层循环里不许出现不可内联的 libm 调用 (pow / 引用进出的 std::clamp 等): 一个调用就让整段
//   退化为标量, 吞吐与 stream 持平. 躲不开的 (log 族) 是已知下限, 该循环显式关向量化器, 免得它标量化拼装反而更慢.
// =============================================================================

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <tuple>

#include "factor/Contract.hpp"

namespace factor::cpu {
inline constexpr int kTileA = 128; // TS 有状态算子的资产列块宽 (状态容量; evaluator 也按它切 TS 任务)
} // namespace factor::cpu

namespace factor::cpu::ts {

// -----------------------------------------------------------------------------
// detail: 列块 / 输出 / 幂和→中心矩 / 滑窗极值 (van Herk) / 桶分位
// -----------------------------------------------------------------------------
namespace detail {

inline constexpr float kInf = std::numeric_limits<float>::infinity();

// ---- 列块: 指针已偏到块首列, 行距仍是 A (i = t*A + a, a ∈ [0, w)) ----
struct Tile {
  const float *xv;
  const uint8_t *xm;
  const float *yv;
  const uint8_t *ym;
  const float *zv;
  const uint8_t *zm;
  float *ov;
  uint8_t *om;
  int T, A, w;
};
template <class P>
inline P *at(P *p, int a0) { return p ? p + a0 : nullptr; }
// 列 [lo, hi) 一整块 (逐点算子)
inline Tile span(const Call &c, int lo, int hi) {
  assert(0 <= lo && lo <= hi && hi <= c.A && c.o.v && c.o.m);
  return {at(c.x.v, lo), at(c.x.m, lo), at(c.y.v, lo), at(c.y.m, lo), at(c.z.v, lo), at(c.z.m, lo), c.o.v + lo, c.o.m + lo, c.T, c.A, hi - lo};
}
// 从 a0 起 ≤ kTileA 列 (有状态算子; 状态从行 0 起算)
inline Tile tile(const Call &c, int a0, int hi) {
  assert(c.t0 == 0 && "非逐点算子从行 0 起算 (段界 / 窗口历史)");
  return span(c, a0, std::min(hi, a0 + kTileA));
}

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
//   状态按列块容量 kTileA 从 Bump 切; 换块 reset(width) 即可 (t = 0 的重建与首行写入盖住全部状态, 不用预填).
//   成员先落局部再进列循环 (经 this 读的界 / 指针会被当成可能被循环体改写)
struct Win { // 窗内极值 [width] (下一次 step 前有效)
  const float *lo = nullptr, *hi = nullptr;
};
class RollMinMax {
public:
  RollMinMax() = default;
  RollMinMax(Bump &b, int w) : w_(w) {
    assert(w >= 1);
    const size_t cap = static_cast<size_t>(kTileA);
    sl_ = b.take<float>(static_cast<size_t>(w + 1) * cap);
    sh_ = b.take<float>(static_cast<size_t>(w + 1) * cap);
    pl_ = b.take<float>(cap);
    ph_ = b.take<float>(cap);
    lo_ = b.take<float>(cap);
    hi_ = b.take<float>(cap);
  }
  void reset(int width) {
    assert(1 <= width && width <= kTileA);
    n_ = width;
  }
  // 按 t = 0, 1, … 递增喂行 (xv/xm 为块首列指针, 行距 stride, 列 [0, width)); 返回窗 [t−w+1, t] 极值
  Win step(const float *xv, const uint8_t *xm, const uint8_t *xm2, int stride, int t) {
    const int pos = t % w_, n = n_;
    if (pos == 0)
      rebuild(xv, xm, xm2, stride, t);
    const size_t r = static_cast<size_t>(t) * stride;
    const float *sl = sl_ + static_cast<size_t>(pos + 1) * n;
    const float *sh = sh_ + static_cast<size_t>(pos + 1) * n;
    float *pl = pl_, *ph = ph_, *lo = lo_, *hi = hi_;
    FACTOR_COLS for (int a = 0; a < n; ++a) {
      const bool m = xm[r + a] && (!xm2 || xm2[r + a]);
      const float vl = m ? xv[r + a] : kInf, vh = m ? xv[r + a] : -kInf;
      pl[a] = pos == 0 ? vl : std::fmin(pl[a], vl);
      ph[a] = pos == 0 ? vh : std::fmax(ph[a], vh);
      lo[a] = std::fmin(sl[a], pl[a]);
      hi[a] = std::fmax(sh[a], ph[a]);
    }
    return {lo, hi};
  }

private:
  void rebuild(const float *xv, const uint8_t *xm, const uint8_t *xm2, int stride, int t) {
    const int w = w_, n = n_;
    std::fill_n(sl_ + static_cast<size_t>(w) * n, n, kInf); // 下标 w = 空后缀哨兵
    std::fill_n(sh_ + static_cast<size_t>(w) * n, n, -kInf);
    for (int i = w - 1; i >= 0; --i) {
      float *cl = sl_ + static_cast<size_t>(i) * n;
      float *ch = sh_ + static_cast<size_t>(i) * n;
      const float *nl = cl + n;
      const float *nh = ch + n;
      const int row = t - w + i;
      if (row < 0) { // 平面开头: 空行
        std::copy_n(nl, n, cl);
        std::copy_n(nh, n, ch);
        continue;
      }
      const size_t r = static_cast<size_t>(row) * stride;
      FACTOR_COLS for (int a = 0; a < n; ++a) {
        const bool m = xm[r + a] && (!xm2 || xm2[r + a]);
        cl[a] = std::fmin(m ? xv[r + a] : kInf, nl[a]);
        ch[a] = std::fmax(m ? xv[r + a] : -kInf, nh[a]);
      }
    }
  }
  int w_ = 0, n_ = 0;
  float *sl_ = nullptr, *sh_ = nullptr, *pl_ = nullptr, *ph_ = nullptr, *lo_ = nullptr, *hi_ = nullptr;
};

// ---- 滑窗极值 + 最旧并列位置 (契约: 严格比较 → 并列保留最旧) ----
//   后缀 (窗的旧半) 反向扫取 ≥/≤ (旧优先), 前缀正向扫取 >/< (新不夺旧), 合并时后缀平局优先.
//   状态切分 / 换块 / 成员落局部同 RollMinMax.
struct ArgWin {                // [width] (下一次 step 前有效)
  const float *best = nullptr; // 窗内极值 (无有效样本 → 哨兵, 由调用方的 n 屏蔽)
  const int *arg = nullptr;    // 最旧极值的全局行号
};
template <bool IsMax>
class RollArgExt {
public:
  RollArgExt() = default;
  RollArgExt(Bump &b, int w) : w_(w) {
    assert(w >= 1);
    const size_t cap = static_cast<size_t>(kTileA);
    sv_ = b.take<float>(static_cast<size_t>(w + 1) * cap);
    sj_ = b.take<int>(static_cast<size_t>(w + 1) * cap);
    pv_ = b.take<float>(cap);
    pj_ = b.take<int>(cap);
    best_ = b.take<float>(cap);
    arg_ = b.take<int>(cap);
  }
  void reset(int width) {
    assert(1 <= width && width <= kTileA);
    n_ = width;
  }
  ArgWin step(const float *xv, const uint8_t *xm, int stride, int t) {
    const int pos = t % w_, n = n_;
    if (pos == 0)
      rebuild(xv, xm, stride, t);
    const size_t r = static_cast<size_t>(t) * stride;
    const float *sv = sv_ + static_cast<size_t>(pos + 1) * n;
    const int *sj = sj_ + static_cast<size_t>(pos + 1) * n;
    float *pv = pv_, *best = best_;
    int *pj = pj_, *arg = arg_;
    FACTOR_COLS for (int a = 0; a < n; ++a) {
      const bool m = xm[r + a] != 0;
      const float x = xv[r + a];
      const bool take = pos == 0 || (m && (IsMax ? x > pv[a] : x < pv[a])); // 块首重开前缀; 其后严格胜出才换 (新不夺旧)
      pv[a] = take ? (m ? x : kSent) : pv[a];
      pj[a] = take ? (m ? t : -1) : pj[a];
      const bool s_wins = IsMax ? sv[a] >= pv[a] : sv[a] <= pv[a];
      best[a] = s_wins ? sv[a] : pv[a];
      arg[a] = s_wins ? sj[a] : pj[a];
    }
    return {best, arg};
  }

private:
  static constexpr float kSent = IsMax ? -kInf : kInf;
  void rebuild(const float *xv, const uint8_t *xm, int stride, int t) {
    const int w = w_, n = n_;
    std::fill_n(sv_ + static_cast<size_t>(w) * n, n, kSent);
    std::fill_n(sj_ + static_cast<size_t>(w) * n, n, -1);
    for (int i = w - 1; i >= 0; --i) {
      float *cv = sv_ + static_cast<size_t>(i) * n;
      int *cj = sj_ + static_cast<size_t>(i) * n;
      const float *nv = cv + n;
      const int *nj = cj + n;
      const int row = t - w + i;
      if (row < 0) {
        std::copy_n(nv, n, cv);
        std::copy_n(nj, n, cj);
        continue;
      }
      const size_t r = static_cast<size_t>(row) * stride;
      FACTOR_COLS for (int a = 0; a < n; ++a) {
        const bool take = xm[r + a] && (IsMax ? xv[r + a] >= nv[a] : xv[r + a] <= nv[a]);
        cv[a] = take ? xv[r + a] : nv[a];
        cj[a] = take ? row : nj[a];
      }
    }
  }
  int w_ = 0, n_ = 0;
  float *sv_ = nullptr, *pv_ = nullptr, *best_ = nullptr;
  int *sj_ = nullptr, *pj_ = nullptr, *arg_ = nullptr;
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

// ---- 一元矩核的退化判据与求值 (EXPAND 与 ROLL 共用公式, 窗形只是状态推进方式不同); 退化 → 0 ----
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
  else if constexpr (K == Mom::SKEW) {
    // m2^{3/2} 写成 v·√v 而非 pow(v, 1.5): pow 是不可内联的 libm 调用, 一进内层循环整段向量化就没了
    const double v2 = m2 / n;
    return (c3_of(s1, s2, s3, n) / n) / (v2 * std::sqrt(v2));
  } else
    return (c4_of(s1, s2, s3, s4, n) / n) / ((m2 / n) * (m2 / n)) - 3.0;
}

// ---- 二元核的求值: 退化 → 0 (Resid → β = 0 即 x − μ^x); 只有 Resid 是相对型 (掩码 = 当前对有效) ----
enum class Pair { COV,
                  CORR,
                  BETA,
                  RESID };

template <Pair K>
inline void pair_put(float *ov, uint8_t *om, size_t i, int n, bool spx, bool spy, double sx,
                     double sy, double sxy, double sxx, double syy, float x, float y, bool mxy) {
  (void)spx, (void)spy, (void)sxx, (void)x, (void)y, (void)mxy;
  bool ok = n >= 2;
  if constexpr (K == Pair::CORR)
    ok = ok && spx && spy;
  else if constexpr (K != Pair::COV)
    ok = ok && spy;
  const double cxy = ok ? sxy - sx * sy / n : 0.0;
  double v = 0.0;
  if constexpr (K == Pair::RESID) { // 当前对有效 ⇒ n ≥ 1, 均值有定义
    const double b = ok ? cxy / (syy - sy * sy / n) : 0.0;
    v = mxy ? (x - sx / n) - b * (y - sy / n) : 0.0;
  } else if (ok) {
    if constexpr (K == Pair::COV)
      v = cxy / (n - 1);
    else if constexpr (K == Pair::CORR)
      v = cxy / std::sqrt((sxx - sx * sx / n) * (syy - sy * sy / n));
    else
      v = cxy / (syy - sy * sy / n);
  }
  put(ov, om, i, v, K == Pair::RESID ? mxy : true);
}

} // namespace detail

// -----------------------------------------------------------------------------
// 统一签名 (Contract【调用形状】; 元数不足的输入槽为空, 未用到的指针一律不许读)
// -----------------------------------------------------------------------------
//   scratch(c) = 每线程暂存字节; run(c, rg, ws) 算资产列 [rg.lo, rg.hi) 的全部 T 行.
//   有状态算子: carve(Bump, c) 按 kTileA 列容量切状态 (scratch 与 run 走同一份切分), 逐列块推进, 块首重置状态.
//   段长 S = c.geo.S 是运行期量 (EXPAND 段界 / TodMask 段内位置)
#define CP_RUN static void run(const Call &c, Range rg, void *ws)
#define CP_SCRATCH                       \
  static size_t scratch(const Call &c) { \
    Bump b;                              \
    (void)carve(b, c);                   \
    return b.used;                       \
  }
#define CP_STATELESS                                \
  static size_t scratch(const Call &) { return 0; } \
  static void run(const Call &c, Range rg, void *)

// =============================================================================
// POINT (21): 逐点无状态, 单遍. 表达式与流式严格同式 (三后端要位级一致).
// =============================================================================
//   逐点算子接受任意行窗 (c.t0 = 行 0 的全局行号; 融合块按小块调用)
#define CP_P1(Name, V, M)                                                                   \
  struct Name {                                                                             \
    CP_STATELESS {                                                                          \
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi); \
      const Param &p = c.p;                                                                 \
      (void)p;                                                                              \
      for (int t = 0; t < T; ++t)                                                           \
        FACTOR_COLS for (size_t i = static_cast<size_t>(t) * A, e = i + w; i < e; ++i) {    \
          const float x = xv[i];                                                            \
          const bool mx = xm[i] != 0;                                                       \
          const bool m = (M);                                                               \
          detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m);                      \
        }                                                                                   \
    }                                                                                       \
  };

#define CP_P2(Name, V, M)                                                                   \
  struct Name {                                                                             \
    CP_STATELESS {                                                                          \
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi); \
      const Param &p = c.p;                                                                 \
      (void)p;                                                                              \
      for (int t = 0; t < T; ++t)                                                           \
        FACTOR_COLS for (size_t i = static_cast<size_t>(t) * A, e = i + w; i < e; ++i) {    \
          const float x = xv[i], y = yv[i];                                                 \
          const bool mx = xm[i] != 0, my = ym[i] != 0;                                      \
          const bool m = (M);                                                               \
          detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m);                      \
        }                                                                                   \
    }                                                                                       \
  };

#define CP_P3(Name, V, M)                                                                   \
  struct Name {                                                                             \
    CP_STATELESS {                                                                          \
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi); \
      const Param &p = c.p;                                                                 \
      (void)p;                                                                              \
      for (int t = 0; t < T; ++t)                                                           \
        FACTOR_COLS for (size_t i = static_cast<size_t>(t) * A, e = i + w; i < e; ++i) {    \
          const float x = xv[i], y = yv[i], z = zv[i];                                      \
          const bool mx = xm[i] != 0, my = ym[i] != 0, mz = zm[i] != 0;                     \
          const bool m = (M);                                                               \
          detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m);                      \
        }                                                                                   \
    }                                                                                       \
  };

CP_P1(TsAbs, std::fabs(x), mx)
CP_P1(TsSign, x > 0.f ? 1.f : (x < 0.f ? -1.f : 0.f), mx)
CP_P1(TsSqrt, std::copysign(std::sqrt(std::fabs(x)), x), mx)

// TsLog: log1p 无向量版 (libm 标量调用是下限). 不禁向量化的话, 向量化器会把 log1pf 逐 lane 标量化再 insert
// 拼回去, 比纯标量循环还慢 —— 故此处显式关掉向量化器, 走干净的标量循环.
struct TsLog {
  CP_STATELESS {
    const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi);
    for (int t = 0; t < T; ++t) {
#pragma clang loop vectorize(disable)
      for (size_t i = static_cast<size_t>(t) * A, e = i + w; i < e; ++i) {
        const float x = xv[i];
        const bool m = xm[i] != 0;
        detail::put(ov, om, i, m ? static_cast<double>(std::copysign(std::log1p(std::fabs(x)), x)) : 0.0, m);
      }
    }
  }
};
CP_P1(TsRelu, std::fmax(0.f, x), mx)
CP_P1(TsNeg, -x, mx)
CP_P1(TsRecip, x != 0.f ? 1.f / x : 0.f, mx) // x = 0 → 0, 溢出由 mk 转无效
// TsGt: 指示 (严格大于); 套 Sum 窗 = 计数, 套 Mean 窗 = 占比
CP_P1(TsGt, x > p.k ? 1.f : 0.f, mx)

// TsClip: clamp 要求 lo ≤ hi, 即 k ≥ 0 —— 参数级约束, 早死在 assert 上
struct TsClip {
  CP_STATELESS {
    const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi);
    assert(c.p.k >= 0.f);
    const float lo = -c.p.k, hi = c.p.k;
    for (int t = 0; t < T; ++t)
      FACTOR_COLS for (size_t i = static_cast<size_t>(t) * A, e = i + w; i < e; ++i) {
        const float x = xv[i];
        const bool m = xm[i] != 0;
        // 与 std::clamp 同式 (v < lo ? lo : hi < v ? hi : v), 展开成值比较让它变成 cmp + blend;
        // std::clamp 走 const& 进出, 向量化器对引用选择不认账
        const float cl = x < lo ? lo : (hi < x ? hi : x);
        detail::put(ov, om, i, m ? static_cast<double>(cl) : 0.0, m);
      }
  }
};

// TsTodMask: 元数 0, 完全不读输入指针 (调用方传 nullptr); 段内位置按全局行号 (c.t0 + t)
struct TsTodMask {
  CP_STATELESS {
    const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi);
    const int S = c.geo.S, k = static_cast<int>(c.p.k), k2 = static_cast<int>(c.p.k2);
    assert(S >= 1);
    for (int t = 0; t < T; ++t) {
      const int t_seg = (c.t0 + t) % S;
      const float v = (t_seg >= k && t_seg < k2) ? 1.f : 0.f;
      FACTOR_COLS for (size_t i = static_cast<size_t>(t) * A, e = i + w; i < e; ++i)
          detail::put(ov, om, i, static_cast<double>(v), true);
    }
  }
};

CP_P2(TsAdd, x + y, mx &&my)
CP_P2(TsSub, x - y, mx &&my)
CP_P2(TsMul, x *y, mx &&my)
CP_P2(TsDiv, y != 0.f ? x / y : 0.f, mx &&my) // y = 0 → 0, 溢出由 mk 转无效
CP_P2(TsMax, std::fmax(x, y), mx &&my)
CP_P2(TsMin, std::fmin(x, y), mx &&my)
// TsImb / TsShare: 和相消 (scale = |x| + |y|) → 0 / 0.5, 分母不钳位
CP_P2(TsImb, den_ok(static_cast<double>(x) + y, std::fabs(x) + std::fabs(y)) ? (x - y) / (x + y) : 0.f, mx &&my)
CP_P2(TsShare, den_ok(static_cast<double>(x) + y, std::fabs(x) + std::fabs(y)) ? x / (x + y) : 0.5f, mx &&my)
// TsLogRatio: 非正侧饱和到 ∓kLogCap (见 Contract::log_ratio); log 无向量版, 关向量化器同 TsLog
struct TsLogRatio {
  CP_STATELESS {
    const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi);
    for (int t = 0; t < T; ++t) {
#pragma clang loop vectorize(disable)
      for (size_t i = static_cast<size_t>(t) * A, e = i + w; i < e; ++i) {
        const bool m = xm[i] && ym[i];
        detail::put(ov, om, i, m ? static_cast<double>(log_ratio(xv[i], yv[i])) : 0.0, m);
      }
    }
  }
};
// TsMask: y 当掩码, y ≤ 0 → 无效 (不是补 0), 让下游窗只吃子集
CP_P2(TsMask, x, mx && my && y > 0.f)
// TsWhere: 未被选中的那支不要求有效
CP_P3(TsWhere, x > 0.f ? y : z, mx && (x > 0.f ? my : mz))

// =============================================================================
// EXPAND (18): 沿 t 递推, 段界 reset. 状态 = 跨资产数组.
// =============================================================================

// ---- Sum / Mean ----
template <bool Mean>
struct ExpSum {
  static auto carve(Bump &b, const Call &) { return std::tuple{b.take<int>(kTileA), b.take<double>(kTileA)}; }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, s] = carve(b, c);
    const int S = c.geo.S;
    assert(S >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      for (int seg = 0; seg < T; seg += S) {
        std::fill_n(n, w, 0);
        std::fill_n(s, w, 0.0);
        const int e = std::min(seg + S, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
            const bool m = xm[r + a] != 0;
            s[a] += m ? static_cast<double>(xv[r + a]) : 0.0;
            n[a] += m;
            detail::put(ov, om, r + a, Mean ? (n[a] >= 1 ? s[a] / n[a] : 0.0) : s[a], true); // 空: 和 0 / 均值 0
          }
        }
      }
    }
  }
};

// ---- Var / Std / Skew / Kurt: 幂和递推, lo/hi 段内只扩不缩 (精确 spread); 退化 → 0 ----
template <detail::Mom K>
struct ExpMom {
  static constexpr bool kS3 = K == detail::Mom::SKEW || K == detail::Mom::KURT;
  static constexpr bool kS4 = K == detail::Mom::KURT;
  static auto carve(Bump &b, const Call &) {
    constexpr size_t W = kTileA;
    return std::tuple{b.take<int>(W), b.take<double>(W), b.take<double>(W), b.take<double>(kS3 ? W : 0), b.take<double>(kS4 ? W : 0),
                      b.take<float>(W), b.take<float>(W)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, s1, s2, s3, s4, lo, hi] = carve(b, c);
    const int S = c.geo.S;
    assert(S >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      for (int seg = 0; seg < T; seg += S) {
        std::fill_n(n, w, 0);
        std::fill_n(s1, w, 0.0);
        std::fill_n(s2, w, 0.0);
        if constexpr (kS3)
          std::fill_n(s3, w, 0.0);
        if constexpr (kS4)
          std::fill_n(s4, w, 0.0);
        std::fill_n(lo, w, detail::kInf);
        std::fill_n(hi, w, -detail::kInf);
        const int e = std::min(seg + S, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
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
                        true);
          }
        }
      }
    }
  }
};

// ---- Max / Min / ArgMax / ArgMin: 递推极值 (严格比较 → 并列保留最旧); 空 → 0 ----
template <bool IsMax, bool IsArg>
struct ExpExt {
  static auto carve(Bump &b, const Call &) { return std::tuple{b.take<int>(kTileA), b.take<int>(kTileA), b.take<float>(kTileA)}; }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, bi, best] = carve(b, c);
    const int S = c.geo.S;
    assert(S >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      for (int seg = 0; seg < T; seg += S) {
        std::fill_n(n, w, 0);
        std::fill_n(bi, w, 0);
        std::fill_n(best, w, 0.f);
        const int e = std::min(seg + S, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A;
          const int ts = t - seg;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
            const float x = xv[r + a];
            const bool m = xm[r + a] != 0;
            const bool take = m && (n[a] == 0 || (IsMax ? x > best[a] : x < best[a]));
            best[a] = take ? x : best[a];
            bi[a] = take ? ts : bi[a];
            n[a] += m;
            const double v = IsArg ? static_cast<double>(ts - bi[a]) : static_cast<double>(best[a]);
            detail::put(ov, om, r + a, n[a] >= 1 ? v : 0.0, true);
          }
        }
      }
    }
  }
};

// ---- RankCum: 增量直方图 (段内 lo/hi 只扩不缩; 扩时用段内样本缓存重建); 相对型, 全并列 → 0.5 ----
struct TsRankCum {
  static auto carve(Bump &b, const Call &c) {
    constexpr size_t W = kTileA;
    return std::tuple{b.take<float>(W * static_cast<size_t>(c.geo.S)), b.take<int>(W * kBuckets), b.take<int>(W), b.take<float>(W), b.take<float>(W)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump bp{static_cast<char *>(ws)};
    const auto [buf, hist, cnt, lo, hi] = carve(bp, c);
    const int S = c.geo.S;
    assert(S >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, W] = detail::tile(c, a0, rg.hi);
      for (int seg = 0; seg < T; seg += S) {
        std::fill_n(cnt, W, 0); // hist 不清: 先有 spread 才读, 而进入 spread 必重建
        std::fill_n(lo, W, detail::kInf);
        std::fill_n(hi, W, -detail::kInf);
        const int e = std::min(seg + S, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A;
          for (int j = 0; j < W; ++j) {
            const float x = xv[r + j];
            const bool m = xm[r + j] != 0;
            int *h = hist + static_cast<size_t>(j) * kBuckets;
            if (m) {
              buf[static_cast<size_t>(j) * S + cnt[j]] = x;
              ++cnt[j];
              const float pl = lo[j], ph = hi[j];
              const float nl = std::fmin(pl, x), nh = std::fmax(ph, x);
              lo[j] = nl;
              hi[j] = nh;
              if (nh > nl) {
                if (nl != pl || nh != ph) { // 值域扩了 (含首次进入 spread): 重建
                  std::fill(h, h + kBuckets, 0);
                  const float *b = buf + static_cast<size_t>(j) * S;
                  for (int s = 0; s < cnt[j]; ++s)
                    ++h[bin_of(b[s], nl, nh)];
                } else {
                  ++h[bin_of(x, nl, nh)];
                }
              }
            }
            double v = 0.0; // 相对型: x_t 无效则无效; x_t 有效 ⇒ cnt ≥ 1
            if (m) {
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
            detail::put(ov, om, r + j, v, m);
          }
        }
      }
    }
  }
};

// ---- HhiCum: Σx 相消 (非负域下即全 0) → 1/n (全体相等的极限); 空 → 0 ----
struct TsHhiCum {
  static auto carve(Bump &b, const Call &) { return std::tuple{b.take<int>(kTileA), b.take<double>(kTileA), b.take<double>(kTileA)}; }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, s, s2] = carve(b, c);
    const int S = c.geo.S;
    assert(S >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      for (int seg = 0; seg < T; seg += S) {
        std::fill_n(n, w, 0);
        std::fill_n(s, w, 0.0);
        std::fill_n(s2, w, 0.0);
        const int e = std::min(seg + S, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
            const float x = xv[r + a];
            const bool m = xm[r + a] != 0;
            s[a] += m ? static_cast<double>(x) : 0.0;
            s2[a] += m ? static_cast<double>(x) * x : 0.0;
            n[a] += m;
            const bool ok = den_ok(s[a] * s[a], n[a] * s2[a]);
            detail::put(ov, om, r + a, n[a] < 1 ? 0.0 : (ok ? s2[a] / (s[a] * s[a]) : 1.0 / n[a]), true);
          }
        }
      }
    }
  }
};

// ---- EntropyCum: 只计 x > 0 的样本; 无正样本 → 0 (单样本熵 = 0 的极限) ----
struct TsEntropyCum {
  static auto carve(Bump &b, const Call &) { return std::tuple{b.take<int>(kTileA), b.take<double>(kTileA), b.take<double>(kTileA)}; }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [np, s, sxl] = carve(b, c);
    const int S = c.geo.S;
    assert(S >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      for (int seg = 0; seg < T; seg += S) {
        std::fill_n(np, w, 0);
        std::fill_n(s, w, 0.0);
        std::fill_n(sxl, w, 0.0);
        const int e = std::min(seg + S, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A;
          for (int a = 0; a < w; ++a) {
            const float x = xv[r + a];
            const bool pos = xm[r + a] && x > 0.f;
            s[a] += pos ? static_cast<double>(x) : 0.0;
            sxl[a] += pos ? static_cast<double>(x) * std::log(static_cast<double>(x)) : 0.0;
            np[a] += pos;
            detail::put(ov, om, r + a, np[a] >= 1 ? std::log(s[a]) - sxl[a] / s[a] : 0.0, true);
          }
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
  static auto carve(Bump &b, const Call &) {
    constexpr size_t W = kTileA, WX = kSpx ? W : 0, WY = kSpy ? W : 0;
    return std::tuple{b.take<int>(W), b.take<double>(W), b.take<double>(W), b.take<double>(W), b.take<double>(WX), b.take<double>(WY),
                      b.take<float>(WX), b.take<float>(WX), b.take<float>(WY), b.take<float>(WY)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, sx, sy, sxy, sxx, syy, lox, hix, loy, hiy] = carve(b, c);
    const int S = c.geo.S;
    assert(S >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      for (int seg = 0; seg < T; seg += S) {
        std::fill_n(n, w, 0);
        std::fill_n(sx, w, 0.0);
        std::fill_n(sy, w, 0.0);
        std::fill_n(sxy, w, 0.0);
        if constexpr (kSpx) {
          std::fill_n(sxx, w, 0.0);
          std::fill_n(lox, w, detail::kInf);
          std::fill_n(hix, w, -detail::kInf);
        }
        if constexpr (kSpy) {
          std::fill_n(syy, w, 0.0);
          std::fill_n(loy, w, detail::kInf);
          std::fill_n(hiy, w, -detail::kInf);
        }
        const int e = std::min(seg + S, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
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
            detail::pair_put<K>(ov, om, r + a, n[a], kSpx ? hix[a] > lox[a] : true,
                                kSpy ? hiy[a] > loy[a] : true, sx[a], sy[a], sxy[a],
                                kSpx ? sxx[a] : 0.0, kSpy ? syy[a] : 0.0, x, y, mm);
          }
        }
      }
    }
  }
};

// ---- WMean 的出口 (Cum / Roll 共用): Σy 相消 → 等权均值 Σx/n (权全相等的极限); 空 → 0 ----
inline double wmean_val(double sx, double sy, double syx, double say, int n) {
  return n < 1 ? 0.0 : (den_ok(sy, say) ? syx / sy : sx / n);
}

// ---- WMeanCum: y 为权 ----
struct TsWMeanCum {
  static auto carve(Bump &b, const Call &) {
    constexpr size_t W = kTileA;
    return std::tuple{b.take<int>(W), b.take<double>(W), b.take<double>(W), b.take<double>(W), b.take<double>(W)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, sx, sy, syx, say] = carve(b, c);
    const int S = c.geo.S;
    assert(S >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      for (int seg = 0; seg < T; seg += S) {
        std::fill_n(n, w, 0);
        std::fill_n(sx, w, 0.0);
        std::fill_n(sy, w, 0.0);
        std::fill_n(syx, w, 0.0);
        std::fill_n(say, w, 0.0);
        const int e = std::min(seg + S, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
            const float x = xv[r + a], y = yv[r + a];
            const bool mm = xm[r + a] && ym[r + a];
            sx[a] += mm ? static_cast<double>(x) : 0.0;
            sy[a] += mm ? static_cast<double>(y) : 0.0;
            syx[a] += mm ? static_cast<double>(y) * x : 0.0;
            say[a] += mm ? static_cast<double>(std::fabs(y)) : 0.0;
            n[a] += mm;
            detail::put(ov, om, r + a, wmean_val(sx[a], sy[a], syx[a], say[a], n[a]), true);
          }
        }
      }
    }
  }
};

// TsArgMaxCum / TsArgMinCum / TsMaxCum / TsMinCum 由 ExpExt 实例化 (见文件末尾)

// =============================================================================
// ROLL (23): 滑动加减 + van Herk 极值; 未满窗 (t < d−1) = 已有前缀, 累加器本来就只装了前缀, 无需判据
// =============================================================================

// ---- Delay / Delta: 纯移位取值, 滞后夹到序列头 (t < d 取第 0 行) ----
struct TsDelayRoll {
  CP_STATELESS {
    const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi);
    const int d = c.p.d;
    assert(d >= 1 && c.t0 == 0);
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A, q = static_cast<size_t>(std::max(t - d, 0)) * A;
      FACTOR_COLS for (int a = 0; a < w; ++a) {
        const bool m = xm[q + a] != 0;
        detail::put(ov, om, r + a, m ? static_cast<double>(xv[q + a]) : 0.0, m);
      }
    }
  }
};

struct TsDeltaRoll {
  CP_STATELESS {
    const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi);
    const int d = c.p.d;
    assert(d >= 1 && c.t0 == 0);
    for (int t = 0; t < T; ++t) {
      const size_t r = static_cast<size_t>(t) * A, q = static_cast<size_t>(std::max(t - d, 0)) * A;
      FACTOR_COLS for (int a = 0; a < w; ++a) {
        const bool m = xm[r + a] && xm[q + a];
        detail::put(ov, om, r + a, m ? static_cast<double>(xv[r + a]) - xv[q + a] : 0.0, m);
      }
    }
  }
};

// ---- Sum / Mean (Roll): 滑动加减 ----
template <bool Mean>
struct RollSum {
  static auto carve(Bump &b, const Call &) { return std::tuple{b.take<int>(kTileA), b.take<double>(kTileA)}; }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, s] = carve(b, c);
    const int d = c.p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      std::fill_n(n, w, 0);
      std::fill_n(s, w, 0.0);
      for (int t = 0; t < T; ++t) {
        if (t >= d) {
          const size_t q = static_cast<size_t>(t - d) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
            const bool m = xm[q + a] != 0;
            s[a] -= m ? static_cast<double>(xv[q + a]) : 0.0;
            n[a] -= m;
          }
        }
        const size_t r = static_cast<size_t>(t) * A;
        FACTOR_COLS for (int a = 0; a < w; ++a) {
          const bool m = xm[r + a] != 0;
          s[a] += m ? static_cast<double>(xv[r + a]) : 0.0;
          n[a] += m;
          detail::put(ov, om, r + a, Mean ? (n[a] >= 1 ? s[a] / n[a] : 0.0) : s[a], true);
        }
      }
    }
  }
};

// ---- Var / Std / Skew / Kurt / Z (Roll): 滑动加减幂和 + van Herk 精确 lo/hi; 退化 → 0 ----
template <detail::Mom K>
struct RollMom {
  static constexpr bool kS3 = K == detail::Mom::SKEW || K == detail::Mom::KURT;
  static constexpr bool kS4 = K == detail::Mom::KURT;
  static auto carve(Bump &b, const Call &c) {
    constexpr size_t W = kTileA;
    return std::tuple{b.take<int>(W), b.take<double>(W), b.take<double>(W), b.take<double>(kS3 ? W : 0), b.take<double>(kS4 ? W : 0),
                      detail::RollMinMax(b, c.p.d)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    auto [n, s1, s2, s3, s4, mm] = carve(b, c);
    const int d = c.p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      std::fill_n(n, w, 0);
      std::fill_n(s1, w, 0.0);
      std::fill_n(s2, w, 0.0);
      if constexpr (kS3)
        std::fill_n(s3, w, 0.0);
      if constexpr (kS4)
        std::fill_n(s4, w, 0.0);
      mm.reset(w);
      for (int t = 0; t < T; ++t) {
        const auto [lo, hi] = mm.step(xv, xm, nullptr, A, t);
        if (t >= d) {
          const size_t q0 = static_cast<size_t>(t - d) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
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
        FACTOR_COLS for (int a = 0; a < w; ++a) {
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
          const bool ok = detail::mom_ok<K>(n[a], hi[a] > lo[a]);
          detail::put(ov, om, r + a,
                      ok ? detail::mom_val<K>(s1[a], s2[a], kS3 ? s3[a] : 0.0, kS4 ? s4[a] : 0.0,
                                              n[a], x)
                         : 0.0,
                      K == detail::Mom::Z ? m : true); // Z 相对型: x_t 无效则无效
        }
      }
    }
  }
};

// ---- Max / Min / ArgMax / ArgMin (Roll): van Herk 极值 + 最旧并列位置; 空 → 0 ----
template <bool IsMax, bool IsArg>
struct RollExt {
  static auto carve(Bump &b, const Call &c) { return std::tuple{detail::RollArgExt<IsMax>(b, c.p.d), b.take<int>(kTileA)}; }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    auto [mm, n] = carve(b, c);
    const int d = c.p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      mm.reset(w);
      std::fill_n(n, w, 0);
      for (int t = 0; t < T; ++t) {
        const auto [best, arg] = mm.step(xv, xm, A, t);
        if (t >= d) {
          const size_t q = static_cast<size_t>(t - d) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a)
              n[a] -= xm[q + a] != 0;
        }
        const size_t r = static_cast<size_t>(t) * A;
        FACTOR_COLS for (int a = 0; a < w; ++a) {
          n[a] += xm[r + a] != 0;
          const double v =
              IsArg ? static_cast<double>(t - arg[a]) : static_cast<double>(best[a]);
          detail::put(ov, om, r + a, n[a] >= 1 ? v : 0.0, true);
        }
      }
    }
  }
};

// ---- Rank / Quantile (Roll): 环形窗缓存 + 直方图 (lo/hi 未变增量改桶, 变了整窗重建) ----
enum class HistK { RANK,
                   QUANTILE };

template <HistK K>
struct RollHist {
  static auto carve(Bump &b, const Call &c) {
    constexpr size_t W = kTileA;
    const size_t dw = static_cast<size_t>(c.p.d) * W;
    return std::tuple{b.take<float>(dw), b.take<uint8_t>(dw), b.take<int>(W * kBuckets), b.take<int>(W), b.take<float>(W), b.take<float>(W),
                      detail::RollMinMax(b, c.p.d)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump bp{static_cast<char *>(ws)};
    auto [rv, rm, hist, n, plo, phi, mm] = carve(bp, c);
    const Param &p = c.p;
    const int d = p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, W] = detail::tile(c, a0, rg.hi);
      mm.reset(W);
      std::fill_n(rv, static_cast<size_t>(d) * W, 0.f);
      std::fill_n(rm, static_cast<size_t>(d) * W, 0);
      std::fill_n(n, W, 0); // hist 不清: 先有 spread 才读, 而进入 spread 必重建
      std::fill_n(plo, W, detail::kInf);
      std::fill_n(phi, W, -detail::kInf);
      for (int t = 0; t < T; ++t) {
        const auto [lo, hi] = mm.step(xv, xm, nullptr, A, t);
        const int slot = t % d;
        const size_t r = static_cast<size_t>(t) * A;
        for (int j = 0; j < W; ++j) {
          const float x = xv[r + j];
          const bool m = xm[r + j] != 0;
          const size_t rs = static_cast<size_t>(slot) * W + j;
          const float xo = rv[rs];
          const bool mo = rm[rs] != 0;
          rv[rs] = x;
          rm[rs] = m ? 1 : 0;
          n[j] += static_cast<int>(m) - static_cast<int>(mo);
          const float nl = lo[j], nh = hi[j];
          int *h = hist + static_cast<size_t>(j) * kBuckets;
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
            double v = 0.0; // 相对型: x_t 无效则无效; x_t 有效 ⇒ n ≥ 1; 全并列 → 0.5
            if (m) {
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
            detail::put(ov, om, r + j, v, m);
          } else { // QUANTILE: 空窗 → 0
            const double v = n[j] >= 1 ? detail::hist_quantile(h, n[j], nl, nh, p.k) : 0.0;
            detail::put(ov, om, r + j, v, true);
          }
        }
      }
    }
  }
};

// ---- WmaRoll: 线性权 (最旧 1 … 最新 d); 滑动时全体权重 −1 (平移), 新样本以权 d 入窗 ----
struct TsWmaRoll {
  static auto carve(Bump &b, const Call &) { return std::tuple{b.take<int>(kTileA), b.take<double>(kTileA), b.take<double>(kTileA), b.take<double>(kTileA)}; }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, sx, sw, swx] = carve(b, c);
    const int d = c.p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      std::fill_n(n, w, 0);
      std::fill_n(sx, w, 0.0);
      std::fill_n(sw, w, 0.0);
      std::fill_n(swx, w, 0.0);
      for (int t = 0; t < T; ++t) {
        FACTOR_COLS for (int a = 0; a < w; ++a) { // 平移: Σw −= n, Σwx −= Σx
          sw[a] -= n[a];
          swx[a] -= sx[a];
        }
        if (t >= d) { // 滑出样本权重已平移到 0, 只影响 n / Σx
          const size_t q = static_cast<size_t>(t - d) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
            const bool mo = xm[q + a] != 0;
            n[a] -= mo;
            sx[a] -= mo ? static_cast<double>(xv[q + a]) : 0.0;
          }
        }
        const size_t r = static_cast<size_t>(t) * A;
        FACTOR_COLS for (int a = 0; a < w; ++a) {
          const float x = xv[r + a];
          const bool m = xm[r + a] != 0;
          n[a] += m;
          sx[a] += m ? static_cast<double>(x) : 0.0;
          sw[a] += m ? static_cast<double>(d) : 0.0;
          swx[a] += m ? static_cast<double>(d) * x : 0.0;
          detail::put(ov, om, r + a, n[a] >= 1 ? swx[a] / sw[a] : 0.0, true); // n ≥ 1 ⇒ Σw ≥ 1; 空 → 0
        }
      }
    }
  }
};

// ---- SlopeRoll: OLS 斜率; 滑动时窗内下标全体 −1 (平移), 下标和/平方和是整数值 double, 平移精确 ----
struct TsSlopeRoll {
  static auto carve(Bump &b, const Call &) {
    constexpr size_t W = kTileA;
    return std::tuple{b.take<int>(W), b.take<double>(W), b.take<double>(W), b.take<double>(W), b.take<double>(W)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, si, sii, sx, six] = carve(b, c);
    const int d = c.p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      std::fill_n(n, w, 0);
      std::fill_n(si, w, 0.0);
      std::fill_n(sii, w, 0.0);
      std::fill_n(sx, w, 0.0);
      std::fill_n(six, w, 0.0);
      for (int t = 0; t < T; ++t) {
        FACTOR_COLS for (int a = 0; a < w; ++a) { // 平移: Σ(i−1)² = Σi² − 2Σi + n
          sii[a] -= 2.0 * si[a] - n[a];
          si[a] -= n[a];
          six[a] -= sx[a];
        }
        if (t >= d) { // 滑出样本平移后下标 = −1
          const size_t q = static_cast<size_t>(t - d) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
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
        const double fi = d - 1; // 新样本的窗内下标
        FACTOR_COLS for (int a = 0; a < w; ++a) {
          const bool m = xm[r + a] != 0;
          const double dx = m ? static_cast<double>(xv[r + a]) : 0.0;
          si[a] += m ? fi : 0.0;
          sii[a] += m ? fi * fi : 0.0;
          sx[a] += dx;
          six[a] += fi * dx;
          n[a] += m;
          const bool ok = n[a] >= 2; // n < 2 → 0 (无趋势)
          double v = 0.0;
          if (ok)
            v = (six[a] - si[a] * sx[a] / n[a]) / (sii[a] - si[a] * si[a] / n[a]);
          detail::put(ov, om, r + a, v, true);
        }
      }
    }
  }
};

// ---- ProductRoll: Π(1+x) − 1 精确. 滑动加减 Σ ln|1+x| (log 确定性 ⇒ 出窗减去的正是入窗加上的),
//      零因子 (x = −1) 计数, 负因子 (x < −1) 计奇偶; 空窗 = 空积 − 1 = 0 ----
//   ln|1+x| 的取值与 Stream 同式: x > −1 用 log1p(x), x < −1 用 log(−1 − x), x = −1 不进对数
inline double log_abs1p(float x) { return x > -1.f ? std::log1p(static_cast<double>(x)) : (x < -1.f ? std::log(-1.0 - x) : 0.0); }
struct TsProductRoll {
  static auto carve(Bump &b, const Call &) { return std::tuple{b.take<int>(kTileA), b.take<int>(kTileA), b.take<double>(kTileA)}; }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [zero, neg, sl] = carve(b, c);
    const int d = c.p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      std::fill_n(zero, w, 0);
      std::fill_n(neg, w, 0);
      std::fill_n(sl, w, 0.0);
      for (int t = 0; t < T; ++t) {
        if (t >= d) {
          const size_t q = static_cast<size_t>(t - d) * A;
          for (int a = 0; a < w; ++a) {
            const float xo = xv[q + a];
            const bool mo = xm[q + a] != 0;
            sl[a] -= mo ? log_abs1p(xo) : 0.0;
            zero[a] -= mo && xo == -1.f;
            neg[a] -= mo && xo < -1.f;
          }
        }
        const size_t r = static_cast<size_t>(t) * A;
        for (int a = 0; a < w; ++a) {
          const float x = xv[r + a];
          const bool m = xm[r + a] != 0;
          sl[a] += m ? log_abs1p(x) : 0.0;
          zero[a] += m && x == -1.f;
          neg[a] += m && x < -1.f;
          const double v = zero[a] > 0 ? -1.0 : (neg[a] & 1 ? -std::exp(sl[a]) - 1.0 : std::expm1(sl[a]));
          detail::put(ov, om, r + a, v, true);
        }
      }
    }
  }
};

// ---- Cov / Corr / Beta / Resid (Roll): 滑动加减二元幂和 + van Herk 双有效极值 ----
template <detail::Pair K>
struct RollPair {
  static constexpr bool kSpx = K == detail::Pair::CORR;
  static constexpr bool kSpy = K != detail::Pair::COV;
  static auto carve(Bump &b, const Call &c) {
    constexpr size_t W = kTileA;
    return std::tuple{b.take<int>(W), b.take<double>(W), b.take<double>(W), b.take<double>(W), b.take<double>(kSpx ? W : 0), b.take<double>(kSpy ? W : 0),
                      kSpx ? detail::RollMinMax(b, c.p.d) : detail::RollMinMax(), // 未用的实例不切不 step
                      kSpy ? detail::RollMinMax(b, c.p.d) : detail::RollMinMax()};
  }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    auto [n, sx, sy, sxy, sxx, syy, mmx, mmy] = carve(b, c);
    const int d = c.p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      std::fill_n(n, w, 0);
      std::fill_n(sx, w, 0.0);
      std::fill_n(sy, w, 0.0);
      std::fill_n(sxy, w, 0.0);
      if constexpr (kSpx) {
        std::fill_n(sxx, w, 0.0);
        mmx.reset(w);
      }
      if constexpr (kSpy) {
        std::fill_n(syy, w, 0.0);
        mmy.reset(w);
      }
      for (int t = 0; t < T; ++t) {
        detail::Win ex, ey;
        if constexpr (kSpx)
          ex = mmx.step(xv, xm, ym, A, t);
        if constexpr (kSpy)
          ey = mmy.step(yv, ym, xm, A, t);
        if (t >= d) {
          const size_t q = static_cast<size_t>(t - d) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
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
        FACTOR_COLS for (int a = 0; a < w; ++a) {
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
          detail::pair_put<K>(ov, om, r + a, n[a], kSpx ? ex.hi[a] > ex.lo[a] : true,
                              kSpy ? ey.hi[a] > ey.lo[a] : true, sx[a], sy[a], sxy[a],
                              kSpx ? sxx[a] : 0.0, kSpy ? syy[a] : 0.0, x, y, mm);
        }
      }
    }
  }
};

// ---- WMeanRoll ----
struct TsWMeanRoll {
  static auto carve(Bump &b, const Call &) {
    constexpr size_t W = kTileA;
    return std::tuple{b.take<int>(W), b.take<double>(W), b.take<double>(W), b.take<double>(W), b.take<double>(W)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, sx, sy, syx, say] = carve(b, c);
    const int d = c.p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      std::fill_n(n, w, 0);
      std::fill_n(sx, w, 0.0);
      std::fill_n(sy, w, 0.0);
      std::fill_n(syx, w, 0.0);
      std::fill_n(say, w, 0.0);
      for (int t = 0; t < T; ++t) {
        if (t >= d) {
          const size_t q = static_cast<size_t>(t - d) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
            const float x = xv[q + a], y = yv[q + a];
            const bool mm = xm[q + a] && ym[q + a];
            sx[a] -= mm ? static_cast<double>(x) : 0.0;
            sy[a] -= mm ? static_cast<double>(y) : 0.0;
            syx[a] -= mm ? static_cast<double>(y) * x : 0.0;
            say[a] -= mm ? static_cast<double>(std::fabs(y)) : 0.0;
            n[a] -= mm;
          }
        }
        const size_t r = static_cast<size_t>(t) * A;
        FACTOR_COLS for (int a = 0; a < w; ++a) {
          const float x = xv[r + a], y = yv[r + a];
          const bool mm = xm[r + a] && ym[r + a];
          sx[a] += mm ? static_cast<double>(x) : 0.0;
          sy[a] += mm ? static_cast<double>(y) : 0.0;
          syx[a] += mm ? static_cast<double>(y) * x : 0.0;
          say[a] += mm ? static_cast<double>(std::fabs(y)) : 0.0;
          n[a] += mm;
          detail::put(ov, om, r + a, wmean_val(sx[a], sy[a], syx[a], say[a], n[a]), true);
        }
      }
    }
  }
};

// =============================================================================
// EXPO (1): 全程递推, 不按段重置
// =============================================================================
// TsMeanEma: x 有效时 y = 已出现过有效值 ? k·x + (1−k)·y : x; x 无效时 y 不动; 尚无有效样本 → 0
struct TsMeanEma {
  static auto carve(Bump &b, const Call &) { return std::tuple{b.take<double>(kTileA), b.take<uint8_t>(kTileA)}; }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [y, on] = carve(b, c);
    assert(c.p.k > 0.f && c.p.k <= 1.f);
    const double k = c.p.k;
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      std::fill_n(y, w, 0.0);
      std::fill_n(on, w, 0);
      for (int t = 0; t < T; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        FACTOR_COLS for (int a = 0; a < w; ++a) {
          const bool m = xm[r + a] != 0;
          const double x = xv[r + a];
          y[a] = m ? (on[a] ? k * x + (1.0 - k) * y[a] : x) : y[a];
          on[a] |= m;
          detail::put(ov, om, r + a, y[a], true);
        }
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
using TsQuantileRoll = RollHist<HistK::QUANTILE>;
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
#undef CP_STATELESS
#undef CP_SCRATCH
#undef CP_RUN

} // namespace factor::cpu::ts
