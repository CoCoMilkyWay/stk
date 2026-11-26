#pragma once

// =============================================================================
// TS 算子的挖掘 CPU 后端 (factor::cpu::ts, 62 个, 对应 OpTable 的 OP_TS0..3 + OP_TSN)
// =============================================================================
//   定位: 因子挖掘无 GPU 时的 fallback —— 整张量批量直算, 不逐点因果推进, 吞吐必须 ≥ stream.
//   语义与流式一致 (掩码逐位相等, 值在对拍容差内); 独立第二实现, **不得** include 任何
//   Stream/Gpu 头, 不复用其算法代码 (否则 cpu↔stream 对拍空转).
//
//   【布局与向量化】SoA 行主序 [T][A]: 内层 A 连续, 一切按"行推进 + 跨资产状态数组"组织,
//   内层循环 branchless select 交给自动向量化. 有状态算子按资产列块 (kTileA 列) 推进: 状态数组按块容量
//   从调用方暂存切出 (scratch 声明, 驻留 L1/L2), 算子内部不分配.
//
//   【算法】(按定义是 O(T·d·A) 重算, 本文件全部摊到 O(T·A) 或 O(T·A·B)):
//     POINT   单遍逐元素
//     EXPAND  沿 t 递推累加 (double 幂和), 段界 reset; 秩 = 段内样本缓存上的精确计数 (O(S), 列分块寄存器累加)
//     ROLL    滑动加减幂和 (double); spread 判据 = 窗内最近变化对 (O(1), 精确); 极值 = van Herk 分块前后缀滑窗极值;
//             秩 = 环形窗缓存上的精确计数 (O(d), 列分块寄存器累加); 分位 = 每列一条有序窗 (出旧入新合成一次搬移, 计数定位, 取第 q_rank 名)
//     EXPO    沿 t 递推
//
//   【数值】滑动加减幂和相对整窗重算有 O(ε_double) 漂移; 近 ulp 简并窗 (真中心矩 ≈ double 舍入噪声)
//   下值可能与流式不一致 —— 与 GPU SCAN 后端同一风险等级, 契约的造数与容差刻意避开该带.
//   高阶矩 (Std / Skew / Kurt / Z) 的幂和累的是 x − ref (ref = 窗 / 段空后进来的首个有效值; 窗非空时不变, 出窗样本按入窗时的 ref 减):
//   中心矩对平移不变, 幂和的量级从"水平" (价格 1500 的 s4 ~ 1e15) 降到"离 ref 的偏差", 相消误差随之降到与离散度同量级.
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
#include <type_traits>

#include "factor/Contract.hpp"

namespace factor::cpu {
inline constexpr int kTileA = 128; // TS 有状态算子的资产列块宽 (状态容量; evaluator 的 TS 任务宽以它为上限)
} // namespace factor::cpu

namespace factor::cpu::ts {

// -----------------------------------------------------------------------------
// detail: 列块 / 输出 / 幂和→中心矩 / 滑窗全并列判据 / 滑窗极值 (van Herk)
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

// ---- 滑窗全并列判据 (契约第 2 条 spread = hi > lo 的 O(1) 等价式; Std / Skew / Kurt / Z / Corr / Beta / Resid 的 Roll) ----
//   窗内有效样本不全相等 ⟺ 时序上相邻的两个有效样本都落在窗内且值不等. 这种 "变化对" 按后者行号单调,
//   故只记最近一个变化对前者的行号 pc: 窗 [t−d+1, t] 有离散度 ⟺ pc > t − d (未满窗的前缀窗同式).
//   判不等用 float != (±0 相等, 与 hi > lo 逐格同判), 精确无阈值. 每列三个数: 末个有效值 lv / 其行号 li (−1 = 尚无) / pc (kNoChg = 尚无);
//   状态按列块容量 kTileA 从 Bump 切, 换块 chg_reset. 调用方在列循环里逐格 chg_step (数组先落局部, 理由同下)
inline constexpr int kNoChg = std::numeric_limits<int>::min();
inline void chg_reset(float *lv, int *li, int *pc, int w) {
  std::fill_n(lv, w, 0.f);
  std::fill_n(li, w, -1);
  std::fill_n(pc, w, kNoChg);
}
// 列 a 喂行 t 的一格 (m = 计入样本); 返回窗 [t−d+1, t] 有无离散度
inline bool chg_step(float *lv, int *li, int *pc, int a, float x, bool m, int t, int d) {
  pc[a] = m && li[a] >= 0 && x != lv[a] ? li[a] : pc[a];
  lv[a] = m ? x : lv[a];
  li[a] = m ? t : li[a];
  return pc[a] > t - d;
}

// ---- 滑窗极值 (van Herk/Gil–Werman 分块前后缀): 每行均摊 O(1) 产出窗内有效样本的极值 (TsMaxRoll / TsMinRoll) ----
//   块长 = 窗长 w; 进入新块时回读前一块 w 行建后缀极值 (读放大 2×), 前缀在线递推. 无有效样本 → 哨兵 (由调用方的 n 屏蔽).
//   状态按列块容量 kTileA 从 Bump 切; 换块 reset(width) 即可 (t = 0 的重建与首行写入盖住全部状态, 不用预填).
//   成员先落局部再进列循环 (经 this 读的界 / 指针会被当成可能被循环体改写)
template <bool IsMax>
class RollExtV {
public:
  RollExtV() = default;
  RollExtV(Bump &b, int w) : w_(w) {
    assert(w >= 1);
    const size_t cap = static_cast<size_t>(kTileA);
    sv_ = b.take<float>(static_cast<size_t>(w + 1) * cap);
    pv_ = b.take<float>(cap);
    best_ = b.take<float>(cap);
  }
  void reset(int width) {
    assert(1 <= width && width <= kTileA);
    n_ = width;
  }
  // 按 t = 0, 1, … 递增喂行 (xv/xm 为块首列指针, 行距 stride, 列 [0, width)); 返回窗 [t−w+1, t] 极值 [width] (下一次 step 前有效)
  const float *step(const float *xv, const uint8_t *xm, int stride, int t) {
    const int pos = t % w_, n = n_;
    if (pos == 0)
      rebuild(xv, xm, stride, t);
    const size_t r = static_cast<size_t>(t) * stride;
    const float *sv = sv_ + static_cast<size_t>(pos + 1) * n;
    float *pv = pv_, *best = best_;
    FACTOR_COLS for (int a = 0; a < n; ++a) {
      const float v = xm[r + a] ? xv[r + a] : kSent;
      pv[a] = pos == 0 ? v : ext(pv[a], v);
      best[a] = ext(sv[a], pv[a]);
    }
    return best;
  }

private:
  static constexpr float kSent = IsMax ? -kInf : kInf;
  static float ext(float a, float b) { return IsMax ? std::fmax(a, b) : std::fmin(a, b); }
  void rebuild(const float *xv, const uint8_t *xm, int stride, int t) {
    const int w = w_, n = n_;
    std::fill_n(sv_ + static_cast<size_t>(w) * n, n, kSent); // 下标 w = 空后缀哨兵
    for (int i = w - 1; i >= 0; --i) {
      float *cv = sv_ + static_cast<size_t>(i) * n;
      const float *nv = cv + n;
      const int row = t - w + i;
      if (row < 0) { // 平面开头: 空行
        std::copy_n(nv, n, cv);
        continue;
      }
      const size_t r = static_cast<size_t>(row) * stride;
      FACTOR_COLS for (int a = 0; a < n; ++a) cv[a] = ext(xm[r + a] ? xv[r + a] : kSent, nv[a]);
    }
  }
  int w_ = 0, n_ = 0;
  float *sv_ = nullptr, *pv_ = nullptr, *best_ = nullptr;
};

// ---- 滑窗极值 + 最旧并列位置 (契约: 严格比较 → 并列保留最旧) ----
//   后缀 (窗的旧半) 反向扫取 ≥/≤ (旧优先), 前缀正向扫取 >/< (新不夺旧), 合并时后缀平局优先.
//   状态切分 / 换块 / 成员落局部同 RollExtV.
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

// ---- 一元矩核的退化判据与求值 (EXPAND 与 ROLL 共用公式, 窗形只是状态推进方式不同); 退化 → 0 ----
enum class Mom { STD,
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
    return n >= 2 && sp; // STD / Z
}

// s1..s4 = (x − ref) 的幂和, x = 当前点的 x − ref (只 Z 用)
template <Mom K>
inline double mom_val(double s1, double s2, double s3, double s4, int n, double x) {
  (void)s3, (void)s4, (void)x;
  const double m2 = std::fmax(c2_of(s1, s2, n), 0.0);
  if constexpr (K == Mom::STD)
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
// POINT (23 = 定元 22 + N 元 1): 逐点无状态, 单遍. 表达式与流式严格同式 (三后端要位级一致).
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

// 标量版 (式内有 log / pow 等无向量版的 libm 调用): 向量化器会把它逐 lane 标量化再 insert 拼回, 比纯标量循环还慢, 故显式关掉
#define CP_P1S(Name, V, M)                                                                                            \
  struct Name {                                                                                                       \
    CP_STATELESS {                                                                                                    \
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi);                           \
      for (int t = 0; t < T; ++t)                                                                                     \
        _Pragma("clang loop vectorize(disable)") for (size_t i = static_cast<size_t>(t) * A, e = i + w; i < e; ++i) { \
          const float x = xv[i];                                                                                      \
          const bool mx = xm[i] != 0;                                                                                 \
          const bool m = (M);                                                                                         \
          detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m);                                                \
        }                                                                                                             \
    }                                                                                                                 \
  };

#define CP_P2S(Name, V, M)                                                                                            \
  struct Name {                                                                                                       \
    CP_STATELESS {                                                                                                    \
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi);                           \
      for (int t = 0; t < T; ++t)                                                                                     \
        _Pragma("clang loop vectorize(disable)") for (size_t i = static_cast<size_t>(t) * A, e = i + w; i < e; ++i) { \
          const float x = xv[i], y = yv[i];                                                                           \
          const bool mx = xm[i] != 0, my = ym[i] != 0;                                                                \
          const bool m = (M);                                                                                         \
          detail::put(ov, om, i, m ? static_cast<double>(V) : 0.0, m);                                                \
        }                                                                                                             \
    }                                                                                                                 \
  };

CP_P1(TsAbs, std::fabs(x), mx)
CP_P1(TsSign, x > 0.f ? 1.f : (x < 0.f ? -1.f : 0.f), mx)
CP_P1(TsSqrt, std::copysign(std::sqrt(std::fabs(x)), x), mx)

CP_P1S(TsLog, std::copysign(std::log1p(std::fabs(x)), x), mx)
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

// TsConst: 元数 0, 不读输入指针; 整格写常数 k
struct TsConst {
  CP_STATELESS {
    const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::span(c, rg.lo, rg.hi);
    const double k = c.p.k;
    for (int t = 0; t < T; ++t)
      FACTOR_COLS for (size_t i = static_cast<size_t>(t) * A, e = i + w; i < e; ++i) detail::put(ov, om, i, k, true);
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
// TsPow: sign(x)·|x|^y, x = 0 → 0 (含负次幂); 溢出由 mk 转无效. float 版 pow (输入输出本是 float, 比 double 版快数倍, 误差 ~1 ulp 远在容差内)
CP_P2S(TsPow, x != 0.f ? std::copysign(std::pow(std::fabs(x), y), x) : 0.f, mx &&my)
// TsImb: 和相消 (scale = |x| + |y|) → 0, 分母不钳位
CP_P2(TsImb, den_ok(static_cast<double>(x) + y, std::fabs(x) + std::fabs(y)) ? (x - y) / (x + y) : 0.f, mx &&my)
// TsLogRatio: 非正侧饱和到 ∓kLogCap (见 Contract::log_ratio)
CP_P2S(TsLogRatio, log_ratio(x, y), mx &&my)
// TsMask: y 当掩码, y ≤ 0 → 无效 (不是补 0), 让下游窗只吃子集
CP_P2(TsMask, x, mx && my && y > 0.f)
// TsWhere: 未被选中的那支不要求有效
CP_P3(TsWhere, x > 0.f ? y : z, mx && (x > 0.f ? my : mz))

// TsLinFix: xs[0..n) 特征元, 权 p.w[0..n) (Contract【N 元】). 每行按 kTileA 列切块, 栈上 double 行缓冲按元序累加 (与流式同序),
// 掩码逐元求与; 每元一趟连续列循环 (向量化)
struct TsLinFix {
  CP_STATELESS {
    assert(c.n >= 2 && c.n <= kLrN && c.o.v && c.o.m && 0 <= rg.lo && rg.lo <= rg.hi && rg.hi <= c.A && "至少 2 个特征元");
    const int n = c.n, A = c.A;
    double acc[kTileA];
    uint8_t ok[kTileA];
    for (int t = 0; t < c.T; ++t)
      for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
        const int cols = std::min(kTileA, rg.hi - a0);
        const size_t r = static_cast<size_t>(t) * A + a0;
        std::fill_n(acc, cols, 0.0);
        std::fill_n(ok, cols, uint8_t{1});
        for (int i = 0; i < n; ++i) {
          const double wi = static_cast<double>(c.p.w[i]);
          const float *xv = c.xs[i].v + r;
          const uint8_t *xm = c.xs[i].m + r;
          FACTOR_COLS for (int a = 0; a < cols; ++a) {
            acc[a] += wi * xv[a];
            ok[a] = static_cast<uint8_t>(ok[a] & static_cast<uint8_t>(xm[a] != 0));
          }
        }
        float *ov = c.o.v + r;
        uint8_t *om = c.o.m + r;
        FACTOR_COLS for (int a = 0; a < cols; ++a) detail::put(ov, om, static_cast<size_t>(a), ok[a] ? acc[a] : 0.0, ok[a] != 0);
      }
  }
};

// =============================================================================
// EXPAND (17): 沿 t 递推, 段界 reset. 状态 = 跨资产数组.
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

// ---- Var / Std / Skew / Kurt: (x − ref) 幂和递推 (ref = 段内首个有效值), lo/hi 段内只扩不缩 (精确 spread); 退化 → 0 ----
template <detail::Mom K>
struct ExpMom {
  static constexpr bool kS3 = K == detail::Mom::SKEW || K == detail::Mom::KURT;
  static constexpr bool kS4 = K == detail::Mom::KURT;
  static auto carve(Bump &b, const Call &) {
    constexpr size_t W = kTileA;
    return std::tuple{b.take<int>(W), b.take<double>(W), b.take<double>(W), b.take<double>(kS3 ? W : 0), b.take<double>(kS4 ? W : 0),
                      b.take<float>(W), b.take<float>(W), b.take<double>(W)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, s1, s2, s3, s4, lo, hi, ref] = carve(b, c);
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
        std::fill_n(ref, w, 0.0);
        const int e = std::min(seg + S, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
            const float x = xv[r + a];
            const bool m = xm[r + a] != 0;
            ref[a] = n[a] == 0 && m ? static_cast<double>(x) : ref[a];
            const double dx = m ? static_cast<double>(x) - ref[a] : 0.0;
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
                                                n[a], static_cast<double>(x) - ref[a])
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

// ---- RankCum / RankRoll 的精确计数 (契约【序统计】): 窗内 s 行样本 rv ([s][W] 行主) 对本行 x 数 less / eq; 全并列 → pct_of 自然给 0.5.
//   无效样本在缓存里记 NaN (契约: 有效值恒有限), 与任何 x 比 < / == 都为假 —— 计数不再读掩码.
//   列按 kRankB 一块, 块内 s 在外、列在内: 块的 lt / eq 是定长局部数组, 整趟 s 驻寄存器 (不逐行回写内存).
//   相对型: x_t 无效则无效 (x_t 有效 ⇒ 样本含它自身) ----
namespace detail {
inline constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
inline constexpr int kRankB = 32;
template <int N> // N > 0: 恰 N 列 (编译期定长); N = 0: nb 列 (列块尾)
inline void rank_cols(const float *rv, int rows, int W, const float *x, int nb, int *lt, int *eq) {
  constexpr int C = N > 0 ? N : kRankB;
  const int m = N > 0 ? N : nb;
  float xb[C];
  int l[C], e[C];
  for (int j = 0; j < m; ++j)
    xb[j] = x[j], l[j] = 0, e[j] = 0;
  for (int s = 0; s < rows; ++s) {
    const float *sv = rv + static_cast<size_t>(s) * W;
    FACTOR_COLS for (int j = 0; j < m; ++j) {
      l[j] += sv[j] < xb[j];
      e[j] += sv[j] == xb[j];
    }
  }
  for (int j = 0; j < m; ++j)
    lt[j] = l[j], eq[j] = e[j];
}
inline void rank_rows(const float *rv, int rows, const float *x, int W, int *lt, int *eq) {
  int j0 = 0;
  for (; j0 + kRankB <= W; j0 += kRankB)
    rank_cols<kRankB>(rv + j0, rows, W, x + j0, kRankB, lt + j0, eq + j0);
  if (j0 < W)
    rank_cols<0>(rv + j0, rows, W, x + j0, W - j0, lt + j0, eq + j0);
}
} // namespace detail

// ---- RankCum: 段内样本按段内位置缓存, 每行整段精确计数 ----
struct TsRankCum {
  static auto carve(Bump &b, const Call &c) {
    constexpr size_t W = kTileA;
    const size_t sw = W * static_cast<size_t>(c.geo.S);
    return std::tuple{b.take<float>(sw), b.take<int>(W), b.take<int>(W), b.take<int>(W)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump bp{static_cast<char *>(ws)};
    const auto [rv, n, lt, eq] = carve(bp, c);
    const int S = c.geo.S;
    assert(S >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, W] = detail::tile(c, a0, rg.hi);
      for (int seg = 0; seg < T; seg += S) {
        std::fill_n(n, W, 0);
        const int e = std::min(seg + S, T);
        for (int t = seg; t < e; ++t) {
          const size_t r = static_cast<size_t>(t) * A, q = static_cast<size_t>(t - seg) * W;
          FACTOR_COLS for (int j = 0; j < W; ++j) {
            const bool m = xm[r + j] != 0;
            rv[q + j] = m ? xv[r + j] : detail::kNaN;
            n[j] += m;
          }
          detail::rank_rows(rv, t - seg + 1, xv + r, W, lt, eq);
          for (int j = 0; j < W; ++j)
            detail::put(ov, om, r + j, xm[r + j] ? pct_of(lt[j], eq[j], n[j]) : 0.0, xm[r + j]);
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
          // 体内有 log (libm), 同 CP_P1S 显式关向量化; x ln x 的 ln 取 float 版 (输入本是 float, 比 double 版快数倍, 精度远在容差内)
          _Pragma("clang loop vectorize(disable)") for (int a = 0; a < w; ++a) {
            const float x = xv[r + a];
            const bool pos = xm[r + a] && x > 0.f;
            s[a] += pos ? static_cast<double>(x) : 0.0;
            sxl[a] += pos ? static_cast<double>(x) * static_cast<double>(std::log(x)) : 0.0;
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
// ROLL (21): 滑动加减 + 变化点全并列判据 + van Herk 极值; 未满窗 (t < d−1) = 已有前缀, 累加器本来就只装了前缀, 无需判据
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

// ---- Var / Std / Skew / Kurt / Z (Roll): 滑动加减 (x − ref) 幂和 + 变化点全并列判据 (detail::chg_step); 退化 → 0 ----
//   ref 只在窗空时换 (窗内样本都按同一个 ref 入、出); 窗空即把幂和清成精确 0, 不留滑动残差
template <detail::Mom K>
struct RollMom {
  static constexpr bool kS3 = K == detail::Mom::SKEW || K == detail::Mom::KURT;
  static constexpr bool kS4 = K == detail::Mom::KURT;
  static auto carve(Bump &b, const Call &) {
    constexpr size_t W = kTileA;
    return std::tuple{b.take<int>(W), b.take<double>(W), b.take<double>(W), b.take<double>(kS3 ? W : 0), b.take<double>(kS4 ? W : 0),
                      b.take<double>(W), b.take<float>(W), b.take<int>(W), b.take<int>(W)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, s1, s2, s3, s4, ref, lv, li, pc] = carve(b, c);
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
      std::fill_n(ref, w, 0.0);
      detail::chg_reset(lv, li, pc, w);
      for (int t = 0; t < T; ++t) {
        if (t >= d) {
          const size_t q0 = static_cast<size_t>(t - d) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a) {
            const bool m = xm[q0 + a] != 0;
            const double dx = m ? static_cast<double>(xv[q0 + a]) - ref[a] : 0.0;
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
          const bool empty = n[a] == 0;
          ref[a] = empty && m ? static_cast<double>(x) : ref[a];
          const double dx = m ? static_cast<double>(x) - ref[a] : 0.0;
          const double q = dx * dx;
          s1[a] = (empty ? 0.0 : s1[a]) + dx;
          s2[a] = (empty ? 0.0 : s2[a]) + q;
          if constexpr (kS3)
            s3[a] = (empty ? 0.0 : s3[a]) + q * dx;
          if constexpr (kS4)
            s4[a] = (empty ? 0.0 : s4[a]) + q * q;
          n[a] += m;
          const bool sp = detail::chg_step(lv, li, pc, a, x, m, t, d);
          const bool ok = detail::mom_ok<K>(n[a], sp);
          detail::put(ov, om, r + a,
                      ok ? detail::mom_val<K>(s1[a], s2[a], kS3 ? s3[a] : 0.0, kS4 ? s4[a] : 0.0,
                                              n[a], static_cast<double>(x) - ref[a])
                         : 0.0,
                      K == detail::Mom::Z ? m : true); // Z 相对型: x_t 无效则无效
        }
      }
    }
  }
};

// ---- Max / Min / ArgMax / ArgMin (Roll): van Herk 极值 (Arg 另带最旧并列位置); 空 → 0 ----
template <bool IsMax, bool IsArg>
struct RollExt {
  using Win = std::conditional_t<IsArg, detail::RollArgExt<IsMax>, detail::RollExtV<IsMax>>;
  static auto carve(Bump &b, const Call &c) { return std::tuple{Win(b, c.p.d), b.take<int>(kTileA)}; }
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
        [[maybe_unused]] const float *best;
        [[maybe_unused]] const int *arg = nullptr;
        if constexpr (IsArg) {
          const detail::ArgWin e = mm.step(xv, xm, A, t);
          best = e.best, arg = e.arg;
        } else
          best = mm.step(xv, xm, A, t);
        if (t >= d) {
          const size_t q = static_cast<size_t>(t - d) * A;
          FACTOR_COLS for (int a = 0; a < w; ++a)
              n[a] -= xm[q + a] != 0;
        }
        const size_t r = static_cast<size_t>(t) * A;
        FACTOR_COLS for (int a = 0; a < w; ++a) {
          n[a] += xm[r + a] != 0;
          double v;
          if constexpr (IsArg)
            v = static_cast<double>(t - arg[a]);
          else
            v = static_cast<double>(best[a]);
          detail::put(ov, om, r + a, n[a] >= 1 ? v : 0.0, true);
        }
      }
    }
  }
};

// ---- RankRoll: 环形窗缓存 (槽 = t % d), 每行整窗精确计数; 未满窗只数已写的前 t + 1 槽 (即已有前缀) ----
struct TsRankRoll {
  static auto carve(Bump &b, const Call &c) {
    constexpr size_t W = kTileA;
    const size_t dw = static_cast<size_t>(c.p.d) * W;
    return std::tuple{b.take<float>(dw), b.take<int>(W), b.take<int>(W), b.take<int>(W)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump bp{static_cast<char *>(ws)};
    const auto [rv, n, lt, eq] = carve(bp, c);
    const int d = c.p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, W] = detail::tile(c, a0, rg.hi);
      std::fill_n(n, W, 0);
      for (int t = 0; t < T; ++t) {
        const size_t r = static_cast<size_t>(t) * A, q = static_cast<size_t>(t % d) * W;
        if (t >= d) {
          const size_t q0 = static_cast<size_t>(t - d) * A;
          FACTOR_COLS for (int j = 0; j < W; ++j)
              n[j] -= xm[q0 + j] != 0;
        }
        FACTOR_COLS for (int j = 0; j < W; ++j) {
          const bool m = xm[r + j] != 0;
          rv[q + j] = m ? xv[r + j] : detail::kNaN;
          n[j] += m;
        }
        detail::rank_rows(rv, std::min(t + 1, d), xv + r, W, lt, eq);
        for (int j = 0; j < W; ++j)
          detail::put(ov, om, r + j, xm[r + j] ? pct_of(lt[j], eq[j], n[j]) : 0.0, xm[r + j]);
      }
    }
  }
};

// ---- QuantileRoll: 每列一条有序窗 sv[j][0..n) (契约【序统计】: 取第 q_rank 名); 出窗 = 行 t−d 的输入格; 空窗 → 0 ----
//   定位 = 有序窗上的计数 (#< 即 lower_bound, #≤ 即 upper_bound; 无分支, 向量化, 窗 ≤ kMaxD 时比二分的误预测便宜);
//   出旧入新同一格时合成一次搬移: 只挪两位置之间那段 (新值 ≥ 旧值 → 左移一格, 否则右移一格), 与值域无关
namespace detail {
inline int count_lt(const float *s, int n, float x) {
  int c = 0;
  for (int i = 0; i < n; ++i)
    c += s[i] < x;
  return c;
}
inline int count_le(const float *s, int n, float x) {
  int c = 0;
  for (int i = 0; i < n; ++i)
    c += s[i] <= x;
  return c;
}
} // namespace detail
struct TsQuantileRoll {
  static auto carve(Bump &b, const Call &c) {
    constexpr size_t W = kTileA;
    return std::tuple{b.take<float>(static_cast<size_t>(c.p.d) * W), b.take<int>(W)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump bp{static_cast<char *>(ws)};
    const auto [sv, n] = carve(bp, c);
    const Param &p = c.p;
    const int d = p.d;
    assert(d >= 1);
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, W] = detail::tile(c, a0, rg.hi);
      std::fill_n(n, W, 0);
      for (int t = 0; t < T; ++t) {
        const size_t r = static_cast<size_t>(t) * A, q = static_cast<size_t>(std::max(t - d, 0)) * A;
        for (int j = 0; j < W; ++j) {
          float *s = sv + static_cast<size_t>(j) * d;
          const int k = n[j];
          const bool out = t >= d && xm[q + j], in = xm[r + j]; // 出窗: 窗里必有它 (±0 判等, 删哪个都一样)
          if (out && in) {
            const float xo = xv[q + j], xn = xv[r + j];
            const int po = detail::count_lt(s, k, xo), pn = detail::count_le(s, k, xn); // 旧值位置 / 新值在含旧值的窗里的 upper_bound
            if (xo <= xn) {                                                             // pn > po: (po, pn) 左移, 新值落 pn − 1
              std::copy(s + po + 1, s + pn, s + po);
              s[pn - 1] = xn;
            } else { // pn ≤ po: [pn, po) 右移, 新值落 pn
              std::copy_backward(s + pn, s + po, s + po + 1);
              s[pn] = xn;
            }
          } else if (out) {
            const int po = detail::count_lt(s, k, xv[q + j]);
            std::copy(s + po + 1, s + k, s + po);
            --n[j];
          } else if (in) {
            const int pn = detail::count_le(s, k, xv[r + j]);
            std::copy_backward(s + pn, s + k, s + k + 1);
            s[pn] = xv[r + j];
            ++n[j];
          }
          detail::put(ov, om, r + j, n[j] >= 1 ? static_cast<double>(s[q_rank(p.k, n[j]) - 1]) : 0.0, true);
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

// ---- Cov / Corr / Beta / Resid (Roll): 滑动加减二元幂和 + 双有效样本上的变化点全并列判据 (detail::chg_step) ----
template <detail::Pair K>
struct RollPair {
  static constexpr bool kSpx = K == detail::Pair::CORR;
  static constexpr bool kSpy = K != detail::Pair::COV;
  static auto carve(Bump &b, const Call &) {
    constexpr size_t W = kTileA, WX = kSpx ? W : 0, WY = kSpy ? W : 0; // 未用的一侧不切
    return std::tuple{b.take<int>(W), b.take<double>(W), b.take<double>(W), b.take<double>(W), b.take<double>(WX), b.take<double>(WY),
                      b.take<float>(WX), b.take<int>(WX), b.take<int>(WX), b.take<float>(WY), b.take<int>(WY), b.take<int>(WY)};
  }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [n, sx, sy, sxy, sxx, syy, lvx, lix, pcx, lvy, liy, pcy] = carve(b, c);
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
        detail::chg_reset(lvx, lix, pcx, w);
      }
      if constexpr (kSpy) {
        std::fill_n(syy, w, 0.0);
        detail::chg_reset(lvy, liy, pcy, w);
      }
      for (int t = 0; t < T; ++t) {
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
          bool spx = true, spy = true;
          if constexpr (kSpx)
            spx = detail::chg_step(lvx, lix, pcx, a, x, mm, t, d);
          if constexpr (kSpy)
            spy = detail::chg_step(lvy, liy, pcy, a, y, mm, t, d);
          detail::pair_put<K>(ov, om, r + a, n[a], spx, spy, sx[a], sy[a], sxy[a],
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
// EXPO (1): 最近 kMaxD 行按行龄加权, 不按段重置
// =============================================================================
// TsMeanEma: y = Σ q^{t−u} x_u / Σ q^{t−u} (q = 1−k, u 取窗 (t−kMaxD, t] 内有效格); 窗内无有效样本 → 0.
//   滑动递推, 权以窗内最新有效格 tv 为 1 (s, sw 同乘公因子, 比值不变; 不下溢): 新有效格进 → 旧和 × q^{t−tv} 再加;
//   滑出格 u 有效 → 减 q^{tv−u}·x_u; 窗内空了归零 (不留减法残差). q^i 查表 (i < kMaxD)
struct TsMeanEma {
  static auto carve(Bump &b, const Call &) { return std::tuple{b.take<double>(kTileA), b.take<double>(kTileA), b.take<int>(kTileA), b.take<int>(kTileA)}; }
  CP_SCRATCH
  CP_RUN {
    Bump b{static_cast<char *>(ws)};
    const auto [s, sw, n, tv] = carve(b, c);
    assert(c.p.k > 0.f && c.p.k <= 1.f);
    double qp[kMaxD];
    qp[0] = 1.0;
    for (int i = 1; i < kMaxD; ++i)
      qp[i] = qp[i - 1] * (1.0 - static_cast<double>(c.p.k));
    for (int a0 = rg.lo; a0 < rg.hi; a0 += kTileA) {
      const auto [xv, xm, yv, ym, zv, zm, ov, om, T, A, w] = detail::tile(c, a0, rg.hi);
      std::fill_n(s, w, 0.0);
      std::fill_n(sw, w, 0.0);
      std::fill_n(n, w, 0);
      std::fill_n(tv, w, -1);
      for (int t = 0; t < T; ++t) {
        const size_t r = static_cast<size_t>(t) * A;
        if (t >= kMaxD) {
          const int u = t - kMaxD;
          const size_t q = static_cast<size_t>(u) * A;
          for (int a = 0; a < w; ++a) {
            if (!xm[q + a])
              continue;
            if (--n[a] == 0) {
              s[a] = 0.0, sw[a] = 0.0;
            } else {
              const double f = qp[tv[a] - u];
              s[a] -= f * xv[q + a];
              sw[a] -= f;
            }
          }
        }
        for (int a = 0; a < w; ++a) {
          if (xm[r + a]) {
            const double f = n[a] > 0 ? qp[t - tv[a]] : 0.0;
            s[a] = f * s[a] + xv[r + a];
            sw[a] = f * sw[a] + 1.0;
            ++n[a];
            tv[a] = t;
          }
          detail::put(ov, om, r + a, n[a] > 0 ? s[a] / sw[a] : 0.0, true);
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
