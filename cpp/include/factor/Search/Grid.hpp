#pragma once

// =============================================================================
// Search 的参数网格 (纯逻辑, 无后端, 无 IO): 表达式 → 可搜参数槽 → 统一坐标 → 粗网格 / 细化网格 / 分支挑选
// =============================================================================
//   【槽】表达式每个算子节点声明的每个参数 (d / k / k2) 各一槽; TS 口径根 TsRankRoll 的 d 是口径定义 (kTsNormD), 不搜.
//   【坐标】每槽一维坐标 u, 单调, **粗网格步长 = 1 个单位**, 细化每层步长减半. 变换按参数含义选 (见 to_u / to_v):
//       d / EMA 系数 / 缩尾 / 对称截断 / 桶数   log2        (窗长 / 尺度参数的效应是对数的)
//       分位 (0,1)                             logit       (两端都有界, 向两端等比加密)
//       阈值 ℝ                                 asinh       (0 附近线性, 远端对数; 无尺度先验下唯一无奇点的选择)
//       日内时段分钟                            / 32        (半小时量级为一单位)
//     整数参数 (d / 桶数 / 分钟) 取整回去; 坐标正好是当前值 (u == u0) 时原样保留原值 (float 往返有舍入, 规范串得与基线一致).
//   【粗网格】每槽 u0 + ℤ: 有界侧枚举到出界为止, 无界侧各 kCoarseSpan 步; 槽间笛卡尔积. 基线 (全 0 偏移) 必在其中, 搜索结果不会劣于当前.
//   【细化】中心 c, 步长 s: 每槽 c + {−kRefineHalf..kRefineHalf}·s, 笛卡尔积 (5^P); 越界 / 参数值域不合 (expr::check_params) 的点丢弃.
//   【分支】粗网格后取 score 最高的 kBranches 个**互不相邻** (粗格点 Chebyshev 距离 ≥ 2) 的候选各开一支; 一支只围自己的 top-1 细化,
//     一层相对增益 < kMinGain 即停 (停机规则在调用方, 本头只给常量 / 格点).
//   出错策略: 只 assert (表达式来自已校验的因子行).
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/Expr.hpp"
#include "factor/Stat/Contract.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace factor::search::grid {

inline constexpr int kCoarseSpan = 4;    // 无界侧粗网格每侧步数
inline constexpr int kRefineHalf = 2;    // 细化网格每槽每侧点数 (5 点)
inline constexpr int kBranches = 3;      // 粗网格后开的分支数
inline constexpr double kMinGain = 0.05; // 分支一层的相对增益低于此即停
inline constexpr int kMaxLevels = 8;     // 细化层数上限 (步长 2^-8 单位, 低于任何参数的分辨率)
inline constexpr double kTodUnit = 32.0; // 时段参数一单位 = 32 分钟

enum class Key : uint8_t { D,
                           K,
                           K2 };
inline constexpr const char *key_name(Key k) { return k == Key::D ? "d" : (k == Key::K ? "k" : "k2"); }

struct Slot {
  int node = -1; // Expr::nodes 下标 (算子节点)
  Key key = Key::D;
  KDom kdom = KDom::NONE; // k 域 (d 槽不看)
  bool integer = false;   // 值取整
  bool lo_bounded = false, hi_bounded = false;
  double u_lo = 0.0, u_hi = 0.0; // 有界侧范围 (闭; 开端点由 check_params 否决)
  double u0 = 0.0;               // 当前值坐标
  std::string name;              // "<Op>.<key>" (显示)
};

// ---- 坐标变换 ----
inline double to_u(const Slot &s, double v) {
  if (s.key == Key::D)
    return std::log2(v);
  switch (s.kdom) {
  case KDom::OPEN01:
    return std::log(v / (1.0 - v));
  case KDom::OPEN0_CLOSED1:
  case KDom::OPEN0_HALF:
  case KDom::POSINT_GROUP:
    return std::log2(v);
  case KDom::GE0:
    return std::log2(v > 0.0 ? v : std::ldexp(1.0, -10)); // k = 0 (截成全 0) 当作极小尺度起搜
  case KDom::ANY:
    return std::asinh(v);
  case KDom::TOD:
    return v / kTodUnit;
  case KDom::NONE:
    break;
  }
  assert(false && "无 k 的算子没有 k 槽");
  return 0.0;
}
inline double to_v(const Slot &s, double u) {
  double v = 0.0;
  if (s.key == Key::D) {
    v = std::exp2(u);
  } else {
    switch (s.kdom) {
    case KDom::OPEN01:
      v = 1.0 / (1.0 + std::exp(-u));
      break;
    case KDom::OPEN0_CLOSED1:
    case KDom::OPEN0_HALF:
    case KDom::POSINT_GROUP:
    case KDom::GE0:
      v = std::exp2(u);
      break;
    case KDom::ANY:
      v = std::sinh(u);
      break;
    case KDom::TOD:
      v = u * kTodUnit;
      break;
    case KDom::NONE:
      assert(false);
    }
  }
  return s.integer ? std::round(v) : v;
}

// 当前值 (基线) 的该槽参数
inline double value_of(const expr::Node &n, Key k) { return k == Key::D ? static_cast<double>(n.p.d) : (k == Key::K ? n.p.k : n.p.k2); }

// 表达式的可搜槽 (节点前序); ts_root = TS 口径 (根 TsRankRoll 的 d 不搜)
inline std::vector<Slot> slots_of(const expr::Expr &e, bool ts_root) {
  std::vector<Slot> out;
  for (size_t i = 0; i < e.nodes.size(); ++i) {
    const expr::Node &n = e.nodes[i];
    if (n.op < 0)
      continue;
    const expr::OpInfo &o = expr::kOps[n.op];
    const auto add = [&](Key key) {
      Slot s;
      s.node = static_cast<int>(i);
      s.key = key;
      s.kdom = o.kdom;
      s.name = std::string(o.name) + "." + key_name(key);
      if (key == Key::D) {
        s.integer = true;
        s.lo_bounded = s.hi_bounded = true;
        s.u_lo = 0.0, s.u_hi = std::log2(static_cast<double>(expr::kMaxD));
      } else {
        switch (o.kdom) {
        case KDom::OPEN01:
        case KDom::GE0:
        case KDom::ANY:
          break; // 两侧无界
        case KDom::OPEN0_CLOSED1:
          s.hi_bounded = true, s.u_hi = 0.0;
          break;
        case KDom::OPEN0_HALF:
          s.hi_bounded = true, s.u_hi = -1.0;
          break;
        case KDom::POSINT_GROUP:
          s.integer = true;
          s.lo_bounded = s.hi_bounded = true;
          s.u_lo = 0.0, s.u_hi = std::log2(static_cast<double>(kMaxGroup));
          break;
        case KDom::TOD:
          s.integer = true;
          s.lo_bounded = s.hi_bounded = true;
          s.u_lo = 0.0, s.u_hi = static_cast<double>(kSegLen) / kTodUnit;
          break;
        case KDom::NONE:
          assert(false);
        }
      }
      s.u0 = to_u(s, value_of(n, key));
      assert(std::isfinite(s.u0) && "当前参数值不在坐标定义域内");
      out.push_back(std::move(s));
    };
    if (expr::declares(o.params, "d") && !(ts_root && i == 0))
      add(Key::D);
    if (expr::declares(o.params, "k"))
      add(Key::K);
    if (expr::declares(o.params, "k2"))
      add(Key::K2);
  }
  return out;
}

inline bool in_range(const Slot &s, double u) { return (!s.lo_bounded || u >= s.u_lo - 1e-9) && (!s.hi_bounded || u <= s.u_hi + 1e-9); }

// 坐标 u[P] → 表达式 (base 的拷贝, 只改槽参数, 刷新 canon); 任一槽出界 / 参数值域不合 → false
inline bool apply(const expr::Expr &base, const std::vector<Slot> &slots, const std::vector<double> &u, expr::Expr &out) {
  assert(u.size() == slots.size());
  out = base;
  for (size_t j = 0; j < slots.size(); ++j) {
    const Slot &s = slots[j];
    if (!in_range(s, u[j]))
      return false;
    expr::Node &n = out.nodes[static_cast<size_t>(s.node)];
    if (u[j] == s.u0)
      continue; // 基线值原样保留 (变换往返有舍入)
    const double v = to_v(s, u[j]);
    if (!std::isfinite(v))
      return false;
    if (s.key == Key::D)
      n.p.d = static_cast<int>(v);
    else if (s.key == Key::K)
      n.p.k = static_cast<float>(v);
    else
      n.p.k2 = static_cast<float>(v);
  }
  for (const Slot &s : slots) {
    const expr::Node &n = out.nodes[static_cast<size_t>(s.node)];
    if (expr::check_params(expr::kOps[n.op], n.p))
      return false;
  }
  out.canon = expr::to_string(out);
  return true;
}

// ---- 格点枚举 (笛卡尔积; 每点 = 每槽一个整数偏移) ----
namespace detail {
inline void product(const std::vector<std::vector<int>> &axes, size_t j, std::vector<int> &cur, std::vector<std::vector<int>> &out) {
  if (j == axes.size()) {
    out.push_back(cur);
    return;
  }
  for (int i : axes[j]) {
    cur[j] = i;
    product(axes, j + 1, cur, out);
  }
}
} // namespace detail

// 粗网格: 每槽 u0 + i (i ∈ ℤ, 有界侧到出界为止, 无界侧 ±kCoarseSpan); 返回每点的整数偏移 (下标与 slots 对齐)
inline std::vector<std::vector<int>> coarse_lattice(const std::vector<Slot> &slots) {
  std::vector<std::vector<int>> axes(slots.size());
  for (size_t j = 0; j < slots.size(); ++j) {
    const Slot &s = slots[j];
    std::vector<int> &ax = axes[j];
    for (int i = 0; in_range(s, s.u0 - i) && (s.lo_bounded || i <= kCoarseSpan); ++i)
      ax.push_back(-i);
    for (int i = 1; in_range(s, s.u0 + i) && (s.hi_bounded || i <= kCoarseSpan); ++i)
      ax.push_back(i);
    std::sort(ax.begin(), ax.end());
  }
  std::vector<std::vector<int>> out;
  std::vector<int> cur(slots.size());
  detail::product(axes, 0, cur, out);
  return out;
}
inline std::vector<double> coarse_u(const std::vector<Slot> &slots, const std::vector<int> &offs) {
  std::vector<double> u(slots.size());
  for (size_t j = 0; j < slots.size(); ++j)
    u[j] = slots[j].u0 + offs[j];
  return u;
}

// 细化网格: 中心 c, 步长 step → 每槽 c + {−kRefineHalf..kRefineHalf}·step 的笛卡尔积 (含中心; 出界点由 apply 丢)
inline std::vector<std::vector<double>> refine_lattice(const std::vector<Slot> &slots, const std::vector<double> &c, double step) {
  assert(c.size() == slots.size() && step > 0.0);
  std::vector<std::vector<int>> axes(slots.size());
  for (std::vector<int> &ax : axes)
    for (int i = -kRefineHalf; i <= kRefineHalf; ++i)
      ax.push_back(i);
  std::vector<std::vector<int>> offs;
  std::vector<int> cur(slots.size());
  detail::product(axes, 0, cur, offs);
  std::vector<std::vector<double>> out;
  out.reserve(offs.size());
  for (const std::vector<int> &o : offs) {
    std::vector<double> u(slots.size());
    for (size_t j = 0; j < slots.size(); ++j)
      u[j] = c[j] + o[j] * step;
    out.push_back(std::move(u));
  }
  return out;
}

// 粗格点相邻 = 每槽偏移差都 ≤ 1 (Chebyshev ≤ 1)
inline bool adjacent(const std::vector<int> &a, const std::vector<int> &b) {
  assert(a.size() == b.size());
  for (size_t j = 0; j < a.size(); ++j)
    if (std::abs(a[j] - b[j]) > 1)
      return false;
  return true;
}

// 分支挑选: score 降序贪心, 与已选者都不相邻才收, 最多 kBranches 个; score 非有限 (−∞) 的不收. 返回 lattice 下标
inline std::vector<int> pick_branches(const std::vector<std::vector<int>> &lattice, const std::vector<double> &score) {
  assert(lattice.size() == score.size());
  std::vector<int> order(lattice.size());
  for (size_t i = 0; i < order.size(); ++i)
    order[i] = static_cast<int>(i);
  std::stable_sort(order.begin(), order.end(), [&](int a, int b) { return score[static_cast<size_t>(a)] > score[static_cast<size_t>(b)]; });
  std::vector<int> out;
  for (int i : order) {
    if (!std::isfinite(score[static_cast<size_t>(i)]) || static_cast<int>(out.size()) >= kBranches)
      break;
    bool ok = true;
    for (int j : out)
      ok = ok && !adjacent(lattice[static_cast<size_t>(i)], lattice[static_cast<size_t>(j)]);
    if (ok)
      out.push_back(i);
  }
  return out;
}

} // namespace factor::search::grid
