#pragma once

// =============================================================================
// Search 的参数网格 (纯逻辑, 无后端, 无 IO): 表达式 → 可搜参数槽 → 统一坐标 → 粗网格 / 模式搜索邻点 / 分支挑选
// =============================================================================
//   【槽】模板表达式 (Expr.hpp 自由参数: 裸键名 d / k / k2) 的每个自由参数各一槽; 写了数字的参数固定不搜 (TS 口径根的 d 必是数字).
//     没有基线值: 每槽的**中心 u0** = 两侧有界取中点 / 单侧有界取该界 / 无界取 0 (阈值 = 0σ, 分位 = 0.5); 粗网格围中心铺.
//   【坐标】每槽一维坐标 u, 单调, **1 个单位 = 该参数的自然刻度**; 粗网格步长 / 模式搜索步长都以单位计. 变换按参数含义选 (见 to_u / to_v):
//       d / EMA 系数 / 缩尾 / 桶数              log2        (窗长 / 尺度参数的效应是对数的)
//       分位 (0,1)                             logit       (两端都有界, 向两端等比加密)
//       阈值 ℝ / 对称截断 ≥ 0 (原始量纲)        / σ          (σ = 该节点输入的标准差, 调用方装载后 set_scale; 1 单位 = 1σ. 没有它阈值搜不动)
//       日内时段分钟                            / 32        (半小时量级为一单位)
//     整数参数 (d / 桶数 / 分钟) 取整回去.
//   【粗网格】只有它是网格: 每槽 u0 + ℤ·s, 有界侧枚举到出界为止, 无界侧覆盖 kSpan 单位; 步长 s 从 1 起按 √2 放大到笛卡尔积 ≤ kLayerMax
//     (精度由预算反推, 不随槽数爆炸).
//   【模式搜索】粗网格之后每支做坐标模式搜索 (compass, 无梯度: 参数多为整数窗长, score 对它分段常数): 中心 c, 步长 s, 邻点 = 每槽 c ± s
//     (2P 点). 有更优 → 移过去 (s 不变); 否则 s 减半; 邻点全与中心同 (整数槽取整后不变) 或 s < kMinStep → 停 (停机规则在调用方).
//     越界 / 参数值域不合 (expr::check_params) 的点丢弃.
//   【分支】粗网格的**局部峰** (score 有限且不低于全部 Chebyshev 相邻格点; 并列取下标小者) 各开一支, 不设支数上限.
//   出错策略: 只 assert (表达式来自已校验的因子行).
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/Expr.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

namespace factor::search::grid {

inline constexpr int kLayerMax = 256;         // 粗网格的格点上限; 粗步长由它反推
inline constexpr double kSpan = 4.0;          // 无界侧粗网格覆盖的坐标范围 (单位)
inline constexpr double kMinStep = 1.0 / 16; // 模式搜索最小步长 (单位; 只对连续槽起作用, 整数槽先到取整分辨率)
inline constexpr double kTodUnit = 32.0;      // 时段参数一单位 = 32 分钟

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
  double u0 = 0.0;               // 中心坐标 (见文件头【槽】)
  double scale = 1.0;            // ANY / GE0: 输入节点标准差 (set_scale 后有效); 其他 1
  std::string name;              // "<Op>.<key>" (显示)
};

// ANY / GE0 槽的坐标要数据尺度 (调用方装载后 set_scale)
inline bool needs_scale(const Slot &s) { return s.key != Key::D && (s.kdom == KDom::ANY || s.kdom == KDom::GE0); }

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
  case KDom::ANY:
    return v / s.scale;
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
      v = std::exp2(u);
      break;
    case KDom::GE0:
    case KDom::ANY:
      v = u * s.scale;
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

// 模板的可搜槽 = 自由参数 (节点前序); 具体表达式 → 空
inline std::vector<Slot> slots_of(const expr::Expr &e) {
  std::vector<Slot> out;
  for (size_t i = 0; i < e.nodes.size(); ++i) {
    const expr::Node &n = e.nodes[i];
    if (n.op < 0 || !n.free)
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
        case KDom::ANY:
          break; // 两侧无界
        case KDom::GE0:
          s.lo_bounded = true, s.u_lo = 0.0;
          break;
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
      s.u0 = s.lo_bounded && s.hi_bounded ? 0.5 * (s.u_lo + s.u_hi) : (s.lo_bounded ? s.u_lo : (s.hi_bounded ? s.u_hi : 0.0));
      out.push_back(std::move(s));
    };
    if (n.free & expr::kFreeD)
      add(Key::D);
    if (n.free & expr::kFreeK)
      add(Key::K);
    if (n.free & expr::kFreeK2)
      add(Key::K2);
  }
  return out;
}

// 中心坐标 u0[P]
inline std::vector<double> center_u(const std::vector<Slot> &slots) {
  std::vector<double> u(slots.size());
  for (size_t j = 0; j < slots.size(); ++j)
    u[j] = slots[j].u0;
  return u;
}

// ANY / GE0 槽装上数据尺度 σ (> 0 有限); 中心 0σ 不随尺度变
inline void set_scale(Slot &s, double sigma) {
  assert(needs_scale(s) && std::isfinite(sigma) && sigma > 0.0);
  s.scale = sigma;
}

inline bool in_range(const Slot &s, double u) { return (!s.lo_bounded || u >= s.u_lo - 1e-9) && (!s.hi_bounded || u <= s.u_hi + 1e-9); }

// 坐标 u[P] → 具体表达式 (base 模板的拷贝, 槽参数落值并清自由位, 刷新 canon); 任一槽出界 / 参数值域不合 → false.
// base 的自由参数须恰为 slots (否则结果仍是模板, 断言)
inline bool apply(const expr::Expr &base, const std::vector<Slot> &slots, const std::vector<double> &u, expr::Expr &out) {
  assert(u.size() == slots.size());
  out = base;
  for (size_t j = 0; j < slots.size(); ++j) {
    const Slot &s = slots[j];
    if (!in_range(s, u[j]))
      return false;
    expr::Node &n = out.nodes[static_cast<size_t>(s.node)];
    const double v = to_v(s, u[j]);
    if (!std::isfinite(v))
      return false;
    if (s.key == Key::D)
      n.p.d = static_cast<int>(v), n.free &= static_cast<uint8_t>(~expr::kFreeD);
    else if (s.key == Key::K)
      n.p.k = static_cast<float>(v), n.free &= static_cast<uint8_t>(~expr::kFreeK);
    else
      n.p.k2 = static_cast<float>(v), n.free &= static_cast<uint8_t>(~expr::kFreeK2);
  }
  assert(!out.is_template() && "槽须覆盖模板的全部自由参数");
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

// 粗网格 (见【预算】): 每槽 u0 + i·step (有界侧到出界为止, 无界侧覆盖 kSpan), step 从 1 按 √2 放大到笛卡尔积 ≤ kLayerMax;
// offs = 每点的整数偏移 (下标与 slots 对齐)
struct Coarse {
  double step = 1.0;
  std::vector<std::vector<int>> offs;
};
inline Coarse coarse_lattice(const std::vector<Slot> &slots) {
  assert(!slots.empty());
  Coarse out;
  std::vector<std::vector<int>> axes(slots.size());
  for (double step = 1.0;; step *= std::sqrt(2.0)) {
    double total = 1.0;
    for (size_t j = 0; j < slots.size(); ++j) {
      const Slot &s = slots[j];
      std::vector<int> &ax = axes[j];
      ax.clear();
      for (int i = 0; in_range(s, s.u0 - i * step) && (s.lo_bounded || i * step <= kSpan + 1e-9); ++i)
        ax.push_back(-i);
      for (int i = 1; in_range(s, s.u0 + i * step) && (s.hi_bounded || i * step <= kSpan + 1e-9); ++i)
        ax.push_back(i);
      std::sort(ax.begin(), ax.end());
      total *= static_cast<double>(ax.size());
    }
    if (total <= kLayerMax) { // step 够大时每槽只剩基线一点 (total = 1), 必停
      out.step = step;
      break;
    }
  }
  std::vector<int> cur(slots.size());
  detail::product(axes, 0, cur, out.offs);
  return out;
}
inline std::vector<double> coarse_u(const std::vector<Slot> &slots, const std::vector<int> &offs, double step) {
  std::vector<double> u(slots.size());
  for (size_t j = 0; j < slots.size(); ++j)
    u[j] = slots[j].u0 + offs[j] * step;
  return u;
}

// 模式搜索邻点 (见【模式搜索】): 每槽 c ± step, 共 2P 点 (不含中心; 出界点由 apply 丢)
inline std::vector<std::vector<double>> compass(const std::vector<Slot> &slots, const std::vector<double> &c, double step) {
  assert(c.size() == slots.size() && step > 0.0);
  std::vector<std::vector<double>> out;
  out.reserve(2 * slots.size());
  for (size_t j = 0; j < slots.size(); ++j)
    for (int sgn : {-1, 1}) {
      std::vector<double> u = c;
      u[j] += sgn * step;
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

// 分支挑选 (见【分支】): 局部峰 = score 有限, 且每个相邻格点 j 都不更高 (并列时 j 下标更大); 返回 lattice 下标, score 降序
inline std::vector<int> pick_branches(const std::vector<std::vector<int>> &lattice, const std::vector<double> &score) {
  assert(lattice.size() == score.size());
  std::vector<int> out;
  for (size_t i = 0; i < lattice.size(); ++i) {
    if (!std::isfinite(score[i]))
      continue;
    bool peak = true;
    for (size_t j = 0; peak && j < lattice.size(); ++j)
      if (j != i && adjacent(lattice[i], lattice[j]))
        peak = score[j] < score[i] || (score[j] == score[i] && j > i);
    if (peak)
      out.push_back(static_cast<int>(i));
  }
  std::stable_sort(out.begin(), out.end(), [&](int a, int b) { return score[static_cast<size_t>(a)] > score[static_cast<size_t>(b)]; });
  return out;
}

} // namespace factor::search::grid
