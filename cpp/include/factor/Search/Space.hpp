#pragma once

// =============================================================================
// Search 的参数空间 (纯逻辑, 无后端, 无 IO; 与 Surface.hpp 对仗: 点从哪来 / 点有多好): 表达式 → 可搜参数槽 → 统一坐标 + 闭区间 → 低差异采样 (第 0 趟)
// =============================================================================
//   【槽】模板表达式 (Expr.hpp 自由参数: 裸键名 d / k / k2) 的每个自由参数各一槽; 写了数字的参数固定不搜 (TS 口径根的 d 必是数字).
//   【坐标与区间】每槽一维坐标 u, 单调; 区间两端全由已有常数推出 (不设跨度常数). 变换按参数含义选 (见 to_v):
//       d (ROLL 窗长)              log2      [1, kMaxD]
//       TsMeanEma 系数             log2      [2/(kMaxD+1), 1]          (对标窗长 ≤ kMaxD, 与 d 同口径)
//       CsWinsor 缩尾              log2      [1/A, 1/2)                (少于一只等于不缩)
//       CsBucket 桶数              log2      [1, min(kMaxGroup, A)]
//       分位 (0,1)                 logit     [1/n, 1 − 1/n]            (n = 分位所取的样本数: 截面 A / 滚窗 d, 自由 d 取 kMaxD)
//       阈值 TsGt / 截断 TsClip    logit 分位 [1/A, 1 − 1/A]            (k = 输入分布 (TsClip: |x|) 的分位, 分位表 qtab 由调用方装载后
//                                                                       set_qtab; 平均每行触发 1 只为界)
//       日内时段 TsTodMask         行        [0, S]
//     A = 池内平均有效资产数 (n_full · kGroups). 整数参数 (d / 桶数 / 时段) 取整回去.
//     中性坐标 u0: 调用方定分位表时, 其他槽取它 (区间中点; 分位 / 阈值 = 中位; 时段 = 全天不掩).
//   【采样】第 0 趟的点 = R_d 低差异序列 (frac(0.5 + n·α), α_j = φ_P^{−j}, φ_P 为 x^{P+1} = x + 1 的正根) 映射到各槽区间:
//     确定性, 无表无种子, 任意 P 每槽 N 个不同取值 (P 大也不退化). 点数 kInit (Config) 是唯一的预算量.
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

namespace factor::search::space {

enum class Key : uint8_t { D,
                           K,
                           K2 };
inline constexpr const char *key_name(Key k) { return k == Key::D ? "d" : (k == Key::K ? "k" : "k2"); }

struct Slot {
  int node = -1; // Expr::nodes 下标 (算子节点)
  Key key = Key::D;
  KDom kdom = KDom::NONE;        // k 域 (d 槽不看)
  bool integer = false;          // 值取整
  double u_lo = 0.0, u_hi = 0.0; // 闭区间 (开端点由 check_params 否决)
  double u0 = 0.0;               // 中性坐标 (见文件头【坐标与区间】)
  std::vector<float> qtab;       // ANY / GE0: 输入分布的升序抽样 (set_qtab 后有效; GE0 存 |x|)
  std::string name;              // "<Op>.<key>" (显示)
};

// ANY / GE0 槽的坐标是输入分布的分位 (调用方装载后 set_qtab)
inline bool needs_qtab(const Slot &s) { return s.key != Key::D && (s.kdom == KDom::ANY || s.kdom == KDom::GE0); }
inline bool ready(const Slot &s) { return !needs_qtab(s) || !s.qtab.empty(); }

inline double logit(double p) { return std::log(p / (1.0 - p)); }
inline double sigmoid(double u) { return 1.0 / (1.0 + std::exp(-u)); }

// 升序表 q 处的分位 (线性插值)
inline double quantile(const std::vector<float> &t, double q) {
  assert(t.size() >= 2 && q >= 0.0 && q <= 1.0);
  const double x = q * static_cast<double>(t.size() - 1);
  const size_t i = std::min(static_cast<size_t>(x), t.size() - 2);
  const double f = x - static_cast<double>(i);
  return t[i] + f * (static_cast<double>(t[i + 1]) - t[i]);
}

// ---- 坐标 → 参数值 ----
inline double to_v(const Slot &s, double u) {
  double v = 0.0;
  if (s.key == Key::D) {
    v = std::exp2(u);
  } else {
    switch (s.kdom) {
    case KDom::OPEN01:
      v = sigmoid(u);
      break;
    case KDom::OPEN0_CLOSED1:
    case KDom::OPEN0_HALF:
    case KDom::POSINT_GROUP:
      v = std::exp2(u);
      break;
    case KDom::GE0:
    case KDom::ANY:
      assert(!s.qtab.empty() && "阈值槽未装分位表 (set_qtab)");
      v = quantile(s.qtab, sigmoid(u));
      break;
    case KDom::TOD:
      v = u;
      break;
    case KDom::NONE:
      assert(false);
    }
  }
  return s.integer ? std::round(v) : v;
}

// 模板的可搜槽 = 自由参数 (节点前序); 具体表达式 → 空. seg = 因子所在层 (TOD 区间按本层行); a = 池内平均有效资产数 (区间端点)
inline std::vector<Slot> slots_of(const expr::Expr &e, int seg, double a) {
  assert(a > 2.0);
  const Geo g = geo_of(seg);
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
        s.u_lo = 0.0, s.u_hi = std::log2(static_cast<double>(kMaxD));
      } else {
        switch (o.kdom) {
        case KDom::OPEN01: { // 分位所取的样本数: 截面 = A; 滚窗 = d (自由 d 取上界)
          const double m = o.a == A::ALL ? a : static_cast<double>((n.free & expr::kFreeD) ? kMaxD : n.p.d);
          s.u_hi = m > 2.0 ? logit(1.0 - 1.0 / m) : 0.0;
          s.u_lo = -s.u_hi;
          break;
        }
        case KDom::ANY:
        case KDom::GE0:
          s.u_hi = logit(1.0 - 1.0 / a);
          s.u_lo = -s.u_hi;
          break;
        case KDom::OPEN0_CLOSED1:
          s.u_lo = std::log2(2.0 / (kMaxD + 1)), s.u_hi = 0.0;
          break;
        case KDom::OPEN0_HALF:
          s.u_lo = -std::log2(a), s.u_hi = -1.0;
          break;
        case KDom::POSINT_GROUP:
          s.integer = true;
          s.u_lo = 0.0, s.u_hi = std::log2(std::min(static_cast<double>(kMaxGroup), a));
          break;
        case KDom::TOD:
          s.integer = true;
          s.u_lo = 0.0, s.u_hi = static_cast<double>(g.S);
          break;
        case KDom::NONE:
          assert(false);
        }
      }
      s.u0 = o.kdom == KDom::TOD && key != Key::D ? (key == Key::K ? s.u_lo : s.u_hi) : 0.5 * (s.u_lo + s.u_hi);
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

// 模板子树 i 的最长回看行数 = 沿各条到叶的路径累加 ROLL 窗长 d (自由 d 取上界 kMaxD; 用 d 而非 d−1 以盖住 Delay / Delta 的 x_{t−d}).
// EXPAND 段内 reset (块首对齐段首) 不计; EXPO 全历史递推不计 (块首重启, 由调用方接受)
inline int lookback(const expr::Expr &e, int i = 0) {
  const expr::Node &n = e.nodes[static_cast<size_t>(i)];
  if (n.op < 0)
    return 0;
  const expr::OpInfo &o = expr::kOps[n.op];
  int m = 0;
  for (int a = 0; a < o.arity; ++a)
    m = std::max(m, lookback(e, n.args[a]));
  return m + (o.t == T::ROLL ? ((n.free & expr::kFreeD) ? kMaxD : n.p.d) : 0);
}

// 阈值槽装上分位表 (升序, ≥ 2 个且不全等: 常量输入调用方先拦)
inline void set_qtab(Slot &s, std::vector<float> t) {
  assert(needs_qtab(s) && t.size() >= 2 && std::is_sorted(t.begin(), t.end()) && t.front() < t.back());
  s.qtab = std::move(t);
}

inline bool in_range(const Slot &s, double u) { return u >= s.u_lo - 1e-9 && u <= s.u_hi + 1e-9; }

// 槽参数落值 v 并清自由位 (不刷 canon)
inline void put(expr::Expr &e, const Slot &s, double v) {
  expr::Node &n = e.nodes[static_cast<size_t>(s.node)];
  if (s.key == Key::D)
    n.p.d = static_cast<int>(v), n.free &= static_cast<uint8_t>(~expr::kFreeD);
  else if (s.key == Key::K)
    n.p.k = static_cast<float>(v), n.free &= static_cast<uint8_t>(~expr::kFreeK);
  else
    n.p.k2 = static_cast<float>(v), n.free &= static_cast<uint8_t>(~expr::kFreeK2);
}

// 坐标 u[P] → 具体表达式 (base 模板的拷贝, 槽参数落值并清自由位, 刷新 canon); 任一槽出界 / 参数值域不合 → false.
// base 的自由参数须恰为 slots (否则结果仍是模板, 断言); seg = 因子所在层 (check_params 按层校验)
inline bool apply(const expr::Expr &base, const std::vector<Slot> &slots, const std::vector<double> &u, int seg, expr::Expr &out) {
  assert(u.size() == slots.size());
  out = base;
  for (size_t j = 0; j < slots.size(); ++j) {
    const Slot &s = slots[j];
    if (!in_range(s, u[j]))
      return false;
    const double v = to_v(s, u[j]);
    if (!std::isfinite(v))
      return false;
    put(out, s, v);
  }
  assert(!out.is_template() && "槽须覆盖模板的全部自由参数");
  for (const Slot &s : slots) {
    const expr::Node &n = out.nodes[static_cast<size_t>(s.node)];
    if (expr::check_params(expr::kOps[n.op], n.p, seg))
      return false;
  }
  out.canon = expr::to_string(out);
  return true;
}

// 中性具体式 (定分位表用): base 的占位值上, 已就绪 (ready) 的槽落中性值 u0; 未就绪的阈值槽留占位值
inline expr::Expr neutral(const expr::Expr &base, const std::vector<Slot> &slots, int seg) {
  expr::Expr out = base.placeholder();
  for (const Slot &s : slots)
    if (ready(s))
      put(out, s, to_v(s, s.u0));
  for (const Slot &s : slots) {
    const expr::Node &n = out.nodes[static_cast<size_t>(s.node)];
    assert(!expr::check_params(expr::kOps[n.op], n.p, seg) && "中性值须合法");
    (void)n, (void)seg;
  }
  out.canon = expr::to_string(out);
  return out;
}

// 单位立方坐标 (曲面拟合用): 每槽 (u − u_lo) / (u_hi − u_lo); 区间退化的槽恒 0
inline std::vector<double> unit(const std::vector<Slot> &slots, const std::vector<double> &u) {
  assert(u.size() == slots.size());
  std::vector<double> x(slots.size());
  for (size_t j = 0; j < slots.size(); ++j) {
    const double w = slots[j].u_hi - slots[j].u_lo;
    x[j] = w > 0.0 ? (u[j] - slots[j].u_lo) / w : 0.0;
  }
  return x;
}

// 第 0 趟采样 (见【采样】): n 个 R_d 点, 坐标 u[P]
inline std::vector<std::vector<double>> design(const std::vector<Slot> &slots, int n) {
  const int P = static_cast<int>(slots.size());
  assert(P >= 1 && n >= 1);
  double phi = 2.0; // x^{P+1} = x + 1 的正根 (不动点迭代, 收敛到双精度)
  for (int it = 0; it < 64; ++it)
    phi = std::pow(1.0 + phi, 1.0 / (P + 1));
  std::vector<double> alpha(static_cast<size_t>(P));
  for (int j = 0; j < P; ++j)
    alpha[static_cast<size_t>(j)] = std::pow(1.0 / phi, j + 1);
  std::vector<std::vector<double>> out(static_cast<size_t>(n), std::vector<double>(static_cast<size_t>(P)));
  for (int i = 0; i < n; ++i)
    for (int j = 0; j < P; ++j) {
      const double f = std::fmod(0.5 + (i + 1) * alpha[static_cast<size_t>(j)], 1.0);
      const Slot &s = slots[static_cast<size_t>(j)];
      out[static_cast<size_t>(i)][static_cast<size_t>(j)] = s.u_lo + f * (s.u_hi - s.u_lo);
    }
  return out;
}

} // namespace factor::search::space
