#pragma once

// 单资产回测, 两个逐根核: 仓位层 Size (目标仓位 → 实际仓位) ↔ 损益 Pnl (实际仓位 → 逐根毛 / 成本 / 净); 仓位按本根收盘价调到
//   仓位 w ∈ [−1, 1]: 正 = 多, 负 = 空
//   仓位层: 波动率定仓 w · min(1, 目标 / σ̂) → 不交易带 (前后同向持仓且 |Δw| < 带宽 不调)
//   损益: 每根毛收益 g = p · a, 净 r = g − 成本; p = 前根收盘调到的仓位, w = 本根收盘调到的, a = 资产本根收益
//   成本 (占净值比例) 分四项逐根记, 四项之和 = 毛 − 净:
//     佣金 = 佣金率 × |w − p|; 印花税 = 印花税率 × 卖出量 max(p − w, 0) (多头减仓 / 融券卖出); 冲击 = 冲击率 × |w − p| (固定, 不按金额变)
//     跳空 = 按本根收盘成交相对按层线价成交的差: 分层目标越过的层线 L (本根刀的线价) 被收盘价穿过 (前根收与本根收在 L 两侧) 才计, 每线 |收 / L − 1|;
//            实际调仓与分层同向才计, 量均摊到越过的各线 (波动率定仓 / 不交易带改量不改线); 线随带移过价 / 带未就绪 = 按收盘决策, 不计
//   simulate = 刀列 (目标仓位 + 层线) 逐根过 Size → Pnl
#include "cta/Model.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iterator>
#include <utility>
#include <vector>

namespace cta {

struct Setup {
  double commission = 0; // 佣金率 (买卖各一次)
  double stamp = 0;      // 印花税率 (卖出单边)
  double impact = 0;     // 冲击率 (买卖各一次)
  double vol = 0;        // 目标年化波动 (0 = 不调)
  double buffer = 0;     // 不交易带宽
  double bpy = 244;      // 每年根数
};

// 成本分项 (占净值比例): 一根 / 累计同一形
struct Costs {
  double commission = 0, stamp = 0, impact = 0, gap = 0;
  double sum() const { return commission + stamp + impact + gap; }
  Costs &operator+=(const Costs &o) {
    commission += o.commission, stamp += o.stamp, impact += o.impact, gap += o.gap;
    return *this;
  }
};

// 成本分项表 (序 = 净值图上自策略净值向上的叠放序)
struct CostItem {
  const char *name;
  double Costs::*f;
};
inline constexpr CostItem kCosts[] = {{"佣金", &Costs::commission}, {"印花税", &Costs::stamp}, {"冲击", &Costs::impact}, {"跳空", &Costs::gap}};
inline constexpr size_t kNC = std::size(kCosts);

// 跳空 (见文件头): 前根收 c0 → 本根收 c, 分层目标 q → 刀 x (分层 m), 实际调仓 dw
inline double gap_of(const Ladder &m, float c0, float c, float q, const Cut &x, float dw) {
  const long a = std::lround(q * static_cast<float>(m.N)), b = std::lround(x.w * static_cast<float>(m.N));
  if (!ready(x) || a == b || dw * static_cast<float>(b - a) <= 0)
    return 0;
  double s = 0;
  for (long k = std::min(a, b); k < std::max(a, b); ++k) {
    const double L = line(m.geo, static_cast<double>(k + 1 > 0 ? k + 1 : k) / m.N, x); // 相邻两层之间的线 = 离中线远的那层
    if ((c0 - L) * (c - L) < 0)
      s += std::fabs(c / L - 1);
  }
  return std::fabs(dw) * s / static_cast<double>(std::labs(b - a));
}

// 一根的成本: 前根收 c0 → 本根收 c, 分层目标 q → 刀 x, 实际仓位 p → w (本根收盘成交); 回测 Pnl 与优化目标 Split 同一份
inline Costs costs_of(const Setup &u, const Ladder &m, float c0, float c, float q, const Cut &x, float p, float w) {
  const double dw = std::fabs(w - p);
  return {u.commission * dw, u.stamp * std::max(0.0f, p - w), u.impact * dw, gap_of(m, c0, c, q, x, w - p)};
}

struct Run {
  Col w;                    // [n] 每根收盘调到的仓位
  std::vector<double> g, r; // [n] 每根毛 / 净收益
  std::vector<Costs> k;     // [n] 每根成本分项
  double turn = 0;          // Σ 换手
  Costs cost;               // Σ 成本分项
};

// 仓位层核: 目标仓位 → 实际仓位
struct Size {
  const Tape &t;
  const Setup &u;
  const double target; // 每根目标波动
  float p = 0;         // 持仓
  Size(const Tape &t, const Setup &u) : t(t), u(u), target(u.vol / std::sqrt(u.bpy)) {}
  float operator()(size_t i, float w) {
    if (u.vol > 0 && t.vol[i] > 0)
      w *= static_cast<float>(std::min(1.0, target / t.vol[i]));
    if (p * w > 0 && std::fabs(w - p) < u.buffer)
      w = p;
    return p = w;
  }
};

// 损益核: 一刀 x + 实际仓位 w → 逐根毛 / 成本 / 净 (攒进 run)
struct Pnl {
  const Tape &t;
  const Setup &u;
  const Ladder m;
  float q = 0, p = 0; // 前根分层目标 / 持仓
  Run run;
  Pnl(const Tape &t, const Setup &u, const Ladder &m)
      : t(t), u(u), m(m), run{Col(t.n), std::vector<double>(t.n), std::vector<double>(t.n), std::vector<Costs>(t.n)} {}
  void operator()(size_t i, const Cut &x, float w) {
    const Costs k = costs_of(u, m, t.c[i ? i - 1 : 0], t.c[i], q, x, p, w);
    run.w[i] = w, run.g[i] = p * t.a[i], run.r[i] = run.g[i] - k.sum(), run.k[i] = k, run.turn += std::fabs(w - p), run.cost += k;
    q = x.w, p = w;
  }
};

// cut = 刀列 (层线 NaN = 无线, 只按收盘); m = 刀列所用的分层
inline Run simulate(const Tape &t, const Cuts &cut, const Ladder &m, const Setup &u) {
  assert(cut.w.size() == t.n && "刀列长度");
  Size size(t, u);
  Pnl pnl(t, u, m);
  for (size_t i = 0; i < t.n; ++i) {
    const Cut x = cut.at(i);
    pnl(i, x, size(i, x.w));
  }
  return std::move(pnl.run);
}

} // namespace cta
