#pragma once

// 单资产回测, 两个逐根核: 仓位层 Size (目标仓位 → 实际仓位) ↔ 损益 Pnl (实际仓位 → 逐根毛 / 净); 仓位按本根收盘价调到 (冲击计入费率)
//   仓位层: 波动率定仓 w · min(1, 目标 / σ̂) → 不交易带 (两边都持仓且 |Δw| < 带宽 不调)
//   损益: 每根毛收益 g = p · a, 净 r = g − 单边费率 × |w − p|; p = 前根收盘调到的仓位, w = 本根收盘调到的, a = 资产本根收益
//   simulate = 目标仓位列逐根过 Size → Pnl
#include "cta/Model.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <utility>
#include <vector>

namespace cta {

struct Setup {
  double cost = 0.001; // 单边费率
  double vol = 0;      // 目标年化波动 (0 = 不调)
  double buffer = 0;   // 不交易带宽
  double bpy = 244;    // 每年根数
};

struct Run {
  Col w;                    // [n] 每根收盘调到的仓位
  std::vector<double> g, r; // [n] 每根毛 / 净收益
  double turn = 0;          // Σ 换手
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
    if (p > 0 && w > 0 && std::fabs(w - p) < u.buffer)
      w = p;
    return p = w;
  }
};

// 损益核: 实际仓位 → 逐根毛 / 净 (攒进 run)
struct Pnl {
  const Tape &t;
  const Setup &u;
  float p = 0; // 持仓
  Run run;
  Pnl(const Tape &t, const Setup &u) : t(t), u(u), run{Col(t.n), std::vector<double>(t.n), std::vector<double>(t.n)} {}
  void operator()(size_t i, float w) {
    const double dw = std::fabs(w - p);
    run.w[i] = w, run.g[i] = p * t.a[i], run.r[i] = run.g[i] - u.cost * dw, run.turn += dw;
    p = w;
  }
};

inline Run simulate(const Tape &t, const Col &w, const Setup &u) {
  assert(w.size() == t.n && "仓位列长度");
  Size size(t, u);
  Pnl pnl(t, u);
  for (size_t i = 0; i < t.n; ++i)
    pnl(i, size(i, w[i]));
  return std::move(pnl.run);
}

} // namespace cta
