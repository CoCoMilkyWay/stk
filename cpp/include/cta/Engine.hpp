#pragma once

// 单资产回测: 目标仓位列 → 仓位层 → 逐根损益; 仓位按本根收盘价调到 (冲击计入费率)
//   每根毛收益 g = p · a, 净 r = g − 单边费率 × |w − p|; p = 前根收盘调到的仓位, w = 本根收盘调到的, a = 资产本根收益
//   仓位层 (模型之后): 波动率定仓 w · min(1, 目标 / σ̂) → 不交易带 (两边都持仓且 |Δw| < 带宽 不调)
#include "cta/Model.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
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

inline Run simulate(Tape &t, Col w, const Setup &u) {
  assert(w.size() == t.n && "仓位列长度");
  const Col &a = t.a, &s = t.vol(kVolSpan);
  const double target = u.vol / std::sqrt(u.bpy); // 每根目标波动
  Run run{std::move(w), std::vector<double>(t.n), std::vector<double>(t.n)};
  float p = 0; // 持仓
  for (size_t i = 0; i < t.n; ++i) {
    float x = run.w[i];
    if (u.vol > 0 && s[i] > 0)
      x *= static_cast<float>(std::min(1.0, target / s[i]));
    if (p > 0 && x > 0 && std::fabs(x - p) < u.buffer)
      x = p;
    const double dw = std::fabs(x - p);
    run.g[i] = p * a[i], run.r[i] = run.g[i] - u.cost * dw, run.turn += dw;
    run.w[i] = p = x;
  }
  return run;
}

} // namespace cta
