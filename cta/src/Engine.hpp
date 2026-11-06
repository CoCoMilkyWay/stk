#pragma once

// 单资产回测: 模型逐根看收盘给目标仓位, 即按收盘价调到 (冲击计入费率)
//   每根收益 = p · (收 / 前收 − 1) − 单边费率 × |w − p|; p = 前根收盘调到的仓位, w = 本根收盘调到的
//   仓位层 (模型之后): 波动率定仓 w · min(1, 目标 / σ̂) → 不交易带 (两边都持仓且 |Δw| < 带宽 不调)
#include "Model.hpp"
#include "Panel.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

struct Setup {
  double cost = 0.001; // 单边费率
  double vol = 0;      // 目标年化波动 (0 = 不调)
  double buffer = 0;   // 不交易带宽
  double bpy = 244;    // 每年根数
};

// 一个资产的有效根 (跳过无数据的根)
struct Series {
  std::vector<Bar> bar;
  std::vector<uint32_t> t; // [i] → 张量时间下标
};

inline Series series_of(const Panel &p, size_t b) {
  Series s;
  for (size_t t = 0; t < p.T; ++t) {
    const float *x = p.at(b, t);
    const float c = x[kClose];
    if (!(c > 0))
      continue;
    const auto ok = [&](float v) { return v > 0 ? v : c; };
    s.bar.push_back({ok(x[kOpen]), std::max(ok(x[kHigh]), c), std::min(ok(x[kLow]), c), c});
    s.t.push_back(static_cast<uint32_t>(t));
  }
  return s;
}

// 一段在场: 建仓根 / 清仓根 (序列下标, 均为收盘) + 段内净收益 (复利, 含两头成本)
struct Trade {
  size_t i0, i1;
  double ret;
};

struct Run {
  std::vector<double> r;     // [n] 每根净收益 (根 0 = 0)
  std::vector<double> gross; // [n] 每根毛收益
  std::vector<float> w;      // [n] 每根收盘调到的仓位
  std::vector<Trade> trades;
  double turn = 0; // Σ 换手
};

inline Run simulate(const Series &s, Model &m, const Setup &u) {
  const size_t n = s.bar.size();
  Run run;
  run.r.assign(n, 0), run.gross.assign(n, 0), run.w.assign(n, 0);
  Vol vol(kVolSpan);
  const double target = u.vol / std::sqrt(u.bpy); // 每根目标波动
  float p = 0;                                    // 持仓
  double nav = 1;                                 // 在场段累计
  size_t i0 = n;                                  // 在场段建仓根 (n = 不在场)
  for (size_t i = 0; i < n; ++i) {
    const Bar &b = s.bar[i];
    double r = 0;
    if (p > 0) {
      r = run.gross[i] = p * (b.c / s.bar[i - 1].c - 1);
      nav *= 1 + r;
    }
    vol.push(b.c);
    float w = m.step(b);
    if (u.vol > 0 && vol.v() > 0)
      w *= static_cast<float>(std::min(1.0, target / vol.v()));
    if (p > 0 && w > 0 && std::fabs(w - p) < u.buffer)
      w = p;
    const double dw = std::fabs(w - p);
    r -= u.cost * dw, run.turn += dw;
    if (i0 == n && w > 0)
      i0 = i, nav = 1;
    if (i0 < n)
      nav *= 1 - u.cost * dw;
    if (i0 < n && w == 0)
      run.trades.push_back({i0, i, nav - 1}), i0 = n;
    p = w;
    run.r[i] = r, run.w[i] = p;
  }
  if (i0 < n)
    run.trades.push_back({i0, n - 1, nav - 1});
  return run;
}
