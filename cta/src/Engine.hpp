#pragma once

// 单资产回测: 收盘出单即按收盘价调仓 (冲击计入费率)
//   止损两种判据 (Stop):
//     Close = 收 ≤ 止损价 → 本根收盘清仓 (收盘竞价); 篮子的高 / 低是成员高低的加权外包, 比真实路径宽, 按低价判会多触发
//     Intra = 低 ≤ 止损价 → 盘中在 min(开, 止损价) 清仓 (跳空穿价 = 开盘价)
//   每根收益 = p · (出价 / 前收 − 1) − 成本; p = 前根收盘调到的仓位, 出价 = 收 (Intra 止损 = 止损成交价)
//   成本 = 单边费率 × 换手 (收盘调仓 |w − p| + 止损清仓 p)
//   仓位层 (模型之后): 波动率定仓 w · min(1, 目标 / σ̂) → 不交易带 (两边都持仓且 |Δw| < 带宽 不调)
#include "Model.hpp"
#include "Panel.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

enum class Stop { Close,
                  Intra };

struct Setup {
  double cost = 0.001; // 单边费率
  double vol = 0;      // 目标年化波动 (0 = 不调)
  double buffer = 0;   // 不交易带宽
  Stop stop = Stop::Close;
  double bpy = 244; // 每年根数
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
  Order o;
  float p = 0;    // 持仓
  double nav = 1; // 在场段累计
  size_t i0 = n;  // 在场段建仓根 (n = 不在场)
  for (size_t i = 0; i < n; ++i) {
    const Bar &b = s.bar[i];
    double r = 0;
    if (p > 0) {
      const bool hit = o.stop > 0 && (u.stop == Stop::Close ? b.c <= o.stop : b.l <= o.stop);
      const double x = !hit || u.stop == Stop::Close ? b.c : std::min(b.o, o.stop);
      run.gross[i] = p * (x / s.bar[i - 1].c - 1);
      r = run.gross[i] - (hit ? u.cost * p : 0);
      nav *= 1 + r;
      if (hit) {
        run.turn += p;
        run.trades.push_back({i0, i, nav - 1}), i0 = n, p = 0;
      }
    }
    vol.push(b.c);
    o = m.step(b, p);
    float w = o.w;
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
