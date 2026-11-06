#pragma once

// 单资产回测: 收盘出单, 次根开盘调仓 (隔夜跳空由旧仓承担)
//   止损两种判据 (Stop):
//     Close = 收 ≤ 止损价 → 本根收盘清仓 (收盘竞价); 篮子的高 / 低是成员高低的加权外包, 比真实路径宽, 按低价判会多触发
//     Intra = 低 ≤ 止损价 → 盘中在 min(开, 止损价) 清仓 (跳空穿价 = 开盘价)
//   每根收益 = (1 + p · (开 / 前收 − 1)) · (1 + w · (出价 / 开 − 1)) − 1 − 成本; p = 前根收盘后仓位, w = 本根开盘调到的仓位
//   成本 = 单边费率 × 换手 (开盘调仓 |w − p| + 止损清仓 w)
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

// 一段在场 (仓位 > 0 的连续根): 起止根 (序列下标) + 段内净收益 (复利)
struct Trade {
  size_t i0, i1;
  double ret;
};

struct Run {
  std::vector<double> r;     // [n] 每根净收益 (根 0 = 0)
  std::vector<double> gross; // [n] 每根毛收益
  std::vector<float> w;      // [n] 每根收盘后仓位
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
  float p = 0;
  double nav = 1; // 在场段累计
  size_t i0 = n;  // 在场段起根 (n = 不在场)
  for (size_t i = 0; i < n; ++i) {
    const Bar &b = s.bar[i];
    if (i > 0) {
      const float w = o.w;
      const bool hit = w > 0 && o.stop > 0 && (u.stop == Stop::Close ? b.c <= o.stop : b.l <= o.stop);
      const double x = !hit || u.stop == Stop::Close ? b.c : std::min(b.o, o.stop);
      const double g = (1 + p * (b.o / s.bar[i - 1].c - 1)) * (1 + w * (x / b.o - 1)) - 1;
      const double dw = std::fabs(w - p) + (hit ? w : 0);
      run.gross[i] = g, run.r[i] = g - u.cost * dw, run.turn += dw;
      if (i0 == n && w > 0)
        i0 = i, nav = 1;
      if (i0 < n)
        nav *= 1 + run.r[i];
      p = hit ? 0 : w;
      if (i0 < n && p == 0)
        run.trades.push_back({i0, i, nav - 1}), i0 = n;
    }
    run.w[i] = p;
    vol.push(b.c);
    o = m.step(b, p);
    if (u.vol > 0 && vol.v() > 0)
      o.w *= static_cast<float>(std::min(1.0, target / vol.v()));
    if (p > 0 && o.w > 0 && std::fabs(o.w - p) < u.buffer)
      o.w = p;
  }
  if (i0 < n)
    run.trades.push_back({i0, n - 1, nav - 1});
  return run;
}
