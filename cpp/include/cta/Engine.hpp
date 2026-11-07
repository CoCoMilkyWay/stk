#pragma once

// 单资产执行: 逐根链 drive = 源 (逐根出刀) → 仓位层 Size (目标仓位 → 实际仓位) → 成交 Fill (前根 / 本根 + 成本) → 汇 (损益 Pnl ↔ 目标 Split); 仓位按本根收盘价调到
//   核 (源 / Size / 成本) 无状态, 前根的刀与实际仓位只在 drive 里记; 汇只读 Fill + Tape, 日末 seal
//   仓位 w ∈ [−1, 1]: 正 = 多, 负 = 空
//   仓位层: 波动率定仓 w · min(1, 目标 / σ̂) → 不交易带 (前后同向持仓且 |Δw| < 带宽 不调)
//   损益: 实际按本根收盘成交, 净 r = p · a − 佣金 − 印花税 − 冲击; p = 前根收盘调到的仓位, w = 本根收盘调到的, a = 资产本根收益
//         毛 g = 按层线价成交的理想毛 = p · a + 越线差 (收盘成交比线价成交少挣的已在 p · a 里, 越线差把它拆出来), 故 净 = 毛 − 五项成本
//   成本 (占净值比例) 分五项逐根记, 五项之和 = 毛 − 净:
//     佣金 = 佣金率 × |w − p|; 印花税 = 印花税率 × 卖出量 max(p − w, 0) (多头减仓 / 融券卖出); 冲击 = 冲击率 × |w − p| (固定, 不按金额变)
//     越线差 = 按本根收盘成交相对按层线价成交的差: 分层目标越过的层线 L (本根刀的线价) 被收盘价穿过 (前根收与本根收在 L 两侧) 才计, 每线 |收 / L − 1|;
//              实际调仓与分层同向才计, 量均摊到越过的各线 (波动率定仓 / 不交易带改量不改线); 线随带移过价 / 带未就绪 = 按收盘决策, 不计
//       分两项: 震荡 = 每线都按离收盘最近那条线的越线差计 (只越过一条线 = 全归震荡); 跳空 = 各线多出的整档 (一根越过几条线, 远处的线比最近那条多越的部分)
#include "cta/Model.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iterator>
#include <vector>

namespace cta {

// 成本率
struct Rates {
  double commission = 0; // 佣金率 (买卖各一次)
  double stamp = 0;      // 印花税率 (卖出单边)
  double impact = 0;     // 冲击率 (买卖各一次)
};

// 仓位层设定; 默认 = 不调 (买入持有)
struct Sizing {
  double vol = 0;    // 目标年化波动 (0 = 不调)
  double buffer = 0; // 不交易带宽
};

struct Setup {
  Rates rates;
  Sizing sizing;
  double bpy = 244; // 每年根数
};

// 成本分项 (占净值比例): 一根 / 累计同一形
struct Costs {
  double commission = 0, stamp = 0, impact = 0, chop = 0, gap = 0;
  double sum() const { return commission + stamp + impact + chop + gap; }
  Costs &operator+=(const Costs &o) {
    commission += o.commission, stamp += o.stamp, impact += o.impact, chop += o.chop, gap += o.gap;
    return *this;
  }
};

// 成本分项表 (序 = 净值图上自策略净值向上的叠放序)
struct CostItem {
  const char *name;
  double Costs::*f;
};
inline constexpr CostItem kCosts[] = {
    {"佣金", &Costs::commission}, {"印花税", &Costs::stamp}, {"冲击", &Costs::impact}, {"震荡", &Costs::chop}, {"跳空", &Costs::gap}};
inline constexpr size_t kNC = std::size(kCosts);

// 越线差 → 震荡 / 跳空 (见文件头) 写进 k: 前根收 c0 → 本根收 c, 刀 z → x (分层 lad), 实际调仓 dw
inline void cross_of(const Ladder &lad, float c0, float c, const Cut &z, const Cut &x, float dw, Costs &k) {
  const long a = std::lround(z.w * static_cast<float>(lad.N)), b = std::lround(x.w * static_cast<float>(lad.N));
  if (!ready(x) || a == b || dw * static_cast<float>(b - a) <= 0)
    return;
  double s = 0, near = 0;
  long hit = 0;
  for (long j = std::min(a, b); j < std::max(a, b); ++j) {
    const double L = lad.line(static_cast<double>(j + 1 > 0 ? j + 1 : j) / lad.N, x); // 相邻两层之间的线 = 离中线远的那层
    if ((c0 - L) * (c - L) < 0) {
      const double o = std::fabs(c / L - 1);
      s += o, near = hit++ ? std::min(near, o) : o;
    }
  }
  const double v = std::fabs(dw) / static_cast<double>(std::labs(b - a)); // 每线份额
  k.chop = v * static_cast<double>(hit) * near, k.gap = v * (s - static_cast<double>(hit) * near);
}

// 一根的成本: 前根收 c0 → 本根收 c, 刀 z → x, 实际仓位 p → w (本根收盘成交)
inline Costs costs_of(const Rates &u, const Ladder &lad, float c0, float c, const Cut &z, const Cut &x, float p, float w) {
  const double dw = std::fabs(w - p);
  Costs k{u.commission * dw, u.stamp * std::max(0.0f, p - w), u.impact * dw};
  cross_of(lad, c0, c, z, x, w - p, k);
  return k;
}

// 仓位层核: 目标仓位 w + 前根实际仓位 p → 本根实际仓位
struct Size {
  const Tape &t;
  const double target, buffer; // 每根目标波动 (0 = 不调) / 不交易带宽
  Size(const Tape &t, const Sizing &z, double bpy) : t(t), target(z.vol / std::sqrt(bpy)), buffer(z.buffer) {}
  float operator()(size_t i, float w, float p) const {
    if (target > 0 && t.vol[i] > 0)
      w *= static_cast<float>(std::min(1.0, target / t.vol[i]));
    return p * w > 0 && std::fabs(w - p) < buffer ? p : w;
  }
};

// 一根成交: 前根 / 本根的刀 z / x + 实际仓位 p / w + 本根成本 k
struct Fill {
  size_t i = 0;
  Cut z, x;
  float p = 0, w = 0;
  Costs k;
};

// 逐根链: 源 src (Signal / Cuts / Hold) 逐根出刀 → 仓位层 size → 成交 (成本按分层 lad 与费率 u) → 汇 sink (Pnl / Split), 日末 sink.seal()
//   前根起始 = 未就绪的刀 + 空仓
template <class Source, class Sink>
void drive(const Tape &t, const Days &days, const Ladder &lad, const Rates &u, const Size &size, const Source &src, Sink &sink) {
  assert(days.off.back() == t.n && "日轴覆盖全部根");
  Fill f;
  for (size_t d = 0; d < days.size(); ++d) {
    for (size_t i = days.off[d]; i < days.off[d + 1]; ++i) {
      const Cut x = src(i);
      const float w = size(i, x.w, f.w);
      f = {i, f.x, x, f.w, w, costs_of(u, lad, t.c[i ? i - 1 : 0], t.c[i], f.x, x, f.w, w)};
      sink(f);
    }
    sink.seal();
  }
}

struct Run {
  Col w;                    // [n] 每根收盘调到的仓位
  std::vector<double> g, r; // [n] 每根毛 (按层线价成交) / 净收益
  std::vector<Costs> k;     // [n] 每根成本分项
  double turn = 0;          // Σ 换手
  Costs cost;               // Σ 成本分项
};

// 损益汇: 逐根毛 / 成本 / 净 (攒进 run)
struct Pnl {
  const Tape &t;
  Run run;
  explicit Pnl(const Tape &t) : t(t), run{Col(t.n), std::vector<double>(t.n), std::vector<double>(t.n), std::vector<Costs>(t.n)} {}
  void operator()(const Fill &f) {
    const size_t i = f.i;
    run.w[i] = f.w, run.g[i] = f.p * t.a[i] + f.k.chop + f.k.gap, run.r[i] = run.g[i] - f.k.sum(), run.k[i] = f.k;
    run.turn += std::fabs(f.w - f.p), run.cost += f.k;
  }
  void seal() {} // 逐根记, 无日末项
};

} // namespace cta
