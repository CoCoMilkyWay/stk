#pragma once

// 单资产执行: 逐根链 drive (日 × 根) = 源 (日首 day(d), 逐根出刀) → 仓位层 Size (目标仓位 → 实际仓位) → 成交 Fill (前根 / 本根仓位 + 成本 + 本根的刀) → 汇 (逐根收 Fill, 日末 close(d));
//   仓位按本根收盘价调到; 调参 / 策略 / 买入持有同一条链, 只换源与汇:
//     调参 = Switch → Capture (日前缀两项, Tune.hpp); 策略 = Laddered → Pnl + Record (录刀, Trace.hpp); 买入持有 = Hold → Pnl
//   Size / 成本无状态, 前根的刀与实际仓位只在 drive 里记; 汇只读 Fill + Tape
//   仓位 w ∈ [−1, 1]: 正 = 多, 负 = 空
//   仓位层: 波动率定仓 w · min(1, 目标 / σ̂) → 不交易带 (前后同向持仓且 |Δw| < 带宽 不调)
//   损益: 实际按本根收盘成交, 净 r = p · a − 佣金 − 印花税 − 冲击; p = 前根收盘调到的仓位, w = 本根收盘调到的, a = 资产本根收益
//         毛 g = 按档线价成交的理想毛 = p · a + 越线差 (收盘成交比档线价成交少挣的已在 p · a 里, 越线差把它拆出来), 故 净 = 毛 − 五项成本
//   成本 (占净值比例) 分五项逐根记, 五项之和 = 毛 − 净:
//     佣金 = 佣金率 × |w − p|; 印花税 = 印花税率 × 卖出量 max(p − w, 0) (多头减仓 / 融券卖出); 冲击 = 冲击率 × |w − p| (固定, 不按金额变)
//     越线差 = 刀的震荡 / 跳空 (分层按档线定的成交, 见 Model.hpp), 按实际调仓占分层调仓的比例计; 实际调仓与分层同向才计 (波动率定仓 / 不交易带改量不改档)
#include "cta/Model.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <iterator>
#include <vector>

namespace cta {

// 成本率
struct Rates {
  double commission = 0; // 佣金率 (买卖各一次)
  double stamp = 0;      // 印花税率 (卖出单边)
  double impact = 0;     // 冲击率 (买卖各一次)
  bool operator==(const Rates &) const = default;
};

// 仓位层设定; 默认 = 不调 (买入持有)
struct Sizing {
  float vol = 0;    // 目标年化波动 (0 = 不调)
  float buffer = 0; // 不交易带宽
  bool operator==(const Sizing &) const = default;
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

// 一根的成本: 刀 z → x (分层仓位 z.w → x.w), 实际仓位 p → w (本根收盘成交)
inline Costs costs_of(const Rates &u, const Cut &z, const Cut &x, float p, float w) {
  const double dw = std::fabs(w - p), dl = x.w - z.w;
  Costs k{u.commission * dw, u.stamp * std::max(0.0f, p - w), u.impact * dw};
  if ((w - p) * dl > 0) {
    const double f = dw / std::fabs(dl);
    k.chop = f * x.chop, k.gap = f * x.gap;
  }
  return k;
}

// 仓位层核: 目标仓位 w + 前根实际仓位 p → 本根实际仓位; Size{t} = 不调
struct Size {
  const Tape &t;
  double target = 0, buffer = 0; // 每根目标波动 (0 = 不调) / 不交易带宽
  // 设定 z, bpy = 每年根数
  static Size of(const Tape &t, const Sizing &z, double bpy) { return {t, z.vol / std::sqrt(bpy), z.buffer}; }
  float operator()(size_t i, float w, float p) const {
    if (target > 0 && t.vol[i] > 0)
      w *= static_cast<float>(std::min(1.0, target / t.vol[i]));
    return p * w > 0 && std::fabs(w - p) < buffer ? p : w;
  }
};

// 一根成交: 实际仓位 p (前根) / w (本根) + 本根成本 k + 本根的刀 x
struct Fill {
  size_t i = 0;
  float p = 0, w = 0;
  Costs k;
  Cut x;
};

// 逐根链: 逐日 src.day(d) → 逐根 src 出刀 → 仓位层 size → 成交 (成本按费率 u + 刀的越线差) → 汇 sink; 日末 sink.close(d)
//   前根起始 = 空刀 + 空仓
template <class Source, class Sink>
void drive(const Tape &t, const Rates &u, const Size &size, Source &&src, Sink &&sink) {
  assert(t.days.off.back() == t.n && "日轴覆盖全部根");
  Cut z;
  float p = 0;
  for (size_t d = 0; d < t.days.size(); ++d) {
    src.day(d);
    for (size_t i = t.days.off[d]; i < t.days.off[d + 1]; ++i) {
      const Cut x = src(i);
      const float w = size(i, x.w, p);
      sink(Fill{i, p, w, costs_of(u, z, x, p, w), x});
      z = x, p = w;
    }
    sink.close(d);
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
  void close(size_t) {}
};

// 录刀汇: 逐根的刀写进轨迹列
struct Record {
  Cuts &cut;
  void operator()(const Fill &f) { cut.put(f.i, f.x); }
  void close(size_t) {}
};

// 并联两汇: 同一条链同时交给 a / b
template <class A, class B>
struct Tee {
  A &a;
  B &b;
  void operator()(const Fill &f) { a(f), b(f); }
  void close(size_t d) { a.close(d), b.close(d); }
};

} // namespace cta
