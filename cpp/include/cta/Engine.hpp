#pragma once

#include "define/Fee.hpp"

// 单资产执行: 逐根链 drive (日 × 根) = 源 (日首 day(d), 逐根出刀) → 成交 Fill (前根仓位 + 本根的刀 + 成本) → 汇 (逐根收 Fill 的可调用);
//   仓位 = 刀的目标仓位, 按本根收盘价调到; 调参 / 策略 / 买入持有同一条链, 只换源与汇:
//     源: Hold (买入持有) / Laddered (完整分层: 减仓线核 + 分层 Ladder)
//     跟随调参 = Laddered (定参, 录逐笔) → 空汇 (Tune.hpp); 策略 = Laddered (变参) → Run + Cuts (Trace.hpp); 买入持有 = Hold → Run; 减仓调参只看中线 (mid_of, 不走链)
//   成本无状态, 前根仓位只在 drive 里记; 汇只读 Fill + Tape
//   仓位 w ∈ [−1, 1]: 正 = 多, 负 = 空
//   损益: 实际按本根收盘成交, 净 r = p · a − 佣金 − 印花税 − 冲击; p = 前根收盘调到的仓位, w = 本根收盘调到的, a = 资产本根收益
//         毛 = 按档线价成交的理想毛 = p · a + 越线差 (收盘成交比档线价成交少挣的已在 p · a 里, 越线差把它拆出来), 故 净 = 毛 − 五项成本
//   成本 (占净值比例) 分五项 Costs 逐根记, 五项之和 = 毛 − 净:
//     佣金 = 佣金率 × |w − p|; 印花税 = 印花税率 × 卖出量 max(p − w, 0) (多头减仓 / 融券卖出); 冲击 = 冲击率 × |w − p| (固定, 不按金额变)
//     越线差 = 刀的震荡 / 跳空 (分层按档线定的成交, 见 Model.hpp)
#include "cta/Model.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <numeric>
#include <vector>

namespace cta {

// 成本分项 (下标; 序 = 净值图上自策略净值向上的叠放序)
enum : size_t { kCommission,
                kStamp,
                kImpact,
                kChop,
                kGap,
                kNC };

// 成本 (占净值比例): 一根 / 累计 / 年化同一形, [项]
struct Costs : std::array<double, kNC> {
  double sum() const { return std::accumulate(begin(), end(), 0.0); }
};

struct CostItem {
  const char *name, *tip;
};
inline constexpr CostItem kCosts[kNC] = {{"佣金", "佣金率 × |Δ仓位|"},
                                         {"印花税", "印花税率 × 卖出量 (多头减仓 / 融券卖出)"},
                                         {"冲击", "冲击率 × |Δ仓位|"},
                                         {"震荡", "越线差的震荡部分: 分层按收盘成交比按档线价成交多付的差, 每份都按离收盘最近那档的差计 (只越过一档 = 全归震荡)"},
                                         {"跳空", "越线差的跳空部分: 一根越过几档时, 远处的档比最近那档多付的部分"}};

// 一根的成本: 前根仓位 p → 本根的刀 x 的仓位 (本根收盘成交)
inline Costs costs_of(const Fee &u, const Cut &x, float p) {
  const double dw = std::fabs(x.w - p), sell = std::max(0.0f, p - x.w);
  const CostParts c = u.parts(0.0, dw, dw - sell, sell); // 一刀 = 买 / 卖两侧之一, 全量付固定冲击
  return {{c.v[kCostTax], c.v[kCostStamp], c.v[kCostImpact], x.chop, x.gap}};
}

// 一根成交: 前根仓位 p → 本根的刀 x (仓位 x.w) + 本根成本 k
struct Fill {
  size_t i = 0;
  float p = 0;
  Cut x;
  Costs k;
};

// ---- 源 (日首 day(d) + 逐根出刀) ----

// 买入持有: 无档线, 恒满多
struct Hold {
  void day(size_t) {}
  Cut operator()(size_t) const { return {.w = 1}; }
};

// 根 i 的中线: 减仓线核 c 在本根参数 p (以根计) 下的出值, 统一往逆向挪一个 band (band 无值 = NaN)
template <class K>
float mid_of(K &c, size_t i, const P &p) { return c.bar(i, p) - c.s * c.G[i]; }

// 完整分层: 减仓线核 c (日 d 用 pr[d]) → 分层 lad (本根成交的份的跟随线用本根的 pf, 之后不变); 参数为轴的单位, 逐根经日内过渡 (Ramp);
//   未选参日 (on[d] = 0) = Hold (分层复位), 刀未就绪 = Hold; 核逐根都走 (状态接续)
template <class K, class F>
struct Laddered {
  K c;
  Ladder<F> lad;
  const std::vector<P> &pr, &pf;  // [日]
  const std::vector<uint8_t> &on; // [日]
  Ramp<P> rr{}, rf{};             // 减仓 / 跟随参数的日内过渡 (以根计)
  size_t d = 0;
  void day(size_t x) {
    d = x, rr.day(K::kind.bars(pr[x], c.t.S)), rf.day(F::kind.bars(pf[x], c.t.S)), lad.day(x);
    if (!on[x])
      lad.reset(c.t.days.off[x]);
  }
  Cut operator()(size_t i) {
    const float m = mid_of(c, i, rr.bar(c.t.f[i]));
    const P &p = rf.bar(c.t.f[i]);
    const Cut x = on[d] ? lad(i, m, p) : Cut{};
    return x.h > 0 ? x : Hold{}(i);
  }
};

// ---- 逐根链 ----

// 逐日 src.day(d) → 逐根 src 出刀 → 成交 (成本按费率 u + 刀的越线差) → sink(Fill); 前根起始 = 空仓
template <class Source, class Sink>
void drive(const Tape &t, const Fee &u, Source &&src, Sink &&sink) {
  assert(t.days.off.back() == t.n && "日轴覆盖全部根");
  float p = 0;
  for (size_t d = 0; d < t.days.size(); ++d) {
    src.day(d);
    for (size_t i = t.days.off[d]; i < t.days.off[d + 1]; ++i) {
      const Cut x = src(i);
      sink(Fill{i, p, x, costs_of(u, x, p)});
      p = x.w;
    }
  }
}

// 一条链的损益列 (put 逐根收 Fill): 毛 = 净 + 成本之和
struct Run {
  Col w;                 // [n] 每根收盘调到的仓位
  std::vector<double> r; // [n] 每根净收益
  std::vector<Costs> k;  // [n] 每根成本分项
  Run() = default;
  explicit Run(size_t n) : w(n), r(n), k(n) {}
  void put(const Tape &t, const Fill &f) { w[f.i] = f.x.w, k[f.i] = f.k, r[f.i] = f.p * t.a[f.i] + f.k[kChop] + f.k[kGap] - f.k.sum(); }
};

} // namespace cta
