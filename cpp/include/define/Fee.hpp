#pragma once

#include "define/Label.hpp" // AmtId

// 交易成本 = 量 (TurnFlow) × 率 (Fee) → 钱 (CostParts); 全仓唯一一式 Fee::parts (因子换手 / 组合与篮子净线 / CTA / 实盘调仓试算 / 账户日内回推同用)
//   成交两侧 Side (下标; Stat 账集流水 / 冲击列 / 费率同一下标); 实盘的方向 ±1 经 live::side_of 映到它
//   税佣: 佣金双边 + 过户费双边 + 印花税卖出单边; 冲击: 金额段 a 的固定段 = 全量 × 固定冲击 Fee::impact,
//   盘口段 = 真实冲击 Σ 量·冲击 (Lob 库建仓冲击列, 封顶 CostSpec::top) + 卖不出的量 stuck × 固定冲击
enum Side : int { kBuy = 0,
                  kSell = 1 };
inline constexpr int kSides = 2;
inline constexpr int kLobAmts = kAmtCount - 1; // 盘口段数 (AmtId a ≥ 1 ↔ 下标 a − 1)

// 成本分项 (下标; CTA 净值图叠放序同此)
enum CostPart : int { kCostTax,
                      kCostStamp,
                      kCostImpact };
inline constexpr int kCostParts = 3;
struct CostParts {
  double v[kCostParts] = {};
  double sum() const { return v[kCostTax] + v[kCostStamp] + v[kCostImpact]; }
  CostParts &operator+=(const CostParts &o) {
    for (int k = 0; k < kCostParts; ++k)
      v[k] += o.v[k];
    return *this;
  }
};

// 两侧流水 (量; 可合并): 成交量 vol + 卖不出 (封跌停) 的卖出量 stuck + 各盘口段的真实冲击和 imp; 各金额段同一份量, 成本按段现乘
struct TurnFlow {
  double vol[kSides] = {};
  double stuck = 0.0;
  double imp[kLobAmts][kSides] = {};
  void merge(const TurnFlow &o) {
    stuck += o.stuck;
    for (int s = 0; s < kSides; ++s) {
      vol[s] += o.vol[s];
      for (int k = 0; k < kLobAmts; ++k)
        imp[k][s] += o.imp[k][s];
    }
  }
  // 金额段 a 一侧: 真实冲击和 / 付固定冲击的量
  double impact(AmtId a, Side s) const { return a == kAmtFixed ? 0.0 : imp[a - 1][s]; }
  double fixed(AmtId a, Side s) const { return a == kAmtFixed ? vol[s] : s == kSell ? stuck
                                                                                    : 0.0; }
};

struct Fee {
  double commission = 1e-4; // 佣金 万 1, 双边各一次
  double stamp = 5e-4;      // 印花税 万 5, 卖出单边
  double transfer = 1e-5;   // 过户费 0.001%, 双边各一次
  double impact = 0.0020;   // 固定冲击 (模拟值, 不按金额变), 两侧各一次
  bool operator==(const Fee &) const = default;
  // 一侧的税佣率 / 全部费率 (税佣 + 固定冲击)
  double tax(Side s) const { return commission + transfer + (s == kSell ? stamp : 0.0); }
  double rate(Side s) const { return tax(s) + impact; }
  // 流水 f 在金额段 a 下一侧 / 两侧的成本分项
  CostParts parts(const TurnFlow &f, AmtId a, Side s) const {
    return {{f.vol[s] * (commission + transfer), s == kSell ? f.vol[s] * stamp : 0.0, f.impact(a, s) + f.fixed(a, s) * impact}};
  }
  CostParts parts(const TurnFlow &f, AmtId a) const {
    CostParts c = parts(f, a, kBuy);
    c += parts(f, a, kSell);
    return c;
  }
  double cost(const TurnFlow &f, AmtId a) const { return parts(f, a).sum(); }
  // 一侧的平均费率 (显示); 无量 → 0
  double rate(const TurnFlow &f, AmtId a, Side s) const { return f.vol[s] > 0.0 ? parts(f, a, s).sum() / f.vol[s] : 0.0; }
};

// 成本口径 (Config 持有一份; 因子评估 / 组合 / Agent 同读): 金额段 amt (固定段 = 全量 × Fee::impact; 盘口段 = Lob 库 L_1min 的买 / 卖冲击列,
//   分钟各层按行末分钟取, 日层收盘撮合建仓 = 冲击 0; 要装平面时评估域并入 Lob 库) + 盘口冲击封顶 top (比率; 账集 Book 逐格 min(冲击, top))
struct CostSpec {
  AmtId amt = kAmtFixed;
  double top = 0.01;
  bool operator==(const CostSpec &) const = default;
};
