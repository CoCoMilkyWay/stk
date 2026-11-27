#pragma once

#include "define/Label.hpp" // AmtId

// 交易费率 (比率; 全仓唯一一份: Config 持有, 因子换手成本 / 组合与篮子净线 / CTA / 实盘调仓试算 / 账户日内回推同用)
//   成交两侧 Side (下标; Stat 账集流水 / 冲击列 / 费率同一下标); 实盘的方向 ±1 经 live::side_of 映到它
//   税佣: 佣金双边 + 过户费双边 + 印花税卖出单边; 冲击 impact = 固定冲击 (无盘口冲击列时两侧都按它)
//   成本唯一一式 parts → cost: 税佣 = 两侧成交量 × (佣金 + 过户) / 印花 = 卖出量 × 印花税 / 冲击 = 真实冲击和 + 付固定冲击的量 × impact
enum Side : int { kBuy = 0,
                  kSell = 1 };
inline constexpr int kSides = 2;

// 成本分项 (下标; CTA 净值图叠放序同此)
enum CostPart : int { kCostTax,
                      kCostStamp,
                      kCostImpact };
inline constexpr int kCostParts = 3;
struct CostParts {
  double v[kCostParts] = {};
  double sum() const { return v[kCostTax] + v[kCostStamp] + v[kCostImpact]; }
};

struct Fee {
  double commission = 1e-4; // 佣金 万 1, 双边各一次
  double stamp = 5e-4;      // 印花税 万 5, 卖出单边
  double transfer = 1e-5;   // 过户费 0.001%, 双边各一次
  double impact = 0.0020;   // 固定冲击 (模拟值, 不按金额变), 两侧各一次
  bool operator==(const Fee &) const = default;
  // 一侧的税佣率
  double tax(Side s) const { return commission + transfer + (s == kSell ? stamp : 0.0); }
  // 一侧的全部费率 (税佣 + 固定冲击)
  double rate(Side s) const { return tax(s) + impact; }
  // 成本分项: imp = Σ 量·真实冲击, fix = 付固定冲击的量, buy / sell = 两侧成交量
  CostParts parts(double imp, double fix, double buy, double sell) const {
    return {{(buy + sell) * (commission + transfer), sell * stamp, imp + fix * impact}};
  }
  double cost(double imp, double fix, double buy, double sell) const { return parts(imp, fix, buy, sell).sum(); }
};

// 成本口径 (Config 持有一份; 因子评估 / 组合 / Agent 同读): 金额段 amt (固定段 = 两侧量 × Fee::impact; 盘口段 = Lob 库 L_1min 的买 / 卖冲击列,
//   分钟各层按行末分钟取, 日层无 → 固定段; 评估域并入 Lob 库) + 盘口冲击封顶 top (比率; 账集 Book 逐格 min(冲击, top))
struct CostSpec {
  AmtId amt = kAmtFixed;
  double top = 0.01;
  bool operator==(const CostSpec &) const = default;
};
