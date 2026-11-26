#pragma once

// 交易费率 (比率; 全仓唯一一份: Config 持有, 因子换手成本 / 组合与篮子净线 / CTA / 实盘调仓试算 / 账户日内回推同用)
//   成交两侧 Side (下标; Stat 账集流水 / 冲击列 / 费率同一下标); 实盘的方向 ±1 经 live::side_of 映到它
//   税佣: 佣金双边 + 过户费双边 + 印花税卖出单边; 冲击 impact = 固定冲击 (无盘口冲击列时两侧都按它)
//   成本唯一一式 cost: 真实冲击和 + 付固定冲击的量 × impact + 两侧成交量 × 各侧税佣
enum Side : int { kBuy = 0,
                  kSell = 1 };
inline constexpr int kSides = 2;

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
  // 成本: imp = Σ 量·真实冲击, fix = 付固定冲击的量, buy / sell = 两侧成交量
  double cost(double imp, double fix, double buy, double sell) const { return imp + fix * impact + buy * tax(kBuy) + sell * tax(kSell); }
};
