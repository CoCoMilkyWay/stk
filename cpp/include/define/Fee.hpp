#pragma once

// 交易费率 (比率; 全仓唯一一份: Config 持有, 因子换手成本 / 组合净线 / CTA / 实盘调仓试算 / 账户日内回推同用)
//   税佣: 佣金双边 + 过户费双边 + 印花税卖出单边; 冲击 impact = 固定冲击 (无盘口冲击列时两侧都按它)
//   一笔的成本 = 量 × tax(侧) + 冲击 (盘口段 = 真实冲击, 固定段 = 量 × impact)
struct Fee {
  double commission = 1e-4; // 佣金 万 1, 双边各一次
  double stamp = 5e-4;      // 印花税 万 5, 卖出单边
  double transfer = 1e-5;   // 过户费 0.001%, 双边各一次
  double impact = 0.0020;   // 固定冲击 (模拟值, 不按金额变), 两侧各一次
  bool operator==(const Fee &) const = default;
  // 一侧的税佣率 (sell = 卖出侧)
  double tax(bool sell) const { return commission + transfer + (sell ? stamp : 0.0); }
  // 一侧的全部费率 (税佣 + 固定冲击)
  double rate(bool sell) const { return tax(sell) + impact; }
  // 账行的成本: imp = Σ 量·真实冲击 (两侧), fix = 付固定冲击的量 (两侧), buy / sell = 两侧成交量
  double cost(double imp, double fix, double buy, double sell) const { return imp + fix * impact + buy * tax(false) + sell * tax(true); }
};
