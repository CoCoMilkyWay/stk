#pragma once

#include "define/Label.hpp" // AmtId / LobAmt

// 交易成本 = 量 × 率 (Fee) → 钱 (CostParts); 全仓唯一一式 Fee::side (一侧), 流水 TurnFlow 的 Fee::parts = 两侧之和 (因子换手 / 组合与篮子净线 / CTA 同用);
//   实盘调仓试算 / 账户日内回推只要税佣 (冲击取真实吃单, define/Sweep.hpp) = Fee::tax
//   成交两侧 Side (下标; Stat 账集流水 / 冲击列 / 费率同一下标); 实盘的方向 ±1 经 live::side_of 映到它
//   税佣: 佣金双边 + 过户费双边 + 印花税卖出单边; 冲击: 口径段 a 的固定段 = 全量 × 固定冲击 Fee::impact,
//   盘口段 = 真实冲击 Σ 量·冲击 (Lob 库建仓冲击列, 封顶 CostSpec::top) + 卖不出的量 stuck × 固定冲击
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
  CostParts &operator+=(const CostParts &o) {
    for (int k = 0; k < kCostParts; ++k)
      v[k] += o.v[k];
    return *this;
  }
};

// 两侧流水 (量; 可合并) = 一张字段表 (账线列 / 落盘 / 合并同一张): 两侧成交量 | 卖不出 (封跌停) 的卖出量 | 各盘口段两侧的真实冲击和;
//   各口径段同一份量, 成本按段现乘
enum FlowField : int { kFlowBuy,   // = kFlowBuy + kBuy
                       kFlowSell,  // = kFlowBuy + kSell
                       kFlowStuck, // 卖不出的量
                       kFlowImp }; // 盘口段 k 侧 s 的冲击和 = kFlowImp + k · kSides + s
inline constexpr int kFlowFields = kFlowImp + kLobAmts * kSides;
inline constexpr int flow_vol(Side s) { return static_cast<int>(kFlowBuy) + static_cast<int>(s); }
inline constexpr int flow_imp(LobAmt k, Side s) { return static_cast<int>(kFlowImp) + static_cast<int>(k) * kSides + static_cast<int>(s); }
inline constexpr bool flow_priced(int f) { return f >= kFlowImp; }                               // 字段 f 是盘口段冲击
inline constexpr LobAmt flow_lob(int f) { return static_cast<LobAmt>((f - kFlowImp) / kSides); } // 冲击字段的盘口段
struct TurnFlow {
  double v[kFlowFields] = {};
  double &operator[](int f) { return v[f]; }
  double operator[](int f) const { return v[f]; }
  double vol(Side s) const { return v[flow_vol(s)]; }
  void merge(const TurnFlow &o) {
    for (int f = 0; f < kFlowFields; ++f)
      v[f] += o.v[f];
  }
  // 口径段 a 一侧: 真实冲击和 / 付固定冲击的量
  double impact(AmtId a, Side s) const { return is_lob(a) ? v[flow_imp(lob_of(a), s)] : 0.0; }
  double fixed(AmtId a, Side s) const { return !is_lob(a) ? vol(s) : s == kSell ? v[kFlowStuck]
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
  // 一侧的成本分项 (唯一一式): 成交量 vol, 已乘好的盘口冲击和 imp, 付固定冲击的量 fixed
  CostParts side(double vol, Side s, double imp, double fixed) const {
    return {{vol * (commission + transfer), s == kSell ? vol * stamp : 0.0, imp + fixed * impact}};
  }
  // 流水 f 在口径段 a 下一侧 / 两侧的成本分项
  CostParts parts(const TurnFlow &f, AmtId a, Side s) const { return side(f.vol(s), s, f.impact(a, s), f.fixed(a, s)); }
  CostParts parts(const TurnFlow &f, AmtId a) const {
    CostParts c = parts(f, a, kBuy);
    c += parts(f, a, kSell);
    return c;
  }
  double cost(const TurnFlow &f, AmtId a) const { return parts(f, a).sum(); }
  // 一侧的平均费率 (显示); 无量 → 0
  double rate(const TurnFlow &f, AmtId a, Side s) const { return f.vol(s) > 0.0 ? parts(f, a, s).sum() / f.vol(s) : 0.0; }
};

// 成本口径 (Config 持有一份; 因子评估 / 组合 / Agent 同读): 口径段 amt (固定段 = 全量 × Fee::impact; 盘口段 = Lob 库 L_1min 的买 / 卖冲击列,
//   分钟各层按行末分钟取, 日层收盘撮合建仓 = 冲击 0; 要装平面时评估域并入 Lob 库) + 盘口冲击封顶 top (比率; 账集 Book 逐格 min(冲击, top))
struct CostSpec {
  AmtId amt = kAmtFixed;
  double top = 0.01;
  bool operator==(const CostSpec &) const = default;
};
