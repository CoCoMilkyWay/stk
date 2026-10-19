#pragma once

// =============================================================================
// Trade - 分钟成交量 / 成交额 (compute=onMinute; Bar 档 bar1m)
// =============================================================================
//   bar1m 只有总量, 不分主动买卖: CoreBar 把总量整份放进 MinuteData 的 bid 侧, ask 侧恒 0 ——
//   与 Lob 档 "bid + ask = 分钟总成交" 的 MinuteData 约定一致, Tod 等读 bid + ask 的节点两档同式.
//   无成交分钟 = 0. 落盘 Log 压尾 (fp16 存不下单分钟成交额 1e9+).
//   TradeT<BinData> = 分钟箱成交量 / 额 (compute=on<n>Min, 箱内分钟求和; 读 min<n>_data); Trade = 分钟版; TradeDay = 日版 (L_day).
// =============================================================================

#include "features/DataDefine.hpp"

template <class Bars>
class TradeT {
public:
  enum Out : size_t { volume,
                      amount,
                      kCount };
  float y[kCount] = {};

  explicit TradeT(const Bars &md) : md_(md) {}

  inline void compute() {
    y[volume] = static_cast<float>(md_.bid_volume.back() + md_.ask_volume.back());
    y[amount] = md_.bid_amount.back() + md_.ask_amount.back();
  }

private:
  const Bars &md_;
};
using Trade = TradeT<MinuteData>;

// 日成交量 / 额 (compute=onMinute 逐分钟累加当日; L_day 收盘行取末值 = 当日总量)
class TradeDay {
public:
  enum Out : size_t { volume,
                      amount,
                      kCount };
  float y[kCount] = {};

  explicit TradeDay(const MinuteData &md) : md_(md) {}

  inline void compute() {
    volume_ += md_.bid_volume.back() + md_.ask_volume.back();
    amount_ += static_cast<double>(md_.bid_amount.back()) + md_.ask_amount.back();
    y[volume] = static_cast<float>(volume_);
    y[amount] = static_cast<float>(amount_);
  }
  void reset() { volume_ = 0, amount_ = 0.0; }

private:
  const MinuteData &md_;
  uint64_t volume_ = 0;
  double amount_ = 0.0;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Trade(N) N(Trade, (Trade), (minute_data), onMinute)
#define NODE_Trade5min(N) N(Trade5min, (TradeT<BinData>), (min5_data), on5Min)
#define NODE_Trade15min(N) N(Trade15min, (TradeT<BinData>), (min15_data), on15Min)
#define NODE_Trade30min(N) N(Trade30min, (TradeT<BinData>), (min30_data), on30Min)
#define NODE_Trade60min(N) N(Trade60min, (TradeT<BinData>), (min60_data), on60Min)
#define NODE_TradeDay(N) N(TradeDay, (TradeDay), (minute_data), onMinute)

#define FIELDS_L_min_Trade(X, CAT1)                                                                              \
  X(volume, CAT1, AUTO, "Volume", "成交量", "分钟成交量(股)", R"(\mathrm{Vol}_t)", TS(Trade, volume, Log, None)) \
  X(amount, CAT1, AUTO, "Amount", "成交额", "分钟成交额(元); 分钟 VWAP = amount / volume (因子层)", R"(\mathrm{Amt}_t)", TS(Trade, amount, Log, None))

// 分钟箱成交量 / 额: 列名带 _<n>min 后缀
#define BAR_BIN_TRADE(X, CAT1, node, n)                                                                                                           \
  X(volume_##n##min, CAT1, AUTO, "Volume (" #n "min)", #n "分钟成交量", #n " 分钟箱成交量(股)", R"(\mathrm{Vol}_k)", TS(node, volume, Log, None)) \
  X(amount_##n##min, CAT1, AUTO, "Amount (" #n "min)", #n "分钟成交额", #n " 分钟箱成交额(元)", R"(\mathrm{Amt}_k)", TS(node, amount, Log, None))
#define FIELDS_L_5min_Trade5min(X, CAT1) BAR_BIN_TRADE(X, CAT1, Trade5min, 5)
#define FIELDS_L_15min_Trade15min(X, CAT1) BAR_BIN_TRADE(X, CAT1, Trade15min, 15)
#define FIELDS_L_30min_Trade30min(X, CAT1) BAR_BIN_TRADE(X, CAT1, Trade30min, 30)
#define FIELDS_L_60min_Trade60min(X, CAT1) BAR_BIN_TRADE(X, CAT1, Trade60min, 60)

#define FIELDS_L_day_TradeDay(X, CAT1)                                                                                        \
  X(volume_day, CAT1, AUTO, "Volume (day)", "日成交量", "日成交量(股)", R"(\mathrm{Vol}_D)", TS(TradeDay, volume, Log, None)) \
  X(amount_day, CAT1, AUTO, "Amount (day)", "日成交额", "日成交额(元)", R"(\mathrm{Amt}_D)", TS(TradeDay, amount, Log, None))
