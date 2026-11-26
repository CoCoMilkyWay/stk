#pragma once

// =============================================================================
// Trade - 分钟成交量 / 成交额 (compute=onMinute; Bar 档 bar1m)
// =============================================================================
//   bar1m 只有总量, 不分主动买卖: CoreBar 把总量整份放进 MinuteData 的 bid 侧, ask 侧恒 0 ——
//   与 Lob 档 "bid + ask = 分钟总成交" 的 MinuteData 约定一致, Tod 等读 bid + ask 的节点两档同式.
//   无成交分钟 = 0. 落盘 Log 压尾 (fp16 存不下单分钟成交额 1e9+).
//   TradeT<BinData> = 分钟箱 / 日成交量 / 额 (compute=on<n>Min / onClose, 行内分钟宽累计求和; 读 min<n>_data / day_data); Trade = 分钟版.
//   VwapT<BinData> = 分钟箱 / 日均价 = 额 / 量 (元, 原值落盘): 量额落盘是 Log, 因子层除不回原量, 故在此用宽累计的原量算;
//   无成交 → 本层 Kline close (K 线携带约定). 分钟层不设: 分钟均价 ≈ 分钟收盘, 且 Lob 档已占列名 vwap.
// =============================================================================

#include "features/DataDefine.hpp"

template <class Bar>
class TradeT {
public:
  enum Out : size_t { volume,
                      amount,
                      kCount };
  float y[kCount] = {};

  explicit TradeT(const Bar &md) : md_(md) {}

  inline void compute() {
    y[volume] = static_cast<float>(md_.bid_volume + md_.ask_volume);
    y[amount] = static_cast<float>(md_.bid_amount + md_.ask_amount);
  }

private:
  const Bar &md_;
};
using Trade = TradeT<MinuteData>;

template <class Bar>
class VwapT {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  VwapT(const Bar &md, const Port &close) : md_(md), close_(close) {}

  inline void compute() {
    y[value] = md_.traded() ? static_cast<float>((md_.bid_amount + md_.ask_amount) / static_cast<double>(md_.bid_volume + md_.ask_volume)) : close_;
  }

private:
  const Bar &md_;
  const Port &close_; // 本层 Kline close (同域, 拓扑序在前, 即本行值)
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Trade(N) N(Trade, (Trade), (minute_data), onMinute)
#define NODE_Trade5min(N) N(Trade5min, (TradeT<BinData>), (min5_data), on5Min)
#define NODE_Trade15min(N) N(Trade15min, (TradeT<BinData>), (min15_data), on15Min)
#define NODE_Trade30min(N) N(Trade30min, (TradeT<BinData>), (min30_data), on30Min)
#define NODE_Trade60min(N) N(Trade60min, (TradeT<BinData>), (min60_data), on60Min)
#define NODE_TradeDay(N) N(TradeDay, (TradeT<BinData>), (day_data), onClose)
#define NODE_Vwap5min(N) N(Vwap5min, (VwapT<BinData>), (min5_data, Kline5min.out(Kline5min.close)), on5Min)
#define NODE_Vwap15min(N) N(Vwap15min, (VwapT<BinData>), (min15_data, Kline15min.out(Kline15min.close)), on15Min)
#define NODE_Vwap30min(N) N(Vwap30min, (VwapT<BinData>), (min30_data, Kline30min.out(Kline30min.close)), on30Min)
#define NODE_Vwap60min(N) N(Vwap60min, (VwapT<BinData>), (min60_data, Kline60min.out(Kline60min.close)), on60Min)
#define NODE_VwapDay(N) N(VwapDay, (VwapT<BinData>), (day_data, KlineDay.out(KlineDay.close)), onClose)

#define FIELDS_L_1min_Trade(X, CAT1)                                                                       \
  X(volume, CAT1, AUTO, "Volume", "成交量", "分钟成交量(股)", R"(\mathrm{Vol}_t)", TS(Trade, volume, Log)) \
  X(amount, CAT1, AUTO, "Amount", "成交额", "分钟成交额(元)", R"(\mathrm{Amt}_t)", TS(Trade, amount, Log))

// 分钟箱成交量 / 额: 列名带 _<n>min 后缀
#define BAR_BIN_TRADE(X, CAT1, node, n)                                                                                                     \
  X(volume_##n##min, CAT1, AUTO, "Volume (" #n "min)", #n "分钟成交量", #n " 分钟箱成交量(股)", R"(\mathrm{Vol}_k)", TS(node, volume, Log)) \
  X(amount_##n##min, CAT1, AUTO, "Amount (" #n "min)", #n "分钟成交额", #n " 分钟箱成交额(元)", R"(\mathrm{Amt}_k)", TS(node, amount, Log))
#define FIELDS_L_5min_Trade5min(X, CAT1) BAR_BIN_TRADE(X, CAT1, Trade5min, 5)
#define FIELDS_L_15min_Trade15min(X, CAT1) BAR_BIN_TRADE(X, CAT1, Trade15min, 15)
#define FIELDS_L_30min_Trade30min(X, CAT1) BAR_BIN_TRADE(X, CAT1, Trade30min, 30)
#define FIELDS_L_60min_Trade60min(X, CAT1) BAR_BIN_TRADE(X, CAT1, Trade60min, 60)

#define FIELDS_L_1day_TradeDay(X, CAT1)                                                                                 \
  X(volume_day, CAT1, AUTO, "Volume (day)", "日成交量", "日成交量(股)", R"(\mathrm{Vol}_D)", TS(TradeDay, volume, Log)) \
  X(amount_day, CAT1, AUTO, "Amount (day)", "日成交额", "日成交额(元)", R"(\mathrm{Amt}_D)", TS(TradeDay, amount, Log))

// 分钟箱 / 日均价: 列名带 _<n>min / _day 后缀
#define BAR_BIN_VWAP(X, CAT1, node, n) \
  X(vwap_##n##min, CAT1, AUTO, "VWAP (" #n "min)", #n "分钟均价", #n " 分钟箱成交额 / 成交量(元); 无成交 → 本箱收盘价", R"(\mathrm{Amt}_k / \mathrm{Vol}_k)", TS(node, value, None))
#define FIELDS_L_5min_Vwap5min(X, CAT1) BAR_BIN_VWAP(X, CAT1, Vwap5min, 5)
#define FIELDS_L_15min_Vwap15min(X, CAT1) BAR_BIN_VWAP(X, CAT1, Vwap15min, 15)
#define FIELDS_L_30min_Vwap30min(X, CAT1) BAR_BIN_VWAP(X, CAT1, Vwap30min, 30)
#define FIELDS_L_60min_Vwap60min(X, CAT1) BAR_BIN_VWAP(X, CAT1, Vwap60min, 60)
#define FIELDS_L_1day_VwapDay(X, CAT1) \
  X(vwap_day, CAT1, AUTO, "VWAP (day)", "日均价", "日成交额 / 成交量(元); 无成交 → 日收盘价 (携带约定)", R"(\mathrm{Amt}_D / \mathrm{Vol}_D)", TS(VwapDay, value, None))
