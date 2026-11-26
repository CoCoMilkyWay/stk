#pragma once

// Bar 档分钟 K + 分钟箱 K + 日 K (bar1m 不复权, 节点类见 Node/Common/Basic/Kline.hpp; 日 K = 整日箱, 读 day_data). 成交量 / 额见 Trade.hpp.
// 后复权价只在日层落盘 (AdjDay: 四价 + 均价 × 复权比, 给因子的跨日价格运算); 分钟层不落 (标签在节点内现乘复权比;
// GUI 篮子漂移用 close_day × adj_factor 现算); CloseAdj5min 只作 MovingAverage 的输入
#include "features/Node/Common/Basic/Kline.hpp"

#define NODE_Kline(N) N(Kline, (Kline), (minute_data, Fund.out(Fund.pre_close)), onMinute)
#define NODE_Kline5min(N) N(Kline5min, (KlineT<BinData>), (min5_data, Fund.out(Fund.pre_close)), on5Min)
#define NODE_Kline15min(N) N(Kline15min, (KlineT<BinData>), (min15_data, Fund.out(Fund.pre_close)), on15Min)
#define NODE_Kline30min(N) N(Kline30min, (KlineT<BinData>), (min30_data, Fund.out(Fund.pre_close)), on30Min)
#define NODE_Kline60min(N) N(Kline60min, (KlineT<BinData>), (min60_data, Fund.out(Fund.pre_close)), on60Min)
#define NODE_KlineDay(N) N(KlineDay, (KlineT<BinData>), (day_data, Fund.out(Fund.pre_close)), onClose)

// AdjDay - 日层后复权价 = 原价 × Fund adj_ratio (首日归一, fp16 存得下): 跨日价格运算 (差分 / 滚动 / 相关) 直接用, 同日比较仍用原价
class AdjDay {
public:
  enum Out : size_t { open,
                      high,
                      low,
                      close,
                      vwap,
                      kCount };
  float y[kCount] = {};

  AdjDay(const Port &o, const Port &h, const Port &l, const Port &c, const Port &v, const Port &r) : px_{&o, &h, &l, &c, &v}, r_(r) {}

  inline void compute() {
    for (size_t i = 0; i < kCount; ++i)
      y[i] = *px_[i] * r_;
  }

private:
  const Port *px_[kCount]; // 日 K 四价 + 日均价 (同域 onClose, 拓扑序在前, 即本日值)
  const Port &r_;          // Fund adj_ratio (Fund 的 onSettle 晚于 onClose, 收盘时仍是当日值)
};

#define NODE_CloseAdj5min(N) N(CloseAdj5min, (AdjClose), (Kline5min.out(Kline5min.close), Fund.out(Fund.adj_ratio)), on5Min)

#define FIELDS_L_1min_Kline(X, CAT1)                                                                       \
  X(open, CAT1, AUTO, "Open", "开盘价", "分钟开盘价(元)", R"(P^{\mathrm{open}}_t)", TS(Kline, open, None)) \
  X(high, CAT1, AUTO, "High", "最高价", "分钟最高价(元)", R"(P^{\mathrm{high}}_t)", TS(Kline, high, None)) \
  X(low, CAT1, AUTO, "Low", "最低价", "分钟最低价(元)", R"(P^{\mathrm{low}}_t)", TS(Kline, low, None))     \
  X(close, CAT1, AUTO, "Close", "收盘价", "分钟收盘价(元)", R"(P^{\mathrm{close}}_t)", TS(Kline, close, None))

// 分钟箱 K: 列名带 _<n>min 后缀 (首箱 = 09:15-09:30 集合竞价段, 箱见 TimeIndex.hpp)
#define BAR_BIN_KLINE(X, CAT1, node, n)                                                                                                     \
  X(open_##n##min, CAT1, AUTO, "Open (" #n "min)", #n "分钟开盘价", #n " 分钟箱开盘价(元)", R"(P^{\mathrm{open}}_k)", TS(node, open, None)) \
  X(high_##n##min, CAT1, AUTO, "High (" #n "min)", #n "分钟最高价", #n " 分钟箱最高价(元)", R"(P^{\mathrm{high}}_k)", TS(node, high, None)) \
  X(low_##n##min, CAT1, AUTO, "Low (" #n "min)", #n "分钟最低价", #n " 分钟箱最低价(元)", R"(P^{\mathrm{low}}_k)", TS(node, low, None))     \
  X(close_##n##min, CAT1, AUTO, "Close (" #n "min)", #n "分钟收盘价", #n " 分钟箱收盘价(元)", R"(P^{\mathrm{close}}_k)", TS(node, close, None))
#define FIELDS_L_5min_Kline5min(X, CAT1) BAR_BIN_KLINE(X, CAT1, Kline5min, 5)
#define FIELDS_L_15min_Kline15min(X, CAT1) BAR_BIN_KLINE(X, CAT1, Kline15min, 15)
#define FIELDS_L_30min_Kline30min(X, CAT1) BAR_BIN_KLINE(X, CAT1, Kline30min, 30)
#define FIELDS_L_60min_Kline60min(X, CAT1) BAR_BIN_KLINE(X, CAT1, Kline60min, 60)

#define FIELDS_L_1day_KlineDay(X, CAT1)                                                                                                         \
  X(open_day, CAT1, AUTO, "Open (day)", "日开盘价", "日开盘价(元): 首个有成交分钟的开盘价", R"(P^{\mathrm{open}}_D)", TS(KlineDay, open, None)) \
  X(high_day, CAT1, AUTO, "High (day)", "日最高价", "日最高价(元)", R"(P^{\mathrm{high}}_D)", TS(KlineDay, high, None))                         \
  X(low_day, CAT1, AUTO, "Low (day)", "日最低价", "日最低价(元)", R"(P^{\mathrm{low}}_D)", TS(KlineDay, low, None))                             \
  X(close_day, CAT1, AUTO, "Close (day)", "日收盘价", "日收盘价(元): 末分钟收盘价", R"(P^{\mathrm{close}}_D)", TS(KlineDay, close, None))

#define NODE_AdjDay(N) N(AdjDay, (AdjDay), (KlineDay.out(KlineDay.open), KlineDay.out(KlineDay.high), KlineDay.out(KlineDay.low), KlineDay.out(KlineDay.close), VwapDay.out(VwapDay.value), Fund.out(Fund.adj_ratio)), onClose)

#define FIELDS_L_1day_AdjDay(X, CAT1)                                                                                                                                                     \
  X(open_adj_day, CAT1, AUTO, "Open Adj (day)", "日开盘后复权", "日开盘价 × 复权比(首日归一); 跨日价格运算用", R"(P^{\mathrm{open}}_D \cdot \mathrm{adj}_D)", TS(AdjDay, open, None))     \
  X(high_adj_day, CAT1, AUTO, "High Adj (day)", "日最高后复权", "日最高价 × 复权比(首日归一); 跨日价格运算用", R"(P^{\mathrm{high}}_D \cdot \mathrm{adj}_D)", TS(AdjDay, high, None))     \
  X(low_adj_day, CAT1, AUTO, "Low Adj (day)", "日最低后复权", "日最低价 × 复权比(首日归一); 跨日价格运算用", R"(P^{\mathrm{low}}_D \cdot \mathrm{adj}_D)", TS(AdjDay, low, None))         \
  X(close_adj_day, CAT1, AUTO, "Close Adj (day)", "日收盘后复权", "日收盘价 × 复权比(首日归一); 跨日价格运算用", R"(P^{\mathrm{close}}_D \cdot \mathrm{adj}_D)", TS(AdjDay, close, None)) \
  X(vwap_adj_day, CAT1, AUTO, "VWAP Adj (day)", "日均价后复权", "日均价 × 复权比(首日归一); 跨日价格运算用", R"(\mathrm{VWAP}_D \cdot \mathrm{adj}_D)", TS(AdjDay, vwap, None))
