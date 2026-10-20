#pragma once

// Lob 档订单簿还原分钟 K (逐笔聚合, 类见 Node/Common/Basic/Kline.hpp). 列名 lob_*: 与 Bar 档 bar1m K 线 (open/high/low/close) 区分.
//   成交量 / 成交额 不在此: 与 Flow 的 vol_taker_bid + vol_taker_ask / amt_taker_* 完全同源同值, 因子层按行求和.
//   另一列后复权收盘价 lob_close_adj (AdjClose; 跨日收益 / 池指数用)
#include "features/Node/Common/Basic/Kline.hpp"

#define NODE_Kline(N) N(Kline, (Kline), (minute_data, Fund.out(Fund.pre_close)), onMinute)
#define NODE_CloseAdj(N) N(CloseAdj, (AdjClose), (Kline.out(Kline.close), Fund.out(Fund.adj_ratio)), onMinute)

#define FIELDS_L_1min_Kline(X, CAT1)                                                                                                \
  X(lob_open, CAT1, AUTO, "LOB Open", "LOB开盘价", "逐笔还原分钟开盘价(元)", R"(P^{\mathrm{open}}_t)", TS(Kline, open, None, None)) \
  X(lob_high, CAT1, AUTO, "LOB High", "LOB最高价", "逐笔还原分钟最高价(元)", R"(P^{\mathrm{high}}_t)", TS(Kline, high, None, None)) \
  X(lob_low, CAT1, AUTO, "LOB Low", "LOB最低价", "逐笔还原分钟最低价(元)", R"(P^{\mathrm{low}}_t)", TS(Kline, low, None, None))     \
  X(lob_close, CAT1, AUTO, "LOB Close", "LOB收盘价", "逐笔还原分钟收盘价(元)", R"(P^{\mathrm{close}}_t)", TS(Kline, close, None, None))

#define FIELDS_L_1min_CloseAdj(X, CAT1) \
  X(lob_close_adj, CAT1, price, "LOB Close Adj", "LOB复权收盘价", "逐笔还原分钟收盘价×当日复权比(后复权, 首日归一); 跨日收益用", R"(P^{\mathrm{close}}_t \cdot r_D)", TS(CloseAdj, value, None, None))
