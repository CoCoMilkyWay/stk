#pragma once

// Bar 档分钟 K + 分钟箱 K + 日 K (bar1m 不复权, 分钟 / 箱类见 Node/Common/Basic/Kline.hpp). 成交量 / 额见 Trade.hpp.
#include "features/Node/Common/Basic/Kline.hpp"

#include <algorithm>

// 日 K (compute=onMinute 逐分钟并入当日; L_day 收盘行取末值 = 当日 bar): bar 口径同 ResamplerMin2Bin (open = 首个有成交分钟的 open,
// 高低只含有成交分钟, close = 末分钟 close); 当日尚无成交 → 四价 = 前收 (Kline 携带约定)
class KlineDay {
public:
  enum Out : size_t { open,
                      high,
                      low,
                      close,
                      kCount };
  float y[kCount] = {};

  KlineDay(const MinuteData &md, const Series &pre_close) : md_(md), pre_close_(pre_close) {}

  inline void compute() {
    const float c = md_.close.back();
    if (c > 0.0f && md_.bid_volume.back() + md_.ask_volume.back() > 0) {
      const float h = md_.high.back(), l = md_.low.back();
      y[open] = traded_ ? y[open] : md_.open.back();
      y[high] = traded_ ? std::max(y[high], h) : h;
      y[low] = traded_ ? std::min(y[low], l) : l;
      traded_ = true;
    }
    if (traded_)
      y[close] = c;
    else
      y[open] = y[high] = y[low] = y[close] = pre_close_.back();
  }
  void reset() { traded_ = false; }

private:
  const MinuteData &md_;
  const Series &pre_close_;
  bool traded_ = false; // 当日已有成交分钟
};

#define NODE_Kline(N) N(Kline, (Kline), (minute_data, Fund.out(Fund.pre_close)), onMinute)
#define NODE_KlineDay(N) N(KlineDay, (KlineDay), (minute_data, Fund.out(Fund.pre_close)), onMinute)
#define NODE_Kline5min(N) N(Kline5min, (KlineT<BinData>), (min5_data, Fund.out(Fund.pre_close)), on5Min)
#define NODE_Kline15min(N) N(Kline15min, (KlineT<BinData>), (min15_data, Fund.out(Fund.pre_close)), on15Min)
#define NODE_Kline30min(N) N(Kline30min, (KlineT<BinData>), (min30_data, Fund.out(Fund.pre_close)), on30Min)
#define NODE_Kline60min(N) N(Kline60min, (KlineT<BinData>), (min60_data, Fund.out(Fund.pre_close)), on60Min)

#define FIELDS_L_min_Kline(X, CAT1)                                                                              \
  X(open, CAT1, AUTO, "Open", "开盘价", "分钟开盘价(元)", R"(P^{\mathrm{open}}_t)", TS(Kline, open, None, None)) \
  X(high, CAT1, AUTO, "High", "最高价", "分钟最高价(元)", R"(P^{\mathrm{high}}_t)", TS(Kline, high, None, None)) \
  X(low, CAT1, AUTO, "Low", "最低价", "分钟最低价(元)", R"(P^{\mathrm{low}}_t)", TS(Kline, low, None, None))     \
  X(close, CAT1, AUTO, "Close", "收盘价", "分钟收盘价(元)", R"(P^{\mathrm{close}}_t)", TS(Kline, close, None, None))

// 分钟箱 K: 列名带 _<n>min 后缀 (首箱 = 09:15-09:30 集合竞价段, 箱见 TimeIndex.hpp)
#define BAR_BIN_KLINE(X, CAT1, node, n)                                                                                                           \
  X(open_##n##min, CAT1, AUTO, "Open (" #n "min)", #n "分钟开盘价", #n " 分钟箱开盘价(元)", R"(P^{\mathrm{open}}_k)", TS(node, open, None, None)) \
  X(high_##n##min, CAT1, AUTO, "High (" #n "min)", #n "分钟最高价", #n " 分钟箱最高价(元)", R"(P^{\mathrm{high}}_k)", TS(node, high, None, None)) \
  X(low_##n##min, CAT1, AUTO, "Low (" #n "min)", #n "分钟最低价", #n " 分钟箱最低价(元)", R"(P^{\mathrm{low}}_k)", TS(node, low, None, None))     \
  X(close_##n##min, CAT1, AUTO, "Close (" #n "min)", #n "分钟收盘价", #n " 分钟箱收盘价(元)", R"(P^{\mathrm{close}}_k)", TS(node, close, None, None))
#define FIELDS_L_5min_Kline5min(X, CAT1) BAR_BIN_KLINE(X, CAT1, Kline5min, 5)
#define FIELDS_L_15min_Kline15min(X, CAT1) BAR_BIN_KLINE(X, CAT1, Kline15min, 15)
#define FIELDS_L_30min_Kline30min(X, CAT1) BAR_BIN_KLINE(X, CAT1, Kline30min, 30)
#define FIELDS_L_60min_Kline60min(X, CAT1) BAR_BIN_KLINE(X, CAT1, Kline60min, 60)

#define FIELDS_L_day_KlineDay(X, CAT1)                                                                                                                \
  X(open_day, CAT1, AUTO, "Open (day)", "日开盘价", "日开盘价(元): 首个有成交分钟的开盘价", R"(P^{\mathrm{open}}_D)", TS(KlineDay, open, None, None)) \
  X(high_day, CAT1, AUTO, "High (day)", "日最高价", "日最高价(元)", R"(P^{\mathrm{high}}_D)", TS(KlineDay, high, None, None))                         \
  X(low_day, CAT1, AUTO, "Low (day)", "日最低价", "日最低价(元)", R"(P^{\mathrm{low}}_D)", TS(KlineDay, low, None, None))                             \
  X(close_day, CAT1, AUTO, "Close (day)", "日收盘价", "日收盘价(元): 末分钟收盘价", R"(P^{\mathrm{close}}_D)", TS(KlineDay, close, None, None))
