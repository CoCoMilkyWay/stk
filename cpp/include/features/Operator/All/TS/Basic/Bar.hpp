#pragma once

// All 档分钟 K (bar1m 不复权, 类见 Operator/Common/Basic/Bar.hpp). 成交量 / 额见 Trade.hpp.
#include "features/Operator/Common/Basic/Bar.hpp"

#define NODE_Bar(N) N(Bar, (Bar), (minute_data, Fund.out(Fund.pre_close)), onMinute)

#define FIELDS_L1_Bar(X, CAT1)                                                                                 \
  X(open, CAT1, AUTO, "Open", "开盘价", "分钟开盘价(元)", R"(P^{\mathrm{open}}_t)", OP(Bar, open, None, None)) \
  X(high, CAT1, AUTO, "High", "最高价", "分钟最高价(元)", R"(P^{\mathrm{high}}_t)", OP(Bar, high, None, None)) \
  X(low, CAT1, AUTO, "Low", "最低价", "分钟最低价(元)", R"(P^{\mathrm{low}}_t)", OP(Bar, low, None, None))     \
  X(close, CAT1, AUTO, "Close", "收盘价", "分钟收盘价(元)", R"(P^{\mathrm{close}}_t)", OP(Bar, close, None, None))
