#pragma once

// Lob 档同分钟历史基准 z 值 (类见 Node/Common/Basic/Tod.hpp; x = 主动买 + 卖成交额 / 量, 与 Flow 同源). 列名 lob_*: 与 Bar 档 bar1m 版区分.
#include "features/Node/Common/Basic/Tod.hpp"

#define NODE_Tod(N) N(Tod, (Tod), (minute_data), onMinute)

#define LOB_TOD_ROWS(X, CAT1, D)                                                                                                                                                                                                                                                                                      \
  X(lob_amt_todz_##D##d, CAT1, AUTO, "LOB Amount TOD Z " #D "d", "LOB成交额同分钟z值" #D "日", "逐笔本分钟成交额对前" #D "日同分钟均值/标准差的z值(样本<3或σ=0→0)", R"(\frac{\mathrm{Amt}_t - \mu^{()" #D R"(d)}_{\mathrm{tod}(t)}}{\sigma^{()" #D R"(d)}_{\mathrm{tod}(t)}})", TS(Tod, amt_todz_##D##d, None, None)) \
  X(lob_vol_todz_##D##d, CAT1, AUTO, "LOB Volume TOD Z " #D "d", "LOB成交量同分钟z值" #D "日", "逐笔本分钟成交量对前" #D "日同分钟均值/标准差的z值(样本<3或σ=0→0)", R"(\frac{\mathrm{Vol}_t - \mu^{()" #D R"(d)}_{\mathrm{tod}(t)}}{\sigma^{()" #D R"(d)}_{\mathrm{tod}(t)}})", TS(Tod, vol_todz_##D##d, None, None))

#define FIELDS_L_min_Tod(X, CAT1) \
  LOB_TOD_ROWS(X, CAT1, 5)        \
  LOB_TOD_ROWS(X, CAT1, 10)
