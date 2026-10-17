#pragma once

// Bar 档同分钟历史基准 z 值 (类见 Node/Common/Basic/Tod.hpp; x = bar1m 总量 / 额, 与 Trade 同源)
#include "features/Node/Common/Basic/Tod.hpp"

#define NODE_Tod(N) N(Tod, (Tod), (minute_data), onMinute)

#define TOD_ROWS(X, CAT1, D)                                                                                                                                                                                                                                                                             \
  X(amt_todz_##D##d, CAT1, AUTO, "Amount TOD Z " #D "d", "成交额同分钟z值" #D "日", "本分钟成交额对前" #D "日同分钟均值/标准差的z值(样本<3或σ=0→NaN)", R"(\frac{\mathrm{Amt}_t - \mu^{()" #D R"(d)}_{\mathrm{tod}(t)}}{\sigma^{()" #D R"(d)}_{\mathrm{tod}(t)}})", TS(Tod, amt_todz_##D##d, None, None)) \
  X(vol_todz_##D##d, CAT1, AUTO, "Volume TOD Z " #D "d", "成交量同分钟z值" #D "日", "本分钟成交量对前" #D "日同分钟均值/标准差的z值(样本<3或σ=0→NaN)", R"(\frac{\mathrm{Vol}_t - \mu^{()" #D R"(d)}_{\mathrm{tod}(t)}}{\sigma^{()" #D R"(d)}_{\mathrm{tod}(t)}})", TS(Tod, vol_todz_##D##d, None, None))

#define FIELDS_L_min_Tod(X, CAT1) \
  TOD_ROWS(X, CAT1, 5)            \
  TOD_ROWS(X, CAT1, 10)
