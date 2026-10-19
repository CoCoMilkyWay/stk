#pragma once

// Bar 档同 5 分钟箱历史基准 z 值 (类见 Node/Common/Basic/Tod.hpp; x = 箱内 bar1m 总量 / 额, 与 Trade5min 同源; 不落 L_min: 逐分钟代价太大)
#include "features/Node/Common/Basic/Tod.hpp"

#define NODE_Tod5min(N) N(Tod5min, (TodT<BinData, bins_per_day(5)>), (min5_data), on5Min)

#define TOD_ROWS(X, CAT1, D)                                                                                                                                                                                                                                                                                \
  X(amt_todz_##D##d, CAT1, AUTO, "Amount TOD Z " #D "d", "成交额同箱z值" #D "日", "本5分钟箱成交额对前" #D "日同箱均值/标准差的z值(样本<3或σ=0→NaN)", R"(\frac{\mathrm{Amt}_k - \mu^{()" #D R"(d)}_{\mathrm{tod}(k)}}{\sigma^{()" #D R"(d)}_{\mathrm{tod}(k)}})", TS(Tod5min, amt_todz_##D##d, None, None)) \
  X(vol_todz_##D##d, CAT1, AUTO, "Volume TOD Z " #D "d", "成交量同箱z值" #D "日", "本5分钟箱成交量对前" #D "日同箱均值/标准差的z值(样本<3或σ=0→NaN)", R"(\frac{\mathrm{Vol}_k - \mu^{()" #D R"(d)}_{\mathrm{tod}(k)}}{\sigma^{()" #D R"(d)}_{\mathrm{tod}(k)}})", TS(Tod5min, vol_todz_##D##d, None, None))

#define FIELDS_L_5min_Tod5min(X, CAT1) \
  TOD_ROWS(X, CAT1, 5)                 \
  TOD_ROWS(X, CAT1, 10)
