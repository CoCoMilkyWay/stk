#pragma once
// 元数据列 (META): 有效标志 / OHLC / 盘口快照, 由 CoreSequential 手工写. 无算子.

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L0_Meta(X)                                                                                                                                      \
  X(_depth_valid, 1, ALL, META, RAW, NONE, "100/00/00", "Depth Valid Flag", "深度有效标志", "LOB深度缓冲区完整性标记", R"(\mathbf{1}_{\mathrm{valid}})", META) \
  X(_data_valid, 1, ALL, META, RAW, NONE, "100/00/00", "Data Valid Flag", "数据有效标志", "事件驱动稀疏性标记", R"(\mathbf{1}_{\mathrm{valid}})", META)

#define FIELDS_L1_Meta(X)                                                                                           \
  X(_ohlc_open, 1, DATA, META, RAW, NONE, "00/100/00", "OHLC Open", "开盘价", "GUI:分钟开盘价(分)", R"(O)", META)   \
  X(_ohlc_high, 1, DATA, META, RAW, NONE, "00/100/00", "OHLC High", "最高价", "GUI:分钟最高价(分)", R"(H)", META)   \
  X(_ohlc_low, 1, DATA, META, RAW, NONE, "00/100/00", "OHLC Low", "最低价", "GUI:分钟最低价(分)", R"(L)", META)     \
  X(_ohlc_close, 1, DATA, META, RAW, NONE, "00/100/00", "OHLC Close", "收盘价", "GUI:分钟收盘价(分)", R"(C)", META) \
  X(_ohlc_volume, 1, DATA, META, RAW, NONE, "00/100/00", "OHLC Volume", "成交量", "GUI:分钟成交量", R"(V)", META)   \
  X(_data_valid, 1, ALL, META, RAW, NONE, "00/100/00", "Data Valid Flag", "数据有效标志", "事件驱动稀疏性标记", R"(\mathbf{1}_{\mathrm{valid}})", META)

#define FIELDS_DEPTH_Meta(X)                                                                                                                                      \
  X(_bid_price, L2::LOB_DEPTH, DEPTH, META, RAW, NONE, "00/00/00", "Bid Prices", "买盘价格", "GUI:N档买盘价格(分)", R"(P^{M,B}_{0:N})", META)                     \
  X(_ask_price, L2::LOB_DEPTH, DEPTH, META, RAW, NONE, "00/00/00", "Ask Prices", "卖盘价格", "GUI:N档卖盘价格(分)", R"(P^{M,A}_{0:N})", META)                     \
  X(_bid_volume, L2::LOB_DEPTH, DEPTH, META, RAW, NONE, "00/00/00", "Bid Volumes", "买盘量", "GUI:N档买盘量(手,100股)", R"(V^{M,B}_{0:N})", META)                 \
  X(_ask_volume, L2::LOB_DEPTH, DEPTH, META, RAW, NONE, "00/00/00", "Ask Volumes", "卖盘量", "GUI:N档卖盘量(手,100股)", R"(V^{M,A}_{0:N})", META)                 \
  X(_mid_price, 1, DEPTH, META, RAW, NONE, "00/00/00", "Mid Price", "中间价", "GUI:实时中间价(分)", R"(\frac{P^{M,B}_1 + P^{M,A}_1}{2})", META)                   \
  X(_depth_valid, 1, ALL, META, RAW, NONE, "00/00/00", "Depth Valid Flag", "深度有效", "LOB深度缓冲区完整性标记", R"(\mathbf{1}_{\mathrm{valid}_{depth}})", META) \
  X(_data_valid, 1, ALL, META, RAW, NONE, "00/00/00", "Data Valid Flag", "数据有效", "事件驱动稀疏性标记", R"(\mathbf{1}_{\mathrm{valid}_{data}})", META)
