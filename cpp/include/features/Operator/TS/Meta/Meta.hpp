#pragma once
// 元数据列: 有效标志 (FLAG) / 盘口快照 (META(w)), 由 CoreSequential 手工写. 无算子.
//   _data_valid   该行有事件写入 (L0 每笔 / L1 有效分钟 / DEPTH 该分钟有事件)
//   _depth_valid  该行盘口有更新 (onDepth)
//   DEPTH 行 = [bid_price[N], ask_price[N], bid_volume[N], ask_volume[N], mid_price, _depth_valid] 一次 ts_write_range 写入,
//   顺序必须与 CoreSequential::run_tick 的快照缓冲一致; _data_valid 单独每笔写.

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L0_Meta(X)                                                                                                        \
  X(_depth_valid, META, RAW, NONE, "Depth Valid Flag", "深度有效标志", "该秒盘口有更新", R"(\mathbf{1}_{\mathrm{depth}})", FLAG) \
  X(_data_valid, META, RAW, NONE, "Data Valid Flag", "数据有效标志", "该秒有事件写入", R"(\mathbf{1}_{\mathrm{data}})", FLAG)

#define FIELDS_L1_Meta(X) \
  X(_data_valid, META, RAW, NONE, "Data Valid Flag", "数据有效标志", "该分钟有效(有成交)", R"(\mathbf{1}_{\mathrm{data}})", FLAG)

#define FIELDS_DEPTH_Meta(X)                                                                                                   \
  X(_bid_price, META, RAW, NONE, "Bid Prices", "买盘价格", "N档买盘价格(元)", R"(P^{M,B}_{1:N})", META(L2::LOB_DEPTH))         \
  X(_ask_price, META, RAW, NONE, "Ask Prices", "卖盘价格", "N档卖盘价格(元)", R"(P^{M,A}_{1:N})", META(L2::LOB_DEPTH))         \
  X(_bid_volume, META, RAW, NONE, "Bid Volumes", "买盘量", "N档买盘量(手)", R"(V^{M,B}_{1:N})", META(L2::LOB_DEPTH))           \
  X(_ask_volume, META, RAW, NONE, "Ask Volumes", "卖盘量", "N档卖盘量(手)", R"(V^{M,A}_{1:N})", META(L2::LOB_DEPTH))           \
  X(_mid_price, META, RAW, NONE, "Mid Price", "中间价", "分钟末中间价(元)", R"(\frac{P^{M,B}_1 + P^{M,A}_1}{2})", META(1))     \
  X(_depth_valid, META, RAW, NONE, "Depth Valid Flag", "深度有效", "该分钟盘口有更新", R"(\mathbf{1}_{\mathrm{depth}})", FLAG) \
  X(_data_valid, META, RAW, NONE, "Data Valid Flag", "数据有效", "该分钟有事件写入", R"(\mathbf{1}_{\mathrm{data}})", FLAG)
