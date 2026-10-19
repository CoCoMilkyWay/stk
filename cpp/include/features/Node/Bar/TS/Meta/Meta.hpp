#pragma once
// 元数据列: 基建标志 ts_valid / cs_valid (FLAG), 由 CoreTs 手工写 (Bar 档 L_min / 分钟箱层 / L_day, 各层对仗). 编码/解码 → features/MetaFlag.hpp
//   ts_valid = 行内有成交 (bar1m volume > 0) ? fmeta::pack(false, 1) : 0. 无盘口, 符号位 (depth) 恒不置.
//            L_min 行稠密 (每分钟都写, 特征列 ALL 不受本列门控), 本列只是"有成交"信息位 + 标签 entry / exit 的推进条件.
//   cs_valid = 当日在市可交易 ? 1 : 0 —— 日频常量 (盘前一次写满全部行, 逐行存, CS 行自足):
//              非北交所 ∧ 已上市 (list_age ≥ 0) ∧ 未退市 (delist_age 未定义) ∧ 当日非停牌 (is_susp, 盘前可知).
//              只看本资产代码 + 本资产 Fund 当日 PIT 口 → TS 资产局部纯函数, 不破一致性红线.
//              全市场截面 (CS 列 / 票池) 只在它上面算; 票池母集的其余条件 (上市满 60 日 / 非 ST / st_* / 行业) 进 PoolKey 键掩码.

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L_min_Meta(X, CAT1)                                                                                                       \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=该分钟无成交 (行仍写, 特征稠密); 1=有成交", R"(\mathbf{1}_{\mathrm{trade}})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在市可交易=1 (非北交所∧已上市∧未退市∧非停牌) 否则 0 (整日常量); 截面只看此列", R"(\mathbf{1}_{\mathrm{mkt},D})", FLAG)

// 分钟箱层与 L_min 对仗: ts_valid = 本箱有成交 (箱内任一分钟有成交), cs_valid 同一日频常量
#define BAR_BIN_META(X, CAT1, n)                                                                                                                     \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=该 " #n " 分钟箱无成交 (行仍写, 特征稠密); 1=有成交", R"(\mathbf{1}_{\mathrm{trade},k})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在市可交易=1 (非北交所∧已上市∧未退市∧非停牌) 否则 0 (整日常量); 截面只看此列", R"(\mathbf{1}_{\mathrm{mkt},D})", FLAG)
#define FIELDS_L_5min_Meta(X, CAT1) BAR_BIN_META(X, CAT1, 5)
#define FIELDS_L_15min_Meta(X, CAT1) BAR_BIN_META(X, CAT1, 15)
#define FIELDS_L_30min_Meta(X, CAT1) BAR_BIN_META(X, CAT1, 30)
#define FIELDS_L_60min_Meta(X, CAT1) BAR_BIN_META(X, CAT1, 60)

// L_day (整日箱) 与分钟箱层对仗: ts_valid = 当日有成交, cs_valid 同一日频常量
#define FIELDS_L_day_Meta(X, CAT1)                                                                                                       \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=当日无成交 (行仍写, 特征稠密); 1=有成交", R"(\mathbf{1}_{\mathrm{trade},D})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在市可交易=1 (非北交所∧已上市∧未退市∧非停牌) 否则 0 (整日常量); 截面只看此列", R"(\mathbf{1}_{\mathrm{mkt},D})", FLAG)
