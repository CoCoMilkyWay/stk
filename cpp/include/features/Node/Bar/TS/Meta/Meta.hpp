#pragma once
// 元数据列: 基建标志 ts_valid / cs_valid (FLAG), 由 CoreTs 手工写 (Bar 档 L_1min / 分钟箱层 / L_1day, 各层对仗). 编码/解码 → features/MetaFlag.hpp
//   ts_valid = 行内有成交 (bar1m volume > 0) ? fmeta::pack(false, 1) : 0. 无盘口, 符号位 (depth) 恒不置.
//            L_1min 行稠密 (每分钟都写, 特征列 ALL 不受本列门控), 本列只是"有成交"信息位 + 标签 entry / exit 的推进条件.
//   cs_valid = 当日在母集 ? 1 : 0 —— 日频常量 (盘前一次写满全部行, 逐行存, CS 行自足):
//              Fund base 口: 在市 (非北交所 ∧ 已上市 ∧ 未退市 ∧ 当日非停牌) ∧ 非次新 (上市满 365 日历日) ∧ 申万一级行业已知
//              ∧ st_level == 0 ∧ st_* 皆 0 (全部盘前可知).
//              只看本资产代码 + 本资产 Fund 当日 PIT 口 → TS 资产局部纯函数, 不破一致性红线.
//              全市场截面 (CS 列) 只在它上面算; 池列 (Select*) 例外走全轴, 成员范围由 PoolKey 键掩码定 (ST 池在母集外).

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L_1min_Meta(X, CAT1)                                                                                                      \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=该分钟无成交 (行仍写, 特征稠密); 1=有成交", R"(\mathbf{1}_{\mathrm{trade}})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在母集=1 (在市∧上市满365日∧行业已知∧无ST/风险警示) 否则 0 (整日常量); 截面只看此列", R"(\mathbf{1}_{\mathrm{base},D})", FLAG)

// 分钟箱层与 L_1min 对仗: ts_valid = 本箱有成交 (箱内任一分钟有成交), cs_valid 同一日频常量
#define BAR_BIN_META(X, CAT1, n)                                                                                                                     \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=该 " #n " 分钟箱无成交 (行仍写, 特征稠密); 1=有成交", R"(\mathbf{1}_{\mathrm{trade},k})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在母集=1 (在市∧上市满365日∧行业已知∧无ST/风险警示) 否则 0 (整日常量); 截面只看此列", R"(\mathbf{1}_{\mathrm{base},D})", FLAG)
#define FIELDS_L_5min_Meta(X, CAT1) BAR_BIN_META(X, CAT1, 5)
#define FIELDS_L_15min_Meta(X, CAT1) BAR_BIN_META(X, CAT1, 15)
#define FIELDS_L_30min_Meta(X, CAT1) BAR_BIN_META(X, CAT1, 30)
#define FIELDS_L_60min_Meta(X, CAT1) BAR_BIN_META(X, CAT1, 60)

// L_1day (整日箱) 与分钟箱层对仗: ts_valid = 当日有成交, cs_valid 同一日频常量
#define FIELDS_L_1day_Meta(X, CAT1)                                                                                                      \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=当日无成交 (行仍写, 特征稠密); 1=有成交", R"(\mathbf{1}_{\mathrm{trade},D})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在母集=1 (在市∧上市满365日∧行业已知∧无ST/风险警示) 否则 0 (整日常量); 截面只看此列", R"(\mathbf{1}_{\mathrm{base},D})", FLAG)
