#pragma once
// 元数据列: 基建标志 ts_valid / cs_valid (FLAG), 由 CoreTs 手工写 (Bar 档只有 L1). 编码/解码 → features/MetaFlag.hpp
//   ts_valid = 本分钟有成交 (bar1m volume > 0) ? fmeta::pack(false, 1) : 0. 无盘口, 符号位 (depth) 恒不置.
//            L1 行稠密 (每分钟都写, 特征列 ALL 不受本列门控), 本列只是"有成交"信息位 + 标签 entry / exit 的推进条件.
//   cs_valid = 当日在市可交易 ? 1 : 0 —— 日频常量 (盘前一次写满全部行, 逐行存, CS 行自足):
//              非北交所 ∧ 已上市 (list_age ≥ 0) ∧ 未退市 (delist_age 未定义) ∧ 当日非停牌 (is_susp, 盘前可知).
//              只看本资产代码 + 本资产 Fund 当日 PIT 口 → TS 资产局部纯函数, 不破一致性红线.
//              全市场截面 (CS 列 / 选池) 只在它上面算; 选池母集的其余条件 (上市满 60 日 / 非 ST / st_* / 行业) 进 PoolKey 键掩码.

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L1_Meta(X, CAT1)                                                                                                          \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=该分钟无成交 (行仍写, 特征稠密); 1=有成交", R"(\mathbf{1}_{\mathrm{trade}})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在市可交易=1 (非北交所∧已上市∧未退市∧非停牌) 否则 0 (整日常量); 截面只看此列", R"(\mathbf{1}_{\mathrm{mkt},D})", FLAG)
