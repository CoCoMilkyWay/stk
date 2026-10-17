#pragma once
// 元数据列: 基建标志 ts_valid / cs_valid (FLAG), 由 CoreTs 手工写, 两层都落. 无节点 (不进 DAG):
//   fmeta        编码/解码唯一事实源 → features/MetaFlag.hpp (稳定头, 消费端不依赖本节点文件)
//   MetaTracker  状态机 + 编码语义 → Node/Common/Meta/Meta.hpp (两档共用; Lob 档每笔另喂 on_tick)
//   cs_valid = 当日在池: 票池 = all 库 D−1 收盘行 (L1 254) 的 pool_<库名> 列 (pool::Panel, 见 shared/Pool.hpp)

#include "features/Node/Common/Meta/Meta.hpp" // IWYU pragma: keep

// ---- 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define FIELDS_L0_Meta(X, CAT1)                                                                                                                             \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=无事件; ±micro price, 负=该秒盘口有更新", R"(\pm P_t^{micro} \cdot \mathbf{1}_{\mathrm{data}})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在池=1 否则 0 (整日常量, 与事件无关); 截面只看此列", R"(\mathbf{1}_{\mathrm{pool},D})", FLAG)

#define FIELDS_L1_Meta(X, CAT1)                                                                                                                                   \
  X(ts_valid, CAT1, AUTO, "TS Valid", "时序有效", "0=该分钟无成交 (行仍写, 特征稠密); ±1 有成交, 负=该分钟盘口有更新", R"(\pm\mathbf{1}_{\mathrm{trade}})", FLAG) \
  X(cs_valid, CAT1, AUTO, "CS Valid", "截面有效", "当日在池=1 否则 0 (整日常量, 与事件无关); 截面只看此列", R"(\mathbf{1}_{\mathrm{pool},D})", FLAG)
