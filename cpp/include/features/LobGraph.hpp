#pragma once

#include "features/DataDefine.hpp"
#include "features/GraphNode.hpp"          // graph::Node + TIER_DAG (两档共用)
#include "features/Lob/NodesGenerated.hpp" // IWYU pragma: keep
#include "features/Node/Lob/TS/Label/LabelLob.hpp"

// ============================================================================
// Lob 档 DAG (namespace lob; 与 BarGraph.hpp 对仗, 一个 TU 只许 include 其一). 公共体见 GraphNode.hpp TIER_DAG.
// 源 = 逐笔 (TickData, LOB 还原写出) + 分钟 K (MinuteData, 由逐笔重采样) + 日频 PIT (fund::Panel).
// ============================================================================
namespace lob {

// TickData 自持而非引用 worker LOB 的: DAG 节点引用它 (终身有效), LOB 经 bind() 把写出口换到这里 →
// 资产可在 worker 间转移处置权 (负载再平衡), 任意 worker 的 LOB 都能驱动本资产
struct DAG_Root : graph::Root {
  TickData tick_data; // L0 输入 (LOB 还原写出)
  using graph::Root::Root;
};

#define DAG_TIER Lob
TIER_DAG(::LabelLob, tick_data, minute_data, Depth.bid_price, Depth.ask_price, Depth.bid_qty, Depth.ask_qty, Fund.y[Fund.lim_up], Fund.y[Fund.lim_dn])
#undef DAG_TIER

} // namespace lob
