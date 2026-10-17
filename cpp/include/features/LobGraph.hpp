#pragma once

#include "features/DataDefine.hpp"
#include "features/GraphNode.hpp"          // graph::Node + TIER_DAG (两档共用)
#include "features/Lob/NodesGenerated.hpp" // IWYU pragma: keep
#include "features/Node/Lob/TS/Label/LabelLob.hpp"

// ============================================================================
// Lob 档 DAG (namespace lob; 与 BarGraph.hpp 对仗, 一个 TU 只许 include 其一). 公共体见 GraphNode.hpp TIER_DAG.
// 源 = 逐笔 (TickData, 订单簿还原写出) + 分钟 K (MinuteData, 由逐笔重采样) + 日频 PIT (fund::Panel).
// ============================================================================
namespace lob {

// TickData 自持而非引用 worker 订单簿的: DAG 节点引用它 (终身有效), 订单簿经 bind() 把写出口换到这里 →
// 资产可在 worker 间转移处置权 (负载再平衡), 任意 worker 的订单簿都能驱动本资产
struct DAG_Root : graph::Root {
  TickData tick_data; // L_sec 输入 (订单簿还原写出)
  using graph::Root::Root;
};

#define DAG_TIER Lob
TIER_DAG(::LabelLob, tick_data, minute_data, Depth.bid_price, Depth.ask_price, Depth.bid_qty, Depth.ask_qty, Fund.y[Fund.lim_up], Fund.y[Fund.lim_dn])
#undef DAG_TIER

} // namespace lob
