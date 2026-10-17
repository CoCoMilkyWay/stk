#pragma once

#include "features/Bar/NodesGenerated.hpp" // IWYU pragma: keep
#include "features/GraphNode.hpp"          // graph::Node + TIER_DAG (两档共用)
#include "features/Node/Bar/TS/Label/LabelBar.hpp"

// ============================================================================
// Bar 档 DAG (namespace bar; 与 LobGraph.hpp 对仗, 一个 TU 只许 include 其一). 公共体见 GraphNode.hpp TIER_DAG.
// 源 = 分钟 K (MinuteData, CoreBar 由 bar1m 填充) + 10 分钟 K (Min10Data, CoreTs 重采样) + 日频 PIT (fund::Panel),
// 触发域只有 onDay / onMinute / on10Min.
// ============================================================================
namespace bar {

struct DAG_Root : graph::Root {
  using graph::Root::Root;
};

#define DAG_TIER Bar
TIER_DAG(::LabelBar, minute_data)
#undef DAG_TIER

} // namespace bar
