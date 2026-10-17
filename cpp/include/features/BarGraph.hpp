#pragma once

#include "features/Bar/NodesGenerated.hpp" // CMake 从 Bar 档算子文件汇总: 全部算子 #include + Bar_NODES(N) + Bar_NODE_PREV_* + 字段表
#include "features/DataDefine.hpp"
#include "features/GraphNode.hpp"          // graph::Node + TIER_DAG (两档共用)
#include "features/Method/Fundamental.hpp" // fund::Pool (DAG_Root 日频输入)
#include "features/Operator/Bar/TS/Label/LabelBar.hpp"
#include <string>

// ============================================================================
// Bar 档 DAG (namespace bar; 与 LobGraph.hpp 对仗, 一个 TU 只许 include 其一). 公共体见 GraphNode.hpp TIER_DAG.
// 源 = 分钟 K (MinuteData, CoreBar 由 bar1m 填充) + 日频 PIT (fund::Pool), 触发域只有 onDay / onMinute.
// ============================================================================
namespace bar {

struct DAG_Root {
  MinuteData minute_data;      // L1 输入 (CoreBar 由 bar1m 填充)
  const fund::Pool &fund_pool; // 日频输入 (外部传入, 只读共享; Fund 节点按 asset_id_/date_ 取用)
  std::string asset_code_;     // "CODE.EX"
  size_t asset_id_;            // 档 A 轴下标
  misc::Ymd date_ = 0;         // 当日 (at_day_start 设置)
  DAG_Root(const fund::Pool &pool, const std::string &code, size_t asset_id)
      : fund_pool(pool), asset_code_(code), asset_id_(asset_id) {}
};

#define DAG_TIER Bar
TIER_DAG(::LabelBar, minute_data)
#undef DAG_TIER

} // namespace bar
