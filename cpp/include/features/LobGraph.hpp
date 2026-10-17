#pragma once

#include "features/DataDefine.hpp"
#include "features/GraphNode.hpp"          // graph::Node + TIER_DAG (两档共用)
#include "features/Lob/NodesGenerated.hpp" // CMake 从 Lob 档算子文件汇总: 全部算子 #include + Lob_NODES(N) + Lob_NODE_PREV_* + 字段表
#include "features/Method/Fundamental.hpp" // fund::Pool (DAG_Root 日频输入)
#include "features/Operator/Lob/TS/Label/LabelLob.hpp"
#include <string>

// ============================================================================
// Lob 档 DAG (namespace lob; 与 BarGraph.hpp 对仗, 一个 TU 只许 include 其一). 公共体见 GraphNode.hpp TIER_DAG.
// 源 = 逐笔 (TickData, LOB 还原写出) + 分钟 K (MinuteData, 由逐笔重采样) + 日频 PIT (fund::Pool).
// ============================================================================
namespace lob {

struct DAG_Root {
  TickData &tick_data;         // L0 输入 (外部传入)
  MinuteData minute_data;      // L1 输入 (CoreLob 由 tick 重采样填充)
  const fund::Pool &fund_pool; // 日频输入 (外部传入, 只读共享; Fund 节点按 asset_id_/date_ 取用)
  std::string asset_code_;     // "CODE.EX"
  size_t asset_id_;            // 档 A 轴下标
  misc::Ymd date_ = 0;         // 当日 (at_day_start 设置)
  DAG_Root(TickData &td, const fund::Pool &pool, const std::string &code, size_t asset_id)
      : tick_data(td), fund_pool(pool), asset_code_(code), asset_id_(asset_id) {}
};

#define DAG_TIER Lob
TIER_DAG(::LabelLob, tick_data, minute_data, Depth.bid_price, Depth.ask_price, Depth.bid_qty, Depth.ask_qty, Fund.y[Fund.lim_up], Fund.y[Fund.lim_dn])
#undef DAG_TIER

} // namespace lob
