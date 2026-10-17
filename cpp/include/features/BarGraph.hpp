#pragma once

#include "features/All/NodesGenerated.hpp" // CMake 从 All 档算子文件汇总: 全部算子 #include + All_NODES(N) + All_NODE_PREV_* + 字段表
#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp"     // NODE_COMPUTE / NODE_FLUSH
#include "features/GraphNode.hpp"          // graph::Node (两档共用)
#include "features/Method/Fundamental.hpp" // fund::Pool (DAG_Root 日频输入)
#include "features/Operator/All/TS/Label/LabelBar.hpp"
#include <string>

// ============================================================================
// All 档 DAG 声明链 (namespace all; 与 ComputeGraph.hpp 的 pool 对仗, 一个 TU 只许 include 其一).
//   struct DAG_<name> : All_NODE_PREV_<name> { graph::Node<Op> name{inputs...}; };
// 源只有分钟 K (MinuteData, CoreBar 由 bar1m 填充) + 日频 PIT (fund::Pool), 触发域只有 onDay / onMinute.
// ============================================================================
namespace all {

struct DAG_Root {
  MinuteData minute_data;      // L1 输入 (CoreBar 逐分钟填充)
  const fund::Pool &fund_pool; // 日频输入 (外部传入, 只读共享; Fund 节点按 asset_id_/date_ 取用)
  std::string asset_code_;     // "CODE.EX"
  size_t asset_id_;            // AssetAxis 下标
  misc::Ymd date_ = 0;         // 当日 (at_day_start 设置)
  DAG_Root(const fund::Pool &pool, const std::string &code, size_t asset_id)
      : fund_pool(pool), asset_code_(code), asset_id_(asset_id) {}
};

#define DAG_DECLARE_NODE(name, type, args, ...)        \
  struct DAG_##name : All_NODE_PREV_##name {           \
    using All_NODE_PREV_##name::All_NODE_PREV_##name;  \
    graph::Node<DAG_UNPAREN type> name DAG_BRACE args; \
  };
All_NODES(DAG_DECLARE_NODE)
#undef DAG_DECLARE_NODE

    // DAG: 声明链末端 + 标签 + 调度
    class DAG : public All_DAG_LAST {
public:
  // 标签 (回填别的时间行, 不在节点表; 调用在 CoreBar::minute / end_day)
  ::LabelBar LabelBar{minute_data};

  DAG(const fund::Pool &pool, const std::string &code, size_t asset_id) : All_DAG_LAST(pool, code, asset_id) {}

  // 触发域调度: 对表里 compute/flush 触发域 == trig 的节点, 按行序调 compute()/flush()
  template <Trigger trig>
  [[gnu::always_inline]] inline void run() {
#define DAG_RUN_NODE(name, type, args, ...)        \
  if constexpr (NODE_COMPUTE(__VA_ARGS__) == trig) \
    name.compute();                                \
  if constexpr (NODE_FLUSH(__VA_ARGS__) == trig)   \
    name.flush();
    All_NODES(DAG_RUN_NODE)
#undef DAG_RUN_NODE
  }

  // 盘前: 设日期 → 全部节点 reset → onDay 域 (日频算子 compute)
  void at_day_start(misc::Ymd date) {
    date_ = date;
#define DAG_RESET_NODE(name, type, args, ...) name.reset();
    All_NODES(DAG_RESET_NODE)
#undef DAG_RESET_NODE
        LabelBar.reset();
    run<Trigger::onDay>();
  }

  void at_day_end() {}
};

} // namespace all
