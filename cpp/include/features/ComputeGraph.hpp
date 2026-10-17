#pragma once

#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp"     // NODE_COMPUTE / NODE_FLUSH
#include "features/GraphNode.hpp"          // graph::Node (两档共用)
#include "features/Method/Fundamental.hpp" // fund::Pool (DAG_Root 日频输入)
#include "features/Operator/Pool/TS/Label/LabelReturn.hpp"
#include "features/Pool/NodesGenerated.hpp" // CMake 从 Pool 档算子文件汇总: 全部算子 #include + Pool_NODES(N) + Pool_NODE_PREV_* + 字段表
#include <cassert>
#include <string>

// ============================================================================
// Pool 档 DAG 声明链 (namespace pool; All 档的图在 BarGraph.hpp, namespace all). 节点按 NodesGenerated.hpp 的拓扑序, 每个节点一层:
//   struct DAG_<name> : Pool_NODE_PREV_<name> { graph::Node<Op> name{inputs...}; };
// 一层里只看得到更早的层, 所以节点输入引用了未排在前面的节点 (漏 #include 依赖) 是编译错误,
// 构造序 (基类先) 和 run<>() 的执行序 (NODES 行序) 因此一定一致. 空基类无运行时开销.
// ============================================================================
namespace pool {

struct DAG_Root {
  TickData &tick_data;         // L0 输入 (外部传入)
  MinuteData minute_data;      // L1 输入 (内部管理, 由 resampler 填充)
  const fund::Pool &fund_pool; // 日频输入 (外部传入, 只读共享; Fund 节点按 asset_id_/date_ 取用)
  std::string asset_code_;     // 股票代码 (用于涨跌幅判断)
  size_t asset_id_;            // AssetAxis 下标
  misc::Ymd date_ = 0;         // 当日 (at_day_start 设置)
  DAG_Root(TickData &td, const fund::Pool &pool, const std::string &code, size_t asset_id)
      : tick_data(td), fund_pool(pool), asset_code_(code), asset_id_(asset_id) {}
};

#define DAG_DECLARE_NODE(name, type, args, ...)         \
  struct DAG_##name : Pool_NODE_PREV_##name {           \
    using Pool_NODE_PREV_##name::Pool_NODE_PREV_##name; \
    graph::Node<DAG_UNPAREN type> name DAG_BRACE args;  \
  };
Pool_NODES(DAG_DECLARE_NODE)
#undef DAG_DECLARE_NODE

    // DAG: 有向无环计算图 = 声明链末端 + 标签 + 调度.
    class DAG : public Pool_DAG_LAST {
public:
  // 标签 (回填别的时间行, 不在节点表; 快照 / 回填调用在 CoreSequential::run<0>)
  ::LabelReturn LabelReturn{tick_data, minute_data, Depth.bid_price, Depth.ask_price, Depth.bid_qty, Depth.ask_qty, Fund.y[Fund.lim_up], Fund.y[Fund.lim_dn]};

  DAG(TickData &td, const fund::Pool &pool, const std::string &code, size_t asset_id) : Pool_DAG_LAST(td, pool, code, asset_id) {}

  // ===========================================================================
  // 触发域调度: 对表里 compute/flush 触发域 == trig 的节点, 按行序调 compute()/flush()
  // ===========================================================================
  template <Trigger trig>
  [[gnu::always_inline]] inline void run() {
#define DAG_RUN_NODE(name, type, args, ...)        \
  if constexpr (NODE_COMPUTE(__VA_ARGS__) == trig) \
    name.compute();                                \
  if constexpr (NODE_FLUSH(__VA_ARGS__) == trig)   \
    name.flush();
    Pool_NODES(DAG_RUN_NODE)
#undef DAG_RUN_NODE
  }

  // 盘前: 设日期 → 全部节点 reset → onDay 域 (日频算子 compute)
  void at_day_start(misc::Ymd date) {
    date_ = date;
#define DAG_RESET_NODE(name, type, args, ...) name.reset();
    Pool_NODES(DAG_RESET_NODE)
#undef DAG_RESET_NODE
        LabelReturn.reset();
    run<Trigger::onDay>();
  }

  // 盘尾钩子 (目前无事可做; 留给需要收盘结算的特征)
  void at_day_end() {}
};

} // namespace pool
