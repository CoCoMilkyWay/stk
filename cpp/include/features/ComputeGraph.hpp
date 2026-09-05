#pragma once

#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp"     // NODE_COMPUTE / NODE_FLUSH
#include "features/Method/Fundamental.hpp" // fund::Pool (DAG_Root 日频输入)
#include "features/NodesGenerated.hpp"     // CMake 从算子文件汇总: 全部算子 #include + NODES(N) + NODE_PREV_* + 字段表
#include "features/Operator/TS/Label/LabelReturn.hpp"
#include <cassert>
#include <string>

// ============================================================================
// Node: 算子实例 + 它的输出 Series. 算子只写 y[口], 推入缓冲由 Node 统一做.
//   node.compute()                   算子的
//   node.flush()                     算子有 flush() 先调 (降频型结算到 y), 然后 y[i] → bufs_[i]
//   node.reset()                     算子有 reset() 才调
//   node.out(i) / node.out()         第 i 个输出口 (下游节点输入用); 单口默认 0
//   node.outs()                      全部输出口数组 Series[K] (下游按口成组消费)
//   node.last(i)                     输出口最新值, 尚无输出时 0 (字段表写回用)
//   node.<port>                      算子 enum Out 的枚举值 (node.out(node.price))
// ============================================================================
namespace graph {

template <class Op, size_t K = Op::kCount>
struct Node : Op {
  using Op::Op;
  Series bufs_[K];

  Series &out(size_t i = 0) {
    assert(i < K);
    return bufs_[i];
  }
  const Series &out(size_t i = 0) const {
    assert(i < K);
    return bufs_[i];
  }
  const Series (&outs() const)[K] { return bufs_; }
  float last(size_t i = 0) const {
    assert(i < K);
    return bufs_[i].empty() ? 0.0f : bufs_[i].back();
  }

  [[gnu::always_inline]] inline void flush() {
    if constexpr (requires(Op &o) { o.flush(); })
      Op::flush();
    for (size_t i = 0; i < K; ++i)
      bufs_[i].push_back(Op::y[i]);
  }
  void reset() {
    if constexpr (requires(Op &o) { o.reset(); })
      Op::reset();
  }
};

// 源层节点 (kCount = 0): 无标量输出口, 自持输出 (如 Depth 的 N 档数组); flush/reset 有才调
template <class Op>
struct Node<Op, 0> : Op {
  using Op::Op;
  void flush() {
    if constexpr (requires(Op &o) { o.flush(); })
      Op::flush();
  }
  void reset() {
    if constexpr (requires(Op &o) { o.reset(); })
      Op::reset();
  }
};

} // namespace graph

#define DAG_UNPAREN(...) __VA_ARGS__
#define DAG_BRACE(...) {__VA_ARGS__}

// ============================================================================
// DAG 声明链. 节点按 NodesGenerated.hpp 的拓扑序, 每个节点一层:
//   struct DAG_<name> : NODE_PREV_<name> { graph::Node<Op> name{inputs...}; };
// 一层里只看得到更早的层, 所以节点输入引用了未排在前面的节点 (漏 #include 依赖) 是编译错误,
// 构造序 (基类先) 和 run<>() 的执行序 (NODES 行序) 因此一定一致. 空基类无运行时开销.
// ============================================================================
struct DAG_Root {
  TickData &tick_data;         // L0 输入 (外部传入)
  MinuteData minute_data;      // L1 输入 (内部管理, 由 resampler 填充)
  const fund::Pool &fund_pool; // 日频输入 (外部传入, 只读共享; Fund 节点按 asset_id_/date_ 取用)
  std::string asset_code_;     // 股票代码 (用于涨跌幅判断)
  size_t asset_id_;            // AssetAxis 下标
  std::string date_;           // 当日 "YYYYMMDD" (at_day_start 设置)
  DAG_Root(TickData &td, const fund::Pool &pool, const std::string &code, size_t asset_id)
      : tick_data(td), fund_pool(pool), asset_code_(code), asset_id_(asset_id) {}
};

#define DAG_DECLARE_NODE(name, type, args, ...)        \
  struct DAG_##name : NODE_PREV_##name {               \
    using NODE_PREV_##name::NODE_PREV_##name;          \
    graph::Node<DAG_UNPAREN type> name DAG_BRACE args; \
  };
NODES(DAG_DECLARE_NODE)
#undef DAG_DECLARE_NODE

// DAG: 有向无环计算图 = 声明链末端 + 标签 + 调度.
class DAG : public DAG_LAST {
public:
  // 标签 (回填别的时间行, 不在节点表; 快照 / 回填调用在 CoreSequential::run_tick)
  ::LabelReturn LabelReturn{Depth.bid_price, Depth.ask_price, Depth.bid_qty, Depth.ask_qty};

  DAG(TickData &td, const fund::Pool &pool, const std::string &code, size_t asset_id) : DAG_LAST(td, pool, code, asset_id) {}

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
    NODES(DAG_RUN_NODE)
#undef DAG_RUN_NODE
  }

  // 盘前: 设日期 → 全部节点 reset → onDay 域 (日频算子 compute)
  void at_day_start(const std::string &date) {
    date_ = date;
#define DAG_RESET_NODE(name, type, args, ...) name.reset();
    NODES(DAG_RESET_NODE)
#undef DAG_RESET_NODE
    LabelReturn.reset();
    run<Trigger::onDay>();
  }

  // 盘尾钩子 (目前无事可做; 留给需要收盘结算的特征)
  void at_day_end() {}
};
