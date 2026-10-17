#pragma once

#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp" // NODE_COMPUTE / NODE_FLUSH (TIER_DAG)
#include <cassert>

// ============================================================================
// Node: 算子实例 + 它的输出 Series. 算子只写 y[口], 推入缓冲由 Node 统一做. 两档构图端共用
// (LobGraph.hpp / BarGraph.hpp; 一个 TU 只许 include 其一, 见 NodesGenerated.hpp).
//   node.compute()                   算子的
//   node.flush()                     算子有 flush() 先调 (降频型结算到 y), 然后 y[i] → bufs_[i]
//   node.reset()                     跨天: 清空全部输出 Series (不留昨日尾值), 算子有 reset() 再调
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
    for (auto &b : bufs_)
      b.clear();
    if constexpr (requires(Op &o) { o.reset(); })
      Op::reset();
  }
};

// 源层节点 (kCount = 0): 无标量输出口, 自持输出 (如 Depth 的 N 档数组, 由算子自己的 reset 清空); flush/reset 有才调
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
// 档 DAG 公共体 (两档同一份). 档图头只写档特有的: 源 (DAG_Root, 输入成员 + 构造) 与标签类型 / 初始化,
// 然后 #define DAG_TIER <Tier> → TIER_DAG(LabelType, label 初始化实参...) → #undef DAG_TIER. 展开为:
//   声明链  struct DAG_<name> : <Tier>_NODE_PREV_<name> { graph::Node<Op> name{inputs...}; };
//           节点按 NodesGenerated.hpp 的拓扑序每个一层, 一层里只看得到更早的层 —— 输入引用了未排在前面的节点
//           是编译错误, 构造序 (基类先) 与 run<>() 执行序 (NODES 行序) 因此一定一致. 空基类无运行时开销.
//   class DAG  声明链末端 + 标签成员 label (回填别的时间行, 不在节点表; 调用在 CoreTs) + 触发域调度:
//     run<trig>()        对表里 compute/flush 触发域 == trig 的节点, 按行序调 compute()/flush()
//     at_day_start(date) 设日期 → 全部节点 reset → 标签 reset → onDay 域 (日频算子 compute)
//     at_day_end()       盘尾钩子 (目前无事可做; 留给需要收盘结算的特征)
// ============================================================================
#define DAG_CAT_(a, b) a##b
#define DAG_CAT(a, b) DAG_CAT_(a, b)
#define DAG_PREV_(T, name) T##_NODE_PREV_##name
#define DAG_PREV(T, name) DAG_PREV_(T, name)
#define DAG_DECLARE_NODE(name, type, args, ...)               \
  struct DAG_##name : DAG_PREV(DAG_TIER, name) {              \
    using DAG_PREV(DAG_TIER, name)::DAG_PREV(DAG_TIER, name); \
    graph::Node<DAG_UNPAREN type> name DAG_BRACE args;        \
  };
#define DAG_RUN_NODE(name, type, args, ...)        \
  if constexpr (NODE_COMPUTE(__VA_ARGS__) == trig) \
    name.compute();                                \
  if constexpr (NODE_FLUSH(__VA_ARGS__) == trig)   \
    name.flush();
#define DAG_RESET_NODE(name, type, args, ...) name.reset();
#define TIER_DAG(LabelType, ...)                                                                \
  DAG_CAT(DAG_TIER, _NODES)(DAG_DECLARE_NODE) class DAG : public DAG_CAT(DAG_TIER, _DAG_LAST) { \
  public:                                                                                       \
    LabelType label{__VA_ARGS__};                                                               \
    template <class... Args>                                                                    \
    explicit DAG(Args &&...args) : DAG_CAT(DAG_TIER, _DAG_LAST)(std::forward<Args>(args)...) {} \
    template <Trigger trig>                                                                     \
    [[gnu::always_inline]] inline void run() { DAG_CAT(DAG_TIER, _NODES)(DAG_RUN_NODE) }        \
    void at_day_start(misc::Ymd date) {                                                         \
      date_ = date;                                                                             \
      DAG_CAT(DAG_TIER, _NODES)(DAG_RESET_NODE)                                                 \
          label.reset();                                                                        \
      run<Trigger::onDay>();                                                                    \
    }                                                                                           \
    void at_day_end() {}                                                                        \
  };
