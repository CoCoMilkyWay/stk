#pragma once

#include "features/DataDefine.hpp"
#include <cassert>

// ============================================================================
// Node: 算子实例 + 它的输出 Series. 算子只写 y[口], 推入缓冲由 Node 统一做. 两档构图端共用
// (Pool: ComputeGraph.hpp / All: BarGraph.hpp; 一个 TU 只许 include 其一, 见 NodesGenerated.hpp).
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
