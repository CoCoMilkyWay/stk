#pragma once

#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp"
#include <cassert>
#include <utility>
// Basic
#include "features/Operator/TS/Basic/MicroPrice.hpp"
#include "features/Operator/TS/Basic/MidPrice.hpp"
#include "features/Operator/TS/Basic/MinuteIndex.hpp"
#include "features/Operator/TS/Basic/Spread.hpp"
#include "features/Operator/TS/Basic/TickIndex.hpp"
#include "features/Operator/TS/Basic/Valuation.hpp"
// Imbalance
#include "features/Operator/TS/Imbalance/CI.hpp"
#include "features/Operator/TS/Imbalance/CI_all.hpp"
#include "features/Operator/TS/Imbalance/CWI.hpp"
#include "features/Operator/TS/Imbalance/DDI.hpp"
#include "features/Operator/TS/Imbalance/EntropyImba.hpp"
#include "features/Operator/TS/Imbalance/GradImba.hpp"
#include "features/Operator/TS/Imbalance/ParaImba.hpp"
// OrderFlow
#include "features/Operator/TS/OrderFlow/CTR.hpp"
#include "features/Operator/TS/OrderFlow/FlowRate.hpp"
#include "features/Operator/TS/OrderFlow/HLA.hpp"
#include "features/Operator/TS/OrderFlow/OA.hpp"
#include "features/Operator/TS/OrderFlow/OFI.hpp"
#include "features/Operator/TS/OrderFlow/OrderInfo.hpp"
#include "features/Operator/TS/OrderFlow/ToxicCr.hpp"
// Behavioral
#include "features/Operator/TS/Behavioral/Behav.hpp"
#include "features/Operator/TS/Behavioral/Manip.hpp"
// Resilience
#include "features/Operator/TS/Resilience/Resiliency.hpp"
// Liquidity
#include "features/Operator/TS/Liquidity/Cost.hpp"
#include "features/Operator/TS/Liquidity/TLR.hpp"
// Shape
#include "features/Operator/TS/Shape/DepthRepresentation.hpp"
#include "features/Operator/TS/Shape/Entropy.hpp"
#include "features/Operator/TS/Shape/Grad.hpp"
#include "features/Operator/TS/Shape/Para.hpp"
#include "features/Operator/TS/Shape/Peak.hpp"
// Meta
#include "features/Operator/TS/Meta/DepthData.hpp"
#include "features/Operator/TS/Meta/DepthIndex.hpp"
// Label
#include "features/Operator/TS/Label/LabelReturn.hpp"

// ============================================================================
// Node: 算子实例 + 它的输出 CBuffer. 算子 (Op) 只拿引用, 缓冲由 Node 持有.
//   node.compute()/flush()/reset()   直接是算子的
//   node.out(i) / node.out()         第 i 个输出口 (下游节点输入用); 单口默认 0
//   node.last(i)                     输出口最新值, 尚无输出时 0 (字段表写回用)
//   node.<port>                      算子 enum Out 的枚举值 (node.out(node.price))
// ============================================================================
namespace graph {

template <size_t K>
struct Outs {
  CBuffer<float, L2::BLEN> bufs_[K];
  CBuffer<float, L2::BLEN> &out(size_t i = 0) {
    assert(i < K);
    return bufs_[i];
  }
  const CBuffer<float, L2::BLEN> &out(size_t i = 0) const {
    assert(i < K);
    return bufs_[i];
  }
  float last(size_t i = 0) const {
    assert(i < K);
    return bufs_[i].empty() ? 0.0f : bufs_[i].back();
  }
};

template <class Op, size_t K = Op::kCount>
struct Node : Outs<K>, Op {
  template <class... A>
  explicit Node(A &&...a) : Outs<K>(), Op(std::forward<A>(a)..., this->bufs_) {}
};

// 源层节点 (kCount = 0): 无标量输出口, 自持输出 (如 DepthData 的 N 档数组)
template <class Op>
struct Node<Op, 0> : Op {
  template <class... A>
  explicit Node(A &&...a) : Op(std::forward<A>(a)...) {}
};

} // namespace graph

#define DAG_UNPAREN(...) __VA_ARGS__
#define DAG_BRACE(...) {__VA_ARGS__}

// DAG: 有向无环计算图. 节点 = FeaturesDefine.hpp NODES 表逐行展开 (行序 = 声明序 = 执行序).
class DAG {
public:
  // ===========================================================================
  // 事件/时间驱动: 底层数据结构 (节点构造参数里可引用)
  // ===========================================================================
  TickData &tick_data;             // L0 输入 (外部传入)
  MinuteData minute_data;          // L1 输入 (内部管理, 由 resampler 填充)
  std::string asset_code_;         // 股票代码 (用于涨跌幅判断)
  const float *fund_row_{nullptr}; // 当日基本面输入行 (fund::kCount, begin_day 设置)

  void set_day_fundamental(const float *row) { fund_row_ = row; }

  // ===========================================================================
  // 节点 (由 NODES 表生成)
  // ===========================================================================
#define DAG_DECLARE_NODE(name, type, args, ct, ft) \
  graph::Node<DAG_UNPAREN type> name DAG_BRACE args;
  NODES(DAG_DECLARE_NODE)
#undef DAG_DECLARE_NODE

  // ===========================================================================
  // 标签 (回填别的时间行, 不在 NODES 表; 写回在 Tick_Sequential)
  // ===========================================================================
  // 组内顺序: [long_5w, long_20w, short_5w, short_20w] × {5m,10m,30m}, 分钟锚定落 L1 12 列
  LabelReturnOp LabelReturn{DepthData.bid_price, DepthData.ask_price, DepthData.bid_qty, DepthData.ask_qty};
  // L0 秒级: 1 分钟 × 5 万, 只落 long
  LabelReturn1mOp LabelReturn1m{DepthData.bid_price, DepthData.ask_price, DepthData.bid_qty, DepthData.ask_qty};

  explicit DAG(TickData &td, const std::string &code) : tick_data(td), asset_code_(code) {}

  // ===========================================================================
  // 触发域调度: 对表里 compute/flush 触发域 == trig 的节点, 按行序调 compute()/flush()
  // ===========================================================================
  template <Trigger trig>
  [[gnu::always_inline]] inline void run() {
#define DAG_RUN_NODE(name, type, args, ct, ft)   \
  if constexpr (Trigger::ct == trig) name.compute(); \
  if constexpr (Trigger::ft == trig) name.flush();
    NODES(DAG_RUN_NODE)
#undef DAG_RUN_NODE
  }

  // ===========================================================================
  // 盘前重置
  // ===========================================================================
  void at_day_start() {
#define DAG_RESET_NODE(name, type, args, ct, ft) name.reset();
    NODES(DAG_RESET_NODE)
#undef DAG_RESET_NODE
    LabelReturn.reset();
    LabelReturn1m.reset();
  }

  // ===========================================================================
  // 盘尾计算 (主要给标签类特征用)
  // ===========================================================================
  void at_day_end() {
    // 例如: LabelReturn 可能需要在此做最后的计算
  }
};
