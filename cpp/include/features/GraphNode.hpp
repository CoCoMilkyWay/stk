#pragma once

#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp" // NODE_COMPUTE / NODE_FLUSH (TIER_DAG)
#include "misc/date.hpp"
#include <cassert>
#include <iterator>
#include <string>

namespace fund {
class Panel;
}

// ============================================================================
// Node: 节点类实例 + 它的输出 Series. 节点类只写 y[口], 推入缓冲由 Node 统一做. 两档构图端共用
// (LobGraph.hpp / BarGraph.hpp; 一个 TU 只许 include 其一, 见 NodesGenerated.hpp).
//   node.compute()                   节点类的 compute()
//   node.flush()                     节点类有 flush() 先调 (降频型结算到 y), 然后 y[i] → bufs_[i]
//   node.reset()                     跨天 (日内节点): 清空全部输出 Series (不留昨日尾值), 节点类有 reset() 再调
//   node.carry()                     跨天 (日频节点, 见 is_day_trigger): Series 保留; 节点类有 carry() 且返回 true → y 推入一格 (盘前承接)
//   node.out(i) / node.out()         第 i 个输出口 (下游节点输入用); 单口默认 0
//   node.outs()                      全部输出口数组 Series[K] (下游按口成组消费)
//   node.last(i)                     输出口最新值, 尚无输出时 NaN (缺失值约定, 见 DataDefine.hpp; 字段表写回用)
//   node.<port>                      节点类 enum Out 的枚举值 (node.out(node.price))
// ============================================================================
namespace graph {

template <class Impl, size_t K = Impl::kCount>
struct Node : Impl {
  using Impl::Impl;
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
    return bufs_[i].empty() ? kNaN : bufs_[i].back();
  }

  [[gnu::always_inline]] inline void flush() {
    if constexpr (requires(Impl &o) { o.flush(); })
      Impl::flush();
    for (size_t i = 0; i < K; ++i)
      bufs_[i].push_back(Impl::y[i]);
  }
  void reset() {
    for (auto &b : bufs_)
      b.clear();
    if constexpr (requires(Impl &o) { o.reset(); })
      Impl::reset();
  }
  void carry() {
    if constexpr (requires(Impl &o) { o.carry(); })
      if (Impl::carry())
        for (size_t i = 0; i < K; ++i)
          bufs_[i].push_back(Impl::y[i]);
  }
};

// 源层节点 (kCount = 0): 无标量输出口, 自持输出 (如 Depth 的 N 价位数组, 由节点自己的 reset 清空); flush/reset 有才调
template <class Impl>
struct Node<Impl, 0> : Impl {
  using Impl::Impl;
  void flush() {
    if constexpr (requires(Impl &o) { o.flush(); })
      Impl::flush();
  }
  void reset() {
    if constexpr (requires(Impl &o) { o.reset(); })
      Impl::reset();
  }
};

// 跨天: 按节点 flush 域分派 (日频节点承接, 日内节点清空)
template <Trigger Flush, class N>
inline void day_start(N &node) {
  if constexpr (is_day_trigger(Flush))
    node.carry();
  else
    node.reset();
}

// 两档共有的源: 分钟 K + 分钟箱 K + 日 K + 日频 PIT + 本资产身份. 档的 DAG_Root 继承它, 只加档特有的源 (Lob: TickData)
struct Root {
  MinuteData minute_data;                                          // L_1min 输入 (档的重采样器填充: Lob 逐笔 / Bar bar1m)
  BinData min5_data, min15_data, min30_data, min60_data, day_data; // 分钟箱层 + L_1day 输入 (CoreTs 由 minute_data 重采样, 两档同一路径); day_data 跨天保留
  const fund::Panel &fund_panel;                                   // 日频输入 (外部传入, 只读共享; Fund 节点按 asset_id_/date_ 取用)
  std::string asset_code_;                                         // "CODE.EX"
  size_t asset_id_;                                                // 档 A 轴下标
  misc::Ymd date_ = 0;                                             // 当日 (at_day_start 设置)
  Root(const fund::Panel &panel, const std::string &code, size_t asset_id)
      : fund_panel(panel), asset_code_(code), asset_id_(asset_id) {}
};
// 层 L_5min + i 的输入 (分钟箱层 + L_1day 整日箱)
inline constexpr BinData Root::*BIN_DATA[] = {&Root::min5_data, &Root::min15_data, &Root::min30_data, &Root::min60_data, &Root::day_data};
static_assert(L_5min + std::size(BIN_DATA) == L_1day + 1);

} // namespace graph

#define DAG_UNPAREN(...) __VA_ARGS__
#define DAG_BRACE(...) {__VA_ARGS__}

// ============================================================================
// 档 DAG 公共体 (两档同一份). 档图头只写档特有的: 源 (DAG_Root : graph::Root, 档特有输入) 与标签类型 / 初始化,
// 然后 #define DAG_TIER <Tier> → TIER_DAG(LabelType, label 初始化实参...) → #undef DAG_TIER. 展开为:
//   声明链  struct DAG_<name> : <Tier>_NODE_PREV_<name> { graph::Node<NodeType> name{inputs...}; };
//           节点按 NodesGenerated.hpp 的拓扑序每个一层, 一层里只看得到更早的层 —— 输入引用了未排在前面的节点
//           是编译错误, 构造序 (基类先) 与 run<>() 执行序 (NODES 行序) 因此一定一致. 空基类无运行时开销.
//   class DAG  声明链末端 + 标签成员 label (回填别的时间行, 不在节点表; 调用在 CoreTs) + 触发域调度:
//     run<trig>()        对表里 compute/flush 触发域 == trig 的节点, 按行序调 compute()/flush()
//     at_day_start(date) 设日期 → 日内节点 reset / 日频节点 carry (按行序) → 标签 reset → onDay 域
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
#define DAG_RESET_NODE(name, type, args, ...) graph::day_start<NODE_FLUSH(__VA_ARGS__)>(name);
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
