#pragma once

#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp" // NODE_COMPUTE / NODE_FLUSH (TIER_DAG)
#include "misc/date.hpp"
#include <algorithm>
#include <cassert>
#include <iterator>
#include <string>
#include <type_traits>

namespace fund {
class Panel;
}

// ============================================================================
// Node: 节点类实例 + 它的输出口 Ports<K>. 节点类只写 y[口], 发布到输出口由 Node 统一做. 两档构图端共用
// (LobGraph.hpp / BarGraph.hpp; 一个 TU 只许 include 其一, 见 NodesGenerated.hpp).
//   输出口 = 最近一次 flush (或 carry) 时 y 的快照, 尚无输出时 NaN (缺失值约定, 见 DataDefine.hpp); 与 y 分开:
//   compute 与 flush 异域的节点 (逐笔累计 / 分钟结算) 在两次 flush 之间改 y, 下游与字段表只看已发布的值
//   node.compute()                   节点类的 compute()
//   node.flush()                     节点类有 flush() 先调 (降频型结算到 y), 然后 y → 输出口
//   node.reset()                     跨天 (日内节点): 输出口置 NaN (不留昨日尾值), 节点类有 reset() 再调
//   node.carry()                     跨天 (日频节点, 见 is_day_trigger): 输出口保留; 节点类有 carry() 且返回 true → y → 输出口 (盘前承接)
//   node.snap<kDay>(ar)              续算点 (misc/snap.hpp): y (reset 不清) + 日频节点的输出口 / hist (carry 保留) + 节点类有 snap(ar) 再调
//   node.out(i) / node.out()         第 i 个输出口 (下游节点输入 / 字段表写回); 单口默认 0
//   node.outs()                      全部输出口 Ports<K> (下游按口成组消费)
//   node.hist()                      节点类声明 kHist = H 才有: 最近 H 次发布的 History<K, H> (每次发布推一行, 与输出口同步;
//                                    reset 清空, carry 保留 —— 与输出口同一跨天约定)
//   node.<port>                      节点类 enum Out 的枚举值 (node.out(node.price))
// ============================================================================
namespace graph {

template <class Impl>
inline constexpr size_t hist_len = [] {
  if constexpr (requires { Impl::kHist; })
    return size_t{Impl::kHist};
  else
    return size_t{0};
}();

struct NoHist {};

template <class Impl, size_t K = Impl::kCount>
struct Node : Impl {
  static constexpr size_t H = hist_len<Impl>;
  using Hist = std::conditional_t<(H > 0), History<K, (H > 0 ? H : 1)>, NoHist>;

  using Impl::Impl;

  const Port &out(size_t i = 0) const {
    assert(i < K);
    return ports_[i];
  }
  const Ports<K> &outs() const { return ports_; }
  const Hist &hist() const
    requires(H > 0)
  {
    return hist_;
  }

  [[gnu::always_inline]] inline void flush() {
    if constexpr (requires(Impl &o) { o.flush(); })
      Impl::flush();
    publish();
  }
  void reset() {
    ports_.fill(kNaN);
    if constexpr (H > 0)
      hist_.clear();
    if constexpr (requires(Impl &o) { o.reset(); })
      Impl::reset();
  }
  void carry() {
    if constexpr (requires(Impl &o) { o.carry(); })
      if (Impl::carry())
        publish();
  }
  template <bool kDay, class Ar>
  void snap(Ar &ar) {
    ar(Impl::y);
    if constexpr (kDay) {
      ar(ports_);
      if constexpr (H > 0)
        ar(hist_);
    }
    if constexpr (requires(Impl &o) { o.snap(ar); })
      Impl::snap(ar);
  }

private:
  [[gnu::always_inline]] inline void publish() {
    std::copy_n(Impl::y, K, ports_.begin());
    if constexpr (H > 0)
      hist_.push_back(ports_);
  }

  Ports<K> ports_ = [] {
    Ports<K> p;
    p.fill(kNaN);
    return p;
  }();
  [[no_unique_address]] Hist hist_;
};

// 源层节点 (kCount = 0): 无标量输出口, 自持输出 (如 Depth 的 N 价位当前簿, 由节点自己的 reset 置 NaN); flush/reset 有才调
template <class Impl>
struct Node<Impl, 0> : Impl {
  static_assert(hist_len<Impl> == 0, "源层节点自持输出, 不走 kHist");
  using Impl::Impl;
  void flush() {
    if constexpr (requires(Impl &o) { o.flush(); })
      Impl::flush();
  }
  void reset() {
    if constexpr (requires(Impl &o) { o.reset(); })
      Impl::reset();
  }
  template <bool kDay, class Ar>
  void snap(Ar &ar) {
    if constexpr (requires(Impl &o) { o.snap(ar); })
      Impl::snap(ar);
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

// 续算点: 同一分派 (日频节点连输出口一起存, 与 day_start 承接 / 清空对仗)
template <Trigger Flush, class Ar, class N>
inline void snap(Ar &ar, N &node) {
  node.template snap<is_day_trigger(Flush)>(ar);
}

// 两档共有的源: 分钟 K + 分钟箱 K + 日 K + 日频 PIT + 本资产身份. 档的 DAG_Root 继承它, 只加档特有的源 (Lob: TickData)
struct Root {
  MinuteData minute_data;                                          // L_1min 输入 (档的重采样器填充: Lob 逐笔 / Bar bar1m)
  BinData min5_data, min15_data, min30_data, min60_data, day_data; // 分钟箱层 + L_1day 输入 (CoreTs 由 minute_data 重采样, 两档同一路径)
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
//     snap(ar)           续算点: 各节点 graph::snap (按行序) → 标签 snap; 与 at_day_start 同表同序
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
#define DAG_SNAP_NODE(name, type, args, ...) graph::snap<NODE_FLUSH(__VA_ARGS__)>(ar, name);
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
    template <class Ar>                                                                         \
    void snap(Ar &ar) {                                                                         \
      DAG_CAT(DAG_TIER, _NODES)(DAG_SNAP_NODE)                                                  \
          label.snap(ar);                                                                       \
    }                                                                                           \
  };
