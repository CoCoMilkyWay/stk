#pragma once

#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp" // NODE_COMPUTE / NODE_FLUSH (TIER_DAG)
#include "misc/date.hpp"
#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <string>

namespace fund {
class Panel;
}

// ============================================================================
// Node: 节点类实例 + 它的输出口. 输出口 = 节点类的 y[] 本体, 无中转缓冲: 两档构图端共用
// (LobGraph.hpp / BarGraph.hpp; 一个 TU 只许 include 其一, 见 NodesGenerated.hpp).
//   契约依据 (DataDefine.hpp): 采样型节点 compute 直接写 y (compute = publish);
//   降频型节点 compute 只累计私有状态, flush 才写 y (flush = publish) ——
//   下游与字段表在两次发布之间读到的 y = 最近一次发布的值, 尚无输出时 NaN (缺失值约定).
//   node.compute()                   节点类的 compute()
//   node.flush()                     节点类有 flush() 才调 (降频型结算到 y)
//   node.reset()                     跨天 (日内节点): y 置 NaN (不留昨日尾值), 节点类有 reset() 再调
//   node.carry()                     跨天 (日频节点, 见 is_day_trigger): y 保留; 节点类有 carry() 再调 (盘前承接: y 不是当日盘前态时改写)
//   node.snap<kDay>(ar)              续算点 (misc/snap.hpp): 日频节点的 y (carry 保留) + 节点类有 snap(ar) 再调
//   node.out(i) / node.out()         第 i 个输出口 (下游节点输入 / 字段表写回); 单口默认 0
//   node.<port>                      节点类 enum Out 的枚举值 (node.out(node.price))
// ============================================================================
namespace graph {

template <class Impl, size_t K = Impl::kCount>
struct Node : Impl {
  using Impl::Impl;

  const Port &out(size_t i = 0) const {
    assert(i < K);
    return Impl::y[i];
  }

  [[gnu::always_inline]] inline void flush() {
    if constexpr (requires(Impl &o) { o.flush(); })
      Impl::flush();
  }
  void reset() {
    std::fill_n(Impl::y, K, kNaN);
    if constexpr (requires(Impl &o) { o.reset(); })
      Impl::reset();
  }
  void carry() {
    if constexpr (requires(Impl &o) { o.carry(); })
      Impl::carry();
  }
  template <bool kDay, class Ar>
  void snap(Ar &ar) {
    if constexpr (kDay)
      ar(Impl::y); // 日频节点的输出口跨天承接 → 存; 日内节点的 y 盘前置 NaN, 不存
    if constexpr (requires(Impl &o) { o.snap(ar); })
      Impl::snap(ar);
  }
};

// 源层节点 (kCount = 0): 无标量输出口, 自持输出 (如 Depth 的 N 价位当前簿, 由节点自己的 reset 置 NaN); flush/reset 有才调
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

// 节点用时采样 (特征面板 Node Time): 时序 worker 对抽中的 asset-day 把本线程的钟挂到 node_clock, run<>() 逐节点累计 TSC 周期
// (下标 = DAG 节点下标 = node_deps::TABLE 行), CoreTs 的标签 / 写张量经 clocked 记进 label / write, 整个 asset-day 记进 total
// (total − 其余 = 源与调度: 解码 / 订单簿 / 重采样 / 日句柄 ...); 不挂 = 原路径, 每处只多一个分支. 只计时, 不碰特征值
struct NodeClock {
  static constexpr size_t kMaxNodes = 64;
  std::array<uint64_t, kMaxNodes> cycles{};
  uint64_t label = 0;   // 标签 (日历 / 逐分钟结算 / 收盘结算)
  uint64_t write = 0;   // 写张量 (TS 行 + 元列)
  uint64_t total = 0;   // 整个 asset-day (begin_day → release)
  uint64_t samples = 0; // 计过的 asset-day 数
};
inline thread_local NodeClock *node_clock = nullptr;

// 非节点段: 本线程挂了节点钟则 f 的 TSC 周期计入 slot (段内不得再进 run<>, 否则重计)
template <class F>
[[gnu::always_inline]] inline void clocked(uint64_t NodeClock::*slot, F &&f) {
  NodeClock *clk = node_clock;
  if (!clk) [[likely]] {
    f();
    return;
  }
  const uint64_t t = __builtin_ia32_rdtsc();
  f();
  clk->*slot += __builtin_ia32_rdtsc() - t;
}

// 两档共有的源: 分钟 K + 分钟箱 K + 日 K + 日频 PIT + 本资产身份. 档的 DAG_Root 继承它, 只加档特有的源 (Lob: TickData)
struct Root {
  MinuteData minute_data;                                          // L_1min 输入 (档的重采样器填充: Lob 逐笔 / Bar bar1m)
  BinData min5_data, min15_data, min30_data, min60_data, day_data; // 分钟箱层 + L_1day 输入 (CoreTs 由 minute_data 重采样, 两档同一路径)
  const fund::Panel &fund_panel;                                   // 日频输入 (外部传入, 只读共享; Fund 节点按 global_id_/date_ 取用)
  size_t global_id_;                                               // 全局 A 轴下标 (fund_panel 的行; 与池无关)
  misc::Ymd date_ = 0;                                             // 当日 (at_day_start 设置)
  Root(const fund::Panel &panel, size_t global_id) : fund_panel(panel), global_id_(global_id) {}
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
//     run<trig>()        对表里 compute/flush 触发域 == trig 的节点, 按行序调 compute()/flush(); 本线程挂了 graph::node_clock 则逐节点计时
//     kNode_<name>       节点下标 (NODES 行序 = node_deps::TABLE 行序); kNodes = node_deps::TABLE (名 / 文件)
//     at_day_start(date) 设日期 → 日内节点 reset / 日频节点 carry (按行序) → 标签 reset
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
#define DAG_RUN_NODE_TIMED(name, type, args, ...)                                       \
  if constexpr (NODE_COMPUTE(__VA_ARGS__) == trig || NODE_FLUSH(__VA_ARGS__) == trig) { \
    DAG_RUN_NODE(name, type, args, __VA_ARGS__)                                         \
    const uint64_t u = __builtin_ia32_rdtsc();                                          \
    clk.cycles[kNode_##name] += u - t;                                                  \
    t = u;                                                                              \
  }
#define DAG_NODE_ID(name, type, args, ...) kNode_##name,
#define DAG_RESET_NODE(name, type, args, ...) graph::day_start<NODE_FLUSH(__VA_ARGS__)>(name);
#define DAG_SNAP_NODE(name, type, args, ...) graph::snap<NODE_FLUSH(__VA_ARGS__)>(ar, name);
#define TIER_DAG(LabelType, ...)                                                                 \
  DAG_CAT(DAG_TIER, _NODES)(DAG_DECLARE_NODE) class DAG : public DAG_CAT(DAG_TIER, _DAG_LAST) {  \
  public:                                                                                        \
    LabelType label{__VA_ARGS__};                                                                \
    enum NodeId : size_t { DAG_CAT(DAG_TIER, _NODES)(DAG_NODE_ID) kNodeCount };                  \
    static constexpr const auto &kNodes = node_deps::TABLE;                                      \
    static_assert(std::size(kNodes) == kNodeCount && kNodeCount <= graph::NodeClock::kMaxNodes); \
    template <class... Args>                                                                     \
    explicit DAG(Args &&...args) : DAG_CAT(DAG_TIER, _DAG_LAST)(std::forward<Args>(args)...) {}  \
    template <Trigger trig>                                                                      \
    [[gnu::always_inline]] inline void run() {                                                   \
      if (graph::NodeClock *clk = graph::node_clock) [[unlikely]]                                \
        run_timed<trig>(*clk);                                                                   \
      else {                                                                                     \
        DAG_CAT(DAG_TIER, _NODES)(DAG_RUN_NODE)                                                  \
      }                                                                                          \
    }                                                                                            \
    template <Trigger trig>                                                                      \
    [[gnu::noinline]] void run_timed(graph::NodeClock &clk) {                                    \
      uint64_t t = __builtin_ia32_rdtsc();                                                       \
      DAG_CAT(DAG_TIER, _NODES)(DAG_RUN_NODE_TIMED)(void)t;                                      \
    }                                                                                            \
    void at_day_start(misc::Ymd date) {                                                          \
      date_ = date;                                                                              \
      DAG_CAT(DAG_TIER, _NODES)(DAG_RESET_NODE)                                                  \
          label.reset();                                                                         \
    }                                                                                            \
    template <class Ar>                                                                          \
    void snap(Ar &ar) {                                                                          \
      DAG_CAT(DAG_TIER, _NODES)(DAG_SNAP_NODE)                                                   \
          label.snap(ar);                                                                        \
    }                                                                                            \
  };
