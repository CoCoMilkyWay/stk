#pragma once

#include "define/CBuffer.hpp"
#include "lob/LimitOrderBookDefine.hpp" // IWYU pragma: export
#include <array>
#include <cstdint>
#include <limits>

//========================================================================================
// HIERARCHICAL DATA DEFINITIONS
//========================================================================================
// Defines shared data structures across feature computation hierarchy:
// Tick (LOB_Feature) -> Minute (MinuteData) -> n-Minute bins + whole-day bin (BinData, n ∈ BIN_MINUTES)
//
// Design:
// - Source levels hold only the last bar (plain fields): resamplers assign it, nodes read it in that level's beat
// - Node outputs hold the latest value per port (Port); history is opt-in (producer kHist → History, or a consumer-owned window)
//========================================================================================

//----------------------------------------------------------------------------------------
// TRIGGER: 节点 compute()/flush() 的采样域. 每个节点在节点文件末尾的 NODE_ 宏里
// 声明 (compute 触发, flush 触发), LobGraph 按 CMake 汇总的 NODES 表展开成直线调度代码.
//----------------------------------------------------------------------------------------
enum class Trigger : uint8_t {
  onDay,    // 盘前 (begin_day, 节点 reset / carry 之后)
  onTaker,  // 成交单
  onMaker,  // 挂单
  onCancel, // 撤单
  onTick,   // 每笔订单 (增/删/改/成交)
  onDepth,  // 盘口更新 (depth_updated)
  onMinute, // 分钟边界 (L_1min 每分钟一次, 含无成交分钟: 降频型节点对空分钟按自身约定归零 / 延续, 不产 NaN)
  on5Min,   // n 分钟箱末 (L_<n>min 每箱一次, 箱末分钟 onMinute 之后, 细箱先于粗箱; 含无成交箱, 同 onMinute 约定)
  on15Min,
  on30Min,
  on60Min,
  onClose,  // 收盘 (L_1day 一日一次, 末分钟 on60Min 之后; 整日箱, 含无成交日, 同 onMinute 约定)
  onSettle, // 盘后 (onClose 之后, L_1day 行写之前): 日频 PIT 推进到次日盘前 (Fund); 本域节点读到的是本日收盘全部节点值
};
// onMinute .. onClose = 层 L_1min .. L_1day 的收行节拍 (同序连续, 见 FeaturesDefine.hpp beat_of)
// 日频节点 = flush 域 onClose / onSettle: 输出口跨天保留 (不 reset), 盘中下游读到的 = 前一交易日 L_1day 行的值
inline constexpr bool is_day_trigger(Trigger t) { return t == Trigger::onClose || t == Trigger::onSettle; }

// 缺失值约定 (全部节点 / 标签 / Fund 同一口径): "无定义" (空盘口 / 零分母 / 无标签 / PIT 缺失) 一律 NaN,
// 不用 0 —— 0 是合法取值 (零收益 / 完全均衡), 消费端无法区分. 【fast-math 契约】只生产 NaN, 不 isnan.
inline constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();

//----------------------------------------------------------------------------------------
// 节点间数据流的统一类型: 一个输出口 = Port (该口最近一次 flush 的值, 尚无输出 NaN); K 个口 = Ports<K>;
// 一侧盘口 N 个价位 = DepthPorts (Depth 的 bid_qty 等, 当前簿). 下游按 const 引用接, 读到的永远是最新值.
// 回看 = History<K, H>: 最近 H 次发布的 Ports<K> 行环 (back() = 当前输出口, [size() − 1 − j][口] = 前 j 次), 上游声明 kHist 才有
//----------------------------------------------------------------------------------------
using Port = float;
template <size_t K>
using Ports = std::array<Port, K>;
using DepthPorts = Ports<L2::LOB_DEPTH>;
template <size_t K, size_t H>
using History = CBuffer<Ports<K>, H>;

// 价格比较容差 (元): 价位价 / 委托价 / 成交价由整数分换算成 float, 与解析的涨跌停价可能差 1 ulp.
// 涨跌停判定 (Depth 钳价位 / Flow 彩票委托 / Valuation 触板标记) 统一用它, 不各写一个.
inline constexpr float kPxEps = 1e-4f;

//----------------------------------------------------------------------------------------
// NODE CONTRACT (节点统一接口)
//----------------------------------------------------------------------------------------
//   enum Out : size_t { <名>..., kCount };   // 输出口; 单输出用 { value, kCount }; 无输出 (源层) kCount = 0
//   float y[kCount] = {};                    // 输出值: compute()/flush() 写 y[口], Node 在 flush 域发布到输出口
//   static constexpr size_t kHist = H;       // (可选) 下游要回看本节点最近 H 次发布: Node 另持 History<kCount, H>, 下游接 Up.hist(); 不声明零开销
//   <NodeType>(<输入引用>...);               // 只接输入 (const Port & / const Ports<K> & / const History<K, H> & / const DepthPorts & / 数据结构引用), 不接输出
// 输入红线: 只准接本资产的数据 (Port/History/DepthPorts/TickData/MinuteData/BinData) 与日频 PIT (fund::Panel).
// 跨资产 / 全局状态 / CS 结果一律禁止 —— TS 必须保持资产局部纯函数, 回测重放可任意重排
// (串行/并行/分片) 且与实盘推流逐值一致, 全系于此; 截面信息的表达一律走 CS 层与因子层.
//   void compute();                          // 采样型: 直接写 y
//   void flush();   (可选)                   // 降频型: compute 累计状态, flush 结算到 y (Node 随后发布)
//   void reset();   (可选)                   // 有跨天状态才写 (日内节点; 日频节点不 reset)
//   bool carry();   (可选, 日频节点)         // 盘前承接: 输出口不是当日盘前态 (区间首日 / 日期轴跳日) → 写 y 并返回 true, Node 发布
//   template <class Ar> void snap(Ar &ar);   // (可选) 续算点: 逐个 ar(成员) 列出 reset 不清的成员 (跨天窗口 / 昨值 / 分布)
//                                            //   (misc/snap.hpp); y / 输出口 / hist 由 Node 存. 漏列 = 续算与全量不再逐位一致
//                                            //   能由 fund::Panel 从头重推的 (Fund 的状态机) 不必列; 状态只许依赖库首日, 不许依赖本轮首日
// 单侧节点 (只看一侧盘口) 只收那一侧的 DepthPorts, 用模板参数 IS_BID 决定符号; 不要两侧都收再挑一侧.
// 输出口由 graph::Node 持有, 默认只存最新值; 下游用 Up.out(口) 接 Port, 字段表用 TS(节点, 口) 引用. 回看两条路:
//   同域 (消费方每次发布恰读一次上游): 消费方自持窗口 (如 Momentum), 窗长归消费方;
//   跨域 (要上游按它自己的 flush 节奏产出的序列, 消费方采样不到): 上游声明 kHist, 下游接 Up.hist().
// 实例接线 (NODE_) + 落盘列 (FIELDS_) 写在节点文件末尾, CMake 扫描汇总 (格式见 FeaturesDefine.hpp).
//----------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------
// CS COLUMN CONTRACT (截面列 = Node/<Tier>/CS/<分类>/<源节点>.hpp 只有 FIELDS_ 行, 无节点; 方法 = Method/CS.hpp, namespace cs)
//----------------------------------------------------------------------------------------
//   struct <Method> {
//     static constexpr bool kNeutral;                       // 是否需要中性化上下文 (NeutralRank::Ctx, L_1min / 分钟箱层 / L_1day; L_sec 不可)
//     static void apply(float *y, size_t n[, const Ctx &]); // dense 列原地: 输入 = 有效资产子集的源列 (缺失 NaN), 输出 = 结果
//   };
//   struct <Tf> { static void apply(float *y, size_t n); }; // 元素预变换, 先于 <Method>
// 无状态 (静态函数), 无实例; 一行字段 CS(src_lvl, src, Tf, Method) = gather → Tf::apply → Method::apply → scatter,
// CoreCs 按字段表编译期展开 (与 TS 的 NODES 展开同构). 无效资产输出 0 由基建写.
// 方法依赖 NaN 语义 (isfinite): 实现在 src/features/Method/CS.cpp (precise-math TU), 头文件只声明.
// 截面列文件按"算的是什么特征"命名 (= 源节点名), 不按方法命名; 一文件 = 该节点输出列的全部截面列.
// 同一套 struct 另有运行期入口 cs::column_fn(TfId, MethodId) (Transform 分析用): 探索里验证的变换 == 落盘截面列.
//----------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------
// TICK LEVEL (L_sec): Raw order book data
//----------------------------------------------------------------------------------------
// Re-export LOB_Feature as part of the data hierarchy
struct TickData {
  // Metadata
  uint32_t asset_id{0};  // static: asset identifier
  uint32_t core_id{0};   // static: core identifier
  uint32_t sec_index{0}; // dynamic: current tick index (trading second index 0-?)

  LOB_Feature lob;
};

// 事件时刻: 交易时段毫秒 (sec_index × 1000 + ms; 分钟边界 = 60000 整数倍, 午休不计时). 时间加权节点 (Book / Realized) 共用.
// 14:57-15:00 全部映射到哨兵秒 15299, ms 每秒回绕 → t 可倒退; 消费方对 uint32 差必须钳零 (t > t_prev ? t - t_prev : 0).
inline uint32_t tick_ms(const TickData &td) { return td.sec_index * 1000u + td.lob.millisecond * 10u; }

// LOB_Feature::ord_* 解码 (字段语义见 LimitOrderBookDefine.hpp; OrderLife / OrderQuad / Moment / Flow 共用, 节点不各写一份):
//   ord_reliable(lob, s)         该侧是可信的在簿委托: Resting 且非占位单 → ord_orig / ord_tick / ord_price 可信
//   ord_filled_before(lob, s, f) 该侧委托截止本笔之前的累计成交量 f (股); false = 该侧不可信 (无 id / 占位单 / 过度抵扣)
inline bool ord_reliable(const LOB_Feature &lob, size_t s) {
  return lob.ord_role[s] == OrderRole::Resting && lob.ord_flag[s] != OrderFlags::OUT_OF_ORDER && lob.ord_flag[s] != OrderFlags::ZERO_PRICE;
}
inline bool ord_filled_before(const LOB_Feature &lob, size_t s, uint32_t &f) {
  switch (lob.ord_role[s]) {
  case OrderRole::Aggressor:
    f = lob.ord_rest[s];
    return true;
  case OrderRole::Resting:
    if (!ord_reliable(lob, s) || lob.ord_rest[s] > lob.ord_orig[s])
      return false;
    f = lob.ord_orig[s] - lob.ord_rest[s];
    return true;
  default:
    return false;
  }
}

//----------------------------------------------------------------------------------------
// BAR LEVELS (L_1min / L_5min .. L_60min / L_1day): 每层一根末 bar, 同一结构, 只差量额宽度
//   MinuteData  L_1min: 档的重采样器填充 (Lob ResamplerTick2Min 逐笔 / Bar ResamplerBar2Min bar1m)
//   BinData     分钟箱层 + L_1day (整日箱): ResamplerMin2Bin 由 MinuteData 重采样 (箱见 TimeIndex.hpp);
//               量额宽累计 (整日成交量可超 uint32)
//   本层节拍域内末 bar = 刚收的这一行; 日末归零 (价 0 = 尚无成交价)
//----------------------------------------------------------------------------------------
template <class Vol, class Amt>
struct BarData {
  // Metadata
  uint32_t asset_id{0}; // static: asset identifier
  uint32_t core_id{0};  // static: core identifier
  uint32_t index{0};    // dynamic: 末 bar 的本层行 (L_1min 0-254 / 箱 0 .. bins_per_day(n) - 1 / L_1day 0)

  // 末 bar: OHLC + 分侧量额
  float open{0}, high{0}, low{0}, close{0};
  Vol bid_volume{0}, ask_volume{0};
  Amt bid_amount{0}, ask_amount{0};

  bool traded() const { return close > 0 && bid_volume + ask_volume > 0; } // 本 bar 有成交

  void clear() { *this = BarData{asset_id, core_id}; }
};
using MinuteData = BarData<uint32_t, float>;
using BinData = BarData<uint64_t, double>;
