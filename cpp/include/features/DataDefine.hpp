#pragma once

#include "define/CBuffer.hpp"
#include "lob/LimitOrderBookDefine.hpp" // IWYU pragma: export
#include <cstdint>

//========================================================================================
// HIERARCHICAL DATA DEFINITIONS
//========================================================================================
// Defines shared data structures across feature computation hierarchy:
// Tick (LOB_Feature) -> Minute (MinuteData)
//
// Design: Each level stores time-series data in CBuffers
// - No duplication: CBuffer serves as both storage and current value access
// - Resamplers write directly to CBuffers
// - Feature processors read from CBuffers (latest = back())
//========================================================================================

//----------------------------------------------------------------------------------------
// TRIGGER: 算子 compute()/flush() 的采样域. 每个节点在算子文件末尾的 NODE_ 宏里
// 声明 (compute 触发, flush 触发), ComputeGraph 按 CMake 汇总的 NODES 表展开成直线调度代码.
//----------------------------------------------------------------------------------------
enum class Trigger : uint8_t {
  onTaker,  // 成交单
  onMaker,  // 挂单
  onCancel, // 撤单
  onTick,   // 每笔订单 (增/删/改/成交)
  onDepth,  // 盘口更新 (depth_updated)
  onMinute, // 有效分钟边界
};

//----------------------------------------------------------------------------------------
// 节点间数据流的统一类型: 一条时序 = Series; 一侧盘口 N 档 = DepthSeries (DepthData 的 bid_qty 等)
//----------------------------------------------------------------------------------------
using Series = CBuffer<float, L2::BLEN>;
using DepthSeries = Series[L2::LOB_DEPTH];

//----------------------------------------------------------------------------------------
// OPERATOR CONTRACT (算子统一接口)
//----------------------------------------------------------------------------------------
//   enum Out : size_t { <名>..., kCount };   // 输出口; 单输出用 { value, kCount }; 无输出 (源层) kCount = 0
//   float y[kCount] = {};                    // 输出值: compute()/flush() 写 y[口], Node 在 flush 域推入 CBuffer
//   Op(<输入引用>...);                       // 只接输入 (const Series & / const DepthSeries & / 数据结构引用), 不接输出
//   void compute();                          // 采样型: 直接写 y
//   void flush();   (可选)                   // 降频型: compute 累计状态, flush 结算到 y (Node 随后推入)
//   void reset();   (可选)                   // 有跨天状态才写
// 单侧算子 (只看一侧盘口) 只收那一侧的 DepthSeries, 用模板参数 IS_BID 决定符号; 不要两侧都收再挑一侧.
// 输出缓冲由 ComputeGraph::Node 持有; 下游用 Up.out(口) 拿 Series, 字段表用 OP(节点, 口) 引用.
// 实例接线 (NODE_) + 落盘列 (FIELDS_) 写在算子文件末尾, CMake 扫描汇总 (格式见 FeaturesDefine.hpp).
//----------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------
// CS OPERATOR CONTRACT (截面算子统一接口, Operator/CS/<Method>.hpp, namespace cs)
//----------------------------------------------------------------------------------------
//   struct <Method> {
//     static constexpr bool kNeutral;                       // 是否需要中性化上下文 (NeutralRank::Ctx, 仅 L1)
//     static void apply(float *y, size_t n[, const Ctx &]); // dense 列原地: 输入 = 有效资产子集的源列 (缺失 NaN), 输出 = 结果
//   };
//   struct <Tf> { static void apply(float *y, size_t n); }; // 元素预变换 (Transform.hpp), 先于 <Method>
// 无状态 (静态函数), 无实例; 一行字段 CS(src_lvl, src, Tf, Method) = gather → Tf::apply → Method::apply → scatter,
// CoreCrosssection 按字段表编译期展开 (与 TS 的 NODES 展开同构). 无效资产输出 0 由基建写.
// kernel 依赖 NaN 语义 (isfinite): 实现集中在 src/features/CSKernels.cpp (precise-math TU), 头文件只声明.
// 落盘列 (FIELDS_) 写在方法文件末尾, CMake 扫描汇总 (格式见 FeaturesDefine.hpp).
//----------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------
// TICK LEVEL (L0): Raw order book data
//----------------------------------------------------------------------------------------
// Re-export LOB_Feature as part of the data hierarchy
struct TickData {
  // Metadata
  uint32_t asset_id{0}; // static: asset identifier
  uint32_t core_id{0};  // static: core identifier
  uint32_t l0_index{0}; // dynamic: current tick index (trading second index 0-?)

  LOB_Feature lob;
};

//----------------------------------------------------------------------------------------
// MINUTE LEVEL (L1): Resampled from tick data
//----------------------------------------------------------------------------------------
struct MinuteData {
  // Metadata
  uint32_t asset_id{0}; // static: asset identifier
  uint32_t core_id{0};  // static: core identifier
  uint32_t l1_index{0}; // dynamic: current minute index (trading minute index 0-254)

  // Time-series: OHLC (环形窗口, 算子只读 back()/尾部窗口, 不按绝对分钟下标访问)
  CBuffer<float, 240> open;
  CBuffer<float, 240> high;
  CBuffer<float, 240> low;
  CBuffer<float, 240> close;

  // Time-series: Volume and amount by side
  CBuffer<uint32_t, 240> bid_volume;
  CBuffer<uint32_t, 240> ask_volume;
  CBuffer<float, 240> bid_amount;
  CBuffer<float, 240> ask_amount;

  void clear() {
    open.clear();
    high.clear();
    low.clear();
    close.clear();
    bid_volume.clear();
    ask_volume.clear();
    bid_amount.clear();
    ask_amount.clear();
  }
};
