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
// OPERATOR CONTRACT (算子统一接口)
//----------------------------------------------------------------------------------------
//   enum Out : size_t { <名>..., kCount };   // 输出口; 单输出用 { value, kCount }; 无输出 (源层) kCount = 0
//   float y[kCount] = {};                    // 输出值: compute()/flush() 写 y[口], Node 在 flush 域推入 CBuffer
//   Op(<输入引用>...);                       // 只接输入, 不接输出
//   void compute();                          // 采样型: 直接写 y
//   void flush();   (可选)                   // 降频型: compute 累计状态, flush 结算到 y (Node 随后推入)
//   void reset();   (可选)                   // 有跨天状态才写
// 输出缓冲由 ComputeGraph::Node 持有; 下游用 Up.out(口) 拿 CBuffer, 字段表用 OP(节点, 口) 引用.
// 实例接线 (NODE_) + 落盘列 (FIELDS_) 写在算子文件末尾, CMake 扫描汇总 (格式见 FeaturesDefine.hpp).
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
};
