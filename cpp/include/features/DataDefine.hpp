#pragma once

#include "define/CBuffer.hpp"
#include "lob/LimitOrderBookDefine.hpp" // IWYU pragma: export
#include <cstdint>

//========================================================================================
// HIERARCHICAL DATA DEFINITIONS
//========================================================================================
// Defines shared data structures across feature computation hierarchy:
// Tick (LOB_Feature) -> Minute (MinuteData) -> Hour (HourData)
//
// Design: Each level stores time-series data in CBuffers
// - No duplication: CBuffer serves as both storage and current value access
// - Resamplers write directly to CBuffers
// - Feature processors read from CBuffers (latest = back())
//========================================================================================

//----------------------------------------------------------------------------------------
// TICK LEVEL (L0): Raw order book data
//----------------------------------------------------------------------------------------
// Re-export LOB_Feature as part of the data hierarchy
struct TickData {
  // Metadata
  uint32_t asset_id{0};  // static: asset identifier
  uint32_t core_id{0};   // static: core identifier
  uint32_t timestamp{0}; // dynamic: current tick timestamp (trading second index 0-?)

  LOB_Feature lob;
};

//----------------------------------------------------------------------------------------
// MINUTE LEVEL (L1): Resampled from tick data
//----------------------------------------------------------------------------------------
struct MinuteData {
  // Metadata
  uint32_t asset_id{0};  // static: asset identifier
  uint32_t core_id{0};   // static: core identifier
  uint32_t timestamp{0}; // dynamic: current minute timestamp (trading minute index 0-239)

  // Time-series: OHLC (240 minutes in a trading day)
  CBuffer<double, 240> open;
  CBuffer<double, 240> high;
  CBuffer<double, 240> low;
  CBuffer<double, 240> close;

  // Time-series: Volume and amount by side
  CBuffer<uint32_t, 240> bid_volume;
  CBuffer<uint32_t, 240> ask_volume;
  CBuffer<double, 240> bid_amount;
  CBuffer<double, 240> ask_amount;
};

//----------------------------------------------------------------------------------------
// HOUR LEVEL (L2): Resampled from minute data
//----------------------------------------------------------------------------------------
struct HourData {
  // Metadata
  uint32_t asset_id{0};  // static: asset identifier
  uint32_t core_id{0};   // static: core identifier
  uint32_t timestamp{0}; // current hour timestamp (trading hour index 0-3)

  // Time-series: OHLC (4 hours in a trading day)
  CBuffer<double, 4> open;
  CBuffer<double, 4> high;
  CBuffer<double, 4> low;
  CBuffer<double, 4> close;

  // Time-series: Volume and amount by side
  CBuffer<uint32_t, 4> bid_volume;
  CBuffer<uint32_t, 4> ask_volume;
  CBuffer<double, 4> bid_amount;
  CBuffer<double, 4> ask_amount;
};
