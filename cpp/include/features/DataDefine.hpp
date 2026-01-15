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
  uint32_t l1_index{0}; // dynamic: current minute index (trading minute index 0-239)

  // Time-series: OHLC (240 minutes in a trading day)
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

