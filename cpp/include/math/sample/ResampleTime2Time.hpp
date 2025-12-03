#pragma once

#include <cassert>
#include <cstdint>

#include "features/DataDefine.hpp"
// #include "misc/logging.hpp"

//========================================================================================
// TIME-TO-TIME RESAMPLER
//========================================================================================
// Resamples time-based OHLC bars into coarser time-based bars (minute -> hour)
// Key features:
// - Input: MinuteData (fine granularity time-series)
// - Output: HourData (coarse granularity time-series)
// - update() only triggers resampling logic
//========================================================================================

class ResampleTime2Time {
public:
  // Constructor: bind I/O references
  ResampleTime2Time(const MinuteData &input, HourData &output, uint32_t bar_period_factor_)
      : input_(input), output_(output), bar_period_factor__(bar_period_factor_) {
    assert(input_.open.capacity() > bar_period_factor__);
  }

  // Trigger resampling logic
  void update() {
    const size_t input_size = input_.open.size();
    const size_t new_bars = input_size - last_processed_index_;

    // Early return if not enough bars to aggregate (common case during accumulation)
    if (new_bars < bar_period_factor__) [[likely]] {
      return;
    }

    const size_t start_idx = last_processed_index_;
    const size_t end_idx = start_idx + bar_period_factor__;

    assert(end_idx <= input_size);

    // Initialize aggregators with first bar
    float agg_open = input_.open[start_idx];
    float agg_high = input_.high[start_idx];
    float agg_low = input_.low[start_idx];
    uint32_t agg_bid_volume = input_.bid_volume[start_idx];
    uint32_t agg_ask_volume = input_.ask_volume[start_idx];
    float agg_bid_amount = input_.bid_amount[start_idx];
    float agg_ask_amount = input_.ask_amount[start_idx];

    // Aggregate remaining bars (start from start_idx+1)
    for (size_t i = start_idx + 1; i < end_idx; ++i) {
      const float high = input_.high[i];
      const float low = input_.low[i];

      // Branchless min/max (compiler will optimize)
      agg_high = (high > agg_high) ? high : agg_high;
      agg_low = (low < agg_low) ? low : agg_low;

      // Accumulate volume and amount
      agg_bid_volume += input_.bid_volume[i];
      agg_ask_volume += input_.ask_volume[i];
      agg_bid_amount += input_.bid_amount[i];
      agg_ask_amount += input_.ask_amount[i];
    }

    // Close price is always the last bar
    const float agg_close = input_.close[end_idx - 1];

    // Push aggregated bar to output buffers
    output_.open.push_back(agg_open);
    output_.high.push_back(agg_high);
    output_.low.push_back(agg_low);
    output_.close.push_back(agg_close);
    output_.bid_volume.push_back(agg_bid_volume);
    output_.ask_volume.push_back(agg_ask_volume);
    output_.bid_amount.push_back(agg_bid_amount);
    output_.ask_amount.push_back(agg_ask_amount);
    // Logger::log(std::to_string(output_.asset_id), "hour_bar idx:" + std::to_string(end_idx) + " price: " + std::to_string(agg_close) + " volume: " + std::to_string(agg_bid_volume + agg_ask_volume) + " amount: " + std::to_string(agg_bid_amount + agg_ask_amount));

    last_processed_index_ = end_idx;
  }

  // Reset resampler state (called when starting a new day)
  void reset() {
    last_processed_index_ = 0;
  }

private:
  // Input/Output references
  const MinuteData &input_;
  HourData &output_;

  // Configuration
  const uint32_t bar_period_factor__; // Number of fine bars to aggregate into one coarse bar

  // State: last processed index in input buffers
  size_t last_processed_index_{0};
};
