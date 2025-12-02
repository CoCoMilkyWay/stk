#pragma once

#include <cstdint>

#include "features/DataDefine.hpp"

//========================================================================================
// TICK-TO-TIME RESAMPLER
//========================================================================================
// Resamples tick-level TickData into time-based OHLC bars with volume/amount stats
// Key features:
// - Input: TickData (tick-by-tick order book events)
// - Output: MinuteData (directly writes to CBuffers)
// - update() only triggers resampling logic
//========================================================================================

class ResampleTick2Time {
public:
  // Constructor: bind I/O references
  ResampleTick2Time(const TickData &input, MinuteData &output, uint32_t bar_period_seconds)
      : input_(input), output_(output), bar_period_seconds_(bar_period_seconds) {}

  // Trigger resampling logic
  void update() {
    // Cache frequently accessed fields
    const auto price = input_.lob.price;
    const auto volume = input_.lob.volume;
    const auto is_bid = input_.lob.is_bid;

    const uint32_t current_time_seconds =
        input_.lob.hour * 3600 + input_.lob.minute * 60 + input_.lob.second;

    // Check if we crossed a bar boundary
    if (last_bar_time_ == 0) {
      // First tick: initialize bar
      last_bar_time_ = current_time_seconds;
      bar_open_ = price;
      bar_high_ = bar_open_;
      bar_low_ = bar_open_;
      bar_close_ = bar_open_;
      bar_bid_volume_ = 0;
      bar_ask_volume_ = 0;
      bar_bid_amount_ = 0.0;
      bar_ask_amount_ = 0.0;
    }

    const uint32_t time_diff = current_time_seconds - last_bar_time_;

    // Emit bar if time boundary crossed
    if (time_diff >= bar_period_seconds_) {
      // Push accumulated bar to output buffers
      output_.open.push_back(bar_open_);
      output_.high.push_back(bar_high_);
      output_.low.push_back(bar_low_);
      output_.close.push_back(bar_close_);
      output_.bid_volume.push_back(bar_bid_volume_);
      output_.ask_volume.push_back(bar_ask_volume_);
      output_.bid_amount.push_back(bar_bid_amount_);
      output_.ask_amount.push_back(bar_ask_amount_);

      // Reset bar accumulators
      last_bar_time_ = current_time_seconds;
      bar_open_ = price;
      bar_high_ = bar_open_;
      bar_low_ = bar_open_;
      bar_close_ = bar_open_;
      bar_bid_volume_ = 0;
      bar_ask_volume_ = 0;
      bar_bid_amount_ = 0.0;
      bar_ask_amount_ = 0.0;
    }

    // Update current bar with new tick
    bar_close_ = price;

    if (price > bar_high_)
      bar_high_ = price;
    if (price < bar_low_)
      bar_low_ = price;

    // Accumulate volume and amount by side
    const float amount = price * volume;
    if (is_bid) {
      bar_bid_volume_ += volume;
      bar_bid_amount_ += amount;
    } else {
      bar_ask_volume_ += volume;
      bar_ask_amount_ += amount;
    }
  }

private:
  // Input/Output references
  const TickData &input_;
  MinuteData &output_;

  // Configuration
  const uint32_t bar_period_seconds_;

  // State: current bar accumulator
  uint32_t last_bar_time_{0};
  float bar_open_{0};
  float bar_high_{0};
  float bar_low_{0};
  float bar_close_{0};
  uint32_t bar_bid_volume_{0};
  uint32_t bar_ask_volume_{0};
  float bar_bid_amount_{0};
  float bar_ask_amount_{0};
};
