#pragma once

#include <cassert>
#include <cstdint>

#include "features/DataDefine.hpp"
// #include "misc/logging.hpp"

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
    // Only process taker orders
    if (input_.lob.order_type != L2::OrderType::TAKER) {
      return;
    };

    const float price = input_.lob.price;
    const uint32_t volume = input_.lob.volume;
    const bool is_bid = input_.lob.order_dir == L2::OrderDirection::BID;

    const uint32_t current_time_seconds = input_.lob.hour * 3600 + input_.lob.minute * 60 + input_.lob.second;

    // Check if we need to emit previous bar and start new one
    const bool emit_bar = (last_bar_time_ != 0) && (current_time_seconds - last_bar_time_ >= bar_period_seconds_);

    if (emit_bar) {
      output_.open.push_back(bar_open_);
      output_.high.push_back(bar_high_);
      output_.low.push_back(bar_low_);
      output_.close.push_back(bar_close_);
      output_.bid_volume.push_back(bar_bid_volume_);
      output_.ask_volume.push_back(bar_ask_volume_);
      output_.bid_amount.push_back(bar_bid_amount_);
      output_.ask_amount.push_back(bar_ask_amount_);
      // Logger::log(std::to_string(output_.asset_id), "min_bar " + std::to_string(input_.lob.hour) + ":" + std::to_string(input_.lob.minute) + ":" + std::to_string(input_.lob.second) + " price: " + std::to_string(bar_close_) + " volume: " + std::to_string(bar_bid_volume_ + bar_ask_volume_) + " amount: " + std::to_string(bar_bid_amount_ + bar_ask_amount_) + " current_time_seconds: " + std::to_string(current_time_seconds));
    }

    // Start new bar (first tick or after emit)
    if (last_bar_time_ == 0 || emit_bar) {
      last_bar_time_ = current_time_seconds;
      bar_open_ = bar_high_ = bar_low_ = bar_close_ = price;
      bar_bid_volume_ = bar_ask_volume_ = 0;
      bar_bid_amount_ = bar_ask_amount_ = 0.0f;
    }

    // Update current bar with new tick
    bar_close_ = price;
    bar_high_ = (price > bar_high_) ? price : bar_high_;
    bar_low_ = (price < bar_low_) ? price : bar_low_;

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
