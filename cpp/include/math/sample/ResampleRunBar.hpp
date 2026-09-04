#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <vector>

#include "codec/L2_DataType.hpp"

//========================================================================================
// VOLUME-IMBALANCE RUN BAR SAMPLER
//========================================================================================
// Resamples tick-by-tick trades into volume-balanced bars using run-length encoding
// Key features:
// - Accumulates buy/sell volumes separately until threshold reached
// - Adapts threshold dynamically using EMA of daily statistics
// - Enforces minimum time spacing between samples (time guard)
// - Labels bars by dominant side (buy/sell) of final trade
//========================================================================================

class ResampleRunBar {
public:
  explicit ResampleRunBar() {
    daily_labels_.reserve(expected_samples_per_day_);
    daily_volumes_.reserve(expected_samples_per_day_);
  }

  // Main sampling interface - returns true when new bar formed
  // Parameters: packed timestamp (from LimitOrderBook::curr_tick_), trade direction, volume
  [[gnu::hot, gnu::always_inline]] inline bool resample(uint32_t timestamp, bool is_bid, uint32_t volume) {
    // Update cumulative volumes and last trade direction
    accumulate_volume(is_bid, volume);

    // Check if bar should be emitted
    if (!should_emit_bar(timestamp))
      return false;

    // Bar formation confirmed - emit and reset
    emit_bar(timestamp, is_bid, volume);
    return true;
  }

private:
  //======================================================================================
  // CONFIGURATION
  //======================================================================================
  const int target_bar_period_ = L2::RESAMPLE_TARGET_PERIOD;                                             // Target period (seconds)
  const int expected_samples_per_day_ = int(3600 * L2::RESAMPLE_TRADE_HRS_PER_DAY / target_bar_period_); // Expected bars per day
  const int threshold_tolerance_ = static_cast<int>(expected_samples_per_day_ * 0.05);                   // ±5% tolerance

  const float ema_alpha_ = 2.0f / (L2::RESAMPLE_EMA_DAYS_PERIOD + 1); // EMA smoothing factor

  //======================================================================================
  // STATE: Volume Accumulators
  //======================================================================================
  uint32_t accum_buy_ = 0;  // Buy-side volume accumulator
  uint32_t accum_sell_ = 0; // Sell-side volume accumulator

  //======================================================================================
  // STATE: Threshold Tracking
  //======================================================================================
  float threshold_ema_ = L2::RESAMPLE_INIT_VOLUME_THD; // EMA of daily thresholds (adaptive)
  float threshold_daily_ = 0.0f;                       // Yesterday's optimal threshold

  //======================================================================================
  // STATE: Timing Control
  //======================================================================================
  uint32_t last_emit_timestamp_ = 0; // Timestamp of last bar emission (for time guard)
  uint8_t last_hour_ = 255;          // Last observed hour (for new day detection)

  //======================================================================================
  // STATE: Daily Statistics
  //======================================================================================
  std::vector<bool> daily_labels_;      // Trade direction history (true=buy, false=sell)
  std::vector<uint32_t> daily_volumes_; // Trade volume history
  uint32_t daily_bar_count_ = 0;        // Number of bars formed today

  //======================================================================================
  // CORE LOGIC: Volume Accumulation
  //======================================================================================
  [[gnu::hot, gnu::always_inline]] inline void accumulate_volume(bool is_bid, uint32_t volume) {
    if (is_bid)
      accum_buy_ += volume;
    else
      accum_sell_ += volume;
  }

  //======================================================================================
  // CORE LOGIC: Bar Emission Check
  //======================================================================================
  [[gnu::hot, gnu::always_inline]] inline bool should_emit_bar(uint32_t timestamp) const {
    // Check 1: Volume threshold reached?
    const uint32_t max_side = std::max(accum_buy_, accum_sell_);
    const float threshold = std::max(threshold_ema_, 0.0f);
    if (max_side < threshold)
      return false;

    // Check 2: Time guard (minimum spacing between bars)
    const uint32_t time_diff_seconds = (timestamp >> 8) - (last_emit_timestamp_ >> 8);
    if (time_diff_seconds < L2::RESAMPLE_MIN_PERIOD) [[unlikely]]
      return false;

    return true;
  }

  //======================================================================================
  // CORE LOGIC: Bar Emission and State Reset
  //======================================================================================
  inline void emit_bar(uint32_t timestamp, bool is_bid, uint32_t volume) {
    // Reset volume accumulators
    accum_buy_ = 0;
    accum_sell_ = 0;

    // Update timing state
    last_emit_timestamp_ = timestamp;
    ++daily_bar_count_;

    // New day detection and threshold update
    const uint8_t hour = (timestamp >> 24) & 0xFF;
    if (hour == 9 && last_hour_ != 9) [[unlikely]] {
      on_new_day();
    }
    last_hour_ = hour;

    // Record trade for daily statistics
    daily_labels_.push_back(is_bid);
    daily_volumes_.push_back(volume);
  }

  //======================================================================================
  // NEW DAY PROCESSING
  //======================================================================================
  inline void on_new_day() {
    daily_bar_count_ = 1;

    // Update threshold using yesterday's data
    if (!daily_labels_.empty()) [[likely]] {
      threshold_daily_ = compute_optimal_threshold();
      threshold_ema_ = (threshold_ema_ < 0.0f)
                           ? threshold_daily_
                           : ema_alpha_ * threshold_daily_ + (1.0f - ema_alpha_) * threshold_ema_;
    }

    // Clear daily statistics
    daily_labels_.clear();
    daily_volumes_.clear();
  }

  //======================================================================================
  // THRESHOLD OPTIMIZATION: Binary Search
  //======================================================================================
  inline float compute_optimal_threshold() {
    if (daily_labels_.empty()) [[unlikely]]
      return 0.0f;

    // Binary search bounds
    float threshold_min = *std::min_element(daily_volumes_.begin(), daily_volumes_.end());
    float threshold_max = std::accumulate(daily_volumes_.begin(), daily_volumes_.end(), 0.0f);

    // Binary search for threshold that yields target sample count
    constexpr int max_iterations = 20;
    for (int iter = 0; iter < max_iterations; ++iter) {
      const float threshold_mid = 0.5f * (threshold_min + threshold_max);
      const int sample_count = simulate_sample_count(threshold_mid);

      // Convergence check
      if (std::abs(sample_count - expected_samples_per_day_) <= threshold_tolerance_ ||
          (threshold_max - threshold_min) < 100.0f) {
        return threshold_mid;
      }

      // Binary search update
      if (sample_count > expected_samples_per_day_)
        threshold_min = threshold_mid;
      else
        threshold_max = threshold_mid;
    }

    return 0.5f * (threshold_min + threshold_max);
  }

  //======================================================================================
  // THRESHOLD OPTIMIZATION: Simulation
  //======================================================================================
  inline int simulate_sample_count(float threshold) const {
    float accum_buy = 0.0f;
    float accum_sell = 0.0f;
    int bar_count = 0;

    for (size_t i = 0; i < daily_volumes_.size(); ++i) {
      // Accumulate volume by side
      if (daily_labels_[i])
        accum_buy += daily_volumes_[i];
      else
        accum_sell += daily_volumes_[i];

      // Check bar formation
      if (accum_buy >= threshold || accum_sell >= threshold) {
        ++bar_count;
        accum_buy = 0.0f;
        accum_sell = 0.0f;
      }
    }

    return bar_count;
  }
};
