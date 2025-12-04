#pragma once

#include "features/DataDefine.hpp"
#include "features/backend/FeatureStore.hpp"
#include <algorithm>
#include <cmath>
#include <deque>
#include <cstdint>

// Hour-level sequential feature computation
// Input: HourData (time-series hour-level data)
class Hour_Sequential {
public:
  Hour_Sequential(const HourData &hour_data,
                  GlobalFeatureStore &store,
                  size_t asset_id,
                  size_t worker_id)
      : hour_data_(hour_data),
        store_(&store),
        asset_id_(asset_id),
        worker_id_(worker_id) {}

  void set_date(const std::string &date_str) {
    date_str_ = date_str;
  }

  // Main computation entry (called by CoreSequential)
  void compute_and_store() {
    if (!store_ || date_str_.empty())
      return;

    // Check if we have data
    if (hour_data_.close.empty())
      return;

    // Read latest bar from CBuffers
    const float close = hour_data_.close.back();
    const uint32_t bid_vol = hour_data_.bid_volume.back();
    const uint32_t ask_vol = hour_data_.ask_volume.back();
    const uint32_t total_volume = bid_vol + ask_vol;

    bool is_valid = close > 0 && total_volume > 0;
    size_t t = hour_data_.timestamp;

    // Compute and write hour-level TS features
    compute_ts_hour(is_valid, t);
  }

private:
  // Level 2: Hour-level TS features computation (leaf node)
  void compute_ts_hour(bool is_valid, size_t t) {
    // Allocate feature array (only TS features)
    float features[5];

    if (!is_valid) {
      std::memset(features, 0, sizeof(features));
    } else {
      features[0] = compute_hour_ret_12h_mom();
      features[1] = compute_hour_volatility();
      features[2] = compute_pivot_dev();
      features[3] = compute_dominant_persist();
      features[4] = compute_hour_overnight_gap();
    }

    // Write TS features
    constexpr size_t level_idx = 2;
    TS_WRITE_FEATURES(store_, date_str_, level_idx, t, asset_id_, L2_TS_RANGE.start, L2_TS_RANGE.end, features, worker_id_);

    // Write data validity flag (event-driven sparsity marker)
    TS_WRITE_SINGLE(store_, date_str_, level_idx, t, L2_FieldOffset::_data_valid, asset_id_, is_valid ? 1.0f : 0.0f, worker_id_);
  }
  // Feature 1: hour_ret_12h_mom - 12-hour momentum z-score
  float compute_hour_ret_12h_mom() const {
    const float close = hour_data_.close.back();
    
    hour_return_window_.push_back(close);
    if (hour_return_window_.size() > 48)
      hour_return_window_.pop_front();
    
    if (hour_return_window_.size() < 13)
      return 0.0f;
    
    // Compute 12-hour cumulative return
    float cum_ret = 0;
    size_t start_idx = hour_return_window_.size() > 12 ? hour_return_window_.size() - 12 : 0;
    for (size_t i = start_idx + 1; i < hour_return_window_.size(); ++i) {
      if (hour_return_window_[i-1] > 0) {
        cum_ret += std::log(hour_return_window_[i] / hour_return_window_[i-1]);
      }
    }
    
    // Z-score with 48-hour rolling window
    float sum = 0, sq_sum = 0;
    for (size_t i = 1; i < hour_return_window_.size(); ++i) {
      if (hour_return_window_[i-1] > 0) {
        float r = std::log(hour_return_window_[i] / hour_return_window_[i-1]);
        sum += r;
        sq_sum += r * r;
      }
    }
    
    float mean = sum / (hour_return_window_.size() - 1);
    float variance = sq_sum / (hour_return_window_.size() - 1) - mean * mean;
    float stddev = std::sqrt(std::max(variance, 1e-10f));
    
    return static_cast<float>(cum_ret / stddev);
  }

  // Feature 2: hour_volatility - 24-hour realized volatility (log normalized)
  float compute_hour_volatility() const {
    const float close = hour_data_.close.back();
    
    hour_vol_window_.push_back(close);
    if (hour_vol_window_.size() > 24)
      hour_vol_window_.pop_front();
    
    if (hour_vol_window_.size() < 2)
      return 0.0f;
    
    // Compute 24-hour realized volatility
    float sum_sq = 0;
    for (size_t i = 1; i < hour_vol_window_.size(); ++i) {
      if (hour_vol_window_[i-1] > 0) {
        float r = std::log(hour_vol_window_[i] / hour_vol_window_[i-1]);
        sum_sq += r * r;
      }
    }
    float rv = std::sqrt(sum_sq / (hour_vol_window_.size() - 1));
    
    // Log transformation
    return static_cast<float>(std::log(rv + 1e-10));
  }

  // Feature 3: pivot_dev - Pivot point deviation
  float compute_pivot_dev() const {
    const float high = hour_data_.high.back();
    const float low = hour_data_.low.back();
    const float close = hour_data_.close.back();
    
    // Pivot point: (high + low + close) / 3
    float pivot = (high + low + close) / 3.0;
    float range = high - low;
    
    if (range < 1e-10)
      return 0.0f;
    
    float dev = (close - pivot) / range;
    
    // Clip to [-3, 3]
    return static_cast<float>(std::clamp(dev, -3.0f, 3.0f));
  }

  // Feature 4: dominant_persist - Dominant side persistence (EMA of buy/sell pressure)
  float compute_dominant_persist() const {
    const float close = hour_data_.close.back();
    const float bid_vol = hour_data_.bid_volume.back();
    const float ask_vol = hour_data_.ask_volume.back();
    const float bid_amt = hour_data_.bid_amount.back();
    const float ask_amt = hour_data_.ask_amount.back();
    const float total_vol = bid_vol + ask_vol;
    const float vwap = (total_vol > 0) ? ((bid_amt + ask_amt) / total_vol) : close;
    
    // Compute dominant side: volume-weighted buy/sell indicator
    // Positive volume = buying pressure, negative = selling pressure
    // For simplicity, use VWAP vs close as proxy
    float dominant = (close > vwap) ? 1.0 : -1.0;
    
    dominant_window_.push_back(dominant);
    if (dominant_window_.size() > 20)
      dominant_window_.pop_front();
    
    if (dominant_window_.size() < 5)
      return static_cast<float>(dominant);
    
    // EMA with α ≈ 0.1
    constexpr float alpha = 0.1;
    float ema = dominant_window_[0];
    for (size_t i = 1; i < dominant_window_.size(); ++i)
      ema = alpha * dominant_window_[i] + (1 - alpha) * ema;
    
    // Z-score normalization
    float sum = 0, sq_sum = 0;
    for (float d : dominant_window_) {
      sum += d;
      sq_sum += d * d;
    }
    float mean = sum / dominant_window_.size();
    float variance = sq_sum / dominant_window_.size() - mean * mean;
    float stddev = std::sqrt(std::max(variance, 1e-10f));
    
    return static_cast<float>((ema - mean) / stddev);
  }

  // Feature 5: hour_overnight_gap - Overnight gap (if applicable)
  float compute_hour_overnight_gap() const {
    const float open = hour_data_.open.back();
    
    // Use first value in return window as reference
    if (hour_return_window_.empty() || hour_return_window_.front() <= 0)
      return 0.0f;
    
    const float reference_close = hour_return_window_.front();
    float gap = open - reference_close;
    
    // Compute intraday volatility from historical data
    if (hour_vol_window_.size() < 5)
      return static_cast<float>(gap / reference_close);
    
    float sum_sq = 0;
    for (size_t i = 1; i < hour_vol_window_.size(); ++i) {
      if (hour_vol_window_[i-1] > 0) {
        float r = std::log(hour_vol_window_[i] / hour_vol_window_[i-1]);
        sum_sq += r * r;
      }
    }
    float intraday_vol = std::sqrt(sum_sq / (hour_vol_window_.size() - 1));
    
    if (intraday_vol < 1e-10)
      return 0.0f;
    
    // Winsorize gap
    float normalized_gap = gap / (reference_close * intraday_vol);
    return static_cast<float>(std::clamp(normalized_gap, -3.0f, 3.0f));
  }

  const HourData &hour_data_;
  GlobalFeatureStore *store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;

  // Rolling windows for TS features
  mutable std::deque<float> hour_return_window_;
  mutable std::deque<float> hour_vol_window_;
  mutable std::deque<float> pivot_window_;
  mutable std::deque<float> dominant_window_;
};

