#pragma once

#include "features/backend/FeatureStore.hpp"
#include <algorithm>
#include <cmath>
#include <deque>
#include <cstdint>

// Resampled hour-level bar data
struct HourBar {
  uint64_t timestamp_1h;    // hour timestamp
  uint32_t instrument_id;   // asset identifier
  double open_1h;           // open price
  double high_1h;           // high price
  double low_1h;            // low price
  double close_1h;          // close price
  double vwap_1h;           // volume-weighted average price
  uint64_t volume_1h;       // total volume
  uint32_t universe_ids_1h; // universe membership flags
  bool market_close_1h;     // market close flag
  double prev_day_close;    // previous day close
};

// Hour-level sequential feature computation
// Input: HourBar (resampled OHLCV data)
class Hour_Sequential {
public:
  explicit Hour_Sequential(const HourBar &hour_bar)
      : hour_bar_(hour_bar) {
  }

  void set_store_context(GlobalFeatureStore &store, size_t asset_id, size_t worker_id = 0) {
    store_ = &store;
    asset_id_ = asset_id;
    worker_id_ = worker_id;
  }

  void set_date(const std::string &date_str) {
    date_str_ = date_str;
  }

  // Main computation entry (called by CoreSequential)
  void compute_and_store() {
    if (!store_ || date_str_.empty())
      return;

    const HourBar &bar = hour_bar_;
    bool is_valid = bar.close_1h > 0 && bar.volume_1h > 0;
    size_t t = bar.timestamp_1h;

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

    // Write asset validity flag
    TS_WRITE_SINGLE(store_, date_str_, level_idx, t, L2_FieldOffset::universe_size, asset_id_, is_valid ? 1.0f : 0.0f, worker_id_);
  }
  // Feature 1: hour_ret_12h_mom - 12-hour momentum z-score
  float compute_hour_ret_12h_mom() const {
    const auto &bar = hour_bar_;
    
    hour_return_window_.push_back(bar.close_1h);
    if (hour_return_window_.size() > 48)
      hour_return_window_.pop_front();
    
    if (hour_return_window_.size() < 13)
      return 0.0f;
    
    // Compute 12-hour cumulative return
    double cum_ret = 0;
    size_t start_idx = hour_return_window_.size() > 12 ? hour_return_window_.size() - 12 : 0;
    for (size_t i = start_idx + 1; i < hour_return_window_.size(); ++i) {
      if (hour_return_window_[i-1] > 0) {
        cum_ret += std::log(hour_return_window_[i] / hour_return_window_[i-1]);
      }
    }
    
    // Z-score with 48-hour rolling window
    double sum = 0, sq_sum = 0;
    for (size_t i = 1; i < hour_return_window_.size(); ++i) {
      if (hour_return_window_[i-1] > 0) {
        double r = std::log(hour_return_window_[i] / hour_return_window_[i-1]);
        sum += r;
        sq_sum += r * r;
      }
    }
    
    double mean = sum / (hour_return_window_.size() - 1);
    double variance = sq_sum / (hour_return_window_.size() - 1) - mean * mean;
    double stddev = std::sqrt(std::max(variance, 1e-10));
    
    return static_cast<float>(cum_ret / stddev);
  }

  // Feature 2: hour_volatility - 24-hour realized volatility (log normalized)
  float compute_hour_volatility() const {
    const auto &bar = hour_bar_;
    
    hour_vol_window_.push_back(bar.close_1h);
    if (hour_vol_window_.size() > 24)
      hour_vol_window_.pop_front();
    
    if (hour_vol_window_.size() < 2)
      return 0.0f;
    
    // Compute 24-hour realized volatility
    double sum_sq = 0;
    for (size_t i = 1; i < hour_vol_window_.size(); ++i) {
      if (hour_vol_window_[i-1] > 0) {
        double r = std::log(hour_vol_window_[i] / hour_vol_window_[i-1]);
        sum_sq += r * r;
      }
    }
    double rv = std::sqrt(sum_sq / (hour_vol_window_.size() - 1));
    
    // Log transformation
    return static_cast<float>(std::log(rv + 1e-10));
  }

  // Feature 3: pivot_dev - Pivot point deviation
  float compute_pivot_dev() const {
    const auto &bar = hour_bar_;
    
    // Pivot point: (high + low + close) / 3
    double pivot = (bar.high_1h + bar.low_1h + bar.close_1h) / 3.0;
    double range = bar.high_1h - bar.low_1h;
    
    if (range < 1e-10)
      return 0.0f;
    
    double dev = (bar.close_1h - pivot) / range;
    
    // Clip to [-3, 3]
    return static_cast<float>(std::clamp(dev, -3.0, 3.0));
  }

  // Feature 4: dominant_persist - Dominant side persistence (EMA of buy/sell pressure)
  float compute_dominant_persist() const {
    const auto &bar = hour_bar_;
    
    // Compute dominant side: volume-weighted buy/sell indicator
    // Positive volume = buying pressure, negative = selling pressure
    // For simplicity, use VWAP vs close as proxy
    double dominant = (bar.close_1h > bar.vwap_1h) ? 1.0 : -1.0;
    
    dominant_window_.push_back(dominant);
    if (dominant_window_.size() > 20)
      dominant_window_.pop_front();
    
    if (dominant_window_.size() < 5)
      return static_cast<float>(dominant);
    
    // EMA with α ≈ 0.1
    constexpr double alpha = 0.1;
    double ema = dominant_window_[0];
    for (size_t i = 1; i < dominant_window_.size(); ++i)
      ema = alpha * dominant_window_[i] + (1 - alpha) * ema;
    
    // Z-score normalization
    double sum = 0, sq_sum = 0;
    for (double d : dominant_window_) {
      sum += d;
      sq_sum += d * d;
    }
    double mean = sum / dominant_window_.size();
    double variance = sq_sum / dominant_window_.size() - mean * mean;
    double stddev = std::sqrt(std::max(variance, 1e-10));
    
    return static_cast<float>((ema - mean) / stddev);
  }

  // Feature 5: hour_overnight_gap - Overnight gap (if applicable)
  float compute_hour_overnight_gap() const {
    const auto &bar = hour_bar_;
    
    // Only compute gap at market open
    if (bar.prev_day_close <= 0)
      return 0.0f;
    
    double gap = bar.open_1h - bar.prev_day_close;
    
    // Compute intraday volatility from historical data
    if (hour_vol_window_.size() < 5)
      return static_cast<float>(gap / bar.prev_day_close);
    
    double sum_sq = 0;
    for (size_t i = 1; i < hour_vol_window_.size(); ++i) {
      if (hour_vol_window_[i-1] > 0) {
        double r = std::log(hour_vol_window_[i] / hour_vol_window_[i-1]);
        sum_sq += r * r;
      }
    }
    double intraday_vol = std::sqrt(sum_sq / (hour_vol_window_.size() - 1));
    
    if (intraday_vol < 1e-10)
      return 0.0f;
    
    // Winsorize gap
    double normalized_gap = gap / (bar.prev_day_close * intraday_vol);
    return static_cast<float>(std::clamp(normalized_gap, -3.0, 3.0));
  }

  const HourBar &hour_bar_;
  GlobalFeatureStore *store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;

  // Rolling windows for TS features
  mutable std::deque<double> hour_return_window_;
  mutable std::deque<double> hour_vol_window_;
  mutable std::deque<double> pivot_window_;
  mutable std::deque<double> dominant_window_;
};

