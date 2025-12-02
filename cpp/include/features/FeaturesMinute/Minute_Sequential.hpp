#pragma once

#include "features/DataDefine.hpp"
#include "features/backend/FeatureStore.hpp"
#include <algorithm>
#include <cmath>
#include <deque>
#include <cstdint>

// Minute-level sequential feature computation
// Input: MinuteData (time-series minute-level data)
class Minute_Sequential {
public:
  Minute_Sequential(const MinuteData &minute_data,
                    GlobalFeatureStore &store,
                    size_t asset_id,
                    size_t worker_id)
      : minute_data_(minute_data),
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
    if (minute_data_.close.empty())
      return;

    // Read latest bar from CBuffers
    const double close = minute_data_.close.back();
    const uint32_t bid_vol = minute_data_.bid_volume.back();
    const uint32_t ask_vol = minute_data_.ask_volume.back();
    const uint32_t total_volume = bid_vol + ask_vol;

    bool is_valid = close > 0 && total_volume > 0;
    size_t t = minute_data_.timestamp;

    // Compute and write minute-level TS features
    compute_ts_minute(is_valid, t);
  }

private:
  // Level 1: Minute-level TS features computation
  void compute_ts_minute(bool is_valid, size_t t) {
    // Allocate feature array (only TS features)
    float features[5];

    if (!is_valid) {
      std::memset(features, 0, sizeof(features));
    } else {
      features[0] = compute_min_ret_z();
      features[1] = compute_rv_5m_norm();
      features[2] = compute_vwap_gap_pct();
      features[3] = compute_momentum_15m();
      features[4] = compute_range_squeeze();
    }

    // Write TS features
    constexpr size_t level_idx = 1;
    TS_WRITE_FEATURES(store_, date_str_, level_idx, t, asset_id_, L1_TS_RANGE.start, L1_TS_RANGE.end, features, worker_id_);

    // Write asset validity flag
    TS_WRITE_SINGLE(store_, date_str_, level_idx, t, L1_FieldOffset::universe_size, asset_id_, is_valid ? 1.0f : 0.0f, worker_id_);
  }
  // Feature 1: min_ret_z - Minute return z-score (rolling 60m)
  float compute_min_ret_z() const {
    const double close = minute_data_.close.back();
    double ret = 0;
    
    if (!minute_return_window_.empty() && minute_return_window_.back() > 0) {
      ret = std::log(close / minute_return_window_.back());
    }
    
    minute_return_window_.push_back(close);
    if (minute_return_window_.size() > 60)
      minute_return_window_.pop_front();
    
    if (minute_return_window_.size() < 10)
      return static_cast<float>(ret);
    
    // Compute rolling z-score
    double sum = 0, sq_sum = 0;
    size_t n = minute_return_window_.size();
    for (size_t i = 1; i < n; ++i) {
      if (minute_return_window_[i-1] > 0) {
        double r = std::log(minute_return_window_[i] / minute_return_window_[i-1]);
        sum += r;
        sq_sum += r * r;
      }
    }
    
    double mean = sum / (n - 1);
    double variance = sq_sum / (n - 1) - mean * mean;
    double stddev = std::sqrt(std::max(variance, 1e-10));
    
    // Winsorize to [-3, 3]
    double z = (ret - mean) / stddev;
    return static_cast<float>(std::clamp(z, -3.0, 3.0));
  }

  // Feature 2: rv_5m_norm - 5-minute realized volatility (log normalized)
  float compute_rv_5m_norm() const {
    const double close = minute_data_.close.back();
    
    rv_window_.push_back(close);
    if (rv_window_.size() > 5)
      rv_window_.pop_front();
    
    if (rv_window_.size() < 2)
      return 0.0f;
    
    // Compute realized volatility
    double sum_sq = 0;
    for (size_t i = 1; i < rv_window_.size(); ++i) {
      if (rv_window_[i-1] > 0) {
        double r = std::log(rv_window_[i] / rv_window_[i-1]);
        sum_sq += r * r;
      }
    }
    double rv = std::sqrt(sum_sq / (rv_window_.size() - 1));
    
    // Log transformation
    return static_cast<float>(std::log(rv + 1e-10));
  }

  // Feature 3: vwap_gap_pct - VWAP gap percentage (rolling z-score)
  float compute_vwap_gap_pct() const {
    const double close = minute_data_.close.back();
    const double bid_vol = minute_data_.bid_volume.back();
    const double ask_vol = minute_data_.ask_volume.back();
    const double bid_amt = minute_data_.bid_amount.back();
    const double ask_amt = minute_data_.ask_amount.back();
    const double total_vol = bid_vol + ask_vol;
    const double vwap = (total_vol > 0) ? ((bid_amt + ask_amt) / total_vol) : close;
    
    if (vwap <= 0)
      return 0.0f;
    
    double gap = (close - vwap) / vwap;
    
    vwap_window_.push_back(gap);
    if (vwap_window_.size() > 30)
      vwap_window_.pop_front();
    
    if (vwap_window_.size() < 10)
      return static_cast<float>(gap);
    
    // Rolling z-score
    double sum = 0, sq_sum = 0;
    for (double g : vwap_window_) {
      sum += g;
      sq_sum += g * g;
    }
    double mean = sum / vwap_window_.size();
    double variance = sq_sum / vwap_window_.size() - mean * mean;
    double stddev = std::sqrt(std::max(variance, 1e-10));
    
    return static_cast<float>((gap - mean) / stddev);
  }

  // Feature 4: momentum_15m - 15-minute momentum (cumulative z-score)
  float compute_momentum_15m() const {
    const double close = minute_data_.close.back();
    
    if (!momentum_window_.empty() && momentum_window_.back() > 0) {
      double ret = std::log(close / momentum_window_.back());
      momentum_returns_.push_back(ret);
    }
    
    momentum_window_.push_back(close);
    if (momentum_window_.size() > 15) {
      momentum_window_.pop_front();
      if (!momentum_returns_.empty())
        momentum_returns_.pop_front();
    }
    
    if (momentum_returns_.empty())
      return 0.0f;
    
    // Cumulative return
    double cum_ret = 0;
    for (double r : momentum_returns_)
      cum_ret += r;
    
    // Rolling stddev
    double sum = 0, sq_sum = 0;
    for (double r : momentum_returns_) {
      sum += r;
      sq_sum += r * r;
    }
    double variance = sq_sum / momentum_returns_.size() - std::pow(sum / momentum_returns_.size(), 2);
    double stddev = std::sqrt(std::max(variance, 1e-10));
    
    return static_cast<float>(cum_ret / stddev);
  }

  // Feature 5: range_squeeze - Range squeeze indicator
  float compute_range_squeeze() const {
    const double high = minute_data_.high.back();
    const double low = minute_data_.low.back();
    const double close = minute_data_.close.back();
    
    double range = high - low;
    
    range_window_.push_back({range, close});
    if (range_window_.size() > 30)
      range_window_.pop_front();
    
    if (range_window_.size() < 10)
      return 0.0f;
    
    // Compute rolling volatility (30m)
    double sum_sq = 0;
    for (size_t i = 1; i < range_window_.size(); ++i) {
      if (range_window_[i-1].second > 0) {
        double r = std::log(range_window_[i].second / range_window_[i-1].second);
        sum_sq += r * r;
      }
    }
    double vol = std::sqrt(sum_sq / (range_window_.size() - 1));
    
    // Range / volatility ratio (clipped)
    double ratio = range / (vol * close + 1e-10);
    return static_cast<float>(std::clamp(ratio, -3.0, 3.0));
  }

  const MinuteData &minute_data_;
  GlobalFeatureStore *store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;

  // Rolling windows for TS features
  mutable std::deque<double> minute_return_window_;
  mutable std::deque<double> rv_window_;
  mutable std::deque<double> vwap_window_;
  mutable std::deque<double> momentum_window_;
  mutable std::deque<double> momentum_returns_;
  mutable std::deque<std::pair<double, double>> range_window_; // {range, close}
};

