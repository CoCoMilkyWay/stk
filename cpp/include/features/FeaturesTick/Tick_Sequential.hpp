#pragma once

#include "features/FeaturesDefine.hpp"
#include "features/backend/FeatureStore.hpp"
#include "lob/LimitOrderBookDefine.hpp"
#include <algorithm>
#include <cmath>
#include <deque>

// Tick-level sequential feature computation
// Input: LOB_Feature from LimitOrderBook
class Tick_Sequential {
public:
  explicit Tick_Sequential(const LOB_Feature *lob_feature,
                           GlobalFeatureStore *store = nullptr,
                           size_t asset_id = 0,
                           size_t worker_id = 0)
      : lob_feature_(lob_feature),
        feature_store_(store),
        asset_id_(asset_id),
        worker_id_(worker_id) {
  }

  void set_store_context(GlobalFeatureStore *store, size_t asset_id, size_t worker_id) {
    feature_store_ = store;
    asset_id_ = asset_id;
    worker_id_ = worker_id;
  }

  void set_date(const std::string &date_str) {
    date_str_ = date_str;
  }

  // Main computation entry (called by CoreSequential)
  void compute_and_store() {
    if (!feature_store_ || date_str_.empty())
      return;

    const LOB_Feature &lob = *lob_feature_;
    size_t t = time_to_trading_seconds(lob.hour, lob.minute, lob.second);

    // Check if this asset is active (has valid LOB data)
    bool is_valid = check_lob_valid(lob);

    // Compute and write tick-level TS features
    compute_ts_tick(is_valid, t);
  }

private:
  // Level 0: Tick-level TS features computation
  void compute_ts_tick(bool is_valid, size_t t) {
    // Allocate feature array (only TS features)
    float features[L0_TS_COUNT];

    if (!is_valid) {
      // Asset inactive: write zeros
      std::memset(features, 0, sizeof(features));
    } else {
      // Compute TS features
      features[0] = compute_tick_ret_z();
      features[1] = compute_tobi_osc();
      features[2] = compute_micro_gap_norm();
      features[3] = compute_spread_momentum();
      features[4] = compute_signed_volume_imb();
    }

    // Write TS features
    constexpr size_t level_idx = 0;
    TS_WRITE_FEATURES(feature_store_, date_str_, level_idx, t, asset_id_, L0_TS_START, L0_TS_END, features, worker_id_);

    // Write asset validity flag (business logic, not backend requirement)
    TS_WRITE_SINGLE(feature_store_, date_str_, level_idx, t, L0_FieldOffset::asset_valid, asset_id_, is_valid ? 1.0f : 0.0f, worker_id_);
  }

  // Check if LOB has valid data
  bool check_lob_valid(const LOB_Feature &lob) const {
    return lob.price > 0 && lob.depth_buffer.size() >= 2 * LOB_FEATURE_DEPTH_LEVELS;
  }

  // Get mid price from depth buffer
  double get_mid_price() const {
    const auto &depth = lob_feature_->depth_buffer;
    if (depth.size() < 2 * LOB_FEATURE_DEPTH_LEVELS)
      return lob_feature_->price * 0.01;
    
    Level *best_ask = depth[LOB_FEATURE_DEPTH_LEVELS - 1]; // sell1
    Level *best_bid = depth[LOB_FEATURE_DEPTH_LEVELS];     // buy1
    
    if (best_ask && best_bid)
      return (best_ask->price + best_bid->price) * 0.005; // 0.01/2
    
    return lob_feature_->price * 0.01;
  }

  // Get spread from depth buffer
  double get_spread() const {
    const auto &depth = lob_feature_->depth_buffer;
    if (depth.size() < 2 * LOB_FEATURE_DEPTH_LEVELS)
      return 0.0;
    
    Level *best_ask = depth[LOB_FEATURE_DEPTH_LEVELS - 1];
    Level *best_bid = depth[LOB_FEATURE_DEPTH_LEVELS];
    
    if (best_ask && best_bid)
      return (best_ask->price - best_bid->price) * 0.01;
    
    return 0.0;
  }

  // Get top-of-book imbalance (TOBI)
  double get_tobi() const {
    const auto &depth = lob_feature_->depth_buffer;
    if (depth.size() < 2 * LOB_FEATURE_DEPTH_LEVELS)
      return 0.0;
    
    Level *best_ask = depth[LOB_FEATURE_DEPTH_LEVELS - 1];
    Level *best_bid = depth[LOB_FEATURE_DEPTH_LEVELS];
    
    if (!best_ask || !best_bid)
      return 0.0;
    
    int32_t bid_qty = best_bid->net_quantity;
    int32_t ask_qty = -best_ask->net_quantity; // ask is negative
    
    if (bid_qty + ask_qty == 0)
      return 0.0;
    
    return static_cast<double>(bid_qty - ask_qty) / (bid_qty + ask_qty);
  }

  // Feature 1: tick_ret_z - Rolling z-score normalized tick return (optimized incremental)
  float compute_tick_ret_z() {
    double mid = get_mid_price();
    
    // Compute log return
    if (last_mid_ <= 0) {
      last_mid_ = mid;
      return 0.0f;
    }
    
    double ret = std::log(mid / last_mid_);
    last_mid_ = mid;
    
    // Update window
    ret_window_.push_back(ret);
    if (ret_window_.size() > 50)
      ret_window_.pop_front();
    
    if (ret_window_.size() < 10)
      return static_cast<float>(ret);
    
    // Incremental mean/variance (Welford's algorithm)
    size_t n = ret_window_.size();
    double sum = 0, sq_sum = 0;
    for (double r : ret_window_) {
      sum += r;
      sq_sum += r * r;
    }
    
    double mean = sum / n;
    double variance = sq_sum / n - mean * mean;
    double stddev = std::sqrt(std::max(variance, 1e-10));
    
    return static_cast<float>((ret - mean) / stddev);
  }

  // Feature 2: tobi_osc - Top-of-book imbalance oscillator
  float compute_tobi_osc() const {
    double tobi = get_tobi();
    
    // Update window
    tobi_window_.push_back(tobi);
    if (tobi_window_.size() > 50)
      tobi_window_.pop_front();
    
    if (tobi_window_.size() < 10)
      return static_cast<float>(tobi);
    
    // Compute median absolute deviation (MAD)
    std::vector<double> sorted_tobi(tobi_window_.begin(), tobi_window_.end());
    std::sort(sorted_tobi.begin(), sorted_tobi.end());
    double median = sorted_tobi[sorted_tobi.size() / 2];
    
    std::vector<double> abs_dev;
    abs_dev.reserve(tobi_window_.size());
    for (double val : tobi_window_)
      abs_dev.push_back(std::abs(val - median));
    std::sort(abs_dev.begin(), abs_dev.end());
    double mad = abs_dev[abs_dev.size() / 2];
    
    if (mad < 1e-8)
      return 0.0f;
    
    // Clip to [-3, 3]
    double normalized = (tobi - median) / mad;
    return static_cast<float>(std::clamp(normalized, -3.0, 3.0));
  }

  // Feature 3: micro_gap_norm - Micro price gap normalized (optimized incremental)
  float compute_micro_gap_norm() {
    double mid = get_mid_price();
    double micro = lob_feature_->price * 0.01; // last trade price
    
    // Update window and incremental stats
    if (mid_price_window_.size() >= 50) {
      double old_val = mid_price_window_.front();
      mid_sum_ -= old_val;
      mid_sq_sum_ -= old_val * old_val;
      mid_price_window_.pop_front();
    }
    
    mid_price_window_.push_back(mid);
    mid_sum_ += mid;
    mid_sq_sum_ += mid * mid;
    
    if (mid_price_window_.size() < 10)
      return 0.0f;
    
    // Use cached rolling statistics (O(1) instead of O(N))
    size_t n = mid_price_window_.size();
    double mean = mid_sum_ / n;
    double variance = mid_sq_sum_ / n - mean * mean;
    double stddev = std::sqrt(std::max(variance, 1e-10));
    
    // tanh((micro-mid)/σ)
    double gap = (micro - mid) / stddev;
    return static_cast<float>(std::tanh(gap));
  }

  // Feature 4: spread_momentum - Short-term spread change (optimized single-pass EMA)
  float compute_spread_momentum() {
    double spread = get_spread();
    
    // Update window
    spread_window_.push_back(spread);
    if (spread_window_.size() > 20)
      spread_window_.pop_front();
    
    if (spread_window_.size() < 2) {
      ema_spread_ = spread;
      return 0.0f;
    }
    
    // Incremental EMA update (O(1) instead of O(N))
    constexpr double alpha = 0.095;
    ema_spread_ = alpha * spread + (1 - alpha) * ema_spread_;
    
    return static_cast<float>(spread - ema_spread_);
  }

  // Feature 5: signed_volume_imb - Signed volume imbalance
  float compute_signed_volume_imb() const {
    // Update window with current tick
    const auto &lob = *lob_feature_;
    
    double sign = 0;
    if (lob.is_taker) {
      sign = lob.is_bid ? 1.0 : -1.0; // buy=+1, sell=-1
    }
    
    double signed_vol = sign * lob.volume;
    volume_imb_window_.push_back({signed_vol, static_cast<double>(lob.volume)});
    
    if (volume_imb_window_.size() > 20)
      volume_imb_window_.pop_front();
    
    // Σ(sign×size) / Σ|size|
    double sum_signed = 0, sum_abs = 0;
    for (const auto &[sv, v] : volume_imb_window_) {
      sum_signed += sv;
      sum_abs += v;
    }
    
    if (sum_abs < 1e-8)
      return 0.0f;
    
    return static_cast<float>(sum_signed / sum_abs);
  }

  const LOB_Feature *lob_feature_;
  GlobalFeatureStore *feature_store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;

  // Rolling windows for TS features (optimized)
  std::deque<double> ret_window_;        // Store returns directly (not prices)
  double last_mid_ = 0;                   // Last mid price for incremental return
  mutable std::deque<double> mid_price_window_;
  mutable std::deque<double> tobi_window_;
  double ema_spread_ = 0;                 // EMA state for spread momentum
  mutable std::deque<double> spread_window_;
  mutable std::deque<std::pair<double, double>> volume_imb_window_; // {signed_vol, vol}
  
  // Cached rolling statistics (avoid recomputation)
  double mid_sum_ = 0;
  double mid_sq_sum_ = 0;
};
