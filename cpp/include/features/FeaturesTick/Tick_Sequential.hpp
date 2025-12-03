#pragma once

#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp"
#include "features/backend/FeatureStore.hpp"
#include <algorithm>
#include <cmath>
#include <deque>
#include <vector>

// Tick-level sequential feature computation
class Tick_Sequential {
public:
  Tick_Sequential(const TickData &tick_data,
                  GlobalFeatureStore &store,
                  size_t asset_id,
                  size_t worker_id)
      : tick_data_(tick_data),
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

    const TickData &tick_data = tick_data_;
    size_t t = time_to_trading_seconds(tick_data_.lob.hour, tick_data_.lob.minute, tick_data_.lob.second);

    // Check if this asset is active (has valid LOB data)
    bool is_valid = check_lob_valid(tick_data);

    // Compute and write tick-level TS features
    compute_ts_tick(is_valid, t);
  }

private:
  // Level 0: Tick-level TS features computation
  void compute_ts_tick(bool is_valid, size_t t) {
    // Allocate feature array (only TS features)
    float features[L0_TS_RANGE.end - L0_TS_RANGE.start];

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
    TS_WRITE_FEATURES(store_, date_str_, level_idx, t, asset_id_, L0_TS_RANGE.start, L0_TS_RANGE.end, features, worker_id_);

    // Write asset validity flag (business logic, not backend requirement)
    TS_WRITE_SINGLE(store_, date_str_, level_idx, t, L0_FieldOffset::asset_valid, asset_id_, is_valid ? 1.0f : 0.0f, worker_id_);
  }

  // Check if LOB has valid data
  bool check_lob_valid(const TickData &) const {
    return tick_data_.lob.price > 0 && tick_data_.lob.depth_buffer.size() >= 2 * LOB_FEATURE_DEPTH_LEVELS;
  }

  // Get mid price from depth buffer
  float get_mid_price() const {
    const auto &depth = tick_data_.lob.depth_buffer;
    if (depth.size() < 2 * LOB_FEATURE_DEPTH_LEVELS)
      return tick_data_.lob.price;

    Level *best_ask = depth[LOB_FEATURE_DEPTH_LEVELS - 1]; // sell1
    Level *best_bid = depth[LOB_FEATURE_DEPTH_LEVELS];     // buy1

    if (best_ask && best_bid)
      return (best_ask->price + best_bid->price) * 0.005; // 0.01/2

    return tick_data_.lob.price;
  }

  // Get spread from depth buffer
  float get_spread() const {
    const auto &depth = tick_data_.lob.depth_buffer;
    if (depth.size() < 2 * LOB_FEATURE_DEPTH_LEVELS)
      return 0.0;

    Level *best_ask = depth[LOB_FEATURE_DEPTH_LEVELS - 1];
    Level *best_bid = depth[LOB_FEATURE_DEPTH_LEVELS];

    if (best_ask && best_bid)
      return (best_ask->price - best_bid->price) * 0.01;

    return 0.0;
  }

  // Get top-of-book imbalance (TOBI)
  float get_tobi() const {
    const auto &depth = tick_data_.lob.depth_buffer;
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

    return static_cast<float>(bid_qty - ask_qty) / (bid_qty + ask_qty);
  }

  // Feature 1: tick_ret_z - Rolling z-score normalized tick return (optimized incremental)
  float compute_tick_ret_z() {
    float mid = get_mid_price();

    // Compute log return
    if (last_mid_ <= 0) {
      last_mid_ = mid;
      return 0.0f;
    }

    float ret = std::log(mid / last_mid_);
    last_mid_ = mid;

    // Update window
    ret_window_.push_back(ret);
    if (ret_window_.size() > 50)
      ret_window_.pop_front();

    if (ret_window_.size() < 10)
      return static_cast<float>(ret);

    // Incremental mean/variance (Welford's algorithm)
    size_t n = ret_window_.size();
    float sum = 0, sq_sum = 0;
    for (float r : ret_window_) {
      sum += r;
      sq_sum += r * r;
    }

    float mean = sum / n;
    float variance = sq_sum / n - mean * mean;
    float stddev = std::sqrt(std::max(variance, 1e-10f));

    return static_cast<float>((ret - mean) / stddev);
  }

  // Feature 2: tobi_osc - Top-of-book imbalance oscillator
  float compute_tobi_osc() const {
    float tobi = get_tobi();

    // Update window
    tobi_window_.push_back(tobi);
    if (tobi_window_.size() > 50)
      tobi_window_.pop_front();

    if (tobi_window_.size() < 10)
      return tobi;

    // Reuse scratch buffer (no heap allocation)
    scratch_.assign(tobi_window_.begin(), tobi_window_.end());
    size_t mid_idx = scratch_.size() / 2;

    // O(N) median via nth_element instead of O(N log N) sort
    std::nth_element(scratch_.begin(), scratch_.begin() + mid_idx, scratch_.end());
    float median = scratch_[mid_idx];

    // Compute abs deviations in-place, reuse same buffer
    for (float &val : scratch_)
      val = std::abs(val - median);

    // O(N) MAD via nth_element
    std::nth_element(scratch_.begin(), scratch_.begin() + mid_idx, scratch_.end());
    float mad = scratch_[mid_idx];

    if (mad < 1e-8f)
      return 0.0f;

    return std::clamp((tobi - median) / mad, -3.0f, 3.0f);
  }

  // Feature 3: micro_gap_norm - Micro price gap normalized (optimized incremental)
  float compute_micro_gap_norm() {
    float mid = get_mid_price();
    float micro = tick_data_.lob.price; // last trade price

    // Update window and incremental stats
    if (mid_price_window_.size() >= 50) {
      float old_val = mid_price_window_.front();
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
    float mean = mid_sum_ / n;
    float variance = mid_sq_sum_ / n - mean * mean;
    float stddev = std::sqrt(std::max(variance, 1e-10f));

    // tanh((micro-mid)/σ)
    float gap = (micro - mid) / stddev;
    return static_cast<float>(std::tanh(gap));
  }

  // Feature 4: spread_momentum - Short-term spread change (optimized single-pass EMA)
  float compute_spread_momentum() {
    float spread = get_spread();

    // Update window
    spread_window_.push_back(spread);
    if (spread_window_.size() > 20)
      spread_window_.pop_front();

    if (spread_window_.size() < 2) {
      ema_spread_ = spread;
      return 0.0f;
    }

    // Incremental EMA update (O(1) instead of O(N))
    constexpr float alpha = 0.095;
    ema_spread_ = alpha * spread + (1 - alpha) * ema_spread_;

    return static_cast<float>(spread - ema_spread_);
  }

  // Feature 5: signed_volume_imb - Signed volume imbalance
  float compute_signed_volume_imb() const {

    float sign = 0;
    if (tick_data_.lob.order_type == L2::OrderType::TAKER) {
      sign = tick_data_.lob.order_dir == L2::OrderDirection::BID ? 1.0 : -1.0; // buy=+1, sell=-1
    }

    float signed_vol = sign * tick_data_.lob.volume;
    volume_imb_window_.push_back({signed_vol, static_cast<float>(tick_data_.lob.volume)});

    if (volume_imb_window_.size() > 20)
      volume_imb_window_.pop_front();

    // Σ(signxsize) / Σ|size|
    float sum_signed = 0, sum_abs = 0;
    for (const auto &[sv, v] : volume_imb_window_) {
      sum_signed += sv;
      sum_abs += v;
    }

    if (sum_abs < 1e-8)
      return 0.0f;

    return static_cast<float>(sum_signed / sum_abs);
  }

  const TickData &tick_data_;
  GlobalFeatureStore *store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;

  // Rolling windows for TS features (optimized)
  std::deque<float> ret_window_; // Store returns directly (not prices)
  float last_mid_ = 0;           // Last mid price for incremental return
  mutable std::deque<float> mid_price_window_;
  mutable std::deque<float> tobi_window_;
  float ema_spread_ = 0; // EMA state for spread momentum
  mutable std::deque<float> spread_window_;
  mutable std::deque<std::pair<float, float>> volume_imb_window_; // {signed_vol, vol}

  // Cached rolling statistics (avoid recomputation)
  float mid_sum_ = 0;
  float mid_sq_sum_ = 0;

  // Scratch buffer for median computation (avoids heap allocation per call)
  mutable std::vector<float> scratch_;
};
