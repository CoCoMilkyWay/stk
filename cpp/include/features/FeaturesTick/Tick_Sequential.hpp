#pragma once

#include "features/DataDefine.hpp"
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

    // Compute and write tick-level TS features
    compute_ts_tick(tick_data_.l0_index);

    // Write LOB depth snapshot for GUI (META features)
    write_lob_depth(tick_data_.l0_index);
  }

private:
  // Level 0: Tick-level TS features computation
  void compute_ts_tick(size_t t) {
    // Compute TS features (reuse member buffer)
    ts_features_buffer_[0] = compute_tick_ret_z();
    ts_features_buffer_[1] = compute_tobi_osc();
    ts_features_buffer_[2] = compute_micro_gap_norm();
    ts_features_buffer_[3] = compute_spread_momentum();
    ts_features_buffer_[4] = compute_signed_volume_imb();

    // Write TS features
    constexpr size_t level_idx = 0;
    TS_WRITE_FEATURES(store_, date_str_, level_idx, t, asset_id_, L0_TS_RANGE.start, L0_TS_RANGE.end, ts_features_buffer_, worker_id_);

    // Write data validity flag (event-driven sparsity marker)
    TS_WRITE_SINGLE(store_, date_str_, level_idx, t, L0_FieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
  }

  // Write LOB depth snapshot (N levels bid/ask price/amount for GUI)
  // No validity check - always write available depth (may be partial)
  void write_lob_depth(size_t t) {
    constexpr size_t N = L2::LOB_DEPTH;
    const auto &depth = tick_data_.lob.depth_buffer;

    constexpr size_t level_idx = 0;
    constexpr size_t bid_price_offset = L0_FIELD_OFFSETS[L0_FieldOffset::_bid_price];
    constexpr size_t mid_price_offset = L0_FIELD_OFFSETS[L0_FieldOffset::_mid_price];

    // depth_buffer layout: [0:N-1]=ask(N→1), [N:2N-1]=bid(1→N)
    // Output: bid[0]=bid1(best), ask[0]=ask1(best)
    // Fill all N levels, unfilled slots will have price/amount = 0
    for (size_t i = 0; i < N; ++i) {
      Level *bid_level = (N + i < depth.size()) ? depth[N + i] : nullptr;
      Level *ask_level = (N - 1 >= i && N - 1 - i < depth.size()) ? depth[N - 1 - i] : nullptr;

      float bid_price = bid_level ? bid_level->price * 0.01f : 0.0f;
      float ask_price = ask_level ? ask_level->price * 0.01f : 0.0f;
      float bid_amount = bid_level ? bid_level->net_quantity * bid_price : 0.0f;
      float ask_amount = ask_level ? -ask_level->net_quantity * ask_price : 0.0f;

      lob_depth_buffer_[i] = bid_price;          // [0:N-1]
      lob_depth_buffer_[N + i] = ask_price;      // [N:2N-1]
      lob_depth_buffer_[2 * N + i] = bid_amount; // [2N:3N-1]
      lob_depth_buffer_[3 * N + i] = ask_amount; // [3N:4N-1]
    }

    // Write mid_price at the end
    lob_depth_buffer_[4 * N] = get_mid_price();

    // Batch write all depth features + mid_price
    TS_WRITE_FEATURES(store_, date_str_, level_idx, t, asset_id_, bid_price_offset, mid_price_offset + 1, lob_depth_buffer_, worker_id_);
  }

  // Get mid price from depth buffer
  float get_mid_price() const {
    const auto &depth = tick_data_.lob.depth_buffer;
    if (depth.size() < 2 * L2::LOB_DEPTH)
      return tick_data_.lob.price;

    Level *best_ask = depth[L2::LOB_DEPTH - 1]; // sell1
    Level *best_bid = depth[L2::LOB_DEPTH];     // buy1

    if (best_ask && best_bid)
      return (best_ask->price + best_bid->price) * 0.005; // 0.01/2

    return tick_data_.lob.price;
  }

  // Get spread from depth buffer
  float get_spread() const {
    const auto &depth = tick_data_.lob.depth_buffer;
    if (depth.size() < 2 * L2::LOB_DEPTH)
      return 0.0;

    Level *best_ask = depth[L2::LOB_DEPTH - 1];
    Level *best_bid = depth[L2::LOB_DEPTH];

    if (best_ask && best_bid)
      return (best_ask->price - best_bid->price) * 0.01;

    return 0.0;
  }

  // Get top-of-book imbalance (TOBI)
  float get_tobi() const {
    const auto &depth = tick_data_.lob.depth_buffer;
    if (depth.size() < 2 * L2::LOB_DEPTH)
      return 0.0;

    Level *best_ask = depth[L2::LOB_DEPTH - 1];
    Level *best_bid = depth[L2::LOB_DEPTH];

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

  // Reusable buffers for batch writes (high-frequency hot path)
  float ts_features_buffer_[L0_TS_RANGE.end - L0_TS_RANGE.start]; // TS features batch write
  float lob_depth_buffer_[4 * L2::LOB_DEPTH + 1];                 // LOB depth + mid_price batch write
};
