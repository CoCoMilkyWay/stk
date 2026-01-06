#pragma once

#include "features/backend/FeatureStore.hpp"
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>

class DAG; // Forward declaration

// Hour-level sequential feature computation
// Input: DAG (访问 hour_data 和 L2 deque)
class Hour_Sequential {
public:
  Hour_Sequential(DAG &dag,
                  GlobalFeatureStore &store,
                  size_t asset_id,
                  size_t worker_id)
      : dag_(dag),
        store_(&store),
        asset_id_(asset_id),
        worker_id_(worker_id) {}

  void set_date(const std::string &date_str) {
    date_str_ = date_str;
  }

  // Main computation entry (called by CoreSequential)
  void compute_and_store();

private:
  // Level 2: Hour-level TS features computation (leaf node)
  void compute_ts_hour(bool is_valid, size_t t);

  // Feature computations
  float compute_hour_ret_12h_mom();
  float compute_hour_volatility();
  float compute_pivot_dev();
  float compute_dominant_persist();
  float compute_hour_overnight_gap();

  DAG &dag_;
  GlobalFeatureStore *store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;

  // 输出缓冲区
  std::array<float, L2_TS_WIDTH> ts_features_buffer_;
};

// 实现需要完整的 DAG 定义，放在头文件末尾
#include "features/ComputeGraph.hpp"

inline void Hour_Sequential::compute_and_store() {
  // Check if we have data
  if (dag_.hour_data.close.empty()) [[unlikely]]
    return;

  // Compute hour index (时间索引)
  dag_.l2.HourIndex.compute();

  // Read latest bar from CBuffers
  const float close = dag_.hour_data.close.back();
  const uint32_t bid_vol = dag_.hour_data.bid_volume.back();
  const uint32_t ask_vol = dag_.hour_data.ask_volume.back();
  const uint32_t total_volume = bid_vol + ask_vol;

  bool is_valid = close > 0 && total_volume > 0;

  // Compute and write hour-level TS features
  compute_ts_hour(is_valid, dag_.hour_data.l2_index);
}

inline void Hour_Sequential::compute_ts_hour(bool is_valid, size_t t) {
  // 写入特征到输出缓冲区 (顺序与 LEVEL_2_FIELDS 定义一致)
  if (!is_valid) {
    std::memset(ts_features_buffer_.data(), 0, sizeof(ts_features_buffer_));
  } else {
    ts_features_buffer_ = {
        dag_.l2.Hour_.back(), // hour
        compute_hour_ret_12h_mom(),
        compute_hour_volatility(),
        compute_pivot_dev(),
        compute_dominant_persist(),
        compute_hour_overnight_gap()};
  }

  // Write TS features [hour, hour_overnight_gap]
  TS_WRITE_FEATURES(store_, date_str_, 2, t, asset_id_, 0, L2_FieldOffset::hour_overnight_gap, ts_features_buffer_.data(), worker_id_);

  // Write data validity flag (event-driven sparsity marker)
  TS_WRITE_SINGLE(store_, date_str_, 2, t, L2_FieldOffset::_data_valid, asset_id_, is_valid ? 1.0f : 0.0f, worker_id_);
}

// Feature 1: hour_ret_12h_mom - 12-hour momentum z-score
inline float Hour_Sequential::compute_hour_ret_12h_mom() {
  auto &window = dag_.l2.hour_return_window;
  const float close = dag_.hour_data.close.back();

  window.push_back(close);
  if (window.size() > 48)
    window.pop_front();

  if (window.size() < 13)
    return 0.0f;

  // Compute 12-hour cumulative return
  float cum_ret = 0;
  size_t start_idx = window.size() > 12 ? window.size() - 12 : 0;
  for (size_t i = start_idx + 1; i < window.size(); ++i) {
    if (window[i - 1] > 0) {
      cum_ret += std::log(window[i] / window[i - 1]);
    }
  }

  // Z-score with 48-hour rolling window
  float sum = 0, sq_sum = 0;
  for (size_t i = 1; i < window.size(); ++i) {
    if (window[i - 1] > 0) {
      float r = std::log(window[i] / window[i - 1]);
      sum += r;
      sq_sum += r * r;
    }
  }

  float mean = sum / (window.size() - 1);
  float variance = sq_sum / (window.size() - 1) - mean * mean;
  float stddev = std::sqrt(std::max(variance, 1e-10f));

  return static_cast<float>(cum_ret / stddev);
}

// Feature 2: hour_volatility - 24-hour realized volatility (log normalized)
inline float Hour_Sequential::compute_hour_volatility() {
  auto &window = dag_.l2.hour_vol_window;
  const float close = dag_.hour_data.close.back();

  window.push_back(close);
  if (window.size() > 24)
    window.pop_front();

  if (window.size() < 2)
    return 0.0f;

  // Compute 24-hour realized volatility
  float sum_sq = 0;
  for (size_t i = 1; i < window.size(); ++i) {
    if (window[i - 1] > 0) {
      float r = std::log(window[i] / window[i - 1]);
      sum_sq += r * r;
    }
  }
  float rv = std::sqrt(sum_sq / (window.size() - 1));

  // Log transformation
  return static_cast<float>(std::log(rv + 1e-10));
}

// Feature 3: pivot_dev - Pivot point deviation
inline float Hour_Sequential::compute_pivot_dev() {
  const float high = dag_.hour_data.high.back();
  const float low = dag_.hour_data.low.back();
  const float close = dag_.hour_data.close.back();

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
inline float Hour_Sequential::compute_dominant_persist() {
  auto &window = dag_.l2.dominant_window;
  const float close = dag_.hour_data.close.back();
  const float bid_vol = dag_.hour_data.bid_volume.back();
  const float ask_vol = dag_.hour_data.ask_volume.back();
  const float bid_amt = dag_.hour_data.bid_amount.back();
  const float ask_amt = dag_.hour_data.ask_amount.back();
  const float total_vol = bid_vol + ask_vol;
  const float vwap = (total_vol > 0) ? ((bid_amt + ask_amt) / total_vol) : close;

  // Compute dominant side: volume-weighted buy/sell indicator
  float dominant = (close > vwap) ? 1.0 : -1.0;

  window.push_back(dominant);
  if (window.size() > 20)
    window.pop_front();

  if (window.size() < 5)
    return static_cast<float>(dominant);

  // EMA with α ≈ 0.1
  constexpr float alpha = 0.1;
  float ema = window[0];
  for (size_t i = 1; i < window.size(); ++i)
    ema = alpha * window[i] + (1 - alpha) * ema;

  // Z-score normalization
  float sum = 0, sq_sum = 0;
  for (float d : window) {
    sum += d;
    sq_sum += d * d;
  }
  float mean = sum / window.size();
  float variance = sq_sum / window.size() - mean * mean;
  float stddev = std::sqrt(std::max(variance, 1e-10f));

  return static_cast<float>((ema - mean) / stddev);
}

// Feature 5: hour_overnight_gap - Overnight gap (if applicable)
inline float Hour_Sequential::compute_hour_overnight_gap() {
  auto &return_window = dag_.l2.hour_return_window;
  auto &vol_window = dag_.l2.hour_vol_window;
  const float open = dag_.hour_data.open.back();

  // Use first value in return window as reference
  if (return_window.empty() || return_window.front() <= 0)
    return 0.0f;

  const float reference_close = return_window.front();
  float gap = open - reference_close;

  // Compute intraday volatility from historical data
  if (vol_window.size() < 5)
    return static_cast<float>(gap / reference_close);

  float sum_sq = 0;
  for (size_t i = 1; i < vol_window.size(); ++i) {
    if (vol_window[i - 1] > 0) {
      float r = std::log(vol_window[i] / vol_window[i - 1]);
      sum_sq += r * r;
    }
  }
  float intraday_vol = std::sqrt(sum_sq / (vol_window.size() - 1));

  if (intraday_vol < 1e-10)
    return 0.0f;

  // Winsorize gap
  float normalized_gap = gap / (reference_close * intraday_vol);
  return static_cast<float>(std::clamp(normalized_gap, -3.0f, 3.0f));
}
