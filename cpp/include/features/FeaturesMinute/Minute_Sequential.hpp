#pragma once

#include "features/backend/FeatureStore.hpp"
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>

class DAG; // Forward declaration

// Minute-level sequential feature computation
// Input: DAG (访问 minute_data 和 L1 deque)
class Minute_Sequential {
public:
  Minute_Sequential(DAG &dag,
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
  // Level 1: Minute-level TS features computation
  void compute_ts_minute(bool is_valid, size_t t);

  // Write OHLC and volume META features for GUI
  void write_ohlc_meta(bool is_valid, size_t t);

  // Feature computations
  float compute_min_ret_z();
  float compute_rv_5m_norm();
  float compute_vwap_gap_pct();
  float compute_momentum_15m();
  float compute_range_squeeze();

  DAG &dag_;
  GlobalFeatureStore *store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;

  // 输出缓冲区
  std::array<float, L1_TS_WIDTH> ts_features_buffer_;
};

// 实现需要完整的 DAG 定义，放在头文件末尾
#include "features/ComputeGraph.hpp"

inline void Minute_Sequential::compute_and_store() {
  // Check if we have data
  if (dag_.minute_data.close.empty()) [[unlikely]]
    return;

  // Compute minute index (时间索引)
  dag_.l1.MinuteIndex.compute();

  // Read latest bar from CBuffers
  const float close = dag_.minute_data.close.back();
  const uint32_t bid_vol = dag_.minute_data.bid_volume.back();
  const uint32_t ask_vol = dag_.minute_data.ask_volume.back();
  const uint32_t total_volume = bid_vol + ask_vol;

  bool is_valid = close > 0 && total_volume > 0;

  // Compute and write minute-level TS features
  compute_ts_minute(is_valid, dag_.minute_data.l1_index);
}

inline void Minute_Sequential::compute_ts_minute(bool is_valid, size_t t) {
  // 写入特征到输出缓冲区 (顺序与 LEVEL_1_FIELDS 定义一致)
  if (!is_valid) {
    std::memset(ts_features_buffer_.data(), 0, sizeof(ts_features_buffer_));
  } else {
    ts_features_buffer_ = {
        dag_.l1.Min_.back(), // min
        compute_min_ret_z(),
        compute_rv_5m_norm(),
        compute_vwap_gap_pct(),
        compute_momentum_15m(),
        compute_range_squeeze()};
  }

  // Write TS features [min, range_squeeze]
  TS_WRITE_FEATURES(store_, date_str_, 1, t, asset_id_, 0, L1_FieldOffset::range_squeeze, ts_features_buffer_.data(), worker_id_);

  // Write data validity flag (event-driven sparsity marker)
  TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_data_valid, asset_id_, is_valid ? 1.0f : 0.0f, worker_id_);

  // Write OHLC + volume for GUI (OrderFlow visualization)
  write_ohlc_meta(is_valid, t);
}

inline void Minute_Sequential::write_ohlc_meta(bool is_valid, size_t t) {
  if (!is_valid) {
    return; // Tensor auto-cleared
  }

  const float open = dag_.minute_data.open.back();
  const float high = dag_.minute_data.high.back();
  const float low = dag_.minute_data.low.back();
  const float close = dag_.minute_data.close.back();
  const float volume = static_cast<float>(dag_.minute_data.bid_volume.back() + dag_.minute_data.ask_volume.back());

  // Store prices as integer cents for fp16 precision (price in yuan * 100)
  TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_ohlc_open, asset_id_, open * 100.0f, worker_id_);
  TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_ohlc_high, asset_id_, high * 100.0f, worker_id_);
  TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_ohlc_low, asset_id_, low * 100.0f, worker_id_);
  TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_ohlc_close, asset_id_, close * 100.0f, worker_id_);
  TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_ohlc_volume, asset_id_, volume, worker_id_);
}

// Feature 1: min_ret_z - Minute return z-score (rolling 60m)
inline float Minute_Sequential::compute_min_ret_z() {
  auto &window = dag_.l1.minute_return_window;
  const float close = dag_.minute_data.close.back();
  float ret = 0;

  if (!window.empty() && window.back() > 0) {
    ret = std::log(close / window.back());
  }

  window.push_back(close);
  if (window.size() > 60)
    window.pop_front();

  if (window.size() < 10)
    return static_cast<float>(ret);

  // Compute rolling z-score
  float sum = 0, sq_sum = 0;
  size_t n = window.size();
  for (size_t i = 1; i < n; ++i) {
    if (window[i - 1] > 0) {
      float r = std::log(window[i] / window[i - 1]);
      sum += r;
      sq_sum += r * r;
    }
  }

  float mean = sum / (n - 1);
  float variance = sq_sum / (n - 1) - mean * mean;
  float stddev = std::sqrt(std::max(variance, 1e-10f));

  // Winsorize to [-3, 3]
  float z = (ret - mean) / stddev;
  return static_cast<float>(std::clamp(z, -3.0f, 3.0f));
}

// Feature 2: rv_5m_norm - 5-minute realized volatility (log normalized)
inline float Minute_Sequential::compute_rv_5m_norm() {
  auto &window = dag_.l1.rv_window;
  const float close = dag_.minute_data.close.back();

  window.push_back(close);
  if (window.size() > 5)
    window.pop_front();

  if (window.size() < 2)
    return 0.0f;

  // Compute realized volatility
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

// Feature 3: vwap_gap_pct - VWAP gap percentage (rolling z-score)
inline float Minute_Sequential::compute_vwap_gap_pct() {
  auto &window = dag_.l1.vwap_window;
  const float close = dag_.minute_data.close.back();
  const float bid_vol = dag_.minute_data.bid_volume.back();
  const float ask_vol = dag_.minute_data.ask_volume.back();
  const float bid_amt = dag_.minute_data.bid_amount.back();
  const float ask_amt = dag_.minute_data.ask_amount.back();
  const float total_vol = bid_vol + ask_vol;
  const float vwap = (total_vol > 0) ? ((bid_amt + ask_amt) / total_vol) : close;

  if (vwap <= 0)
    return 0.0f;

  float gap = (close - vwap) / vwap;

  window.push_back(gap);
  if (window.size() > 30)
    window.pop_front();

  if (window.size() < 10)
    return static_cast<float>(gap);

  // Rolling z-score
  float sum = 0, sq_sum = 0;
  for (float g : window) {
    sum += g;
    sq_sum += g * g;
  }
  float mean = sum / window.size();
  float variance = sq_sum / window.size() - mean * mean;
  float stddev = std::sqrt(std::max(variance, 1e-10f));

  return static_cast<float>((gap - mean) / stddev);
}

// Feature 4: momentum_15m - 15-minute momentum (cumulative z-score)
inline float Minute_Sequential::compute_momentum_15m() {
  auto &window = dag_.l1.momentum_window;
  auto &returns = dag_.l1.momentum_returns;
  const float close = dag_.minute_data.close.back();

  if (!window.empty() && window.back() > 0) {
    float ret = std::log(close / window.back());
    returns.push_back(ret);
  }

  window.push_back(close);
  if (window.size() > 15) {
    window.pop_front();
    if (!returns.empty())
      returns.pop_front();
  }

  if (returns.empty())
    return 0.0f;

  // Cumulative return
  float cum_ret = 0;
  for (float r : returns)
    cum_ret += r;

  // Rolling stddev
  float sum = 0, sq_sum = 0;
  for (float r : returns) {
    sum += r;
    sq_sum += r * r;
  }
  float variance = sq_sum / returns.size() - std::pow(sum / returns.size(), 2);
  float stddev = std::sqrt(std::max(variance, 1e-10f));

  return static_cast<float>(cum_ret / stddev);
}

// Feature 5: range_squeeze - Range squeeze indicator
inline float Minute_Sequential::compute_range_squeeze() {
  auto &window = dag_.l1.range_window;
  const float high = dag_.minute_data.high.back();
  const float low = dag_.minute_data.low.back();
  const float close = dag_.minute_data.close.back();

  float range = high - low;

  window.push_back({range, close});
  if (window.size() > 30)
    window.pop_front();

  if (window.size() < 10)
    return 0.0f;

  // Compute rolling volatility (30m)
  float sum_sq = 0;
  for (size_t i = 1; i < window.size(); ++i) {
    if (window[i - 1].second > 0) {
      float r = std::log(window[i].second / window[i - 1].second);
      sum_sq += r * r;
    }
  }
  float vol = std::sqrt(sum_sq / (window.size() - 1));

  // Range / volatility ratio (clipped)
  float ratio = range / (vol * close + 1e-10);
  return static_cast<float>(std::clamp(ratio, -3.0f, 3.0f));
}
