#pragma once

#include "features/backend/FeatureStore.hpp"
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
  float compute_min_ret();
  float compute_next_1m_ret();

  DAG &dag_;
  GlobalFeatureStore *store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;

  // 输出缓冲区
  std::array<float, L1_TS_WIDTH> ts_features_buffer_;

  // 标签计算状态
  float pending_1m_ret_ = 0.0f; // 待输出的下一分钟收益
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
  float min_ret = 0.0f;
  float next_1m_ret = 0.0f;

  if (is_valid) {
    min_ret = compute_min_ret();
    next_1m_ret = compute_next_1m_ret();
  }

  dag_.l1.Ci_5.compute();   // input: l0.BidQty_, l0.AskQty_
  dag_.l1.Ci_5.flush();     // output: Ci_5_
  dag_.l1.Ci_10.compute();  // input: l0.BidQty_, l0.AskQty_
  dag_.l1.Ci_10.flush();    // output: Ci_10_
  dag_.l1.Ci_30.compute();  // input: l0.BidQty_, l0.AskQty_
  dag_.l1.Ci_30.flush();    // output: Ci_30_
  dag_.l1.Ci_all.compute(); // input: l0.td
  dag_.l1.Ci_all.flush();   // output: Ci_all_
  dag_.l1.Cwi_1.compute();  // input: l0.BidQty_, l0.AskQty_
  dag_.l1.Cwi_1.flush();    // output: Cwi_1_
  dag_.l1.Cwi_2.compute();  // input: l0.BidQty_, l0.AskQty_
  dag_.l1.Cwi_2.flush();    // output: Cwi_2_
  dag_.l1.Ddi_1.compute();  // input: l0.BidQty_, l0.AskQty_, l0.BidPrice_, l0.AskPrice_
  dag_.l1.Ddi_1.flush();    // output: Ddi_1_
  dag_.l1.Ddi_2.compute();  // input: l0.BidQty_, l0.AskQty_, l0.BidPrice_, l0.AskPrice_
  dag_.l1.Ddi_2.flush();    // output: Ddi_2_
  dag_.l1.Tbr_5.compute();  // input: l0.BidQty_, l0.td
  dag_.l1.Tbr_5.flush();    // output: Tbr_5_
  dag_.l1.Tar_5.compute();  // input: l0.AskQty_, l0.td
  dag_.l1.Tar_5.flush();    // output: Tar_5_

  ts_features_buffer_[L1_FieldOffset::min] = is_valid ? dag_.l1.Min_.back() : 0.0f;
  ts_features_buffer_[L1_FieldOffset::min_ret] = min_ret;
  ts_features_buffer_[L1_FieldOffset::ci_5] = dag_.l1.Ci_5_.back();
  ts_features_buffer_[L1_FieldOffset::ci_10] = dag_.l1.Ci_10_.back();
  ts_features_buffer_[L1_FieldOffset::ci_30] = dag_.l1.Ci_30_.back();
  ts_features_buffer_[L1_FieldOffset::ci_all] = dag_.l1.Ci_all_.back();
  ts_features_buffer_[L1_FieldOffset::cwi_1] = dag_.l1.Cwi_1_.back();
  ts_features_buffer_[L1_FieldOffset::cwi_2] = dag_.l1.Cwi_2_.back();
  ts_features_buffer_[L1_FieldOffset::ddi_1] = dag_.l1.Ddi_1_.back();
  ts_features_buffer_[L1_FieldOffset::ddi_2] = dag_.l1.Ddi_2_.back();
  ts_features_buffer_[L1_FieldOffset::tbr_5] = dag_.l1.Tbr_5_.back();
  ts_features_buffer_[L1_FieldOffset::tar_5] = dag_.l1.Tar_5_.back();

  // Write TS features [min, min_ret, ci_5, ci_10, ci_30, ci_all, cwi_1, cwi_2, ddi_1, ddi_2, tbr_5, tar_5]
  TS_WRITE_FEATURES(store_, date_str_, 1, t, asset_id_, L1_FieldOffset::min, L1_FieldOffset::tar_5, ts_features_buffer_.data(), worker_id_);

  // Write label: next_1m_ret (滞后1分钟输出)
  TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::next_1m_ret, asset_id_, next_1m_ret, worker_id_);

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

// Feature: min_ret - Minute return (log return)
inline float Minute_Sequential::compute_min_ret() {
  auto &window = dag_.l1.minute_return_window;
  const float close = dag_.minute_data.close.back();
  float ret = 0.0f;

  if (!window.empty() && window.back() > 0) {
    ret = std::log(close / window.back());
  }

  window.push_back(close);
  if (window.size() > 2)
    window.pop_front();

  return ret;
}

// Label: next_1m_ret - Next minute return (滞后1分钟输出)
inline float Minute_Sequential::compute_next_1m_ret() {
  // 输出上一分钟待定的收益 (作为该分钟的next_1m_ret标签)
  float output = pending_1m_ret_;

  // 计算当前分钟的收益，作为上一分钟的next_1m_ret
  auto &window = dag_.l1.minute_return_window;
  if (window.size() >= 2) {
    float prev_close = window[window.size() - 2];
    float curr_close = window.back();
    if (prev_close > 0 && curr_close > 0) {
      pending_1m_ret_ = std::log(curr_close / prev_close);
    } else {
      pending_1m_ret_ = 0.0f;
    }
  } else {
    pending_1m_ret_ = 0.0f;
  }

  return output;
}
