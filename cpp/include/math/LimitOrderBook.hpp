// 2021-Deep Learning for Market by Order Data
// https://arxiv.org/abs/2102.08811

// 2025-An Efficient deep learning model to Predict Stock Price Movement Based on Limit Order Book
// https://arxiv.org/abs/2505.22678

#pragma once

#include "define/CBuffer.hpp"
#include "define/Dtype.hpp"
#include "misc/print.hpp"

#define TICK_SIZE 0.01f

template <typename T1, typename T_delta_t, typename T_direction, size_t N> // compiler auto derive
class LimitOrderBook {
public:
  // Constructor
  explicit LimitOrderBook(
      CBuffer<T_delta_t, N> *delta_t,
      CBuffer<T1, N> *prices,
      CBuffer<T1, N> *volumes,
      CBuffer<T1, N> *turnovers,
      CBuffer<T1, N> *vwaps,
      CBuffer<T_direction, N> *directions,
      CBuffer<T1, N> *spreads,
      CBuffer<T1, N> *mid_prices)
      : delta_t_(delta_t),
        prices_(prices),
        volumes_(volumes),
        turnovers_(turnovers),
        vwaps_(vwaps),
        directions_(directions),
        spreads_(spreads),
        mid_prices_(mid_prices) {}

  inline void update(const Table::Snapshot_Record *snapshot, const bool is_new_session_start) {
    T_delta_t delta_t = static_cast<T_delta_t>(is_new_session_start ? 0 : (snapshot->seconds_in_day - last_seconds_in_day));
    const float best_bid_price = snapshot->bid_price_ticks[0];
    const float best_ask_price = snapshot->ask_price_ticks[0];

    // Calculate derived metrics with conditional moves instead of branches
    const float mid_price = (best_bid_price + best_ask_price) * 0.5f;
    const float spread = best_ask_price - best_bid_price;

    // Optimize volume calculations
    const float volume = static_cast<float>(snapshot->volume) * 100.0f; // hands -> shares
    const float turnover = static_cast<float>(snapshot->turnover);
    const float vwap = (volume > 0) ? (turnover / volume) : vwaps_->back();

    println(delta_t, snapshot->latest_price_tick, volume, turnover, vwap, spread, mid_price);

    // Batch update analysis buffers for better cache locality
    delta_t_->push_back(delta_t);
    prices_->push_back(snapshot->latest_price_tick);
    volumes_->push_back(volume);
    turnovers_->push_back(turnover);
    vwaps_->push_back(vwap);
    directions_->push_back(static_cast<T_direction>(snapshot->direction));
    spreads_->push_back(spread);
    mid_prices_->push_back(mid_price);

    last_seconds_in_day = snapshot->seconds_in_day;
  }

private:
  uint32_t last_seconds_in_day = 0;

  CBuffer<T_delta_t, N> *delta_t_;
  CBuffer<T1, N> *prices_;
  CBuffer<T1, N> *volumes_;
  CBuffer<T1, N> *turnovers_;
  CBuffer<T1, N> *vwaps_;
  CBuffer<T_direction, N> *directions_;
  CBuffer<T1, N> *spreads_;
  CBuffer<T1, N> *mid_prices_;
};