#pragma once

#include "features/FeaturesTick/Tick_Sequential.hpp"
#include "features/FeaturesMinute/Minute_Sequential.hpp"
#include "features/FeaturesHour/Hour_Sequential.hpp"
#include "features/backend/FeatureStore.hpp"
#include "lob/LimitOrderBookDefine.hpp"
#include "math/sample/ResampleTimeBar.hpp"

// Sequential Core: Hierarchical 3-level feature computation with resampling
// Architecture: LOB -> Tick -> (resample) -> Minute -> (resample) -> Hour
class CoreSequential {
public:
  CoreSequential(const LOB_Feature *lob_feature,
                 GlobalFeatureStore *feature_store = nullptr,
                 size_t asset_id = 0,
                 size_t core_id = 0)
      : lob_feature_(lob_feature),
        tick_sequential_(lob_feature, feature_store, asset_id, core_id),
        minute_sequential_(&minute_bar_),
        hour_sequential_(&hour_bar_),
        feature_store_(feature_store),
        asset_id_(asset_id),
        core_id_(core_id) {
    if (feature_store_) {
      tick_sequential_.set_store_context(feature_store_, asset_id_, core_id_);
      minute_sequential_.set_store_context(feature_store_, asset_id_);
      minute_sequential_.set_worker_id(core_id_);
      hour_sequential_.set_store_context(feature_store_, asset_id_);
      hour_sequential_.set_worker_id(core_id_);
    }
  }

  void set_date(const std::string &date_str) {
    date_str_ = date_str;
    tick_sequential_.set_date(date_str);
    minute_sequential_.set_date(date_str);
    hour_sequential_.set_date(date_str);
  }

  // Main entry: compute all 3 levels with cascading resampling
  void compute_and_store() noexcept {
    const LOB_Feature &lob = *lob_feature_;
    const double mid_price = get_mid_price();
    const uint64_t tick_volume = lob.volume;
    const uint32_t minute_now = lob.hour * 60u + lob.minute;
    const uint32_t hour_now = lob.hour;

    // Update accumulators with current tick data
    minute_accumulator_.update(mid_price, tick_volume);
    hour_accumulator_.update(mid_price, tick_volume);

    // LEVEL 0: Tick-level features (direct from LOB_feature_)
    tick_sequential_.compute_and_store();

    // Fast path: same minute → nothing to resample
    if (minute_now == last_minute_) [[likely]] {
      // CRITICAL: Mark L0 even in fast path!
      // CS worker needs to know each tick is done
      if (feature_store_ && !date_str_.empty()) {
        size_t l0_t = time_to_trading_seconds(lob.hour, lob.minute, lob.second);
        feature_store_->ts_update(date_str_, core_id_, asset_id_, l0_t);
      }
      return;
    }

    // Resample tick → minute
    const bool is_minute_close = (lob.hour == 11 && lob.minute == 30) ||
                                 (lob.hour == 15 && lob.minute == 0);
    resample_tick_to_minute(mid_price, minute_now, is_minute_close);
    minute_sequential_.compute_and_store();

    // Resample minute → hour
    if (hour_now != last_hour_) {
      resample_minute_to_hour(hour_now);
      hour_sequential_.compute_and_store();
    }

    // CRITICAL: Mark L0 progress after ALL levels (L0/L1/L2) computed
    // CS worker depends on L1/L2 being ready when L0 timeslot is marked
    // This ensures order-level synchronization for real-time CS computation
    if (feature_store_ && !date_str_.empty()) {
      size_t l0_t = time_to_trading_seconds(lob.hour, lob.minute, lob.second);
      feature_store_->ts_update(date_str_, core_id_, asset_id_, l0_t);
    }
  }

private:
  // Tick → Minute resampling
  void resample_tick_to_minute(double close_price, uint32_t current_minute, bool is_minute_close) noexcept {
    last_minute_ = current_minute;

    // Convert clock minute to trading minute index (0-239)
    const uint8_t hour = current_minute / 60;
    const uint8_t minute = current_minute % 60;
    const size_t trading_minute_idx = time_to_trading_seconds(hour, minute, 0) / 60;

    // Build minute bar from accumulated tick data
    minute_bar_ = {
        .timestamp_1m = trading_minute_idx,
        .instrument_id = static_cast<uint32_t>(asset_id_),
        .open_1m = minute_accumulator_.open,
        .high_1m = minute_accumulator_.high,
        .low_1m = minute_accumulator_.low,
        .close_1m = close_price,
        .vwap_1m = minute_accumulator_.vwap(),
        .volume_1m = minute_accumulator_.volume,
        .universe_ids_1m = 0,
        .market_close_1m = is_minute_close};

    minute_accumulator_.reset(close_price);
  }

  // Minute → Hour resampling
  void resample_minute_to_hour(uint32_t current_hour) noexcept {
    last_hour_ = current_hour;

    bool is_close = (current_hour == 11 || current_hour == 15);

    // Convert clock hour to trading hour index (0-3)
    // Morning: 9, 10, 11 → 0, 1, 2
    // Afternoon: 13, 14 → 2, 3 (overlapping with hour 11-12 boundary)
    const size_t trading_hour_idx = time_to_trading_seconds(current_hour, 0, 0) / 3600;

    // Build hour bar from accumulated minute data
    hour_bar_ = {
        .timestamp_1h = trading_hour_idx,
        .instrument_id = static_cast<uint32_t>(asset_id_),
        .open_1h = hour_accumulator_.open,
        .high_1h = hour_accumulator_.high,
        .low_1h = hour_accumulator_.low,
        .close_1h = minute_bar_.close_1m,
        .vwap_1h = hour_accumulator_.vwap(),
        .volume_1h = hour_accumulator_.volume,
        .universe_ids_1h = 0,
        .market_close_1h = is_close,
        .prev_day_close = prev_day_close_};

    if (is_close)
      prev_day_close_ = hour_bar_.close_1h;
    hour_accumulator_.reset(hour_bar_.close_1h);
  }

  // Get mid price from depth buffer
  double get_mid_price() const noexcept {
    if (lob_feature_->depth_buffer.size() < 2 * LOB_FEATURE_DEPTH_LEVELS) {
      return lob_feature_->price;
    }
    Level *best_ask = lob_feature_->depth_buffer[LOB_FEATURE_DEPTH_LEVELS - 1];
    Level *best_bid = lob_feature_->depth_buffer[LOB_FEATURE_DEPTH_LEVELS];
    return (best_ask && best_bid) ? (best_bid->price + best_ask->price) * 0.5 : lob_feature_->price;
  }

  const LOB_Feature *lob_feature_;
  Tick_Sequential tick_sequential_;
  Minute_Sequential minute_sequential_;
  Hour_Sequential hour_sequential_;

  GlobalFeatureStore *feature_store_;
  size_t asset_id_;
  size_t core_id_;
  std::string date_str_;

  // Resampled data buffers
  MinuteBar minute_bar_;
  HourBar hour_bar_;

  // Resampling state
  uint32_t last_minute_ = 0;
  uint32_t last_hour_ = 0;
  ResampleTimeBar<> minute_accumulator_;
  ResampleTimeBar<> hour_accumulator_;
  double prev_day_close_ = 0;
};
