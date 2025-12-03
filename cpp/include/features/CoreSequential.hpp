#pragma once

#include "features/DataDefine.hpp"
#include "features/FeaturesHour/Hour_Sequential.hpp"
#include "features/FeaturesMinute/Minute_Sequential.hpp"
#include "features/FeaturesTick/Tick_Sequential.hpp"
#include "features/backend/FeatureStore.hpp"
#include "math/sample/ResampleTick2Time.hpp"
#include "math/sample/ResampleTime2Time.hpp"

// Sequential Core: Hierarchical 3-level feature computation with resampling
// Architecture: LOB -> Tick -> (resample) -> Minute -> (resample) -> Hour
class CoreSequential {
public:
  CoreSequential(TickData &tick_data,
                 GlobalFeatureStore &store,
                 size_t asset_id = 0,
                 size_t core_id = 0)
      : store_(store),

        asset_id_(asset_id),
        core_id_(core_id),

        tick_data_(tick_data),

        tick_sequential_(tick_data_, store_, asset_id_, core_id_),
        minute_sequential_(minute_data_, store_, asset_id_, core_id_),
        hour_sequential_(hour_data_, store_, asset_id_, core_id_),

        tick2min_resampler_(tick_data_, minute_data_, 60),
        min2hour_resampler_(minute_data_, hour_data_, 60) {
    // Initialize metadata
    tick_data_.asset_id = static_cast<uint32_t>(asset_id_);
    minute_data_.asset_id = static_cast<uint32_t>(asset_id_);
    hour_data_.asset_id = static_cast<uint32_t>(asset_id_);
    tick_data_.core_id = static_cast<uint32_t>(core_id);
    minute_data_.core_id = static_cast<uint32_t>(core_id);
    hour_data_.core_id = static_cast<uint32_t>(core_id);
  }

  void set_date(const std::string &date_str) {
    date_str_ = date_str;
    tick_sequential_.set_date(date_str);
    minute_sequential_.set_date(date_str);
    hour_sequential_.set_date(date_str);
  }

  // Reset all resampler and data buffer state (called when starting a new day)
  void reset() {
    tick2min_resampler_.reset();
    min2hour_resampler_.reset();
    minute_data_.open.clear();
    minute_data_.high.clear();
    minute_data_.low.clear();
    minute_data_.close.clear();
    minute_data_.bid_volume.clear();
    minute_data_.ask_volume.clear();
    minute_data_.bid_amount.clear();
    minute_data_.ask_amount.clear();
    hour_data_.open.clear();
    hour_data_.high.clear();
    hour_data_.low.clear();
    hour_data_.close.clear();
    hour_data_.bid_volume.clear();
    hour_data_.ask_volume.clear();
    hour_data_.bid_amount.clear();
    hour_data_.ask_amount.clear();
  }

  // Main entry: compute all 3 levels with cascading resampling
  void compute_and_store() noexcept {
    // LEVEL 0: Tick-level features (direct from LOB_feature_)
    update_tick_metadata();
    tick_sequential_.compute_and_store();

    // Track sizes before resampling
    const size_t prev_minute_count = minute_data_.open.size();
    const size_t prev_hour_count = hour_data_.open.size();

    // Trigger tick -> minute resampling
    tick2min_resampler_.update();

    // Check if new minute bar was generated
    if (minute_data_.open.size() > prev_minute_count) {
      // Update minute metadata
      update_minute_metadata();
      minute_sequential_.compute_and_store();

      // Trigger minute -> hour resampling
      min2hour_resampler_.update();

      // Check if new hour bar was generated
      if (hour_data_.open.size() > prev_hour_count) {
        // Update hour metadata
        update_hour_metadata();
        hour_sequential_.compute_and_store();
      }
    }

    // Mark L0 progress after all levels (L0/L1/L2) computed
    if (!date_str_.empty()) {
      store_.ts_update(date_str_, core_id_, asset_id_, tick_data_.timestamp);
    }
  }

private:
  // Update tick-level metadata
  void inline update_tick_metadata() noexcept {
    tick_data_.timestamp = time_to_trading_seconds(tick_data_.lob.hour, tick_data_.lob.minute, tick_data_.lob.second);
  }

  // Update minute-level metadata
  void inline update_minute_metadata() noexcept {
    minute_data_.timestamp = tick_data_.timestamp / 60;
  }

  // Update hour-level metadata
  void inline update_hour_metadata() noexcept {
    hour_data_.timestamp = minute_data_.timestamp / 60;
  }

  GlobalFeatureStore &store_;
  size_t asset_id_;
  size_t core_id_;
  std::string date_str_;

  // Hierarchical data structures
  TickData &tick_data_;
  MinuteData minute_data_;
  HourData hour_data_;

  // Sequential feature processors
  Tick_Sequential tick_sequential_;
  Minute_Sequential minute_sequential_;
  Hour_Sequential hour_sequential_;

  // Resamplers
  ResampleTick2Time tick2min_resampler_;
  ResampleTime2Time min2hour_resampler_;
};
