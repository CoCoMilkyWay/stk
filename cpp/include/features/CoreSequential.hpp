#pragma once

#include "misc/profiler.hpp"
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
    TraceN("TS");
    TraceColor(C_Cyan);

    // LEVEL 0: Tick-level features (direct from LOB_feature_)
    update_tick_metadata();
    {
      TraceN("TS_Tick");
      tick_sequential_.compute_and_store();
    }

    // Trigger tick -> minute resampling
    if (tick2min_resampler_.update()) {
      // New minute bar generated
      update_minute_metadata();
      {
        TraceN("TS_Minute");
        minute_sequential_.compute_and_store();
      }

      // Trigger minute -> hour resampling
      if (min2hour_resampler_.update()) {
        // New hour bar generated
        update_hour_metadata();
        {
          TraceN("TS_Hour");
          hour_sequential_.compute_and_store();
        }
      }
    }

    // Mark L0 progress after all levels (L0/L1/L2) computed
    if (!date_str_.empty()) {
      store_.ts_update(date_str_, core_id_, asset_id_, tick_data_.l0_index);
    }
  }

private:
  // Update tick-level metadata
  void inline update_tick_metadata() noexcept {
    tick_data_.l0_index = tick2index(tick_data_.lob.hour, tick_data_.lob.minute, tick_data_.lob.second);
  }

  // Update minute-level metadata
  void inline update_minute_metadata() noexcept {
    minute_data_.l1_index = tick_data_.l0_index / 60;
  }

  // Update hour-level metadata
  void inline update_hour_metadata() noexcept {
    hour_data_.l2_index = minute_data_.l1_index / 60;
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
