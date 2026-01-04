#pragma once

#include "misc/profiler.hpp"
#include "features/ComputeGraph.hpp"
#include "features/FeaturesTick/Tick_Sequential.hpp"
#include "features/FeaturesMinute/Minute_Sequential.hpp"
#include "features/FeaturesHour/Hour_Sequential.hpp"
#include "features/backend/FeatureStore.hpp"
#include "math/sample/ResampleTick2Time.hpp"
#include "math/sample/ResampleTime2Time.hpp"

// Sequential Core: Hierarchical 3-level feature computation with resampling
// Architecture: LOB -> Tick -> (resample) -> Minute -> (resample) -> Hour
// 数据结构和算子在 DAG 中，各级 Sequential 负责 trigger
class CoreSequential {
public:
  CoreSequential(TickData &tick_data,
                 GlobalFeatureStore &store,
                 size_t asset_id = 0,
                 size_t core_id = 0)
      : store_(store),
        asset_id_(asset_id),
        core_id_(core_id),
        dag_(tick_data),
        tick_sequential_(dag_, store_, asset_id_, core_id_),
        minute_sequential_(dag_, store_, asset_id_, core_id_),
        hour_sequential_(dag_, store_, asset_id_, core_id_),
        tick2min_resampler_(dag_.tick_data, dag_.minute_data, 60),
        min2hour_resampler_(dag_.minute_data, dag_.hour_data, 60) {
    // Initialize metadata
    dag_.tick_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.minute_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.hour_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.tick_data.core_id = static_cast<uint32_t>(core_id);
    dag_.minute_data.core_id = static_cast<uint32_t>(core_id);
    dag_.hour_data.core_id = static_cast<uint32_t>(core_id);
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
    dag_.minute_data.open.clear();
    dag_.minute_data.high.clear();
    dag_.minute_data.low.clear();
    dag_.minute_data.close.clear();
    dag_.minute_data.bid_volume.clear();
    dag_.minute_data.ask_volume.clear();
    dag_.minute_data.bid_amount.clear();
    dag_.minute_data.ask_amount.clear();
    dag_.hour_data.open.clear();
    dag_.hour_data.high.clear();
    dag_.hour_data.low.clear();
    dag_.hour_data.close.clear();
    dag_.hour_data.bid_volume.clear();
    dag_.hour_data.ask_volume.clear();
    dag_.hour_data.bid_amount.clear();
    dag_.hour_data.ask_amount.clear();
  }

  // Main entry: compute all 3 levels with cascading resampling
  void compute_and_store() noexcept {
    TraceN("TS");
    TraceColor(C_Cyan);

    // LEVEL 0: Tick-level features
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
      store_.ts_update(date_str_, core_id_, asset_id_, dag_.tick_data.l0_index);
    }
  }

private:
  // Update tick-level metadata
  void inline update_tick_metadata() noexcept {
    dag_.tick_data.l0_index = tick2index(dag_.tick_data.lob.hour, dag_.tick_data.lob.minute, dag_.tick_data.lob.second);
  }

  // Update minute-level metadata
  void inline update_minute_metadata() noexcept {
    dag_.minute_data.l1_index = dag_.tick_data.l0_index / 60;
  }

  // Update hour-level metadata
  void inline update_hour_metadata() noexcept {
    dag_.hour_data.l2_index = dag_.minute_data.l1_index / 60;
  }

  GlobalFeatureStore &store_;
  size_t asset_id_;
  size_t core_id_;
  std::string date_str_;

  // DAG: 数据结构和算子
  DAG dag_;

  // Sequential feature processors (各级调度)
  Tick_Sequential tick_sequential_;
  Minute_Sequential minute_sequential_;
  Hour_Sequential hour_sequential_;

  // Resamplers
  ResampleTick2Time tick2min_resampler_;
  ResampleTime2Time min2hour_resampler_;
};
