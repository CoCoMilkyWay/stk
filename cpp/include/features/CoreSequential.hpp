#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/ComputeGraph.hpp"
#include "features/FeaturesMinute/Minute_Sequential.hpp"
#include "features/FeaturesTick/Tick_Sequential.hpp"
#include "math/sample/ResamplerTick2Min.hpp"
#include "misc/profiler.hpp"


// Sequential Core: Hierarchical 2-level feature computation with resampling
// Architecture: LOB -> Tick -> (resample) -> Minute
class CoreSequential {
public:
  CoreSequential(TickData &tick_data,
                 GlobalFeatureStore &store,
                 const std::string &asset_code,
                 size_t asset_id = 0,
                 size_t core_id = 0)
      : store_(store),
        asset_id_(asset_id),
        core_id_(core_id),
        asset_code_(asset_code),
        dag_(tick_data, asset_code),
        tick_sequential_(dag_, store_, asset_id_, core_id_),
        minute_sequential_(dag_, store_, asset_id_, core_id_),
        tick2min_(dag_.tick_data, dag_.minute_data) {
    dag_.tick_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.minute_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.tick_data.core_id = static_cast<uint32_t>(core_id);
    dag_.minute_data.core_id = static_cast<uint32_t>(core_id);
  }

  void begin_day(const std::string &date_str) {
    date_str_ = date_str;
    tick_sequential_.set_date(date_str);
    minute_sequential_.set_date(date_str);
  }

  void end_day() {
    dag_.at_day_end();
  }

  void reset() {
    tick2min_.reset();
    dag_.minute_data.open.clear();
    dag_.minute_data.high.clear();
    dag_.minute_data.low.clear();
    dag_.minute_data.close.clear();
    dag_.minute_data.bid_volume.clear();
    dag_.minute_data.ask_volume.clear();
    dag_.minute_data.bid_amount.clear();
    dag_.minute_data.ask_amount.clear();
  }

  void compute_and_store() noexcept {
    TraceN("TS");
    TraceColor(C_Cyan);

    // LEVEL 0: Tick
    dag_.tick_data.l0_index = static_cast<uint32_t>(Clock_to_L0(dag_.tick_data.lob.hour, dag_.tick_data.lob.minute, dag_.tick_data.lob.second));
    {
      TraceN("TS_Tick");
      tick_sequential_.compute_and_store();
    }

    // Tick -> Minute
    if (tick2min_.update()) {
      TraceN("TS_Minute");
      minute_sequential_.compute_and_store();
    }

    if (!date_str_.empty()) {
      store_.ts_update(date_str_, core_id_, asset_id_, dag_.tick_data.l0_index);
    }
  }

private:
  GlobalFeatureStore &store_;
  size_t asset_id_;
  size_t core_id_;
  std::string asset_code_;
  std::string date_str_;

  DAG dag_;

  Tick_Sequential tick_sequential_;
  Minute_Sequential minute_sequential_;

  ResamplerTick2Min tick2min_;
};
