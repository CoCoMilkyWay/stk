#pragma once

#include "features/FeaturesMinute/Minute_Crosssection.hpp"
#include "features/FeaturesTick/Tick_Crosssection.hpp"
#include "features/backend/FeatureStore.hpp"
#include "misc/profiler.hpp"

// ============================================================================
// CoreCrosssection: Hierarchical 2-level cross-sectional feature computation
// Architecture: Tick -> (cascade) -> Minute
// ============================================================================

class CoreCrosssection {
public:
  CoreCrosssection(GlobalFeatureStore &store)
      : store_(store),
        A_(store.query_A()),
        valid_indices_(),
        input_fp32_(A_),
        output_fp32_(A_),
        output_fp16_(A_),
        tick_cs_(store, valid_indices_, input_fp32_, output_fp32_, output_fp16_),
        minute_cs_(store, valid_indices_, input_fp32_, output_fp32_, output_fp16_) {
    valid_indices_.reserve(A_);
  }

  void set_date(const std::string &date_str) {
    date_str_ = date_str;
    tick_cs_.set_date(date_str);
    minute_cs_.set_date(date_str);
  }

  // Main entry: compute all 2 levels with cascading on time boundaries
  void compute_and_store(size_t t) noexcept {
    TraceN("CS");
    TraceColor(C_Magenta);

    // LEVEL 0: Tick-level features
    {
      TraceN("CS_Tick");
      tick_cs_.compute_and_store(t);
    }

    // Cascade: If this tick crosses minute boundary, trigger minute computation
    size_t t_minute = t / 60;
    if (t % 60 == 0 && t > 0) {
      TraceN("CS_Minute");
      minute_cs_.compute_and_store(t_minute);
    }
  }

private:
  GlobalFeatureStore &store_;
  std::string date_str_;
  size_t A_;

  // Shared buffers (avoid per-timeslot allocation)
  std::vector<size_t> valid_indices_;
  std::vector<float> input_fp32_;
  std::vector<float> output_fp32_;
  std::vector<_Float16> output_fp16_;

  // Crosssection feature processors
  Tick_Crosssection tick_cs_;
  Minute_Crosssection minute_cs_;
};
