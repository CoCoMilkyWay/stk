#pragma once

#include "features/FeaturesDefine.hpp"
#include "features/backend/FeatureStore.hpp"
#include "lob/LimitOrderBookDefine.hpp"

// Tick-level sequential feature computation
// Input: LOB_Feature from LimitOrderBook
class Tick_Sequential {
public:
  explicit Tick_Sequential(const LOB_Feature *lob_feature,
                           GlobalFeatureStore *store = nullptr,
                           size_t asset_id = 0,
                           size_t worker_id = 0)
      : lob_feature_(lob_feature),
        feature_store_(store),
        asset_id_(asset_id),
        worker_id_(worker_id) {
  }

  void set_store_context(GlobalFeatureStore *store, size_t asset_id, size_t worker_id) {
    feature_store_ = store;
    asset_id_ = asset_id;
    worker_id_ = worker_id;
  }

  void set_date(const std::string &date_str) {
    date_str_ = date_str;
  }

  // Main computation entry
  void compute_and_store() {
    if (!feature_store_ || date_str_.empty())
      return;

    const LOB_Feature &lob = *lob_feature_;
    size_t t = time_to_trading_seconds(lob.hour, lob.minute, lob.second);

    // Check if this asset is active (has valid LOB data)
    bool is_valid = check_lob_valid(lob);

    // Allocate feature array (only TS features)
    float features[L0_TS_COUNT];

    if (!is_valid) {
      // Asset inactive: write zeros
      std::memset(features, 0, sizeof(features));
    } else {
      // Compute TS features
      features[0] = compute_tick_ret_z();
      features[1] = compute_tobi_osc();
      features[2] = compute_micro_gap_norm();
      features[3] = compute_spread_momentum();
      features[4] = compute_signed_volume_imb();
    }

    // Write TS features
    constexpr size_t level_idx = 0;
    TS_WRITE_FEATURES(feature_store_, date_str_, level_idx, t, asset_id_, L0_TS_START, L0_TS_END, features, worker_id_);

    // Write asset validity flag (business logic, not backend requirement)
    TS_WRITE_SINGLE(feature_store_, date_str_, level_idx, t, L0_FieldOffset::asset_valid, asset_id_, is_valid ? 1.0f : 0.0f, worker_id_);

    // Note: mark_ts_core_done is now called in CoreSequential after all levels computed
  }

private:
  // Check if LOB has valid data
  bool check_lob_valid(const LOB_Feature &lob) const {
    return lob.price > 0 && lob.depth_buffer.size() >= 2 * LOB_FEATURE_DEPTH_LEVELS;
  }

  // Placeholder implementations (TODO: implement actual logic)
  float compute_tick_ret_z() const { return 0.0f; }
  float compute_tobi_osc() const { return 0.0f; }
  float compute_micro_gap_norm() const { return 0.0f; }
  float compute_spread_momentum() const { return 0.0f; }
  float compute_signed_volume_imb() const { return 0.0f; }

  const LOB_Feature *lob_feature_;
  GlobalFeatureStore *feature_store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;
};
