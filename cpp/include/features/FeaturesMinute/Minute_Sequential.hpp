#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/Backend/FeatureStoreConfig.hpp"
#include <cstdint>

class DAG; // Forward declaration

// Minute-level sequential feature computation
// 数据结构在 DAG::L1，这里只负责 compute 调度
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

  inline void set_date(const std::string &date_str);

  // Main computation entry (called by CoreSequential)
  inline void compute_and_store();

private:
  DAG &dag_;
  GlobalFeatureStore *store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;
};

// 实现需要完整的 DAG 定义
#include "features/ComputeGraph.hpp"

inline void Minute_Sequential::set_date(const std::string &date_str) {
  date_str_ = date_str;
}

inline void Minute_Sequential::compute_and_store() {
  // 每个算子的compute()和其输入buffer在同一个采样域(Trigger), 原则上支持多个采样域
  // 每个算子的flush()和其输出buffer在同一个采样域(Trigger), 原则上支持多个采样域
  // 具体绑定关系请看DAG向量图

  if (dag_.minute_data.close.empty()) [[unlikely]]
    return;

  const size_t t = dag_.minute_data.l1_index;
  const float close = dag_.minute_data.close.back();
  const uint32_t bid_vol = dag_.minute_data.bid_volume.back();
  const uint32_t ask_vol = dag_.minute_data.ask_volume.back();
  const uint32_t total_volume = bid_vol + ask_vol;

  const bool Trigger_onMinute = (close > 0 && total_volume > 0); // [ON MINUTE] 有效分钟数据时更新

  if (Trigger_onMinute) {
    // 节点调度全部由 NODES 表展开 (行序 = 执行序): 采样型 compute+flush, 降频型只 flush
    dag_.run<Trigger::onMinute>();

    // Write TS features: 字段表 SRC 列驱动 (OP → 节点输出口最新值, FUND → 当日基本面行; CS 列由 CS worker 写)
    TS_WRITE_ROW(store_, date_str_, 1, t, asset_id_, dag_, worker_id_);
  }

  // Write data validity flag
  TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_data_valid, asset_id_, Trigger_onMinute ? 1.0f : 0.0f, worker_id_);

  // Write OHLC + volume META for GUI (OrderFlow visualization)
  if (Trigger_onMinute) {
    const float open = dag_.minute_data.open.back();
    const float high = dag_.minute_data.high.back();
    const float low = dag_.minute_data.low.back();
    const float volume = static_cast<float>(bid_vol + ask_vol);

    TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_ohlc_open, asset_id_, open * 100.0f, worker_id_);
    TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_ohlc_high, asset_id_, high * 100.0f, worker_id_);
    TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_ohlc_low, asset_id_, low * 100.0f, worker_id_);
    TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_ohlc_close, asset_id_, close * 100.0f, worker_id_);
    TS_WRITE_SINGLE(store_, date_str_, 1, t, L1_FieldOffset::_ohlc_volume, asset_id_, volume, worker_id_);
  }
}
