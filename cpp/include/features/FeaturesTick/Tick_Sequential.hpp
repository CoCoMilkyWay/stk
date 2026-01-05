#pragma once

#include "features/backend/FeatureStore.hpp"
#include "features/backend/FeatureStoreConfig.hpp"

class DAG; // Forward declaration

// Tick-level sequential feature computation
// 数据结构在 DAG::L0，这里只负责 compute 调度
class Tick_Sequential {
public:
  Tick_Sequential(DAG &dag,
                  GlobalFeatureStore &store,
                  size_t asset_id,
                  size_t worker_id)
      : dag_(dag),
        store_(&store),
        asset_id_(asset_id),
        worker_id_(worker_id) {}

  void set_date(const std::string &date_str);

  // Main computation entry (called by CoreSequential)
  void compute_and_store();

private:
  // Level 0: Tick-level TS features computation
  void compute_ts_tick(size_t t);

  // Write LOB depth snapshot (N levels bid/ask price/volume for GUI)
  void write_lob_depth(size_t t);

  DAG &dag_;
  GlobalFeatureStore *store_ = nullptr;
  size_t asset_id_ = 0;
  size_t worker_id_ = 0;
  std::string date_str_;

  // 输出缓冲区
  float ts_features_buffer_[L0_TS_WIDTH];
  float lob_depth_buffer_[4 * L2::LOB_DEPTH + 2];
};

// 实现需要完整的 DAG 定义
#include "features/ComputeGraph.hpp"

inline void Tick_Sequential::set_date(const std::string &date_str) {
  dag_.reset_for_new_day();
  date_str_ = date_str;
}

inline void Tick_Sequential::compute_and_store() {
  // Compute and write tick-level TS features
  compute_ts_tick(dag_.tick_data.l0_index);

  // Write LOB depth snapshot for GUI (META features)
  write_lob_depth(dag_.tick_data.l0_index);
}

inline void Tick_Sequential::compute_ts_tick(size_t t) {
  // [EVERY TICK] 逐笔更新
  dag_.l0.delta_t.compute();

  if (dag_.tick_data.lob.depth_updated) {
    // =============================================================
    // 数据层: 基础数据提取 (每tick只算一次, 供下游因子复用)
    // =============================================================
    dag_.l0.depth_data.compute();   // N档price/qty → 4N个CBuffer
    dag_.l0.mid_price.compute();    // 中间价
    dag_.l0.micro_price.compute();  // 微观价格
    dag_.l0.spread.compute();       // 买卖价差
    dag_.l0.trade_price.compute();  // 成交价

    // =============================================================
    // 因子层: 从共享CBuffer读取, 无重复计算
    // =============================================================
    // 订单量类因子
    dag_.l0.voi1.compute();     // VOI 1档
    dag_.l0.voi30.compute();    // VOI 30档
    dag_.l0.oir5.compute();     // OIR 5档比率
    dag_.l0.oir10.compute();    // OIR 10档
    dag_.l0.soir5.compute();    // SOIR 5档加权
    dag_.l0.soir5s.compute();   // SOIR 第5档单独
    dag_.l0.soir10s.compute();  // SOIR 第10档单独
    dag_.l0.soir30s.compute();  // SOIR 第30档单独

    // 价格类因子
    dag_.l0.mpb.compute();   // 市价偏离度
    dag_.l0.mpc1.compute();  // 中间价变化率 lag=1
    dag_.l0.mpc5.compute();  // 中间价变化率 lag=5 + 日内max/skew

    // 写入因子到输出缓冲区 (顺序与 LEVEL_0_FIELDS 定义一致)
    ts_features_buffer_[0] = dag_.l0.VOI1.back();
    ts_features_buffer_[1] = dag_.l0.VOI30.back();
    ts_features_buffer_[2] = dag_.l0.OIR5.back();
    ts_features_buffer_[3] = dag_.l0.OIR10.back();
    ts_features_buffer_[4] = dag_.l0.SOIR5.back();
    ts_features_buffer_[5] = dag_.l0.SOIR5s.back();
    ts_features_buffer_[6] = dag_.l0.SOIR10s.back();
    ts_features_buffer_[7] = dag_.l0.SOIR30s.back();
    ts_features_buffer_[8] = dag_.l0.MPB.back();
    ts_features_buffer_[9] = dag_.l0.MPC1.back();
    ts_features_buffer_[10] = dag_.l0.MPC5.back();
    ts_features_buffer_[11] = dag_.l0.MPC5_Max.back();
    ts_features_buffer_[12] = dag_.l0.MPC5_Skew.back();
  }

  // Write TS features [voi1, mpc5_skew]
  TS_WRITE_FEATURES(store_, date_str_, 0, t, asset_id_, 0, L0_FieldOffset::mpc5_skew, ts_features_buffer_, worker_id_);

  // Write data validity flag (event-driven sparsity marker)
  TS_WRITE_SINGLE(store_, date_str_, 0, t, L0_FieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
  DEPTH_WRITE_SINGLE(store_, date_str_, t, DepthFieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
}

inline void Tick_Sequential::write_lob_depth(size_t t) {
  if (!dag_.tick_data.lob.depth_updated)
    return;

  constexpr size_t N = L2::LOB_DEPTH;
  constexpr float VOLUME_TO_LOT = 0.01f; // 股 → 手 (1手=100股)

  for (size_t i = 0; i < N; ++i) {
    lob_depth_buffer_[i] = dag_.l0.BidPrice[i].back();
    lob_depth_buffer_[N + i] = dag_.l0.AskPrice[i].back();
    lob_depth_buffer_[2 * N + i] = dag_.l0.BidQty[i].back() * VOLUME_TO_LOT;
    lob_depth_buffer_[3 * N + i] = dag_.l0.AskQty[i].back() * VOLUME_TO_LOT;
  }

  lob_depth_buffer_[4 * N] = dag_.l0.MidPrice.back();
  lob_depth_buffer_[4 * N + 1] = 1.0f;

  DEPTH_WRITE_FEATURES(store_, date_str_, t, asset_id_, 0, DepthFieldOffset::_depth_valid, lob_depth_buffer_, worker_id_);
}

