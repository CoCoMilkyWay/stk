#pragma once

#include "features/backend/FeatureStore.hpp"
#include "features/backend/FeatureStoreConfig.hpp"
#include <array>

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
  std::array<float, L0_TS_WIDTH> ts_features_buffer_;
  std::array<float, 4 * L2::LOB_DEPTH + 2> lob_depth_buffer_;
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
  dag_.l0.DeltaT.compute();

  if (dag_.tick_data.lob.depth_updated) {
    // =============================================================
    // 数据层: 基础数据提取 (每tick只算一次, 供下游因子复用)
    // =============================================================
    dag_.l0.TickIndex.compute();  // 秒级索引 (供时间滤波器使用)
    dag_.l0.DepthData.compute();  // N档price/qty → 4N个CBuffer
    dag_.l0.MidPrice.compute();   // 中间价
    dag_.l0.MicroPrice.compute(); // 微观价格
    dag_.l0.Spread.compute();     // 买卖价差
    dag_.l0.TradePrice.compute(); // 成交价

    // =============================================================
    // 因子层: 从共享CBuffer读取, 无重复计算
    // =============================================================
    // 订单量类因子
    dag_.l0.VOI1.compute();    // VOI 1档
    dag_.l0.VOI30.compute();   // VOI 30档
    dag_.l0.OIR5.compute();    // OIR 5档比率
    dag_.l0.OIR10.compute();   // OIR 10档
    dag_.l0.SOIR5.compute();   // SOIR 5档加权
    dag_.l0.SOIR5s.compute();  // SOIR 第5档单独
    dag_.l0.SOIR10s.compute(); // SOIR 第10档单独
    dag_.l0.SOIR30s.compute(); // SOIR 第30档单独

    // 价格类因子
    dag_.l0.MPB.compute();  // 市价偏离度
    dag_.l0.MPC1.compute(); // 中间价变化率 lag=1
    dag_.l0.MPC5.compute(); // 中间价变化率 lag=5 + 日内max/skew

    // 写入因子到输出缓冲区 (顺序与 LEVEL_0_FIELDS 定义一致)
    ts_features_buffer_ = {
        dag_.l0.Sec_.back(),
        dag_.l0.VOI1_.back(),
        dag_.l0.VOI30_.back(),
        dag_.l0.OIR5_.back(),
        dag_.l0.OIR10_.back(),
        dag_.l0.SOIR5_.back(),
        dag_.l0.SOIR5s_.back(),
        dag_.l0.SOIR10s_.back(),
        dag_.l0.SOIR30s_.back(),
        dag_.l0.MPB_.back(),
        dag_.l0.MPC1_.back(),
        dag_.l0.MPC5_.back(),
        dag_.l0.MPC5_Max_.back(),
        dag_.l0.MPC5_Skew_.back()};
  }

  // Write TS features [sec, mpc5_skew]
  TS_WRITE_FEATURES(store_, date_str_, 0, t, asset_id_, 0, L0_FieldOffset::mpc5_skew, ts_features_buffer_.data(), worker_id_);

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
    lob_depth_buffer_[i] = dag_.l0.BidPrice_[i].back();
    lob_depth_buffer_[N + i] = dag_.l0.AskPrice_[i].back();
    lob_depth_buffer_[2 * N + i] = dag_.l0.BidQty_[i].back() * VOLUME_TO_LOT;
    lob_depth_buffer_[3 * N + i] = dag_.l0.AskQty_[i].back() * VOLUME_TO_LOT;
  }

  lob_depth_buffer_[4 * N] = dag_.l0.MidPrice_.back();
  lob_depth_buffer_[4 * N + 1] = 1.0f;

  DEPTH_WRITE_FEATURES(store_, date_str_, t, asset_id_, 0, DepthFieldOffset::_depth_valid, lob_depth_buffer_.data(), worker_id_);
}
