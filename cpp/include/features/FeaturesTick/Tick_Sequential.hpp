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
  // =========================================================================
  // [EVERY TICK] 逐笔更新 - 每个订单(增/删/改/成交)都触发
  // =========================================================================
  dag_.l0.DeltaT.compute();
  dag_.l0.TickIndex.compute();
  ts_features_buffer_[L0_FieldOffset::sec] = dag_.l0.Sec_.back();

  // =========================================================================
  // [ON TAKER] 成交时更新 - order_type == TAKER 时触发
  // =========================================================================
  if (dag_.tick_data.lob.order_type == L2::OrderType::TAKER) {
    dag_.l0.TradePrice.compute();
  }

  // =========================================================================
  // [ON DEPTH] 盘口更新时触发
  // =========================================================================
  if (dag_.tick_data.lob.depth_updated) {
    // --- 数据层 ---
    dag_.l0.DepthIndex.compute();
    dag_.l0.DepthData.compute();
    dag_.l0.MidPrice.compute();
    dag_.l0.MicroPrice.compute();
    dag_.l0.Spread.compute();

    // --- CI: Cumulative Imbalance ---
    dag_.l0.ci_1.compute();
    dag_.l0.ci_5.compute();
    dag_.l0.ci_10.compute();
    dag_.l0.ci_30.compute();
    dag_.l0.ci_all.compute();

    // --- CWI: Convexity-Weighted Imbalance ---
    dag_.l0.cwi_1.compute();
    dag_.l0.cwi_2.compute();

    // --- DDI: Distance-Discounted Imbalance ---
    dag_.l0.ddi_1.compute();
    dag_.l0.ddi_2.compute();

    // --- TLR: Top Level Ratio ---
    dag_.l0.tbr_5.compute();
    dag_.l0.tar_5.compute();

    // --- PARA: Parabola Fit (Layer 1: 买卖两侧) ---
    dag_.l0.b_para_c0.compute();
    dag_.l0.b_para_c1.compute();
    dag_.l0.b_para_c2.compute();
    dag_.l0.a_para_c0.compute();
    dag_.l0.a_para_c1.compute();
    dag_.l0.a_para_c2.compute();
    // --- PARA: Parabola Fit (Layer 2: 失衡) ---
    dag_.l0.imba_para_c0.compute();
    dag_.l0.imba_para_c1.compute();
    dag_.l0.imba_para_c2.compute();

    // --- GRAD: Gradient (Layer 1) ---
    dag_.l0.b_5_c1.compute();
    dag_.l0.a_5_c1.compute();
    // --- GRAD: Gradient (Layer 2: 失衡) ---
    dag_.l0.imba_5_c1.compute();

    // --- ENTROPY: Shannon Entropy (Layer 1) ---
    dag_.l0.b_5_entropy.compute();
    dag_.l0.a_5_entropy.compute();
    dag_.l0.b_30_entropy.compute();
    dag_.l0.a_30_entropy.compute();
    // --- ENTROPY: Shannon Entropy (Layer 2: 失衡) ---
    dag_.l0.imba_5_entropy.compute();
    dag_.l0.imba_30_entropy.compute();

    // --- OFI: Order Flow Imbalance ---
    dag_.l0.ofi_1.compute();
    dag_.l0.ofi_5.compute();

    // --- 写入缓冲区 (用 L0_FieldOffset 索引) ---
    ts_features_buffer_[L0_FieldOffset::ci_1] = dag_.l0.ci_1_.back();
    ts_features_buffer_[L0_FieldOffset::ci_5] = dag_.l0.ci_5_.back();
    ts_features_buffer_[L0_FieldOffset::ci_10] = dag_.l0.ci_10_.back();
    ts_features_buffer_[L0_FieldOffset::ci_30] = dag_.l0.ci_30_.back();
    ts_features_buffer_[L0_FieldOffset::ci_all] = dag_.l0.ci_all_.back();
    ts_features_buffer_[L0_FieldOffset::cwi_1] = dag_.l0.cwi_1_.back();
    ts_features_buffer_[L0_FieldOffset::cwi_2] = dag_.l0.cwi_2_.back();
    ts_features_buffer_[L0_FieldOffset::ddi_1] = dag_.l0.ddi_1_.back();
    ts_features_buffer_[L0_FieldOffset::ddi_2] = dag_.l0.ddi_2_.back();
    ts_features_buffer_[L0_FieldOffset::tbr_5] = dag_.l0.tbr_5_.back();
    ts_features_buffer_[L0_FieldOffset::tar_5] = dag_.l0.tar_5_.back();
    ts_features_buffer_[L0_FieldOffset::b_para_c0] = dag_.l0.b_para_c0_.back();
    ts_features_buffer_[L0_FieldOffset::b_para_c1] = dag_.l0.b_para_c1_.back();
    ts_features_buffer_[L0_FieldOffset::b_para_c2] = dag_.l0.b_para_c2_.back();
    ts_features_buffer_[L0_FieldOffset::a_para_c0] = dag_.l0.a_para_c0_.back();
    ts_features_buffer_[L0_FieldOffset::a_para_c1] = dag_.l0.a_para_c1_.back();
    ts_features_buffer_[L0_FieldOffset::a_para_c2] = dag_.l0.a_para_c2_.back();
    ts_features_buffer_[L0_FieldOffset::imba_para_c0] = dag_.l0.imba_para_c0_.back();
    ts_features_buffer_[L0_FieldOffset::imba_para_c1] = dag_.l0.imba_para_c1_.back();
    ts_features_buffer_[L0_FieldOffset::imba_para_c2] = dag_.l0.imba_para_c2_.back();
    ts_features_buffer_[L0_FieldOffset::b_5_c1] = dag_.l0.b_5_c1_.back();
    ts_features_buffer_[L0_FieldOffset::a_5_c1] = dag_.l0.a_5_c1_.back();
    ts_features_buffer_[L0_FieldOffset::imba_5_c1] = dag_.l0.imba_5_c1_.back();
    ts_features_buffer_[L0_FieldOffset::b_5_entropy] = dag_.l0.b_5_entropy_.back();
    ts_features_buffer_[L0_FieldOffset::a_5_entropy] = dag_.l0.a_5_entropy_.back();
    ts_features_buffer_[L0_FieldOffset::imba_5_entropy] = dag_.l0.imba_5_entropy_.back();
    ts_features_buffer_[L0_FieldOffset::b_30_entropy] = dag_.l0.b_30_entropy_.back();
    ts_features_buffer_[L0_FieldOffset::a_30_entropy] = dag_.l0.a_30_entropy_.back();
    ts_features_buffer_[L0_FieldOffset::imba_30_entropy] = dag_.l0.imba_30_entropy_.back();
    ts_features_buffer_[L0_FieldOffset::ofi_1] = dag_.l0.ofi_1_.back();
    ts_features_buffer_[L0_FieldOffset::ofi_5] = dag_.l0.ofi_5_.back();

    TS_WRITE_SINGLE(store_, date_str_, 0, t, L0_FieldOffset::_depth_valid, asset_id_, 1.0f, worker_id_);
  }

  // Write TS features
  TS_WRITE_FEATURES(store_, date_str_, 0, t, asset_id_, 0, L0_FieldOffset::imba_30_entropy, ts_features_buffer_.data(), worker_id_);

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
