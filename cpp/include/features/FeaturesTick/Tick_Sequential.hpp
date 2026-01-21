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

  // --- FLOW_RATE: 累计订单流统计 ---
  dag_.l0.flow_acc.accumulate();
  if (dag_.tick_data.lob.order_type == L2::OrderType::CANCEL) {
    dag_.l0.toxic_cr.accumulate(1.0f); // 撤单计数
  }

  // --- RESIL: 累计韧性统计 ---
  dag_.l0.resil_acc.accumulate();

  // --- CTR: 累计成交比率统计 ---
  dag_.l0.ctr_acc.accumulate();

  // --- BEHAV: 行为特征统计 ---
  dag_.l0.behav_acc.accumulate();

  // --- OA: 集合竞价统计 ---
  dag_.l0.oa_acc.accumulate();

  // --- HLA: 潜在流动性统计 ---
  dag_.l0.hla_acc.accumulate();

  // --- TOXIC: 毒性特征统计 ---
  dag_.l0.toxic_acc.accumulate();

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

    // --- COST: Impact Cost ---
    dag_.l0.cost_buy_1.compute();
    dag_.l0.cost_buy_5.compute();
    dag_.l0.cost_buy_10.compute();
    dag_.l0.cost_sell_1.compute();
    dag_.l0.cost_sell_5.compute();
    dag_.l0.cost_sell_10.compute();

    // --- PEAK: Peak Location & Concentration ---
    dag_.l0.peak_loc_bid.compute();
    dag_.l0.peak_loc_ask.compute();
    dag_.l0.peak_ratio_bid.compute();
    dag_.l0.peak_ratio_ask.compute();

    // --- FLOW_RATE: 输出订单流统计 (每秒flush) ---
    dag_.l0.flow_acc.flush();
    dag_.l0.toxic_cr.flush();

    // --- RESIL: 输出韧性统计 (每秒flush) ---
    dag_.l0.resil_acc.flush();

    // --- CTR: 输出累计成交比率 (每秒flush) ---
    dag_.l0.ctr_acc.flush();

    // --- BEHAV: 输出行为特征 (每秒flush) ---
    dag_.l0.behav_acc.flush();

    // --- OA: 输出集合竞价特征 (每秒flush) ---
    dag_.l0.oa_acc.flush();

    // --- HLA: 输出潜在流动性特征 (每秒flush) ---
    dag_.l0.hla_acc.flush();

    // --- TOXIC: 输出毒性特征 (每秒flush) ---
    dag_.l0.toxic_acc.flush();

    // --- LABEL: 输出标签 ---
    dag_.l0.next_tick_ret.compute();

    // --- REPRE: 输出深度表征 (DUMMY) ---
    dag_.l0.depth_repre.compute();

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
    ts_features_buffer_[L0_FieldOffset::cost_buy_1] = dag_.l0.cost_buy_1_.back();
    ts_features_buffer_[L0_FieldOffset::cost_buy_5] = dag_.l0.cost_buy_5_.back();
    ts_features_buffer_[L0_FieldOffset::cost_buy_10] = dag_.l0.cost_buy_10_.back();
    ts_features_buffer_[L0_FieldOffset::cost_sell_1] = dag_.l0.cost_sell_1_.back();
    ts_features_buffer_[L0_FieldOffset::cost_sell_5] = dag_.l0.cost_sell_5_.back();
    ts_features_buffer_[L0_FieldOffset::cost_sell_10] = dag_.l0.cost_sell_10_.back();
    ts_features_buffer_[L0_FieldOffset::peak_loc_bid] = dag_.l0.peak_loc_bid_.back();
    ts_features_buffer_[L0_FieldOffset::peak_loc_ask] = dag_.l0.peak_loc_ask_.back();
    ts_features_buffer_[L0_FieldOffset::peak_ratio_bid] = dag_.l0.peak_ratio_bid_.back();
    ts_features_buffer_[L0_FieldOffset::peak_ratio_ask] = dag_.l0.peak_ratio_ask_.back();
    ts_features_buffer_[L0_FieldOffset::toxic_cr] = dag_.l0.toxic_cr_.back();
    ts_features_buffer_[L0_FieldOffset::arr_bid] = dag_.l0.arr_bid_.back();
    ts_features_buffer_[L0_FieldOffset::arr_ask] = dag_.l0.arr_ask_.back();
    ts_features_buffer_[L0_FieldOffset::can_bid] = dag_.l0.can_bid_.back();
    ts_features_buffer_[L0_FieldOffset::can_ask] = dag_.l0.can_ask_.back();
    ts_features_buffer_[L0_FieldOffset::trd_buy] = dag_.l0.trd_buy_.back();
    ts_features_buffer_[L0_FieldOffset::trd_sell] = dag_.l0.trd_sell_.back();
    ts_features_buffer_[L0_FieldOffset::net_ord] = dag_.l0.net_ord_.back();
    ts_features_buffer_[L0_FieldOffset::foi] = dag_.l0.foi_.back();
    ts_features_buffer_[L0_FieldOffset::ratio_bid] = dag_.l0.ratio_bid_.back();
    ts_features_buffer_[L0_FieldOffset::ratio_ask] = dag_.l0.ratio_ask_.back();
    ts_features_buffer_[L0_FieldOffset::imba] = dag_.l0.imba_.back();
    ts_features_buffer_[L0_FieldOffset::dev_bid] = dag_.l0.dev_bid_.back();
    ts_features_buffer_[L0_FieldOffset::dev_ask] = dag_.l0.dev_ask_.back();
    ts_features_buffer_[L0_FieldOffset::mr_bid] = dag_.l0.mr_bid_.back();
    ts_features_buffer_[L0_FieldOffset::mr_ask] = dag_.l0.mr_ask_.back();
    ts_features_buffer_[L0_FieldOffset::recovery_bid] = dag_.l0.recovery_bid_.back();
    ts_features_buffer_[L0_FieldOffset::recovery_ask] = dag_.l0.recovery_ask_.back();
    ts_features_buffer_[L0_FieldOffset::cc_r] = dag_.l0.cc_r_.back();
    ts_features_buffer_[L0_FieldOffset::ctr_xl] = dag_.l0.ctr_xl_.back();
    ts_features_buffer_[L0_FieldOffset::ctr_l] = dag_.l0.ctr_l_.back();
    ts_features_buffer_[L0_FieldOffset::ctr_m] = dag_.l0.ctr_m_.back();
    ts_features_buffer_[L0_FieldOffset::ctr_s] = dag_.l0.ctr_s_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi] = dag_.l0.cnbi_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_xl] = dag_.l0.cnbi_xl_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_l] = dag_.l0.cnbi_l_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_m] = dag_.l0.cnbi_m_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_s] = dag_.l0.cnbi_s_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_am] = dag_.l0.cnbi_am_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_pm] = dag_.l0.cnbi_pm_.back();
    ts_features_buffer_[L0_FieldOffset::agg_buy] = dag_.l0.agg_buy_.back();
    ts_features_buffer_[L0_FieldOffset::agg_sell] = dag_.l0.agg_sell_.back();
    ts_features_buffer_[L0_FieldOffset::agg_dif] = dag_.l0.agg_dif_.back();
    ts_features_buffer_[L0_FieldOffset::cpr] = dag_.l0.cpr_.back();
    ts_features_buffer_[L0_FieldOffset::agg_trd] = dag_.l0.agg_trd_.back();
    ts_features_buffer_[L0_FieldOffset::ord_size] = dag_.l0.ord_size_.back();
    ts_features_buffer_[L0_FieldOffset::oa_bcr] = dag_.l0.oa_bcr_.back();
    ts_features_buffer_[L0_FieldOffset::oa_acr] = dag_.l0.oa_acr_.back();
    ts_features_buffer_[L0_FieldOffset::oa_btr] = dag_.l0.oa_btr_.back();
    ts_features_buffer_[L0_FieldOffset::oa_atr] = dag_.l0.oa_atr_.back();
    ts_features_buffer_[L0_FieldOffset::hla_imba] = dag_.l0.hla_imba_.back();
    ts_features_buffer_[L0_FieldOffset::ptc_rt] = dag_.l0.ptc_rt_.back();
    ts_features_buffer_[L0_FieldOffset::fleet_rt] = dag_.l0.fleet_rt_.back();
    ts_features_buffer_[L0_FieldOffset::spoof_int] = dag_.l0.spoof_int_.back();
    ts_features_buffer_[L0_FieldOffset::stale_ratio_bid] = dag_.l0.stale_ratio_bid_.back();
    ts_features_buffer_[L0_FieldOffset::stale_ratio_ask] = dag_.l0.stale_ratio_ask_.back();
    ts_features_buffer_[L0_FieldOffset::depth_repre] = dag_.l0.depth_repre_.back();
    ts_features_buffer_[L0_FieldOffset::next_tick_ret] = dag_.l0.next_tick_ret_.back();

    TS_WRITE_SINGLE(store_, date_str_, 0, t, L0_FieldOffset::_depth_valid, asset_id_, 1.0f, worker_id_);
  }

  // Write TS features (全部特征一次性写入, 范围到最后一个已实现的特征)
  TS_WRITE_FEATURES(store_, date_str_, 0, t, asset_id_, 0, L0_FieldOffset::next_tick_ret, ts_features_buffer_.data(), worker_id_);

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
