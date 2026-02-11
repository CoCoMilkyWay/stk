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

  inline void set_date(const std::string &date_str);

  // Main computation entry (called by CoreSequential)
  inline void compute_and_store();

private:
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
  // 每个算子的compute()和其输入buffer在同一个采样域(trigger)
  // 每个算子的flush()和其输出buffer在同一个采样域(trigger)
  // 具体绑定关系请看DAG向量图

  const size_t t = dag_.tick_data.l0_index;

  bool Trigger0 = true;                                                  // [EVERY TICK] 逐笔更新
  bool Trigger1 = dag_.tick_data.lob.order_type == L2::OrderType::TAKER; // [ON TAKER] 成交时更新
  bool Trigger2 = dag_.tick_data.lob.depth_updated;                      // [ON DEPTH] 盘口更新时触发

  if (Trigger0) {
    dag_.l0.DeltaT.compute();    // input: td
    dag_.l0.DeltaT.flush();      // output: DeltaTMaker_, DeltaTTaker_, DeltaTCancel_
    dag_.l0.TickIndex.compute(); // input: td
    dag_.l0.TickIndex.flush();   // output: Sec_, TickIndex_

    // 订单流累计特征 compute (flush在Trigger2)
    dag_.l0.flow_rate.compute(); // input: td
    dag_.l0.toxic_cr.compute();  // input: td
    dag_.l0.resil.compute();     // input: td, BidQty_, AskQty_
    dag_.l0.ctr.compute();       // input: td
    dag_.l0.behav.compute();     // input: td
    dag_.l0.oa.compute();        // input: td
    dag_.l0.hla.compute();       // input: td, BidQty_, AskQty_
    dag_.l0.toxic.compute();     // input: td, BidQty_, AskQty_

    ts_features_buffer_[L0_FieldOffset::sec] = dag_.l0.Sec_.back();
  }

  if (Trigger1) {
    dag_.l0.TradePrice.compute(); // input: td
    dag_.l0.TradePrice.flush();   // output: TradePrice_
  }

  if (Trigger2) {
    dag_.l0.DepthData.compute();  // input: td
    dag_.l0.DepthData.flush();    // output: BidPrice_, AskPrice_, BidQty_, AskQty_, BidAmt_, AskAmt_

    // --- 基础数据层 ---
    dag_.l0.DepthIndex.compute(); // input: td
    dag_.l0.DepthIndex.flush();   // output: DepthIndex_
    dag_.l0.MidPrice.compute();   // input: BidPrice_[0], AskPrice_[0]
    dag_.l0.MidPrice.flush();     // output: MidPrice_
    dag_.l0.MicroPrice.compute(); // input: td
    dag_.l0.MicroPrice.flush();   // output: MicroPrice_
    dag_.l0.Spread.compute();     // input: BidPrice_[0], AskPrice_[0]
    dag_.l0.Spread.flush();       // output: Spread_

    // --- CI ---
    dag_.l0.ci_1.compute();   // input: BidQty_, AskQty_
    dag_.l0.ci_1.flush();     // output: ci_1_
    dag_.l0.ci_5.compute();   // input: BidQty_, AskQty_
    dag_.l0.ci_5.flush();     // output: ci_5_
    dag_.l0.ci_10.compute();  // input: BidQty_, AskQty_
    dag_.l0.ci_10.flush();    // output: ci_10_
    dag_.l0.ci_30.compute();  // input: BidQty_, AskQty_
    dag_.l0.ci_30.flush();    // output: ci_30_
    dag_.l0.ci_all.compute(); // input: BidQty_, AskQty_
    dag_.l0.ci_all.flush();   // output: ci_all_

    // --- CWI ---
    dag_.l0.cwi_1.compute(); // input: BidQty_, AskQty_
    dag_.l0.cwi_1.flush();   // output: cwi_1_
    dag_.l0.cwi_2.compute(); // input: BidQty_, AskQty_
    dag_.l0.cwi_2.flush();   // output: cwi_2_

    // --- DDI ---
    dag_.l0.ddi_1.compute(); // input: BidQty_, AskQty_, BidPrice_, AskPrice_
    dag_.l0.ddi_1.flush();   // output: ddi_1_
    dag_.l0.ddi_2.compute(); // input: BidQty_, AskQty_, BidPrice_, AskPrice_
    dag_.l0.ddi_2.flush();   // output: ddi_2_

    // --- TLR ---
    dag_.l0.tbr_5.compute(); // input: BidQty_, AskQty_
    dag_.l0.tbr_5.flush();   // output: tbr_5_
    dag_.l0.tar_5.compute(); // input: BidQty_, AskQty_
    dag_.l0.tar_5.flush();   // output: tar_5_

    // --- PARA (Layer 1) ---
    dag_.l0.b_para_c0.compute(); // input: BidQty_, AskQty_
    dag_.l0.b_para_c0.flush();   // output: b_para_c0_
    dag_.l0.b_para_c1.compute(); // input: BidQty_, AskQty_
    dag_.l0.b_para_c1.flush();   // output: b_para_c1_
    dag_.l0.b_para_c2.compute(); // input: BidQty_, AskQty_
    dag_.l0.b_para_c2.flush();   // output: b_para_c2_
    dag_.l0.a_para_c0.compute(); // input: BidQty_, AskQty_
    dag_.l0.a_para_c0.flush();   // output: a_para_c0_
    dag_.l0.a_para_c1.compute(); // input: BidQty_, AskQty_
    dag_.l0.a_para_c1.flush();   // output: a_para_c1_
    dag_.l0.a_para_c2.compute(); // input: BidQty_, AskQty_
    dag_.l0.a_para_c2.flush();   // output: a_para_c2_
    // --- PARA (Layer 2: 失衡, 依赖Layer 1 flush后的CBuffer) ---
    dag_.l0.imba_para_c0.compute(); // input: b_para_c0_, a_para_c0_
    dag_.l0.imba_para_c0.flush();   // output: imba_para_c0_
    dag_.l0.imba_para_c1.compute(); // input: b_para_c1_, a_para_c1_
    dag_.l0.imba_para_c1.flush();   // output: imba_para_c1_
    dag_.l0.imba_para_c2.compute(); // input: b_para_c2_, a_para_c2_
    dag_.l0.imba_para_c2.flush();   // output: imba_para_c2_

    // --- GRAD (Layer 1) ---
    dag_.l0.b_5_c1.compute(); // input: BidQty_, AskQty_
    dag_.l0.b_5_c1.flush();   // output: b_5_c1_
    dag_.l0.a_5_c1.compute(); // input: BidQty_, AskQty_
    dag_.l0.a_5_c1.flush();   // output: a_5_c1_
    // --- GRAD (Layer 2) ---
    dag_.l0.imba_5_c1.compute(); // input: b_5_c1_, a_5_c1_
    dag_.l0.imba_5_c1.flush();   // output: imba_5_c1_

    // --- ENTROPY (Layer 1) ---
    dag_.l0.b_5_entropy.compute();  // input: BidQty_, AskQty_
    dag_.l0.b_5_entropy.flush();    // output: b_5_entropy_
    dag_.l0.a_5_entropy.compute();  // input: BidQty_, AskQty_
    dag_.l0.a_5_entropy.flush();    // output: a_5_entropy_
    dag_.l0.b_30_entropy.compute(); // input: BidQty_, AskQty_
    dag_.l0.b_30_entropy.flush();   // output: b_30_entropy_
    dag_.l0.a_30_entropy.compute(); // input: BidQty_, AskQty_
    dag_.l0.a_30_entropy.flush();   // output: a_30_entropy_
    // --- ENTROPY (Layer 2) ---
    dag_.l0.imba_5_entropy.compute();  // input: b_5_entropy_, a_5_entropy_
    dag_.l0.imba_5_entropy.flush();    // output: imba_5_entropy_
    dag_.l0.imba_30_entropy.compute(); // input: b_30_entropy_, a_30_entropy_
    dag_.l0.imba_30_entropy.flush();   // output: imba_30_entropy_

    // --- OFI ---
    dag_.l0.ofi_1.compute(); // input: BidQty_, AskQty_, BidPrice_, AskPrice_
    dag_.l0.ofi_1.flush();   // output: ofi_1_
    dag_.l0.ofi_5.compute(); // input: BidQty_, AskQty_, BidPrice_, AskPrice_
    dag_.l0.ofi_5.flush();   // output: ofi_5_

    // --- COST ---
    dag_.l0.cost_buy_1.compute();   // input: AskPrice_, AskQty_, MidPrice_
    dag_.l0.cost_buy_1.flush();     // output: cost_buy_1_
    dag_.l0.cost_buy_5.compute();   // input: AskPrice_, AskQty_, MidPrice_
    dag_.l0.cost_buy_5.flush();     // output: cost_buy_5_
    dag_.l0.cost_buy_10.compute();  // input: AskPrice_, AskQty_, MidPrice_
    dag_.l0.cost_buy_10.flush();    // output: cost_buy_10_
    dag_.l0.cost_sell_1.compute();  // input: BidPrice_, BidQty_, MidPrice_
    dag_.l0.cost_sell_1.flush();    // output: cost_sell_1_
    dag_.l0.cost_sell_5.compute();  // input: BidPrice_, BidQty_, MidPrice_
    dag_.l0.cost_sell_5.flush();    // output: cost_sell_5_
    dag_.l0.cost_sell_10.compute(); // input: BidPrice_, BidQty_, MidPrice_
    dag_.l0.cost_sell_10.flush();   // output: cost_sell_10_

    // --- PEAK ---
    dag_.l0.peak_loc_bid.compute();   // input: BidQty_
    dag_.l0.peak_loc_bid.flush();     // output: peak_loc_bid_
    dag_.l0.peak_loc_ask.compute();   // input: AskQty_
    dag_.l0.peak_loc_ask.flush();     // output: peak_loc_ask_
    dag_.l0.peak_ratio_bid.compute(); // input: BidQty_
    dag_.l0.peak_ratio_bid.flush();   // output: peak_ratio_bid_
    dag_.l0.peak_ratio_ask.compute(); // input: AskQty_
    dag_.l0.peak_ratio_ask.flush();   // output: peak_ratio_ask_

    // --- 订单流累计特征 flush (compute在Trigger0, 读取深度后输出) ---
    dag_.l0.flow_rate.flush(); // output: arr_bid_, arr_ask_, can_bid_, can_ask_, trd_buy_, trd_sell_, net_ord_, foi_
    dag_.l0.toxic_cr.flush();  // output: toxic_cr_
    dag_.l0.resil.flush();     // output: ratio_bid_, ratio_ask_, imba_, dev_bid_, dev_ask_, mr_bid_, mr_ask_, recovery_bid_, recovery_ask_
    dag_.l0.ctr.flush();       // output: cc_r_, ctr_xl_, ctr_l_, ctr_m_, ctr_s_, cnbi_, cnbi_xl_, cnbi_l_, cnbi_m_, cnbi_s_, cnbi_am_, cnbi_pm_
    dag_.l0.behav.flush();     // output: agg_buy_, agg_sell_, agg_dif_, cpr_, agg_trd_, ord_size_
    dag_.l0.oa.flush();        // output: oa_bcr_, oa_acr_, oa_btr_, oa_atr_
    dag_.l0.hla.flush();       // output: hla_imba_
    dag_.l0.toxic.flush();     // output: ptc_rt_, fleet_rt_, spoof_int_, stale_ratio_bid_, stale_ratio_ask_

    // --- LABEL ---
    dag_.l0.next_tick_ret.compute(); // input: MidPrice_
    dag_.l0.next_tick_ret.flush();   // output: next_tick_ret_

    // --- REPRE ---
    dag_.l0.depth_repre.compute(); // input: (none)
    dag_.l0.depth_repre.flush();   // output: depth_repre_

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

    // --- Write LOB depth snapshot for GUI ---
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

  // Write TS features
  TS_WRITE_FEATURES(store_, date_str_, 0, t, asset_id_, 0, L0_FieldOffset::next_tick_ret, ts_features_buffer_.data(), worker_id_);

  // Write data validity flag
  TS_WRITE_SINGLE(store_, date_str_, 0, t, L0_FieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
  DEPTH_WRITE_SINGLE(store_, date_str_, t, DepthFieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
}
