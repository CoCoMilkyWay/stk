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
  dag_.reset_at_day_start();
  date_str_ = date_str;
}

inline void Tick_Sequential::compute_and_store() {
  // 每个算子的compute()和其输入buffer在同一个采样域(Trigger), 原则上支持多个采样域
  // 每个算子的flush()和其输出buffer在同一个采样域(Trigger), 原则上支持多个采样域
  // 具体绑定关系请看DAG向量图

  const size_t t = dag_.tick_data.l0_index;
  const auto &lob = dag_.tick_data.lob;
  const auto order_type = lob.order_type;

  const bool Trigger_onTick = true;                                                     // [ON TICK]   逐笔更新
  const bool Trigger_onTaker [[maybe_unused]] = (order_type == L2::OrderType::TAKER);   // [ON TAKER]  成交时更新
  const bool Trigger_onMaker [[maybe_unused]] = (order_type == L2::OrderType::MAKER);   // [ON MAKER]  挂单时更新
  const bool Trigger_onCancel [[maybe_unused]] = (order_type == L2::OrderType::CANCEL); // [ON CANCEL] 撤单时更新
  const bool Trigger_onDepth = lob.depth_updated;                                       // [ON DEPTH]  盘口更新时触发

  if (Trigger_onTick) {
    dag_.l0.TickIndex.compute(); // input: td
    dag_.l0.TickIndex.flush();   // output: Sec_, TickIndex_

    // 订单流累计特征 compute (flush在onDepth)
    dag_.l0.FlowRate.compute();   // input: td
    dag_.l0.ToxicCr.compute();    // input: td
    dag_.l0.Resiliency.compute(); // input: td, BidQty_, AskQty_
    dag_.l0.Ctr.compute();        // input: td
    dag_.l0.Behav.compute();      // input: td
    dag_.l0.Oa.compute();         // input: td
    dag_.l0.Hla.compute();        // input: td, BidQty_, AskQty_
    dag_.l0.Toxic.compute();      // input: td, BidQty_, AskQty_

    ts_features_buffer_[L0_FieldOffset::sec] = dag_.l0.Sec_.back();
  }

  if (Trigger_onTaker) {
    dag_.l0.Taker.compute(); // input: td
    dag_.l0.Taker.flush();   // output: Taker_price_, Taker_timestamp_, Taker_tickindex_, Taker_volume_
  }

  if (Trigger_onMaker) {
    dag_.l0.Maker.compute(); // input: td
    dag_.l0.Maker.flush();   // output: Maker_price_, Maker_timestamp_, Maker_tickindex_, Maker_volume_
  }

  if (Trigger_onCancel) {
    dag_.l0.Cancel.compute(); // input: td
    dag_.l0.Cancel.flush();   // output: Cancel_price_, Cancel_timestamp_, Cancel_tickindex_, Cancel_volume_
  }

  if (Trigger_onDepth) {
    // 深度
    dag_.l0.DepthIndex.compute(); // input: td
    dag_.l0.DepthIndex.flush();   // output: DepthIndex_
    dag_.l0.DepthData.compute();  // input: td, Taker_price_
    dag_.l0.DepthData.flush();    // output: BidPrice_, AskPrice_, BidQty_, AskQty_, BidAmt_, AskAmt_

    // 基础
    dag_.l0.MidPrice.compute();   // input: BidPrice_[0], AskPrice_[0]
    dag_.l0.MidPrice.flush();     // output: MidPrice_
    dag_.l0.MicroPrice.compute(); // input: BidPrice_[0], AskPrice_[0], BidQty_[0], AskQty_[0]
    dag_.l0.MicroPrice.flush();   // output: MicroPrice_
    dag_.l0.Spread.compute();     // input: BidPrice_[0], AskPrice_[0]
    dag_.l0.Spread.flush();       // output: Spread_

    // --- CI ---
    dag_.l0.Ci_1.compute();   // input: BidQty_, AskQty_
    dag_.l0.Ci_1.flush();     // output: Ci_1_
    dag_.l0.Ci_5.compute();   // input: BidQty_, AskQty_
    dag_.l0.Ci_5.flush();     // output: Ci_5_
    dag_.l0.Ci_10.compute();  // input: BidQty_, AskQty_
    dag_.l0.Ci_10.flush();    // output: Ci_10_
    dag_.l0.Ci_30.compute();  // input: BidQty_, AskQty_
    dag_.l0.Ci_30.flush();    // output: Ci_30_
    dag_.l0.Ci_all.compute(); // input: td.lob.all_bid_volume, td.lob.all_ask_volume (全市场挂单量)
    dag_.l0.Ci_all.flush();   // output: Ci_all_

    // --- CWI ---
    dag_.l0.Cwi_1.compute(); // input: BidQty_, AskQty_
    dag_.l0.Cwi_1.flush();   // output: Cwi_1_
    dag_.l0.Cwi_2.compute(); // input: BidQty_, AskQty_
    dag_.l0.Cwi_2.flush();   // output: Cwi_2_

    // --- DDI ---
    dag_.l0.Ddi_1.compute(); // input: BidQty_, AskQty_, BidPrice_, AskPrice_
    dag_.l0.Ddi_1.flush();   // output: Ddi_1_
    dag_.l0.Ddi_2.compute(); // input: BidQty_, AskQty_, BidPrice_, AskPrice_
    dag_.l0.Ddi_2.flush();   // output: Ddi_2_

    // --- TLR ---
    dag_.l0.Tbr_5.compute(); // input: BidQty_[0:4], td.lob.all_bid_volume
    dag_.l0.Tbr_5.flush();   // output: Tbr_5_
    dag_.l0.Tar_5.compute(); // input: AskQty_[0:4], td.lob.all_ask_volume
    dag_.l0.Tar_5.flush();   // output: Tar_5_

    // --- Para (Layer 1) ---
    dag_.l0.Para_b_c0.compute(); // input: BidQty_, AskQty_
    dag_.l0.Para_b_c0.flush();   // output: Para_b_c0_
    dag_.l0.Para_b_c1.compute(); // input: BidQty_, AskQty_
    dag_.l0.Para_b_c1.flush();   // output: Para_b_c1_
    dag_.l0.Para_b_c2.compute(); // input: BidQty_, AskQty_
    dag_.l0.Para_b_c2.flush();   // output: Para_b_c2_
    dag_.l0.Para_a_c0.compute(); // input: BidQty_, AskQty_
    dag_.l0.Para_a_c0.flush();   // output: Para_a_c0_
    dag_.l0.Para_a_c1.compute(); // input: BidQty_, AskQty_
    dag_.l0.Para_a_c1.flush();   // output: Para_a_c1_
    dag_.l0.Para_a_c2.compute(); // input: BidQty_, AskQty_
    dag_.l0.Para_a_c2.flush();   // output: Para_a_c2_
    // --- Para (Layer 2: 失衡, 依赖Layer 1 flush后的CBuffer) ---
    dag_.l0.ParaImba_c0.compute(); // input: Para_b_c0_, Para_a_c0_
    dag_.l0.ParaImba_c0.flush();   // output: ParaImba_c0_
    dag_.l0.ParaImba_c1.compute(); // input: Para_b_c1_, Para_a_c1_
    dag_.l0.ParaImba_c1.flush();   // output: ParaImba_c1_
    dag_.l0.ParaImba_c2.compute(); // input: Para_b_c2_, Para_a_c2_
    dag_.l0.ParaImba_c2.flush();   // output: ParaImba_c2_

    // --- GRAD (Layer 1) ---
    dag_.l0.Grad_b_5_c1.compute(); // input: BidQty_, AskQty_
    dag_.l0.Grad_b_5_c1.flush();   // output: Grad_b_5_c1_
    dag_.l0.Grad_a_5_c1.compute(); // input: BidQty_, AskQty_
    dag_.l0.Grad_a_5_c1.flush();   // output: Grad_a_5_c1_
    // --- GRAD (Layer 2) ---
    dag_.l0.GradImba_5_c1.compute(); // input: Grad_b_5_c1_, Grad_a_5_c1_
    dag_.l0.GradImba_5_c1.flush();   // output: GradImba_5_c1_

    // --- ENTROPY (Layer 1) ---
    dag_.l0.Entropy_b_5.compute();  // input: BidQty_, AskQty_
    dag_.l0.Entropy_b_5.flush();    // output: Entropy_b_5_
    dag_.l0.Entropy_a_5.compute();  // input: BidQty_, AskQty_
    dag_.l0.Entropy_a_5.flush();    // output: Entropy_a_5_
    dag_.l0.Entropy_b_30.compute(); // input: BidQty_, AskQty_
    dag_.l0.Entropy_b_30.flush();   // output: Entropy_b_30_
    dag_.l0.Entropy_a_30.compute(); // input: BidQty_, AskQty_
    dag_.l0.Entropy_a_30.flush();   // output: Entropy_a_30_
    // --- ENTROPY (Layer 2) ---
    dag_.l0.EntropyImba_5.compute();  // input: Entropy_b_5_, Entropy_a_5_
    dag_.l0.EntropyImba_5.flush();    // output: EntropyImba_5_
    dag_.l0.EntropyImba_30.compute(); // input: Entropy_b_30_, Entropy_a_30_
    dag_.l0.EntropyImba_30.flush();   // output: EntropyImba_30_

    // --- OFI ---
    dag_.l0.Ofi_1.compute(); // input: BidQty_, AskQty_, BidPrice_, AskPrice_
    dag_.l0.Ofi_1.flush();   // output: Ofi_1_
    dag_.l0.Ofi_5.compute(); // input: BidQty_, AskQty_, BidPrice_, AskPrice_
    dag_.l0.Ofi_5.flush();   // output: Ofi_5_

    // --- COST ---
    dag_.l0.Cost_buy_1.compute();   // input: AskPrice_, AskQty_, MidPrice_
    dag_.l0.Cost_buy_1.flush();     // output: Cost_buy_1_
    dag_.l0.Cost_buy_5.compute();   // input: AskPrice_, AskQty_, MidPrice_
    dag_.l0.Cost_buy_5.flush();     // output: Cost_buy_5_
    dag_.l0.Cost_buy_10.compute();  // input: AskPrice_, AskQty_, MidPrice_
    dag_.l0.Cost_buy_10.flush();    // output: Cost_buy_10_
    dag_.l0.Cost_sell_1.compute();  // input: BidPrice_, BidQty_, MidPrice_
    dag_.l0.Cost_sell_1.flush();    // output: Cost_sell_1_
    dag_.l0.Cost_sell_5.compute();  // input: BidPrice_, BidQty_, MidPrice_
    dag_.l0.Cost_sell_5.flush();    // output: Cost_sell_5_
    dag_.l0.Cost_sell_10.compute(); // input: BidPrice_, BidQty_, MidPrice_
    dag_.l0.Cost_sell_10.flush();   // output: Cost_sell_10_

    // --- PEAK ---
    dag_.l0.Peak_loc_bid.compute();   // input: BidQty_
    dag_.l0.Peak_loc_bid.flush();     // output: Peak_loc_bid_
    dag_.l0.Peak_loc_ask.compute();   // input: AskQty_
    dag_.l0.Peak_loc_ask.flush();     // output: Peak_loc_ask_
    dag_.l0.Peak_ratio_bid.compute(); // input: BidQty_
    dag_.l0.Peak_ratio_bid.flush();   // output: Peak_ratio_bid_
    dag_.l0.Peak_ratio_ask.compute(); // input: AskQty_
    dag_.l0.Peak_ratio_ask.flush();   // output: Peak_ratio_ask_

    // --- 订单流累计特征 flush (compute在onTick, 读取深度后输出) ---
    dag_.l0.FlowRate.flush();   // output: FlowRate_arr_bid_, FlowRate_arr_ask_, FlowRate_can_bid_, FlowRate_can_ask_, FlowRate_trd_buy_, FlowRate_trd_sell_, FlowRate_net_ord_, FlowRate_foi_
    dag_.l0.ToxicCr.flush();    // output: ToxicCr_
    dag_.l0.Resiliency.flush(); // output: Resil_ratio_bid_, Resil_ratio_ask_, Resil_imba_, Resil_dev_bid_, Resil_dev_ask_, Resil_mr_bid_, Resil_mr_ask_, Resil_recovery_bid_, Resil_recovery_ask_
    dag_.l0.Ctr.flush();        // output: Ctr_cc_r_, Ctr_xl_, Ctr_l_, Ctr_m_, Ctr_s_, Ctr_cnbi_, Ctr_cnbi_xl_, Ctr_cnbi_l_, Ctr_cnbi_m_, Ctr_cnbi_s_, Ctr_cnbi_am_, Ctr_cnbi_pm_
    dag_.l0.Behav.flush();      // output: Behav_agg_buy_, Behav_agg_sell_, Behav_agg_dif_, Behav_cpr_, Behav_agg_trd_, Behav_ord_size_
    dag_.l0.Oa.flush();         // output: Oa_bcr_, Oa_acr_, Oa_btr_, Oa_atr_
    dag_.l0.Hla.flush();        // output: Hla_imba_
    dag_.l0.Toxic.flush();      // output: Toxic_ptc_rt_, Toxic_fleet_rt_, Toxic_spoof_int_, Toxic_stale_ratio_bid_, Toxic_stale_ratio_ask_

    // --- LABEL ---
    dag_.l0.Label_next_tick_ret.compute(); // input: MidPrice_
    dag_.l0.Label_next_tick_ret.flush();   // output: Label_next_tick_ret_

    // --- REPRE ---
    dag_.l0.DepthRepresentation.compute(); // input: (none)
    dag_.l0.DepthRepresentation.flush();   // output: DepthRepresentation_

    // --- 写入缓冲区 (用 L0_FieldOffset 索引) ---
    ts_features_buffer_[L0_FieldOffset::ci_1] = dag_.l0.Ci_1_.back();
    ts_features_buffer_[L0_FieldOffset::ci_5] = dag_.l0.Ci_5_.back();
    ts_features_buffer_[L0_FieldOffset::ci_10] = dag_.l0.Ci_10_.back();
    ts_features_buffer_[L0_FieldOffset::ci_30] = dag_.l0.Ci_30_.back();
    ts_features_buffer_[L0_FieldOffset::ci_all] = dag_.l0.Ci_all_.back();
    ts_features_buffer_[L0_FieldOffset::cwi_1] = dag_.l0.Cwi_1_.back();
    ts_features_buffer_[L0_FieldOffset::cwi_2] = dag_.l0.Cwi_2_.back();
    ts_features_buffer_[L0_FieldOffset::ddi_1] = dag_.l0.Ddi_1_.back();
    ts_features_buffer_[L0_FieldOffset::ddi_2] = dag_.l0.Ddi_2_.back();
    ts_features_buffer_[L0_FieldOffset::tbr_5] = dag_.l0.Tbr_5_.back();
    ts_features_buffer_[L0_FieldOffset::tar_5] = dag_.l0.Tar_5_.back();
    ts_features_buffer_[L0_FieldOffset::b_para_c0] = dag_.l0.Para_b_c0_.back();
    ts_features_buffer_[L0_FieldOffset::b_para_c1] = dag_.l0.Para_b_c1_.back();
    ts_features_buffer_[L0_FieldOffset::b_para_c2] = dag_.l0.Para_b_c2_.back();
    ts_features_buffer_[L0_FieldOffset::a_para_c0] = dag_.l0.Para_a_c0_.back();
    ts_features_buffer_[L0_FieldOffset::a_para_c1] = dag_.l0.Para_a_c1_.back();
    ts_features_buffer_[L0_FieldOffset::a_para_c2] = dag_.l0.Para_a_c2_.back();
    ts_features_buffer_[L0_FieldOffset::imba_para_c0] = dag_.l0.ParaImba_c0_.back();
    ts_features_buffer_[L0_FieldOffset::imba_para_c1] = dag_.l0.ParaImba_c1_.back();
    ts_features_buffer_[L0_FieldOffset::imba_para_c2] = dag_.l0.ParaImba_c2_.back();
    ts_features_buffer_[L0_FieldOffset::b_5_c1] = dag_.l0.Grad_b_5_c1_.back();
    ts_features_buffer_[L0_FieldOffset::a_5_c1] = dag_.l0.Grad_a_5_c1_.back();
    ts_features_buffer_[L0_FieldOffset::imba_5_c1] = dag_.l0.GradImba_5_c1_.back();
    ts_features_buffer_[L0_FieldOffset::b_5_entropy] = dag_.l0.Entropy_b_5_.back();
    ts_features_buffer_[L0_FieldOffset::a_5_entropy] = dag_.l0.Entropy_a_5_.back();
    ts_features_buffer_[L0_FieldOffset::imba_5_entropy] = dag_.l0.EntropyImba_5_.back();
    ts_features_buffer_[L0_FieldOffset::b_30_entropy] = dag_.l0.Entropy_b_30_.back();
    ts_features_buffer_[L0_FieldOffset::a_30_entropy] = dag_.l0.Entropy_a_30_.back();
    ts_features_buffer_[L0_FieldOffset::imba_30_entropy] = dag_.l0.EntropyImba_30_.back();
    ts_features_buffer_[L0_FieldOffset::ofi_1] = dag_.l0.Ofi_1_.back();
    ts_features_buffer_[L0_FieldOffset::ofi_5] = dag_.l0.Ofi_5_.back();
    ts_features_buffer_[L0_FieldOffset::cost_buy_1] = dag_.l0.Cost_buy_1_.back();
    ts_features_buffer_[L0_FieldOffset::cost_buy_5] = dag_.l0.Cost_buy_5_.back();
    ts_features_buffer_[L0_FieldOffset::cost_buy_10] = dag_.l0.Cost_buy_10_.back();
    ts_features_buffer_[L0_FieldOffset::cost_sell_1] = dag_.l0.Cost_sell_1_.back();
    ts_features_buffer_[L0_FieldOffset::cost_sell_5] = dag_.l0.Cost_sell_5_.back();
    ts_features_buffer_[L0_FieldOffset::cost_sell_10] = dag_.l0.Cost_sell_10_.back();
    ts_features_buffer_[L0_FieldOffset::peak_loc_bid] = dag_.l0.Peak_loc_bid_.back();
    ts_features_buffer_[L0_FieldOffset::peak_loc_ask] = dag_.l0.Peak_loc_ask_.back();
    ts_features_buffer_[L0_FieldOffset::peak_ratio_bid] = dag_.l0.Peak_ratio_bid_.back();
    ts_features_buffer_[L0_FieldOffset::peak_ratio_ask] = dag_.l0.Peak_ratio_ask_.back();
    ts_features_buffer_[L0_FieldOffset::toxic_cr] = dag_.l0.ToxicCr_.back();
    ts_features_buffer_[L0_FieldOffset::arr_bid] = dag_.l0.FlowRate_arr_bid_.back();
    ts_features_buffer_[L0_FieldOffset::arr_ask] = dag_.l0.FlowRate_arr_ask_.back();
    ts_features_buffer_[L0_FieldOffset::can_bid] = dag_.l0.FlowRate_can_bid_.back();
    ts_features_buffer_[L0_FieldOffset::can_ask] = dag_.l0.FlowRate_can_ask_.back();
    ts_features_buffer_[L0_FieldOffset::trd_buy] = dag_.l0.FlowRate_trd_buy_.back();
    ts_features_buffer_[L0_FieldOffset::trd_sell] = dag_.l0.FlowRate_trd_sell_.back();
    ts_features_buffer_[L0_FieldOffset::net_ord] = dag_.l0.FlowRate_net_ord_.back();
    ts_features_buffer_[L0_FieldOffset::foi] = dag_.l0.FlowRate_foi_.back();
    ts_features_buffer_[L0_FieldOffset::ratio_bid] = dag_.l0.Resil_ratio_bid_.back();
    ts_features_buffer_[L0_FieldOffset::ratio_ask] = dag_.l0.Resil_ratio_ask_.back();
    ts_features_buffer_[L0_FieldOffset::imba] = dag_.l0.Resil_imba_.back();
    ts_features_buffer_[L0_FieldOffset::dev_bid] = dag_.l0.Resil_dev_bid_.back();
    ts_features_buffer_[L0_FieldOffset::dev_ask] = dag_.l0.Resil_dev_ask_.back();
    ts_features_buffer_[L0_FieldOffset::mr_bid] = dag_.l0.Resil_mr_bid_.back();
    ts_features_buffer_[L0_FieldOffset::mr_ask] = dag_.l0.Resil_mr_ask_.back();
    ts_features_buffer_[L0_FieldOffset::recovery_bid] = dag_.l0.Resil_recovery_bid_.back();
    ts_features_buffer_[L0_FieldOffset::recovery_ask] = dag_.l0.Resil_recovery_ask_.back();
    ts_features_buffer_[L0_FieldOffset::cc_r] = dag_.l0.Ctr_cc_r_.back();
    ts_features_buffer_[L0_FieldOffset::ctr_xl] = dag_.l0.Ctr_xl_.back();
    ts_features_buffer_[L0_FieldOffset::ctr_l] = dag_.l0.Ctr_l_.back();
    ts_features_buffer_[L0_FieldOffset::ctr_m] = dag_.l0.Ctr_m_.back();
    ts_features_buffer_[L0_FieldOffset::ctr_s] = dag_.l0.Ctr_s_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi] = dag_.l0.Ctr_cnbi_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_xl] = dag_.l0.Ctr_cnbi_xl_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_l] = dag_.l0.Ctr_cnbi_l_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_m] = dag_.l0.Ctr_cnbi_m_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_s] = dag_.l0.Ctr_cnbi_s_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_am] = dag_.l0.Ctr_cnbi_am_.back();
    ts_features_buffer_[L0_FieldOffset::cnbi_pm] = dag_.l0.Ctr_cnbi_pm_.back();
    ts_features_buffer_[L0_FieldOffset::agg_buy] = dag_.l0.Behav_agg_buy_.back();
    ts_features_buffer_[L0_FieldOffset::agg_sell] = dag_.l0.Behav_agg_sell_.back();
    ts_features_buffer_[L0_FieldOffset::agg_dif] = dag_.l0.Behav_agg_dif_.back();
    ts_features_buffer_[L0_FieldOffset::cpr] = dag_.l0.Behav_cpr_.back();
    ts_features_buffer_[L0_FieldOffset::agg_trd] = dag_.l0.Behav_agg_trd_.back();
    ts_features_buffer_[L0_FieldOffset::ord_size] = dag_.l0.Behav_ord_size_.back();
    ts_features_buffer_[L0_FieldOffset::oa_bcr] = dag_.l0.Oa_bcr_.back();
    ts_features_buffer_[L0_FieldOffset::oa_acr] = dag_.l0.Oa_acr_.back();
    ts_features_buffer_[L0_FieldOffset::oa_btr] = dag_.l0.Oa_btr_.back();
    ts_features_buffer_[L0_FieldOffset::oa_atr] = dag_.l0.Oa_atr_.back();
    ts_features_buffer_[L0_FieldOffset::hla_imba] = dag_.l0.Hla_imba_.back();
    ts_features_buffer_[L0_FieldOffset::ptc_rt] = dag_.l0.Toxic_ptc_rt_.back();
    ts_features_buffer_[L0_FieldOffset::fleet_rt] = dag_.l0.Toxic_fleet_rt_.back();
    ts_features_buffer_[L0_FieldOffset::spoof_int] = dag_.l0.Toxic_spoof_int_.back();
    ts_features_buffer_[L0_FieldOffset::stale_ratio_bid] = dag_.l0.Toxic_stale_ratio_bid_.back();
    ts_features_buffer_[L0_FieldOffset::stale_ratio_ask] = dag_.l0.Toxic_stale_ratio_ask_.back();
    ts_features_buffer_[L0_FieldOffset::depth_repre] = dag_.l0.DepthRepresentation_.back();
    ts_features_buffer_[L0_FieldOffset::next_tick_ret] = dag_.l0.Label_next_tick_ret_.back();

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
