#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/Backend/FeatureStoreConfig.hpp"
#include <array>
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

  // 输出缓冲区
  std::array<float, L1_TS_WIDTH> ts_features_buffer_;
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
    dag_.l1.MinuteIndex.compute(); // input: minute_data

    // --- CI ---
    dag_.l1.Ci_5.compute();   // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Ci_5.flush();     // output: Ci_5_
    dag_.l1.Ci_10.compute();  // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Ci_10.flush();    // output: Ci_10_
    dag_.l1.Ci_30.compute();  // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Ci_30.flush();    // output: Ci_30_
    dag_.l1.Ci_all.compute(); // input: l0.td
    dag_.l1.Ci_all.flush();   // output: Ci_all_

    // --- CWI ---
    dag_.l1.Cwi_1.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Cwi_1.flush();   // output: Cwi_1_
    dag_.l1.Cwi_2.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Cwi_2.flush();   // output: Cwi_2_

    // --- DDI ---
    dag_.l1.Ddi_1.compute(); // input: l0.BidQty_, l0.AskQty_, l0.BidPrice_, l0.AskPrice_
    dag_.l1.Ddi_1.flush();   // output: Ddi_1_
    dag_.l1.Ddi_2.compute(); // input: l0.BidQty_, l0.AskQty_, l0.BidPrice_, l0.AskPrice_
    dag_.l1.Ddi_2.flush();   // output: Ddi_2_

    // --- TLR ---
    dag_.l1.Tbr_5.compute(); // input: l0.BidQty_, l0.td
    dag_.l1.Tbr_5.flush();   // output: Tbr_5_
    dag_.l1.Tar_5.compute(); // input: l0.AskQty_, l0.td
    dag_.l1.Tar_5.flush();   // output: Tar_5_

    // --- Para (降频, Layer 1) ---
    dag_.l1.Para_b_c0.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Para_b_c0.flush();   // output: Para_b_c0_
    dag_.l1.Para_b_c1.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Para_b_c1.flush();   // output: Para_b_c1_
    dag_.l1.Para_b_c2.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Para_b_c2.flush();   // output: Para_b_c2_
    dag_.l1.Para_a_c0.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Para_a_c0.flush();   // output: Para_a_c0_
    dag_.l1.Para_a_c1.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Para_a_c1.flush();   // output: Para_a_c1_
    dag_.l1.Para_a_c2.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Para_a_c2.flush();   // output: Para_a_c2_
    // --- ParaImba (降频, Layer 2: 依赖 Layer 1 flush 后的 CBuffer) ---
    dag_.l1.ParaImba_c0.compute(); // input: Para_b_c0_, Para_a_c0_
    dag_.l1.ParaImba_c0.flush();   // output: ParaImba_c0_
    dag_.l1.ParaImba_c1.compute(); // input: Para_b_c1_, Para_a_c1_
    dag_.l1.ParaImba_c1.flush();   // output: ParaImba_c1_
    dag_.l1.ParaImba_c2.compute(); // input: Para_b_c2_, Para_a_c2_
    dag_.l1.ParaImba_c2.flush();   // output: ParaImba_c2_

    // --- Grad (降频, Layer 1) ---
    dag_.l1.Grad_b_5_c1.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Grad_b_5_c1.flush();   // output: Grad_b_5_c1_
    dag_.l1.Grad_a_5_c1.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Grad_a_5_c1.flush();   // output: Grad_a_5_c1_
    // --- GradImba (降频, Layer 2) ---
    dag_.l1.GradImba_5_c1.compute(); // input: Grad_b_5_c1_, Grad_a_5_c1_
    dag_.l1.GradImba_5_c1.flush();   // output: GradImba_5_c1_

    // --- Entropy (降频, Layer 1) ---
    dag_.l1.Entropy_b_5.compute();  // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Entropy_b_5.flush();    // output: Entropy_b_5_
    dag_.l1.Entropy_a_5.compute();  // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Entropy_a_5.flush();    // output: Entropy_a_5_
    dag_.l1.Entropy_b_30.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Entropy_b_30.flush();   // output: Entropy_b_30_
    dag_.l1.Entropy_a_30.compute(); // input: l0.BidQty_, l0.AskQty_
    dag_.l1.Entropy_a_30.flush();   // output: Entropy_a_30_
    // --- EntropyImba (降频, Layer 2) ---
    dag_.l1.EntropyImba_5.compute();  // input: Entropy_b_5_, Entropy_a_5_
    dag_.l1.EntropyImba_5.flush();    // output: EntropyImba_5_
    dag_.l1.EntropyImba_30.compute(); // input: Entropy_b_30_, Entropy_a_30_
    dag_.l1.EntropyImba_30.flush();   // output: EntropyImba_30_

    // --- DepthRepresentation (降频) ---
    dag_.l1.DepthRepresentation.compute(); // input: (none)
    dag_.l1.DepthRepresentation.flush();   // output: DepthRepresentation_

    // --- 降频算子 flush ---
    dag_.l1.Ctr.flush();        // output: Ctr_cc_r_, Ctr_xl_, Ctr_l_, Ctr_m_, Ctr_s_, Ctr_cnbi_, Ctr_cnbi_xl_, Ctr_cnbi_l_, Ctr_cnbi_m_, Ctr_cnbi_s_, Ctr_cnbi_am_, Ctr_cnbi_pm_
    dag_.l1.Oa.flush();         // output: Oa_bcr_, Oa_acr_, Oa_btr_, Oa_atr_
    dag_.l1.Hla.flush();        // output: Hla_imba_
    dag_.l1.ToxicCr.flush();    // output: ToxicCr_
    dag_.l1.FlowRate.flush();   // output: FlowRate_mk_bid_, FlowRate_mk_ask_, FlowRate_cn_bid_, FlowRate_cn_ask_, FlowRate_tk_bid_, FlowRate_tk_ask_, FlowRate_net_ord_, FlowRate_foi_
    dag_.l1.Behav.flush();      // output: Behav_agg_buy_, Behav_agg_sell_, Behav_agg_dif_, Behav_cpr_, Behav_agg_trd_, Behav_ord_size_
    dag_.l1.Manip.flush();      // output: Manip_ptc_rt_, Manip_fleet_rt_, Manip_spoof_int_, Manip_stale_ratio_bid_, Manip_stale_ratio_ask_
    dag_.l1.Resiliency.flush(); // output: Resil_ratio_bid_, Resil_ratio_ask_, Resil_imba_, Resil_dev_bid_, Resil_dev_ask_, Resil_mr_bid_, Resil_mr_ask_, Resil_recovery_bid_, Resil_recovery_ask_

    // --- Cost (降频) ---
    dag_.l1.Cost_buy_1.compute();   // input: l0.AskPrice_, l0.AskQty_, l0.MidPrice_
    dag_.l1.Cost_buy_1.flush();     // output: Cost_buy_1_
    dag_.l1.Cost_buy_5.compute();   // input: l0.AskPrice_, l0.AskQty_, l0.MidPrice_
    dag_.l1.Cost_buy_5.flush();     // output: Cost_buy_5_
    dag_.l1.Cost_buy_10.compute();  // input: l0.AskPrice_, l0.AskQty_, l0.MidPrice_
    dag_.l1.Cost_buy_10.flush();    // output: Cost_buy_10_
    dag_.l1.Cost_sell_1.compute();  // input: l0.BidPrice_, l0.BidQty_, l0.MidPrice_
    dag_.l1.Cost_sell_1.flush();    // output: Cost_sell_1_
    dag_.l1.Cost_sell_5.compute();  // input: l0.BidPrice_, l0.BidQty_, l0.MidPrice_
    dag_.l1.Cost_sell_5.flush();    // output: Cost_sell_5_
    dag_.l1.Cost_sell_10.compute(); // input: l0.BidPrice_, l0.BidQty_, l0.MidPrice_
    dag_.l1.Cost_sell_10.flush();   // output: Cost_sell_10_

    // --- Peak (降频) ---
    dag_.l1.Peak_loc_bid.compute();   // input: l0.BidQty_
    dag_.l1.Peak_loc_bid.flush();     // output: Peak_loc_bid_
    dag_.l1.Peak_loc_ask.compute();   // input: l0.AskQty_
    dag_.l1.Peak_loc_ask.flush();     // output: Peak_loc_ask_
    dag_.l1.Peak_ratio_bid.compute(); // input: l0.BidQty_
    dag_.l1.Peak_ratio_bid.flush();   // output: Peak_ratio_bid_
    dag_.l1.Peak_ratio_ask.compute(); // input: l0.AskQty_
    dag_.l1.Peak_ratio_ask.flush();   // output: Peak_ratio_ask_

    // --- 写入缓冲区 (按 FeaturesDefine.hpp 中的定义顺序) ---
    ts_features_buffer_[L1_FieldOffset::min] = dag_.l1.Min_.back();
    ts_features_buffer_[L1_FieldOffset::ci_5] = dag_.l1.Ci_5_.back();
    ts_features_buffer_[L1_FieldOffset::ci_10] = dag_.l1.Ci_10_.back();
    ts_features_buffer_[L1_FieldOffset::ci_30] = dag_.l1.Ci_30_.back();
    ts_features_buffer_[L1_FieldOffset::ci_all] = dag_.l1.Ci_all_.back();
    ts_features_buffer_[L1_FieldOffset::cwi_1] = dag_.l1.Cwi_1_.back();
    ts_features_buffer_[L1_FieldOffset::cwi_2] = dag_.l1.Cwi_2_.back();
    ts_features_buffer_[L1_FieldOffset::ddi_1] = dag_.l1.Ddi_1_.back();
    ts_features_buffer_[L1_FieldOffset::ddi_2] = dag_.l1.Ddi_2_.back();
    ts_features_buffer_[L1_FieldOffset::tbr_5] = dag_.l1.Tbr_5_.back();
    ts_features_buffer_[L1_FieldOffset::tar_5] = dag_.l1.Tar_5_.back();
    ts_features_buffer_[L1_FieldOffset::b_para_c0] = dag_.l1.Para_b_c0_.back();
    ts_features_buffer_[L1_FieldOffset::b_para_c1] = dag_.l1.Para_b_c1_.back();
    ts_features_buffer_[L1_FieldOffset::b_para_c2] = dag_.l1.Para_b_c2_.back();
    ts_features_buffer_[L1_FieldOffset::a_para_c0] = dag_.l1.Para_a_c0_.back();
    ts_features_buffer_[L1_FieldOffset::a_para_c1] = dag_.l1.Para_a_c1_.back();
    ts_features_buffer_[L1_FieldOffset::a_para_c2] = dag_.l1.Para_a_c2_.back();
    ts_features_buffer_[L1_FieldOffset::imba_para_c0] = dag_.l1.ParaImba_c0_.back();
    ts_features_buffer_[L1_FieldOffset::imba_para_c1] = dag_.l1.ParaImba_c1_.back();
    ts_features_buffer_[L1_FieldOffset::imba_para_c2] = dag_.l1.ParaImba_c2_.back();
    ts_features_buffer_[L1_FieldOffset::b_5_c1] = dag_.l1.Grad_b_5_c1_.back();
    ts_features_buffer_[L1_FieldOffset::a_5_c1] = dag_.l1.Grad_a_5_c1_.back();
    ts_features_buffer_[L1_FieldOffset::imba_5_c1] = dag_.l1.GradImba_5_c1_.back();
    ts_features_buffer_[L1_FieldOffset::b_5_entropy] = dag_.l1.Entropy_b_5_.back();
    ts_features_buffer_[L1_FieldOffset::a_5_entropy] = dag_.l1.Entropy_a_5_.back();
    ts_features_buffer_[L1_FieldOffset::imba_5_entropy] = dag_.l1.EntropyImba_5_.back();
    ts_features_buffer_[L1_FieldOffset::b_30_entropy] = dag_.l1.Entropy_b_30_.back();
    ts_features_buffer_[L1_FieldOffset::a_30_entropy] = dag_.l1.Entropy_a_30_.back();
    ts_features_buffer_[L1_FieldOffset::imba_30_entropy] = dag_.l1.EntropyImba_30_.back();
    ts_features_buffer_[L1_FieldOffset::depth_repre] = dag_.l1.DepthRepresentation_.back();
    ts_features_buffer_[L1_FieldOffset::toxic_cr] = dag_.l1.ToxicCr_.back();
    ts_features_buffer_[L1_FieldOffset::mk_bid] = dag_.l1.FlowRate_mk_bid_.back();
    ts_features_buffer_[L1_FieldOffset::mk_ask] = dag_.l1.FlowRate_mk_ask_.back();
    ts_features_buffer_[L1_FieldOffset::cn_bid] = dag_.l1.FlowRate_cn_bid_.back();
    ts_features_buffer_[L1_FieldOffset::cn_ask] = dag_.l1.FlowRate_cn_ask_.back();
    ts_features_buffer_[L1_FieldOffset::tk_bid] = dag_.l1.FlowRate_tk_bid_.back();
    ts_features_buffer_[L1_FieldOffset::tk_ask] = dag_.l1.FlowRate_tk_ask_.back();
    ts_features_buffer_[L1_FieldOffset::net_ord] = dag_.l1.FlowRate_net_ord_.back();
    ts_features_buffer_[L1_FieldOffset::foi] = dag_.l1.FlowRate_foi_.back();
    ts_features_buffer_[L1_FieldOffset::cc_r] = dag_.l1.Ctr_cc_r_.back();
    ts_features_buffer_[L1_FieldOffset::ctr_xl] = dag_.l1.Ctr_xl_.back();
    ts_features_buffer_[L1_FieldOffset::ctr_l] = dag_.l1.Ctr_l_.back();
    ts_features_buffer_[L1_FieldOffset::ctr_m] = dag_.l1.Ctr_m_.back();
    ts_features_buffer_[L1_FieldOffset::ctr_s] = dag_.l1.Ctr_s_.back();
    ts_features_buffer_[L1_FieldOffset::cnbi] = dag_.l1.Ctr_cnbi_.back();
    ts_features_buffer_[L1_FieldOffset::cnbi_xl] = dag_.l1.Ctr_cnbi_xl_.back();
    ts_features_buffer_[L1_FieldOffset::cnbi_l] = dag_.l1.Ctr_cnbi_l_.back();
    ts_features_buffer_[L1_FieldOffset::cnbi_m] = dag_.l1.Ctr_cnbi_m_.back();
    ts_features_buffer_[L1_FieldOffset::cnbi_s] = dag_.l1.Ctr_cnbi_s_.back();
    ts_features_buffer_[L1_FieldOffset::cnbi_am] = dag_.l1.Ctr_cnbi_am_.back();
    ts_features_buffer_[L1_FieldOffset::cnbi_pm] = dag_.l1.Ctr_cnbi_pm_.back();
    ts_features_buffer_[L1_FieldOffset::oa_bcr] = dag_.l1.Oa_bcr_.back();
    ts_features_buffer_[L1_FieldOffset::oa_acr] = dag_.l1.Oa_acr_.back();
    ts_features_buffer_[L1_FieldOffset::oa_btr] = dag_.l1.Oa_btr_.back();
    ts_features_buffer_[L1_FieldOffset::oa_atr] = dag_.l1.Oa_atr_.back();
    ts_features_buffer_[L1_FieldOffset::hla_imba] = dag_.l1.Hla_imba_.back();
    // --- Behavioral (降频) ---
    ts_features_buffer_[L1_FieldOffset::agg_buy] = dag_.l1.Behav_agg_buy_.back();
    ts_features_buffer_[L1_FieldOffset::agg_sell] = dag_.l1.Behav_agg_sell_.back();
    ts_features_buffer_[L1_FieldOffset::agg_dif] = dag_.l1.Behav_agg_dif_.back();
    ts_features_buffer_[L1_FieldOffset::cpr] = dag_.l1.Behav_cpr_.back();
    ts_features_buffer_[L1_FieldOffset::ptc_rt] = dag_.l1.Manip_ptc_rt_.back();
    ts_features_buffer_[L1_FieldOffset::fleet_rt] = dag_.l1.Manip_fleet_rt_.back();
    ts_features_buffer_[L1_FieldOffset::spoof_int] = dag_.l1.Manip_spoof_int_.back();
    ts_features_buffer_[L1_FieldOffset::agg_trd] = dag_.l1.Behav_agg_trd_.back();
    ts_features_buffer_[L1_FieldOffset::ord_size] = dag_.l1.Behav_ord_size_.back();
    ts_features_buffer_[L1_FieldOffset::stale_ratio_bid] = dag_.l1.Manip_stale_ratio_bid_.back();
    ts_features_buffer_[L1_FieldOffset::stale_ratio_ask] = dag_.l1.Manip_stale_ratio_ask_.back();
    // --- Resiliency (降频) ---
    ts_features_buffer_[L1_FieldOffset::ratio_bid] = dag_.l1.Resil_ratio_bid_.back();
    ts_features_buffer_[L1_FieldOffset::ratio_ask] = dag_.l1.Resil_ratio_ask_.back();
    ts_features_buffer_[L1_FieldOffset::resil_imba] = dag_.l1.Resil_imba_.back();
    ts_features_buffer_[L1_FieldOffset::dev_bid] = dag_.l1.Resil_dev_bid_.back();
    ts_features_buffer_[L1_FieldOffset::dev_ask] = dag_.l1.Resil_dev_ask_.back();
    ts_features_buffer_[L1_FieldOffset::mr_bid] = dag_.l1.Resil_mr_bid_.back();
    ts_features_buffer_[L1_FieldOffset::mr_ask] = dag_.l1.Resil_mr_ask_.back();
    ts_features_buffer_[L1_FieldOffset::recovery_bid] = dag_.l1.Resil_recovery_bid_.back();
    ts_features_buffer_[L1_FieldOffset::recovery_ask] = dag_.l1.Resil_recovery_ask_.back();
    // --- Cost (降频) ---
    ts_features_buffer_[L1_FieldOffset::cost_buy_1] = dag_.l1.Cost_buy_1_.back();
    ts_features_buffer_[L1_FieldOffset::cost_buy_5] = dag_.l1.Cost_buy_5_.back();
    ts_features_buffer_[L1_FieldOffset::cost_buy_10] = dag_.l1.Cost_buy_10_.back();
    ts_features_buffer_[L1_FieldOffset::cost_sell_1] = dag_.l1.Cost_sell_1_.back();
    ts_features_buffer_[L1_FieldOffset::cost_sell_5] = dag_.l1.Cost_sell_5_.back();
    ts_features_buffer_[L1_FieldOffset::cost_sell_10] = dag_.l1.Cost_sell_10_.back();
    // --- Peak (降频) ---
    ts_features_buffer_[L1_FieldOffset::peak_loc_bid] = dag_.l1.Peak_loc_bid_.back();
    ts_features_buffer_[L1_FieldOffset::peak_loc_ask] = dag_.l1.Peak_loc_ask_.back();
    ts_features_buffer_[L1_FieldOffset::peak_ratio_bid] = dag_.l1.Peak_ratio_bid_.back();
    ts_features_buffer_[L1_FieldOffset::peak_ratio_ask] = dag_.l1.Peak_ratio_ask_.back();

    // Write TS features
    TS_WRITE_FEATURES(store_, date_str_, 1, t, asset_id_, L1_FieldOffset::min, L1_FieldOffset::peak_ratio_ask, ts_features_buffer_.data(), worker_id_);
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
