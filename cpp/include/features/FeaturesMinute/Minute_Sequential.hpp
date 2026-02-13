#pragma once

#include "features/backend/FeatureStore.hpp"
#include "features/backend/FeatureStoreConfig.hpp"
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
    ts_features_buffer_[L1_FieldOffset::toxic_cr] = dag_.l0.ToxicCr_.back();
    ts_features_buffer_[L1_FieldOffset::arr_bid] = dag_.l0.FlowRate_arr_bid_.back();
    ts_features_buffer_[L1_FieldOffset::arr_ask] = dag_.l0.FlowRate_arr_ask_.back();
    ts_features_buffer_[L1_FieldOffset::can_bid] = dag_.l0.FlowRate_can_bid_.back();
    ts_features_buffer_[L1_FieldOffset::can_ask] = dag_.l0.FlowRate_can_ask_.back();
    ts_features_buffer_[L1_FieldOffset::trd_buy] = dag_.l0.FlowRate_trd_buy_.back();
    ts_features_buffer_[L1_FieldOffset::trd_sell] = dag_.l0.FlowRate_trd_sell_.back();
    ts_features_buffer_[L1_FieldOffset::net_ord] = dag_.l0.FlowRate_net_ord_.back();
    ts_features_buffer_[L1_FieldOffset::foi] = dag_.l0.FlowRate_foi_.back();

    // Write TS features
    TS_WRITE_FEATURES(store_, date_str_, 1, t, asset_id_, L1_FieldOffset::min, L1_FieldOffset::foi, ts_features_buffer_.data(), worker_id_);
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
