#pragma once

#include "features/Backend/FeatureStore.hpp"
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
  dag_.at_day_start();
  date_str_ = date_str;
}

inline void Tick_Sequential::compute_and_store() {
  // 每个算子的compute()和其输入buffer在同一个采样域(Trigger), 原则上支持多个采样域
  // 每个算子的flush()和其输出buffer在同一个采样域(Trigger), 原则上支持多个采样域
  // 具体绑定关系请看DAG向量图

  const size_t t = dag_.tick_data.l0_index;
  const auto &lob = dag_.tick_data.lob;
  const auto order_type = lob.order_type;

  const bool Trigger_onTaker [[maybe_unused]] = (order_type == L2::OrderType::TAKER);   // [ON TAKER]  成交时更新
  const bool Trigger_onMaker [[maybe_unused]] = (order_type == L2::OrderType::MAKER);   // [ON MAKER]  挂单时更新
  const bool Trigger_onCancel [[maybe_unused]] = (order_type == L2::OrderType::CANCEL); // [ON CANCEL] 撤单时更新
  const bool Trigger_onTick = true;                                                     // [ON TICK]   逐笔更新
  const bool Trigger_onDepth = lob.depth_updated;                                       // [ON DEPTH]  盘口更新时触发

  if (Trigger_onTaker) {
    dag_.l0.Taker.compute(); // input: td
    dag_.l0.Taker.flush();   // output: Taker_price_, Taker_timestamp_, Taker_tickindex_, Taker_volume_, Taker_dir_
  }

  if (Trigger_onMaker) {
    dag_.l0.Maker.compute(); // input: td
    dag_.l0.Maker.flush();   // output: Maker_price_, Maker_timestamp_, Maker_tickindex_, Maker_volume_, Maker_dir_
  }

  if (Trigger_onCancel) {
    dag_.l0.Cancel.compute(); // input: td
    dag_.l0.Cancel.flush();   // output: Cancel_price_, Cancel_timestamp_, Cancel_tickindex_, Cancel_volume_, Cancel_dir_
  }

  if (Trigger_onTick) {
    dag_.l0.TickIndex.compute(); // input: td
    dag_.l0.TickIndex.flush();   // output: Sec_, TickIndex_

    // 降频算子 compute (flush在onMinute)
    dag_.l1.Ctr.compute();        // input: l0.td
    dag_.l1.Oa.compute();         // input: l0.td
    dag_.l1.Hla.compute();        // input: l0.td, l0.BidQty_, l0.AskQty_
    dag_.l1.ToxicCr.compute();    // input: l0.td
    dag_.l1.FlowRate.compute();   // input: l0.td
    dag_.l1.Behav.compute();      // input: l0.td
    dag_.l1.Manip.compute();      // input: l0.td, l0.BidQty_, l0.AskQty_
    dag_.l1.Resiliency.compute(); // input: l0.td, l0.BidQty_, l0.AskQty_

    ts_features_buffer_[L0_FieldOffset::sec] = dag_.l0.Sec_.back();
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
    dag_.l0.Ci_1.compute(); // input: BidQty_, AskQty_
    dag_.l0.Ci_1.flush();   // output: Ci_1_

    // --- OFI ---
    dag_.l0.Ofi_1.compute(); // input: BidQty_, AskQty_, BidPrice_, AskPrice_
    dag_.l0.Ofi_1.flush();   // output: Ofi_1_
    dag_.l0.Ofi_5.compute(); // input: BidQty_, AskQty_, BidPrice_, AskPrice_
    dag_.l0.Ofi_5.flush();   // output: Ofi_5_

    // --- LabelReturn: 分钟锚定惰性回填 → L1 12 列 ---
    // 组0: 5min, 组1: 10min, 组2: 30min; 每组4个连续字段
    {
      constexpr size_t LABEL_GROUP_START[] = {
          L1_FieldOffset::lb_long_5m_5w,  // 5min组起始
          L1_FieldOffset::lb_long_10m_5w, // 10min组起始
          L1_FieldOffset::lb_long_30m_5w  // 30min组起始
      };
      constexpr size_t LABEL_GROUP_END[] = {
          L1_FieldOffset::lb_short_5m_20w,  // 5min组结束
          L1_FieldOffset::lb_short_10m_20w, // 10min组结束
          L1_FieldOffset::lb_short_30m_20w  // 30min组结束
      };
      dag_.l0.LabelReturn.compute_minute_anchored(
          t, [&](size_t h, size_t label_l1, const float *values) {
            TS_WRITE_FEATURES(store_, date_str_, 1, label_l1, asset_id_,
                              LABEL_GROUP_START[h], LABEL_GROUP_END[h], values,
                              worker_id_);
          });
    }

    // --- LabelReturn1m: L0 秒级回填 (1min×5w, 只落 long) ---
    dag_.l0.LabelReturn1m.compute(t);
    if (dag_.l0.LabelReturn1m.group_valid(0)) {
      TS_WRITE_SINGLE(store_, date_str_, 0, dag_.l0.LabelReturn1m.group_l0(0), L0_FieldOffset::lb_long_1m_5w,
                      asset_id_, dag_.l0.LabelReturn1m.group_values(0)[0], worker_id_);
    }

    // --- 写入缓冲区 (按 FeaturesDefine.hpp 中的定义顺序) ---
    ts_features_buffer_[L0_FieldOffset::spread] = dag_.l0.Spread_.back();
    ts_features_buffer_[L0_FieldOffset::ci_1] = dag_.l0.Ci_1_.back();
    ts_features_buffer_[L0_FieldOffset::ofi_1] = dag_.l0.Ofi_1_.back();
    ts_features_buffer_[L0_FieldOffset::ofi_5] = dag_.l0.Ofi_5_.back();

    TS_WRITE_SINGLE(store_, date_str_, 0, t, L0_FieldOffset::_depth_valid, asset_id_, 1.0f, worker_id_);

    // --- Write LOB depth snapshot for GUI (分钟频: 同分钟覆盖, 终值=分钟末盘口) ---
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

    DEPTH_WRITE_FEATURES(store_, date_str_, L0_to_L1(t), asset_id_, 0, DepthFieldOffset::_depth_valid, lob_depth_buffer_.data(), worker_id_);
  }

  // Write TS features (只到最后一个 TS 字段; cs_spread_rank 由 CS worker 写)
  TS_WRITE_FEATURES(store_, date_str_, 0, t, asset_id_, 0, L0_FieldOffset::ofi_5, ts_features_buffer_.data(), worker_id_);

  // Write data validity flag
  TS_WRITE_SINGLE(store_, date_str_, 0, t, L0_FieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
  DEPTH_WRITE_SINGLE(store_, date_str_, L0_to_L1(t), DepthFieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
}
