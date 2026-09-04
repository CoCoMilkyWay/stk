#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/ComputeGraph.hpp"
#include "math/sample/ResamplerTick2Min.hpp"
#include "misc/profiler.hpp"
#include <array>

// ============================================================================
// CoreSequential: 单资产时序计算. LOB → L0 (tick, 秒索引) → resample → L1 (minute)
//   每笔: run_tick()  按触发域 (onTaker|onMaker|onCancel → onTick → onDepth) 调 DAG, 写 L0 行 + 标签回填 + 盘口快照
//   分钟: run_minute() 调 onMinute 域, 写 L1 行 + OHLC META
//   节点调度全部由 NODES 表展开 (行序 = 执行序), 这里只按触发域分发和写回
// ============================================================================
class CoreSequential {
public:
  CoreSequential(TickData &tick_data,
                 GlobalFeatureStore &store,
                 const std::string &asset_code,
                 size_t asset_id = 0,
                 size_t core_id = 0)
      : store_(store),
        asset_id_(asset_id),
        core_id_(core_id),
        asset_code_(asset_code),
        dag_(tick_data, asset_code),
        tick2min_(dag_.tick_data, dag_.minute_data) {
    dag_.tick_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.minute_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.tick_data.core_id = static_cast<uint32_t>(core_id);
    dag_.minute_data.core_id = static_cast<uint32_t>(core_id);
  }

  void begin_day(const std::string &date_str, const float *fund_row) {
    date_str_ = date_str;
    dag_.at_day_start();
    dag_.set_day_fundamental(fund_row);
  }

  void end_day() {
    dag_.at_day_end();
  }

  void reset() {
    tick2min_.reset();
    dag_.minute_data.open.clear();
    dag_.minute_data.high.clear();
    dag_.minute_data.low.clear();
    dag_.minute_data.close.clear();
    dag_.minute_data.bid_volume.clear();
    dag_.minute_data.ask_volume.clear();
    dag_.minute_data.bid_amount.clear();
    dag_.minute_data.ask_amount.clear();
  }

  void compute_and_store() noexcept {
    TraceN("TS");
    TraceColor(C_Cyan);

    dag_.tick_data.l0_index = static_cast<uint32_t>(Clock_to_L0(dag_.tick_data.lob.hour, dag_.tick_data.lob.minute, dag_.tick_data.lob.second));
    {
      TraceN("TS_Tick");
      run_tick();
    }
    if (tick2min_.update()) {
      TraceN("TS_Minute");
      run_minute();
    }
    if (!date_str_.empty())
      store_.ts_update(date_str_, core_id_, asset_id_, dag_.tick_data.l0_index);
  }

private:
  // ---------------------------------------------------------------- L0: 每笔 ----
  inline void run_tick() {
    const size_t t = dag_.tick_data.l0_index;
    const auto &lob = dag_.tick_data.lob;
    const int w = static_cast<int>(core_id_);

    switch (lob.order_type) {
    case L2::OrderType::TAKER:
      dag_.run<Trigger::onTaker>();
      break;
    case L2::OrderType::MAKER:
      dag_.run<Trigger::onMaker>();
      break;
    case L2::OrderType::CANCEL:
      dag_.run<Trigger::onCancel>();
      break;
    default:
      break;
    }
    dag_.run<Trigger::onTick>();

    if (lob.depth_updated) {
      dag_.run<Trigger::onDepth>();

      // 标签: L1 分钟锚定惰性回填 (组 h: 5/10/30min, 每组 4 个连续列)
      dag_.LabelReturn.compute_minute_anchored(t, [&](size_t h, size_t label_l1, const float *values) {
        fstore::ts_write_range<1>(store_, date_str_, label_l1, asset_id_, kLabelGroupBegin[h], kLabelGroupEnd[h], values, w);
      });
      // 标签: L0 秒级回填 (1min × 5w, 只落 long)
      dag_.LabelReturn1m.compute(t);
      if (dag_.LabelReturn1m.group_valid(0))
        fstore::ts_write<0>(store_, date_str_, dag_.LabelReturn1m.group_l0(0), L0_FieldOffset::lb_long_1m_5w, asset_id_, dag_.LabelReturn1m.group_values(0)[0], w);

      fstore::ts_write<0>(store_, date_str_, t, L0_FieldOffset::_depth_valid, asset_id_, 1.0f, w);

      // 盘口快照 (GUI, 分钟频: 同分钟覆盖, 终值 = 分钟末盘口); 布局 = DEPTH_FIELDS 行序
      constexpr size_t N = L2::LOB_DEPTH;
      constexpr float VOLUME_TO_LOT = 0.01f; // 股 → 手
      for (size_t i = 0; i < N; ++i) {
        lob_depth_buffer_[i] = dag_.DepthData.bid_price[i].back();
        lob_depth_buffer_[N + i] = dag_.DepthData.ask_price[i].back();
        lob_depth_buffer_[2 * N + i] = dag_.DepthData.bid_qty[i].back() * VOLUME_TO_LOT;
        lob_depth_buffer_[3 * N + i] = dag_.DepthData.ask_qty[i].back() * VOLUME_TO_LOT;
      }
      lob_depth_buffer_[4 * N] = dag_.MidPrice.last();
      lob_depth_buffer_[4 * N + 1] = 1.0f; // _depth_valid
      fstore::depth_write_range(store_, date_str_, L0_to_L1(t), asset_id_, DepthFieldOffset::_bid_price, DepthFieldOffset::_depth_valid, lob_depth_buffer_.data(), w);
    }

    fstore::ts_write_row<0>(store_, date_str_, t, asset_id_, dag_, w);
    fstore::ts_write<0>(store_, date_str_, t, L0_FieldOffset::_data_valid, asset_id_, 1.0f, w);
    fstore::depth_write(store_, date_str_, L0_to_L1(t), DepthFieldOffset::_data_valid, asset_id_, 1.0f, w);
  }

  // ---------------------------------------------------------------- L1: 每分钟 ----
  inline void run_minute() {
    const auto &md = dag_.minute_data;
    if (md.close.empty()) [[unlikely]]
      return;

    const size_t t = md.l1_index;
    const int w = static_cast<int>(core_id_);
    const float close = md.close.back();
    const float volume = static_cast<float>(md.bid_volume.back() + md.ask_volume.back());
    const bool valid = close > 0 && volume > 0; // [ON MINUTE] 有效分钟数据时更新

    if (valid) {
      dag_.run<Trigger::onMinute>();
      fstore::ts_write_row<1>(store_, date_str_, t, asset_id_, dag_, w);

      // OHLC + volume META (GUI), 价格单位: 分
      fstore::ts_write<1>(store_, date_str_, t, L1_FieldOffset::_ohlc_open, asset_id_, md.open.back() * 100.0f, w);
      fstore::ts_write<1>(store_, date_str_, t, L1_FieldOffset::_ohlc_high, asset_id_, md.high.back() * 100.0f, w);
      fstore::ts_write<1>(store_, date_str_, t, L1_FieldOffset::_ohlc_low, asset_id_, md.low.back() * 100.0f, w);
      fstore::ts_write<1>(store_, date_str_, t, L1_FieldOffset::_ohlc_close, asset_id_, close * 100.0f, w);
      fstore::ts_write<1>(store_, date_str_, t, L1_FieldOffset::_ohlc_volume, asset_id_, volume, w);
    }
    fstore::ts_write<1>(store_, date_str_, t, L1_FieldOffset::_data_valid, asset_id_, valid ? 1.0f : 0.0f, w);
  }

  // L1 标签组 [long_5w, long_20w, short_5w, short_20w] × {5m, 10m, 30m}
  static constexpr size_t kLabelGroupBegin[] = {L1_FieldOffset::lb_long_5m_5w, L1_FieldOffset::lb_long_10m_5w, L1_FieldOffset::lb_long_30m_5w};
  static constexpr size_t kLabelGroupEnd[] = {L1_FieldOffset::lb_short_5m_20w, L1_FieldOffset::lb_short_10m_20w, L1_FieldOffset::lb_short_30m_20w};
  static_assert(sizeof(kLabelGroupBegin) / sizeof(size_t) == LabelReturnOp::hold_count());

  GlobalFeatureStore &store_;
  size_t asset_id_;
  size_t core_id_;
  std::string asset_code_;
  std::string date_str_;

  DAG dag_;
  ResamplerTick2Min tick2min_;

  // 盘口快照输出缓冲: 4 × N 档 + mid + valid (= DEPTH_TOTAL_WIDTH - _data_valid)
  std::array<float, 4 * L2::LOB_DEPTH + 2> lob_depth_buffer_;
};
