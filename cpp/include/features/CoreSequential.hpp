#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/ComputeGraph.hpp"
#include "math/sample/ResamplerTick2Min.hpp"
#include "misc/profiler.hpp"
#include <array>

// ============================================================================
// CoreSequential: 单资产时序计算. LOB → L0 (tick, 秒索引) → resample → L1 (minute)
//   每笔: run_tick()  按触发域 (onTaker|onMaker|onCancel → onTick → onDepth) 调 DAG, 写 L0 行 + 标签回填 + DEPTH 快照
//   分钟: run_minute() 调 onMinute 域, 写 L1 行
//   节点调度全部由 NODES 表展开 (行序 = 执行序), 这里只按触发域分发和写回; 手写的只有 FLAG / LABEL / META 列
// ============================================================================
class CoreSequential {
public:
  CoreSequential(TickData &tick_data,
                 GlobalFeatureStore &store,
                 const fund::Pool &fund_pool,
                 const std::string &asset_code,
                 size_t asset_id = 0,
                 size_t core_id = 0)
      : store_(store),
        asset_id_(asset_id),
        core_id_(core_id),
        asset_code_(asset_code),
        dag_(tick_data, fund_pool, asset_code, asset_id),
        tick2min_(dag_.tick_data, dag_.minute_data) {
    dag_.tick_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.minute_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.tick_data.core_id = static_cast<uint32_t>(core_id);
    dag_.minute_data.core_id = static_cast<uint32_t>(core_id);
  }

  void begin_day(const std::string &date_str) {
    date_str_ = date_str;
    dag_.at_day_start(date_str);
  }

  void end_day() {
    dag_.at_day_end();
  }

  void reset() {
    tick2min_.reset();
    dag_.minute_data.clear();
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

      // 标签: 共享快照, 然后 L1 分钟锚定回填 (组 h 占 GROUP_SIZE 个连续列) + L0 秒级回填 (long only)
      dag_.LabelReturn.snapshot(t);
      dag_.LabelReturn.minute_anchored(t, [&](size_t h, size_t label_l1, const float *values) {
        const size_t f = kL1LabelBase + h * LabelReturn::GROUP_SIZE;
        fstore::ts_write_range<1>(store_, date_str_, label_l1, asset_id_, f, f + LabelReturn::GROUP_SIZE - 1, values, w);
      });
      size_t label_l0;
      float label_v;
      if (dag_.LabelReturn.second(t, label_l0, label_v))
        fstore::ts_write<0>(store_, date_str_, label_l0, kL0LabelBase, asset_id_, label_v, w);

      fstore::ts_write<0>(store_, date_str_, t, L0_Field::_depth_valid, asset_id_, 1.0f, w);

      // DEPTH 快照 (GUI, 分钟频: 同分钟覆盖, 终值 = 分钟末盘口); 布局 = DEPTH_FIELDS 行序 (见 Meta.hpp)
      constexpr size_t N = L2::LOB_DEPTH;
      constexpr float VOLUME_TO_LOT = 0.01f; // 股 → 手
      for (size_t i = 0; i < N; ++i) {
        depth_row_[i] = dag_.DepthData.bid_price[i].back();
        depth_row_[N + i] = dag_.DepthData.ask_price[i].back();
        depth_row_[2 * N + i] = dag_.DepthData.bid_qty[i].back() * VOLUME_TO_LOT;
        depth_row_[3 * N + i] = dag_.DepthData.ask_qty[i].back() * VOLUME_TO_LOT;
      }
      depth_row_[4 * N] = dag_.MidPrice.last();
      depth_row_[4 * N + 1] = 1.0f; // _depth_valid
      fstore::ts_write_range<2>(store_, date_str_, L0_to_L1(t), asset_id_, DEPTH_Field::_bid_price, DEPTH_Field::_depth_valid, depth_row_.data(), w);
    }

    fstore::ts_write_row<0>(store_, date_str_, t, asset_id_, dag_, w);
    fstore::ts_write<0>(store_, date_str_, t, L0_Field::_data_valid, asset_id_, 1.0f, w);
    fstore::ts_write<2>(store_, date_str_, L0_to_L1(t), DEPTH_Field::_data_valid, asset_id_, 1.0f, w);
  }

  // ---------------------------------------------------------------- L1: 每分钟 ----
  inline void run_minute() {
    const auto &md = dag_.minute_data;
    if (md.close.empty()) [[unlikely]]
      return;

    const size_t t = md.l1_index;
    const int w = static_cast<int>(core_id_);
    const bool valid = md.close.back() > 0 && (md.bid_volume.back() + md.ask_volume.back()) > 0; // 有成交的分钟才算

    if (valid) {
      dag_.run<Trigger::onMinute>();
      fstore::ts_write_row<1>(store_, date_str_, t, asset_id_, dag_, w);
    }
    fstore::ts_write<1>(store_, date_str_, t, L1_Field::_data_valid, asset_id_, valid ? 1.0f : 0.0f, w);
  }

  // 标签列定位: 按类型 (LB) 在字段表里找, 不依赖列名; 列数 / 连续性与 LabelReturn 配置对账
  static constexpr size_t kL0LabelBase = first_of_kind(L0_FIELD_INFO, FeatureDataType::LB);
  static constexpr size_t kL1LabelBase = first_of_kind(L1_FIELD_INFO, FeatureDataType::LB);
  static_assert(count_of_kind(L0_FIELD_INFO, FeatureDataType::LB) == 1, "L0 has exactly one label column");
  static_assert(count_of_kind(L1_FIELD_INFO, FeatureDataType::LB) == LabelReturn::L1_LABEL_COUNT && kind_contiguous(L1_FIELD_INFO, FeatureDataType::LB),
                "L1 label columns must be HOLD_COUNT × GROUP_SIZE contiguous");

  GlobalFeatureStore &store_;
  size_t asset_id_;
  size_t core_id_;
  std::string asset_code_;
  std::string date_str_;

  DAG dag_;
  ResamplerTick2Min tick2min_;

  // DEPTH 快照行缓冲: 4 × N 档 + mid + _depth_valid (= DEPTH_TOTAL_WIDTH - _data_valid)
  std::array<float, DEPTH_TOTAL_WIDTH - 1> depth_row_;
  static_assert(DEPTH_Field::_data_valid == DEPTH_FIELD_COUNT - 1 && DEPTH_FIELD_OFFSETS[DEPTH_Field::_depth_valid] == 4 * L2::LOB_DEPTH + 1, "DEPTH row layout");
};
