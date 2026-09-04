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

  // 盘口快照输出缓冲区
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

  // 节点调度全部由 NODES 表展开 (行序 = 执行序), 这里只按触发域分发
  if (Trigger_onTaker)
    dag_.run<Trigger::onTaker>();
  if (Trigger_onMaker)
    dag_.run<Trigger::onMaker>();
  if (Trigger_onCancel)
    dag_.run<Trigger::onCancel>();
  if (Trigger_onTick)
    dag_.run<Trigger::onTick>();

  if (Trigger_onDepth) {
    dag_.run<Trigger::onDepth>();

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
      dag_.LabelReturn.compute_minute_anchored(
          t, [&](size_t h, size_t label_l1, const float *values) {
            TS_WRITE_FEATURES(store_, date_str_, 1, label_l1, asset_id_,
                              LABEL_GROUP_START[h], LABEL_GROUP_END[h], values,
                              worker_id_);
          });
    }

    // --- LabelReturn1m: L0 秒级回填 (1min×5w, 只落 long) ---
    dag_.LabelReturn1m.compute(t);
    if (dag_.LabelReturn1m.group_valid(0)) {
      TS_WRITE_SINGLE(store_, date_str_, 0, dag_.LabelReturn1m.group_l0(0), L0_FieldOffset::lb_long_1m_5w,
                      asset_id_, dag_.LabelReturn1m.group_values(0)[0], worker_id_);
    }

    TS_WRITE_SINGLE(store_, date_str_, 0, t, L0_FieldOffset::_depth_valid, asset_id_, 1.0f, worker_id_);

    // --- Write LOB depth snapshot for GUI (分钟频: 同分钟覆盖, 终值=分钟末盘口) ---
    constexpr size_t N = L2::LOB_DEPTH;
    constexpr float VOLUME_TO_LOT = 0.01f; // 股 → 手 (1手=100股)

    for (size_t i = 0; i < N; ++i) {
      lob_depth_buffer_[i] = dag_.DepthData.bid_price[i].back();
      lob_depth_buffer_[N + i] = dag_.DepthData.ask_price[i].back();
      lob_depth_buffer_[2 * N + i] = dag_.DepthData.bid_qty[i].back() * VOLUME_TO_LOT;
      lob_depth_buffer_[3 * N + i] = dag_.DepthData.ask_qty[i].back() * VOLUME_TO_LOT;
    }

    lob_depth_buffer_[4 * N] = dag_.MidPrice.last();
    lob_depth_buffer_[4 * N + 1] = 1.0f;

    DEPTH_WRITE_FEATURES(store_, date_str_, L0_to_L1(t), asset_id_, 0, DepthFieldOffset::_depth_valid, lob_depth_buffer_.data(), worker_id_);
  }

  // Write TS features: 字段表 SRC 列驱动 (值 = 各节点输出口最新值; CS 列由 CS worker 写)
  TS_WRITE_ROW(store_, date_str_, 0, t, asset_id_, dag_, worker_id_);

  // Write data validity flag
  TS_WRITE_SINGLE(store_, date_str_, 0, t, L0_FieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
  DEPTH_WRITE_SINGLE(store_, date_str_, L0_to_L1(t), DepthFieldOffset::_data_valid, asset_id_, 1.0f, worker_id_);
}
