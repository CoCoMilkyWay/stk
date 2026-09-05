#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/ComputeGraph.hpp"
#include "math/sample/ResamplerTick2Min.hpp"
#include "misc/profiler.hpp"

// ============================================================================
// CoreSequential: 单资产时序计算. LOB → L0 (tick, 秒索引) → resample → L1 (minute)
//   每笔: run_tick()  按触发域 (onTaker|onMaker|onCancel → onTick → onDepth) 调 DAG, 写 L0 行 + 标签回填
//   分钟: run_minute() 调 onMinute 域, 写 L1 行
//   节点调度全部由 NODES 表展开 (行序 = 执行序), 这里只按触发域分发和写回; 手写的只有 FLAG / LABEL 列
//
//   一致性红线: TS 是资产局部纯函数 —— 输入只有本资产的逐笔流 + 日频 PIT
//   (fund::Pool), 不读其他资产 / 全局状态 / CS 结果. 张量行 (t, f, a) 因此只由
//   资产 a 截止秒 t 的事件决定, 对重放调度是不变量: 逐资产串行回测、并行回测、
//   实盘全市场推流, 写出的值相同. 回测结果可迁移到实盘全系于此条, 破了它一切
//   重排立即失效. (算子输入面契约见 DataDefine.hpp)
// ============================================================================
class CoreSequential {
public:
  CoreSequential(const fund::Pool &fund_pool,
                 const std::string &asset_code,
                 size_t asset_id = 0,
                 size_t core_id = 0)
      : asset_id_(asset_id),
        core_id_(core_id),
        asset_code_(asset_code),
        dag_(tick_data_, fund_pool, asset_code, asset_id),
        tick2min_(dag_.tick_data, dag_.minute_data) {
    dag_.tick_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.minute_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.tick_data.core_id = static_cast<uint32_t>(core_id);
    dag_.minute_data.core_id = static_cast<uint32_t>(core_id);
  }

  // 本资产的 L0 工作区: 自持而非引用 worker LOB 的 —— DAG 节点引用它 (终身
  // 有效), LOB 经 bind() 把写出口换到这里. 资产因此可在 worker 间转移处置权
  // (负载再平衡), 任意 worker 的 LOB 都能驱动本 core.
  TickData &tick_data() { return tick_data_; }

  // day = worker 本日的写句柄 (store.ts_open, 每 worker 每日一次), 之后
  // 本类的全部写回都是纯指针算术 —— 热路径不再携带 date / worker_id.
  void begin_day(const std::string &date_str, const GlobalFeatureStore::Day &day) {
    day_ = day;
    meta_.reset();
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
  }

private:
  // ---------------------------------------------------------------- L0: 每笔 ----
  inline void run_tick() {
    const size_t t = dag_.tick_data.l0_index;
    const auto &lob = dag_.tick_data.lob;

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

      // 标签: 共享快照, 然后 L1 分钟锚定回填 (组 h 占 GROUP_SIZE 个连续列)
      dag_.LabelReturn.snapshot(t);
      dag_.LabelReturn.minute_anchored(t, [&](size_t h, size_t label_l1, const float *values) {
        const size_t f = kL1LabelBase + h * LabelReturn::GROUP_SIZE;
        fstore::ts_write_range<1>(day_, label_l1, f, f + LabelReturn::GROUP_SIZE - 1, asset_id_, values);
      });
    }

    fstore::ts_write_row<0>(day_, t, asset_id_, dag_);

    // _meta (编码/累积语义见 Meta.hpp): 逐笔覆盖写, 行终值 = 秒内累积值
    meta_.on_tick(t, lob.depth_updated, dag_.MicroPrice.last(), dag_.Depth.bid_price[0].back(), dag_.Depth.ask_price[0].back(), lob.price);
    fstore::ts_write<0>(day_, t, L0_Field::_meta, asset_id_, meta_.l0());
  }

  // ---------------------------------------------------------------- L1: 每分钟 ----
  inline void run_minute() {
    const auto &md = dag_.minute_data;
    if (md.close.empty()) [[unlikely]]
      return;

    const size_t t = md.l1_index;
    const bool valid = md.close.back() > 0 && (md.bid_volume.back() + md.ask_volume.back()) > 0; // 有成交的分钟才算

    if (valid) {
      dag_.run<Trigger::onMinute>();
      fstore::ts_write_row<1>(day_, t, asset_id_, dag_);
    }
    fstore::ts_write<1>(day_, t, L1_Field::_meta, asset_id_, meta_.l1(valid));
  }

  // 标签列定位: 按类型 (LB) 在字段表里找, 不依赖列名; 列数 / 连续性与 LabelReturn 配置对账
  static constexpr size_t kL1LabelBase = first_of_kind(L1_FIELD_INFO, FeatureDataType::LB);
  static_assert(count_of_kind(L1_FIELD_INFO, FeatureDataType::LB) == LabelReturn::L1_LABEL_COUNT && kind_contiguous(L1_FIELD_INFO, FeatureDataType::LB),
                "L1 label columns must be HOLD_COUNT × GROUP_SIZE contiguous");

  GlobalFeatureStore::Day day_{}; // 本日写句柄, begin_day 换入
  size_t asset_id_;
  size_t core_id_;
  std::string asset_code_;

  TickData tick_data_; // 本资产 L0 工作区 (dag_ 引用它, 须先于 dag_ 声明)
  DAG dag_;
  ResamplerTick2Min tick2min_;

  MetaTracker meta_; // _meta 基建列状态机 (编码/累积语义见 Meta.hpp; begin_day 重置)
};
