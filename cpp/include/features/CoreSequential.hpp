#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/ComputeGraph.hpp"
#include "math/sample/ResamplerTick2Min.hpp"
#include "misc/profiler.hpp"

#include <array>

// ============================================================================
// CoreSequential: 单资产时序计算. LOB → L0 (tick, 秒索引) → resample → L1 (minute)
//   每笔: run_tick()  按触发域 (onTaker|onMaker|onCancel → onTick → onDepth) 调 DAG, 写 L0 行 + 标签回填
//   分钟: run_minute() 调 onMinute 域, 写 L1 行 (末分钟由 end_day 结算, 收盘后再无 tick 触发 roll)
//   顺序: 本笔若跨入新分钟, 先结算上一分钟 (roll → run_minute), 再让本笔进 DAG (run_tick → accumulate).
//   所以 L1 行 m 的一切 (bar / 累计型 / 盘口采样型) 都只含分钟 m 内的事件 = "分钟 m 末的状态".
//   节点调度全部由 NODES 表展开 (行序 = 执行序), 这里只按触发域分发和写回; 手写的只有 FLAG / LABEL 列
//
//   一致性红线: TS 是资产局部纯函数 —— 输入只有本资产的逐笔流 + 日频 PIT
//   (fund::Pool), 不读其他资产 / 全局状态 / CS 结果. 张量行 (t, f, a) 因此只由
//   资产 a 截止秒 t 的事件决定, 对重放调度是不变量: 逐资产串行回测、并行回测、
//   实盘全市场推流, 写出的值相同. 回测结果可迁移到实盘全系于此条, 破了它一切
//   重排立即失效. (算子输入面契约见 DataDefine.hpp)
// ============================================================================
class CoreSequential {
  // 标签回调 (返回类型推导, 须定义在使用点之前): writer(h, l1, values, days_ago) 写 days_ago 天前那日的标签组;
  // releaser(days_ago) = 该日标签全部结清, 本资产对该日 ts_close (计数攒齐全轴后 CS 放行)
  inline auto label_writer() {
    return [this](size_t h, size_t label_l1, const float *values, size_t days_ago) { write_label(h, label_l1, values, days_ago); };
  }
  inline auto day_releaser(GlobalFeatureStore &store) {
    return [this, &store](size_t days_ago) { store.ts_close(day_at(days_ago)); };
  }

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
  // 句柄进日环 (与 LabelReturn 的悬挂日环同步前进): 开盘档标签 (T+N) 要回填旧日张量,
  // 旧日句柄持到该日标签结清才 ts_close (见 end_day / LabelReturn 文件头).
  void begin_day(const std::string &date_str, const GlobalFeatureStore::Day &day) {
    push_day(day);
    meta_.reset();
    dag_.at_day_start(date_str);
    dag_.LabelReturn.day_begin();
  }

  // 收盘: 末分钟 (收盘集合竞价 → L1 254) 没有后续 tick 触发 roll, 这里结算;
  // 标签: 分钟档尾部 / 收盘档 / 到期的开盘档 (见 LabelReturn::day_end), 结清的日 ts_close.
  void end_day(GlobalFeatureStore &store) {
    if (tick2min_.finish())
      run_minute();
    dag_.LabelReturn.day_end(label_writer(), day_releaser(store));
    dag_.at_day_end();
  }

  // 无数据日 (缺 binary): 张量保持默认值, DAG / Fund 不推进 (warm 状态不动), 只走标签日历 ——
  // 悬挂的开盘档照常计龄 / 到期结算, 当日句柄立刻归还.
  void no_data_day(const std::string &date_str, const GlobalFeatureStore::Day &day, GlobalFeatureStore &store) {
    push_day(day);
    dag_.LabelReturn.reset(date_str);
    dag_.LabelReturn.day_begin();
    dag_.LabelReturn.day_end(label_writer(), day_releaser(store));
  }

  // 回测区间末 (最后一日 end_day / no_data_day 之后): 悬挂日全部按最后盘口结算并归还句柄
  void finish_all(GlobalFeatureStore &store) {
    dag_.LabelReturn.finish_all(label_writer(), day_releaser(store));
  }

  void reset() {
    tick2min_.reset();
    dag_.minute_data.clear();
  }

  void compute_and_store() noexcept {
    TraceN("TS");
    TraceColor(C_Cyan);

    dag_.tick_data.l0_index = static_cast<uint32_t>(Clock_to_L0(dag_.tick_data.lob.hour, dag_.tick_data.lob.minute, dag_.tick_data.lob.second));
    if (tick2min_.roll()) [[unlikely]] {
      TraceN("TS_Minute");
      run_minute(); // 上一分钟结算: 本笔尚未进任何节点 / meta_
    }
    {
      TraceN("TS_Tick");
      run_tick();
    }
    tick2min_.accumulate();
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

    const bool depth_updated = lob.depth_updated;
    if (depth_updated) {
      dag_.run<Trigger::onDepth>();

      // 标签: 共享快照, 然后 L1 分钟锚定回填 (组 h 占 GROUP_SIZE 个连续列)
      dag_.LabelReturn.snapshot(t);
      dag_.LabelReturn.minute_anchored(t, label_writer());
    }

    fstore::ts_write_row<0>(day_, t, asset_id_, dag_);

    // _meta (编码/累积语义见 Meta.hpp): 逐笔覆盖写, 行终值 = 秒内累积值.
    // 盘口价只在 depth_updated 时取 (Depth 序列每日清空, 首次更新前为空环)
    meta_.on_tick(t, depth_updated, dag_.MicroPrice.last(),
                  depth_updated ? dag_.Depth.bid_price[0].back() : 0.0f,
                  depth_updated ? dag_.Depth.ask_price[0].back() : 0.0f, lob.price);
    fstore::ts_write<0>(day_, t, L0_Field::_meta, asset_id_, meta_.l0());
  }

  // ---------------------------------------------------------------- 日句柄环 ----
  // days_[cur_] = 当日 (= day_), days_ago 天前的句柄 = days_[(cur_ − days_ago) mod PEND]; 与 LabelReturn::pend_ 下标同步
  static constexpr size_t kPendDays = LabelReturn::PEND_DAYS;

  inline void push_day(const GlobalFeatureStore::Day &day) {
    cur_ = (cur_ + 1) % kPendDays;
    days_[cur_] = day;
    day_ = day;
  }

  inline const GlobalFeatureStore::Day &day_at(size_t days_ago) const {
    assert(days_ago < kPendDays);
    const GlobalFeatureStore::Day &d = days_[(cur_ + kPendDays - days_ago) % kPendDays];
    assert(d && "日句柄环: 该日尚未 begin_day");
    return d;
  }

  // 标签组 h 的 GROUP_SIZE 个连续列写到 days_ago 天前那日的 L1 行 label_l1
  inline void write_label(size_t h, size_t label_l1, const float *values, size_t days_ago) {
    const size_t f = kL1LabelBase + h * LabelReturn::GROUP_SIZE;
    fstore::ts_write_range<1>(day_at(days_ago), label_l1, f, f + LabelReturn::GROUP_SIZE - 1, asset_id_, values);
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

  GlobalFeatureStore::Day day_{};                         // 本日写句柄 (= days_[cur_]), 热路径直接用
  std::array<GlobalFeatureStore::Day, kPendDays> days_{}; // 日句柄环 (开盘档跨日回填 / 延迟 ts_close)
  size_t cur_ = kPendDays - 1;                            // 首个 push_day 推到 0
  size_t asset_id_;
  size_t core_id_;
  std::string asset_code_;

  TickData tick_data_; // 本资产 L0 工作区 (dag_ 引用它, 须先于 dag_ 声明)
  DAG dag_;
  ResamplerTick2Min tick2min_;

  MetaTracker meta_; // _meta 基建列状态机 (编码/累积语义见 Meta.hpp; begin_day 重置)
};
