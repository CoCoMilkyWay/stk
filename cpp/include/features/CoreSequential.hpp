#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/ComputeGraph.hpp"
#include "math/sample/ResamplerTick2Min.hpp"
#include "misc/profiler.hpp"
#include "shared/Universe.hpp"

#include <array>

// ============================================================================
// CoreSequential: 单资产时序计算. LOB → L0 (tick, 秒索引) → resample → L1 (minute)
//   与 CoreCrosssection 对仗: begin_day / compute_and_store (L0 节拍, 级联 L1) / end_day, 逐层 run<0> / run<1>
//   每笔: run<0>(t) 按触发域 (onTaker|onMaker|onCancel → onTick → onDepth) 调 DAG, 写 L0 行 + 标签回填
//   分钟: run<1>(m) 调 onMinute 域, 写 L1 行; 只经 advance (时钟推进) 触发
//   顺序: 本笔先 advance 到本笔所在分钟 (逐分钟结算之前尚未结算的分钟), 再进 DAG (run<0> → accumulate).
//   所以 L1 行 m 的一切 (bar / 累计型 / 盘口采样型) 都只含分钟 m 内的事件 = "分钟 m 末的状态";
//   onMinute 域只读 MinuteData / 分钟边界 (md.l1_index), 不读本笔 —— 结算时刻 (下一笔 / 收盘 / 墙钟) 不影响值.
//   L1 稠密: 255 行每行都跑 onMinute 并写 (无成交分钟 bar 为日内携带价 / 0, 各节点按自身约定兜底, 见 ResamplerTick2Min);
//   缺数据日 = 零事件日, 同一路径 (empty_day). 本类不读任何特征值, 不特判任何节点.
//   ts_valid 只标"本分钟有成交", 标签分钟档 exit 仍只在有成交的分钟推进 (无成交无法平仓).
//   Pool 档 (逐笔 → LOB 源). 节点调度全部由 Pool_NODES 表展开 (行序 = 执行序), 这里只按触发域分发和写回; 手写的只有 FLAG / LABEL 列
//
//   一致性红线: TS 是资产局部纯函数 —— 输入只有本资产的逐笔流 + 日频 PIT
//   (fund::Pool), 不读其他资产 / 全局状态 / CS 结果. 张量行 (t, f, a) 因此只由
//   资产 a 截止秒 t 的事件决定, 对重放调度是不变量: 逐资产串行回测、并行回测、
//   实盘全市场推流, 写出的值相同. 回测结果可迁移到实盘全系于此条, 破了它一切
//   重排立即失效. (算子输入面契约见 DataDefine.hpp)
// ============================================================================
class CoreSequential {
  // 标签回调 (返回类型推导, 须定义在使用点之前): writer(col, n, l1, values, days_ago) 写 days_ago 天前那日 LB 块内偏移 col 起的 n 列;
  // releaser(days_ago) = 该日标签全部结清, 本资产对该日 ts_close (计数攒齐全轴后 CS 放行)
  inline auto label_writer() {
    return [this](size_t col, size_t n, size_t label_l1, const float *values, size_t days_ago) { write_label(col, n, label_l1, values, days_ago); };
  }
  inline auto day_releaser(PoolFeatureStore &store) {
    return [this, &store](size_t days_ago) { store.ts_close(day_at(days_ago)); };
  }

public:
  // code_ex = "CODE.EX" (universe::Pool / AssetAxis 的键; 6 位裸码查池查不到 → cs_valid 全 0)
  CoreSequential(const fund::Pool &fund_pool,
                 const universe::Pool &universe_pool,
                 const std::string &code_ex,
                 size_t asset_id = 0,
                 size_t core_id = 0)
      : asset_id_(asset_id),
        core_id_(core_id),
        code_ex_(code_ex),
        universe_pool_(universe_pool),
        dag_(tick_data_, fund_pool, code_ex, asset_id),
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
  void begin_day(misc::Ymd date, const PoolFeatureStore::Day &day) {
    push_day(day);
    meta_.reset();
    // 当日池成员位: 日频 PIT 名单 (universe::Pool, 盘后 python 产出, 第 D 日由 ≤ D−1 数据定), 与 Fund 吃 fund_pool 同形
    meta_.begin_day(universe_pool_.in_pool(code_ex_, date));
    // cs_valid: 当日池成员位, 整日常量, 与事件无关 → 盘前一次写满两层全部行 (池外不写, slot 清零即 0).
    // 逐行存而非每日一个标量: CS 在 t 只读张量第 t 行 (见 CoreCrosssection.hpp 一致性锚); L0 也写 (预留层, 见 FeaturesDefine.hpp TIER_LEVELS_Pool)
    if (const float cs = meta_.cs(); cs != 0.0f) {
      for (size_t t = 0; t < pool::LEVELS[0].rows; ++t)
        fstore::ts_write<0>(day_, t, pool::L0_Field::cs_valid, asset_id_, cs);
      for (size_t t = 0; t < pool::LEVELS[1].rows; ++t)
        fstore::ts_write<1>(day_, t, pool::L1_Field::cs_valid, asset_id_, cs);
    }
    dag_.at_day_start(date);
    tick2min_.begin_day();
    dag_.LabelReturn.day_begin();
  }

  // 收盘: 末分钟 (收盘集合竞价 → L1 254) 没有后续 tick 触发结算, 这里 advance 到收盘 (补齐到 254 含);
  // 标签: 分钟档尾部 / 收盘档 / 到期的开盘档 (见 LabelReturn::day_end), 结清的日 ts_close.
  // 末尾归零重采样器与分钟缓冲: begin_day / end_day 成对即一日完整生命周期.
  void end_day(PoolFeatureStore &store) {
    advance(TRADE_MINUTES_PER_DAY);
    dag_.LabelReturn.day_end(label_writer(), day_releaser(store));
    dag_.at_day_end();
    tick2min_.reset();
    dag_.minute_data.clear();
  }

  // 无数据日 (缺 binary) = 零事件日: 走与正常日完全相同的路径 (onDay / 255 根空 bar 逐分钟 onMinute / 标签日历),
  // 各节点按自身的无成交约定给值 —— 本类不认识"缺数据"这个概念, 不特判任何特征.
  void empty_day(misc::Ymd date, const PoolFeatureStore::Day &day, PoolFeatureStore &store) {
    begin_day(date, day);
    end_day(store);
  }

  // 回测区间末 (最后一日 end_day 之后): 悬挂日全部按最后盘口结算并归还句柄
  void finish_all(PoolFeatureStore &store) {
    dag_.LabelReturn.finish_all(label_writer(), day_releaser(store));
  }

  // 每笔一调 (L0 节拍): 先把时钟推进到本笔所在分钟 (本笔尚未进任何节点 / meta_), 再让本笔进 DAG
  void compute_and_store() noexcept {
    TraceN("TS");
    TraceColor(C_Cyan);

    const size_t t = Clock_to_L0(dag_.tick_data.lob.hour, dag_.tick_data.lob.minute, dag_.tick_data.lob.second);
    dag_.tick_data.l0_index = static_cast<uint32_t>(t);
    advance(L0_to_L1(t));
    run<0>(t);
    tick2min_.accumulate();
  }

  // 时钟推进到 L1 分钟 m 起点: 逐分钟结算 m 之前尚未结算的分钟 (事件空窗的分钟一并补齐), 每分钟 run<1>.
  //   回测: 由本资产下一笔 (compute_and_store) / 收盘 (end_day) 调.
  //   实盘: feed 层另须在每分钟末 (墙钟) 对全部资产调 advance(m + 1), 再调 CoreCrosssection::compute_and_store(t):
  //         CS 在分钟末级联读 L1 行 m, 不能等本资产下一笔才结算 (冷门股可能几分钟无事件 → 读到空行, 回测 ≠ 实盘).
  //   结算时刻不影响值 (见文件头), 两种触发写出同一行.
  void advance(size_t m) noexcept {
    while (tick2min_.roll(m))
      run<1>(dag_.minute_data.l1_index);
  }

private:
  // ---------------------------------------------------------------- 逐层: L0 每笔 / L1 每分钟 ----
  // 与 CoreCrosssection::run<LVL> 对仗: 本层触发域进 DAG → 写本层行 + ts_valid
  //   L0 (t = 秒):   本笔按触发域 onTaker|onMaker|onCancel → onTick → onDepth, 标签快照 / 锚定
  //   L1 (t = 分钟): 刚出 bar 的分钟 (advance 内 roll 返回 true 后), 每分钟都跑 onMinute 并写整行 (稠密);
  //                  有成交 = ts_valid 非零 + 标签开盘价截取
  template <size_t LVL>
  inline void run(size_t t) {
    if constexpr (LVL == 0) {
      TraceN("TS_Tick");
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

        // 标签: 盘口快照入环, 然后 L1 分钟锚定捕获 entry (+ 写该行冲击成本列 + 以此为 exit 的分钟档行); 开盘价走分钟 bar (run<1> → minute_closed)
        dag_.LabelReturn.snapshot(t);
        dag_.LabelReturn.minute_anchored(t, label_writer());
      }

      // 逐笔写行, 同秒后一笔覆盖前一笔, 行终值 = 该秒最后一笔后的状态 (事件驱动: 实盘就是边到边写).
      // 不要攒到秒末 / 日末再批量提交 —— 那是回测专属的形态, 会让回测与实盘走两条路.
      fstore::ts_write_row<0>(day_, t, asset_id_, dag_);

      // ts_valid (编码/累积语义见 Meta.hpp): 逐笔覆盖写, 行终值 = 秒内累积值. (cs_valid 整日常量, begin_day 已写)
      // 盘口价只在 depth_updated 时取 (Depth 序列每日清空, 首次更新前为空环)
      meta_.on_tick(t, depth_updated, dag_.MicroPrice.last(),
                    depth_updated ? dag_.Depth.bid_price[0].back() : 0.0f,
                    depth_updated ? dag_.Depth.ask_price[0].back() : 0.0f, lob.price);
      fstore::ts_write<0>(day_, t, pool::L0_Field::ts_valid, asset_id_, meta_.l0());
    } else {
      TraceN("TS_Minute");
      const auto &md = dag_.minute_data;
      assert(!md.close.empty() && t == md.l1_index && "run<1> 只在 advance 内 roll 出 bar 后调");
      const bool traded = md.close.back() > 0 && (md.bid_volume.back() + md.ask_volume.back()) > 0;

      dag_.run<Trigger::onMinute>();
      fstore::ts_write_row<1>(day_, t, asset_id_, dag_);
      fstore::ts_write<1>(day_, t, pool::L1_Field::ts_valid, asset_id_, meta_.l1(traded));
      // 标签开盘价 (开盘档 exit): 只在有成交的分钟截 (09:25 撮合价 / 首个连续竞价 bar 首笔)
      if (traded)
        dag_.LabelReturn.minute_closed(t);
    }
  }

  // ---------------------------------------------------------------- 日句柄环 ----
  // days_[cur_] = 当日 (= day_), days_ago 天前的句柄 = days_[(cur_ − days_ago) mod PEND]; 与 LabelReturn::pend_ 下标同步
  static constexpr size_t kPendDays = LabelReturn::PEND_DAYS;

  inline void push_day(const PoolFeatureStore::Day &day) {
    cur_ = (cur_ + 1) % kPendDays;
    days_[cur_] = day;
    day_ = day;
  }

  inline const PoolFeatureStore::Day &day_at(size_t days_ago) const {
    assert(days_ago < kPendDays);
    const PoolFeatureStore::Day &d = days_[(cur_ + kPendDays - days_ago) % kPendDays];
    assert(d && "日句柄环: 该日尚未 begin_day");
    return d;
  }

  // LB 块内偏移 col 起的 n 个连续列写到 days_ago 天前那日的 L1 行 label_l1 (hold 组 [long, short] / 冲击成本列)
  inline void write_label(size_t col, size_t n, size_t label_l1, const float *values, size_t days_ago) {
    assert(n >= 1 && col + n <= LabelReturn::L1_LABEL_COUNT);
    const PoolFeatureStore::Day &d = day_at(days_ago);
    for (size_t i = 0; i < n; ++i)
      fstore::ts_write<1>(d, label_l1, kL1LabelBase + col + i, asset_id_, values[i]);
  }

  // 标签列定位: 按类型 (LB) 在字段表里找, 不依赖列名; 列数 / 连续性与 LabelReturn 配置对账
  static constexpr size_t kL1LabelBase = first_of_kind(pool::L1_FIELD_INFO, FeatureDataType::LB);
  static_assert(count_of_kind(pool::L1_FIELD_INFO, FeatureDataType::LB) == LabelReturn::L1_LABEL_COUNT && kind_contiguous(pool::L1_FIELD_INFO, FeatureDataType::LB),
                "L1 label columns must be HOLD_COUNT × GROUP_SIZE + COST_COUNT contiguous");

  PoolFeatureStore::Day day_{};                         // 本日写句柄 (= days_[cur_]), 热路径直接用
  std::array<PoolFeatureStore::Day, kPendDays> days_{}; // 日句柄环 (开盘档跨日回填 / 延迟 ts_close)
  size_t cur_ = kPendDays - 1;                          // 首个 push_day 推到 0
  size_t asset_id_;
  size_t core_id_;
  std::string code_ex_;                 // "CODE.EX" (universe::Pool 的键)
  const universe::Pool &universe_pool_; // 动态池逐日名单 (只读共享; begin_day 查 (code_ex_, date) → cs_valid)

  TickData tick_data_; // 本资产 L0 工作区 (dag_ 引用它, 须先于 dag_ 声明)
  pool::DAG dag_;
  ResamplerTick2Min tick2min_;

  MetaTracker meta_; // ts_valid / cs_valid 基建列状态机 (编码/累积语义见 Meta.hpp; begin_day 重置 + 给当日池成员位)
};
