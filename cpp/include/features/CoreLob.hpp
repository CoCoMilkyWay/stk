#pragma once

#include "features/CoreTs.hpp"
#include "features/LobGraph.hpp"
#include "math/sample/ResamplerTick2Min.hpp"
#include "misc/profiler.hpp"
#include "shared/Universe.hpp"

// ============================================================================
// CoreLob: Lob 档单资产时序计算. LOB → L0 (tick, 秒索引) → resample → L1 (minute). 日 / 分钟 / 标签骨架见 CoreTs.hpp
//   与 CoreBar 对仗: compute_and_store = 本源单位 (逐笔) 先 advance 到所在分钟, 再进 DAG + 重采样器
//   每笔: run<0>(t) 按触发域 (onTaker|onMaker|onCancel → onTick → onDepth) 调 DAG, 写 L0 行 + 标签回填
//   分钟: close_minute(m) 调 onMinute 域, 写 L1 行; 只经 CoreTs::advance (时钟推进) 触发
//   顺序: 本笔先 advance 到本笔所在分钟 (逐分钟结算之前尚未结算的分钟), 再进 DAG (run<0> → accumulate).
//   所以 L1 行 m 的一切 (bar / 累计型 / 盘口采样型) 都只含分钟 m 内的事件 = "分钟 m 末的状态";
//   onMinute 域只读 MinuteData / 分钟边界 (md.l1_index), 不读本笔 —— 结算时刻 (下一笔 / 收盘 / 墙钟) 不影响值.
//   L1 稠密: 255 行每行都跑 onMinute 并写 (无成交分钟 bar 为日内携带价 / 0, 各节点按自身约定兜底, 见 ResamplerTick2Min);
//   缺数据日 = 零事件日, 同一路径 (empty_day). 本类不读任何特征值, 不特判任何节点.
//   ts_valid 只标"本分钟有成交", 标签分钟档 exit 仍只在有成交的分钟推进 (无成交无法平仓).
//   Lob 档 (逐笔 → LOB 源). 节点调度全部由 Lob_NODES 表展开 (行序 = 执行序), 这里只按触发域分发和写回; 手写的只有 FLAG / LABEL 列
//
//   一致性红线: TS 是资产局部纯函数 —— 输入只有本资产的逐笔流 + 日频 PIT
//   (fund::Pool), 不读其他资产 / 全局状态 / CS 结果. 张量行 (t, f, a) 因此只由
//   资产 a 截止秒 t 的事件决定, 对重放调度是不变量: 逐资产串行回测、并行回测、
//   实盘全市场推流, 写出的值相同. 回测结果可迁移到实盘全系于此条, 破了它一切
//   重排立即失效. (算子输入面契约见 DataDefine.hpp)
// ============================================================================
class CoreLob : public CoreTs<CoreLob, lob::Tier, lob::DAG> {
  using Base = CoreTs<CoreLob, lob::Tier, lob::DAG>;
  friend Base;

public:
  // asset_id = 本库 A 轴下标 (= universe::Pool 的子轴下标, 查当日成员位)
  CoreLob(const fund::Pool &fund_pool,
          const universe::Pool &universe_pool,
          const std::string &code_ex,
          size_t asset_id = 0,
          size_t core_id = 0)
      : Base(asset_id, fund_pool, code_ex, asset_id),
        core_id_(core_id),
        universe_pool_(universe_pool),
        tick2min_(dag_.tick_data, dag_.minute_data) {
    dag_.tick_data.asset_id = static_cast<uint32_t>(asset_id_);
    dag_.tick_data.core_id = static_cast<uint32_t>(core_id);
    dag_.minute_data.core_id = static_cast<uint32_t>(core_id);
  }

  // 本资产的 L0 工作区 (DAG_Root 自持, 见 LobGraph.hpp): LOB 经 bind() 把写出口换到这里
  TickData &tick_data() { return dag_.tick_data; }

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

private:
  // ---------------------------------------------------------------- 档钩子 (CoreTs) ----
  // 当日在池: 选池 (universe::Pool, 第 D 日由 all 库 D−1 收盘行定), 与 Fund 吃 fund_pool 同形
  bool member(misc::Ymd date) const { return universe_pool_.in_pool(asset_id_, date); }
  ResamplerTick2Min &resampler() { return tick2min_; }

  // ---------------------------------------------------------------- L0 每笔 ----
  // 与 CoreCrosssection::run<LVL> 对仗: 本笔按触发域 onTaker|onMaker|onCancel → onTick → onDepth 进 DAG → 写 L0 行 + ts_valid,
  // 标签快照 / 锚定. L1 行走 CoreTs::close_minute (advance 内 roll 出 bar 后)
  template <size_t LVL>
  inline void run(size_t t) {
    static_assert(LVL == 0);
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

      // 标签: 盘口快照入环, 然后 L1 分钟锚定捕获 entry (+ 写该行冲击成本列 + 以此为 exit 的分钟档行); 开盘价走分钟 bar (close_minute → minute_closed)
      dag_.label.snapshot(t);
      dag_.label.minute_anchored(t, writer());
    }

    // 逐笔写行, 同秒后一笔覆盖前一笔, 行终值 = 该秒最后一笔后的状态 (事件驱动: 实盘就是边到边写).
    // 不要攒到秒末 / 日末再批量提交 —— 那是回测专属的形态, 会让回测与实盘走两条路.
    fstore::ts_write_row<0>(day_, t, asset_id_, dag_);

    // ts_valid (编码/累积语义见 Meta.hpp): 逐笔覆盖写, 行终值 = 秒内累积值. (cs_valid 整日常量, begin_day 已写)
    // 盘口价只在 depth_updated 时取 (Depth 序列每日清空, 首次更新前为空环)
    meta_.on_tick(t, depth_updated, dag_.MicroPrice.last(),
                  depth_updated ? dag_.Depth.bid_price[0].back() : 0.0f,
                  depth_updated ? dag_.Depth.ask_price[0].back() : 0.0f, lob.price);
    fstore::ts_write<0>(day_, t, lob::L0_Field::ts_valid, asset_id_, meta_.l0());
  }

  size_t core_id_;
  const universe::Pool &universe_pool_; // 选池逐日成员 (只读共享; member 查 (asset_id_, date) → cs_valid)

  ResamplerTick2Min tick2min_;
};
