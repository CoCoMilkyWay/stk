#pragma once

#include "features/Storage/FeatureStore.hpp"
#include "features/DataDefine.hpp"
#include "features/Node/Common/Meta/Meta.hpp" // MetaTracker (ts_valid / cs_valid)
#include "misc/date.hpp"
#include "misc/profiler.hpp"

#include <array>
#include <cassert>
#include <span>
#include <utility>

// ============================================================================
// CoreTs<Derived, T, Dag>: 单资产时序计算的档无关骨架 (CRTP). CoreLob / CoreBar 只写"源单位怎么进 DAG / 重采样器".
//   日:   begin_day  句柄进日环 → onDay → 当日成员位 (member) → 元数据 / 重采样器归零 → cs_valid 盘前写满本档全部层 → 标签日历
//         end_day    advance 到收盘 → 标签收盘结算 → 日末 flush → 归零分钟缓冲
//         empty_day  无数据日 = 零事件日, 同一路径
//   分钟: advance(m)  重采样器逐根出 m 之前未出的 bar, 每根 close_minute: onMinute → 写 L1 行 + ts_valid → 有成交则标签 minute_closed
//   源:   档的 compute_and_store (Lob 每笔 / Bar 每分钟 bar1m) 先 advance 到本单位所在分钟, 再把本单位收进重采样器
//   标签: writer(col, n, l1, values, days_ago) 写 days_ago 天前那日 LB 块内偏移 col 起的 n 列;
//         releaser(days_ago) = 该日标签全部结清, 本资产对该日 ts_close (计数攒齐全轴后 CS 放行).
//   日句柄环与 LabelHold 悬挂日环同步前进 (PEND_DAYS): 开盘型 (T+N) 回填旧日张量, 旧日句柄持到结清才 ts_close.
//   档钩子 (Derived 实现):
//     bool  member(misc::Ymd)  日开 (onDay 之后): 当日成员位 → cs_valid (Lob 票池 / Bar 在市)
//     auto &resampler()        源 → MinuteData 的重采样器 (ResamplerTick2Min / ResamplerBar2Min: reset / roll)
// ============================================================================
template <class Derived, class T, class Dag>
class CoreTs {
public:
  using Store = FeatureStore<T>;
  using Day = typename Store::Day;
  using Label = decltype(Dag::label);
  static constexpr size_t kMinRingSlots = Label::MIN_RING_SLOTS; // 日环下限 (见 LabelHold)

  void begin_day(misc::Ymd date, const Day &day) {
    push_day(day);
    dag_.at_day_start(date);
    meta_.reset();
    meta_.begin_day(self().member(date));
    self().resampler().reset();
    if (const float cs = meta_.cs(); cs != 0.0f)
      [&]<size_t... L>(std::index_sequence<L...>) { (write_cs_valid<L>(cs), ...); }(std::make_index_sequence<LEVEL_COUNT>{});
    dag_.label.day_begin();
  }

  // 收盘: 末分钟 (收盘集合竞价 → L1 254) 没有后续源单位触发结算, 这里 advance 到收盘 (补齐到 254 含)
  void end_day(Store &store) {
    advance(TRADE_MINUTES_PER_DAY);
    dag_.label.day_end(writer(), releaser(store));
    dag_.at_day_end();
    dag_.minute_data.clear();
  }

  // 无数据日 (缺 binary / 无 bar1m) = 零事件日: 走与正常日完全相同的路径 (onDay / 255 根空 bar 逐分钟 onMinute / 标签日历),
  // 各节点按自身的无成交约定给值 —— 本类不认识"缺数据"这个概念, 不特判任何特征.
  void empty_day(misc::Ymd date, const Day &day, Store &store) {
    begin_day(date, day);
    end_day(store);
  }

  // 时钟推进到 L1 分钟 m 起点: 逐分钟结算 m 之前尚未结算的分钟 (事件空窗的分钟一并补齐), 每分钟 close_minute.
  //   回测: 由本资产下一个源单位 (compute_and_store) / 收盘 (end_day) 调.
  //   实盘: feed 层另须在每分钟末 (墙钟) 对全部资产调 advance(m + 1), 再调 CoreCs::compute_and_store(t):
  //         CS 在分钟末级联读 L1 行 m, 不能等本资产下一个源单位才结算 (冷门股可能几分钟无事件 → 读到空行, 回测 ≠ 实盘).
  //   结算时刻不影响值 (onMinute 域只读 MinuteData / 分钟边界), 两种触发写出同一行.
  void advance(size_t m) noexcept {
    while (self().resampler().roll(m)) {
      TraceN("TS_Minute");
      close_minute(dag_.minute_data.l1_index);
    }
  }

  // 回测区间末 (最后一日 end_day 之后): 悬挂日全部按最近收盘价结算并归还句柄
  void finish_all(Store &store) { dag_.label.finish_all(writer(), releaser(store)); }

protected:
  template <class... Args>
  explicit CoreTs(size_t asset_id, Args &&...dag_args) : asset_id_(asset_id), dag_(std::forward<Args>(dag_args)...) {
    dag_.minute_data.asset_id = static_cast<uint32_t>(asset_id);
  }

  // 标签回调 (L0 路径的分钟锚定也用)
  auto writer() {
    return [this](size_t col, size_t n, size_t label_l1, const float *values, size_t days_ago) { write_label(col, n, label_l1, values, days_ago); };
  }
  auto releaser(Store &store) {
    return [this, &store](size_t days_ago) { store.ts_close(day_at(days_ago)); };
  }

  Day day_{}; // 本日写句柄 (= days_[cur_]), 热路径直接用
  size_t asset_id_;
  Dag dag_;
  MetaTracker meta_; // ts_valid / cs_valid 元列状态机 (begin_day 重置 + 给当日成员位; Lob 档每笔另喂 on_tick)

private:
  static constexpr size_t kPendDays = Label::PEND_DAYS;

  Derived &self() { return static_cast<Derived &>(*this); }

  // 分钟 m 结算 (MinuteData 末 bar = 分钟 m): 每分钟都跑 onMinute 并写整行 (L1 稠密); 有成交 = ts_valid 非零 + 标签
  void close_minute(size_t m) {
    const MinuteData &md = dag_.minute_data;
    assert(!md.close.empty() && m == md.l1_index && "close_minute: MinuteData 末 bar 不是分钟 m");
    const bool traded = md.close.back() > 0 && (md.bid_volume.back() + md.ask_volume.back()) > 0;
    dag_.template run<Trigger::onMinute>();
    fstore::ts_write_row<1>(day_, m, asset_id_, dag_);
    fstore::ts_write<1>(day_, m, kL1TsValid, asset_id_, meta_.l1(traded));
    if (traded)
      dag_.label.minute_closed(m, writer());
  }

  // cs_valid: 当日成员位, 整日常量, 与事件无关 → 盘前一次写满本档全部层全部行 (不在截面不写, slot 清零即 0).
  // 逐行存而非每日一个标量: CS 在 t 只读张量第 t 行 (见 CoreCs.hpp 一致性锚)
  template <size_t LVL>
  void write_cs_valid(float cs) {
    if constexpr (T::has(LVL)) {
      constexpr size_t F = field_of<T>(LVL, "cs_valid");
      static_assert(F < T::LEVELS[LVL].field_count, "档的每层都必须有 cs_valid 列");
      for (size_t t = 0; t < T::LEVELS[LVL].rows; ++t)
        fstore::ts_write<LVL>(day_, t, F, asset_id_, cs);
    }
  }

  // ---------------------------------------------------------------- 日句柄环 ----
  // days_[cur_] = 当日 (= day_), days_ago 天前的句柄 = days_[(cur_ − days_ago) mod PEND]; 与 LabelHold::pend_ 下标同步
  void push_day(const Day &day) {
    cur_ = (cur_ + 1) % kPendDays;
    days_[cur_] = day;
    day_ = day;
  }

  const Day &day_at(size_t days_ago) const {
    assert(days_ago < kPendDays);
    const Day &d = days_[(cur_ + kPendDays - days_ago) % kPendDays];
    assert(d && "日句柄环: 该日尚未 begin_day");
    return d;
  }

  // LB 块内偏移 col 起的 n 个连续列写到 days_ago 天前那日的 L1 行 label_l1
  void write_label(size_t col, size_t n, size_t label_l1, const float *values, size_t days_ago) {
    assert(n >= 1 && col + n <= Label::L1_LABEL_COUNT);
    const Day &d = day_at(days_ago);
    for (size_t i = 0; i < n; ++i)
      fstore::ts_write<1>(d, label_l1, kL1LabelBase + col + i, asset_id_, values[i]);
  }

  // 元列按名 / 标签列按类型 (LB) 在字段表里定位; 列数 / 连续性与标签配置对账
  static_assert(T::has(1), "TS 档必须有 L1 (分钟行 / 标签)");
  static constexpr std::span<const FieldInfo> kL1Fields{T::LEVELS[1].fields, T::LEVELS[1].field_count};
  static constexpr size_t kL1TsValid = field_of<T>(1, "ts_valid");
  static_assert(kL1TsValid < T::LEVELS[1].field_count, "L1 必须有 ts_valid 列");
  static constexpr size_t kL1LabelBase = first_of_kind(kL1Fields, FeatureDataType::LB);
  static_assert(count_of_kind(kL1Fields, FeatureDataType::LB) == Label::L1_LABEL_COUNT && kind_contiguous(kL1Fields, FeatureDataType::LB),
                "L1 标签列须与标签配置 L1_LABEL_COUNT 等长且连续");

  std::array<Day, kPendDays> days_{}; // 日句柄环 (开盘型跨日回填 / 延迟 ts_close)
  size_t cur_ = kPendDays - 1;        // 首个 push_day 推到 0
};
