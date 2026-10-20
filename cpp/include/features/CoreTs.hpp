#pragma once

#include "features/DataDefine.hpp"
#include "features/GraphNode.hpp"                     // graph::BIN_DATA (分钟箱层 + L_1day 输入)
#include "features/Node/Common/Label/LabelGroups.hpp" // LABEL_NAME (标签列按名定址)
#include "features/Node/Common/Meta/Meta.hpp"         // MetaTracker (ts_valid / cs_valid)
#include "features/Storage/FeatureStore.hpp"
#include "math/sample/ResamplerMin2Bin.hpp"
#include "misc/date.hpp"
#include "misc/profiler.hpp"

#include <array>
#include <cassert>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

// ============================================================================
// CoreTs<Derived, T, Dag>: 单资产时序计算的档无关骨架 (CRTP). CoreLob / CoreBar 只写"源单位怎么进 DAG / 重采样器".
//   日:   begin_day  句柄进日环 → 日内节点 reset / 日频节点承接 → onDay → 当日成员位 (member) → 元数据 / 重采样器归零 → cs_valid 盘前写满本档全部层 → 标签日历
//         end_day    advance 到收盘 (末分钟级联收 L_1day 行: onClose → onSettle → 写行) → 标签收盘结算 → 日末 flush → 归零分钟 / 分钟箱缓冲 (日 bar 跨天保留)
//         empty_day  无数据日 = 零事件日, 同一路径
//   分钟: advance(m)  重采样器逐根出 m 之前未出的 bar, 每根 close_minute: 分钟 bar + 成交位并入 L_1min 以上各层当前行
//                     → 按收行判据 (for_each_close) 级联 close_row<LVL>, 细 → 粗: 出该层 bar (L_1min 已出) → 该层节拍域 (beat_of; L_1day 另跟 onSettle) → 写该层行 + ts_valid
//                     → 有成交则标签 minute_closed. L_1min / 分钟箱层 / L_1day 同一路径, 两档同一路径
//   源:   档的 compute_and_store (Lob 每笔 / Bar 每分钟 bar1m) 先 advance 到本单位所在分钟, 再把本单位收进重采样器
//   标签: writer(col, n, t_min, values, days_ago) 写 days_ago 天前那日 LB 块内偏移 col 起的 n 列, 落到以 t_min 为行末分钟的各层行
//         (哪层有哪些标签列由字段表定: Lob 只 L_1min; Bar 只 L_5min 及以上, 每层只留本层及以上的持有期组);
//         releaser(days_ago) = 该日标签全部结清, 本资产对该日 ts_close (计数攒齐全轴后 CS 放行).
//   日句柄环与 LabelHold 悬挂日环同步前进 (PEND_DAYS): 开盘型 (T+N) 回填旧日张量, 旧日句柄持到结清才 ts_close.
//   档钩子 (Derived 实现):
//     bool  member(misc::Ymd)  日开 (onDay 之后): 当日成员位 → cs_valid (Lob 池 / Bar 在市)
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
    for (ResamplerMin2Bin &r : min2bin_)
      r.reset();
    if (const float cs = meta_.cs(); cs != 0.0f)
      [&]<size_t... L>(std::index_sequence<L...>) { (write_cs_valid<L>(cs), ...); }(std::make_index_sequence<LEVEL_COUNT>{});
    dag_.label.day_begin();
  }

  // 收盘: 末分钟 (收盘集合竞价 → L_1min 254) 没有后续源单位触发结算, 这里 advance 到收盘 (补齐到 254 含),
  // 末分钟级联写 L_1day 收盘行; 须在标签收盘结算之前 (day_end 可能 ts_close 本日)
  void end_day(Store &store) {
    advance(TRADE_MINUTES_PER_DAY);
    dag_.label.day_end(writer(), releaser(store));
    dag_.at_day_end();
    dag_.minute_data.clear();
    for (BinData graph::Root::*b : graph::BIN_DATA)
      if (b != &graph::Root::day_data)
        (dag_.*b).clear();
  }

  // 无数据日 (缺 binary / 无 bar1m) = 零事件日: 走与正常日完全相同的路径 (onDay / 255 根空 bar 逐分钟级联收行 / 标签日历),
  // 各节点按自身的无成交约定给值 —— 本类不认识"缺数据"这个概念, 不特判任何特征.
  void empty_day(misc::Ymd date, const Day &day, Store &store) {
    begin_day(date, day);
    end_day(store);
  }

  // 时钟推进到 L_1min 分钟 m 起点: 逐分钟结算 m 之前尚未结算的分钟 (事件空窗的分钟一并补齐), 每分钟 close_minute.
  //   回测: 由本资产下一个源单位 (compute_and_store) / 收盘 (end_day) 调.
  //   实盘: feed 层另须在每分钟末 (墙钟) 对全部资产调 advance(m + 1), 再调 CoreCs::compute_and_store(t):
  //         CS 在分钟末级联读 L_1min 行 m (该分钟收行的更粗层另读其行), 不能等本资产下一个源单位才结算 (冷门股可能几分钟无事件 → 读到空行, 回测 ≠ 实盘).
  //   结算时刻不影响值 (onMinute 域只读 MinuteData / 分钟边界), 两种触发写出同一行.
  void advance(size_t m) noexcept {
    while (self().resampler().roll(m)) {
      TraceN("TS_Minute");
      close_minute(dag_.minute_data.index);
    }
  }

  // 回测区间末 (最后一日 end_day 之后): 悬挂日全部按最近收盘价结算并归还句柄
  void finish_all(Store &store) { dag_.label.finish_all(writer(), releaser(store)); }

protected:
  template <class... Args>
  explicit CoreTs(size_t asset_id, Args &&...dag_args)
      : asset_id_(asset_id), dag_(std::forward<Args>(dag_args)...), min2bin_(make_min2bin(dag_, std::make_index_sequence<std::size(graph::BIN_DATA)>{})) {
    dag_.minute_data.asset_id = static_cast<uint32_t>(asset_id);
    for (BinData graph::Root::*b : graph::BIN_DATA)
      (dag_.*b).asset_id = static_cast<uint32_t>(asset_id);
  }

  // 标签回调 (L_sec 路径的分钟锚定也用)
  auto writer() {
    return [this](size_t col, size_t n, size_t label_min, const float *values, size_t days_ago) { write_label(col, n, label_min, values, days_ago); };
  }
  auto releaser(Store &store) {
    return [this, &store](size_t days_ago) { store.ts_close(day_at(days_ago)); };
  }

  Day day_{}; // 本日写句柄 (= days_[cur_]), 热路径直接用
  size_t asset_id_;
  Dag dag_;
  MetaTracker meta_; // ts_valid / cs_valid 元列状态机 (begin_day 重置 + 给当日成员位; Lob 档每笔另喂 on_tick)
  // minute_data → 层 L_5min + i 的 bar (graph::BIN_DATA[i], 分钟箱层 + L_1day; 两档共用, 档钩子 resampler() 只管源 → 分钟)
  std::array<ResamplerMin2Bin, std::size(graph::BIN_DATA)> min2bin_;

private:
  static constexpr size_t kPendDays = Label::PEND_DAYS;

  Derived &self() { return static_cast<Derived &>(*this); }

  template <size_t... I>
  static std::array<ResamplerMin2Bin, std::size(graph::BIN_DATA)> make_min2bin(Dag &d, std::index_sequence<I...>) {
    return {ResamplerMin2Bin(d.minute_data, d.*graph::BIN_DATA[I], L_5min + I)...};
  }

  // 分钟 m 结算 (MinuteData 末 bar = 分钟 m): 分钟 bar + 成交位并入 L_1min 以上各层当前行 → 分钟 m 末秒收行的层逐层 close_row (细 → 粗)
  // → 有成交则标签 (只读 MinuteData, 写标签列; 与各层行无关)
  void close_minute(size_t m) {
    const MinuteData &md = dag_.minute_data;
    assert(!md.close.empty() && m == md.index && "close_minute: MinuteData 末 bar 不是分钟 m");
    const bool traded = md.close.back() > 0 && (md.bid_volume.back() + md.ask_volume.back()) > 0;
    meta_.on_minute(traded);
    for (ResamplerMin2Bin &r : min2bin_)
      r.accumulate();
    for_each_close<L_1min>(level_end_sec(L_1min, m) - 1, [&]<size_t LVL>(size_t r) { close_row<LVL>(r); });
    if (traded)
      dag_.label.minute_closed(m, writer());
  }

  // 层 LVL 收行 r: 出该层 bar (L_1min 的 bar 已由档重采样器出) → 该层节拍域 → 写整行 + ts_valid (每行都写, L_1min 及以上稠密; 有成交 = ts_valid 非零).
  // 档没有该层也照走 (bar / 节拍域节点两档同一路径), 只是不落盘
  template <size_t LVL>
  void close_row(size_t r) {
    if constexpr (LVL != L_1min)
      min2bin_[LVL - L_5min].emit(r);
    dag_.template run<beat_of(LVL)>();
    if constexpr (LVL == L_1day)
      dag_.template run<Trigger::onSettle>();
    const float flag = meta_.flag<LVL>();
    if constexpr (T::has(LVL)) {
      constexpr size_t F = field_of<T>(LVL, "ts_valid");
      static_assert(F < T::LEVELS[LVL].field_count, "L_1min 及以上每层必须有 ts_valid 列");
      fstore::ts_write_row<LVL>(day_, r, asset_id_, dag_);
      fstore::ts_write<LVL>(day_, r, F, asset_id_, flag);
    }
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

  // 分钟 label_min 的标签 (LB 块内偏移 col 起的 n 个连续列) 写到 days_ago 天前那日: 以 label_min 为行末分钟的各层 (细 → 粗) 的那一行,
  // 层里没有的列 (该层滤掉的持有期组) 不写
  void write_label(size_t col, size_t n, size_t label_min, const float *values, size_t days_ago) {
    assert(n >= 1 && col + n <= Label::MIN_LABEL_COUNT);
    const Day &d = day_at(days_ago);
    for_each_close<L_1min>(level_end_sec(L_1min, label_min) - 1, [&]<size_t LVL>(size_t r) {
      if constexpr (kLabCount[LVL] != 0)
        for (size_t i = 0; i < n; ++i)
          if (const size_t f = kLabCol[LVL][col + i]; f != kNoCol)
            fstore::ts_write<LVL>(d, r, f, asset_id_, values[i]);
    });
  }

  // 标签列定址: LB 块偏移 → 层 LVL 的列下标 (本层没有 = kNoCol). 持有期列 hold_col(h) + side 按名 lb_<side>_<name> 找;
  // 块尾的档附带列 (Lob 冲击成本, 偏移 ≥ kHoldCols) = 本层按名对不上的 LB 列, 按序对应, 要么全在要么全无. 对不齐 → 编译期失败
  static constexpr size_t kNoCol = SIZE_MAX;
  static constexpr size_t kHoldCols = Label::HOLD_COUNT * Label::GROUP_SIZE;
  struct LabLayout { // 无默认成员初始化器: 外层类未完整时 (下方静态成员初始化) 不许用
    std::array<std::array<size_t, Label::MIN_LABEL_COUNT>, LEVEL_COUNT> col;
    std::array<size_t, LEVEL_COUNT> count;
    bool ok;
  };
  static constexpr LabLayout kLab = [] {
    constexpr auto is_label_code = [](std::string_view code, size_t h, size_t s) {
      const std::string_view side = Label::SIDE_NAME[s], name = LABEL_NAME[h];
      return code.size() == 3 + side.size() + 1 + name.size() && code.starts_with("lb_") && code.substr(3, side.size()) == side &&
             code[3 + side.size()] == '_' && code.ends_with(name);
    };
    LabLayout L{};
    L.ok = true;
    for (size_t l = 0; l < LEVEL_COUNT; ++l) {
      L.col[l].fill(kNoCol);
      if (!T::has(l))
        continue;
      const std::span<const FieldInfo> fs{T::LEVELS[l].fields, T::LEVELS[l].field_count};
      const size_t base = first_of_kind(fs, FeatureDataType::LB), cnt = count_of_kind(fs, FeatureDataType::LB);
      L.count[l] = cnt;
      if (cnt > Label::MIN_LABEL_COUNT || !kind_contiguous(fs, FeatureDataType::LB)) {
        L.ok = false;
        continue;
      }
      std::array<bool, Label::MIN_LABEL_COUNT> named{}; // 层内第 f − base 个 LB 列已按名对上
      for (size_t f = base; f < base + cnt; ++f)
        for (size_t h = 0; h < Label::HOLD_COUNT; ++h)
          for (size_t s = 0; s < Label::GROUP_SIZE; ++s)
            if (is_label_code(fs[f].code, h, s)) {
              L.ok = L.ok && L.col[l][Label::hold_col(h) + s] == kNoCol;
              L.col[l][Label::hold_col(h) + s] = f;
              named[f - base] = true;
            }
      size_t extra = kHoldCols;
      for (size_t f = base; f < base + cnt; ++f)
        if (!named[f - base]) {
          L.ok = L.ok && extra < Label::MIN_LABEL_COUNT;
          if (extra < Label::MIN_LABEL_COUNT)
            L.col[l][extra++] = f;
        }
      L.ok = L.ok && (extra == kHoldCols || extra == Label::MIN_LABEL_COUNT);
    }
    size_t total = 0;
    for (const size_t c : L.count)
      total += c;
    L.ok = L.ok && total > 0; // 至少一层落标签
    return L;
  }();
  static_assert(kLab.ok, "各层 LB 列须连续, 且每列都能按名 (lb_<side>_<name>) 或块尾附带列 (全在 / 全无) 定址回标签 LB 块");
  static constexpr const auto &kLabCount = kLab.count;
  static constexpr const auto &kLabCol = kLab.col;

  std::array<Day, kPendDays> days_{}; // 日句柄环 (开盘型跨日回填 / 延迟 ts_close)
  size_t cur_ = kPendDays - 1;        // 首个 push_day 推到 0
};
