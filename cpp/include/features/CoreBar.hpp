#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/Bar1mDay.hpp"
#include "features/BarGraph.hpp"
#include "features/MetaFlag.hpp"

#include <array>

// ============================================================================
// CoreBar: All 档单资产时序计算 (与 CoreSequential 对仗, 源 = bar1m 分钟 K 而非逐笔; 只有 L1)
//   begin_day / minute × 255 / end_day, 每分钟: 装 bar → run<onMinute> → 写 L1 行 + ts_valid → 标签.
//   L1 稠密: 255 行每行都跑 onMinute 并写; 无成交分钟 (volume 0 / 无 bar) O=H=L=C=日内最近成交价 (日内尚无成交 → 0),
//   量额 0 —— 与 ResamplerTick2Min 同约, 各节点按自身约定兜底.
//   缺数据日 (无 bar1m) = 255 根空 bar, 同一路径. 本类不读任何特征值, 不特判任何节点.
//   MinuteData 量额全放 bid 侧 (bar1m 不分主买 / 主卖, ask 侧恒 0), Trade 节点 = bid + ask.
//
//   一致性红线: TS 是资产局部纯函数 —— 输入只有本资产的 bar1m + 日频 PIT (fund::Pool) + 本资产代码,
//   不读其他资产 / 全局状态 / CS 结果. 实盘按分钟收 bar1m 推流, 写出同一行.
// ============================================================================
class CoreBar {
  inline auto label_writer() {
    return [this](size_t col, size_t n, size_t label_l1, const float *values, size_t days_ago) { write_label(col, n, label_l1, values, days_ago); };
  }
  inline auto day_releaser(AllFeatureStore &store) {
    return [this, &store](size_t days_ago) { store.ts_close(day_at(days_ago)); };
  }

public:
  CoreBar(const fund::Pool &fund_pool, const std::string &code_ex, size_t asset_id)
      : asset_id_(asset_id), bj_(code_ex.ends_with(".BJ")), dag_(fund_pool, code_ex, asset_id) {
    dag_.minute_data.asset_id = static_cast<uint32_t>(asset_id_);
  }

  // 一日: bars = 本资产当日 255 行 (Bar1mDay::asset; nullptr = 无数据日)
  void run_day(misc::Ymd date, const AllFeatureStore::Day &day, const Bar1m *bars, AllFeatureStore &store) {
    begin_day(date, day);
    for (size_t m = 0; m < TRADE_MINUTES_PER_DAY; ++m)
      minute(m, bars ? bars + m : nullptr);
    end_day(store);
  }

  // 回测区间末 (最后一日 end_day 之后): 悬挂日全部按最近收盘价结算并归还句柄
  void finish_all(AllFeatureStore &store) {
    dag_.LabelBar.finish_all(label_writer(), day_releaser(store));
  }

private:
  // 句柄进日环 (与 LabelBar 悬挂日环同步前进) → onDay → cs_valid 盘前写满 (在市: 本资产代码 + Fund 当日 PIT 口)
  void begin_day(misc::Ymd date, const AllFeatureStore::Day &day) {
    push_day(day);
    dag_.at_day_start(date);
    const float *f = dag_.Fund.y;
    const bool in_market = !bj_ && f[Fund::list_age] >= 0.0f && !(f[Fund::delist_age] >= 0.0f) && f[Fund::is_susp] == 0.0f;
    if (in_market)
      for (size_t t = 0; t < all::LEVELS[1].rows; ++t)
        fstore::ts_write<1>(day_, t, all::L1_Field::cs_valid, asset_id_, 1.0f);
    last_px_ = 0.0f;
    dag_.LabelBar.day_begin();
  }

  // 分钟 m 收盘: bar 入 MinuteData (无成交 → 携带价) → onMinute → 写行; 有成交 → 标签
  void minute(size_t m, const Bar1m *bar) {
    auto &md = dag_.minute_data;
    const bool traded = bar && bar->volume > 0 && bar->close > 0.0f;
    if (traded)
      last_px_ = bar->close;
    md.l1_index = static_cast<uint32_t>(m);
    md.open.push_back(traded ? bar->open : last_px_);
    md.high.push_back(traded ? bar->high : last_px_);
    md.low.push_back(traded ? bar->low : last_px_);
    md.close.push_back(last_px_);
    md.bid_volume.push_back(traded ? bar->volume : 0u);
    md.ask_volume.push_back(0u);
    md.bid_amount.push_back(traded ? bar->amount : 0.0f);
    md.ask_amount.push_back(0.0f);

    dag_.run<Trigger::onMinute>();
    fstore::ts_write_row<1>(day_, m, asset_id_, dag_);
    fstore::ts_write<1>(day_, m, all::L1_Field::ts_valid, asset_id_, traded ? fmeta::pack(false, 1.0f) : 0.0f);
    if (traded)
      dag_.LabelBar.minute_closed(m, label_writer());
  }

  // 收盘: 标签 (分钟档尾部 / 收盘档 / 到期的开盘档, 结清的日 ts_close) → 归零分钟缓冲
  void end_day(AllFeatureStore &store) {
    dag_.LabelBar.day_end(label_writer(), day_releaser(store));
    dag_.at_day_end();
    dag_.minute_data.clear();
  }

  // ---------------------------------------------------------------- 日句柄环 ----
  // days_[cur_] = 当日 (= day_), days_ago 天前的句柄 = days_[(cur_ − days_ago) mod PEND]; 与 LabelBar::pend_ 下标同步
  static constexpr size_t kPendDays = LabelBar::PEND_DAYS;

  inline void push_day(const AllFeatureStore::Day &day) {
    cur_ = (cur_ + 1) % kPendDays;
    days_[cur_] = day;
    day_ = day;
  }

  inline const AllFeatureStore::Day &day_at(size_t days_ago) const {
    assert(days_ago < kPendDays);
    const AllFeatureStore::Day &d = days_[(cur_ + kPendDays - days_ago) % kPendDays];
    assert(d && "日句柄环: 该日尚未 begin_day");
    return d;
  }

  inline void write_label(size_t col, size_t n, size_t label_l1, const float *values, size_t days_ago) {
    assert(n >= 1 && col + n <= LabelBar::L1_LABEL_COUNT);
    const AllFeatureStore::Day &d = day_at(days_ago);
    for (size_t i = 0; i < n; ++i)
      fstore::ts_write<1>(d, label_l1, kL1LabelBase + col + i, asset_id_, values[i]);
  }

  // 标签列定位: 按类型 (LB) 在字段表里找, 不依赖列名; 列数 / 连续性与 LabelBar 配置对账
  static constexpr size_t kL1LabelBase = first_of_kind(all::L1_FIELD_INFO, FeatureDataType::LB);
  static_assert(count_of_kind(all::L1_FIELD_INFO, FeatureDataType::LB) == LabelBar::L1_LABEL_COUNT && kind_contiguous(all::L1_FIELD_INFO, FeatureDataType::LB),
                "All L1 label columns must be HOLD_COUNT × GROUP_SIZE contiguous");

  AllFeatureStore::Day day_{};                         // 本日写句柄 (= days_[cur_])
  std::array<AllFeatureStore::Day, kPendDays> days_{}; // 日句柄环 (开盘档跨日回填 / 延迟 ts_close)
  size_t cur_ = kPendDays - 1;                         // 首个 push_day 推到 0
  size_t asset_id_;
  bool bj_;              // 北交所: 不在市 (cs_valid 0)
  float last_px_ = 0.0f; // 日内最近成交价 (无成交分钟携带; 盘前 0)

  all::DAG dag_;
};
