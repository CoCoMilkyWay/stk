#pragma once

#include "features/Backend/FeatureStore.hpp"
#include "features/Bar1mDay.hpp"
#include "features/BarGraph.hpp"
#include "features/CoreTs.hpp"
#include "features/MetaFlag.hpp"

// ============================================================================
// CoreBar: Bar 档单资产时序计算 (与 CoreLob 对仗, 源 = bar1m 分钟 K 而非逐笔; 只有 L1). 日 / 分钟 / 标签骨架见 CoreTs.hpp
//   begin_day / minute × 255 / end_day, 每分钟: 装 bar → close_minute (onMinute → 写 L1 行 + ts_valid → 标签).
//   L1 稠密: 255 行每行都跑 onMinute 并写; 无成交分钟 (volume 0 / 无 bar) O=H=L=C=日内最近成交价 (日内尚无成交 → 0),
//   量额 0 —— 与 ResamplerTick2Min 同约, 各节点按自身约定兜底.
//   缺数据日 (无 bar1m) = 255 根空 bar, 同一路径. 本类不读任何特征值, 不特判任何节点.
//   MinuteData 量额全放 bid 侧 (bar1m 不分主买 / 主卖, ask 侧恒 0), Trade 节点 = bid + ask.
//
//   一致性红线: TS 是资产局部纯函数 —— 输入只有本资产的 bar1m + 日频 PIT (fund::Pool) + 本资产代码,
//   不读其他资产 / 全局状态 / CS 结果. 实盘按分钟收 bar1m 推流, 写出同一行.
// ============================================================================
class CoreBar : public CoreTs<CoreBar, bar::Tier, bar::DAG> {
  using Base = CoreTs<CoreBar, bar::Tier, bar::DAG>;
  friend Base;

public:
  CoreBar(const fund::Pool &fund_pool, const std::string &code_ex, size_t asset_id)
      : Base(asset_id, fund_pool, code_ex, asset_id), bj_(code_ex.ends_with(".BJ")) {}

  // 一日: bars = 本资产当日 255 行 (Bar1mDay::asset; nullptr = 无数据日)
  void run_day(misc::Ymd date, const Day &day, const Bar1m *bars, Store &store) {
    begin_day(date, day);
    for (size_t m = 0; m < TRADE_MINUTES_PER_DAY; ++m)
      minute(m, bars ? bars + m : nullptr);
    end_day(store);
  }

private:
  // ---------------------------------------------------------------- 档钩子 (CoreTs) ----
  // 在市 (cs_valid): 本资产代码 + Fund 当日 PIT 口 (onDay 之后才有)
  float begin_source(misc::Ymd) {
    last_px_ = 0.0f;
    const float *f = dag_.Fund.y;
    const bool in_market = !bj_ && f[Fund::list_age] >= 0.0f && !(f[Fund::delist_age] >= 0.0f) && f[Fund::is_susp] == 0.0f;
    return in_market ? 1.0f : 0.0f;
  }

  void end_source() {}

  float ts_valid_l1(bool traded) const { return traded ? fmeta::pack(false, 1.0f) : 0.0f; }

  // 分钟 m 收盘: bar 入 MinuteData (无成交 → 携带价) → close_minute
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
    close_minute(m);
  }

  bool bj_;              // 北交所: 不在市 (cs_valid 0)
  float last_px_ = 0.0f; // 日内最近成交价 (无成交分钟携带; 盘前 0)
};
