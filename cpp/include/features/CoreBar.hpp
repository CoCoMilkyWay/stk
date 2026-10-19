#pragma once

#include "features/Bar1mDay.hpp"
#include "features/BarGraph.hpp"
#include "features/CoreTs.hpp"
#include "math/sample/ResamplerBar2Min.hpp"

// ============================================================================
// CoreBar: Bar 档单资产时序计算 (源 = bar1m 分钟 K; L_min / 分钟箱层 / L_day). 日 / 分钟 / 分钟箱 / 标签骨架见 CoreTs.hpp
//   与 CoreLob 对仗: compute_and_store = 本源单位 (分钟 m 的 bar1m) 先 advance 到分钟 m, 再收进重采样器
//   L_min 稠密: 255 行每行都跑 onMinute 并写; 无成交分钟 (volume 0 / 无 bar) 由 ResamplerBar2Min 出携带价 bar, 各节点按自身约定兜底.
//   缺数据日 (无 bar1m) = 零事件日 (CoreTs::empty_day), 同一路径. 本类不读任何特征值, 不特判任何节点.
//
//   一致性红线: TS 是资产局部纯函数 —— 输入只有本资产的 bar1m + 日频 PIT (fund::Panel) + 本资产代码,
//   不读其他资产 / 全局状态 / CS 结果. 实盘按分钟收 bar1m 推流, 写出同一行.
// ============================================================================
class CoreBar : public CoreTs<CoreBar, bar::Tier, bar::DAG> {
  using Base = CoreTs<CoreBar, bar::Tier, bar::DAG>;
  friend Base;

public:
  CoreBar(const fund::Panel &fund_panel, const std::string &code_ex, size_t asset_id)
      : Base(asset_id, fund_panel, code_ex, asset_id), bj_(code_ex.ends_with(".BJ")), bar2min_(dag_.minute_data) {}

  // 每分钟一调 (L_min 节拍): 先把时钟推进到分钟 m (逐根结算之前的分钟), 再把本分钟 bar1m 收进重采样器
  void compute_and_store(size_t m, const Bar1m &bar) noexcept {
    advance(m);
    bar2min_.accumulate(bar);
  }

private:
  // ---------------------------------------------------------------- 档钩子 (CoreTs) ----
  // 当日在市: 本资产代码 + Fund 当日 PIT 口 (onDay 之后才有)
  bool member(misc::Ymd) const { return Fund::in_market(dag_.Fund.y, bj_); }
  ResamplerBar2Min &resampler() { return bar2min_; }

  bool bj_; // 北交所: 不在市 (cs_valid 0)

  ResamplerBar2Min bar2min_;
};
