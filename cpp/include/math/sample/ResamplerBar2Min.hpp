#pragma once

#include <cstdint>

#include "features/Bar1mDay.hpp"
#include "features/DataDefine.hpp"

//========================================================================================
// RESAMPLER: BAR1M -> MINUTE (与 ResamplerTick2Min 对仗: 源 = bar1m 分钟 K 而非逐笔)
//========================================================================================
// 职责 / 调用序与 Tick2Min 同:
// - roll(min_end): 出 min_end 之前尚未出的 bar (时钟推进, 见 CoreTs::advance; 本分钟 bar 收进之前 / 收盘)
// - accumulate(bar): 分钟 m 的 bar1m 收进当前分钟 (roll(m) 之后调)
// 【稠密】同约: 每个 L_min 分钟 (0..254) 恰出一根 bar, 无成交分钟 (volume 0 / 无 bar) O=H=L=C=日内最近成交价
//   (日内尚无成交 → 0 = 无价), 量额 0. bar1m 不分主买 / 主卖: 量额全放 bid 侧, ask 侧恒 0 (Trade 节点 = bid + ask).
//   本类纯重采样, 不认识任何特征 / 日频数据.
//========================================================================================

class ResamplerBar2Min {
public:
  explicit ResamplerBar2Min(MinuteData &output)
      : output_(output) {}

  // 分钟翻转: min_end 之前仍有未出 bar 的分钟 → emit 其中最早的一根, 返回 true (调用方循环至 false)
  bool roll(size_t min_end) noexcept { return emit_before(min_end); }

  void reset() noexcept {
    next_min_ = 0;
    bar_ = {};
    last_price_ = 0.0f;
  }

  // 本分钟 bar1m 收进当前分钟 (roll 之后调); 无成交 (volume 0 / 无价) 不收 → 出携带价 bar
  void accumulate(const Bar1m &bar) noexcept {
    if (bar.volume == 0 || bar.close <= 0.0f)
      return;
    bar_ = bar;
    last_price_ = bar.close;
  }

private:
  // next_min_ < min_end → 出 next_min_ 这一根 (有成交用收进的 bar, 无成交用携带价), 推进 next_min_
  bool emit_before(size_t min_end) noexcept {
    if (next_min_ >= min_end) [[likely]]
      return false;
    output_.index = static_cast<uint32_t>(next_min_++);
    const bool traded = bar_.volume > 0;
    const float px = last_price_; // 有成交 = 本 bar close

    output_.open.push_back(traded ? bar_.open : px);
    output_.high.push_back(traded ? bar_.high : px);
    output_.low.push_back(traded ? bar_.low : px);
    output_.close.push_back(px);
    output_.bid_volume.push_back(bar_.volume);
    output_.ask_volume.push_back(0u);
    output_.bid_amount.push_back(bar_.amount);
    output_.ask_amount.push_back(0.0f);

    bar_ = {}; // 下一分钟由收进的 bar 重新开
    return true;
  }

  MinuteData &output_;

  size_t next_min_{0};  // 下一根待出 bar 的 L_min 分钟 (= 当前收 bar 的分钟)
  Bar1m bar_{};         // 本分钟收进的 bar1m (无成交 = 全 0)
  float last_price_{0}; // 日内最近成交价 (盘前 0): 无成交分钟携带价
};
