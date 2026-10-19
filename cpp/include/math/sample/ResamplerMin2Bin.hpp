#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>

#include "features/DataDefine.hpp"
#include "features/Storage/FeatureLevels.hpp" // level_end_sec

//========================================================================================
// RESAMPLER: MINUTE -> 本层 BAR (分钟箱层 + L_day 整日箱; 与 ResamplerTick2Min / ResamplerBar2Min 对仗: 源 = 已结算的分钟 bar)
//========================================================================================
// 职责 / 调用序 (CoreTs::close_minute, 每分钟收行之前 / close_row 收本层行时):
// - accumulate(): 刚结算的分钟 bar (MinuteData 末项) 收进本层当前行
// - emit(k):      本层行 k 末分钟 accumulate 之后, 出行 k 一根 bar 并开新行 (行划分见 FeatureLevels.hpp, 箱首箱 15 分钟)
// 分钟 bar 已稠密 (每分钟恰一根), 所以本层每行恰出一根 bar.
// bar 口径: open = 行内首个有成交分钟的 open, high/low 只含有成交分钟, close = 行末分钟 close (日内最近成交价);
//   无成交行 O=H=L=C=日内最近成交价 (日内尚无成交 → 0 = 无价), 量额 0. "有成交" 与 CoreTs::close_minute 同判据.
//   量额宽累计 (uint64 / double): 整日成交量可超 uint32.
//   本类纯重采样, 不认识任何特征 / 日频数据.
//========================================================================================

class ResamplerMin2Bin {
public:
  ResamplerMin2Bin(const MinuteData &input, BinData &output, size_t lvl)
      : input_(input), output_(output), lvl_(lvl) {}

  void reset() noexcept {
    traded_ = false;
    open_ = high_ = low_ = 0.0f;
    bid_volume_ = ask_volume_ = 0;
    bid_amount_ = ask_amount_ = 0.0;
  }

  void accumulate() noexcept {
    const uint32_t bv = input_.bid_volume.back(), av = input_.ask_volume.back();
    if (input_.close.back() > 0.0f && bv + av > 0) {
      const float h = input_.high.back(), l = input_.low.back();
      if (!traded_) {
        open_ = input_.open.back();
        high_ = h;
        low_ = l;
        traded_ = true;
      } else {
        high_ = std::max(high_, h);
        low_ = std::min(low_, l);
      }
    }
    bid_volume_ += bv;
    ask_volume_ += av;
    bid_amount_ += input_.bid_amount.back();
    ask_amount_ += input_.ask_amount.back();
  }

  void emit(size_t k) noexcept {
    assert(level_end_sec(lvl_, k) == level_end_sec(L_min, input_.index) && "emit: MinuteData 末 bar 不是本层行 k 末分钟");
    const float px = input_.close.back();
    output_.index = static_cast<uint32_t>(k);
    output_.open.push_back(traded_ ? open_ : px);
    output_.high.push_back(traded_ ? high_ : px);
    output_.low.push_back(traded_ ? low_ : px);
    output_.close.push_back(px);
    output_.bid_volume.push_back(bid_volume_);
    output_.ask_volume.push_back(ask_volume_);
    output_.bid_amount.push_back(bid_amount_);
    output_.ask_amount.push_back(ask_amount_);
    reset();
  }

private:
  const MinuteData &input_;
  BinData &output_;
  size_t lvl_; // 本层 (L_5min .. L_day)

  bool traded_{false}; // 本行已有成交分钟
  float open_{0}, high_{0}, low_{0};
  uint64_t bid_volume_{0}, ask_volume_{0};
  double bid_amount_{0}, ask_amount_{0};
};
