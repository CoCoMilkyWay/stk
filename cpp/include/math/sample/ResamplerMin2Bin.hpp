#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"

//========================================================================================
// RESAMPLER: MINUTE -> n-MINUTE BIN (与 ResamplerTick2Min / ResamplerBar2Min 对仗: 源 = 已结算的分钟 bar)
//========================================================================================
// 职责 / 调用序 (CoreTs::close_minute, 每分钟 onMinute 之后):
// - accumulate(): 刚结算的分钟 bar (MinuteData 末项) 收进当前箱
// - emit(k):      箱 k 末分钟 accumulate 之后, 出箱 k 一根 bar 并开新箱 (箱划分见 TimeIndex.hpp, 首箱 15 分钟)
// 分钟 bar 已稠密 (每分钟恰一根), 所以每箱 (0 .. bins_per_day(n) - 1) 恰出一根 bar.
// bar 口径: open = 箱内首个有成交分钟的 open, high/low 只含有成交分钟, close = 箱末分钟 close (日内最近成交价);
//   无成交箱 O=H=L=C=日内最近成交价 (日内尚无成交 → 0 = 无价), 量额 0. "有成交" 与 CoreTs::close_minute 同判据.
//   本类纯重采样, 不认识任何特征 / 日频数据.
//========================================================================================

class ResamplerMin2Bin {
public:
  ResamplerMin2Bin(const MinuteData &input, BinData &output, size_t n)
      : input_(input), output_(output), n_(n) {}

  void reset() noexcept {
    traded_ = false;
    open_ = high_ = low_ = 0.0f;
    bid_volume_ = ask_volume_ = 0;
    bid_amount_ = ask_amount_ = 0.0f;
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
    assert(bin_last_min(n_, k) == input_.min_index && "emit: MinuteData 末 bar 不是箱 k 末分钟");
    const float px = input_.close.back();
    output_.bin_index = static_cast<uint32_t>(k);
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
  size_t n_; // 箱长 (分钟)

  bool traded_{false}; // 本箱已有成交分钟
  float open_{0}, high_{0}, low_{0};
  uint32_t bid_volume_{0}, ask_volume_{0};
  float bid_amount_{0}, ask_amount_{0};
};
