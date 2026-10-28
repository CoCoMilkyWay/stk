#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>

#include "features/DataDefine.hpp"
#include "features/Storage/FeatureLevels.hpp" // level_end_sec

//========================================================================================
// RESAMPLER: MINUTE -> 本层 BAR (分钟箱层 + L_1day 整日箱; 与 ResamplerTick2Min / ResamplerBar2Min 对仗: 源 = 已结算的分钟 bar)
//========================================================================================
// 职责 / 调用序 (CoreTs::close_minute, 每分钟收行之前 / close_row 收本层行时):
// - accumulate(): 刚结算的分钟 bar (MinuteData 末 bar) 收进本层当前行
// - emit(k):      本层行 k 末分钟 accumulate 之后, 出行 k 一根 bar 并开新行 (行划分见 FeatureLevels.hpp, 箱首箱 15 分钟)
// 分钟 bar 已稠密 (每分钟恰一根), 所以本层每行恰出一根 bar.
// bar 口径: open = 行内首个有成交分钟的 open, high/low 只含有成交分钟, close = 行末分钟 close (日内最近成交价);
//   无成交行 O=H=L=C=日内最近成交价 (日内尚无成交 → 0 = 无价), 量额 0. "有成交" = MinuteData::traded.
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
    const MinuteData &m = input_;
    if (m.traded()) {
      if (!traded_) {
        open_ = m.open;
        high_ = m.high;
        low_ = m.low;
        traded_ = true;
      } else {
        high_ = std::max(high_, m.high);
        low_ = std::min(low_, m.low);
      }
    }
    bid_volume_ += m.bid_volume;
    ask_volume_ += m.ask_volume;
    bid_amount_ += m.bid_amount;
    ask_amount_ += m.ask_amount;
  }

  void emit(size_t k) noexcept {
    assert(level_end_sec(lvl_, k) == level_end_sec(L_1min, input_.index) && "emit: MinuteData 末 bar 不是本层行 k 末分钟");
    const float px = input_.close;
    BinData &o = output_;
    o.index = static_cast<uint32_t>(k);
    o.open = traded_ ? open_ : px;
    o.high = traded_ ? high_ : px;
    o.low = traded_ ? low_ : px;
    o.close = px;
    o.bid_volume = bid_volume_;
    o.ask_volume = ask_volume_;
    o.bid_amount = bid_amount_;
    o.ask_amount = ask_amount_;
    reset();
  }

private:
  const MinuteData &input_;
  BinData &output_;
  size_t lvl_; // 本层 (L_5min .. L_1day)

  bool traded_{false}; // 本行已有成交分钟
  float open_{0}, high_{0}, low_{0};
  uint64_t bid_volume_{0}, ask_volume_{0};
  double bid_amount_{0}, ask_amount_{0};
};
