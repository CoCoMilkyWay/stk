#pragma once

#include <cstdint>

#include "features/DataDefine.hpp"
#include "features/FeaturesDefine.hpp"

//========================================================================================
// RESAMPLER: MINUTE -> HOUR
//========================================================================================
// 职责:
// - 基于 l2_index 变化触发 emit hour bar
// - 累积 OHLC + volume/amount
//========================================================================================

class ResamplerMin2Hour {
public:
  ResamplerMin2Hour(const MinuteData &input, HourData &output)
      : input_(input), output_(output) {}

  // 返回 true 表示新 hour bar 生成
  bool update() noexcept {
    if (input_.open.empty()) [[unlikely]] return false;

    const size_t l2_index = L1_to_L2(input_.l1_index);
    bool emitted = false;

    // 检测 l2_index 变化
    if (l2_index != prev_l2_index_) [[unlikely]] {
      if (prev_l2_index_ != SIZE_MAX && bar_open_ > 0.0f) {
        emit();
        emitted = true;
      }
      prev_l2_index_ = l2_index;
    }

    accumulate();
    return emitted;
  }

  void reset() noexcept {
    prev_l2_index_ = SIZE_MAX;
    bar_open_ = bar_high_ = bar_low_ = bar_close_ = 0.0f;
    bar_bid_volume_ = bar_ask_volume_ = 0;
    bar_bid_amount_ = bar_ask_amount_ = 0.0f;
  }

private:
  void accumulate() noexcept {
    const float o = input_.open.back();
    const float h = input_.high.back();
    const float l = input_.low.back();
    const float c = input_.close.back();

    if (bar_open_ == 0.0f) [[unlikely]] {
      bar_open_ = (bar_close_ > 0.0f) ? bar_close_ : o;
      bar_high_ = bar_open_;
      bar_low_ = bar_open_;
    }

    bar_close_ = c;
    bar_high_ = (h > bar_high_) ? h : bar_high_;
    bar_low_ = (l < bar_low_) ? l : bar_low_;
    bar_bid_volume_ += input_.bid_volume.back();
    bar_ask_volume_ += input_.ask_volume.back();
    bar_bid_amount_ += input_.bid_amount.back();
    bar_ask_amount_ += input_.ask_amount.back();
  }

  void emit() noexcept {
    output_.l2_index = static_cast<uint32_t>(prev_l2_index_);

    output_.open.push_back(bar_open_);
    output_.high.push_back(bar_high_);
    output_.low.push_back(bar_low_);
    output_.close.push_back(bar_close_);
    output_.bid_volume.push_back(bar_bid_volume_);
    output_.ask_volume.push_back(bar_ask_volume_);
    output_.bid_amount.push_back(bar_bid_amount_);
    output_.ask_amount.push_back(bar_ask_amount_);

    bar_open_ = bar_close_;
    bar_high_ = bar_close_;
    bar_low_ = bar_close_;
    bar_bid_volume_ = 0;
    bar_ask_volume_ = 0;
    bar_bid_amount_ = 0.0f;
    bar_ask_amount_ = 0.0f;
  }

  const MinuteData &input_;
  HourData &output_;

  size_t prev_l2_index_{SIZE_MAX};
  float bar_open_{0}, bar_high_{0}, bar_low_{0}, bar_close_{0};
  uint32_t bar_bid_volume_{0}, bar_ask_volume_{0};
  float bar_bid_amount_{0}, bar_ask_amount_{0};
};
