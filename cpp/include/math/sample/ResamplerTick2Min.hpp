#pragma once

#include <cstdint>

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"

//========================================================================================
// RESAMPLER: TICK -> MINUTE
//========================================================================================
// 职责:
// - 更新 input.l0_index (tick 时间索引)
// - 基于 l1_index 变化触发 emit minute bar
// - 累积 OHLC + volume/amount
//========================================================================================

class ResamplerTick2Min {
public:
  ResamplerTick2Min(TickData &input, MinuteData &output)
      : input_(input), output_(output) {}

  // 返回 true 表示新 minute bar 生成
  // 前置条件: input_.l0_index 已由调用方更新
  bool update() noexcept {
    const size_t l1_index = L0_to_L1(input_.l0_index);
    bool emitted = false;

    // 检测 l1_index 变化
    if (l1_index != prev_l1_index_) [[unlikely]] {
      if (prev_l1_index_ != SIZE_MAX && bar_open_ > 0.0f) {
        emit();
        emitted = true;
      }
      prev_l1_index_ = l1_index;
    }

    accumulate();
    return emitted;
  }

  void reset() noexcept {
    prev_l1_index_ = SIZE_MAX;
    bar_open_ = bar_high_ = bar_low_ = bar_close_ = 0.0f;
    bar_bid_volume_ = bar_ask_volume_ = 0;
    bar_bid_amount_ = bar_ask_amount_ = 0.0f;
  }

private:
  void accumulate() noexcept {
    if (input_.lob.order_type != L2::OrderType::TAKER)
      return;

    float price = input_.lob.price;
    if (price <= 0.0f) [[unlikely]] {
      price = bar_close_;
    }

    const uint32_t volume = input_.lob.volume;
    const bool is_bid = input_.lob.order_dir == L2::OrderDirection::BID;

    if (bar_open_ == 0.0f) [[unlikely]] {
      bar_open_ = (bar_close_ > 0.0f) ? bar_close_ : price;
      bar_high_ = bar_open_;
      bar_low_ = bar_open_;
    }

    bar_close_ = price;
    bar_high_ = (price > bar_high_) ? price : bar_high_;
    bar_low_ = (price < bar_low_) ? price : bar_low_;

    const float amount = price * static_cast<float>(volume);
    if (is_bid) {
      bar_bid_volume_ += volume;
      bar_bid_amount_ += amount;
    } else {
      bar_ask_volume_ += volume;
      bar_ask_amount_ += amount;
    }
  }

  void emit() noexcept {
    output_.l1_index = static_cast<uint32_t>(prev_l1_index_);

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

  TickData &input_;
  MinuteData &output_;

  size_t prev_l1_index_{SIZE_MAX};
  float bar_open_{0}, bar_high_{0}, bar_low_{0}, bar_close_{0};
  uint32_t bar_bid_volume_{0}, bar_ask_volume_{0};
  float bar_bid_amount_{0}, bar_ask_amount_{0};
};
