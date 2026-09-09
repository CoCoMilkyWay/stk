#pragma once

#include <cstdint>

#include "features/DataDefine.hpp"
#include "features/TimeIndex.hpp"

//========================================================================================
// RESAMPLER: TICK -> MINUTE
//========================================================================================
// 职责:
// - 基于 l1_index 变化触发 emit minute bar (roll, 在本笔进入任何算子之前)
// - 累积 OHLC + volume/amount (accumulate, 在本笔跑完 DAG 之后)
// - 收盘后 finish: 末分钟 (收盘集合竞价 → L1 254) 没有后续 tick 触发 roll, 由 end_day 结算
// 两步分开调: 分钟 m 的 bar 与 onMinute 域的结算都必须只看 m 内的事件, 不混入 m+1 的首笔.
// bar 口径: open = 本分钟首笔成交价, high/low 只含本分钟成交; 无成交的分钟不出 bar (L1 行不写).
//========================================================================================

class ResamplerTick2Min {
public:
  ResamplerTick2Min(TickData &input, MinuteData &output)
      : input_(input), output_(output) {}

  // 分钟翻转检测: 本笔属于新分钟且上一分钟有成交 (bar_open_ > 0) → emit, 返回 true
  // 前置条件: input_.l0_index 已由调用方更新; 本笔尚未 accumulate
  bool roll() noexcept {
    const size_t l1_index = L0_to_L1(input_.l0_index);
    if (l1_index == prev_l1_index_) [[likely]]
      return false;
    const bool emitted = bar_open_ > 0.0f;
    if (emitted)
      emit();
    prev_l1_index_ = l1_index;
    return emitted;
  }

  // 收盘: 结算尚未出 bar 的末分钟 (有成交才出), 返回 true
  bool finish() noexcept {
    if (bar_open_ <= 0.0f)
      return false;
    emit();
    return true;
  }

  void reset() noexcept {
    prev_l1_index_ = SIZE_MAX;
    bar_open_ = bar_high_ = bar_low_ = bar_close_ = last_price_ = 0.0f;
    bar_bid_volume_ = bar_ask_volume_ = 0;
    bar_bid_amount_ = bar_ask_amount_ = 0.0f;
  }

  // 本笔累积到当前分钟 bar (roll 之后调)
  void accumulate() noexcept {
    if (input_.lob.order_type != L2::OrderType::TAKER)
      return;

    float price = input_.lob.price;
    if (price <= 0.0f) [[unlikely]] {
      price = last_price_; // 零价成交沿用最近成交价
    }

    const uint32_t volume = input_.lob.volume;
    const bool is_bid = input_.lob.order_dir == L2::OrderDirection::BID;

    if (bar_open_ == 0.0f) [[unlikely]] {
      bar_open_ = price; // 本分钟首笔成交价
      bar_high_ = price;
      bar_low_ = price;
    }
    last_price_ = price;

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

private:
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

    bar_open_ = bar_high_ = bar_low_ = bar_close_ = 0.0f; // 下一分钟由首笔成交重新开 bar
    bar_bid_volume_ = 0;
    bar_ask_volume_ = 0;
    bar_bid_amount_ = 0.0f;
    bar_ask_amount_ = 0.0f;
  }

  TickData &input_;
  MinuteData &output_;

  size_t prev_l1_index_{SIZE_MAX};
  float bar_open_{0}, bar_high_{0}, bar_low_{0}, bar_close_{0};
  float last_price_{0}; // 日内最近成交价 (零价成交回退用)
  uint32_t bar_bid_volume_{0}, bar_ask_volume_{0};
  float bar_bid_amount_{0}, bar_ask_amount_{0};
};
