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
// bar 口径: open = 本分钟首笔成交价, high/low 只含本分钟成交.
// 【稠密】每个 L1 分钟 (0..254) 恰出一根 bar, 无成交分钟 O=H=L=C=日内最近成交价 (日内尚无成交 → 0 = 无价), 量额 0;
//   L1 行因此每分钟都写, 特征稠密无缺口 (0 价 / 零量由各节点按自身约定兜底: Bar 退前收等; ts_valid 仍标"本分钟有成交").
//   本类纯重采样, 不认识任何特征 / 日频数据.
//   roll / finish 每调一次至多出一根 bar (返回 true), 调用方循环 "while (roll()) run_minute()" 直到追平:
//   跨越多分钟的事件空窗 (09:25 撮合后到 09:30 / 停牌 / 14:57 起映射到哨兵秒) 由此逐分钟补齐.
//========================================================================================

class ResamplerTick2Min {
public:
  ResamplerTick2Min(TickData &input, MinuteData &output)
      : input_(input), output_(output) {}

  // 盘前: 分钟游标归零, 携带价归零 (日内首笔成交前的无成交分钟价为 0)
  void begin_day() noexcept {
    next_l1_ = 0;
    last_price_ = 0.0f;
  }

  // 分钟翻转: 本笔所在分钟之前仍有未出 bar 的分钟 → emit 其中最早的一根, 返回 true (调用方循环至 false)
  // 前置条件: input_.l0_index 已由调用方更新; 本笔尚未 accumulate
  bool roll() noexcept { return emit_before(L0_to_L1(input_.l0_index)); }

  // 收盘: 补齐到末分钟 (含) 的全部 bar, 每调一根
  bool finish() noexcept { return emit_before(TRADE_MINUTES_PER_DAY); }

  void reset() noexcept {
    next_l1_ = 0;
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
  // next_l1_ < l1_end → 出 next_l1_ 这一根 (有成交用累积 bar, 无成交用携带价), 推进 next_l1_
  bool emit_before(size_t l1_end) noexcept {
    if (next_l1_ >= l1_end) [[likely]]
      return false;
    output_.l1_index = static_cast<uint32_t>(next_l1_++);
    const bool traded = bar_open_ > 0.0f;
    const float px = traded ? bar_close_ : last_price_;

    output_.open.push_back(traded ? bar_open_ : px);
    output_.high.push_back(traded ? bar_high_ : px);
    output_.low.push_back(traded ? bar_low_ : px);
    output_.close.push_back(px);
    output_.bid_volume.push_back(bar_bid_volume_);
    output_.ask_volume.push_back(bar_ask_volume_);
    output_.bid_amount.push_back(bar_bid_amount_);
    output_.ask_amount.push_back(bar_ask_amount_);

    bar_open_ = bar_high_ = bar_low_ = bar_close_ = 0.0f; // 下一分钟由首笔成交重新开 bar
    bar_bid_volume_ = 0;
    bar_ask_volume_ = 0;
    bar_bid_amount_ = 0.0f;
    bar_ask_amount_ = 0.0f;
    return true;
  }

  TickData &input_;
  MinuteData &output_;

  size_t next_l1_{0}; // 下一根待出 bar 的 L1 分钟 (= 当前累积中的分钟)
  float bar_open_{0}, bar_high_{0}, bar_low_{0}, bar_close_{0};
  float last_price_{0}; // 日内最近成交价 (盘前 0): 零价成交回退 / 无成交分钟携带价
  uint32_t bar_bid_volume_{0}, bar_ask_volume_{0};
  float bar_bid_amount_{0}, bar_ask_amount_{0};
};
