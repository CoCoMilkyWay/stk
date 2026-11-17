#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>

#include "features/Bar1mDay.hpp"
#include "features/DataDefine.hpp"
#include "features/Storage/FeatureLevels.hpp" // level_end_sec

//========================================================================================
// RESAMPLE: 源 → 稠密行 bar 的唯一核
//========================================================================================
// BarAcc<Vol, Amt>  行内累加器 (幺半群): OHLC = 首个有成交输入 open / 有成交输入极值 / 行末价,
//                   量额全输入加总 (无成交输入只可能有量额贡献: 零价成交, 见 ResamplerTick2Min).
// emit(o, i, px)    出行 i 一根 bar: 有成交用累积值, 无成交 O=H=L=C=px 携带价 (日内尚无成交 → 0), 量额 = 累积.
//                   px = 日内最近成交价 (行末时刻口径, 各源自带的最近 close). 三源 emit 口径完全同一.
//
// 三个推进器, 只差源:
//   ResamplerTick2Min  逐笔 (TickData) → MinuteData: roll(min_end) 每调一次出 min_end 前一根未出的 bar
//                      (时钟推进, 见 CoreTs::advance; 本笔进入任何节点之前 / 收盘 / 实盘墙钟),
//                      accumulate() 本笔成交收进当前分钟 (roll 之后调). 两步分开调:
//                      分钟 m 的 bar 与 onMinute 域结算都只看 m 内的事件, 不混入 m+1 的首笔.
//                      【稠密】每个 L_1min 分钟 (0..254) 恰出一根; roll 循环至追平 → 停牌 / 空窗逐分钟补齐.
//   ResamplerBar2Min   bar1m 分钟 K → MinuteData: 调用序同 (roll 后 accumulate(bar)); bar1m 不分主买/主卖,
//                      量额全放 bid 侧, ask 侧恒 0 (Trade 节点 = bid + ask).
//   ResamplerMin2Bin   MinuteData → 本层行 (分钟箱层 + L_1day 整日箱, BinData): 分钟 bar 已稠密 → 每行恰出一根;
//                      accumulate() 刚结算的分钟 bar 收进当前行, emit(k) 本层行 k 末分钟之后出行.
//   本文件纯重采样, 不认识任何特征 / 日频数据.
//========================================================================================

template <class Vol, class Amt>
struct BarAcc {
  float open{0}, high{0}, low{0}, close{0};
  Vol bid_volume{0}, ask_volume{0};
  Amt bid_amount{0}, ask_amount{0};

  bool traded() const { return open > 0.0f; }
  void clear() { *this = BarAcc{}; }

  // 有成交输入 (o/l/h/c > 0 或全 0 的零价输入 — 全 0 价不动 OHLC 标志, 同逐笔零价成交口径)
  void add(float o, float h, float l, float c, Vol bv, Vol av, Amt ba, Amt aa) {
    if (!traded()) {
      open = o;
      high = h;
      low = l;
    } else {
      high = std::max(high, h);
      low = std::min(low, l);
    }
    close = c;
    bid_volume += bv;
    ask_volume += av;
    bid_amount += ba;
    ask_amount += aa;
  }
  // 无成交输入 (价忽略, 只加量额)
  void add_untraded(Vol bv, Vol av, Amt ba, Amt aa) {
    bid_volume += bv;
    ask_volume += av;
    bid_amount += ba;
    ask_amount += aa;
  }

  // 出行 i 一根 bar (BarData<Vol, Amt>: MinuteData / BinData); px = 无成交携带价 (行末时刻日内最近成交价)
  template <class OV, class OA>
  void emit(BarData<OV, OA> &o, size_t i, float px) const {
    const bool t = traded();
    const float p = t ? close : px;
    o.index = static_cast<uint32_t>(i);
    o.open = t ? open : p;
    o.high = t ? high : p;
    o.low = t ? low : p;
    o.close = p;
    o.bid_volume = static_cast<OV>(bid_volume);
    o.ask_volume = static_cast<OV>(ask_volume);
    o.bid_amount = static_cast<OA>(bid_amount);
    o.ask_amount = static_cast<OA>(ask_amount);
  }
};

//----------------------------------------------------------------------------------------
// TICK -> MINUTE
//----------------------------------------------------------------------------------------
class ResamplerTick2Min {
public:
  ResamplerTick2Min(TickData &input, MinuteData &output) : input_(input), output_(output) {}

  // 分钟翻转: min_end 之前仍有未出 bar 的分钟 → emit 其中最早的一根, 返回 true (调用方循环至 false)
  // min_end = 本笔所在分钟 (本笔尚未 accumulate) / TRADE_MINUTES_PER_DAY (收盘补齐到末分钟含)
  bool roll(size_t min_end) noexcept {
    if (next_min_ >= min_end) [[likely]]
      return false;
    acc_.emit(output_, next_min_++, last_price_);
    acc_.clear();
    return true;
  }

  void reset() noexcept {
    next_min_ = 0;
    acc_.clear();
    last_price_ = 0.0f;
  }

  // 本笔累积到当前分钟 bar (roll 之后调); 零价成交沿用日内最近成交价 (尚无 → 0, 只贡献量额)
  void accumulate() noexcept {
    if (input_.lob.order_type != L2::OrderType::TAKER)
      return;
    const float price = input_.lob.price > 0.0f ? input_.lob.price : last_price_;
    const uint32_t volume = input_.lob.volume;
    const float amount = price * static_cast<float>(volume);
    if (input_.lob.order_dir == L2::OrderDirection::BID)
      acc_.add(price, price, price, price, volume, 0, amount, 0.0f);
    else
      acc_.add(price, price, price, price, 0, volume, 0.0f, amount);
    last_price_ = price;
  }

private:
  TickData &input_;
  MinuteData &output_;
  BarAcc<uint32_t, float> acc_;
  size_t next_min_{0};     // 下一根待出 bar 的 L_1min 分钟 (= 当前累积中的分钟)
  float last_price_{0.0f}; // 日内最近成交价 (盘前 0): 零价成交回退 / 无成交分钟携带价
};

//----------------------------------------------------------------------------------------
// BAR1M -> MINUTE (与 Tick2Min 同构: 源 = bar1m 分钟 K)
//----------------------------------------------------------------------------------------
class ResamplerBar2Min {
public:
  explicit ResamplerBar2Min(MinuteData &output) : output_(output) {}

  bool roll(size_t min_end) noexcept {
    if (next_min_ >= min_end) [[likely]]
      return false;
    acc_.emit(output_, next_min_++, last_price_);
    acc_.clear();
    return true;
  }

  void reset() noexcept {
    next_min_ = 0;
    acc_.clear();
    last_price_ = 0.0f;
  }

  // 本分钟 bar1m 收进当前分钟 (roll 之后调); 无成交 (volume 0 / 无价) 不收 → 出携带价 bar
  void accumulate(const Bar1m &bar) noexcept {
    if (bar.volume == 0 || bar.close <= 0.0f)
      return;
    acc_.add(bar.open, bar.high, bar.low, bar.close, bar.volume, 0u, bar.amount, 0.0f);
    last_price_ = bar.close;
  }

private:
  MinuteData &output_;
  BarAcc<uint32_t, float> acc_;
  size_t next_min_{0};
  float last_price_{0.0f};
};

//----------------------------------------------------------------------------------------
// MINUTE -> 本层 BAR (分钟箱层 + L_1day 整日箱; 源 = 已结算的分钟 bar)
//----------------------------------------------------------------------------------------
// 分钟 bar 已稠密 (每分钟恰一根) → 本层每行恰出一根. 量额宽累计 (uint64 / double): 整日成交量可超 uint32.
class ResamplerMin2Bin {
public:
  ResamplerMin2Bin(const MinuteData &input, BinData &output, size_t lvl) : input_(input), output_(output), lvl_(lvl) {}

  void reset() noexcept { acc_.clear(); }

  void accumulate() noexcept {
    const MinuteData &m = input_;
    if (m.traded())
      acc_.add(m.open, m.high, m.low, m.close, m.bid_volume, m.ask_volume, m.bid_amount, m.ask_amount);
    else
      acc_.add_untraded(m.bid_volume, m.ask_volume, m.bid_amount, m.ask_amount);
  }

  void emit(size_t k) noexcept {
    assert(level_end_sec(lvl_, k) == level_end_sec(L_1min, input_.index) && "emit: MinuteData 末 bar 不是本层行 k 末分钟");
    acc_.emit(output_, k, input_.close);
    acc_.clear();
  }

private:
  const MinuteData &input_;
  BinData &output_;
  size_t lvl_; // 本层 (L_5min .. L_1day)
  BarAcc<uint64_t, double> acc_;
};
