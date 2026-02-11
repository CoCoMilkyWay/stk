#pragma once

// =============================================================================
// FLOW_RATE - 订单流率特征
// =============================================================================
// 计算单位时间内的订单流统计
//   arr_bid/ask = #O^{M,B/A} / Δt   (买/卖单到达率)
//   can_bid/ask = #O^{C,B/A} / Δt   (买/卖单撤单率)
//   trd_buy/sell = #O^{T,B/A} / Δt  (主动买/卖成交率)
//   net_ord = (#O^{M,B} - #O^{C,B}) - (#O^{M,A} - #O^{C,A})  (净订单流)
//   foi = (ΔO^B - ΔO^A) / (ΔO^B + ΔO^A)  (订单流失衡)
//
// 输入频率: PER_ORDER (每笔订单)
// 输出频率: per sec (由外部按秒读取)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

// compute: 每笔订单时累计, flush: 按秒输出
class FlowRate {
public:
  FlowRate(TickData &td,
           CBuffer<float, L2::BLEN> &arr_bid,
           CBuffer<float, L2::BLEN> &arr_ask,
           CBuffer<float, L2::BLEN> &can_bid,
           CBuffer<float, L2::BLEN> &can_ask,
           CBuffer<float, L2::BLEN> &trd_buy,
           CBuffer<float, L2::BLEN> &trd_sell,
           CBuffer<float, L2::BLEN> &net_ord,
           CBuffer<float, L2::BLEN> &foi)
      : td_(td),
        arr_bid_(arr_bid), arr_ask_(arr_ask),
        can_bid_(can_bid), can_ask_(can_ask),
        trd_buy_(trd_buy), trd_sell_(trd_sell),
        net_ord_(net_ord), foi_(foi) {}

  inline void compute() {
    // 从tick_data读取订单信息
    const auto &lob = td_.lob;
    const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);

    // 根据订单类型和方向，累计各类订单数量
    switch (lob.order_type) {
    case L2::OrderType::MAKER:  // 挂单
      if (is_bid) ++cnt_arr_bid_; else ++cnt_arr_ask_;
      break;
    case L2::OrderType::TAKER:  // 成交
      if (is_bid) ++cnt_trd_buy_; else ++cnt_trd_sell_;
      break;
    case L2::OrderType::CANCEL: // 撤单
      if (is_bid) ++cnt_can_bid_; else ++cnt_can_ask_;
      break;
    }
  }

  inline void flush() {
    // 将累计的订单数量输出为流率（单位时间）
    float dt = 1.0f;  // 时间窗口（暂定1秒）

    // 计算各类订单流率：订单数量 / 时间窗口
    arr_bid_.push_back(static_cast<float>(cnt_arr_bid_) / dt);   // 买单到达率
    arr_ask_.push_back(static_cast<float>(cnt_arr_ask_) / dt);   // 卖单到达率
    can_bid_.push_back(static_cast<float>(cnt_can_bid_) / dt);   // 买单撤单率
    can_ask_.push_back(static_cast<float>(cnt_can_ask_) / dt);   // 卖单撤单率
    trd_buy_.push_back(static_cast<float>(cnt_trd_buy_) / dt);   // 主动买成交率
    trd_sell_.push_back(static_cast<float>(cnt_trd_sell_) / dt); // 主动卖成交率

    // 计算净订单流：(买挂单-买撤单) - (卖挂单-卖撤单)
    // 正值表示买方净挂单多，负值表示卖方净挂单多
    int net_bid = cnt_arr_bid_ - cnt_can_bid_;
    int net_ask = cnt_arr_ask_ - cnt_can_ask_;
    net_ord_.push_back(static_cast<float>(net_bid - net_ask));

    // 计算订单流失衡FOI：(买方流-卖方流) / (|买方流|+|卖方流|)
    // 流 = 成交 - 撤单，反映实际有效的订单活动
    int delta_bid = cnt_trd_buy_ - cnt_can_bid_;
    int delta_ask = cnt_trd_sell_ - cnt_can_ask_;
    int sum = std::abs(delta_bid) + std::abs(delta_ask);
    foi_.push_back(sum > 0 ? static_cast<float>(delta_bid - delta_ask) / static_cast<float>(sum) : 0.0f);

    // 重置计数器，准备下一个窗口
    cnt_arr_bid_ = cnt_arr_ask_ = 0;
    cnt_can_bid_ = cnt_can_ask_ = 0;
    cnt_trd_buy_ = cnt_trd_sell_ = 0;
  }

private:
  TickData &td_;

  CBuffer<float, L2::BLEN> &arr_bid_;
  CBuffer<float, L2::BLEN> &arr_ask_;
  CBuffer<float, L2::BLEN> &can_bid_;
  CBuffer<float, L2::BLEN> &can_ask_;
  CBuffer<float, L2::BLEN> &trd_buy_;
  CBuffer<float, L2::BLEN> &trd_sell_;
  CBuffer<float, L2::BLEN> &net_ord_;
  CBuffer<float, L2::BLEN> &foi_;

  int cnt_arr_bid_ = 0, cnt_arr_ask_ = 0;
  int cnt_can_bid_ = 0, cnt_can_ask_ = 0;
  int cnt_trd_buy_ = 0, cnt_trd_sell_ = 0;
};

// =============================================================================
// TOXIC_CR - 毒订单流撤单率
// =============================================================================
// toxic_cr = Σ|O^C|_{t-5}^{t} / Σ|O^C|_{t-60}^{t}
// 短窗口撤单占长窗口撤单比例, 检测高频撤单行为
//
// 使用滚动窗口实现
// =============================================================================

// compute: 每笔订单时累计 (内部判断CANCEL), flush: 按秒输出
class ToxicCR {
  static constexpr size_t SHORT_WINDOW = 5;   // 5秒
  static constexpr size_t LONG_WINDOW = 60;   // 60秒

public:
  ToxicCR(TickData &td, CBuffer<float, L2::BLEN> &out)
      : td_(td), out_(out) {}

  inline void compute() {
    // 每笔订单时，如果是撤单则累计到当前秒的缓冲区
    if (td_.lob.order_type == L2::OrderType::CANCEL) {
      cancel_buffer_[write_idx_] += 1.0f;
    }
  }

  inline void flush() {
    float short_sum = 0.0f;  // 短窗口（5秒）撤单数
    float long_sum = 0.0f;   // 长窗口（60秒）撤单数

    // 遍历滚动窗口缓冲区，计算短长窗口撤单数
    for (size_t i = 0; i < LONG_WINDOW; ++i) {
      size_t idx = (write_idx_ + LONG_WINDOW - i) % LONG_WINDOW;
      float v = cancel_buffer_[idx];
      long_sum += v;         // 累加到长窗口
      if (i < SHORT_WINDOW) {
        short_sum += v;      // 前5秒累加到短窗口
      }
    }

    // 计算毒订单流撤单率：短窗口撤单 / 长窗口撤单
    // 值越大表示最近5秒撤单占比越高，可能存在高频撤单行为
    float ratio = (long_sum > 1e-6f) ? (short_sum / long_sum) : 0.0f;
    out_.push_back(ratio);

    // 移动写指针到下一个时间窗口
    write_idx_ = (write_idx_ + 1) % LONG_WINDOW;
    cancel_buffer_[write_idx_] = 0.0f;  // 清空新窗口位置
  }

private:
  TickData &td_;
  CBuffer<float, L2::BLEN> &out_;
  float cancel_buffer_[LONG_WINDOW] = {};
  size_t write_idx_ = 0;
};
