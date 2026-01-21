#pragma once

// =============================================================================
// BEHAV (Behavioral) - 行为特征
// =============================================================================
// 计算订单行为特征
//   agg_buy/sell = avg(log(P_order / P_best))   (买/卖单侵略性)
//   agg_dif = agg_buy - agg_sell                (侵略性差)
//   cpr = |O^C| / |O^M|                         (撤挂比)
//   agg_trd = linear_slope(agg)                 (侵略性趋势)
//   ord_size = avg(|O^M|)                       (平均单笔规模)
//
// 输入频率: PER_ORDER
// 输出频率: per sec
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include <cmath>

class BehavioralFeatures {
  static constexpr size_t AGG_WINDOW = 20; // 侵略性趋势窗口

public:
  BehavioralFeatures(TickData &td,
                     CBuffer<float, L2::BLEN> &agg_buy,
                     CBuffer<float, L2::BLEN> &agg_sell,
                     CBuffer<float, L2::BLEN> &agg_dif,
                     CBuffer<float, L2::BLEN> &cpr,
                     CBuffer<float, L2::BLEN> &agg_trd,
                     CBuffer<float, L2::BLEN> &ord_size)
      : td_(td),
        agg_buy_(agg_buy), agg_sell_(agg_sell), agg_dif_(agg_dif),
        cpr_(cpr), agg_trd_(agg_trd), ord_size_(ord_size) {}

  // 每笔订单调用
  void accumulate() {
    const auto &lob = td_.lob;
    const float vol = static_cast<float>(lob.volume);

    switch (lob.order_type) {
    case L2::OrderType::MAKER: {
      // 计算侵略性: log(P_order / P_best)
      // P_best: 对于买单是best_bid, 对于卖单是best_ask
      const float order_price = lob.price;
      const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);
      // Note: best_price 需要从深度数据获取，这里简化处理用order_price
      const float best_price = order_price; // TODO: 从depth获取

      if (best_price > 1e-6f && order_price > 1e-6f) {
        float agg = 0.0f;
        if (is_bid) {
          // 买单侵略性 = log(P_order / P_best_bid)
          // P_order > P_best_bid 表示更激进
          agg = std::log(order_price / best_price);
          sum_agg_buy_ += agg;
          cnt_agg_buy_++;
        } else {
          // 卖单侵略性 = log(P_best_ask / P_order)
          // P_order < P_best_ask 表示更激进
          agg = std::log(best_price / order_price);
          sum_agg_sell_ += agg;
          cnt_agg_sell_++;
        }
      }

      vol_maker_ += vol;
      cnt_maker_++;
      break;
    }
    case L2::OrderType::CANCEL:
      vol_cancel_ += vol;
      break;
    default:
      break;
    }
  }

  // 每秒输出 (ON_DEPTH 时调用)
  void flush() {
    // agg_buy/sell = avg(agg)
    float avg_agg_buy = cnt_agg_buy_ > 0 ? sum_agg_buy_ / cnt_agg_buy_ : 0.0f;
    float avg_agg_sell = cnt_agg_sell_ > 0 ? sum_agg_sell_ / cnt_agg_sell_ : 0.0f;
    agg_buy_.push_back(avg_agg_buy);
    agg_sell_.push_back(avg_agg_sell);

    // agg_dif = agg_buy - agg_sell
    agg_dif_.push_back(avg_agg_buy - avg_agg_sell);

    // cpr = cancel / maker
    cpr_.push_back(vol_maker_ > 1e-6f ? vol_cancel_ / vol_maker_ : 0.0f);

    // ord_size = avg(maker_vol)
    ord_size_.push_back(cnt_maker_ > 0 ? vol_maker_ / cnt_maker_ : 0.0f);

    // agg_trd = linear slope of agg_dif over window
    // 更新滑动窗口
    agg_window_[agg_idx_] = avg_agg_buy - avg_agg_sell;
    agg_idx_ = (agg_idx_ + 1) % AGG_WINDOW;
    if (agg_cnt_ < AGG_WINDOW) ++agg_cnt_;

    // 计算线性回归斜率
    if (agg_cnt_ >= 2) {
      float sum_x = 0.0f, sum_y = 0.0f, sum_xy = 0.0f, sum_xx = 0.0f;
      for (size_t i = 0; i < agg_cnt_; ++i) {
        size_t idx = (agg_idx_ + AGG_WINDOW - agg_cnt_ + i) % AGG_WINDOW;
        float x = static_cast<float>(i);
        float y = agg_window_[idx];
        sum_x += x;
        sum_y += y;
        sum_xy += x * y;
        sum_xx += x * x;
      }
      float n = static_cast<float>(agg_cnt_);
      float denom = n * sum_xx - sum_x * sum_x;
      float slope = denom > 1e-6f ? (n * sum_xy - sum_x * sum_y) / denom : 0.0f;
      agg_trd_.push_back(slope);
    } else {
      agg_trd_.push_back(0.0f);
    }

    // 重置秒内累计器
    sum_agg_buy_ = sum_agg_sell_ = 0.0f;
    cnt_agg_buy_ = cnt_agg_sell_ = 0;
    vol_maker_ = vol_cancel_ = 0.0f;
    cnt_maker_ = 0;
  }

private:
  TickData &td_;

  // 输出 CBuffer
  CBuffer<float, L2::BLEN> &agg_buy_, &agg_sell_, &agg_dif_;
  CBuffer<float, L2::BLEN> &cpr_, &agg_trd_, &ord_size_;

  // 秒内累计
  float sum_agg_buy_ = 0.0f, sum_agg_sell_ = 0.0f;
  int cnt_agg_buy_ = 0, cnt_agg_sell_ = 0;
  float vol_maker_ = 0.0f, vol_cancel_ = 0.0f;
  int cnt_maker_ = 0;

  // 侵略性趋势滑动窗口
  float agg_window_[AGG_WINDOW] = {};
  size_t agg_idx_ = 0, agg_cnt_ = 0;
};
