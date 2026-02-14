#pragma once

// =============================================================================
// BEHAV (Behavioral) - 行为特征 (降频版)
// =============================================================================
// 计算订单行为特征
//
// 【公式定义】
//   agg_buy/sell = avg(log(P_order / P_best))   (买/卖单侵略性)
//   agg_dif = agg_buy - agg_sell                (侵略性差)
//   cpr = |O^C| / |O^M|                         (撤挂比)
//   agg_trd = linear_slope(agg)                 (侵略性趋势)
//   ord_size = avg(|O^M|)                       (平均单笔规模)
//
// 【触发域】
//   compute: onMaker / onCancel (内部按秒推进)
//   flush:   onMinute
//
// 【输入输出】
//   输入: TickData.lob.{order_type, order_dir, volume, price, depth_buffer, l0_index} (onMaker/onCancel)
//   输出: agg_buy, agg_sell, agg_dif, cpr, agg_trd, ord_size (onMinute)
//
// 【备注】
//   - 使用20秒滑动窗口计算侵略性趋势
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include <cmath>

class Behav {
  static constexpr size_t AGG_WINDOW = 20;    // 侵略性趋势窗口 (秒)
  static constexpr float PRICE_SCALE = 0.01f; // Level->price 是0.01元(分)单位 → 转为元

public:
  Behav(TickData &td,
        CBuffer<float, L2::BLEN> &agg_buy,
        CBuffer<float, L2::BLEN> &agg_sell,
        CBuffer<float, L2::BLEN> &agg_dif,
        CBuffer<float, L2::BLEN> &cpr,
        CBuffer<float, L2::BLEN> &agg_trd,
        CBuffer<float, L2::BLEN> &ord_size)
      : td_(td),
        agg_buy_(agg_buy), agg_sell_(agg_sell), agg_dif_(agg_dif),
        cpr_(cpr), agg_trd_(agg_trd), ord_size_(ord_size) {}

  inline void compute() {
    const uint32_t cur_sec = td_.l0_index;

    // 按秒推进：当秒变化时，聚合上一秒的数据
    while (last_sec_ < cur_sec) {
      flush_second_();
      ++last_sec_;
    }

    // 每笔订单时，计算订单行为特征
    const auto &lob = td_.lob;
    const float vol = static_cast<float>(lob.volume);

    switch (lob.order_type) {
    case L2::OrderType::MAKER: { // 挂单
      // 计算订单侵略性：log(P_order / P_best)
      // 侵略性衡量挂单价格偏离最优价格的程度
      // P_best: 对于买单是当前买一价, 对于卖单是当前卖一价
      // depth_buffer 布局: [0:N-1]=ask(N→1), [N:2N-1]=bid(1→N)

      // 检查 depth_buffer 是否有足够元素 (早期数据可能不足, 跳过)
      if (lob.depth_buffer.size() < L2::LOB_DEPTH + 1) [[unlikely]] {
        break; // 数据不足, 跳过本次计算
      }

      const Level *bid1 = lob.depth_buffer[L2::LOB_DEPTH];     // 买一
      const Level *ask1 = lob.depth_buffer[L2::LOB_DEPTH - 1]; // 卖一

      const float order_price = lob.price * PRICE_SCALE;
      const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);

      const float best_price = is_bid ? (bid1->price * PRICE_SCALE) : (ask1->price * PRICE_SCALE);

      if (best_price > 1e-6f && order_price > 1e-6f) {
        float agg = 0.0f;
        if (is_bid) {
          // 买单侵略性 = log(P_order / P_best_bid)
          // P_order > P_best_bid 表示更激进（愿意出更高价），agg > 0
          // P_order < P_best_bid 表示保守（低于市价挂单），agg < 0
          agg = std::log(order_price / best_price);
          sum_agg_buy_ += agg;
          cnt_agg_buy_++;
        } else {
          // 卖单侵略性 = log(P_best_ask / P_order)
          // P_order < P_best_ask 表示更激进（愿意以更低价卖），agg > 0
          // P_order > P_best_ask 表示保守（高于市价挂单），agg < 0
          agg = std::log(best_price / order_price);
          sum_agg_sell_ += agg;
          cnt_agg_sell_++;
        }
      }

      vol_maker_ += vol; // 累计挂单量
      cnt_maker_++;      // 累计挂单笔数
      break;
    }
    case L2::OrderType::CANCEL: // 撤单
      vol_cancel_ += vol;       // 累计撤单量
      break;
    default:
      break;
    }
  }

  // 跨天重置
  void reset() {
    sum_agg_buy_ = sum_agg_sell_ = 0.0f;
    cnt_agg_buy_ = cnt_agg_sell_ = 0;
    vol_maker_ = vol_cancel_ = 0.0f;
    cnt_maker_ = 0;
    for (size_t i = 0; i < AGG_WINDOW; ++i) {
      agg_window_[i] = 0.0f;
    }
    agg_idx_ = 0;
    agg_cnt_ = 0;
    last_sec_ = 0;
    // 重置输出缓存
    out_agg_buy_ = out_agg_sell_ = out_agg_dif_ = 0.0f;
    out_cpr_ = out_agg_trd_ = out_ord_size_ = 0.0f;
  }

  // 每分钟输出
  inline void flush() {
    agg_buy_.push_back(out_agg_buy_);
    agg_sell_.push_back(out_agg_sell_);
    agg_dif_.push_back(out_agg_dif_);
    cpr_.push_back(out_cpr_);
    agg_trd_.push_back(out_agg_trd_);
    ord_size_.push_back(out_ord_size_);
  }

private:
  // 秒级聚合（内部调用）
  inline void flush_second_() {
    // 1. 计算平均侵略性：sum(agg) / count(agg)
    out_agg_buy_ = cnt_agg_buy_ > 0 ? sum_agg_buy_ / cnt_agg_buy_ : out_agg_buy_;
    out_agg_sell_ = cnt_agg_sell_ > 0 ? sum_agg_sell_ / cnt_agg_sell_ : out_agg_sell_;

    // 2. 侵略性差：买侧 - 卖侧
    // 正值表示买方更激进，负值表示卖方更激进
    out_agg_dif_ = out_agg_buy_ - out_agg_sell_;

    // 3. 撤挂比CPR：撤单量 / 挂单量
    // 值越大表示撤单频繁，可能存在虚假挂单或试探行为
    out_cpr_ = vol_maker_ > 1e-6f ? vol_cancel_ / vol_maker_ : out_cpr_;

    // 4. 平均订单规模：总挂单量 / 挂单笔数
    out_ord_size_ = cnt_maker_ > 0 ? vol_maker_ / cnt_maker_ : out_ord_size_;

    // 5. 侵略性趋势：计算agg_dif的线性回归斜率
    // 更新滑动窗口（保存最近AGG_WINDOW个agg_dif值）
    agg_window_[agg_idx_] = out_agg_dif_;
    agg_idx_ = (agg_idx_ + 1) % AGG_WINDOW;
    if (agg_cnt_ < AGG_WINDOW)
      ++agg_cnt_;

    // 用最小二乘法计算线性回归斜率：y = ax + b 中的 a
    // 斜率正值表示侵略性上升趋势，负值表示下降趋势
    if (agg_cnt_ >= 2) {
      // 优化：sum_x 和 sum_xx 可以用公式直接计算（x = 0, 1, 2, ..., n-1）
      const float n = static_cast<float>(agg_cnt_);
      const float sum_x = n * (n - 1.0f) * 0.5f;                      // Σi = n(n-1)/2
      const float sum_xx = n * (n - 1.0f) * (2.0f * n - 1.0f) / 6.0f; // Σi² = n(n-1)(2n-1)/6

      float sum_y = 0.0f, sum_xy = 0.0f;
      for (size_t i = 0; i < agg_cnt_; ++i) {
        const size_t idx = (agg_idx_ + AGG_WINDOW - agg_cnt_ + i) % AGG_WINDOW;
        const float x = static_cast<float>(i); // 时间轴
        const float y = agg_window_[idx];      // agg_dif值
        sum_y += y;
        sum_xy += x * y;
      }

      const float denom = n * sum_xx - sum_x * sum_x;
      // 斜率公式：(n*Σxy - Σx*Σy) / (n*Σx² - (Σx)²)
      out_agg_trd_ = denom > 1e-6f ? (n * sum_xy - sum_x * sum_y) / denom : 0.0f;
    }

    // 重置秒内累计器，准备下一个窗口
    sum_agg_buy_ = sum_agg_sell_ = 0.0f;
    cnt_agg_buy_ = cnt_agg_sell_ = 0;
    vol_maker_ = vol_cancel_ = 0.0f;
    cnt_maker_ = 0;
  }

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

  // 秒推进状态
  uint32_t last_sec_ = 0;

  // 输出缓存（分钟末输出）
  float out_agg_buy_ = 0.0f, out_agg_sell_ = 0.0f, out_agg_dif_ = 0.0f;
  float out_cpr_ = 0.0f, out_agg_trd_ = 0.0f, out_ord_size_ = 0.0f;
};
