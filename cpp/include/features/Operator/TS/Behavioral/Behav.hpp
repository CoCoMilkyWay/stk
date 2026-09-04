#pragma once

// =============================================================================
// BEHAV (Behavioral) - 行为特征 (降频版): 每笔订单累计, 按秒结算, 分钟末输出
// =============================================================================
//   agg_buy/sell = avg(log(P_order / P_best))   (买/卖单侵略性)
//   agg_dif = agg_buy - agg_sell                (侵略性差)
//   cpr = |O^C| / |O^M|                         (撤挂比)
//   agg_trd = linear_slope(agg_dif, 20s 窗口)   (侵略性趋势)
//   ord_size = avg(|O^M|)                       (平均单笔规模)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"
#include <cmath>

class Behav {
  static constexpr size_t AGG_WINDOW = 20;    // 侵略性趋势窗口 (秒)
  static constexpr float PRICE_SCALE = 0.01f; // Level->price 是0.01元(分)单位 → 转为元

public:
  enum Out : size_t { agg_buy,
                      agg_sell,
                      agg_dif,
                      cpr,
                      agg_trd,
                      ord_size,
                      kCount };
  float y[kCount] = {}; // 秒结算写入, 分钟末由 Node 推出

  explicit Behav(TickData &td) : td_(td) {}

  inline void compute() {
    const uint32_t cur_sec = td_.l0_index;

    // 按秒推进：当秒变化时，聚合上一秒的数据
    while (last_sec_ < cur_sec) {
      flush_second_();
      ++last_sec_;
    }

    const auto &lob = td_.lob;
    const float vol = static_cast<float>(lob.volume);

    switch (lob.order_type) {
    case L2::OrderType::MAKER: { // 挂单
      // 订单侵略性 log(P_order / P_best), P_best: 买单取买一, 卖单取卖一
      // depth_buffer 布局: [0:N-1]=ask(N→1), [N:2N-1]=bid(1→N)
      if (lob.depth_buffer.size() < L2::LOB_DEPTH + 1) [[unlikely]]
        break; // 早期数据不足, 跳过

      const Level *bid1 = lob.depth_buffer[L2::LOB_DEPTH];     // 买一
      const Level *ask1 = lob.depth_buffer[L2::LOB_DEPTH - 1]; // 卖一

      const float order_price = lob.price * PRICE_SCALE;
      const bool is_bid = (lob.order_dir == L2::OrderDirection::BID);

      // Level::price 是档位下标, 加上基准才是绝对价 (分). 低价股基准为 0.
      const float base = static_cast<float>(lob.price_base);
      const float best_price = (base + (is_bid ? bid1->price : ask1->price)) * PRICE_SCALE;

      if (best_price > 1e-6f && order_price > 1e-6f) {
        if (is_bid) {
          // 买单: P_order > P_best_bid 更激进 (agg > 0)
          sum_agg_buy_ += std::log(order_price / best_price);
          cnt_agg_buy_++;
        } else {
          // 卖单: P_order < P_best_ask 更激进 (agg > 0)
          sum_agg_sell_ += std::log(best_price / order_price);
          cnt_agg_sell_++;
        }
      }

      vol_maker_ += vol;
      cnt_maker_++;
      break;
    }
    case L2::OrderType::CANCEL: // 撤单
      vol_cancel_ += vol;
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
    for (size_t i = 0; i < AGG_WINDOW; ++i)
      agg_window_[i] = 0.0f;
    agg_idx_ = 0;
    agg_cnt_ = 0;
    last_sec_ = 0;
    for (size_t i = 0; i < kCount; ++i)
      y[i] = 0.0f;
  }

private:
  // 秒级聚合: 无新样本的秒沿用上一值
  inline void flush_second_() {
    // 1. 平均侵略性
    y[agg_buy] = cnt_agg_buy_ > 0 ? sum_agg_buy_ / cnt_agg_buy_ : y[agg_buy];
    y[agg_sell] = cnt_agg_sell_ > 0 ? sum_agg_sell_ / cnt_agg_sell_ : y[agg_sell];

    // 2. 侵略性差: 正值买方更激进
    y[agg_dif] = y[agg_buy] - y[agg_sell];

    // 3. 撤挂比: 值越大撤单越频繁 (虚假挂单 / 试探)
    y[cpr] = vol_maker_ > 1e-6f ? vol_cancel_ / vol_maker_ : y[cpr];

    // 4. 平均订单规模
    y[ord_size] = cnt_maker_ > 0 ? vol_maker_ / cnt_maker_ : y[ord_size];

    // 5. 侵略性趋势: agg_dif 滑动窗口的线性回归斜率
    agg_window_[agg_idx_] = y[agg_dif];
    agg_idx_ = (agg_idx_ + 1) % AGG_WINDOW;
    if (agg_cnt_ < AGG_WINDOW)
      ++agg_cnt_;

    if (agg_cnt_ >= 2) {
      // x = 0, 1, ..., n-1: Σx, Σx² 闭式
      const float n = static_cast<float>(agg_cnt_);
      const float sum_x = n * (n - 1.0f) * 0.5f;
      const float sum_xx = n * (n - 1.0f) * (2.0f * n - 1.0f) / 6.0f;

      float sum_y = 0.0f, sum_xy = 0.0f;
      for (size_t i = 0; i < agg_cnt_; ++i) {
        const size_t idx = (agg_idx_ + AGG_WINDOW - agg_cnt_ + i) % AGG_WINDOW;
        const float x = static_cast<float>(i);
        const float v = agg_window_[idx];
        sum_y += v;
        sum_xy += x * v;
      }

      const float denom = n * sum_xx - sum_x * sum_x;
      y[agg_trd] = denom > 1e-6f ? (n * sum_xy - sum_x * sum_y) / denom : 0.0f;
    }

    // 重置秒内累计器
    sum_agg_buy_ = sum_agg_sell_ = 0.0f;
    cnt_agg_buy_ = cnt_agg_sell_ = 0;
    vol_maker_ = vol_cancel_ = 0.0f;
    cnt_maker_ = 0;
  }

  TickData &td_;

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
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Behav(N) N(Behav, (Behav), (tick_data), onTick, onMinute)

#define FIELDS_L1_Behav(X)                                                                                                                                                                                                                                                                                                                     \
  X(agg_buy, BEHAVIORAL, RAW, NONE, "Bid Aggressiveness", "买单平均侵略性", "限价买单相对best bid的激进程度(降频)", R"(\frac{1}{\#O_W^{M,B,\mathrm{lmt}}}\sum_{i\in O_W^{M,B,\mathrm{lmt}}}\log\frac{P_i}{P_{1,\tau_i}^{M,B}}, \quad O_W^{M,B,\mathrm{lmt}}=\{i: \tau_i\in W, s_i=B, \mathrm{type}_i=\mathrm{limit}\})", OP(Behav, agg_buy))   \
  X(agg_sell, BEHAVIORAL, RAW, NONE, "Ask Aggressiveness", "卖单平均侵略性", "限价卖单相对best ask的激进程度(降频)", R"(\frac{1}{\#O_W^{M,A,\mathrm{lmt}}}\sum_{i\in O_W^{M,A,\mathrm{lmt}}}\log\frac{P_{1,\tau_i}^{M,A}}{P_i}, \quad O_W^{M,A,\mathrm{lmt}}=\{i: \tau_i\in W, s_i=A, \mathrm{type}_i=\mathrm{limit}\})", OP(Behav, agg_sell)) \
  X(agg_dif, BEHAVIORAL, RAW, NONE, "Aggressiveness Diff", "侵略性差", "买卖侵略性差值(降频)", R"(\bar{a}_W^{B} - \bar{a}_W^{A}, \quad \bar{a}_W^{s}=\frac{1}{\#O_W^{M,s,\mathrm{lmt}}}\sum_{i\in O_W^{M,s,\mathrm{lmt}}}\log\frac{P_i}{P_{1,\tau_i}^{M,s}}, \quad s \in \{B,A\})", OP(Behav, agg_dif))                                        \
  X(cpr, BEHAVIORAL, RATIO, NONE, "Cancel-to-Post Ratio", "撤挂比", "撤单量占挂单量比例(降频)", R"(\frac{\sum_{\tau\in W}(|O_{\tau}^{C,B}|+|O_{\tau}^{C,A}|)}{\sum_{\tau\in W}(|O_{\tau}^{M,B}|+|O_{\tau}^{M,A}|)})", OP(Behav, cpr))                                                                                                          \
  X(agg_trd, BEHAVIORAL, RAW, NONE, "Aggressiveness Trend", "侵略性趋势", "子窗口侵略性序列线性回归斜率(降频)", R"(\hat{\beta}_1, \quad \bar{a}_{\tau}=\hat{\beta}_0+\hat{\beta}_1\tau+\epsilon_{\tau}, \quad \tau\in\{t-W,\ldots,t\})", OP(Behav, agg_trd))                                                                                   \
  X(ord_size, BEHAVIORAL, RAW, LOG_ZSCORE, "Avg Order Size", "平均单笔规模", "窗口内订单平均量(降频)", R"(\frac{1}{\#O_W^{M}}\sum_{i\in O_W^{M}}|O_i|)", OP(Behav, ord_size))
