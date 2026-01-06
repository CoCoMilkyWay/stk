#pragma once

// =============================================================================
// VOI (Volume Order Imbalance) - 订单流失衡因子
// =============================================================================
// Reference: 中信建投《高频量价选股因子初探》2020.07.09
//
// 核心思想:
//   委托量的增量变化(delta)差异，捕捉订单流动态
//   VOI = ΔV^B - ΔV^A
//
// 研报发现:
//   - 高频正向相关 (分钟IC 11.66%~12.42%)
//   - 低频反转 (月频IC -2.74%~-2.97%)
//   - 反转原因: 散户追涨杀跌 + 主力对倒操纵
//
// 价格变化规则:
//   买方 ΔV^B:
//     - 价格下跌 => 0 (买盘退缩，增量无效)
//     - 价格不变 => V_t - V_{t-1}
//     - 价格上涨 => V_t (买盘激进，全量有效)
//   卖方 ΔV^A:
//     - 价格下跌 => V_t (卖盘激进，全量有效)
//     - 价格不变 => V_t - V_{t-1}
//     - 价格上涨 => 0 (卖盘退缩，增量无效)
//
// 后处理:
//   1. log1p 变换 (保持符号): sign(x) * log1p(|x|)
//   2. RollingZScoreDiscrete 高通滤波:
//      - 减去窗口均值 = 高通滤波器 H_HP = 1 - H_MA
//      - 移动平均的 -3dB 点: |H_MA| ≈ 0.293 → f_c ≈ 0.6/W
//      - 计算 W: W = 0.6 × T_cutoff (T_cutoff 为截止周期秒数)
//      - 例: cutoff 周期 60s → W = 0.6 × 60 = 36s
//
// 参数:
//   - n_levels: 使用档位数 (1-30)
//   - 衰减权重: w_i = 1 - (i-1)/n_levels
//
// 注意:
//   使用金额(万元)而非数量(股)，保证截面可比性
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "math/normalize/RollingZScore.hpp"
#include <cmath>

template <size_t N_LEVELS, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class VOI {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS must be 1-DEPTH_SIZE");
  static constexpr size_t W = 36; // W = 0.6 × T_cutoff, T_cutoff=60s → W=36s

public:
  // 从共享CBuffer读取盘口数据 (只使用前N_LEVELS档)
  // 使用金额(万元)而非数量(股)，保证截面可比性
  VOI(const CBuffer<float, L2::BLEN> (&bid_price)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_price)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&bid_amt)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_amt)[DEPTH_SIZE],
      CBuffer<float, L2::BLEN> &tick_index,
      CBuffer<float, L2::BLEN> &buffer)
      : bid_price_(bid_price),
        ask_price_(ask_price),
        bid_amt_(bid_amt),
        ask_amt_(ask_amt),
        buffer_(buffer),
        zscore_(log1p_buf_, tick_index) {
    // 初始化权重: w_i = 1 - (i-1)/N_LEVELS
    float weight_sum = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      weights_[i] = 1.0f - static_cast<float>(i) / N_LEVELS;
      weight_sum += weights_[i];
    }
    // 归一化权重
    for (size_t i = 0; i < N_LEVELS; ++i) {
      weights_[i] /= weight_sum;
    }
    // 初始化prev缓存
    for (size_t i = 0; i < N_LEVELS; ++i) {
      prev_bid_price_[i] = 0.0f;
      prev_bid_amt_[i] = 0.0f;
      prev_ask_price_[i] = 0.0f;
      prev_ask_amt_[i] = 0.0f;
    }
  }

  void compute() {
    float voi = 0.0f;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      const float cur_bid_price = bid_price_[i].back();
      const float cur_bid_amt = bid_amt_[i].back();
      const float cur_ask_price = ask_price_[i].back();
      const float cur_ask_amt = ask_amt_[i].back();

      const float prev_bid_price = prev_bid_price_[i];
      const float prev_bid_amt = prev_bid_amt_[i];
      const float prev_ask_price = prev_ask_price_[i];
      const float prev_ask_amt = prev_ask_amt_[i];

      // Branchless: bid delta
      // 价格下跌: 0, 价格不变: cur-prev, 价格上涨: cur
      const float bid_not_down = cur_bid_price >= prev_bid_price;
      const float bid_same = cur_bid_price == prev_bid_price;
      const float delta_bid = bid_not_down * (cur_bid_amt - bid_same * prev_bid_amt);

      // Branchless: ask delta (ask_amt是负值)
      // 价格下跌: |cur|, 价格不变: |cur|-|prev|, 价格上涨: 0
      const float ask_not_up = cur_ask_price <= prev_ask_price;
      const float ask_same = cur_ask_price == prev_ask_price;
      const float delta_ask = ask_not_up * (-cur_ask_amt + ask_same * prev_ask_amt);

      voi += weights_[i] * (delta_bid - delta_ask);

      // 更新prev缓存
      prev_bid_price_[i] = cur_bid_price;
      prev_bid_amt_[i] = cur_bid_amt;
      prev_ask_price_[i] = cur_ask_price;
      prev_ask_amt_[i] = cur_ask_amt;
    }

    // log1p 变换 (保持符号): sign(x) * log1p(|x|)
    float voi_log1p = std::copysign(std::log1p(std::abs(voi)), voi);
    log1p_buf_.push_back(voi_log1p);

    // zscore 高通滤波
    buffer_.push_back(zscore_.compute());
  }

  float back() const { return buffer_.back(); }

private:
  const CBuffer<float, L2::BLEN> (&bid_price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&bid_amt_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_amt_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &buffer_;

  float weights_[N_LEVELS];

  // 缓存prev值，避免每次从CBuffer读两次
  float prev_bid_price_[N_LEVELS];
  float prev_bid_amt_[N_LEVELS];
  float prev_ask_price_[N_LEVELS];
  float prev_ask_amt_[N_LEVELS];

  // 内部状态: log1p 中间值 + zscore 算子
  CBuffer<float, L2::BLEN> log1p_buf_;
  RollingZScoreDiscrete<float, L2::BLEN, W> zscore_;
};
