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
// 参数:
//   - n_levels: 使用档位数 (1-5)
//   - 衰减权重: w_i = 1 - (i-1)/n_levels
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS = 5, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class VOI {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS must be 1-DEPTH_SIZE");

public:
  // 从共享CBuffer读取盘口数据 (只使用前N_LEVELS档)
  VOI(const CBuffer<float, L2::BLEN> (&bid_price)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_price)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      CBuffer<float, L2::BLEN> &buffer)
      : bid_price_(bid_price),
        ask_price_(ask_price),
        bid_qty_(bid_qty),
        ask_qty_(ask_qty),
        buffer_(buffer) {
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
  }

  void compute() {
    float voi = 0.0f;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      // 从CBuffer获取当前值和前值
      size_t sz = bid_price_[i].size();
      if (sz < 2) continue;  // 需要至少两个数据点

      float cur_bid_price = bid_price_[i][sz - 1];
      float prev_bid_price = bid_price_[i][sz - 2];
      float cur_bid_qty = bid_qty_[i][sz - 1];
      float prev_bid_qty = bid_qty_[i][sz - 2];

      float cur_ask_price = ask_price_[i][sz - 1];
      float prev_ask_price = ask_price_[i][sz - 2];
      float cur_ask_qty = ask_qty_[i][sz - 1];
      float prev_ask_qty = ask_qty_[i][sz - 2];

      // 计算买方增量 ΔV^B
      float delta_bid = 0.0f;
      if (cur_bid_price < prev_bid_price) {
        delta_bid = 0.0f;  // 价格下跌，增量无效
      } else if (cur_bid_price == prev_bid_price) {
        delta_bid = cur_bid_qty - prev_bid_qty;
      } else {
        delta_bid = cur_bid_qty;  // 价格上涨，全量有效
      }

      // 计算卖方增量 ΔV^A (注意: ask_qty 在 DepthData 中保持原始负值)
      float cur_ask_qty_abs = -cur_ask_qty;    // 转为正数
      float prev_ask_qty_abs = -prev_ask_qty;
      float delta_ask = 0.0f;
      if (cur_ask_price < prev_ask_price) {
        delta_ask = cur_ask_qty_abs;  // 价格下跌，全量有效
      } else if (cur_ask_price == prev_ask_price) {
        delta_ask = cur_ask_qty_abs - prev_ask_qty_abs;
      } else {
        delta_ask = 0.0f;  // 价格上涨，增量无效
      }

      voi += weights_[i] * (delta_bid - delta_ask);
    }

    buffer_.push_back(voi);
  }

  float back() const { return buffer_.back(); }

private:
  const CBuffer<float, L2::BLEN> (&bid_price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &buffer_;

  // 权重
  float weights_[N_LEVELS];
};
