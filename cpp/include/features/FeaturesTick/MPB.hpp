#pragma once

// =============================================================================
// MPB (Mid-Price Basis) - 市价偏离度因子
// =============================================================================
// Reference: 中信建投《高频量价选股因子初探》2020.07.09
//
// 核心思想:
//   成交均价与盘口中间价的偏离，揭示实际交易的方向性
//   MPB = TP - MP
//   其中:
//     M_t = (P_t^B + P_t^A) / 2        (当前中间价)
//     MP_t = (M_t + M_{t-1}) / 2       (平滑中间价)
//     TP_t = 成交价 (TAKER时)          (平均成交价)
//          = TP_{t-1} (无成交时)
//
// 研报发现:
//   - 高频负向 (分钟IC -13.78%)
//   - 低频负向 (月频IC -5.23%) => 无反转!
//   - 选股效果最好: 年化多空21.24%, 夏普2.68
//   - 原因: 价格信息不存在量信息的欺骗性
//
// 投资逻辑:
//   MPB > 0: 成交价高于中间价，接近卖一价，卖压大，未来下跌
//   MPB < 0: 成交价低于中间价，接近买一价，买压大，未来上涨
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class MPB {
public:
  // 从共享CBuffer读取中间价和成交价
  MPB(const CBuffer<float, L2::BLEN> &mid_price,
      const CBuffer<float, L2::BLEN> &trade_price,
      CBuffer<float, L2::BLEN> &buffer)
      : mid_price_(mid_price),
        trade_price_(trade_price),
        buffer_(buffer) {}

  void compute() {
    size_t sz = mid_price_.size();
    if (sz == 0) {
      buffer_.push_back(0.0f);
      return;
    }

    // 当前中间价 (0.01元单位)
    float cur_mid = mid_price_[sz - 1];

    // 平滑中间价 (当前 + 上一期的平均)
    float smoothed_mid = cur_mid;
    if (sz > 1) {
      float prev_mid = mid_price_[sz - 2];
      smoothed_mid = (cur_mid + prev_mid) * 0.5f;
    }

    // 成交价 (元单位)
    float tp = trade_price_.back();

    // 计算MPB (需要单位一致: 中间价从0.01元转为元)
    float mpb = 0.0f;
    if (tp > 0.0f && smoothed_mid > 0.0f) {
      mpb = tp - smoothed_mid * 0.01f;
    }

    buffer_.push_back(mpb);
  }

  float back() const { return buffer_.back(); }

private:
  const CBuffer<float, L2::BLEN> &mid_price_;
  const CBuffer<float, L2::BLEN> &trade_price_;
  CBuffer<float, L2::BLEN> &buffer_;
};
