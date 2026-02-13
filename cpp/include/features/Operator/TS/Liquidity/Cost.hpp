#pragma once

// =============================================================================
// COST (Impact Cost) - 冲击成本
// =============================================================================
// 计算吃掉N档的加权平均执行价与中间价的偏离
//   cost_buy_N  = VWAP(ask[1:N]) / mid_price - 1  (买方冲击成本, 正值)
//   cost_sell_N = 1 - VWAP(bid[1:N]) / mid_price  (卖方冲击成本, 正值)
//
// 模板参数:
//   N_LEVELS - 档位数 (1, 5, 10)
//   IS_BUY   - true=买方冲击(吃ask), false=卖方冲击(吃bid)
//
// 输入频率: ON_DEPTH (盘口更新时)
// 输出频率: per sec (由外部按秒读取)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, bool IS_BUY, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Cost {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  Cost(const CBuffer<float, L2::BLEN> (&price)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> (&qty)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> &mid_price,
       CBuffer<float, L2::BLEN> &out)
      : price_(price), qty_(qty), mid_price_(mid_price), out_(out) {}

  inline void compute() {
    float sum_pv = 0.0f; // 价格*数量的累计（计算VWAP用）
    float sum_v = 0.0f;  // 总数量

    // 遍历前N档，计算VWAP（成交量加权平均价）
    for (size_t i = 0; i < N_LEVELS; ++i) {
      float p = price_[i].back(); // 档位价格
      float v = qty_[i].back();   // 档位数量
      if constexpr (!IS_BUY) {
        // bid qty 是正值，直接使用
      } else {
        // ask qty 是负值，取绝对值
        v = -v;
      }
      sum_pv += p * v; // 累加价格*数量
      sum_v += v;      // 累加数量
    }

    // 从MidPrice CBuffer读取中间价
    float mid = mid_price_.back();
    float cost = 0.0f;

    if (sum_v > 1e-6f && mid > 1e-6f) {
      // 计算VWAP：总金额 / 总数量
      float vwap = sum_pv / sum_v;
      if constexpr (IS_BUY) {
        // 买方冲击成本：吃掉N档ask的VWAP比mid高多少（比例）
        cost = vwap / mid - 1.0f; // 正值表示成本高于mid
      } else {
        // 卖方冲击成本：吃掉N档bid的VWAP比mid低多少（比例）
        cost = 1.0f - vwap / mid; // 正值表示收益低于mid
      }
    }

    value_ = cost;
  }

  inline void flush() {
    // 将compute中计算的冲击成本写入输出CBuffer
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> (&price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> &mid_price_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
