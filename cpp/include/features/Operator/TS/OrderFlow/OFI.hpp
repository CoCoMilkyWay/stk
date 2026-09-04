#pragma once

// =============================================================================
// OFI (Order Flow Imbalance) - 订单流失衡
// =============================================================================
// 委托量增量变化的差异，捕捉订单流动态
//
// 【公式定义】
//   ΔV^B: price↓→0, price=→V-V_prev, price↑→V
//   ΔV^A: price↓→V, price=→V-V_prev, price↑→0
//   OFI_N = Σ w_i·(ΔV_{i}^B - ΔV_{i}^A), i=1..N
//
// 【触发域】
//   compute: onDepth
//   flush:   onDepth
//
// 【输入输出】
//   输入: bid_qty[0:N-1] (onDepth), ask_qty[0:N-1] (onDepth), bid_price[0:N-1] (onDepth), ask_price[0:N-1] (onDepth)
//   输出: ofi_N (onDepth)
//
// 【模板参数】
//   N_LEVELS - 档位数 (1 或 5)
//
// 【使用示例】
//   OFI<1> ofi_1{bid_qty_, ask_qty_, bid_price_, ask_price_, ofi_1_};
//   OFI<5> ofi_5{bid_qty_, ask_qty_, bid_price_, ask_price_, ofi_5_};
//
// 【备注】
//   - 权重: w_i = 1 - (i-1)/N, 近端权重更高
//   - 需要reset()清空跨天状态
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class OFI {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  enum Out : size_t { value,
                      kCount };

  OFI(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&bid_price)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_price)[DEPTH_SIZE],
      CBuffer<float, L2::BLEN> (&out)[kCount])
      : bid_qty_(bid_qty),
        ask_qty_(ask_qty),
        bid_price_(bid_price),
        ask_price_(ask_price),
        out_(out[value]) {
    // 初始化prev缓存
    for (size_t i = 0; i < N_LEVELS; ++i) {
      prev_bid_price_[i] = 0.0f;
      prev_bid_qty_[i] = 0.0f;
      prev_ask_price_[i] = 0.0f;
      prev_ask_qty_[i] = 0.0f;
    }
    // 权重: w_i = 1 - (i-1)/N
    float w_sum = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      weights_[i] = 1.0f - static_cast<float>(i) / static_cast<float>(N_LEVELS);
      w_sum += weights_[i];
    }
    for (size_t i = 0; i < N_LEVELS; ++i) {
      weights_[i] /= w_sum;
    }
  }

  inline void compute() {
    float ofi = 0.0f;

    // 遍历前N档，计算订单流失衡
    for (size_t i = 0; i < N_LEVELS; ++i) {
      // 从各档CBuffer读取当前价格和数量
      float cur_bid_price = bid_price_[i].back();
      float cur_bid_qty = bid_qty_[i].back();
      float cur_ask_price = ask_price_[i].back();
      float cur_ask_qty = -ask_qty_[i].back(); // 卖方数量转为正值

      // 读取上一次的价格和数量（状态变量）
      float prev_bp = prev_bid_price_[i];
      float prev_bq = prev_bid_qty_[i];
      float prev_ap = prev_ask_price_[i];
      float prev_aq = prev_ask_qty_[i];

      // 买方订单流增量：根据价格变化判断订单流方向
      // price↓ → 0 (价格下跌，原订单被消耗)
      // price= → cur-prev (价格不变，净增量)
      // price↑ → cur (价格上涨，新挂单)
      float delta_bid;
      if (cur_bid_price < prev_bp) {
        delta_bid = 0.0f; // 价格下跌，清零
      } else if (cur_bid_price == prev_bp) {
        delta_bid = cur_bid_qty - prev_bq; // 价格不变，计算增量
      } else {
        delta_bid = cur_bid_qty; // 价格上涨，全部算新增
      }

      // 卖方订单流增量：逻辑相反
      // price↓ → cur (价格下跌，新挂单)
      // price= → cur-prev (价格不变，净增量)
      // price↑ → 0 (价格上涨，原订单被消耗)
      float delta_ask;
      if (cur_ask_price < prev_ap) {
        delta_ask = cur_ask_qty; // 价格下跌，全部算新增
      } else if (cur_ask_price == prev_ap) {
        delta_ask = cur_ask_qty - prev_aq; // 价格不变，计算增量
      } else {
        delta_ask = 0.0f; // 价格上涨，清零
      }

      // 加权累加订单流失衡：买方增量 - 卖方增量
      // 正值表示买方主动挂单强，负值表示卖方主动挂单强
      ofi += weights_[i] * (delta_bid - delta_ask);

      // 更新状态：保存当前值作为下一次的prev
      prev_bid_price_[i] = cur_bid_price;
      prev_bid_qty_[i] = cur_bid_qty;
      prev_ask_price_[i] = cur_ask_price;
      prev_ask_qty_[i] = cur_ask_qty;
    }

    value_ = ofi;
  }

  inline void flush() {
    // 将compute中计算的OFI值写入输出CBuffer
    out_.push_back(value_);
  }

  inline void reset() {
    // 跨天时清空状态：重置prev缓存为初始值
    for (size_t i = 0; i < N_LEVELS; ++i) {
      prev_bid_price_[i] = 0.0f;
      prev_bid_qty_[i] = 0.0f;
      prev_ask_price_[i] = 0.0f;
      prev_ask_qty_[i] = 0.0f;
    }
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&bid_price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_price_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;

  float weights_[N_LEVELS];
  float prev_bid_price_[N_LEVELS];
  float prev_bid_qty_[N_LEVELS];
  float prev_ask_price_[N_LEVELS];
  float prev_ask_qty_[N_LEVELS];
  float value_ = 0.0f;
};
