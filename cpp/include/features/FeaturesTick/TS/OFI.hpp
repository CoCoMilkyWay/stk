#pragma once

// =============================================================================
// OFI (Order Flow Imbalance) - 订单流失衡
// =============================================================================
// 委托量增量变化的差异，捕捉订单流动态
//   ΔV_bid: price↓→0, price=→V-Vprev, price↑→V
//   ΔV_ask: price↓→V, price=→V-Vprev, price↑→0
//   OFI = ΔV_bid - ΔV_ask
//
// 模板参数:
//   N_LEVELS - 档位数 (1 或 5)
//
// DAG中使用:
//   OFI<1> ofi_1{bid_qty_, ask_qty_, bid_price_, ask_price_, ofi_1_};
//   OFI<5> ofi_5{bid_qty_, ask_qty_, bid_price_, ask_price_, ofi_5_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class OFI {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  OFI(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&bid_price)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_price)[DEPTH_SIZE],
      CBuffer<float, L2::BLEN> &out)
      : bid_qty_(bid_qty),
        ask_qty_(ask_qty),
        bid_price_(bid_price),
        ask_price_(ask_price),
        out_(out) {
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

  void compute() {
    float ofi = 0.0f;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      float cur_bid_price = bid_price_[i].back();
      float cur_bid_qty = bid_qty_[i].back();
      float cur_ask_price = ask_price_[i].back();
      float cur_ask_qty = -ask_qty_[i].back(); // 转为正值

      float prev_bp = prev_bid_price_[i];
      float prev_bq = prev_bid_qty_[i];
      float prev_ap = prev_ask_price_[i];
      float prev_aq = prev_ask_qty_[i];

      // Bid delta: price↓→0, price=→cur-prev, price↑→cur
      float delta_bid;
      if (cur_bid_price < prev_bp) {
        delta_bid = 0.0f;
      } else if (cur_bid_price == prev_bp) {
        delta_bid = cur_bid_qty - prev_bq;
      } else {
        delta_bid = cur_bid_qty;
      }

      // Ask delta: price↓→cur, price=→cur-prev, price↑→0
      float delta_ask;
      if (cur_ask_price < prev_ap) {
        delta_ask = cur_ask_qty;
      } else if (cur_ask_price == prev_ap) {
        delta_ask = cur_ask_qty - prev_aq;
      } else {
        delta_ask = 0.0f;
      }

      ofi += weights_[i] * (delta_bid - delta_ask);

      // 更新prev
      prev_bid_price_[i] = cur_bid_price;
      prev_bid_qty_[i] = cur_bid_qty;
      prev_ask_price_[i] = cur_ask_price;
      prev_ask_qty_[i] = cur_ask_qty;
    }

    out_.push_back(ofi);
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
};
