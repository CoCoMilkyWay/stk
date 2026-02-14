#pragma once

// =============================================================================
// CI (Cumulative Imbalance) - 累计失衡
// =============================================================================
// 计算前N档的累计买卖失衡率
//
// 【公式定义】
//   CI_N = (Σ V_{i,t}^{M,B} - Σ V_{i,t}^{M,A}) / (Σ V_{i,t}^{M,B} + Σ V_{i,t}^{M,A}), i=1..N
//
// 【触发域】
//   compute: onDepth
//   flush:   onDepth
//
// 【输入输出】
//   输入: bid_qty[0:N-1] (onDepth), ask_qty[0:N-1] (onDepth)
//   输出: ci_N (onDepth)
//
// 【模板参数】
//   N_LEVELS - 累计档位数 (1, 5, 10, 30, ...)
//
// 【使用示例】
//   CI<1>  ci_1{BidQty_, AskQty_, Ci_1_};
//   CI<5>  ci_5{BidQty_, AskQty_, Ci_5_};
//   CI<10> ci_10{BidQty_, AskQty_, Ci_10_};
//   CI<30> ci_30{BidQty_, AskQty_, Ci_30_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class CI {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  CI(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
     const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
     CBuffer<float, L2::BLEN> &out)
      : bid_qty_(bid_qty), ask_qty_(ask_qty), out_(out) {}

  inline void compute() {
    float sum_bid = 0.0f;
    float sum_ask = 0.0f;

    // 累加前N档买卖数量（从各档的CBuffer读取最新值）
    for (size_t i = 0; i < N_LEVELS; ++i) {
      sum_bid += bid_qty_[i].back();  // 买方数量（正值）
      sum_ask += -ask_qty_[i].back(); // 卖方数量（取反，变正值）
    }

    // 计算累计失衡率：(买量-卖量)/(买量+卖量)
    // 值域[-1,1]，正值表示买方占优，负值表示卖方占优
    float denom = sum_bid + sum_ask;
    value_ = denom > 1e-6f ? (sum_bid - sum_ask) / denom : 0.0f;
  }

  inline void flush() {
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
