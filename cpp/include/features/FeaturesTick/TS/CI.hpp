#pragma once

// =============================================================================
// CI (Cumulative Imbalance) - 累计失衡
// =============================================================================
// 计算前N档的累计买卖失衡率
//   CI_N = (Σ V_bid[1:N] - Σ V_ask[1:N]) / (Σ V_bid[1:N] + Σ V_ask[1:N])
//
// 模板参数:
//   N_LEVELS - 累计档位数 (1, 5, 10, 30, ...)
//
// DAG中使用:
//   CI<1>  ci_1{bid_qty_, ask_qty_, ci_1_};
//   CI<5>  ci_5{bid_qty_, ask_qty_, ci_5_};
//   CI<10> ci_10{bid_qty_, ask_qty_, ci_10_};
//   CI<30> ci_30{bid_qty_, ask_qty_, ci_30_};
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

  void compute() {
    float sum_bid = 0.0f;
    float sum_ask = 0.0f;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      sum_bid += bid_qty_[i].back();
      sum_ask += -ask_qty_[i].back(); // ask_qty是负值
    }

    float denom = sum_bid + sum_ask;
    out_.push_back(denom > 1e-6f ? (sum_bid - sum_ask) / denom : 0.0f);
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
};
