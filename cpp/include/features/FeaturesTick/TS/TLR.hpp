#pragma once

// =============================================================================
// TLR (Top Level Ratio) - 顶部档位占比
// =============================================================================
// 前N档占总量的比例，衡量是否容易被击穿
//   TBR_N = Σ V_bid[1:N] / Σ V_bid[all]
//   TAR_N = Σ V_ask[1:N] / Σ V_ask[all]
//
// 模板参数:
//   N_LEVELS - 顶部档位数
//   IS_BID   - true=买侧(TBR), false=卖侧(TAR)
//
// DAG中使用:
//   TLR<5, true>  tbr_5{bid_qty_, ask_qty_, tbr_5_};
//   TLR<5, false> tar_5{bid_qty_, ask_qty_, tar_5_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, bool IS_BID, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class TLR {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  TLR(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      CBuffer<float, L2::BLEN> &out)
      : bid_qty_(bid_qty), ask_qty_(ask_qty), out_(out) {}

  inline void compute() {
    float top_sum = 0.0f;
    float total_sum = 0.0f;

    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      float v;
      if constexpr (IS_BID) {
        v = bid_qty_[i].back();
      } else {
        v = -ask_qty_[i].back(); // ask是负值
      }

      total_sum += v;
      if (i < N_LEVELS) {
        top_sum += v;
      }
    }

    value_ = total_sum > 1e-6f ? top_sum / total_sum : 0.0f;
  }

  inline void flush() { out_.push_back(value_); }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
