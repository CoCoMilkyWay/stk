#pragma once

// =============================================================================
// PEAK (Peak Location & Concentration) - 峰值位置与集中度
// =============================================================================
// 计算深度分布的峰值特征
//   peak_loc_bid/ask   = argmax(V[1:N])         (最大量所在档位, 1-indexed)
//   peak_ratio_bid/ask = max(V) / mean(V)       (峰值集中度, >=1)
//
// 模板参数:
//   IS_BID - true=买侧, false=卖侧
//   IS_LOC - true=输出位置, false=输出集中度
//
// 输入频率: ON_DEPTH (盘口更新时)
// 输出频率: per sec (由外部按秒读取)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <bool IS_BID, bool IS_LOC, size_t N_LEVELS = L2::LOB_DEPTH>
class PEAK {
public:
  PEAK(const CBuffer<float, L2::BLEN> (&qty)[N_LEVELS],
       CBuffer<float, L2::BLEN> &out)
      : qty_(qty), out_(out) {}

  void compute() {
    float max_v = 0.0f;
    float sum_v = 0.0f;
    size_t max_idx = 0;

    for (size_t i = 0; i < N_LEVELS; ++i) {
      float v = qty_[i].back();
      if constexpr (!IS_BID) {
        v = -v; // ask qty 是负值
      }
      sum_v += v;
      if (v > max_v) {
        max_v = v;
        max_idx = i;
      }
    }

    float result = 0.0f;
    if constexpr (IS_LOC) {
      // 返回 1-indexed 档位
      result = static_cast<float>(max_idx + 1);
    } else {
      // 返回集中度 = max / mean
      float mean_v = sum_v / static_cast<float>(N_LEVELS);
      result = (mean_v > 1e-6f) ? (max_v / mean_v) : 1.0f;
    }

    value_ = result;
  }

  void flush() { out_.push_back(value_); }

private:
  const CBuffer<float, L2::BLEN> (&qty_)[N_LEVELS];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
