#pragma once

// =============================================================================
// PEAK (Peak Location & Concentration) - 峰值位置与集中度
// =============================================================================
// 计算深度分布的峰值特征（只考虑前5档）
//
// 【公式定义】
//   peak_loc   = argmax(V[1:5])               (最大量所在档位, 1-indexed)
//   peak_ratio = max(V[1:5]) / mean(V[1:5])  (峰值集中度, >=1)
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: {bid/ask}_qty[0:4] (onDepth)
//   输出: peak_{loc/ratio}_{bid/ask} (onMinute)
//
// 【模板参数】
//   IS_BID - true=买侧, false=卖侧
//   IS_LOC - true=输出位置, false=输出集中度
//
// 【备注】
//   - peak_loc: 值越大表示峰值离盘口越远
//   - peak_ratio: 值越大表示订单越集中在单一档位
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <bool IS_BID, bool IS_LOC, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Peak {
  static constexpr size_t N_LEVELS = 5; // 只考虑前5档

public:
  enum Out : size_t { value,
                      kCount };

  Peak(const CBuffer<float, L2::BLEN> (&qty)[DEPTH_SIZE],
       CBuffer<float, L2::BLEN> (&out)[kCount])
      : qty_(qty), out_(out[value]) {}

  inline void compute() {
    float max_v = 0.0f; // 最大数量
    float sum_v = 0.0f; // 总数量
    size_t max_idx = 0; // 最大值所在档位

    // 遍历前5档，找到峰值位置和总量
    for (size_t i = 0; i < N_LEVELS; ++i) {
      float v = qty_[i].back();
      if constexpr (!IS_BID) {
        v = -v; // ask qty 是负值，取绝对值
      }
      sum_v += v;      // 累加总量
      if (v > max_v) { // 更新最大值和位置
        max_v = v;
        max_idx = i;
      }
    }

    float result = 0.0f;
    if constexpr (IS_LOC) {
      // 输出峰值位置：1-indexed档位（1表示第一档）
      // 值越大表示峰值离盘口越远
      result = static_cast<float>(max_idx + 1);
    } else {
      // 输出峰值集中度：max / mean
      // 值越大表示订单越集中在单一档位，>=1
      float mean_v = sum_v / static_cast<float>(N_LEVELS);
      result = (mean_v > 1e-6f) ? (max_v / mean_v) : 1.0f;
    }

    value_ = result;
  }

  inline void flush() {
    // 将compute中计算的峰值特征写入输出CBuffer
    out_.push_back(value_);
  }

  inline void reset() {}

private:
  const CBuffer<float, L2::BLEN> (&qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
