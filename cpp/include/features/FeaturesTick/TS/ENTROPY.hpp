#pragma once

// =============================================================================
// ENTROPY - 深度分布香农熵
// =============================================================================
// H = -Σ π_i * log(π_i), where π_i = V_i / Σ V
// 0: 极端集中 (单档占全部)
// ln(N): 极端均匀 (各档相等)
//
// 模板参数:
//   N_LEVELS - 档位数 (5 或 30)
//   IS_BID   - true=买侧, false=卖侧
//
// DAG中使用:
//   ENTROPY<5, true>   b_5_entropy{bid_qty_, ask_qty_, b_5_entropy_};
//   ENTROPY<5, false>  a_5_entropy{bid_qty_, ask_qty_, a_5_entropy_};
//   ENTROPY<30, true>  b_30_entropy{bid_qty_, ask_qty_, b_30_entropy_};
//   ENTROPY<30, false> a_30_entropy{bid_qty_, ask_qty_, a_30_entropy_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <cmath>

template <size_t N_LEVELS, bool IS_BID, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class ENTROPY {
  static_assert(N_LEVELS >= 2 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  ENTROPY(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
          const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
          CBuffer<float, L2::BLEN> &out)
      : bid_qty_(bid_qty), ask_qty_(ask_qty), out_(out) {}

  inline void compute() {
    float v[N_LEVELS];
    float total = 0.0f;

    // 从各档CBuffer收集数据
    for (size_t i = 0; i < N_LEVELS; ++i) {
      if constexpr (IS_BID) {
        v[i] = bid_qty_[i].back();   // 买i+1档数量
      } else {
        v[i] = -ask_qty_[i].back();  // 卖i+1档数量（取反）
      }
      v[i] = std::max(v[i], 0.0f);   // 确保非负
      total += v[i];                  // 累计总量
    }

    // 计算香农熵：H = -Σ π_i * log(π_i)
    // π_i = v[i] / total 为各档占比
    float entropy = 0.0f;
    if (total > 1e-6f) {
      for (size_t i = 0; i < N_LEVELS; ++i) {
        float p = v[i] / total;      // 计算概率分布
        if (p > 1e-9f) {
          entropy -= p * std::log(p); // 累加香农熵
        }
      }
    }
    // 熵越大表示分布越均匀，熵越小表示订单集中在少数档位

    value_ = entropy;
  }

  inline void flush() {
    // 将compute中计算的熵值写入输出CBuffer
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};

// =============================================================================
// ENTROPY_IMBA - 熵失衡
// =============================================================================
// imba = (H_bid - H_ask) / (H_bid + H_ask)
// =============================================================================

class ENTROPY_IMBA {
public:
  ENTROPY_IMBA(const CBuffer<float, L2::BLEN> &bid_entropy,
               const CBuffer<float, L2::BLEN> &ask_entropy,
               CBuffer<float, L2::BLEN> &out)
      : bid_entropy_(bid_entropy), ask_entropy_(ask_entropy), out_(out) {}

  inline void compute() {
    // 从买卖两侧的熵CBuffer读取最新值
    float b = bid_entropy_.back();  // 买侧熵
    float a = ask_entropy_.back();  // 卖侧熵
    // 计算熵失衡：(买侧-卖侧) / (买侧+卖侧)
    // 值域[-1,1]，正值表示买侧分布更均匀（做市意愿强）
    float denom = b + a;
    value_ = denom > 1e-6f ? (b - a) / denom : 0.0f;
  }

  inline void flush() {
    // 将compute中计算的熵失衡写入输出CBuffer
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> &bid_entropy_;
  const CBuffer<float, L2::BLEN> &ask_entropy_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
