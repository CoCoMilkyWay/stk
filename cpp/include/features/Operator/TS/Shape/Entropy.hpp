#pragma once

// =============================================================================
// ENTROPY - 深度分布香农熵
// =============================================================================
// 计算深度分布的均匀性
//
// 【公式定义】
//   H = -Σ π_i · log(π_i), where π_i = V_i / Σ V
//   0: 极端集中 (单档占全部)
//   ln(N): 极端均匀 (各档相等)
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: bid_qty[0:N-1] (onDepth), ask_qty[0:N-1] (onDepth)
//   输出: {b/a}_N_entropy (onMinute)
//
// 【模板参数】
//   N_LEVELS - 档位数 (5 或 30)
//   IS_BID   - true=买侧, false=卖侧
//
// 【使用示例】
//   Entropy<5, true>   b_5_entropy{bid_qty_, ask_qty_, b_5_entropy_};
//   Entropy<5, false>  a_5_entropy{bid_qty_, ask_qty_, a_5_entropy_};
//   Entropy<30, true>  b_30_entropy{bid_qty_, ask_qty_, b_30_entropy_};
//   Entropy<30, false> a_30_entropy{bid_qty_, ask_qty_, a_30_entropy_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <cmath>

template <size_t N_LEVELS, bool IS_BID, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Entropy {
  static_assert(N_LEVELS >= 2 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  enum Out : size_t { value,
                      kCount };

  Entropy(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
          const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
          CBuffer<float, L2::BLEN> (&out)[kCount])
      : bid_qty_(bid_qty), ask_qty_(ask_qty), out_(out[value]) {}

  inline void compute() {
    float v[N_LEVELS];
    float total = 0.0f;

    // 从各档CBuffer收集数据
    for (size_t i = 0; i < N_LEVELS; ++i) {
      if constexpr (IS_BID) {
        v[i] = bid_qty_[i].back(); // 买i+1档数量
      } else {
        v[i] = -ask_qty_[i].back(); // 卖i+1档数量（取反）
      }
      v[i] = std::max(v[i], 0.0f); // 确保非负
      total += v[i];               // 累计总量
    }

    // 计算香农熵：H = -Σ π_i * log(π_i)
    // π_i = v[i] / total 为各档占比
    float entropy = 0.0f;
    if (total > 1e-6f) {
      for (size_t i = 0; i < N_LEVELS; ++i) {
        float p = v[i] / total; // 计算概率分布
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

  inline void reset() {}

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Entropy_a_30(N) N(Entropy_a_30, (Entropy<30, false>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Entropy_a_30(X) \
  X(a_30_entropy, 1, DATA, TS, SHAPE, RAW, NONE, "00/100/00", "Top 30-level Ask ShannonEntropy", "卖侧三十档香农熵", "0:极端集中; ln(N):极端均匀(降频)", R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,A} \log(\pi_{i,t}^{M,A}), \quad \pi_{i,t}^{M,A} = \frac{V_{i,t}^{M,A}}{\sum_{j=1}^{N} V_{j,t}^{M,A}}, \quad N = 30)", OP(Entropy_a_30))

#define NODE_Entropy_a_5(N) N(Entropy_a_5, (Entropy<5, false>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Entropy_a_5(X) \
  X(a_5_entropy, 1, DATA, TS, SHAPE, RAW, NONE, "00/100/00", "Top 5-level Ask ShannonEntropy", "卖侧五档香农熵", "0:极端集中; ln(N):极端均匀(降频)", R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,A} \log(\pi_{i,t}^{M,A}), \quad \pi_{i,t}^{M,A} = \frac{V_{i,t}^{M,A}}{\sum_{j=1}^{N} V_{j,t}^{M,A}}, \quad N = 5)", OP(Entropy_a_5))

#define NODE_Entropy_b_30(N) N(Entropy_b_30, (Entropy<30, true>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Entropy_b_30(X) \
  X(b_30_entropy, 1, DATA, TS, SHAPE, RAW, NONE, "00/100/00", "Top 30-level Bid ShannonEntropy", "买侧三十档香农熵", "0:极端集中; ln(N):极端均匀(降频)", R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,B} \log(\pi_{i,t}^{M,B}), \quad \pi_{i,t}^{M,B} = \frac{V_{i,t}^{M,B}}{\sum_{j=1}^{N} V_{j,t}^{M,B}}, \quad N = 30)", OP(Entropy_b_30))

#define NODE_Entropy_b_5(N) N(Entropy_b_5, (Entropy<5, true>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Entropy_b_5(X) \
  X(b_5_entropy, 1, DATA, TS, SHAPE, RAW, NONE, "00/100/00", "Top 5-level Bid ShannonEntropy", "买侧五档香农熵", "0:极端集中; ln(N):极端均匀(降频)", R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,B} \log(\pi_{i,t}^{M,B}), \quad \pi_{i,t}^{M,B} = \frac{V_{i,t}^{M,B}}{\sum_{j=1}^{N} V_{j,t}^{M,B}}, \quad N = 5)", OP(Entropy_b_5))
