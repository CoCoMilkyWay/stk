#pragma once

// =============================================================================
// ENTROPY - 前 N 档深度分布的香农熵: 0 极端集中, ln(N) 极端均匀
// =============================================================================
//   H = -Σ π_i · log(π_i),  π_i = V_i / Σ V
//   IS_BID: true=买侧, false=卖侧
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
  float y[kCount] = {};

  Entropy(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
          const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE])
      : bid_qty_(bid_qty), ask_qty_(ask_qty) {}

  inline void compute() {
    float v[N_LEVELS];
    float total = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      v[i] = std::max(IS_BID ? bid_qty_[i].back() : -ask_qty_[i].back(), 0.0f); // ask 存负值
      total += v[i];
    }

    float entropy = 0.0f;
    if (total > 1e-6f) {
      for (size_t i = 0; i < N_LEVELS; ++i) {
        float p = v[i] / total;
        if (p > 1e-9f)
          entropy -= p * std::log(p);
      }
    }
    y[value] = entropy;
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Entropy_a_30(N) N(Entropy_a_30, (Entropy<30, false>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Entropy_a_30(X) \
  X(a_30_entropy, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Top 30-level Ask ShannonEntropy", "卖侧三十档香农熵", "0:极端集中; ln(N):极端均匀(降频)", R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,A} \log(\pi_{i,t}^{M,A}), \quad \pi_{i,t}^{M,A} = \frac{V_{i,t}^{M,A}}{\sum_{j=1}^{N} V_{j,t}^{M,A}}, \quad N = 30)", OP(Entropy_a_30))

#define NODE_Entropy_a_5(N) N(Entropy_a_5, (Entropy<5, false>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Entropy_a_5(X) \
  X(a_5_entropy, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Top 5-level Ask ShannonEntropy", "卖侧五档香农熵", "0:极端集中; ln(N):极端均匀(降频)", R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,A} \log(\pi_{i,t}^{M,A}), \quad \pi_{i,t}^{M,A} = \frac{V_{i,t}^{M,A}}{\sum_{j=1}^{N} V_{j,t}^{M,A}}, \quad N = 5)", OP(Entropy_a_5))

#define NODE_Entropy_b_30(N) N(Entropy_b_30, (Entropy<30, true>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Entropy_b_30(X) \
  X(b_30_entropy, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Top 30-level Bid ShannonEntropy", "买侧三十档香农熵", "0:极端集中; ln(N):极端均匀(降频)", R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,B} \log(\pi_{i,t}^{M,B}), \quad \pi_{i,t}^{M,B} = \frac{V_{i,t}^{M,B}}{\sum_{j=1}^{N} V_{j,t}^{M,B}}, \quad N = 30)", OP(Entropy_b_30))

#define NODE_Entropy_b_5(N) N(Entropy_b_5, (Entropy<5, true>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Entropy_b_5(X) \
  X(b_5_entropy, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Top 5-level Bid ShannonEntropy", "买侧五档香农熵", "0:极端集中; ln(N):极端均匀(降频)", R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,B} \log(\pi_{i,t}^{M,B}), \quad \pi_{i,t}^{M,B} = \frac{V_{i,t}^{M,B}}{\sum_{j=1}^{N} V_{j,t}^{M,B}}, \quad N = 5)", OP(Entropy_b_5))
