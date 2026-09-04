#pragma once

// =============================================================================
// ENTROPY - 前 N 档深度分布的香农熵: 0 极端集中, ln(N) 极端均匀
// =============================================================================
//   H = -Σ π_i · log(π_i),  π_i = V_i / Σ V
//   单侧算子: 只收一侧 qty; IS_BID 决定符号 (ask 存负值)
// =============================================================================

#include "features/DataDefine.hpp"
#include <algorithm>
#include <cmath>

template <size_t N_LEVELS, bool IS_BID>
class Entropy {
  static_assert(N_LEVELS >= 2 && N_LEVELS <= L2::LOB_DEPTH, "N_LEVELS out of range");

public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  explicit Entropy(const DepthSeries &qty) : qty_(qty) {}

  inline void compute() {
    float v[N_LEVELS];
    float total = 0.0f;
    for (size_t i = 0; i < N_LEVELS; ++i) {
      v[i] = std::max(IS_BID ? qty_[i].back() : -qty_[i].back(), 0.0f);
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
  const DepthSeries &qty_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
// 家族行: side ∈ {a, b}, S = 公式侧标 "A"/"B", en/cn = 侧名, n = 档数
#define ENTROPY_FIELD(X, side, S, en, cn, n) \
  X(side##_##n##_entropy, SHAPE, RAW, NONE, "Top " #n "-level " en " ShannonEntropy", cn "侧" #n "档香农熵", "0:极端集中; ln(N):极端均匀(降频)", R"(-\sum_{i=1}^{N} \pi_{i,t}^{M,)" S R"(} \log(\pi_{i,t}^{M,)" S R"(}), \quad \pi_{i,t}^{M,)" S R"(} = \frac{V_{i,t}^{M,)" S R"(}}{\sum_{j=1}^{N} V_{j,t}^{M,)" S R"(}}, \quad N = )" #n, OP(Entropy_##side##_##n))

#define NODE_Entropy_a_30(N) N(Entropy_a_30, (Entropy<30, false>), (DepthData.ask_qty), onMinute)
#define FIELDS_L1_Entropy_a_30(X) ENTROPY_FIELD(X, a, "A", "Ask", "卖", 30)

#define NODE_Entropy_a_5(N) N(Entropy_a_5, (Entropy<5, false>), (DepthData.ask_qty), onMinute)
#define FIELDS_L1_Entropy_a_5(X) ENTROPY_FIELD(X, a, "A", "Ask", "卖", 5)

#define NODE_Entropy_b_30(N) N(Entropy_b_30, (Entropy<30, true>), (DepthData.bid_qty), onMinute)
#define FIELDS_L1_Entropy_b_30(X) ENTROPY_FIELD(X, b, "B", "Bid", "买", 30)

#define NODE_Entropy_b_5(N) N(Entropy_b_5, (Entropy<5, true>), (DepthData.bid_qty), onMinute)
#define FIELDS_L1_Entropy_b_5(X) ENTROPY_FIELD(X, b, "B", "Bid", "买", 5)
