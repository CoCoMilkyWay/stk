#pragma once

// =============================================================================
// EntropyImba - 熵失衡: 对比买卖两侧熵值的大小
// =============================================================================
//   entropy_imba = (H^B - H^A) / (H^B + H^A)
//   正值表示买侧分布更均匀 (做市意愿强)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class EntropyImba {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  EntropyImba(const CBuffer<float, L2::BLEN> &bid_entropy,
              const CBuffer<float, L2::BLEN> &ask_entropy)
      : bid_entropy_(bid_entropy), ask_entropy_(ask_entropy) {}

  inline void compute() {
    float b = bid_entropy_.back();
    float a = ask_entropy_.back();
    float denom = b + a;
    y[value] = denom > 1e-6f ? (b - a) / denom : 0.0f;
  }

private:
  const CBuffer<float, L2::BLEN> &bid_entropy_;
  const CBuffer<float, L2::BLEN> &ask_entropy_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_EntropyImba_30(N) N(EntropyImba_30, (EntropyImba), (Entropy_b_30.out(), Entropy_a_30.out()), onMinute, onMinute)

#define FIELDS_L1_EntropyImba_30(X) \
  X(imba_30_entropy, 1, DATA, IMBALANCE, RATIO, NONE, "00/100/00", "Top 30-level Entropy Imba", "三十档香农熵失衡", "三十档香农熵失衡(降频)", R"(\frac{H_{t}^{M,B} - H_{t}^{M,A}}{H_{t}^{M,B} + H_{t}^{M,A}}, \quad H_{t}^{M,s} = -\sum_{i=1}^{N} \pi_{i,t}^{M,s} \log(\pi_{i,t}^{M,s}), \quad N = 30)", OP(EntropyImba_30))

#define NODE_EntropyImba_5(N) N(EntropyImba_5, (EntropyImba), (Entropy_b_5.out(), Entropy_a_5.out()), onMinute, onMinute)

#define FIELDS_L1_EntropyImba_5(X) \
  X(imba_5_entropy, 1, DATA, IMBALANCE, RATIO, NONE, "00/100/00", "Top 5-level Entropy Imba", "五档香农熵失衡", "五档香农熵失衡(降频)", R"(\frac{H_{t}^{M,B} - H_{t}^{M,A}}{H_{t}^{M,B} + H_{t}^{M,A}}, \quad H_{t}^{M,s} = -\sum_{i=1}^{N} \pi_{i,t}^{M,s} \log(\pi_{i,t}^{M,s}), \quad N = 5)", OP(EntropyImba_5))
