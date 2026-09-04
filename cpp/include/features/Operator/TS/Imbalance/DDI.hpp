#pragma once

// =============================================================================
// DDI (Distance-Discounted Imbalance) - 距离折扣失衡: 按价格距离 e^(-λΔp) 折扣的多档失衡
// =============================================================================
//   Δp_i = 档位 i 到中间价的距离,  w_i = e^(-λ·Δp_i)
//   DDI = Σ w_i·(V_{i,t}^{M,B} - V_{i,t}^{M,A}) / Σ w_i·(V_{i,t}^{M,B} + V_{i,t}^{M,A})
//   LAMBDA_X100 = λ×100; exp 用泰勒前 4 项近似 (相对误差 < 1%)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <int LAMBDA_X100, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class DDI {
public:
  static constexpr float LAMBDA = static_cast<float>(LAMBDA_X100) / 100.0f;

  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  DDI(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&bid_price)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_price)[DEPTH_SIZE])
      : bid_qty_(bid_qty), ask_qty_(ask_qty), bid_price_(bid_price), ask_price_(ask_price) {}

  // e^x ≈ 1 + x + x²/2 + x³/6, x ∈ [-10, 0]
  static inline float fast_exp(float x) {
    x = x < -10.0f ? -10.0f : x;
    x = x > 0.0f ? 0.0f : x;
    float x2 = x * x;
    return 1.0f + x + x2 * 0.5f + x2 * x * 0.16666667f;
  }

  inline void compute() {
    float mid = (bid_price_[0].back() + ask_price_[0].back()) * 0.5f;
    float numer = 0.0f;
    float denom = 0.0f;
    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      float b = bid_qty_[i].back();
      float a = -ask_qty_[i].back(); // 卖方存负值
      float dist = ((mid - bid_price_[i].back()) + (ask_price_[i].back() - mid)) * 0.5f;
      float w = fast_exp(-LAMBDA * dist);
      numer += w * (b - a);
      denom += w * (b + a);
    }
    y[value] = denom > 1e-6f ? numer / denom : 0.0f;
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&bid_price_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_price_)[DEPTH_SIZE];
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Ddi_1(N) N(Ddi_1, (DDI<1>), (DepthData.bid_qty, DepthData.ask_qty, DepthData.bid_price, DepthData.ask_price), onMinute, onMinute)

#define FIELDS_L1_Ddi_1(X) \
  X(ddi_1, 1, DATA, IMBALANCE, RATIO, NONE, "00/100/00", "Distance-discounted Imb λ=0.01", "距离折扣失衡λ=0.01", "考虑全量, 但是近端更高权重(按距离, 降频)", R"(\frac{\sum_{i=1}^{N} e^{-\lambda \Delta P_{i,t}} (V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{N} e^{-\lambda \Delta P_{i,t}} (V_{i,t}^{M,B} + V_{i,t}^{M,A})}, \quad \Delta P_{i,t} = i \cdot \text{tick}, \quad \lambda = 0.01)", OP(Ddi_1))

#define NODE_Ddi_2(N) N(Ddi_2, (DDI<2>), (DepthData.bid_qty, DepthData.ask_qty, DepthData.bid_price, DepthData.ask_price), onMinute, onMinute)

#define FIELDS_L1_Ddi_2(X) \
  X(ddi_2, 1, DATA, IMBALANCE, RATIO, NONE, "00/100/00", "Distance-discounted Imb λ=0.02", "距离折扣失衡λ=0.02", "考虑全量, 但是近端更高权重(按距离, 降频)", R"(\frac{\sum_{i=1}^{N} e^{-\lambda \Delta P_{i,t}} (V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{N} e^{-\lambda \Delta P_{i,t}} (V_{i,t}^{M,B} + V_{i,t}^{M,A})}, \quad \Delta P_{i,t} = i \cdot \text{tick}, \quad \lambda = 0.02)", OP(Ddi_2))
