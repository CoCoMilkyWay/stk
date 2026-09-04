#pragma once

// =============================================================================
// MidPrice - 中间价
// =============================================================================
//   mid_price = (P_{1,t}^{M,B} + P_{1,t}^{M,A}) / 2  (元)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class MidPrice {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  MidPrice(const CBuffer<float, L2::BLEN> &bid_price_0,
           const CBuffer<float, L2::BLEN> &ask_price_0)
      : bid_price_0_(bid_price_0), ask_price_0_(ask_price_0) {}

  inline void compute() {
    y[value] = (bid_price_0_.back() + ask_price_0_.back()) * 0.5f;
  }

private:
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_MidPrice(N) N(MidPrice, (MidPrice), (DepthData.bid_price[0], DepthData.ask_price[0]), onDepth, onDepth)
