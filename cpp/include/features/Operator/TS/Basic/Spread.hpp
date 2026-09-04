#pragma once

// =============================================================================
// Spread - 买卖价差
// =============================================================================
//   spread = P_{1,t}^{M,A} - P_{1,t}^{M,B}  (元)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class Spread {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  Spread(const CBuffer<float, L2::BLEN> &bid_price_0,
         const CBuffer<float, L2::BLEN> &ask_price_0)
      : bid_price_0_(bid_price_0), ask_price_0_(ask_price_0) {}

  inline void compute() {
    float bid = bid_price_0_.back();
    float ask = ask_price_0_.back();
    y[value] = (bid > 0 && ask > 0) ? (ask - bid) : 0.0f;
  }

private:
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Spread(N) N(Spread, (Spread), (DepthData.bid_price[0], DepthData.ask_price[0]), onDepth, onDepth)

#define FIELDS_L0_Spread(X) \
  X(spread, 1, DEPTH, BASIC, RAW, NONE, "100/00/00", "Bid-Ask Spread", "买卖价差", "卖一减买一", R"(P_{1,t}^{M,A} - P_{1,t}^{M,B})", OP(Spread))
