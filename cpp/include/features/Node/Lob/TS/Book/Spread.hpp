#pragma once

// =============================================================================
// Spread - 买卖价差
// =============================================================================
//   spread = P_{1,t}^{M,A} - P_{1,t}^{M,B}  (元)
//   【勿删】计算图内无消费者 (Book 自算 bp 价差), 无落盘列; 留作 L_sec 盘口回放的接入口 (输出口 = 当前簿价差).
// =============================================================================

#include "features/DataDefine.hpp"

class Spread {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  Spread(const Port &bid_price_0,
         const Port &ask_price_0)
      : bid_price_0_(bid_price_0), ask_price_0_(ask_price_0) {}

  inline void compute() {
    float bid = bid_price_0_;
    float ask = ask_price_0_;
    y[value] = (bid > 0 && ask > 0) ? (ask - bid) : 0.0f;
  }

private:
  const Port &bid_price_0_;
  const Port &ask_price_0_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Spread(N) N(Spread, (Spread), (Depth.bid_price[0], Depth.ask_price[0]), onDepth)

// #define FIELDS_L_sec_Spread(X, CAT1) X(spread, CAT1, AUTO, "Bid-Ask Spread", "买卖价差", "卖一减买一", R"(P_{1,t}^{M,A} - P_{1,t}^{M,B})", TS(Spread, None, None))
