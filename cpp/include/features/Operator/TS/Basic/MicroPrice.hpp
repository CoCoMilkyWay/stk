#pragma once

// =============================================================================
// MicroPrice - 微观价格 (量加权中间价)
// =============================================================================
//   micro_price = (P_{1,t}^{M,A} · V_{1,t}^{M,B} + P_{1,t}^{M,B} · V_{1,t}^{M,A}) / (V_{1,t}^{M,B} + V_{1,t}^{M,A})
// =============================================================================

#include "features/DataDefine.hpp"

class MicroPrice {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  MicroPrice(const Series &bid_price_0,
             const Series &ask_price_0,
             const Series &bid_qty_0,
             const Series &ask_qty_0)
      : bid_price_0_(bid_price_0), ask_price_0_(ask_price_0),
        bid_qty_0_(bid_qty_0), ask_qty_0_(ask_qty_0) {}

  inline void compute() {
    float bid_price = bid_price_0_.back();
    float ask_price = ask_price_0_.back();
    float bid_qty = bid_qty_0_.back();
    float ask_qty = -ask_qty_0_.back(); // 卖方存负值
    y[value] = (ask_price * bid_qty + bid_price * ask_qty) / (bid_qty + ask_qty);
  }

private:
  const Series &bid_price_0_;
  const Series &ask_price_0_;
  const Series &bid_qty_0_;
  const Series &ask_qty_0_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_MicroPrice(N) N(MicroPrice, (MicroPrice), (Depth.bid_price[0], Depth.ask_price[0], Depth.bid_qty[0], Depth.ask_qty[0]), onDepth)
