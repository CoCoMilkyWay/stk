#pragma once

// =============================================================================
// MicroPrice - 微观价格 (量加权中间价)
// =============================================================================
//   micro_price = (P_{1,t}^{M,A} · V_{1,t}^{M,B} + P_{1,t}^{M,B} · V_{1,t}^{M,A}) / (V_{1,t}^{M,B} + V_{1,t}^{M,A})
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class MicroPrice {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  MicroPrice(const CBuffer<float, L2::BLEN> &bid_price_0,
             const CBuffer<float, L2::BLEN> &ask_price_0,
             const CBuffer<float, L2::BLEN> &bid_qty_0,
             const CBuffer<float, L2::BLEN> &ask_qty_0)
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
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
  const CBuffer<float, L2::BLEN> &bid_qty_0_;
  const CBuffer<float, L2::BLEN> &ask_qty_0_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_MicroPrice(N) N(MicroPrice, (MicroPrice), (DepthData.bid_price[0], DepthData.ask_price[0], DepthData.bid_qty[0], DepthData.ask_qty[0]), onDepth, onDepth)
