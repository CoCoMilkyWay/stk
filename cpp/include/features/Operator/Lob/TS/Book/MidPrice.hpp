#pragma once

// =============================================================================
// MidPrice - 中间价
// =============================================================================
//   mid_price = (P_{1,t}^{M,B} + P_{1,t}^{M,A}) / 2  (元)
//   集合竞价 (交叉簿, bid1 可 ≥ ask1): 均值无意义, 取 LOB 预撮合参考价 (最大成交量价位).
//   auction_ref_price 仅竞价期且簿交叉时 > 0, 其余恒 0 → 单分支, 无 NaN (fast-math 安全).
// =============================================================================

#include "features/DataDefine.hpp"

class MidPrice {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  MidPrice(const TickData &tick_data,
           const Series &bid_price_0,
           const Series &ask_price_0)
      : tick_data_(tick_data), bid_price_0_(bid_price_0), ask_price_0_(ask_price_0) {}

  inline void compute() {
    const float ref = tick_data_.lob.auction_ref_price; // 竞价预撮合价 (连续竞价恒 0)
    y[value] = ref > 0.0f ? ref : (bid_price_0_.back() + ask_price_0_.back()) * 0.5f;
  }

private:
  const TickData &tick_data_;
  const Series &bid_price_0_;
  const Series &ask_price_0_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_MidPrice(N) N(MidPrice, (MidPrice), (tick_data, Depth.bid_price[0], Depth.ask_price[0]), onDepth)
