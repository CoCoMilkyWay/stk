#pragma once

// =============================================================================
// MicroPrice - 微观价格 (量加权中间价)
// =============================================================================
//   micro_price = (P_{1,t}^{M,A} · V_{1,t}^{M,B} + P_{1,t}^{M,B} · V_{1,t}^{M,A}) / (V_{1,t}^{M,B} + V_{1,t}^{M,A})
//   集合竞价 (交叉簿): 一档量加权无意义, 取 LOB 预撮合参考价 (与 MidPrice 同口, 见彼处注释).
// =============================================================================

#include "features/DataDefine.hpp"

class MicroPrice {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  MicroPrice(const TickData &tick_data,
             const Series &bid_price_0,
             const Series &ask_price_0,
             const Series &bid_qty_0,
             const Series &ask_qty_0)
      : tick_data_(tick_data),
        bid_price_0_(bid_price_0), ask_price_0_(ask_price_0),
        bid_qty_0_(bid_qty_0), ask_qty_0_(ask_qty_0) {}

  inline void compute() {
    const float ref = tick_data_.lob.auction_ref_price; // 竞价预撮合价 (连续竞价恒 0)
    if (ref > 0.0f) {
      y[value] = ref;
      return;
    }
    float bid_price = bid_price_0_.back();
    float ask_price = ask_price_0_.back();
    float bid_qty = bid_qty_0_.back();
    float ask_qty = -ask_qty_0_.back(); // 卖方存负值
    const float denom = bid_qty + ask_qty;
    // 两侧一档量皆为 0 (挂单全撤, 档位仍在): 权重退化, 取普通中间价 —— 仍是价格量纲, 不产 NaN 污染下游时间加权
    y[value] = denom > 0.0f ? (ask_price * bid_qty + bid_price * ask_qty) / denom : (bid_price + ask_price) * 0.5f;
  }

private:
  const TickData &tick_data_;
  const Series &bid_price_0_;
  const Series &ask_price_0_;
  const Series &bid_qty_0_;
  const Series &ask_qty_0_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_MicroPrice(N) N(MicroPrice, (MicroPrice), (tick_data, Depth.bid_price[0], Depth.ask_price[0], Depth.bid_qty[0], Depth.ask_qty[0]), onDepth)
