#pragma once

// =============================================================================
// OrderInfo - 订单记录层: 当前笔的 price / timestamp(ms) / tickindex / volume(股) / dir(1=BID,-1=ASK)
//   按触发域分别实例化为 Taker / Maker / Cancel 三个节点
// =============================================================================

#include "features/DataDefine.hpp"

class OrderInfo {
public:
  enum Out : size_t { price,
                      timestamp,
                      tickindex,
                      volume,
                      dir,
                      kCount };
  float y[kCount] = {};

  explicit OrderInfo(const TickData &td) : td_(td) {}

  inline void compute() {
    const auto &lob = td_.lob;
    y[price] = lob.price;
    y[timestamp] = static_cast<float>(lob.hour * 3600000 + lob.minute * 60000 + lob.second * 1000 + lob.millisecond * 10);
    y[tickindex] = static_cast<float>(td_.l0_index);
    y[volume] = static_cast<float>(lob.volume);
    y[dir] = (lob.order_dir == L2::OrderDirection::BID) ? 1.0f : -1.0f;
  }

private:
  const TickData &td_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Cancel(N) N(Cancel, (OrderInfo), (tick_data), onCancel)

#define NODE_Maker(N) N(Maker, (OrderInfo), (tick_data), onMaker)

#define NODE_Taker(N) N(Taker, (OrderInfo), (tick_data), onTaker)
