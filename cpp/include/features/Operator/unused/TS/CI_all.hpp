#pragma once

// =============================================================================
// CI_all (Cumulative Imbalance - All Levels) - 全档累计失衡
// =============================================================================
//   CI_all = (V_{all}^{M,B} - V_{all}^{M,A}) / (V_{all}^{M,B} + V_{all}^{M,A})
//   数据来源: 交易所提供的全市场挂单量 (含 30 档之外)
// =============================================================================

#include "features/DataDefine.hpp"

class CI_all {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  explicit CI_all(TickData &td) : td_(td) {}

  inline void compute() {
    float sum_bid = static_cast<float>(td_.lob.all_bid_volume);
    float sum_ask = static_cast<float>(td_.lob.all_ask_volume);
    float denom = sum_bid + sum_ask;
    y[value] = denom > 1e-6f ? (sum_bid - sum_ask) / denom : 0.0f;
  }

private:
  TickData &td_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Ci_all(N) N(Ci_all, (CI_all), (tick_data), onMinute)

#define FIELDS_L1_Ci_all(X, CAT1) \
  X(ci_all, CAT1, RATIO, NONE, "Cumu Imba All-Level", "累计全档失衡", "累计所有档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{\infty}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{\infty}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_all))
