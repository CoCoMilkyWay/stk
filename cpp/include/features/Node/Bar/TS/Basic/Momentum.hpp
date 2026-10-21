#pragma once

// =============================================================================
// Momentum - 日频动量 (compute=onClose, 日频节点; Series 跨天保留)
// =============================================================================
//   ret_20d = P^adj_D / P^adj_{D−20} − 1   (CloseAdjDay 后复权收盘价; 回看 20 个交易日, 停牌日 = 携带价, 照常占一格)
//   历史不足 21 格 → NaN. 本 TU (bar_pipeline.cpp) precise-math: 缺失 NaN 经除法透传.
// =============================================================================

#include "features/DataDefine.hpp"

#include <cstddef>
#include <limits>

class Momentum {
public:
  enum Out : size_t { ret_20d,
                      kCount };
  float y[kCount] = {};

  explicit Momentum(const Series &close_adj) : close_adj_(close_adj) {}

  inline void compute() {
    constexpr size_t kLag = 20;
    static_assert(kLag < L2::BLEN, "Series 容量不足以回看 kLag 格");
    const size_t n = close_adj_.size();
    y[ret_20d] = n > kLag ? close_adj_.back() / close_adj_[n - 1 - kLag] - 1.0f : std::numeric_limits<float>::quiet_NaN();
  }

private:
  const Series &close_adj_; // CloseAdjDay.value (同域 onClose, 拓扑序在前, back() = 本日收盘)
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Momentum(N) N(Momentum, (Momentum), (CloseAdjDay.out(CloseAdjDay.value)), onClose)

#define FIELDS_L_1day_Momentum(X, CAT1) \
  X(ret_20d, CAT1, AUTO, "Return 20d", "20日收益率", "后复权收盘价20个交易日涨跌幅", R"(\frac{P^{adj}_D}{P^{adj}_{D-20}} - 1)", TS(Momentum, ret_20d, None, None))
