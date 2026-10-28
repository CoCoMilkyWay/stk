#pragma once

// =============================================================================
// Momentum - 日频动量 (compute=onClose, 日频节点; 回看窗自持, 跨天保留)
// =============================================================================
//   ret_20d = P^adj_D / P^adj_{D−20} − 1   (CloseAdjDay 后复权收盘价; 回看 20 个交易日, 停牌日 = 携带价, 照常占一格)
//   窗 = 最近 kLag + 1 个交易日的 CloseAdjDay.value (同域 onClose, 每日 compute 一次推一格); 不满 → NaN.
//   本 TU (bar_pipeline.cpp) precise-math: 缺失 NaN 经除法透传.
// =============================================================================

#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

#include <cstddef>

class Momentum {
  static constexpr size_t kLag = 20;

public:
  enum Out : size_t { ret_20d,
                      kCount };
  float y[kCount] = {};

  explicit Momentum(const Port &close_adj) : close_adj_(close_adj) {}

  inline void compute() {
    win_.push_back(close_adj_);
    y[ret_20d] = win_.full() ? close_adj_ / win_.front() - 1.0f : kNaN;
  }

private:
  const Port &close_adj_;        // CloseAdjDay.value (同域 onClose, 拓扑序在前, 即本日收盘)
  CBuffer<float, kLag + 1> win_; // 回看窗: front() = D − kLag, back() = D
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Momentum(N) N(Momentum, (Momentum), (CloseAdjDay.out(CloseAdjDay.value)), onClose)

#define FIELDS_L_1day_Momentum(X, CAT1) \
  X(ret_20d, CAT1, AUTO, "Return 20d", "20日收益率", "后复权收盘价20个交易日涨跌幅", R"(\frac{P^{adj}_D}{P^{adj}_{D-20}} - 1)", TS(Momentum, ret_20d, None, None))
