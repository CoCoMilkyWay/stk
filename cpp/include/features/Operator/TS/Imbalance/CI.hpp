#pragma once

// =============================================================================
// CI (Cumulative Imbalance) - 累计失衡
// =============================================================================
// 计算前N档的累计买卖失衡率
//
// 【公式定义】
//   CI_N = (Σ V_{i,t}^{M,B} - Σ V_{i,t}^{M,A}) / (Σ V_{i,t}^{M,B} + Σ V_{i,t}^{M,A}), i=1..N
//
// 【触发域】
//   compute: onDepth
//   flush:   onDepth
//
// 【输入输出】
//   输入: bid_qty[0:N-1] (onDepth), ask_qty[0:N-1] (onDepth)
//   输出: ci_N (onDepth)
//
// 【模板参数】
//   N_LEVELS - 累计档位数 (1, 5, 10, 30, ...)
//
// 【使用示例】
//   CI<1>  ci_1{BidQty_, AskQty_, Ci_1_};
//   CI<5>  ci_5{BidQty_, AskQty_, Ci_5_};
//   CI<10> ci_10{BidQty_, AskQty_, Ci_10_};
//   CI<30> ci_30{BidQty_, AskQty_, Ci_30_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class CI {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  enum Out : size_t { value,
                      kCount };

  CI(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
     const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
     CBuffer<float, L2::BLEN> (&out)[kCount])
      : bid_qty_(bid_qty), ask_qty_(ask_qty), out_(out[value]) {}

  inline void compute() {
    float sum_bid = 0.0f;
    float sum_ask = 0.0f;

    // 累加前N档买卖数量（从各档的CBuffer读取最新值）
    for (size_t i = 0; i < N_LEVELS; ++i) {
      sum_bid += bid_qty_[i].back();  // 买方数量（正值）
      sum_ask += -ask_qty_[i].back(); // 卖方数量（取反，变正值）
    }

    // 计算累计失衡率：(买量-卖量)/(买量+卖量)
    // 值域[-1,1]，正值表示买方占优，负值表示卖方占优
    float denom = sum_bid + sum_ask;
    value_ = denom > 1e-6f ? (sum_bid - sum_ask) / denom : 0.0f;
  }

  inline void flush() {
    out_.push_back(value_);
  }

  inline void reset() {}

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Ci_1(N) N(Ci_1, (CI<1>), (DepthData.bid_qty, DepthData.ask_qty), onDepth, onDepth)

#define FIELDS_L0_Ci_1(X) \
  X(ci_1, 1, DEPTH, TS, IMBALANCE, RATIO, NONE, "100/00/00", "Cumu Imba 1-Level", "顶部1档失衡", "顶部1档订单失衡率", R"(\frac{V_{1,t}^{M,B} - V_{1,t}^{M,A}}{V_{1,t}^{M,B} + V_{1,t}^{M,A}})", OP(Ci_1))

#define NODE_Ci_10(N) N(Ci_10, (CI<10>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Ci_10(X) \
  X(ci_10, 1, DATA, TS, IMBALANCE, RATIO, NONE, "00/100/00", "Cumu Imba 10-Level", "累计10档失衡", "累计10档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{10}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{10}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_10))

#define NODE_Ci_30(N) N(Ci_30, (CI<30>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Ci_30(X) \
  X(ci_30, 1, DATA, TS, IMBALANCE, RATIO, NONE, "00/100/00", "Cumu Imba 30-Level", "累计30档失衡", "累计30档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{30}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{30}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_30))

#define NODE_Ci_5(N) N(Ci_5, (CI<5>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Ci_5(X) \
  X(ci_5, 1, DATA, TS, IMBALANCE, RATIO, NONE, "00/100/00", "Cumu Imba 5-Level", "累计5档失衡", "累计5档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{5}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{5}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_5))
