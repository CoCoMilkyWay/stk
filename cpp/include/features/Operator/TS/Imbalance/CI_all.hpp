#pragma once

// =============================================================================
// CI_all (Cumulative Imbalance - All Levels) - 全档累计失衡
// =============================================================================
// 使用交易所提供的全市场挂单量计算失衡率（包括30档之外的所有挂单）
//
// 【公式定义】
//   CI_all = (V_{all}^{M,B} - V_{all}^{M,A}) / (V_{all}^{M,B} + V_{all}^{M,A})
//
// 【触发域】
//   compute: onDepth
//   flush:   onDepth
//
// 【输入输出】
//   输入: TickData.lob.all_bid_volume (onDepth), all_ask_volume (onDepth)
//   输出: ci_all (onDepth)
//
// 【备注】
//   - 数据来源: 交易所提供的全市场挂单量
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class CI_all {
public:
  enum Out : size_t { value,
                      kCount };

  CI_all(TickData &td, CBuffer<float, L2::BLEN> (&out)[kCount]) : td_(td), out_(out[value]) {}

  inline void compute() {
    float sum_bid = static_cast<float>(td_.lob.all_bid_volume); // 全市场买单量
    float sum_ask = static_cast<float>(td_.lob.all_ask_volume); // 全市场卖单量

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
  TickData &td_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Ci_all(N) N(Ci_all, (CI_all), (tick_data), onMinute, onMinute)

#define FIELDS_L1_Ci_all(X) \
  X(ci_all, 1, DATA, TS, IMBALANCE, RATIO, NONE, "00/100/00", "Cumu Imba All-Level", "累计全档失衡", "累计所有档订单失衡率(降频)", R"(\frac{\sum_{i=1}^{\infty}(V_{i,t}^{M,B} - V_{i,t}^{M,A})}{\sum_{i=1}^{\infty}(V_{i,t}^{M,B} + V_{i,t}^{M,A})})", OP(Ci_all))
