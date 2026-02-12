#pragma once

// =============================================================================
// CI_all (Cumulative Imbalance - All Levels) - 全档累计失衡
// =============================================================================
// 使用交易所提供的全市场挂单量计算失衡率（包括30档之外的所有挂单）
//   CI_all = (全市场买量 - 全市场卖量) / (全市场买量 + 全市场卖量)
//
// 数据来源:
//   TickData.lob.all_bid_volume - 交易所提供的全市场买单总量
//   TickData.lob.all_ask_volume - 交易所提供的全市场卖单总量
//
// DAG中使用:
//   CI_all Ci_all{td, Ci_all_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class CI_all {
public:
  CI_all(TickData &td, CBuffer<float, L2::BLEN> &out) : td_(td), out_(out) {}

  inline void compute() {
    // 从TickData读取交易所提供的全市场挂单量
    float sum_bid = static_cast<float>(td_.lob.all_bid_volume); // 全市场买单量
    float sum_ask = static_cast<float>(td_.lob.all_ask_volume); // 全市场卖单量

    // 计算累计失衡率：(买量-卖量)/(买量+卖量)
    // 值域[-1,1]，正值表示买方占优，负值表示卖方占优
    float denom = sum_bid + sum_ask;
    value_ = denom > 1e-6f ? (sum_bid - sum_ask) / denom : 0.0f;
  }

  inline void flush() {
    // 将compute中计算的CI值写入输出CBuffer
    out_.push_back(value_);
  }

private:
  TickData &td_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
