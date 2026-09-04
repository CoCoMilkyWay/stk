#pragma once

// =============================================================================
// MidPrice - 中间价
// =============================================================================
// 计算买一卖一中间价
//
// 【公式定义】
//   mid_price = (P_{1,t}^{M,B} + P_{1,t}^{M,A}) / 2  (单位: 元)
//
// 【触发域】
//   compute: onDepth
//   flush:   onDepth
//
// 【输入输出】
//   输入: bid_price_0 (onDepth), ask_price_0 (onDepth)
//   输出: mid_price (onDepth)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class MidPrice {
public:
  enum Out : size_t { value,
                      kCount };

  MidPrice(const CBuffer<float, L2::BLEN> &bid_price_0,
           const CBuffer<float, L2::BLEN> &ask_price_0,
           CBuffer<float, L2::BLEN> (&out)[kCount])
      : bid_price_0_(bid_price_0),
        ask_price_0_(ask_price_0),
        buffer_(out[value]) {}

  inline void compute() {
    // 从BidPrice_[0]和AskPrice_[0] CBuffer读取买一卖一价格（已转换为元）
    float bid = bid_price_0_.back(); // 买一价
    float ask = ask_price_0_.back(); // 卖一价
    // 计算中间价：(买一+卖一)/2
    mid_value_ = (bid + ask) * 0.5f;
  }

  inline void flush() {
    // 将compute中计算的中间价写入CBuffer
    buffer_.push_back(mid_value_);
  }

  inline void reset() {}

private:
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
  CBuffer<float, L2::BLEN> &buffer_;
  float mid_value_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_MidPrice(N) N(MidPrice, (MidPrice), (DepthData.bid_price[0], DepthData.ask_price[0]), onDepth, onDepth)
