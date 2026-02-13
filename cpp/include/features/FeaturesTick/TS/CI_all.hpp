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
// 模板参数:
//   BUFFER_DEPTH - 内部缓存深度，用于降频 (0=不缓存直接输出, 5=缓存5个样本平均后输出)
//
// DAG中使用:
//   CI_all<0> Ci_all{td, Ci_all_};        // 秒级，直接输出
//   CI_all<5> Ci_all{td, Ci_all_};        // 降频到分钟级
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include <array>

template <size_t BUFFER_DEPTH = 0>
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

    // 如果有内部缓存，存入缓存
    if constexpr (BUFFER_DEPTH > 0) {
      buffer_[buffer_idx_] = value_;
      buffer_idx_ = (buffer_idx_ + 1) % BUFFER_DEPTH;
      if (buffer_size_ < BUFFER_DEPTH) {
        buffer_size_++;
      }
    }
  }

  inline void flush() {
    if constexpr (BUFFER_DEPTH == 0) {
      // 无缓存：直接输出
      out_.push_back(value_);
    } else {
      // 有缓存：取最近 N 个样本平均
      float sum = 0.0f;
      for (size_t i = 0; i < buffer_size_; ++i) {
        sum += buffer_[i];
      }
      float avg = buffer_size_ > 0 ? sum / static_cast<float>(buffer_size_) : 0.0f;
      out_.push_back(avg);

      // 清空缓存，准备下一轮
      buffer_size_ = 0;
      buffer_idx_ = 0;
    }
  }

private:
  TickData &td_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;

  // 内部缓存（仅当 BUFFER_DEPTH > 0 时使用）
  std::array<float, (BUFFER_DEPTH > 0 ? BUFFER_DEPTH : 1)> buffer_{};
  size_t buffer_idx_ = 0;
  size_t buffer_size_ = 0;
};
