#pragma once

// =============================================================================
// TLR (Top Level Ratio) - 顶部档位占比
// =============================================================================
// 前N档占总量的比例，衡量是否容易被击穿
//   TBR_N = Σ V_bid[1:N] / 全市场买单量
//   TAR_N = Σ V_ask[1:N] / 全市场卖单量
//
// 数据来源:
//   前N档: 从 bid_qty_/ask_qty_ 数组累加
//   全市场量: TickData.lob.all_bid_volume / all_ask_volume (交易所提供)
//
// 模板参数:
//   N_LEVELS - 顶部档位数
//   IS_BID   - true=买侧(TBR), false=卖侧(TAR)
//   BUFFER_DEPTH - 内部缓存深度，用于降频 (0=不缓存直接输出, 5=缓存5个样本平均后输出)
//
// DAG中使用:
//   TLR<5, true, 0>  tbr_5{bid_qty_, ask_qty_, td, tbr_5_};  // 秒级直接输出
//   TLR<5, false, 0> tar_5{bid_qty_, ask_qty_, td, tar_5_};  // 秒级直接输出
//   TLR<5, true, 5>  tbr_5{bid_qty_, ask_qty_, td, tbr_5_};  // 降频到分钟级
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include <array>

template <size_t N_LEVELS, bool IS_BID, size_t BUFFER_DEPTH = 0, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class TLR {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  TLR(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
      const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
      TickData &td,
      CBuffer<float, L2::BLEN> &out)
      : bid_qty_(bid_qty), ask_qty_(ask_qty), td_(td), out_(out) {}

  inline void compute() {
    float top_sum = 0.0f; // 前N档总量

    // 遍历前N档，累计数量
    for (size_t i = 0; i < N_LEVELS; ++i) {
      // 根据IS_BID标志选择买侧或卖侧数据
      if constexpr (IS_BID) {
        top_sum += bid_qty_[i].back(); // 累加前N档买量
      } else {
        top_sum += -ask_qty_[i].back(); // 累加前N档卖量（取反）
      }
    }

    // 从TickData读取交易所提供的全市场挂单量作为分母
    float total_sum;
    if constexpr (IS_BID) {
      total_sum = static_cast<float>(td_.lob.all_bid_volume); // 全市场买单量
    } else {
      total_sum = static_cast<float>(td_.lob.all_ask_volume); // 全市场卖单量
    }

    // 计算顶部档位占比：前N档 / 全市场总量
    // 值越大说明订单集中在前N档，容易被击穿
    value_ = total_sum > 1e-6f ? top_sum / total_sum : 0.0f;

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
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  TickData &td_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;

  // 内部缓存（仅当 BUFFER_DEPTH > 0 时使用）
  std::array<float, (BUFFER_DEPTH > 0 ? BUFFER_DEPTH : 1)> buffer_{};
  size_t buffer_idx_ = 0;
  size_t buffer_size_ = 0;
};
