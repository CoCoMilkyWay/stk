#pragma once

// =============================================================================
// CI (Cumulative Imbalance) - 累计失衡
// =============================================================================
// 计算前N档的累计买卖失衡率
//   CI_N = (Σ V_bid[1:N] - Σ V_ask[1:N]) / (Σ V_bid[1:N] + Σ V_ask[1:N])
//
// 模板参数:
//   N_LEVELS - 累计档位数 (1, 5, 10, 30, ...)
//   BUFFER_DEPTH - 内部缓存深度，用于降频 (0=不缓存直接输出, 5=缓存5个样本平均后输出)
//
// DAG中使用:
//   CI<1, 0>  Ci_1{BidQty_, AskQty_, Ci_1_};        // 秒级，直接输出
//   CI<5, 5>  Ci_5{BidQty_, AskQty_, Ci_5_};        // 降频到分钟级
//   CI<10, 5> Ci_10{BidQty_, AskQty_, Ci_10_};
//   CI<30, 5> Ci_30{BidQty_, AskQty_, Ci_30_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <array>

template <size_t N_LEVELS, size_t BUFFER_DEPTH = 0, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class CI {
  static_assert(N_LEVELS >= 1 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS out of range");

public:
  CI(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
     const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
     CBuffer<float, L2::BLEN> &out)
      : bid_qty_(bid_qty), ask_qty_(ask_qty), out_(out) {}

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
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;

  // 内部缓存（仅当 BUFFER_DEPTH > 0 时使用）
  std::array<float, (BUFFER_DEPTH > 0 ? BUFFER_DEPTH : 1)> buffer_{};
  size_t buffer_idx_ = 0;
  size_t buffer_size_ = 0;
};
