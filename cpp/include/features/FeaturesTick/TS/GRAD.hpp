#pragma once

// =============================================================================
// GRAD (Gradient) - 深度梯度
// =============================================================================
// 前N档的平均梯度 (一阶差分均值)
//   GRAD_N = (1/(N-1)) * Σ (V_{i+1} - V_i), i=0..N-2
//
// 模板参数:
//   N_LEVELS - 档位数
//   IS_BID   - true=买侧(b_grad), false=卖侧(a_grad)
//
// DAG中使用:
//   GRAD<5, true>  b_5_c1{bid_qty_, ask_qty_, b_5_c1_};
//   GRAD<5, false> a_5_c1{bid_qty_, ask_qty_, a_5_c1_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, bool IS_BID, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class GRAD {
  static_assert(N_LEVELS >= 2 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS must be >= 2");

public:
  GRAD(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
       CBuffer<float, L2::BLEN> &out)
      : bid_qty_(bid_qty), ask_qty_(ask_qty), out_(out) {}

  inline void compute() {
    float sum_diff = 0.0f;

    // 计算前N档的一阶差分：ΔV_i = V_{i+1} - V_i
    for (size_t i = 0; i < N_LEVELS - 1; ++i) {
      float v_i, v_ip1;
      // 根据IS_BID选择买侧或卖侧数据
      if constexpr (IS_BID) {
        v_i = bid_qty_[i].back();      // 第i+1档数量
        v_ip1 = bid_qty_[i + 1].back(); // 第i+2档数量
      } else {
        v_i = -ask_qty_[i].back();     // 卖方取反
        v_ip1 = -ask_qty_[i + 1].back();
      }
      sum_diff += v_ip1 - v_i;  // 累加差分
    }

    // 计算平均梯度：总差分 / (N-1)
    // 正值表示越远离盘口数量越多（做市类挂单），负值表示集中在盘口（冲击类订单）
    value_ = sum_diff / static_cast<float>(N_LEVELS - 1);
  }

  inline void flush() {
    // 将compute中计算的梯度值写入输出CBuffer
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};

// =============================================================================
// GRAD_IMBA - 梯度失衡
// =============================================================================
// imba = (b_grad - a_grad) / (|b_grad| + |a_grad|)
// =============================================================================

class GRAD_IMBA {
public:
  GRAD_IMBA(const CBuffer<float, L2::BLEN> &bid_grad,
            const CBuffer<float, L2::BLEN> &ask_grad,
            CBuffer<float, L2::BLEN> &out)
      : bid_grad_(bid_grad), ask_grad_(ask_grad), out_(out) {}

  inline void compute() {
    // 从买卖两侧的梯度CBuffer读取最新值
    float b = bid_grad_.back();  // 买侧梯度
    float a = ask_grad_.back();  // 卖侧梯度
    // 计算梯度失衡：(买侧-卖侧) / (|买侧|+|卖侧|)
    // 值域[-1,1]
    float denom = std::abs(b) + std::abs(a);
    value_ = denom > 1e-6f ? (b - a) / denom : 0.0f;
  }

  inline void flush() {
    // 将compute中计算的梯度失衡写入输出CBuffer
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> &bid_grad_;
  const CBuffer<float, L2::BLEN> &ask_grad_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
