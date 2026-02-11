#pragma once

// =============================================================================
// ParaImba - 抛物线参数失衡
// =============================================================================
// imba = (b_coef - a_coef) / (|b_coef| + |a_coef|)
//
// DAG中使用:
//   ParaImba<0> imba_para_c0{b_para_c0_, a_para_c0_, imba_para_c0_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <cmath>

template <int COEF>
class ParaImba {
public:
  ParaImba(const CBuffer<float, L2::BLEN> &bid_coef,
           const CBuffer<float, L2::BLEN> &ask_coef,
           CBuffer<float, L2::BLEN> &out)
      : bid_coef_(bid_coef), ask_coef_(ask_coef), out_(out) {}

  inline void compute() {
    // 从买卖两侧的抛物线系数CBuffer读取最新值
    float b = bid_coef_.back();  // 买侧的c0/c1/c2系数
    float a = ask_coef_.back();  // 卖侧的c0/c1/c2系数
    // 计算系数失衡：(买侧-卖侧) / (|买侧|+|卖侧|)
    // 值域[-1,1]，正值表示买侧该特征更强
    float denom = std::abs(b) + std::abs(a);
    value_ = denom > 1e-6f ? (b - a) / denom : 0.0f;
  }

  inline void flush() {
    // 将compute中计算的系数失衡写入输出CBuffer
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> &bid_coef_;
  const CBuffer<float, L2::BLEN> &ask_coef_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};
