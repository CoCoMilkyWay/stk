#pragma once

// =============================================================================
// Para (Parabola Fit) - 抛物线拟合
// =============================================================================
// 对depth做二次拟合: V_i ~ c0 + c1*i + c2*i^2
//   c0 - 截距 (近端流动性)
//   c1 - 斜率 (风偏方向)
//   c2 - 曲率 (<0:近端订单块; >0:做市类挂单)
//
// 模板参数:
//   IS_BID - true=买侧(b_para), false=卖侧(a_para)
//   COEF   - 0=c0, 1=c1, 2=c2
//
// DAG中使用:
//   Para<true, 0>  b_para_c0{bid_qty_, ask_qty_, b_para_c0_};
//   Para<true, 1>  b_para_c1{bid_qty_, ask_qty_, b_para_c1_};
//   Para<true, 2>  b_para_c2{bid_qty_, ask_qty_, b_para_c2_};
//   Para<false, 0> a_para_c0{bid_qty_, ask_qty_, a_para_c0_};
//   ...
//
// 使用30档数据拟合，最小二乘法: (X'X)^-1 * X'y
// X = [1, i, i^2] for i=0..29
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <bool IS_BID, int COEF, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Para {
  static_assert(COEF >= 0 && COEF <= 2, "COEF must be 0, 1, or 2");
  static_assert(DEPTH_SIZE >= 3, "Need at least 3 levels for parabola fit");

public:
  Para(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
       CBuffer<float, L2::BLEN> &out)
      : bid_qty_(bid_qty), ask_qty_(ask_qty), out_(out) {
    // 预计算 (X'X)^-1 矩阵元素
    // X'X = [n,    Σi,   Σi²  ]
    //       [Σi,   Σi²,  Σi³  ]
    //       [Σi²,  Σi³,  Σi⁴  ]
    double s1 = 0, s2 = 0, s3 = 0, s4 = 0;
    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      double di = static_cast<double>(i);
      s1 += di;
      s2 += di * di;
      s3 += di * di * di;
      s4 += di * di * di * di;
    }
    double n = static_cast<double>(DEPTH_SIZE);

    // 计算逆矩阵 (使用伴随矩阵法)
    double det = n * (s2 * s4 - s3 * s3) - s1 * (s1 * s4 - s2 * s3) + s2 * (s1 * s3 - s2 * s2);
    double inv_det = 1.0 / det;

    // 只存储我们需要的行 (COEF行)
    if constexpr (COEF == 0) {
      inv_row_[0] = static_cast<float>((s2 * s4 - s3 * s3) * inv_det);
      inv_row_[1] = static_cast<float>((s2 * s3 - s1 * s4) * inv_det);
      inv_row_[2] = static_cast<float>((s1 * s3 - s2 * s2) * inv_det);
    } else if constexpr (COEF == 1) {
      inv_row_[0] = static_cast<float>((s2 * s3 - s1 * s4) * inv_det);
      inv_row_[1] = static_cast<float>((n * s4 - s2 * s2) * inv_det);
      inv_row_[2] = static_cast<float>((s1 * s2 - n * s3) * inv_det);
    } else { // COEF == 2
      inv_row_[0] = static_cast<float>((s1 * s3 - s2 * s2) * inv_det);
      inv_row_[1] = static_cast<float>((s1 * s2 - n * s3) * inv_det);
      inv_row_[2] = static_cast<float>((n * s2 - s1 * s1) * inv_det);
    }
  }

  inline void compute() {
    // 最小二乘法拟合抛物线：V_i ~ c0 + c1*i + c2*i^2
    // 计算 X'y = [Σv, Σi*v, Σi²*v]，即观测值的加权和
    float xy0 = 0.0f, xy1 = 0.0f, xy2 = 0.0f;

    for (size_t i = 0; i < DEPTH_SIZE; ++i) {
      float v;
      // 根据IS_BID选择买侧或卖侧数据
      if constexpr (IS_BID) {
        v = bid_qty_[i].back();      // 买i+1档数量
      } else {
        v = -ask_qty_[i].back();     // 卖i+1档数量（取反）
      }

      float fi = static_cast<float>(i);
      xy0 += v;           // Σv (总量)
      xy1 += fi * v;      // Σi*v (一阶矩)
      xy2 += fi * fi * v; // Σi²*v (二阶矩)
    }

    // 用预计算的逆矩阵行提取目标系数: c[COEF] = inv_row * [xy0, xy1, xy2]'
    // c0: 截距（近端流动性），c1: 斜率（倾斜方向），c2: 曲率（凸凹性）
    value_ = inv_row_[0] * xy0 + inv_row_[1] * xy1 + inv_row_[2] * xy2;
  }

  inline void flush() {
    // 将compute中计算的抛物线系数写入输出CBuffer
    out_.push_back(value_);
  }

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float inv_row_[3]; // (X'X)^-1 的第COEF行
  float value_ = 0.0f;
};
