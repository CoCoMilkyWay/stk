#pragma once

// =============================================================================
// ParaImba - 抛物线参数失衡
// =============================================================================
// 对比买卖两侧抛物线系数的绝对值大小
//
// 【公式定义】
//   para_imba = (|coef^B| - |coef^A|) / (|coef^B| + |coef^A|)
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: bid_coef (onMinute), ask_coef (onMinute)
//   输出: para_imba_cN (onMinute)
//
// 【模板参数】
//   COEF - 系数索引 (0=c0, 1=c1, 2=c2)
//
// 【使用示例】
//   ParaImba<0> imba_para_c0{b_para_c0_, a_para_c0_, imba_para_c0_};
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include <cmath>

template <int COEF>
class ParaImba {
public:
  enum Out : size_t { value,
                      kCount };

  ParaImba(const CBuffer<float, L2::BLEN> &bid_coef,
           const CBuffer<float, L2::BLEN> &ask_coef,
           CBuffer<float, L2::BLEN> (&out)[kCount])
      : bid_coef_(bid_coef), ask_coef_(ask_coef), out_(out[value]) {}

  inline void compute() {
    // 从买卖两侧的抛物线系数CBuffer读取最新值
    float b = bid_coef_.back(); // 买侧的c0/c1/c2系数
    float a = ask_coef_.back(); // 卖侧的c0/c1/c2系数

    // 计算绝对值
    float abs_b = std::abs(b);
    float abs_a = std::abs(a);

    // 计算系数失衡：(|买侧|-|卖侧|) / (|买侧|+|卖侧|)
    // 值域[-1,1]，正值表示买侧该系数绝对值更大
    float denom = abs_b + abs_a;
    value_ = denom > 1e-6f ? (abs_b - abs_a) / denom : 0.0f;
  }

  inline void flush() {
    // 将compute中计算的系数失衡写入输出CBuffer
    out_.push_back(value_);
  }

  inline void reset() {}

private:
  const CBuffer<float, L2::BLEN> &bid_coef_;
  const CBuffer<float, L2::BLEN> &ask_coef_;
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_ParaImba_c0(N) N(ParaImba_c0, (ParaImba<0>), (Para_b_c0.out(), Para_a_c0.out()), onMinute, onMinute)

#define FIELDS_L1_ParaImba_c0(X) \
  X(imba_para_c0, 1, DATA, TS, IMBALANCE, RATIO, NONE, "00/100/00", "Depth Parabola c0 Imba", "买卖抛物线截距失衡", "对比买卖近端流动性(降频)", R"(\frac{|c_{0,t}^{M,B}| - |c_{0,t}^{M,A}|}{|c_{0,t}^{M,B}| + |c_{0,t}^{M,A}|})", OP(ParaImba_c0))

#define NODE_ParaImba_c1(N) N(ParaImba_c1, (ParaImba<1>), (Para_b_c1.out(), Para_a_c1.out()), onMinute, onMinute)

#define FIELDS_L1_ParaImba_c1(X) \
  X(imba_para_c1, 1, DATA, TS, IMBALANCE, RATIO, NONE, "00/100/00", "Depth Parabola c1 Imba", "买卖抛物线斜率失衡", "对比买卖风偏(降频)", R"(\frac{|c_{1,t}^{M,B}| - |c_{1,t}^{M,A}|}{|c_{1,t}^{M,B}| + |c_{1,t}^{M,A}|})", OP(ParaImba_c1))

#define NODE_ParaImba_c2(N) N(ParaImba_c2, (ParaImba<2>), (Para_b_c2.out(), Para_a_c2.out()), onMinute, onMinute)

#define FIELDS_L1_ParaImba_c2(X) \
  X(imba_para_c2, 1, DATA, TS, IMBALANCE, RATIO, NONE, "00/100/00", "Depth Parabola c2 Imba", "买卖抛物线曲率失衡", "对比买卖订单块距离(降频)", R"(\frac{|c_{2,t}^{M,B}| - |c_{2,t}^{M,A}|}{|c_{2,t}^{M,B}| + |c_{2,t}^{M,A}|})", OP(ParaImba_c2))
