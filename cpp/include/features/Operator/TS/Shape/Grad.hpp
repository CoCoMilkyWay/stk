#pragma once

// =============================================================================
// GRAD (Gradient) - 深度梯度
// =============================================================================
// 前N档的平均梯度 (一阶差分均值)
//
// 【公式定义】
//   GRAD_N = (1/(N-1)) · Σ (V_{i+1} - V_i), i=1..N-1
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: bid_qty[0:N-1] (onDepth), ask_qty[0:N-1] (onDepth)
//   输出: {b/a}_N_c1 (onMinute)
//
// 【模板参数】
//   N_LEVELS - 档位数
//   IS_BID   - true=买侧(b_grad), false=卖侧(a_grad)
//
// 【使用示例】
//   Grad<5, true>  b_5_c1{bid_qty_, ask_qty_, b_5_c1_};
//   Grad<5, false> a_5_c1{bid_qty_, ask_qty_, a_5_c1_};
//
// 【备注】
//   - 正值表示越远离盘口数量越多（做市类挂单）
//   - 负值表示集中在盘口（冲击类订单）
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

template <size_t N_LEVELS, bool IS_BID, size_t DEPTH_SIZE = L2::LOB_DEPTH>
class Grad {
  static_assert(N_LEVELS >= 2 && N_LEVELS <= DEPTH_SIZE, "N_LEVELS must be >= 2");

public:
  enum Out : size_t { value,
                      kCount };

  Grad(const CBuffer<float, L2::BLEN> (&bid_qty)[DEPTH_SIZE],
       const CBuffer<float, L2::BLEN> (&ask_qty)[DEPTH_SIZE],
       CBuffer<float, L2::BLEN> (&out)[kCount])
      : bid_qty_(bid_qty), ask_qty_(ask_qty), out_(out[value]) {}

  inline void compute() {
    float sum_diff = 0.0f;

    // 计算前N档的一阶差分：ΔV_i = V_{i+1} - V_i
    for (size_t i = 0; i < N_LEVELS - 1; ++i) {
      float v_i, v_ip1;
      // 根据IS_BID选择买侧或卖侧数据
      if constexpr (IS_BID) {
        v_i = bid_qty_[i].back();       // 第i+1档数量
        v_ip1 = bid_qty_[i + 1].back(); // 第i+2档数量
      } else {
        v_i = -ask_qty_[i].back(); // 卖方取反
        v_ip1 = -ask_qty_[i + 1].back();
      }
      sum_diff += v_ip1 - v_i; // 累加差分
    }

    // 计算平均梯度：总差分 / (N-1)
    // 正值表示越远离盘口数量越多（做市类挂单），负值表示集中在盘口（冲击类订单）
    value_ = sum_diff / static_cast<float>(N_LEVELS - 1);
  }

  inline void flush() {
    // 将compute中计算的梯度值写入输出CBuffer
    out_.push_back(value_);
  }

  inline void reset() {}

private:
  const CBuffer<float, L2::BLEN> (&bid_qty_)[DEPTH_SIZE];
  const CBuffer<float, L2::BLEN> (&ask_qty_)[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> &out_;
  float value_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Grad_a_5_c1(N) N(Grad_a_5_c1, (Grad<5, false>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Grad_a_5_c1(X) \
  X(a_5_c1, 1, DATA, TS, SHAPE, RAW, NONE, "00/100/00", "Top 5-level Ask Grad", "卖侧五档梯度", "卖侧梯度(近端斜率)(降频)", R"(\frac{1}{N-1}\sum_{i=1}^{N-1}(V_{i+1,t}^{M,A} - V_{i,t}^{M,A}), \quad N = 5)", OP(Grad_a_5_c1))

#define NODE_Grad_b_5_c1(N) N(Grad_b_5_c1, (Grad<5, true>), (DepthData.bid_qty, DepthData.ask_qty), onMinute, onMinute)

#define FIELDS_L1_Grad_b_5_c1(X) \
  X(b_5_c1, 1, DATA, TS, SHAPE, RAW, NONE, "00/100/00", "Top 5-level Bid Grad", "买侧五档梯度", "买侧梯度(近端斜率)(降频)", R"(\frac{1}{N-1}\sum_{i=1}^{N-1}(V_{i+1,t}^{M,B} - V_{i,t}^{M,B}), \quad N = 5)", OP(Grad_b_5_c1))
