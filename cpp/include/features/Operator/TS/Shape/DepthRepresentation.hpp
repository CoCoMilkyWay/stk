#pragma once

// =============================================================================
// REPRE (Depth Representation) - 多档深度表征空间 (DUMMY)
// =============================================================================
// 多档深度的自监督表征学习
//
// 【公式定义】
//   depth_repre = f_θ(X), X = {V_{i}^{B}, V_{i}^{A}}_{i=1}^{N}
//
// 【触发域】
//   compute: onMinute
//   flush:   onMinute
//
// 【输入输出】
//   输入: 多档深度数据 (onDepth)
//   输出: depth_repre (onMinute)
//
// 【备注】
//   - 当前实现: 返回0作为placeholder
//   - 后续需要接入预训练backbone (自编码器、对比学习等)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class DepthRepresentation {
public:
  enum Out : size_t { value,
                      kCount };

  explicit DepthRepresentation(CBuffer<float, L2::BLEN> (&out)[kCount])
      : depth_repre_(out[value]) {}

  inline void compute() {
    // DUMMY实现：当前仅返回占位符0
    // 未来实现：将多档深度数据输入预训练的表征学习backbone
    // 提取高维深度特征的低维表征（如自编码器、对比学习等）
    value_ = 0.0f;
  }

  inline void flush() {
    // 将compute中的表征值写入CBuffer
    // 当前为占位符，后续接入实际模型后会输出有意义的表征
    depth_repre_.push_back(value_);
  }

  inline void reset() {}

private:
  CBuffer<float, L2::BLEN> &depth_repre_;
  float value_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_DepthRepresentation(N) N(DepthRepresentation, (DepthRepresentation), (), onMinute, onMinute)

#define FIELDS_L1_DepthRepresentation(X) \
  X(depth_repre, 1, DATA, TS, SHAPE, RAW, NONE, "00/100/00", "Depth Representation", "多档深度表征空间", "多档深度表征学习(降频)", R"(\text{depth\_repre}_t = f_{\theta}(X_t), \quad X_t = \{V_{i,t}^{M,B}, V_{i,t}^{M,A}\}_{i=1}^{N})", OP(DepthRepresentation))
