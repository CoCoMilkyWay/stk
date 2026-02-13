#pragma once

// =============================================================================
// REPRE (Depth Representation) - 多档深度表征空间 (DUMMY)
// =============================================================================
// 多档深度的自监督表征学习
// 当前实现: 返回0作为placeholder, 后续需要接入预训练backbone
//
// 输入频率: ON_DEPTH
// 输出频率: per sec
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class DepthRepresentation {
public:
  DepthRepresentation(CBuffer<float, L2::BLEN> &depth_repre)
      : depth_repre_(depth_repre) {}

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

private:
  CBuffer<float, L2::BLEN> &depth_repre_;
  float value_ = 0.0f;
};
