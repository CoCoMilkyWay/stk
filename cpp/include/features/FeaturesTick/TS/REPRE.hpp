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

  void compute() {
    // DUMMY: placeholder
    value_ = 0.0f;
  }

  void flush() { depth_repre_.push_back(value_); }

private:
  CBuffer<float, L2::BLEN> &depth_repre_;
  float value_ = 0.0f;
};
