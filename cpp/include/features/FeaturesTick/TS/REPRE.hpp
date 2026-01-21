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

  // 每秒输出 (ON_DEPTH 时调用)
  void compute() {
    // DUMMY: 返回0作为placeholder
    // TODO: 接入预训练的 encoder backbone
    depth_repre_.push_back(0.0f);
  }

private:
  CBuffer<float, L2::BLEN> &depth_repre_;
};
