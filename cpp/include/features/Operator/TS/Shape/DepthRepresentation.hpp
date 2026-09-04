#pragma once

// =============================================================================
// REPRE (Depth Representation) - 多档深度表征 (DUMMY, 占位 0)
// =============================================================================
//   depth_repre = f_θ(X), X = {V_i^B, V_i^A}_{i=1}^N   后续接入预训练 backbone
// =============================================================================

#include "codec/L2_DataType.hpp"

class DepthRepresentation {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  DepthRepresentation() = default;

  inline void compute() { y[value] = 0.0f; }
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_DepthRepresentation(N) N(DepthRepresentation, (DepthRepresentation), (), onMinute, onMinute)

#define FIELDS_L1_DepthRepresentation(X) \
  X(depth_repre, 1, DATA, SHAPE, RAW, NONE, "00/100/00", "Depth Representation", "多档深度表征空间", "多档深度表征学习(降频)", R"(\text{depth\_repre}_t = f_{\theta}(X_t), \quad X_t = \{V_{i,t}^{M,B}, V_{i,t}^{M,A}\}_{i=1}^{N})", OP(DepthRepresentation))
