#pragma once

// =============================================================================
// DepthIndex - 盘口更新时对应的原始 tick 索引 (l0_index), 供 depth 域算子对齐用
// =============================================================================

#include "features/DataDefine.hpp"

class DepthIndex {
public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  explicit DepthIndex(const TickData &td) : td_(td) {}

  inline void compute() { y[value] = static_cast<float>(td_.l0_index); }

private:
  const TickData &td_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_DepthIndex(N) N(DepthIndex, (DepthIndex), (tick_data), onDepth, onDepth)
