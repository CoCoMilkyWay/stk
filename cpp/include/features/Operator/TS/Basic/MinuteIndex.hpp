#pragma once

// =============================================================================
// MinuteIndex - Minute 索引: 分钟索引的正弦相位嵌入
// =============================================================================
//   min = sin(2π · l1_index / 60)   (60 分钟一周期, 相位连续可导, 频谱干净)
//   minute_index = l1_index          (原始索引 [0-254])
// =============================================================================

#include "features/DataDefine.hpp"
#include <cmath>
#include <numbers>

constexpr float MIN_PHASE_SCALE = 2.0f * std::numbers::pi_v<float> / 60.0f;

class MinuteIndex {
public:
  enum Out : size_t { min,
                      minute_index,
                      kCount };
  float y[kCount] = {};

  explicit MinuteIndex(const MinuteData &md) : md_(md) {}

  void compute() {
    float clock_min = static_cast<float>(md_.l1_index);
    y[min] = std::sin(clock_min * MIN_PHASE_SCALE);
    y[minute_index] = clock_min;
  }

private:
  const MinuteData &md_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_MinuteIndex(N) N(MinuteIndex, (MinuteIndex), (minute_data), onMinute)

#define FIELDS_L1_MinuteIndex(X) \
  X(min, BASIC, OSCILLATOR, SINCOS, "Time Min Phase", "时间-分钟相位", "用于因子组合", R"(\sin(\frac{2\pi t}{60\mathrm{m}}))", OP(MinuteIndex, min))
