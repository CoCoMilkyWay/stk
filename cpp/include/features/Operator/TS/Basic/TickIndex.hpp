#pragma once

// =============================================================================
// TickIndex - Tick 索引: 分钟内秒位置的正弦相位嵌入
// =============================================================================
//   sec = sin(2π · (l0_index % 60) / 60)   (60 秒一周期, 相位连续可导, 频谱干净)
//   tick_index = l0_index                   (原始索引 [0-15299])
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"
#include "features/Misc/Misc.hpp"
#include <array>

// 60 秒周期 sin 查找表
inline const std::array<float, 60> &get_sec_phase_lut() {
  static const auto lut = []() {
    std::array<float, 60> result{};
    constexpr float scale = 2.0f * PI / 60.0f;
    for (int i = 0; i < 60; ++i)
      result[i] = std::sin(static_cast<float>(i) * scale);
    return result;
  }();
  return lut;
}

class TickIndex {
public:
  enum Out : size_t { sec,
                      tick_index,
                      kCount };
  float y[kCount] = {};

  explicit TickIndex(const TickData &td) : td_(td) {}

  inline void compute() {
    uint32_t idx = td_.l0_index;
    y[sec] = get_sec_phase_lut()[idx % 60];
    y[tick_index] = static_cast<float>(idx);
  }

private:
  const TickData &td_;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_TickIndex(N) N(TickIndex, (TickIndex), (tick_data), onTick, onTick)

#define FIELDS_L0_TickIndex(X) \
  X(sec, 1, DATA, BASIC, OSCILLATOR, SINCOS, "100/00/00", "Time Sec Phase", "时间-秒相位", "用于因子组合", R"(\sin(\frac{2\pi t}{60\mathrm{s}}))", OP(TickIndex, sec))
