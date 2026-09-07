#pragma once

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <vector>

// ============================================================================
// Time-of-Day Profile (日内轮廓去季节)
// ============================================================================
//
// 日内特征的 U 形是确定性周期, 不是单位根 —— 差分 / MA 去趋势都不对症, 第一位的
// 平稳化手段是减掉日内均值轮廓:
//   TodSub:  y_{d,t} = x_{d,t} - μ_t^{(<d)}
//   TodZ:    y_{d,t} = (x_{d,t} - μ_t^{(<d)}) / σ_t^{(<d)}
// μ_t / σ_t = 该资产该分钟槽截至 **昨日** 的 expanding Welford 统计 (per-asset expanding):
// 先用历史轮廓变换今天, 再把今天并入 —— 严格因果, 今天的值不进自己的轮廓,
// 跨天状态是 TS 算子允许的 (DataDefine.hpp: reset() 有跨天状态才写).
// 槛: 槽内天数 < kMinDays 输出 NaN (轮廓未成形, 不硬吐 0 污染下游统计).
// ============================================================================

namespace math::stationary {

struct TodProfile {
  enum class Mode : uint8_t { None = 0,
                              Sub,
                              Z };
  static constexpr const char *MODE_NAMES[] = {"无", "TOD 减均值", "TOD Z"};
  static constexpr uint32_t kMinDays = 2;

  struct Slot {
    float mean = 0.0f, m2 = 0.0f;
    uint32_t n = 0;
    float sd() const { return n >= 2 ? std::sqrt(m2 / static_cast<float>(n - 1)) : 0.0f; }
  };
  std::vector<Slot> slots; // [VR]

  void reset(size_t VR) { slots.assign(VR, Slot{}); }

  // 一天: x[VR] (NaN = 缺) → y[VR] 用截至昨日的轮廓变换, 然后今天并入轮廓. y 可 == x.
  void apply_day(const float *x, float *y, Mode mode) {
    assert(mode != Mode::None);
    const size_t VR = slots.size();
    for (size_t t = 0; t < VR; ++t) {
      const float v = x[t];
      Slot &s = slots[t];
      float out;
      if (v != v) {
        out = v; // 缺 → 缺, 轮廓不动
      } else {
        if (s.n < kMinDays) {
          out = std::nanf("");
        } else if (mode == Mode::Sub) {
          out = v - s.mean;
        } else {
          const float sd = s.sd();
          out = sd > 0.0f ? (v - s.mean) / sd : std::nanf(""); // 常数槽: 无尺度可言
        }
        // Welford 并入今天
        ++s.n;
        const float d = v - s.mean;
        s.mean += d / static_cast<float>(s.n);
        s.m2 += d * (v - s.mean);
      }
      y[t] = out;
    }
  }
};

} // namespace math::stationary
