#pragma once

// =============================================================================
// Noise - 合成白噪声 (compute=onMinute): 诊断链路 (PSD / ACF / 归一化) 的参照列, 无信息量
// =============================================================================
//   gauss_t ~ N(0, 1) i.i.d., 确定性: 值 = hash(asset_id, date, l1_index) → Box-Muller.
//   不依赖任何行情输入, 无跨分钟状态 → 重放任意重排逐值一致 (TS 纯函数契约). 永不 NaN.
//   期望频谱: 每 bin 功率 = 1 (平线); ACF: lag ≥ 1 为 0.
// =============================================================================

#include "features/DataDefine.hpp"
#include <cmath>
#include <cstdint>
#include <numbers>
#include <string>

class Noise {
public:
  enum Out : size_t { gauss,
                      kCount };
  float y[kCount] = {};

  Noise(const MinuteData &md, const std::string &date) : md_(md), date_(date) {}

  inline void compute() {
    const uint64_t h1 = mix(day_seed_ + md_.l1_index);
    const uint64_t h2 = mix(h1);
    const double u1 = (static_cast<double>(h1 >> 11) + 1.0) * 0x1.0p-53; // (0, 1]: log 不取 0
    const double u2 = static_cast<double>(h2 >> 11) * 0x1.0p-53;         // [0, 1)
    y[gauss] = static_cast<float>(std::sqrt(-2.0 * std::log(u1)) * std::cos(2.0 * std::numbers::pi * u2));
  }

  // 跨日: 日种子 = (asset_id, date) 混合; at_day_start 先设 date_ 再 reset, 此处读到的是当天
  void reset() {
    day_seed_ = mix((static_cast<uint64_t>(md_.asset_id) << 32) ^ static_cast<uint64_t>(std::stoul(date_)));
  }

private:
  // splitmix64 终混: 64 位雪崩, 相邻输入输出不相关
  static inline uint64_t mix(uint64_t z) {
    z += 0x9e3779b97f4a7c15ull;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    return z ^ (z >> 31);
  }

  const MinuteData &md_;
  const std::string &date_; // DAG_Root::date_
  uint64_t day_seed_ = 0;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_Noise(N) N(Noise, (Noise), (minute_data, date_), onMinute)

#define FIELDS_L1_Noise(X, CAT1) \
  X(noise_gauss, CAT1, AUTO, "White Noise", "白噪声", "合成 i.i.d. 标准正态白噪声, hash(资产, 日期, 分钟) 确定性生成; PSD/ACF 诊断参照, 无信息量", R"(\varepsilon_t \sim \mathcal{N}(0, 1))", OP(Noise, gauss, None, None))
