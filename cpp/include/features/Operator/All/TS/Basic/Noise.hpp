#pragma once

// All 档白噪声参照列 (类见 Operator/Common/Basic/Noise.hpp)
#include "features/Operator/Common/Basic/Noise.hpp"

#define NODE_Noise(N) N(Noise, (Noise), (minute_data, date_), onMinute)

#define FIELDS_L1_Noise(X, CAT1) \
  X(noise_gauss, CAT1, AUTO, "White Noise", "白噪声", "合成 i.i.d. 标准正态白噪声, hash(资产, 日期, 分钟) 确定性生成; PSD/ACF 诊断参照, 无信息量", R"(\varepsilon_t \sim \mathcal{N}(0, 1))", OP(Noise, gauss, None, None))
