#pragma once

// Lob 档白噪声参照列 (类见 Node/Common/Basic/Noise.hpp). 列名 lob_noise: 与 Bar 档 noise_gauss 区分.
#include "features/Node/Common/Basic/Noise.hpp"

#define NODE_Noise(N) N(Noise, (Noise), (minute_data, date_), onMinute)

#define FIELDS_L1_Noise(X, CAT1) \
  X(lob_noise, CAT1, AUTO, "LOB White Noise", "LOB白噪声", "合成 i.i.d. 标准正态白噪声, hash(资产, 日期, 分钟) 确定性生成; PSD/ACF 诊断参照, 无信息量", R"(\varepsilon_t \sim \mathcal{N}(0, 1))", OP(Noise, gauss, None, None))
