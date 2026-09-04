#pragma once

// =============================================================================
// 元素预变换 (CS 行第三参): 截面方法之前对 dense 列原地逐元素变换. 契约见 DataDefine.hpp
// =============================================================================
//   None        x → x
//   Reciprocal  x → 1/x  (x==0 或非 finite → NaN; 估值比率 → 收益率口径: pe→ep, pb→bp, ps→sp, pcf→cp)
// =============================================================================

#include <cstddef>

namespace cs {

struct None {
  static void apply(float *, std::size_t) {}
};

struct Reciprocal {
  static void apply(float *y, std::size_t n); // CSKernels.cpp
};

} // namespace cs
