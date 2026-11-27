#pragma once

// 保序键 (宿主全仓唯一一份; Stat 两后端经 FACTOR_HD 同用): 有限 float → 无符号整数, a < b ⟺ ord(a) < ord(b); ±0 先归一为 +0 (与 float 判等一致)
#include "define/HostDev.hpp"

FACTOR_HD inline unsigned ord(float f) {
  const unsigned u = __builtin_bit_cast(unsigned, f + 0.f);
  return (u & 0x80000000u) ? ~u : (u | 0x80000000u);
}
