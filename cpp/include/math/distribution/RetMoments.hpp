#pragma once

// =============================================================================
// RetMoments - 收益序列的分钟内可加幂和 + 极值 (基点量纲)
// =============================================================================
//   add(r) 逐样本; 口序固定 (消费方 write 按 kCount 连续写):
//     rv = Σr²   rv_up = Σ_{r>0} r²   rv_dn = Σ_{r<0} r²   m3 = Σr³   m4 = Σr⁴   rmax = max r   rmin = min r
//   偏度 = m3 / rv^{3/2}, 峰度 = m4 / rv² (因子层做; 日级 = 各行求和后再合, 精确).
//   极值有效性用 n 计数 (不用 ±inf 哨兵: fast-math TU); n = 0 → 极值落 0 = 无变动观测.
//   使用方: Realized (Δ 网格格收益, 另加 bpv / tpv / 大跳跃), RealizedMid (盘口更新中间价变化率).
// =============================================================================

#include <algorithm>
#include <cstddef>
#include <cstdint>

struct RetMoments {
  enum Out : size_t { rv,
                      rv_up,
                      rv_dn,
                      m3,
                      m4,
                      rmax,
                      rmin,
                      kCount };

  float v[kCount] = {};
  uint32_t n = 0;

  inline void add(float r) {
    const float r2 = r * r;
    v[rv] += r2;
    v[rv_up] += r > 0.0f ? r2 : 0.0f;
    v[rv_dn] += r < 0.0f ? r2 : 0.0f;
    v[m3] += r2 * r;
    v[m4] += r2 * r2;
    ext(r);
  }
  // 只更新极值 (零收益格: 幂和加 0 恒等, 极值须计入)
  inline void ext(float r) {
    if (n++ == 0)
      v[rmax] = v[rmin] = r;
    else
      v[rmax] = std::max(v[rmax], r), v[rmin] = std::min(v[rmin], r);
  }
  inline void write(float *y) const {
    for (size_t i = 0; i < kCount; ++i)
      y[i] = v[i];
  }
  inline void clear() {
    for (auto &x : v)
      x = 0.0f;
    n = 0;
  }
};
