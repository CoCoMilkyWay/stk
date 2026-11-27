#pragma once

// =============================================================================
// 层几何 Geo: 一层 (L_1min / 分钟箱 n / 日) 只由三个数刻画 —— S = 每段 (交易日) 行数, N = 每行分钟数, M0 = 行 0 的末分钟
//   行 t_seg 覆盖的最后一分钟 (L_1min 分钟号 0..254) = minute(t_seg) = M0 + t_seg·N:
//     L_1min  S 255, N 1,   M0 0
//     分钟箱 n (5 / 15 / 30 / 60)  S = 1 + 240/n, N = n, M0 = 14 (首箱 = 09:15–09:30 集合竞价 15 分钟, 其后每箱 n 分钟)
//     日     S 1,   N 255, M0 254 (收盘行)
//   S 两两不同, 故 S 唯一确定层 (geo_of). 算子 / 标签协议 (define/Label.hpp Hold) / 评估 / 引擎同一份; 与 features/TimeIndex 的对账在 gui/engine/Load.
// =============================================================================

#include "define/HostDev.hpp"

#include <cassert>

struct Geo {
  int S = 0, N = 0, M0 = 0;                                                  // 每段行数 / 每行分钟数 / 行 0 的末分钟
  FACTOR_HD constexpr int minute(int t_seg) const { return M0 + t_seg * N; } // 行 t_seg 的末分钟 (L_1min 分钟号)
};
// 全部层 (S, N, M0), 细 → 粗; 加层只改这一行
#define GEOS(X) X(255, 1, 0) X(49, 5, 14) X(17, 15, 14) X(9, 30, 14) X(5, 60, 14) X(1, 255, 254)
#define GEO_ROW(S, N, M0) Geo{S, N, M0},
inline constexpr Geo kGeos[] = {GEOS(GEO_ROW)};
#undef GEO_ROW
inline constexpr int kGeoCount = static_cast<int>(sizeof(kGeos) / sizeof(kGeos[0]));
inline constexpr int kSegMax = 255; // 最大段长 (= L_1min; GPU EXPAND 块容量 256 须盖住)
constexpr bool geos_consistent() {
  for (int i = 0; i < kGeoCount; ++i) {
    const Geo &g = kGeos[i];
    if (g.S < 1 || g.S > kSegMax || g.minute(g.S - 1) != kSegMax - 1) // 末行 = 收盘分钟 254
      return false;
    for (int j = 0; j < i; ++j)
      if (kGeos[j].S == g.S)
        return false;
  }
  return true;
}
static_assert(geos_consistent(), "层几何: S 须两两不同且 ∈ [1, kSegMax], 末行末分钟 = 254");
constexpr bool is_seg(int S) {
  for (const Geo &g : kGeos)
    if (g.S == S)
      return true;
  return false;
}
constexpr int geo_index(int S) {
  for (int i = 0; i < kGeoCount; ++i)
    if (kGeos[i].S == S)
      return i;
  assert(false && "seg 不是任何层的每日行数 (kGeos)");
  return 0;
}
constexpr Geo geo_of(int S) { return kGeos[geo_index(S)]; }
