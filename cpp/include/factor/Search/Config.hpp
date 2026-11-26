#pragma once

// =============================================================================
// Search 的全部配置 (编译期常量; 目标见 Stat/Roll.hpp, 参数空间见 Space.hpp)
// =============================================================================

#include "define/Label.hpp"

#include <cassert>
#include <cmath>

namespace factor::search {

// ---- 持有期 (Stat/Roll.hpp 目标; 一次搜索针对一个固定持有期) ----
// 可搜集合 (按时间序列出全部持有期, 注释掉 = 不搜); 层上合法另看 hold_ok
inline constexpr HoldSet kSearchHolds = 0
                                        // | hold_bit(kHoldM1)
                                        // | hold_bit(kHoldM5)
                                        // | hold_bit(kHoldM15)
                                        | hold_bit(kHoldM30) | hold_bit(kHoldM60) | hold_bit(kHoldClose)
                                        // | hold_bit(kHoldT1O)
                                        | hold_bit(kHoldT1)
    // | hold_bit(kHoldT2)
    // | hold_bit(kHoldT3)
    ;
inline constexpr bool search_hold(HoldId h) { return (kSearchHolds & hold_bit(h)) != 0; }

// ---- 预算 ----
inline constexpr int kInit = 64;       // 单槽采样点数 (Space【采样】; 唯一的预算量, 整数槽取整去重后可能更少)
inline constexpr int kQtabRows = 1024; // 阈值槽分位表的抽样行数 (整个作用域上均匀)

// P 个槽的采样点数 = kInit · (1 + log2 P): 维数多点随之增, 按对数增长免得预算爆掉 (1 → 64, 2 → 128, 3 → 165, 5 → 213, 8 → 256)
inline int n_init(int P) {
  assert(P >= 1);
  return static_cast<int>(std::lround(kInit * (1.0 + std::log2(static_cast<double>(P)))));
}

} // namespace factor::search
