#pragma once

// =============================================================================
// Search 的全部配置 (编译期常量; 语义见 Contract.hpp / Space.hpp 对应章节, 那里只引用不定义)
// =============================================================================

#include "factor/Stat/Contract.hpp" // hold_same

#include <cassert>
#include <cmath>

namespace factor::search {

// ---- 持有期 (Contract【目标】) ----
// 可搜集合 (持有期键, 按时间序列出全部标签组, 注释掉 = 不搜); 层上合法另看 hold_ok
inline constexpr int kSearchHolds[] = {
    // 1,                           // 1m
    // 5,                           // 5m
    // 15,                          // 15m

    30,                       // 30m
    60,                       // 60m
    factor::stat::kHoldClose, // close
    // factor::stat::hold_open(1),  // T+1O
    factor::stat::hold_same(1), // T+1
                                // factor::stat::hold_same(2), // T+2
                                // factor::stat::hold_same(3), // T+3
};

// ---- 目标 (Contract【目标】) ----
inline constexpr int kRollParts = 10; // 滚动窗长 = 评估期交易日数 / kRollParts (10 年 → 1 年窗)

// ---- 预算 ----
inline constexpr int kInit = 64;       // 单槽采样点数 (Space【采样】; 唯一的预算量, 整数槽取整去重后可能更少)
inline constexpr int kQtabRows = 1024; // 阈值槽分位表的抽样行数 (整个作用域上均匀)

// P 个槽的采样点数 = kInit · (1 + log2 P): 维数多点随之增, 按对数增长免得预算爆掉 (1 → 64, 2 → 128, 3 → 165, 5 → 213, 8 → 256)
inline int n_init(int P) {
  assert(P >= 1);
  return static_cast<int>(std::lround(kInit * (1.0 + std::log2(static_cast<double>(P)))));
}

} // namespace factor::search
