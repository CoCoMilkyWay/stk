#pragma once

// =============================================================================
// Search 的全部配置 (编译期常量; 语义见 Contract.hpp / Space.hpp 对应章节, 那里只引用不定义)
// =============================================================================

#include "factor/Stat/Contract.hpp" // hold_close

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
    factor::stat::hold_close(1), // T+1C
                                 // factor::stat::hold_close(2), // T+2C
                                 // factor::stat::hold_close(3), // T+3C
};

// ---- 目标 (Contract【目标】) ----
inline constexpr int kRollParts = 10; // 滚动窗长 = 评估期交易日数 / kRollParts (10 年 → 1 年窗)

// ---- 预算 ----
inline constexpr int kInit = 64;       // 采样点数 (Space【采样】; 唯一的预算量, 整数槽取整去重后可能更少)
inline constexpr int kQtabRows = 1024; // 阈值槽分位表的抽样行数 (整个作用域上均匀)

} // namespace factor::search
