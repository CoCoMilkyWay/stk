#pragma once

// =============================================================================
// Search 的全部配置 (编译期常量; 语义见 Contract.hpp / Space.hpp 对应章节, 那里只引用不定义)
// =============================================================================

#include "factor/Stat/Contract.hpp" // hold_close

namespace factor::search {

// ---- 持有期 (Contract【目标】) ----
// 可搜集合 (持有期键, 按时间序列出全部标签组, 注释掉 = 不搜); 层上合法另看 hold_ok. 分钟型 ≥ 30m = 不看成本的前提 (换手 ≈ 100%)
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

// ---- 硬门槛 (Contract【目标】) ----
inline constexpr double kMinMono = 0.95; // 分层单调性 mono (定向后) 下限: 分层不清的因子没有分析价值

// ---- 预算 ----
inline constexpr int kInit = 64;       // 第 0 趟采样点数 (Space【采样】; 唯一的预算量, 整数槽取整去重后可能更少)
inline constexpr int kCorrRows = 1024; // r16 抽样行数 (Contract【多权重】; 整个作用域上均匀; 截面相关 + 阈值槽分位表)

} // namespace factor::search
