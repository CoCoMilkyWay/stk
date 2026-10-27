#pragma once

// =============================================================================
// Stat 的配置 (编译期常量; 语义见 Contract.hpp 对应章节, 那里只引用不定义)
// =============================================================================

namespace factor::stat {

// ---- 分层 (Contract【一级 Row】grp) ----
// 分层两侧: false = 20 组全按做多侧; true = 上半 (G11~G20) 做多侧, 下半 (G1~G10) 做空侧 (存 −做空超额, 与 S 腿同幅度).
// 落盘 stat 记它 (run.grp_split), 不符即作废
inline constexpr bool kGrpSplit = false;

} // namespace factor::stat
