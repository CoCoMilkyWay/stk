#pragma once

// =============================================================================
// 标签持有期组 (两档共用): Pool LabelLob (盘口中间价) / All LabelBar (分钟 VWAP) 同组同列名 lb_<side>_<name>,
// 因子层 / Stat 的持有期键只认 name, 与档无关. 平仓口径各档文件头自述.
// =============================================================================

#include <cstddef>
#include <cstdint>

// 持有期组 (name, KIND, n): name = 列名 token (lb_<side>_<name>; 也是 Stat 持有期键的来源, 见 factor/Stat/Contract.hpp 【持有期键】)
//   MIN   持仓 n 分钟 (exit = 行 m+n 的 entry)
//   CLOSE 持有到当日收盘 (n 不用, 写 0)
//   OPEN  T+n 开盘平仓 (跨日回填)
// 按层截取: LABEL_GROUPS_FROM_<LVL> = 本层可落的组 (滤掉比本层行还短的持有期: 5min 层不要 1m; L_day 只剩开盘型), 序同 LABEL_GROUPS
#define LABEL_GROUPS(X, ...) LABEL_GROUPS_FROM_L_min(X, __VA_ARGS__)
#define LABEL_GROUPS_FROM_L_min(X, ...) X(1m, MIN, 1, __VA_ARGS__) LABEL_GROUPS_FROM_L_5min(X, __VA_ARGS__)
#define LABEL_GROUPS_FROM_L_5min(X, ...) X(5m, MIN, 5, __VA_ARGS__) LABEL_GROUPS_FROM_L_15min(X, __VA_ARGS__)
#define LABEL_GROUPS_FROM_L_15min(X, ...) X(15m, MIN, 15, __VA_ARGS__) LABEL_GROUPS_FROM_L_30min(X, __VA_ARGS__)
#define LABEL_GROUPS_FROM_L_30min(X, ...) X(30m, MIN, 30, __VA_ARGS__) LABEL_GROUPS_FROM_L_60min(X, __VA_ARGS__)
#define LABEL_GROUPS_FROM_L_60min(X, ...) X(60m, MIN, 60, __VA_ARGS__) X(close, CLOSE, 0, __VA_ARGS__) LABEL_GROUPS_FROM_L_day(X, __VA_ARGS__)
#define LABEL_GROUPS_FROM_L_day(X, ...) X(t1, OPEN, 1, __VA_ARGS__) X(t3, OPEN, 3, __VA_ARGS__) X(t5, OPEN, 5, __VA_ARGS__)

enum class HoldKind : uint8_t { MIN,
                                CLOSE,
                                OPEN };
#define LABEL_KIND_ONE(name, kind, n, ...) HoldKind::kind,
#define LABEL_N_ONE(name, kind, n, ...) n,
#define LABEL_NAME_ONE(name, kind, n, ...) #name,
inline constexpr HoldKind LABEL_KIND[] = {LABEL_GROUPS(LABEL_KIND_ONE)};
inline constexpr size_t LABEL_N[] = {LABEL_GROUPS(LABEL_N_ONE)};
inline constexpr const char *LABEL_NAME[] = {LABEL_GROUPS(LABEL_NAME_ONE)}; // 列名 token (lb_<side>_<name>; 各层按名定址)
#undef LABEL_KIND_ONE
#undef LABEL_N_ONE
#undef LABEL_NAME_ONE

// 列名片段 (英 / 中), 两档描述按 kind token 拼接
#define LABEL_EN_MIN(n) #n "min"
#define LABEL_EN_CLOSE(n) "to-Close"
#define LABEL_EN_OPEN(n) "T+" #n " Open"
#define LABEL_CN_MIN(n) #n "分钟"
#define LABEL_CN_CLOSE(n) "至收盘"
#define LABEL_CN_OPEN(n) "至T+" #n "开盘"
