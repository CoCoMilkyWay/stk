#pragma once

#include "codec/L2_DataType.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>

// ============================================================================
// 时间索引: Clock ↔ L0 (秒) ↔ L1 (分钟)
// ============================================================================
// A股交易时段 (含集合竞价):
//
//   时段       时钟时间         分钟数    秒数      L0 范围        L1 范围
//   ──────────────────────────────────────────────────────────────────────
//   上午       09:15 - 11:30    135 min   8100 s    [0, 8099]      [0, 134]
//   午休       11:30 - 13:00    (非交易, Clock 映射到 8100)
//   下午       13:00 - 15:00    120 min   7200 s    [8100, 15299]  [135, 254]
//   ──────────────────────────────────────────────────────────────────────
//   合计                        255 min   15300 s
//
// 命名规则: X_to_Y. 落盘 T 维 = *_ROWS (多一行哨兵).

constexpr size_t TRADE_MINUTES_PER_DAY = 255;
constexpr size_t TRADE_SECONDS_PER_DAY = TRADE_MINUTES_PER_DAY * 60; // 15300
constexpr size_t MORNING_MINUTES = 135;
constexpr size_t MORNING_SECONDS = MORNING_MINUTES * 60; // 8100, 也是下午 L0 起点

constexpr size_t L0_ROWS = TRADE_SECONDS_PER_DAY + 1; // 15301
constexpr size_t L1_ROWS = TRADE_MINUTES_PER_DAY + 1; // 256

// 时钟分钟数 (hour*60+minute)
constexpr uint16_t MORNING_START_MIN = L2::MORNING_CALL_AUCTION_START_HOUR * 60 + L2::MORNING_CALL_AUCTION_START_MINUTE;                   // 555 (09:15)
constexpr uint16_t MORNING_END_MIN = L2::CONTINUOUS_TRADING_MORNING_END_HOUR * 60 + L2::CONTINUOUS_TRADING_MORNING_END_MINUTE;             // 690 (11:30)
constexpr uint16_t AFTERNOON_START_MIN = L2::CONTINUOUS_TRADING_AFTERNOON_START_HOUR * 60 + L2::CONTINUOUS_TRADING_AFTERNOON_START_MINUTE; // 780 (13:00)
constexpr uint16_t AFTERNOON_END_MIN = L2::CONTINUOUS_TRADING_AFTERNOON_END_HOUR * 60 + L2::CONTINUOUS_TRADING_AFTERNOON_END_MINUTE;       // 897 (14:57, 之后 → 盘后哨兵 15299)
static_assert((MORNING_END_MIN - MORNING_START_MIN) == MORNING_MINUTES);

struct ClockTime {
  uint8_t hour = 0;
  uint8_t minute = 0;
  uint8_t second = 0;
};

// ---------------------------------------------------------------------------
// Clock → L0: 编译期 1440 项 (hour, minute) → L0 分钟起始秒 LUT, 运行时 O(1) 查表 + 秒
//   -1 = 盘前 (钳到 0); 午休 → 8100; 盘后 → 15299
// ---------------------------------------------------------------------------
namespace detail {

constexpr int16_t minute_offset(uint8_t hour, uint8_t minute) {
  const uint16_t m = hour * 60 + minute;
  if (m >= MORNING_START_MIN && m < MORNING_END_MIN)
    return static_cast<int16_t>((m - MORNING_START_MIN) * 60);
  if (m >= AFTERNOON_START_MIN && m < AFTERNOON_END_MIN)
    return static_cast<int16_t>(MORNING_SECONDS + (m - AFTERNOON_START_MIN) * 60);
  if (m >= MORNING_END_MIN && m < AFTERNOON_START_MIN)
    return static_cast<int16_t>(MORNING_SECONDS);
  if (m < MORNING_START_MIN)
    return -1;
  return static_cast<int16_t>(TRADE_SECONDS_PER_DAY - 1);
}

constexpr auto make_minute_offset_lut() {
  std::array<int16_t, 24 * 60> table{};
  for (size_t i = 0; i < 24 * 60; ++i)
    table[i] = minute_offset(static_cast<uint8_t>(i / 60), static_cast<uint8_t>(i % 60));
  return table;
}

inline constexpr auto MINUTE_OFFSET_LUT = make_minute_offset_lut();

} // namespace detail

// 09:15:00→0, 11:29:59→8099, 13:00:00→8100, 14:59:59→15299
inline constexpr size_t Clock_to_L0(uint8_t hour, uint8_t minute, uint8_t second) {
  const int16_t base = detail::MINUTE_OFFSET_LUT[hour * 60 + minute];
  const size_t clamped = base & ~(base >> 15); // branchless: 负数 (盘前) → 0
  const size_t result = clamped + second;
  return (result < TRADE_SECONDS_PER_DAY) ? result : (TRADE_SECONDS_PER_DAY - 1);
}

// 0→0, 59→0, 60→1, 8099→134, 8100→135, 15299→254
inline constexpr size_t L0_to_L1(size_t l0) {
  return l0 < MORNING_SECONDS ? l0 / 60 : MORNING_MINUTES + (l0 - MORNING_SECONDS) / 60;
}

// 分钟起始秒: 0→0, 134→8040, 135→8100, 254→15240
inline constexpr size_t L1_to_L0(size_t l1) {
  return l1 < MORNING_MINUTES ? l1 * 60 : MORNING_SECONDS + (l1 - MORNING_MINUTES) * 60;
}

// 0→09:15:00, 8099→11:29:59, 8100→13:00:00, 15299→14:59:59
inline constexpr ClockTime L0_to_Clock(size_t l0) {
  const size_t total = l0 < MORNING_SECONDS ? MORNING_START_MIN * 60 + l0 : AFTERNOON_START_MIN * 60 + (l0 - MORNING_SECONDS);
  return {static_cast<uint8_t>(total / 3600), static_cast<uint8_t>((total % 3600) / 60), static_cast<uint8_t>(total % 60)};
}

// 0→09:15, 134→11:29, 135→13:00, 254→14:59
inline constexpr ClockTime L1_to_Clock(size_t l1) {
  const size_t total = l1 < MORNING_MINUTES ? MORNING_START_MIN + l1 : AFTERNOON_START_MIN + (l1 - MORNING_MINUTES);
  return {static_cast<uint8_t>(total / 60), static_cast<uint8_t>(total % 60), 0};
}

inline void format_time(char *buf, size_t buf_size, const ClockTime &t) {
  std::snprintf(buf, buf_size, "%02d:%02d:%02d", t.hour, t.minute, t.second);
}

inline void format_time_hm(char *buf, size_t buf_size, const ClockTime &t) {
  std::snprintf(buf, buf_size, "%02d:%02d", t.hour, t.minute);
}

static_assert(Clock_to_L0(9, 15, 0) == 0);
static_assert(Clock_to_L0(11, 29, 59) == 8099);
static_assert(Clock_to_L0(13, 0, 0) == 8100);
static_assert(Clock_to_L0(14, 56, 59) == 15299 - 180);
static_assert(Clock_to_L0(14, 59, 59) == 15299);
static_assert(L0_to_L1(8099) == 134 && L0_to_L1(8100) == 135 && L0_to_L1(15299) == 254);
static_assert(L1_to_L0(135) == 8100 && L1_to_L0(254) == 15240);
