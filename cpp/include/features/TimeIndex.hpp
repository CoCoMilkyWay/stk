#pragma once

#include "codec/L2_DataType.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>

// ============================================================================
// 时间索引: Clock ↔ L_sec (秒) ↔ L_1min (分钟) → 分钟箱 (n 分钟, n ∈ BIN_MINUTES); L_1day (日) 一日一行 = 收盘
// ============================================================================
// A股交易时段 (含集合竞价):
//
//   时段       时钟时间         分钟数    秒数      L_sec 范围        L_1min 范围     5 / 15 / 30 / 60 分钟箱范围
//   ──────────────────────────────────────────────────────────────────────────────────────────
//   上午       09:15 - 11:30    135 min   8100 s    [0, 8099]      [0, 134]       [0, 24] / [0, 8] / [0, 4] / [0, 2]
//   午休       11:30 - 13:00    (非交易, Clock 映射到 8100)
//   下午       13:00 - 15:00    120 min   7200 s    [8100, 15299]  [135, 254]     [25, 48] / [9, 16] / [5, 8] / [3, 4]
//   ──────────────────────────────────────────────────────────────────────────────────────────
//   合计                        255 min   15300 s                                 49 / 17 / 9 / 5 箱
//
// n 分钟箱 k 按收盘时刻标 (09:30, 09:30 + n, ..., 11:30, 13:00 + n, ..., 15:00):
//   首箱 k=0 = [09:15, 09:30) 15 分钟 (集合竞价段, 各 n 同一首箱), 其余 [09:30, 11:30) + [13:00, 15:00) 各 n 分钟;
//   午休前后恰好对齐 (120 % n == 0), 箱 k 末分钟 = n·k + 14 (统一式, k=0 亦然).
//
// 命名规则: X_to_Y. 落盘 T 维 = *_ROWS (多一行哨兵).

constexpr size_t TRADE_MINUTES_PER_DAY = 255;
constexpr size_t TRADE_SECONDS_PER_DAY = TRADE_MINUTES_PER_DAY * 60; // 15300
constexpr size_t MORNING_MINUTES = 135;
constexpr size_t MORNING_SECONDS = MORNING_MINUTES * 60; // 8100, 也是下午 L_sec 起点

constexpr size_t OPEN_BIN_MINUTES = 15;                            // 分钟箱首箱 [09:15, 09:30)
inline constexpr std::array<size_t, 4> BIN_MINUTES{5, 15, 30, 60}; // 分钟箱层的箱长 (细 → 粗, 与 FeatureLevel L_5min.. 同序)
inline constexpr size_t BIN_COUNT = BIN_MINUTES.size();
inline constexpr size_t bins_per_day(size_t n) { return 1 + (TRADE_MINUTES_PER_DAY - OPEN_BIN_MINUTES) / n; }
constexpr bool bins_aligned() {
  for (const size_t n : BIN_MINUTES)
    if ((MORNING_MINUTES - OPEN_BIN_MINUTES) % n != 0 || (TRADE_MINUTES_PER_DAY - MORNING_MINUTES) % n != 0)
      return false;
  return true;
}
static_assert(bins_aligned(), "分钟箱须与午休 / 收盘对齐");

constexpr size_t L_SEC_ROWS = TRADE_SECONDS_PER_DAY + 1; // 15301
constexpr size_t L_1MIN_ROWS = TRADE_MINUTES_PER_DAY + 1; // 256
constexpr size_t L_1DAY_ROWS = 1 + 1;                     // 2
inline constexpr size_t bin_rows(size_t n) { return bins_per_day(n) + 1; }

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
// Clock → L_sec: 编译期 1440 项 (hour, minute) → L_sec 分钟起始秒 LUT, 运行时 O(1) 查表 + 秒
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
inline constexpr size_t Clock_to_L_sec(uint8_t hour, uint8_t minute, uint8_t second) {
  const int16_t base = detail::MINUTE_OFFSET_LUT[hour * 60 + minute];
  const size_t clamped = base & ~(base >> 15); // branchless: 负数 (盘前) → 0
  const size_t result = clamped + second;
  return (result < TRADE_SECONDS_PER_DAY) ? result : (TRADE_SECONDS_PER_DAY - 1);
}

// 0→0, 59→0, 60→1, 8099→134, 8100→135, 15299→254
inline constexpr size_t L_sec_to_L_1min(size_t t_sec) {
  return t_sec < MORNING_SECONDS ? t_sec / 60 : MORNING_MINUTES + (t_sec - MORNING_SECONDS) / 60;
}

// 分钟起始秒: 0→0, 134→8040, 135→8100, 254→15240
inline constexpr size_t L_1min_to_L_sec(size_t t_min) {
  return t_min < MORNING_MINUTES ? t_min * 60 : MORNING_SECONDS + (t_min - MORNING_MINUTES) * 60;
}

// 0→09:15:00, 8099→11:29:59, 8100→13:00:00, 15299→14:59:59
inline constexpr ClockTime L_sec_to_Clock(size_t t_sec) {
  const size_t total = t_sec < MORNING_SECONDS ? MORNING_START_MIN * 60 + t_sec : AFTERNOON_START_MIN * 60 + (t_sec - MORNING_SECONDS);
  return {static_cast<uint8_t>(total / 3600), static_cast<uint8_t>((total % 3600) / 60), static_cast<uint8_t>(total % 60)};
}

// 0→09:15, 134→11:29, 135→13:00, 254→14:59
inline constexpr ClockTime L_1min_to_Clock(size_t t_min) {
  const size_t total = t_min < MORNING_MINUTES ? MORNING_START_MIN + t_min : AFTERNOON_START_MIN + (t_min - MORNING_MINUTES);
  return {static_cast<uint8_t>(total / 60), static_cast<uint8_t>(total % 60), 0};
}

// 分钟 → 所在 n 分钟箱 (n = 10 例): 0..14→0, 15..24→1, 134→12, 135→13, 254→24
inline constexpr size_t L_1min_to_bin(size_t n, size_t t_min) {
  return t_min < OPEN_BIN_MINUTES ? 0 : 1 + (t_min - OPEN_BIN_MINUTES) / n;
}

// 箱末分钟 (该箱的结算分钟; n = 10 例): 0→14, 1→24, 12→134, 13→144, 24→254
inline constexpr size_t bin_last_min(size_t n, size_t k) { return k * n + OPEN_BIN_MINUTES - 1; }

// 箱收盘时刻 (箱标签; n = 10 例): 0→09:30, 12→11:30, 13→13:10, 24→15:00
inline constexpr ClockTime bin_to_Clock(size_t n, size_t k) {
  const ClockTime c = L_1min_to_Clock(bin_last_min(n, k));
  const size_t total = c.hour * 60 + c.minute + 1;
  return {static_cast<uint8_t>(total / 60), static_cast<uint8_t>(total % 60), 0};
}

inline void format_time(char *buf, size_t buf_size, const ClockTime &t) {
  std::snprintf(buf, buf_size, "%02d:%02d:%02d", t.hour, t.minute, t.second);
}

inline void format_time_hm(char *buf, size_t buf_size, const ClockTime &t) {
  std::snprintf(buf, buf_size, "%02d:%02d", t.hour, t.minute);
}

static_assert(Clock_to_L_sec(9, 15, 0) == 0);
static_assert(Clock_to_L_sec(11, 29, 59) == 8099);
static_assert(Clock_to_L_sec(13, 0, 0) == 8100);
static_assert(Clock_to_L_sec(14, 56, 59) == 15299 - 180);
static_assert(Clock_to_L_sec(14, 59, 59) == 15299);
static_assert(L_sec_to_L_1min(8099) == 134 && L_sec_to_L_1min(8100) == 135 && L_sec_to_L_1min(15299) == 254);
static_assert(L_1min_to_L_sec(135) == 8100 && L_1min_to_L_sec(254) == 15240);
static_assert(bins_per_day(5) == 49 && bins_per_day(15) == 17 && bins_per_day(30) == 9 && bins_per_day(60) == 5);
constexpr bool bins_consistent() {
  for (const size_t n : BIN_MINUTES) {
    const size_t K = bins_per_day(n);
    if (L_1min_to_bin(n, 0) != 0 || L_1min_to_bin(n, OPEN_BIN_MINUTES - 1) != 0 || L_1min_to_bin(n, OPEN_BIN_MINUTES) != 1 ||
        L_1min_to_bin(n, TRADE_MINUTES_PER_DAY - 1) != K - 1 || bin_last_min(n, K - 1) != TRADE_MINUTES_PER_DAY - 1 ||
        L_1min_to_bin(n, MORNING_MINUTES) != L_1min_to_bin(n, MORNING_MINUTES - 1) + 1)
      return false;
    for (size_t k = 0; k < K; ++k)
      if (L_1min_to_bin(n, bin_last_min(n, k)) != k || (k + 1 < K && L_1min_to_bin(n, bin_last_min(n, k) + 1) != k + 1))
        return false;
  }
  return true;
}
static_assert(bins_consistent(), "L_1min_to_bin / bin_last_min 互逆, 首末箱与午休对齐");
static_assert(bin_to_Clock(10, 0).hour == 9 && bin_to_Clock(10, 0).minute == 30 && bin_to_Clock(10, 13).hour == 13 && bin_to_Clock(10, 13).minute == 10 &&
              bin_to_Clock(10, 24).hour == 15 && bin_to_Clock(10, 24).minute == 0);
static_assert(bin_to_Clock(60, 2).hour == 11 && bin_to_Clock(60, 2).minute == 30 && bin_to_Clock(60, 3).hour == 14 && bin_to_Clock(60, 3).minute == 0);
