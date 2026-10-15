#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

// 通用日期工具: Ymd / Ym 与 std::chrono::sys_days 互转 + 日历日运算. 业务无关.
namespace misc {

// ============================================================================
// 日期的唯一内部表示: Ymd = int32 YYYYMMDD, Ym = int32 YYYYMM; 0 = 缺失.
// 整数序 = 时间序, 可直接比较 / 排序 / 作下标键. 字符串只在边界 (SQL / 路径 / API / UI) 生成:
//   dense "YYYYMMDD" · dash "YYYY-MM-DD" · slash "YYYY/MM/DD" · dash_ym "YYYY-MM"
// ============================================================================
using Ymd = std::int32_t;
using Ym = std::int32_t;

Ymd today_ymd(); // 本地时间

std::chrono::sys_days to_days(Ymd d); // d > 0
Ymd from_days(std::chrono::sys_days d);
Ymd add_days(Ymd d, int n);
int weekday_of(Ymd d); // Mon=0 .. Sun=6

constexpr Ym ym_of(Ymd d) { return d / 100; }
constexpr Ymd first_day(Ym m) { return m * 100 + 1; }
Ymd last_day(Ym m);
constexpr Ym next_ym(Ym m) { return m % 100 == 12 ? (m / 100 + 1) * 100 + 1 : m + 1; }

std::string dense(Ymd d);  // 0 → ""
std::string dash(Ymd d);   // 0 → ""
std::string slash(Ymd d);  // "YYYY/MM/DD" (按日分层的目录); 0 → assert
std::string dash_ym(Ym m); // 0 → ""

// "YYYYMMDD" / "YYYY-MM-DD" → Ymd; 其余 → 0
Ymd parse_ymd(std::string_view s);
// "YYYY-MM" → Ym; 其余 → 0
Ym parse_ym(std::string_view s);

// [ym_of(lo), ym_of(hi)] 覆盖的月份, 升序 (lo > hi → 空)
std::vector<Ym> months_between(Ymd lo, Ymd hi);

} // namespace misc
