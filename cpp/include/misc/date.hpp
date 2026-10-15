#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

// 通用日期工具：YYYYMMDD 字符串与 std::chrono::sys_days 互转 + 日历日运算。
// 业务无关，可在任意子系统复用。
namespace misc {

// ============================================================================
// 日期的唯一内部表示: Ymd = int32 YYYYMMDD, Ym = int32 YYYYMM; 0 = 缺失.
// 整数序 = 时间序, 可直接比较 / 排序 / 作下标键. 字符串只在边界 (SQL / 路径 / API / UI) 生成:
//   dense "YYYYMMDD" · dash "YYYY-MM-DD" · dash_ym "YYYY-MM"
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
std::string dash_ym(Ym m); // 0 → ""

// "YYYYMMDD" / "YYYY-MM-DD" → Ymd; 其余 → 0
Ymd parse_ymd(std::string_view s);
// "YYYY-MM" → Ym; 其余 → 0
Ym parse_ym(std::string_view s);

// ============================================================================
// 字符串日期 (旧接口, 其余模块迁到 Ymd 后删除)
// ============================================================================

// 当天 YYYYMMDD (本地时间)
std::string today_yyyymmdd();

// "YYYYMMDD" -> sys_days; size != 8 直接 assert
std::chrono::sys_days parse_yyyymmdd(std::string_view s);

// YYYYMMDD int32 形式 (PitPool event 内部 ev.report_date / ev.ex_date / ...).
//   yyyymmdd <= 0 视为缺失 → 调用方应在外面先判空跳过, 不传入此函数.
std::chrono::sys_days parse_yyyymmdd_int(std::int32_t yyyymmdd);

// "YYYYMMDD" string ↔ int32 互转 (event aggregate 时单点入口).
//   空串 / 长度非 8 / 含非数字 → 返回 0 (= "未提供"; 业务侧用 ==0 跳过).
std::int32_t to_yyyymmdd_int(std::string_view s);

// sys_days -> "YYYYMMDD"
std::string fmt_yyyymmdd(std::chrono::sys_days d);

// [start, end] 闭区间所有日历日，按字典序升序
std::vector<std::string> iter_days(std::string_view start, std::string_view end);

// "YYYYMMDD" + n 个日历日 (n 可为负)
std::string add_days(std::string_view yyyymmdd, int n);

// "YYYYMM" -> 该月最后一天 "DD" ("28" / "29" / "30" / "31")
std::string month_last_dd(std::string_view yyyymm);

// [start, end] 覆盖的月份 "YYYYMM" 升序 (start/end: "YYYY-MM-DD" 或 "YYYYMMDD"; start > end → 空)
std::vector<std::string> iter_months(std::string_view start, std::string_view end);

// "YYYYMMDD" -> 星期 (Mon=0 .. Sun=6)
int weekday_of(std::string_view yyyymmdd);

} // namespace misc
