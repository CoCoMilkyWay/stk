#include "misc/date.hpp"

#include <cassert>
#include <cstdio>
#include <ctime>

namespace misc {

using namespace std::chrono;

std::chrono::sys_days parse_yyyymmdd(std::string_view s) {
  assert(s.size() == 8);
  int y = std::stoi(std::string(s.substr(0, 4)));
  int m = std::stoi(std::string(s.substr(4, 2)));
  int d = std::stoi(std::string(s.substr(6, 2)));
  return sys_days{year{y} / m / d};
}

std::chrono::sys_days parse_yyyymmdd_int(std::int32_t yyyymmdd) {
  assert(yyyymmdd > 0);
  int y = yyyymmdd / 10000;
  int m = (yyyymmdd / 100) % 100;
  int d = yyyymmdd % 100;
  return sys_days{year{y} / m / d};
}

std::int32_t to_yyyymmdd_int(std::string_view s) {
  if (s.size() != 8)
    return 0;
  std::int32_t v = 0;
  for (char c : s) {
    if (c < '0' || c > '9')
      return 0;
    v = v * 10 + (c - '0');
  }
  return v;
}

std::string fmt_yyyymmdd(std::chrono::sys_days d) {
  year_month_day ymd{d};
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04d%02d%02d", static_cast<int>(ymd.year()),
                static_cast<unsigned>(ymd.month()),
                static_cast<unsigned>(ymd.day()));
  return buf;
}

std::string today_yyyymmdd() {
  auto now = system_clock::now();
  auto t = system_clock::to_time_t(now);
  std::tm tm_buf{};
  localtime_r(&t, &tm_buf);
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04d%02d%02d", tm_buf.tm_year + 1900,
                tm_buf.tm_mon + 1, tm_buf.tm_mday);
  return buf;
}

std::vector<std::string> iter_days(std::string_view start,
                                   std::string_view end) {
  std::vector<std::string> out;
  sys_days s = parse_yyyymmdd(start);
  sys_days e = parse_yyyymmdd(end);
  for (sys_days d = s; d <= e; d += days{1}) {
    out.push_back(fmt_yyyymmdd(d));
  }
  return out;
}

std::string add_days(std::string_view yyyymmdd, int n) {
  return fmt_yyyymmdd(parse_yyyymmdd(yyyymmdd) + days{n});
}

std::vector<std::string> iter_months(std::string_view start, std::string_view end) {
  // 去 '-' 取前 6 位 "YYYYMM"
  auto yyyymm = [](std::string_view s) {
    std::string d;
    for (char c : s)
      if (c != '-')
        d += c;
    assert(d.size() >= 6);
    return d.substr(0, 6);
  };
  const std::string s = yyyymm(start), e = yyyymm(end);
  std::vector<std::string> out;
  int y = std::stoi(s.substr(0, 4)), m = std::stoi(s.substr(4, 2));
  for (;;) {
    char buf[8];
    std::snprintf(buf, sizeof(buf), "%04d%02d", y, m);
    if (std::string_view(buf) > e)
      break;
    out.emplace_back(buf);
    if (++m > 12) {
      m = 1;
      ++y;
    }
  }
  return out;
}

int weekday_of(std::string_view yyyymmdd) {
  return static_cast<int>(weekday{parse_yyyymmdd(yyyymmdd)}.iso_encoding()) - 1;
}

std::string month_last_dd(std::string_view yyyymm) {
  assert(yyyymm.size() == 6);
  int y = std::stoi(std::string(yyyymm.substr(0, 4)));
  int m = std::stoi(std::string(yyyymm.substr(4, 2)));
  year_month_day_last ymdl{year{y} / month{static_cast<unsigned>(m)} / last};
  char buf[3];
  std::snprintf(buf, sizeof(buf), "%02u",
                static_cast<unsigned>(ymdl.day()));
  return std::string(buf, 2);
}

} // namespace misc
