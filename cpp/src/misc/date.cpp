#include "misc/date.hpp"

#include <cassert>
#include <cstdio>
#include <ctime>

namespace misc {

using namespace std::chrono;

// ============================================================================
// Ymd / Ym
// ============================================================================
Ymd today_ymd() {
  const std::time_t t = std::time(nullptr);
  std::tm tm_buf{};
  localtime_r(&t, &tm_buf);
  return (tm_buf.tm_year + 1900) * 10000 + (tm_buf.tm_mon + 1) * 100 + tm_buf.tm_mday;
}

sys_days to_days(Ymd d) {
  assert(d > 0 && "to_days: 缺失日期");
  return sys_days{year{d / 10000} / static_cast<unsigned>(d / 100 % 100) / static_cast<unsigned>(d % 100)};
}

Ymd from_days(sys_days d) {
  const year_month_day ymd{d};
  return static_cast<int>(ymd.year()) * 10000 + static_cast<int>(static_cast<unsigned>(ymd.month())) * 100 +
         static_cast<int>(static_cast<unsigned>(ymd.day()));
}

Ymd add_days(Ymd d, int n) { return from_days(to_days(d) + days{n}); }

int weekday_of(Ymd d) { return static_cast<int>(weekday{to_days(d)}.iso_encoding()) - 1; }

Ymd last_day(Ym m) {
  const year_month_day_last l{year{m / 100} / month{static_cast<unsigned>(m % 100)} / last};
  return m * 100 + static_cast<int>(static_cast<unsigned>(l.day()));
}

std::string dense(Ymd d) {
  if (d <= 0)
    return {};
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%08d", d);
  return std::string(buf, 8);
}

std::string dash(Ymd d) {
  if (d <= 0)
    return {};
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", d / 10000, d / 100 % 100, d % 100);
  return std::string(buf, 10);
}

std::string slash(Ymd d) {
  assert(d > 0 && "slash: 缺失日期");
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04d/%02d/%02d", d / 10000, d / 100 % 100, d % 100);
  return std::string(buf, 10);
}

std::string dash_ym(Ym m) {
  if (m <= 0)
    return {};
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04d-%02d", m / 100, m % 100);
  return std::string(buf, 7);
}

namespace {

// s 的 [i, i+n) 全是数字 → 数值; 否则 -1
int digits(std::string_view s, std::size_t i, std::size_t n) {
  int v = 0;
  for (std::size_t k = i; k < i + n; ++k) {
    if (s[k] < '0' || s[k] > '9')
      return -1;
    v = v * 10 + (s[k] - '0');
  }
  return v;
}

} // namespace

Ymd parse_ymd(std::string_view s) {
  if (s.size() == 8) {
    const int v = digits(s, 0, 8);
    return v < 0 ? 0 : v;
  }
  if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
    const int y = digits(s, 0, 4), m = digits(s, 5, 2), d = digits(s, 8, 2);
    return (y < 0 || m < 0 || d < 0) ? 0 : y * 10000 + m * 100 + d;
  }
  return 0;
}

Ym parse_ym(std::string_view s) {
  if (s.size() != 7 || s[4] != '-')
    return 0;
  const int y = digits(s, 0, 4), m = digits(s, 5, 2);
  return (y < 0 || m < 1 || m > 12) ? 0 : y * 100 + m;
}

std::vector<Ym> months_between(Ymd lo, Ymd hi) {
  std::vector<Ym> out;
  for (Ym m = ym_of(lo); m <= ym_of(hi); m = next_ym(m))
    out.push_back(m);
  return out;
}

} // namespace misc
