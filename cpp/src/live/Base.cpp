#include "live/Base.hpp"

#include "misc/cross_platform.hpp"

#include <cassert>
#include <cctype>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <utility>

namespace live {

void fail(std::string why) {
  trace::ev("fail", {{"why", why}});
  throw Fail{std::move(why)};
}

std::string strf(const char *fmt, ...) {
  va_list a, b;
  va_start(a, fmt);
  va_copy(b, a);
  const int n = std::vsnprintf(nullptr, 0, fmt, a);
  va_end(a);
  assert(n >= 0);
  std::string s(static_cast<size_t>(n), '\0');
  std::vsnprintf(s.data(), s.size() + 1, fmt, b);
  va_end(b);
  return s;
}

int sec_now() {
  const std::time_t t = std::time(nullptr);
  const std::tm tm = safe_localtime(&t);
  return tm.tm_hour * 3600 + tm.tm_min * 60 + tm.tm_sec;
}

std::string fmt_sec(int sec) {
  if (sec < 0)
    return "—";
  return strf("%02d:%02d:%02d", sec / 3600, sec / 60 % 60, sec % 60);
}

int parse_sec(std::string_view s) {
  const auto dig = [](char c) { return std::isdigit(static_cast<unsigned char>(c)) != 0; };
  if (s.size() != 8 || !dig(s[0]) || !dig(s[1]) || s[2] != ':' || !dig(s[3]) || !dig(s[4]) || s[5] != ':' || !dig(s[6]) || !dig(s[7]))
    return -1;
  const int h = (s[0] - '0') * 10 + (s[1] - '0');
  const int m = (s[3] - '0') * 10 + (s[4] - '0');
  const int c = (s[6] - '0') * 10 + (s[7] - '0');
  if (h > 23 || m > 59 || c > 59)
    return -1;
  return h * 3600 + m * 60 + c;
}

Mkt market_of(std::string_view code) {
  assert(code.size() == 6);
  if (code.starts_with("4") || code.starts_with("8") || code.starts_with("92"))
    return Mkt::BJ;
  if (code.starts_with("5") || code.starts_with("6") || code.starts_with("9"))
    return Mkt::SH;
  return Mkt::SZ;
}

Mkt mkt_named(std::string_view s) {
  for (const Mkt m : {Mkt::SZ, Mkt::SH, Mkt::BJ})
    if (s == mkt_name(m))
      return m;
  assert(false && "市场名");
  return Mkt::SZ;
}

} // namespace live
