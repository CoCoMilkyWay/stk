#include "live/Broker.hpp"

#include "misc/cross_platform.hpp"

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <ctime>

namespace live {

void fail(std::string why) { throw Fail{std::move(why)}; }

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

std::string clock_now() {
  const std::time_t t = std::time(nullptr);
  const std::tm tm = safe_localtime(&t);
  char buf[16];
  std::strftime(buf, sizeof buf, "%H:%M:%S", &tm);
  return buf;
}

std::vector<std::string> cells(const std::string &line) {
  std::vector<std::string> f(1);
  for (const char ch : line) {
    if (ch == ',')
      f.emplace_back();
    else
      f.back().push_back(ch);
  }
  return f;
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

std::map<std::string, double> balances(const Keyed<Position> &pos) {
  std::map<std::string, double> out;
  for (const Position &h : pos.list)
    out[h.code] = h.balance;
  return out;
}

} // namespace live
