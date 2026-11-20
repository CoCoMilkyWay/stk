#include "live/tdx/Cols.hpp"

#include <cctype>
#include <cmath>
#include <cstdlib>

namespace live::tdx::cols {

namespace {

bool digit(char c) { return std::isdigit(static_cast<unsigned char>(c)) != 0; }

long integer(const char *tab, const std::string &s) {
  char *e = nullptr;
  const long v = std::strtol(s.c_str(), &e, 10);
  need(!s.empty() && e && !*e, "%s列「%s」不是整数", tab, s.c_str());
  return v;
}

} // namespace

const std::string &cell(const Cells &f, int c) {
  need(c >= 0 && static_cast<size_t>(c) < f.size(), "应答行只 %zu 列, 取不到列 %d", f.size(), c);
  return f[static_cast<size_t>(c)];
}

bool isclose(double a, double b, double rel, double abs) { return std::fabs(a - b) <= std::max(rel * std::max(std::fabs(a), std::fabs(b)), abs); }

void need_row(const char *tab, bool ok, const std::string &key, const char *what) {
  if (!ok)
    fail(strf("%s %s %s", tab, key.c_str(), what));
}

namespace as {

std::string text::get(const char *, const Cells &f, int c) { return cell(f, c); }

std::string name::get(const char *tab, const Cells &f, int c) {
  const std::string &s = cell(f, c);
  need(!s.empty(), "%s名称为空", tab);
  return s;
}

std::string code::get(const char *tab, const Cells &f, int c) {
  const std::string &s = cell(f, c);
  need(s.size() == 6 && std::all_of(s.begin(), s.end(), digit), "%s非法代码「%s」", tab, s.c_str());
  return s;
}

int clock::get(const char *tab, const Cells &f, int c) {
  const std::string &s = cell(f, c);
  const int sec = parse_sec(s);
  need(sec >= 0, "%s时间「%s」不认识", tab, s.c_str());
  return sec;
}

misc::Ymd ymd::get(const char *tab, const Cells &f, int c) {
  const std::string &s = cell(f, c);
  const misc::Ymd d = s.size() == 8 ? misc::parse_ymd(s) : 0;
  need(d != 0, "%s日期「%s」不认识", tab, s.c_str());
  return d;
}

double num::get(const char *tab, const Cells &f, int c) {
  const std::string &s = cell(f, c);
  char *e = nullptr;
  const double v = std::strtod(s.c_str(), &e);
  while (e && *e && std::isspace(static_cast<unsigned char>(*e)))
    ++e;
  need(!s.empty() && e && !*e, "%s列「%s」不是数", tab, s.c_str());
  return v;
}

std::optional<double> num_or::get(const char *tab, const Cells &f, int c) {
  if (cell(f, c).empty())
    return std::nullopt;
  return num::get(tab, f, c);
}

int64_t shares::get(const char *tab, const Cells &f, int c) {
  const double v = num::get(tab, f, c);
  const int64_t n = static_cast<int64_t>(std::llround(v));
  need(std::fabs(v - static_cast<double>(n)) < 1e-4, "%s股数「%s」不是整数", tab, cell(f, c).c_str());
  return n;
}

std::optional<int64_t> shares_or::get(const char *tab, const Cells &f, int c) {
  if (cell(f, c).empty())
    return std::nullopt;
  return shares::get(tab, f, c);
}

Mkt flag::get(const char *tab, const Cells &f, int c) {
  const std::string &s = cell(f, c);
  switch (integer(tab, s)) {
  case 0:
    return Mkt::SZ;
  case 1:
    return Mkt::SH;
  case 60:
    return Mkt::BJ;
  default:
    fail(strf("%s沪深标志「%s」不认识", tab, s.c_str()));
  }
}

int side::get(const char *tab, const Cells &f, int c) {
  const std::string &s = cell(f, c);
  if (s == "买入")
    return kBuy;
  if (s == "卖出")
    return kSell;
  fail(strf("%s买卖方向「%s」不认识", tab, s.c_str()));
}

Sides<int64_t> sides_i::get(const char *tab, const Cells &f, int c) {
  Sides<int64_t> b;
  b[kBuy] = shares::get(tab, f, c);
  b[kSell] = shares::get(tab, f, c + 1);
  return b;
}

std::array<std::pair<std::string, std::string>, 3> groups::get(const char *, const Cells &f, int c) {
  std::array<std::pair<std::string, std::string>, 3> g;
  for (int i = 0; i < 3; ++i)
    g[static_cast<size_t>(i)] = {cell(f, c + 3 * i), cell(f, c + 3 * i + 1)};
  return g;
}

} // namespace as

// ---------- 索引 ----------

Fills Tally::build(const char *tab, std::vector<Fill> rows) {
  std::stable_sort(rows.begin(), rows.end(), [](const Fill &a, const Fill &b) { return std::tie(a.sec, a.fill_id) < std::tie(b.sec, b.fill_id); });
  Keyed<Traded> by;
  for (const Fill &r : rows) {
    const auto [it, fresh] = by.at.emplace(r.code, by.list.size());
    if (fresh) {
      Traded t;
      static_cast<Sec &>(t) = r;
      by.list.push_back(t);
    }
    Traded &t = by.list[it->second];
    need(t.mkt == r.mkt, "%s %s 沪深标志不一致", tab, r.code.c_str());
    t.qty[r.side] += r.qty;
    t.amount[r.side] += r.amount;
  }
  return {std::move(rows), std::move(by)};
}

} // namespace live::tdx::cols
