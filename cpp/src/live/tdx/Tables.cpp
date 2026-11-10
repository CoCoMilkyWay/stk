#include "live/tdx/Tables.hpp"

#include "misc/date.hpp"

#include <algorithm>
#include <cctype>
#include <set>

namespace live::tdx::tables {

namespace {

bool digit(char c) { return std::isdigit(static_cast<unsigned char>(c)); }
bool digits(std::string_view s) { return !s.empty() && std::all_of(s.begin(), s.end(), digit); }
bool no_nul(std::string_view s) { return s.find('\0') == std::string_view::npos; }

// 吃掉 s 开头的 lit
bool eat(std::string_view &s, std::string_view lit) {
  if (!s.starts_with(lit))
    return false;
  s.remove_prefix(lit.size());
  return true;
}
// 吃掉 s 开头的 ≥ 1 位数字 → 那段
bool eat_digits(std::string_view &s, std::string_view &got) {
  size_t i = 0;
  while (i < s.size() && digit(s[i]))
    ++i;
  if (!i)
    return false;
  got = s.substr(0, i);
  s.remove_prefix(i);
  return true;
}
long count(std::string_view d) { return d.size() > 9 ? 1'000'000'000L : std::stol(std::string(d)); }

// 时间 + | + 6 位代码 + |
bool clock_code(std::string_view l) { return l.size() >= 16 && mem::clock_text(l.substr(0, 8)) && l[8] == '|' && digits(l.substr(9, 6)) && l[15] == '|'; }

std::string field(const Fields &f, int i) { return std::string(f[static_cast<size_t>(i)]); }

void need_row(const Shape &t, bool ok, const std::string &code, const char *what) {
  if (!ok)
    fail(strf("%s %s %s", t.name, code.c_str(), what));
}

} // namespace

namespace match {

bool positions_head(std::string_view l, std::string_view &tag, long &n) {
  n = -1;
  return eat(l, "0||") && eat_digits(l, tag) && eat(l, "||P00");
}

// 6 位代码 + 至少 5 个 |
bool positions_row(std::string_view l) {
  if (l.size() < 7 || !digits(l.substr(0, 6)) || l[6] != '|')
    return false;
  return std::count(l.begin() + 7, l.end(), '|') >= 4;
}

bool fills_head(std::string_view l, std::string_view &tag, long &n) {
  std::string_view d;
  if (!eat(l, "0|") || l.size() < 18 || !digits(l.substr(0, 18)))
    return false;
  tag = l.substr(0, 8);
  l.remove_prefix(18);
  if (!eat(l, "|") || !eat_digits(l, d) || l != "||P|")
    return false;
  n = count(d);
  return true;
}

// 时间 + 6 位代码 + 行尾空列
bool fills_row(std::string_view l) { return clock_code(l) && l.size() >= 18 && l.ends_with("||") && no_nul(l); }

bool pending_head(std::string_view l, std::string_view &tag, long &n) {
  if (!eat(l, "0||") || !eat_digits(l, tag) || l != "||P|")
    return false;
  n = count(tag);
  return true;
}

bool orders_row(std::string_view l) { return clock_code(l) && no_nul(l); }

bool exch_row(std::string_view l) {
  std::string_view d;
  if (!no_nul(l) || !eat_digits(l, d) || !eat(l, "|"))
    return false;
  const size_t c = l.find('|');
  if (c == std::string_view::npos)
    return false;
  l.remove_prefix(c + 1);
  return l.size() >= 7 && digits(l.substr(0, 6)) && l[6] == '|';
}

bool pairs_head(std::string_view l, std::string_view &tag, long &n) {
  if (!eat(l, "0||") || !eat_digits(l, tag) || l != "|||")
    return false;
  n = count(tag);
  return true;
}

// 名称打头 (标的表的行以数字打头)
bool credit_row(std::string_view l) { return !l.empty() && !digit(l[0]) && l[0] != '|' && no_nul(l) && l.find('|') != std::string_view::npos; }

bool margin_row(std::string_view l) { return l.size() >= 7 && digits(l.substr(0, 6)) && l[6] == '|' && no_nul(l); }

bool collat_head(std::string_view l, std::string_view &tag, long &n) {
  n = -1;
  return eat(l, "0||0||P") && eat_digits(l, tag) && l == "|";
}

} // namespace match

// ---------- 持仓 ----------

Position Positions::row(const Shape &t, const Fields &f, size_t view) {
  const bool full = view == 0;
  const auto col = [&](int a, int b) { return f[static_cast<size_t>(full ? a : b)]; };
  Position r;
  r.code = field(f, 0);
  mem::check_code(t, r.code);
  r.name = field(f, 2);
  r.mkt = mem::market_flag(t, r.code, col(14, 13));
  r.balance = mem::num(t, f[3]);
  r.available = mem::num(t, f[4]);
  if (full) {
    BySide tr;
    tr[kBuy] = mem::num(t, f[5]);
    tr[kSell] = mem::num(t, f[6]);
    r.traded = tr;
    r.financed = mem::num_or(t, f[7]);
    r.collateral = mem::num_or(t, f[16]);
  }
  r.cost = mem::num(t, col(8, 7));
  r.price = mem::num(t, col(9, 8));
  r.market_value = mem::num(t, col(10, 9));
  r.pnl = mem::num(t, col(11, 10));
  r.position_pct = mem::num_or(t, col(13, 12));
  need_row(t, !r.name.empty(), r.code, "名称为空");
  // 「市值 ≈ 现价 × 余额」交叉验证数值列没错位; 停牌价为 0 时不核
  const double mv = r.market_value, px = r.price, bal = r.balance;
  if (mv && px && bal && !mem::isclose(px * bal, mv, 0.02, 1.0))
    fail(strf("%s %s 市值 (%.2f) ≠ 现价 × 余额 (%.2f), 数值列疑似错位", t.name, r.code.c_str(), mv, px * bal));
  return r;
}

Keyed<Position> Positions::index(const Shape &t, const std::vector<Position> &rows) {
  Keyed<Position> out;
  for (const Position &r : rows) {
    if (!out.at.emplace(r.code, out.list.size()).second)
      fail(strf("%s代码 %s 有重复", t.name, r.code.c_str()));
    out.list.push_back(r);
  }
  return out;
}

// ---------- 成交 ----------

std::string Fills::tag() { return misc::dense(misc::today_ymd()); }

Fill Fills::row(const Shape &t, const Fields &f, size_t) {
  Fill r;
  r.time = field(f, 0);
  r.code = field(f, 1);
  mem::check_code(t, r.code);
  r.name = field(f, 3);
  r.side = mem::side_text(t, f[5]); // 列 4 是买卖标志
  const std::string status = field(f, 6);
  r.price = mem::num(t, f[7]);
  r.qty = mem::num(t, f[8]);
  r.amount = mem::num(t, f[9]);
  r.fill_id = field(f, 10);
  r.order_id = field(f, 11);
  r.mkt = mem::market_flag(t, r.code, f[13]); // 与列 12 股东号同市场
  need_row(t, mem::clock_text(r.time), r.code, "成交时间不认识");
  need_row(t, status == "成交", r.code, ("成交状态「" + status + "」不认识").c_str());
  need_row(t, r.qty > 0 && r.price > 0 && !r.fill_id.empty(), r.code, "成交行数值异常");
  need_row(t, mem::isclose(r.price * r.qty, r.amount, 1e-4, 0.02), r.code, "金额 ≠ 价 × 量");
  return r;
}

Keyed<Flow> Fills::index(const Shape &t, const std::vector<Fill> &rows) {
  Keyed<Flow> out;
  for (const Fill &r : rows) {
    const auto [it, fresh] = out.at.emplace(r.code, out.list.size());
    if (fresh) {
      Flow f;
      static_cast<Sec &>(f) = r;
      out.list.push_back(f);
    }
    Flow &a = out.list[it->second];
    if (a.mkt != r.mkt)
      fail(strf("%s %s 沪深标志不一致", t.name, r.code.c_str()));
    a.traded[r.side] += r.qty;
    a.amount[r.side] += r.amount;
  }
  return out;
}

// ---------- 挂单 ----------

Pending Orders::row(const Shape &t, const Fields &f, size_t) {
  Pending r;
  r.time = field(f, 0);
  r.code = field(f, 1);
  mem::check_code(t, r.code);
  r.name = field(f, 3);
  r.side = mem::side_text(t, f[5]); // 列 4 是买卖标志
  r.kind = field(f, 6);             // 委托类别 (信用交易 / …)
  r.status = field(f, 7);           // 已报 / 部成 / …
  r.price = mem::num(t, f[8]);
  r.qty = mem::num(t, f[9]); // 委托量 [股]
  r.order_id = field(f, 10);
  r.filled = mem::num(t, f[12]); // 已成交量 [股]
  r.mkt = mem::market_flag(t, r.code, f[16]);
  need_row(t, mem::clock_text(r.time), r.code, "委托时间不认识");
  need_row(t, !r.status.empty() && !r.order_id.empty(), r.code, "挂单状态 / 委托编号为空");
  need_row(t, r.price > 0 && r.qty > 0 && 0 <= r.filled && r.filled < r.qty, r.code, "挂单量价异常");
  return r;
}

Orders::Index Orders::index(const Shape &t, const std::vector<Pending> &rows) {
  std::set<std::string> ids;
  for (const Pending &r : rows)
    if (!ids.insert(r.order_id).second)
      fail(strf("%s委托编号 %s 有重复", t.name, r.order_id.c_str()));
  return {};
}

// ---------- 融资负债 ----------

Debt Debts::row(const Shape &t, const Fields &f, size_t) {
  Debt r;
  r.code = field(f, 2);
  mem::check_code(t, r.code);
  r.mkt = mem::market_flag(t, r.code, f[0]);
  r.name = field(f, 4);
  r.balance = mem::num(t, f[5]);
  r.qty = mem::num(t, f[7]);
  r.amount = mem::num(t, f[9]);
  need_row(t, r.qty >= 0 && r.amount >= 0, r.code, "融资负债数值异常");
  return r;
}

Debts::Index Debts::index(const Shape &t, const std::vector<Debt> &rows) {
  Index out;
  for (const Debt &r : rows)
    if (!out.emplace(r.code, r).second)
      fail(strf("%s代码 %s 有重复", t.name, r.code.c_str()));
  return out;
}

// ---------- 信用资产 ----------

Pairs Credit::row(const Shape &t, const Fields &f, size_t) {
  Pairs r;
  for (int i = 0; i < 3; ++i) {
    r.k[i] = field(f, 3 * i);
    r.v[i] = field(f, 3 * i + 1);
  }
  if (r.k[0].empty())
    fail(strf("%s行首组名称为空", t.name));
  return r;
}

Credit::Index Credit::index(const Shape &t, const std::vector<Pairs> &rows) {
  Index out;
  for (const Pairs &r : rows)
    for (int i = 0; i < 3; ++i)
      if (!r.k[i].empty() && !out.emplace(r.k[i], r.v[i]).second)
        fail(strf("%s名称「%s」有重复", t.name, r.k[i].c_str()));
  return out;
}

// ---------- 标的证券 ----------

MarginRow Margin::row(const Shape &t, const Fields &f, size_t) {
  MarginRow r;
  r.code = field(f, 0);
  mem::check_code(t, r.code);
  r.name = field(f, 2);
  r.mkt = mem::market_flag(t, r.code, f[3]);
  r.ratio = mem::num(t, f[5]);
  r.remark = field(f, 7);
  r.status = field(f, 10);
  need_row(t, r.ratio > 0, r.code, "融资保证金比例不为正");
  return r;
}

Margin::Index Margin::index(const Shape &t, const std::vector<MarginRow> &rows) {
  Index out;
  for (const MarginRow &r : rows)
    if (!out.emplace(MktCode{r.mkt, r.code}, r).second)
      fail(strf("%s (%s, %s) 有重复", t.name, kMktPrefix[static_cast<int>(r.mkt)], r.code.c_str()));
  return out;
}

// ---------- 担保品 ----------

CollatRow Collat::row(const Shape &t, const Fields &f, size_t) {
  CollatRow r;
  r.code = field(f, 2);
  mem::check_code(t, r.code);
  r.mkt = mem::market_flag(t, r.code, f[0]);
  r.name = field(f, 4);
  r.rate = mem::num(t, f[5]);
  r.status = field(f, 6);
  r.remark = field(f, 12);
  need_row(t, 0 <= r.rate && r.rate <= 1, r.code, "担保折算率不在 [0, 1]");
  return r;
}

Collat::Index Collat::index(const Shape &t, const std::vector<CollatRow> &rows) {
  Index out;
  for (const CollatRow &r : rows)
    if (!out.emplace(MktCode{r.mkt, r.code}, r).second)
      fail(strf("%s (%s, %s) 有重复", t.name, kMktPrefix[static_cast<int>(r.mkt)], r.code.c_str()));
  return out;
}

} // namespace live::tdx::tables
