#include "live/Rebal.hpp"

#include "disk/Disk.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace live::rebal {

namespace {

double ref_of(const Quote &q, const std::string &code) {
  const auto r = ref(q);
  if (!r)
    fail(code + " 没有参考价");
  return *r;
}

// 一单最少股数: 够 lot 股、按参考价 r 计够 unit 元, 取整步长
int least(double r, double unit, int lot, int step) { return static_cast<int>(std::ceil(std::max<double>(lot, unit / r) / step)) * step; }

// 一单 n 股: 参考价计额、5 档吃出滑点; 不足 min_n 股 → 原因
std::optional<Order> leg(const Quote &q, int s, int n, int min_n, double px, Credit credit, const Sec &x, std::string &skip) {
  if (n < min_n) {
    skip = strf("不足最小单元 (%d 股)", min_n);
    return std::nullopt;
  }
  const double amt = n * *ref(q);
  const auto [slip, full] = cost(q, s, amt);
  Order o;
  static_cast<Sec &>(o) = x;
  o.s = s;
  o.credit = credit;
  o.qty = n;
  o.px = px;
  o.amount = amt;
  o.slip = slip.value_or(0);
  o.fee = fee(amt, s);
  o.full = full;
  return o;
}

const Quote &quote_at(const Quotes &quotes, const std::string &code) {
  const auto it = quotes.find(code);
  assert(it != quotes.end());
  return it->second;
}

const Term *term_of(const Terms &terms, const std::string &code) {
  const auto it = terms.find(code);
  return it == terms.end() ? nullptr : &it->second;
}

// 各持仓行 (带可融、卖出设置与卖单)
std::vector<Hold> sells_of(const Keyed<Position> &pos, const Quotes &quotes, const Terms &terms, const Cfg &cfg) {
  std::vector<Hold> rows;
  for (const Position &h : pos.list) {
    const Quote &q = quote_at(quotes, h.code);
    const Term *t = term_of(terms, h.code);
    const double px = ref_of(q, h.code);
    const auto sv = cfg.sells.find(h.code);
    const int v = sv == cfg.sells.end() ? 0 : sv->second;
    const auto [lt, step] = lot(h.code);
    Hold row{.code = h.code, .name = h.name, .financeable = t && t->financeable, .value = h.balance * q.price, .sellable = 0, .avail = static_cast<int>(h.available), .px = px, .lot = lt, .step = step, .sell = v};
    row.sellable = row.avail * px;
    if (v > 0) {
      const int n = std::min(v, row.avail);
      if (!(n == row.avail || n % step == 0))
        fail(strf("%s 卖 %d 股不是 %d 股的整数倍", h.code.c_str(), n, step));
      // 全部可卖可含零股
      const int min_n = n == row.avail ? least(px, cfg.unit, 1, 1) : least(px, cfg.unit, lt, step);
      row.order = leg(q, kSell, n, min_n, order_px(q, kSell, cfg.px, n * px).value_or(px), Credit::Plain, h, row.skip);
    }
    rows.push_back(std::move(row));
  }
  return rows;
}

// 买 n 股的委托价: 按 n × 参考价的额定 (与下单同算); 竞价中或空盘取参考价
double buy_px(const Quote &q, PxMode mode, int n) {
  const double r = *ref(q);
  return order_px(q, kBuy, mode, n * r).value_or(r);
}

// a 元能买的最多股数 (整步长): n × buy_px(n) ≤ a, 左边随 n 增, 二分 n / step
int qty_of(const Quote &q, PxMode mode, double a, int step) {
  int lo = 0, hi = static_cast<int>(a / (step * buy_px(q, mode, step))) + 1;
  while (lo < hi) {
    const int k = (lo + hi + 1) / 2;
    if (k * step * buy_px(q, mode, k * step) <= a)
      lo = k;
    else
      hi = k - 1;
  }
  return lo * step;
}

struct Tgt {
  std::string code;
  double held = 0;             // 卖后市值
  std::optional<double> ratio; // 融资保证金比例; 非融资标的为空
  int least = 0;               // 一单最少股数
  double min = 0;              // 最小单元 [元]
};
using Legs = std::unordered_map<std::string, std::pair<double, double>>; // 代码 → (融资额, 现金额)

std::unordered_map<std::string, Tgt> targets_of(const std::vector<std::string> &ok, const Terms &terms, const Quotes &quotes, const Bal &bal, const Cfg &cfg) {
  std::unordered_map<std::string, Tgt> tgt;
  for (const std::string &c : ok) {
    const Term &t = terms.at(c);
    const Quote &q = quote_at(quotes, c);
    const double r = ref_of(q, c + " " + t.name);
    const auto [lt, step] = lot(c);
    const int n = least(r, cfg.unit, lt, step);
    const auto b = bal.find(c);
    tgt[c] = {c, (b == bal.end() ? 0 : b->second) * q.price, t.financeable ? t.ratio : std::nullopt, n, n * buy_px(q, cfg.px, n)};
  }
  return tgt;
}

// 目标市值 V 下各只两腿与现金需求; s 已按保证金比例从低到高排
std::pair<Legs, double> fill(double V, const std::vector<const Tgt *> &s, double X, double L) {
  Legs legs;
  double xb = std::max(0.0, X), lb = L;
  for (const Tgt *x : s) {
    const double m = x->min, g = V - x->held;
    if (g < m) {
      legs[x->code] = {0, 0};
      continue;
    }
    double f = x->ratio ? std::min({g, lb, xb / *x->ratio}) : 0;
    double p = g - f;
    if (0 < p && p < m) {
      f = g - m;
      p = m;
    }
    if (f < m) {
      f = 0;
      p = g;
    }
    legs[x->code] = {f, p};
    if (f) {
      xb -= *x->ratio * f;
      lb -= f;
    }
  }
  double need = 0, cash = 0;
  for (const auto &[c, fp] : legs) {
    need += fp.first + fp.second;
    cash += fp.second;
  }
  return {std::move(legs), cash + fee(need, kBuy)};
}

// s 等权补齐可行的目标市值 V: 在 [0, hi] 里二分到 kTol
double level(const std::vector<const Tgt *> &s, double C, double X, double L, double hi) {
  double lo = 0;
  while (hi - lo > kTol) {
    const double mid = (lo + hi) / 2;
    if (fill(mid, s, X, L).second <= C)
      lo = mid;
    else
      hi = mid;
  }
  return lo;
}

// 按 tries 的先后逐只试并入 → (入选的, 按保证金比例排; V)
std::pair<std::vector<const Tgt *>, double> pick(const std::vector<std::string> &tries, const std::vector<std::string> &ok,
                                                 const std::unordered_map<std::string, Tgt> &tgt, double C, double X, double L) {
  std::vector<const Tgt *> by_ratio;
  for (const std::string &c : ok)
    by_ratio.push_back(&tgt.at(c));
  std::stable_sort(by_ratio.begin(), by_ratio.end(), [](const Tgt *a, const Tgt *b) { return a->ratio.value_or(0) < b->ratio.value_or(0); });
  std::unordered_set<std::string> picked;
  double V = C + L;
  double top = 0;
  for (const auto &[c, x] : tgt)
    top = std::max(top, x.held);
  V += top;
  for (const std::string &c : tries) {
    std::vector<const Tgt *> s;
    for (const Tgt *x : by_ratio)
      if (picked.contains(x->code) || x->code == c)
        s.push_back(x);
    const double v = level(s, C, X, L, V);
    if (std::all_of(s.begin(), s.end(), [&](const Tgt *x) { return x->held || v >= x->min; })) {
      picked.insert(c);
      V = v;
    }
  }
  std::vector<const Tgt *> out;
  for (const Tgt *x : by_ratio)
    if (picked.contains(x->code))
      out.push_back(x);
  return {out, picked.empty() ? 0 : V};
}

// 名单各行 (带融资 / 现金两腿) → (行, 买单), 买单融资在前; 各腿按 qty_of 折股
std::pair<std::vector<Target>, std::vector<Order>> buys_of(const std::vector<std::string> &codes, const Terms &terms, const Quotes &quotes,
                                                           const std::unordered_map<std::string, Tgt> &tgt, const std::vector<const Tgt *> &picked,
                                                           double V, const Legs &legs, const Cfg &cfg) {
  std::vector<Target> rows;
  std::vector<Order> buys;
  int rank = 0;
  for (const std::string &c : codes) {
    const Term *t = term_of(terms, c);
    Target row{.rank = ++rank, .code = c, .name = t ? t->name : "", .why = why(t), .financeable = t && t->financeable};
    if (t) {
      row.ratio = t->ratio;
      row.rate = t->rate;
    }
    if (!row.why.empty()) {
      rows.push_back(std::move(row));
      continue;
    }
    const Tgt &x = tgt.at(c);
    row.held = x.held;
    if (std::none_of(picked.begin(), picked.end(), [&](const Tgt *p) { return p->code == c; })) {
      row.skip = "购买力不够开仓";
      rows.push_back(std::move(row));
      continue;
    }
    const auto [f, p] = legs.at(c);
    row.need = f + p;
    if (!*row.need && V > x.held) {
      row.skip = "缺口不足最小单元";
      rows.push_back(std::move(row));
      continue;
    }
    const Quote &q = quote_at(quotes, c);
    const int step = lot(c).second;
    for (const auto [cr, part] : {std::pair{Credit::Margin, f}, std::pair{Credit::Plain, p}}) {
      if (!part)
        continue;
      const int n = qty_of(q, cfg.px, part, step);
      Leg l{.credit = cr, .want = part};
      l.order = leg(q, kBuy, n, x.least, buy_px(q, cfg.px, n), cr, Sec{c, t->name, t->mkt}, l.skip);
      if (l.order)
        buys.push_back(*l.order);
      row.legs.push_back(std::move(l));
    }
    rows.push_back(std::move(row));
  }
  std::stable_sort(buys.begin(), buys.end(), [](const Order &a, const Order &b) { return a.credit == Credit::Margin && b.credit != Credit::Margin; });
  return {std::move(rows), std::move(buys)};
}

Sheet sheet(double assets, double net, double debt, double cash, std::optional<double> X, double L) {
  return {assets, net, debt, cash, X, L, assets / net, debt ? std::optional<double>(assets / debt) : std::nullopt};
}

Prices prices(const Bal &bal, const std::map<std::string, std::pair<double, double>> &debts, const Quotes &quotes) {
  Prices px;
  for (const auto &[c, n] : bal)
    px[c] = quote_at(quotes, c).price;
  for (const auto &[c, d] : debts)
    px[c] = quote_at(quotes, c).price;
  return px;
}

nlohmann::json opt(const std::optional<double> &x) { return x ? nlohmann::json(*x) : nlohmann::json(); }

nlohmann::json to_json(const Sheet &s) {
  return {{"assets", s.assets}, {"net", s.net}, {"debt", s.debt}, {"cash", s.cash}, {"X", opt(s.X)}, {"L", s.L}, {"lev", s.lev}, {"maint", opt(s.maint)}};
}

} // namespace

double fee(double amt, int s) { return amt * (kCommission + kTransfer + (s == kSell ? kStamp : 0)); }

double surplus(const Bal &bal, const std::map<std::string, std::pair<double, double>> &debts, const Prices &px, const Terms &terms) {
  std::set<std::string> codes;
  for (const auto &[c, n] : bal)
    codes.insert(c);
  for (const auto &[c, d] : debts)
    codes.insert(c);
  double s = 0;
  for (const std::string &code : codes) {
    const auto b = bal.find(code);
    const auto d = debts.find(code);
    const double n = b == bal.end() ? 0 : b->second;
    const auto [dq, amt] = d == debts.end() ? std::pair{0.0, 0.0} : d->second;
    const Term *t = term_of(terms, code);
    if (!t)
      fail("两融条款里没有 " + code);
    const double p = px.at(code), fin = std::min(dq, n);
    const double x = fin * p - amt;
    s += t->rate * (n - fin) * p + (x > 0 ? t->rate * x : x);
    if (amt) {
      if (!t->ratio)
        fail(code + " 有融资负债却不在标的表");
      s -= amt * *t->ratio;
    }
  }
  return s;
}

double avail(const Account &a, const Prices &px0, const Bal &bal, const Quotes &quotes, const Terms &terms) {
  return a.avail + surplus(bal, a.debts, prices(bal, a.debts, quotes), terms) - surplus(a.bal, a.debts, px0, terms);
}

std::string why(const Term *t) {
  if (!t)
    return "不在担保品表";
  if (t->mkt == Mkt::BJ)
    return "北交所";
  if (!t->pledgeable)
    return "担保状态非正常";
  return "";
}

std::vector<Sec> buyable(const std::vector<std::string> &codes, const Terms &terms) {
  std::vector<Sec> out;
  for (const std::string &c : codes)
    if (const Term *t = term_of(terms, c); why(t).empty())
      out.push_back({c, t->name, t->mkt});
  return out;
}

Result plan(const Keyed<Position> &pos, const Account &acct, const Prices &px0, const Quotes &quotes, const Terms &terms,
            const std::vector<std::string> &codes, const Cfg &cfg) {
  std::vector<std::string> ok;
  for (const std::string &c : codes)
    if (why(term_of(terms, c)).empty())
      ok.push_back(c);
  std::vector<std::string> lack;
  const auto want = [&](const std::string &c) {
    if (!quotes.contains(c))
      lack.push_back(c);
  };
  for (const Position &h : pos.list)
    want(h.code);
  for (const std::string &c : ok)
    want(c);
  for (const auto &[c, d] : acct.debts)
    want(c);
  if (!lack.empty())
    return {std::nullopt, strf("等行情 (%zu 只, 如 %s)", lack.size(), lack[0].c_str())};
  const double pct = cfg.pct / 100;

  Plan pl;
  pl.holds = sells_of(pos, quotes, terms, cfg);
  std::vector<Order> sells;
  for (const Hold &h : pl.holds)
    if (h.order)
      sells.push_back(*h.order);
  double proceeds = 0;
  for (const Order &o : sells)
    proceeds += o.amount - o.slip - o.fee;
  Bal bal = balances(pos);
  for (const Order &o : sells)
    bal[o.code] -= o.qty;
  const double C = acct.cash + proceeds;
  const double X = avail(acct, px0, bal, quotes, terms) + proceeds;
  const double L = std::max(0.0, acct.line);

  const auto tgt = targets_of(ok, terms, quotes, bal, cfg);
  std::vector<std::string> tries = ok;
  std::stable_sort(tries.begin(), tries.end(), [&](const std::string &a, const std::string &b) { return tgt.at(a).ratio && !tgt.at(b).ratio; });
  const auto [picked, V] = pick(tries, ok, tgt, C * pct, X * pct, L * pct);
  const Legs legs = fill(V, picked, X * pct, L * pct).first;
  auto [targets, buys] = buys_of(codes, terms, quotes, tgt, picked, V, legs, cfg);

  double F = 0, use = 0, cost = 0, buy_cost = 0;
  for (const Order &o : sells)
    cost += o.slip + o.fee;
  for (const Order &o : buys) {
    if (o.credit == Credit::Margin)
      F += o.amount;
    use += o.amount;
    buy_cost += o.slip + o.fee;
  }
  cost += buy_cost;
  double assets = acct.cash;
  for (const Position &h : pos.list)
    assets += h.balance * quote_at(quotes, h.code).price;
  const double net = assets - acct.debt;
  const double X_now = avail(acct, px0, balances(pos), quotes, terms);

  pl.now = sheet(assets, net, acct.debt, acct.cash, X_now, L);
  pl.exp = sheet(assets + F - cost, net - cost, acct.debt + F, C - (use - F) - buy_cost, std::nullopt, L - F);
  pl.power = C + std::min(L, std::max(0.0, X));
  pl.use = use;
  pl.financed = F;
  pl.cost = cost;
  pl.C = C;
  pl.X = X;
  pl.V = V;
  pl.picked = static_cast<int>(picked.size());
  pl.buyable = static_cast<int>(ok.size());
  pl.targets = std::move(targets);
  pl.orders = std::move(sells);
  pl.orders.insert(pl.orders.end(), buys.begin(), buys.end());
  for (const auto &[c, q] : quotes)
    pl.time = std::max(pl.time, quote_time(q));
  return {std::move(pl), ""};
}

nlohmann::json to_json(const Order &o) {
  return {{"code", o.code}, {"name", o.name}, {"s", o.s}, {"credit", credit_name(o.credit)}, {"qty", o.qty}, {"px", o.px}, {"amount", o.amount}, {"slip", o.slip}, {"fee", o.fee}, {"full", o.full}};
}

nlohmann::json to_json(const Plan &p) {
  nlohmann::json orders = nlohmann::json::array();
  for (const Order &o : p.orders)
    orders.push_back(to_json(o));
  return {{"now", to_json(p.now)}, {"exp", to_json(p.exp)}, {"power", p.power}, {"use", p.use}, {"financed", p.financed}, {"cost", p.cost}, {"after_sell", {{"C", p.C}, {"X", p.X}}}, {"V", p.V}, {"picked", p.picked}, {"buyable", p.buyable}, {"orders", orders}, {"time", p.time}};
}

nlohmann::json to_json(const Cfg &c) {
  return {{"list", c.list}, {"unit", c.unit}, {"pct", c.pct}, {"px", kPxModeNames[static_cast<int>(c.px)]}, {"sells", c.sells}};
}

std::vector<std::string> lists() {
  std::vector<std::pair<std::filesystem::file_time_type, std::string>> v;
  for (const disk::Key &k : disk::list(disk::LiveListFile))
    v.emplace_back(std::filesystem::last_write_time(disk::path(disk::LiveListFile, k)), k.name);
  std::stable_sort(v.begin(), v.end(), [](const auto &a, const auto &b) { return a.first > b.first; });
  std::vector<std::string> out;
  for (auto &[t, n] : v)
    out.push_back(std::move(n));
  return out;
}

std::vector<std::string> read_list(const std::string &name) {
  std::string text = disk::read(disk::LiveListFile, {.name = name});
  if (text.starts_with("\xEF\xBB\xBF"))
    text.erase(0, 3);
  const auto trim = [](std::string_view s) {
    const size_t a = s.find_first_not_of(" \t\r");
    return a == std::string_view::npos ? std::string_view{} : s.substr(a, s.find_last_not_of(" \t\r") - a + 1);
  };
  std::vector<std::string> rows;
  size_t at = 0;
  while (at < text.size()) {
    size_t e = text.find('\n', at);
    if (e == std::string::npos)
      e = text.size();
    const std::string_view line(text.data() + at, e - at);
    at = e + 1;
    if (trim(line).empty())
      continue;
    rows.emplace_back(trim(line.substr(0, line.find(','))));
  }
  if (!rows.empty()) {
    std::string h = rows[0];
    std::transform(h.begin(), h.end(), h.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (h == "code")
      rows.erase(rows.begin());
  }
  std::vector<std::string> out;
  std::unordered_set<std::string> seen;
  for (const std::string &r : rows) {
    if (r.size() != 6 || !std::all_of(r.begin(), r.end(), [](unsigned char c) { return std::isdigit(c); }))
      fail(strf("名单 %s 里有非 6 位代码: %s", name.c_str(), r.c_str()));
    if (seen.insert(r).second)
      out.push_back(r);
  }
  return out;
}

} // namespace live::rebal
