#include "live/Rebal.hpp"

#include "disk/Disk.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <set>
#include <span>
#include <unordered_map>
#include <unordered_set>

namespace live::rebal {

namespace {

double ref_of(const Quote &q, const std::string &code) {
  const auto r = ref(q);
  need(r.has_value(), code + " 没有参考价");
  return *r;
}

int64_t least(double r, double unit, int64_t lot_n, int64_t step) {
  return static_cast<int64_t>(std::ceil(std::max<double>(static_cast<double>(lot_n), unit / r) / static_cast<double>(step))) * step;
}

std::optional<Order> leg(const Quote &q, int s, int64_t n, int64_t min_n, double px, Credit credit, const Sec &x, std::string &skip) {
  if (n < min_n) {
    skip = strf("不足最小单元 (%lld 股)", static_cast<long long>(min_n));
    return std::nullopt;
  }
  const double amt = static_cast<double>(n) * *ref(q);
  const auto [slip, full] = cost(q, s, amt);
  Order o;
  static_cast<Sec &>(o) = x;
  o.side = s;
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

std::vector<Hold> sells_of(const Keyed<Position> &pos, const Keyed<Debt> &debts, const Quotes &quotes, const Terms &terms, const Cfg &cfg) {
  std::vector<Hold> rows;
  rows.reserve(pos.list.size());
  for (const Position &h : pos.list) {
    const Quote &q = quote_at(quotes, h.code);
    const Term *t = term_of(terms, h.code);
    const double px = ref_of(q, h.code);
    const auto sv = cfg.sells.find(h.code);
    const int64_t v = sv == cfg.sells.end() ? 0 : sv->second;
    const auto [lt, step] = lot(h.code);
    const Debt *d = debts.find(h.code);
    Hold row{.code = h.code,
             .name = h.name,
             .financeable = t && t->financeable,
             .value = static_cast<double>(h.balance) * q.price,
             .sellable = static_cast<double>(h.available) * px,
             .bal = h.balance,
             .avail = h.available,
             .own = h.balance - std::min<int64_t>(d ? d->qty : 0, h.balance),
             .debt = d ? d->amount : 0,
             .px = px,
             .lot = lt,
             .step = step,
             .sell = v};
    if (row.own > 0)
      row.full = row.avail, row.base = row.bal;
    if (v > 0) {
      const int64_t n = std::min(v, row.avail);
      assert(n == row.avail || n % step == 0 || n % step == row.avail % step);
      const double unit = cfg.lever && n == row.avail ? 0 : cfg.unit;
      const int64_t min_n = n == row.avail ? least(px, unit, 1, 1) : least(px, unit, lt, step);
      row.order = leg(q, kSell, n, min_n, order_px(q, kSell, cfg.px, static_cast<double>(n) * px).value_or(px), Credit::Plain, h, row.skip);
      if (row.order)
        row.repay = std::min(row.order->amount - row.order->slip - row.order->fee, row.debt);
    }
    rows.push_back(std::move(row));
  }
  return rows;
}

Keyed<Debt> repaid(const Keyed<Debt> &debts, const std::vector<Hold> &holds) {
  Keyed<Debt> out = debts;
  for (const Hold &h : holds)
    if (h.repay > 0) {
      Debt &d = out.list[out.at.at(h.code)];
      d.qty = static_cast<int64_t>(std::llround(static_cast<double>(d.qty) * (d.amount - h.repay) / d.amount));
      d.amount -= h.repay;
    }
  return out;
}

double buy_px(const Quote &q, PxMode mode, int64_t n) {
  const double r = *ref(q);
  return order_px(q, kBuy, mode, static_cast<double>(n) * r).value_or(r);
}

int64_t qty_of(const Quote &q, PxMode mode, double a, int64_t step) {
  int64_t lo = 0, hi = static_cast<int64_t>(a / (static_cast<double>(step) * buy_px(q, mode, step))) + 1;
  while (lo < hi) {
    const int64_t k = (lo + hi + 1) / 2;
    if (static_cast<double>(k * step) * buy_px(q, mode, k * step) <= a)
      lo = k;
    else
      hi = k - 1;
  }
  return lo * step;
}

struct Tgt {
  std::string code;
  double held = 0;
  std::optional<double> ratio;
  int64_t least = 0;
  double min = 0;
};

struct Budget {
  double C = 0, X = 0, L = 0;
  bool lever = false;
};

// 目标市值 V 下按序分配两腿 (零哈希、零分配: 复用 out); 返回总现金需求
double fill(double V, std::span<const size_t> s, const std::vector<Tgt> &tgts, const Budget &b, std::vector<std::pair<double, double>> *out = nullptr) {
  double xb = std::max(0.0, b.X), lb = b.L, need_amt = 0, cash = 0;
  for (const size_t idx : s) {
    const Tgt &x = tgts[idx];
    const double m = x.min, g = V - x.held;
    double f = 0, p = 0;
    if (g >= m) {
      f = x.ratio ? std::min({g, lb, xb / *x.ratio}) : 0;
      p = g - f;
      if (0 < p && p < m) {
        f = g - m;
        p = m;
      }
      if (f < m) {
        f = 0;
        p = g;
      }
      if (f) {
        xb -= *x.ratio * f;
        lb -= f;
      }
      need_amt += f + p;
      cash += p;
    }
    if (out)
      (*out)[idx] = {f, p};
  }
  return cash + (b.lever ? 0 : fee(need_amt, kBuy));
}

double level(std::span<const size_t> s, const std::vector<Tgt> &tgts, const Budget &b, double hi) {
  double lo = 0;
  while (hi - lo > kTol) {
    const double mid = (lo + hi) / 2;
    if (fill(mid, s, tgts, b) <= b.C)
      lo = mid;
    else
      hi = mid;
  }
  return lo;
}

// 连续下标版入选: 先融资标的、后其余逐只试并入 → (入选下标按保证金比例升序, V)
std::pair<std::vector<size_t>, double> pick(const std::vector<Tgt> &tgts, const Budget &b) {
  const size_t n = tgts.size();
  std::vector<size_t> by_ratio(n), tries(n);
  for (size_t i = 0; i < n; ++i)
    by_ratio[i] = tries[i] = i;
  std::stable_sort(by_ratio.begin(), by_ratio.end(), [&](size_t a, size_t c) { return tgts[a].ratio.value_or(0) < tgts[c].ratio.value_or(0); });
  std::stable_sort(tries.begin(), tries.end(), [&](size_t a, size_t c) { return tgts[a].ratio.has_value() && !tgts[c].ratio.has_value(); });

  std::vector<uint8_t> in(n, 0);
  double V = b.C + b.L, top = 0;
  for (const Tgt &x : tgts)
    top = std::max(top, x.held);
  V += top;

  std::vector<size_t> cur;
  cur.reserve(n);
  for (const size_t idx : tries) {
    cur.clear();
    for (const size_t j : by_ratio)
      if (in[j] || j == idx)
        cur.push_back(j);
    const double v = level(cur, tgts, b, V);
    if (std::all_of(cur.begin(), cur.end(), [&](size_t j) { return tgts[j].held > 0 || v >= tgts[j].min; })) {
      in[idx] = 1;
      V = v;
    }
  }
  cur.clear();
  for (const size_t j : by_ratio)
    if (in[j])
      cur.push_back(j);
  return {std::move(cur), cur.empty() ? 0 : V};
}

Sheet sheet(double assets, double net, double debt, double cash, std::optional<double> X, double L) {
  return {assets, net, debt, cash, assets - cash, X, L, assets / net, debt ? std::optional<double>(assets / debt) : std::nullopt};
}

Prices prices(const Bal &bal, const Keyed<Debt> &debts, const Quotes &quotes) {
  Prices px;
  for (const auto &[c, n] : bal)
    px[c] = quote_at(quotes, c).price;
  for (const Debt &d : debts.list)
    px[d.code] = quote_at(quotes, d.code).price;
  return px;
}

Bal balances(const Keyed<Position> &pos) {
  Bal out;
  for (const Position &h : pos.list)
    out[h.code] = h.balance;
  return out;
}

nlohmann::json opt(const std::optional<double> &x) { return x ? nlohmann::json(*x) : nlohmann::json(); }

nlohmann::json to_json(const Order &o) {
  return {{"code", o.code}, {"name", o.name}, {"side", side_name(o.side)}, {"credit", credit_name(o.credit)}, {"qty", o.qty}, {"px", o.px}, {"amount", o.amount}, {"slip", o.slip}, {"fee", o.fee}, {"full", o.full}};
}

nlohmann::json to_json(const Sheet &s) {
  return {{"assets", s.assets}, {"net", s.net}, {"debt", s.debt}, {"cash", s.cash}, {"mv", s.mv}, {"X", opt(s.X)}, {"L", s.L}, {"lev", s.lev}, {"maint", opt(s.maint)}};
}

} // namespace

double fee(double amt, int s) { return amt * (kCommission + kTransfer + (s == kSell ? kStamp : 0)); }

int64_t snap(const Hold &h, double unit, double x, bool down, bool lever) {
  const int64_t first = least(h.px, unit, h.lot, h.step), r = h.avail % h.step;
  int64_t best = 0;
  const auto take = [&](int64_t c) {
    if (down ? static_cast<double>(c) <= x && c > best : std::abs(static_cast<double>(c) - x) <= std::abs(static_cast<double>(best) - x))
      best = c;
  };
  if (lever || static_cast<double>(h.avail) * h.px >= unit)
    take(h.avail);
  for (const int64_t o : {int64_t{0}, r}) {
    const int64_t lo = (first - o + h.step - 1) / h.step, hi = (h.avail - o) / h.step;
    const double k = (x - static_cast<double>(o)) / static_cast<double>(h.step);
    if (lo <= hi)
      take(std::clamp<int64_t>(static_cast<int64_t>(down ? std::floor(k) : std::llround(k)), lo, hi) * h.step + o);
  }
  return best;
}

double surplus(const Bal &bal, const Keyed<Debt> &debts, const Prices &px, const Terms &terms) {
  std::set<std::string> codes;
  for (const auto &[c, n] : bal)
    codes.insert(c);
  for (const Debt &d : debts.list)
    codes.insert(d.code);
  double s = 0;
  for (const std::string &code : codes) {
    const auto b = bal.find(code);
    const Debt *d = debts.find(code);
    const double n = static_cast<double>(b == bal.end() ? 0 : b->second);
    const double dq = static_cast<double>(d ? d->qty : 0), amt = d ? d->amount : 0;
    const Term *t = term_of(terms, code);
    need(t != nullptr, "两融条款里没有 " + code);
    const double p = px.at(code), fin = std::min(dq, n);
    const double x = fin * p - amt;
    s += t->rate * (n - fin) * p + (x > 0 ? t->rate * x : x);
    if (amt) {
      need(t->ratio.has_value(), code + " 有融资负债却不在标的表");
      s -= amt * *t->ratio;
    }
  }
  return s;
}

double avail(const Snapshot &s, const Prices &px0, const Bal &bal, const Keyed<Debt> &debts, const Quotes &quotes, const Terms &terms) {
  return s.acct.avail + surplus(bal, debts, prices(bal, debts, quotes), terms) - surplus(balances(s.pos), s.acct.debts, px0, terms);
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

std::optional<Plan> plan(const Snapshot &s, const Prices &px0, const Quotes &quotes, const Terms &terms,
                         const std::vector<std::string> &codes, const Cfg &cfg) {
  const Keyed<Position> &pos = s.pos;
  const Account &acct = s.acct;

  std::vector<std::string> ok;
  for (const std::string &c : codes)
    if (const Term *t = term_of(terms, c); why(t).empty() && (!cfg.lever || t->financeable)) {
      assert(!cfg.lever || t->ratio);
      ok.push_back(c);
    }
  const auto has = [&](const std::string &c) { return quotes.contains(c); };
  if (!std::all_of(ok.begin(), ok.end(), has) || !std::all_of(pos.list.begin(), pos.list.end(), [&](const Position &h) { return has(h.code); }) ||
      !std::all_of(acct.debts.list.begin(), acct.debts.list.end(), [&](const Debt &d) { return has(d.code); }))
    return std::nullopt;
  const double pct = cfg.pct / 100;

  Plan pl;
  pl.holds = sells_of(pos, acct.debts, quotes, terms, cfg);
  std::vector<Order> sells;
  for (const Hold &h : pl.holds)
    if (h.order)
      sells.push_back(*h.order);
  double proceeds = 0, R = 0;
  for (const Order &o : sells)
    proceeds += o.amount - o.slip - o.fee;
  for (const Hold &h : pl.holds)
    R += h.repay;
  Bal bal = balances(pos);
  for (const Order &o : sells)
    bal[o.code] -= o.qty;

  const double C = acct.cash + proceeds - R;
  const double X = avail(s, px0, bal, repaid(acct.debts, pl.holds), quotes, terms) + proceeds - R;
  const double L0 = std::max(0.0, acct.line), L = L0 + R;

  std::vector<Tgt> tgts;
  tgts.reserve(ok.size());
  std::unordered_map<std::string, size_t> tgt_at;
  tgt_at.reserve(ok.size());
  for (const std::string &c : ok) {
    const Term &t = terms.at(c);
    const Quote &q = quote_at(quotes, c);
    const double r = ref_of(q, c + " " + t.name);
    const auto [lt, step] = lot(c);
    const int64_t n = least(r, cfg.unit, lt, step);
    const auto b = bal.find(c);
    tgt_at.emplace(c, tgts.size());
    tgts.push_back({c, static_cast<double>(b == bal.end() ? 0 : b->second) * q.price, t.financeable ? t.ratio : std::nullopt, n, static_cast<double>(n) * buy_px(q, cfg.px, n)});
  }

  const Budget bud{cfg.lever ? 0 : C * pct, X * pct, L * pct, cfg.lever};
  const auto [picked, V] = pick(tgts, bud);
  std::vector<std::pair<double, double>> legs(tgts.size(), {0, 0});
  std::vector<uint8_t> is_picked(tgts.size(), 0);
  for (const size_t j : picked)
    is_picked[j] = 1;
  fill(V, picked, tgts, bud, &legs);

  std::vector<Target> targets;
  targets.reserve(codes.size());
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
      targets.push_back(std::move(row));
      continue;
    }
    const auto ti = tgt_at.find(c);
    if (ti == tgt_at.end()) {
      row.skip = "融资加仓只买融资标的";
      targets.push_back(std::move(row));
      continue;
    }
    const size_t idx = ti->second;
    const Tgt &x = tgts[idx];
    row.held = x.held;
    if (!is_picked[idx]) {
      row.skip = "购买力不够开仓";
      targets.push_back(std::move(row));
      continue;
    }
    const auto [f, p] = legs[idx];
    row.need = f + p;
    if (!*row.need && V > x.held) {
      row.skip = "缺口不足最小单元";
      targets.push_back(std::move(row));
      continue;
    }
    const Quote &q = quote_at(quotes, c);
    const int64_t step = lot(c).second;
    for (const auto [cr, part] : {std::pair{Credit::Margin, f}, std::pair{Credit::Plain, p}}) {
      if (!part)
        continue;
      const int64_t n = qty_of(q, cfg.px, part, step);
      Leg l{.credit = cr, .want = part};
      l.order = leg(q, kBuy, n, x.least, buy_px(q, cfg.px, n), cr, Sec{c, t->name, t->mkt}, l.skip);
      if (l.order)
        buys.push_back(*l.order);
      row.legs.push_back(std::move(l));
    }
    targets.push_back(std::move(row));
  }
  std::stable_sort(buys.begin(), buys.end(), [](const Order &a, const Order &b) { return a.credit == Credit::Margin && b.credit != Credit::Margin; });

  double F = 0, use = 0, cost_all = 0, buy_cost = 0;
  for (const Order &o : sells)
    cost_all += o.slip + o.fee;
  for (const Order &o : buys) {
    if (o.credit == Credit::Margin)
      F += o.amount;
    use += o.amount;
    buy_cost += o.slip + o.fee;
  }
  cost_all += buy_cost;

  double assets = acct.cash;
  for (const Position &h : pos.list)
    assets += static_cast<double>(h.balance) * quote_at(quotes, h.code).price;
  const double net = assets - acct.debt;
  const double X_now = avail(s, px0, balances(pos), acct.debts, quotes, terms);

  pl.now = sheet(assets, net, acct.debt, acct.cash, X_now, L0);
  pl.exp = sheet(assets + F - cost_all - R, net - cost_all, acct.debt - R + F, C - (use - F) - buy_cost, std::nullopt, L - F);
  pl.power = (cfg.lever ? 0 : C) + std::min(L, std::max(0.0, X));
  pl.use = use;
  pl.financed = F;
  pl.cost = cost_all;
  pl.C = C;
  pl.X = X;
  pl.repaid = R;
  pl.V = V;
  pl.picked = static_cast<int>(picked.size());
  pl.buyable = static_cast<int>(ok.size());
  pl.targets = std::move(targets);
  pl.orders = std::move(sells);
  pl.orders.insert(pl.orders.end(), buys.begin(), buys.end());
  for (const auto &[c, q] : quotes)
    pl.sec = std::max(pl.sec, q.sec);
  return pl;
}

Sheet now_of(const Account &a) { return sheet(a.assets, a.net, a.debt, a.cash, a.avail, std::max(0.0, a.line)); }

nlohmann::json to_json(const Plan &p) {
  nlohmann::json orders = nlohmann::json::array();
  for (const Order &o : p.orders)
    orders.push_back(to_json(o));
  return {{"now", to_json(p.now)}, {"exp", to_json(p.exp)}, {"power", p.power}, {"use", p.use}, {"financed", p.financed}, {"cost", p.cost}, {"after_sell", {{"C", p.C}, {"X", p.X}, {"repaid", p.repaid}}}, {"V", p.V}, {"picked", p.picked}, {"buyable", p.buyable}, {"orders", orders}, {"time", fmt_sec(p.sec)}};
}

nlohmann::json to_json(const Cfg &c) {
  return {{"list", c.list}, {"unit", c.unit}, {"pct", c.pct}, {"px", kPxModeNames[static_cast<int>(c.px)]}, {"sells", c.sells}, {"lever", c.lever}};
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
    need(r.size() == 6 && std::all_of(r.begin(), r.end(), [](unsigned char c) { return std::isdigit(c) != 0; }), strf("名单 %s 里有非 6 位代码: %s", name.c_str(), r.c_str()));
    if (seen.insert(r).second)
      out.push_back(r);
  }
  return out;
}

} // namespace live::rebal
