#include "account/Ledger.hpp"

#include "disk/Disk.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <iterator>
#include <optional>
#include <set>
#include <sstream>

namespace account {

namespace {

using live::strf;

constexpr const char *kPulled = "pulled";
constexpr const char *kRate = "rate";
constexpr const char *kHead = "date,code,name,price,qty,kind,amount,balance";
constexpr double kCent = 0.005; // 余额链容差 [元]
constexpr double kShare = 0.5;  // 股数相等的容差 [股]

std::atomic<uint64_t> gen{1};

// 带股数的业务: 计入持仓 / 不计 (新股申购途中的配号、中签与回冲, 股份以「新股入账」为准)
const std::set<std::string> kHolds = {"证券买入", "证券卖出", "融资买入", "担保品划入", "新股入账"};
const std::set<std::string> kPass = {"申购配号", "市值申购中签", "托管转出", "新股申购确认缴款"};
const std::set<std::string> kBank = {"银行转存", "银行转取"};
const std::string kPledge = "担保品划入";

bool holds(const CashFlow &r) {
  if (kHolds.contains(r.kind))
    return true;
  if (r.qty != 0 && !kPass.contains(r.kind))
    live::fail(strf("资金流水 %d %s %s: 业务「%s」带股数 %.0f, 不认识", r.date, r.code.c_str(), r.name.c_str(), r.kind.c_str(), r.qty));
  return false;
}
bool debt(const CashFlow &r) { return r.kind == "融资借款" || r.kind.ends_with("偿还融资负债") || r.kind.ends_with("偿还融资费用"); }
bool interest(const CashFlow &r) { return r.kind.ends_with("偿还融资利息"); }

std::vector<std::string> fields(const std::string &s) {
  std::vector<std::string> f(1);
  for (const char ch : s) {
    if (ch == ',')
      f.emplace_back();
    else
      f.back().push_back(ch);
  }
  return f;
}

// 日序、余额链 (动资金的行: 上一行剩余 + 本行发生 = 本行剩余)、业务分类
void check(const std::vector<CashFlow> &rows) {
  const CashFlow *prev = nullptr;
  for (size_t i = 0; i < rows.size(); ++i) {
    const CashFlow &r = rows[i];
    if (i > 0 && rows[i - 1].date > r.date)
      live::fail(strf("资金流水第 %zu 行日期 %d 早于上一行 %d", i, r.date, rows[i - 1].date));
    holds(r);
    if (r.amount == 0)
      continue;
    if (prev && std::fabs(prev->balance + r.amount - r.balance) > kCent)
      live::fail(strf("资金流水 %d %s: 上一行剩余 %.2f + 发生 %.2f ≠ 剩余 %.2f, 余额链断开", r.date, r.kind.c_str(), prev->balance, r.amount, r.balance));
    prev = &r;
  }
}

std::optional<double> close_on(const Closes &px, const std::string &code, misc::Ymd d) {
  const auto c = px.find(code);
  if (c == px.end())
    return std::nullopt;
  const auto it = c->second.find(d);
  return it == c->second.end() ? std::nullopt : std::optional<double>(it->second);
}

// px 里全无收盘的证券在 days 上的估价: 各日成交按股数加权的均价 + 券商现价为观测点, 相邻两点间按自然日线性插值, 末点之后持平, 首点之前无
Closes marks(const std::vector<CashFlow> &rows, const std::vector<misc::Ymd> &days, const Closes &px, const Spot &spot) {
  std::map<std::string, std::map<misc::Ymd, std::pair<double, double>>> obs; // 代码 → 日 → (Σ 股 × 价, Σ 股)
  for (const CashFlow &r : rows)
    if (holds(r) && r.price > 0 && r.qty != 0 && !px.contains(r.code)) {
      auto &[v, q] = obs[r.code][r.date];
      v += std::fabs(r.qty) * r.price, q += std::fabs(r.qty);
    }
  for (auto &[code, o] : obs)
    if (const auto it = spot.px.find(code); it != spot.px.end() && it->second > 0 && spot.date > o.rbegin()->first)
      o[spot.date] = {it->second, 1};
  const auto avg = [](const auto &o) { return o.second.first / o.second.second; };
  Closes out;
  for (const auto &[code, o] : obs) {
    std::map<misc::Ymd, double> &m = out[code];
    auto at = o.begin(); // 不晚于 d 的末个观测点
    for (const misc::Ymd d : days) {
      if (d < o.begin()->first)
        continue;
      while (std::next(at) != o.end() && std::next(at)->first <= d)
        ++at;
      const auto nx = std::next(at);
      if (nx == o.end()) {
        m[d] = avg(*at);
        continue;
      }
      const double w = static_cast<double>((misc::to_days(d) - misc::to_days(at->first)).count()) /
                       static_cast<double>((misc::to_days(nx->first) - misc::to_days(at->first)).count());
      m[d] = avg(*at) + (avg(*nx) - avg(*at)) * w;
    }
  }
  return out;
}

} // namespace

Ledger load() {
  Ledger l;
  if (!disk::exists(disk::LiveFlowsFile))
    return l;
  std::istringstream in(disk::read(disk::LiveFlowsFile));
  std::string line;
  std::getline(in, line);
  const std::vector<std::string> h = fields(line);
  assert(h.size() == 4 && h[0] == kPulled && h[2] == kRate && "资金流水账本: 首行");
  l.pulled = misc::parse_ymd(h[1]);
  assert(l.pulled > 0 && "资金流水账本: 拉于");
  l.rate = std::stod(h[3]);
  std::getline(in, line);
  assert(line == kHead && "资金流水账本: 表头");
  while (std::getline(in, line)) {
    const std::vector<std::string> f = fields(line);
    assert(f.size() == 8 && "资金流水账本: 列数");
    CashFlow r{misc::parse_ymd(f[0]), f[1], f[2], std::stod(f[3]), std::stod(f[4]), f[5], std::stod(f[6]), std::stod(f[7])};
    assert(r.date > 0 && "资金流水账本: 日期");
    l.rows.push_back(std::move(r));
  }
  return l;
}

void save(const Ledger &l) {
  assert(l.pulled > 0);
  std::string s = strf("%s,%s,%s,%.6g\n%s\n", kPulled, misc::dense(l.pulled).c_str(), kRate, l.rate, kHead);
  for (const CashFlow &r : l.rows) {
    live::need((r.code + r.name + r.kind).find(',') == std::string::npos, strf("资金流水 %d %s %s 含逗号", r.date, r.name.c_str(), r.kind.c_str()));
    s += strf("%s,%s,%s,%.4f,%.2f,%s,%.2f,%.2f\n", misc::dense(r.date).c_str(), r.code.c_str(), r.name.c_str(), r.price, r.qty, r.kind.c_str(), r.amount,
              r.balance);
  }
  disk::write(disk::LiveFlowsFile, {}, s);
  gen.fetch_add(1, std::memory_order_release);
}

uint64_t generation() { return gen.load(std::memory_order_acquire); }

std::vector<CashFlow> merge(const std::vector<CashFlow> &old, misc::Ymd from, std::vector<CashFlow> fresh) {
  for (const CashFlow &r : fresh)
    live::need(r.date >= from, strf("资金流水重查 %s 起, 回来 %d 的行", misc::dash(from).c_str(), r.date));
  const auto cut = std::ranges::lower_bound(old, from, {}, &CashFlow::date);
  const size_t k = static_cast<size_t>(old.end() - cut);
  live::need(k <= fresh.size() && std::equal(cut, old.end(), fresh.begin()),
             strf("资金流水重查与账本对不上: %s 起账本 %zu 行不是重查 %zu 行的开头", misc::dash(from).c_str(), k, fresh.size()));
  std::vector<CashFlow> out(old.begin(), cut);
  out.insert(out.end(), std::make_move_iterator(fresh.begin()), std::make_move_iterator(fresh.end()));
  check(out);
  return out;
}

std::map<std::string, double> holdings(const std::vector<CashFlow> &rows) {
  std::map<std::string, double> h;
  for (const CashFlow &r : rows)
    if (holds(r))
      h[r.code] += r.qty;
  std::erase_if(h, [](const auto &x) { return std::fabs(x.second) < kShare; });
  return h;
}

std::string reconcile(const std::vector<CashFlow> &rows, const live::Keyed<live::Position> &pos, const live::Fills &fills) {
  std::map<std::string, double> want = holdings(rows), got;
  for (const live::Flow &f : fills.by_code.list)
    want[f.code] += f.traded[live::kBuy] - f.traded[live::kSell];
  for (const live::Position &p : pos.list)
    got[p.code] += p.balance;
  std::set<std::string> codes;
  for (const auto &[c, n] : want)
    codes.insert(c);
  for (const auto &[c, n] : got)
    codes.insert(c);
  std::string s;
  for (const std::string &c : codes)
    if (const double a = want[c], b = got[c]; std::fabs(a - b) >= kShare)
      s += strf("%s%s %.0f/%.0f", s.empty() ? "" : " · ", c.c_str(), a, b);
  return s;
}

std::vector<std::string> securities(const std::vector<CashFlow> &rows) {
  std::set<std::string> s;
  for (const CashFlow &r : rows)
    if (holds(r))
      s.insert(r.code);
  return {s.begin(), s.end()};
}

double growth(double nav0, double nav, double flow) {
  const double base = nav0 + std::max(flow, 0.0);
  return base > 0 ? 1 + (nav - nav0 - flow) / base : 1;
}

std::vector<Day> replay(const std::vector<CashFlow> &rows, double rate, const std::vector<misc::Ymd> &days, const Closes &px, const Spot &spot) {
  std::vector<Day> out;
  if (rows.empty())
    return out;
  const Closes mk = marks(rows, days, px, spot);
  const auto price_on = [&](const std::string &code, misc::Ymd d) {
    const std::optional<double> c = close_on(px, code, d);
    return c ? c : close_on(mk, code, d);
  };
  std::map<std::string, double> hold;
  std::unordered_map<std::string, double> last; // 代码 → 上一收盘 (起于入账价)
  double cash = 0, owe = 0, acc = 0, idx = 1, nav0 = 0;
  misc::Ymd prev = 0;
  size_t i = 0;
  for (const misc::Ymd d : days) {
    if (d < rows.front().date)
      continue;
    if (prev)
      acc += owe * rate / 360 * static_cast<double>((misc::to_days(d) - misc::to_days(prev)).count());
    double flow = 0;
    for (; i < rows.size() && rows[i].date <= d; ++i) {
      const CashFlow &r = rows[i];
      if (r.amount != 0)
        cash = r.balance;
      if (holds(r)) {
        hold[r.code] += r.qty;
        last.emplace(r.code, r.price);
      }
      if (debt(r))
        owe += r.amount;
      if (interest(r))
        acc += r.amount;
      if (kBank.contains(r.kind))
        flow += r.amount;
      if (r.kind == kPledge)
        flow += r.qty * price_on(r.code, d).value_or(r.price);
    }
    double mv = 0;
    std::vector<std::pair<std::string, double>> held;
    for (const auto &[code, q] : hold) {
      if (std::fabs(q) < kShare)
        continue;
      if (const std::optional<double> c = price_on(code, d))
        last[code] = *c;
      mv += q * last.at(code);
      held.emplace_back(code, q);
    }
    const double nav = cash + mv - owe - acc;
    if (prev)
      idx *= growth(nav0, nav, flow);
    out.push_back({d, nav, flow, idx, std::move(held)});
    prev = d;
    nav0 = nav;
  }
  return out;
}

double flow_after(const std::vector<CashFlow> &rows, misc::Ymd d) {
  double f = 0;
  for (const CashFlow &r : rows)
    if (r.date > d)
      f += kBank.contains(r.kind) ? r.amount : r.kind == kPledge ? r.qty * r.price
                                                                 : 0;
  return f;
}

} // namespace account
