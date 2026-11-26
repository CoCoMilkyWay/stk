#include "live/Flows.hpp"

#include "disk/Disk.hpp"
#include "live/Pulled.hpp"
#include "live/Trace.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <string_view>

namespace live::flows {

namespace {

constexpr const char *kRate = "rate";
constexpr const char *kHead = "date,code,name,price,qty,kind,amount,balance";
constexpr const char *kDealHead = "date,time,code,name,side,price,qty,amount,fill,order";
constexpr double kCent = 0.005; // 余额链容差 [元]

std::atomic<uint64_t> gen{1};

// 业务表: 业务名 (suffix = 只比结尾, 如各种「…偿还融资负债」) → 所动的账 (见 Move); 表外的 = 不动账
struct Biz {
  std::string_view name;
  bool suffix;
  uint8_t moves;
};
constexpr Biz kBiz[] = {
    {"证券买入", false, kHold | kTrade},
    {"证券卖出", false, kHold | kTrade},
    {"融资买入", false, kHold | kTrade},
    {"担保品划入", false, kHold | kPledge},
    {"新股入账", false, kHold},
    {"申购配号", false, kPass},
    {"市值申购中签", false, kPass},
    {"托管转出", false, kPass},
    {"新股申购确认缴款", false, kPass},
    {"融资借款", false, kDebt},
    {"偿还融资负债", true, kDebt},
    {"偿还融资费用", true, kDebt},
    {"偿还融资利息", true, kInterest},
    {"银行转存", false, kBank},
    {"银行转取", false, kBank},
};

// 日序、余额链 (动资金的行: 上一行剩余 + 本行发生 = 本行剩余)、业务分类
void check(const std::vector<CashFlow> &rows) {
  const CashFlow *prev = nullptr;
  for (size_t i = 0; i < rows.size(); ++i) {
    const CashFlow &r = rows[i];
    if (i > 0 && rows[i - 1].date > r.date)
      fail(strf("资金流水第 %zu 行日期 %d 早于上一行 %d", i, r.date, rows[i - 1].date));
    moves(r);
    if (r.amount == 0)
      continue;
    if (prev && std::fabs(prev->balance + r.amount - r.balance) > kCent)
      fail(strf("资金流水 %d %s: 上一行剩余 %.2f + 发生 %.2f ≠ 剩余 %.2f, 余额链断开", r.date, r.kind.c_str(), prev->balance, r.amount, r.balance));
    prev = &r;
  }
}

std::string show(const CashFlow &r) {
  return strf("%d|%s|%s|%.10g|%.10g|%s|%.10g|%.10g", r.date, r.code.c_str(), r.name.c_str(), r.price, r.qty, r.kind.c_str(), r.amount, r.balance);
}

// 同一条流水: 名称除外 (券商给证券现名, 改名后旧行随之变)
bool same(const CashFlow &a, const CashFlow &b) {
  return a.date == b.date && a.code == b.code && a.price == b.price && a.qty == b.qty && a.kind == b.kind && a.amount == b.amount && a.balance == b.balance;
}

// 账本的一样 (流水 / 历史成交) 并入一次重查: from 日起整段换成重查的 (重查回来的须都不早于 from)
template <class T>
std::vector<T> splice(const char *what, const std::vector<T> &old, misc::Ymd from, std::vector<T> fresh) {
  for (const T &r : fresh)
    if (r.date < from)
      fail(strf("%s重查 %s 起, 回来 %d 的行", what, misc::dash(from).c_str(), r.date));
  std::vector<T> out(old.begin(), std::ranges::lower_bound(old, from, {}, &T::date));
  out.insert(out.end(), std::make_move_iterator(fresh.begin()), std::make_move_iterator(fresh.end()));
  return out;
}

// 流水另核: 账本 from 日起的各行须恰为重查的开头几行; 并后核日序 / 余额链 / 业务分类
std::vector<CashFlow> merge(const std::vector<CashFlow> &old, misc::Ymd from, std::vector<CashFlow> fresh) {
  const auto cut = std::ranges::lower_bound(old, from, {}, &CashFlow::date);
  std::vector<CashFlow> out = splice("资金流水", old, from, std::move(fresh));
  const auto at = out.begin() + (cut - old.begin()); // 重查的首行
  const auto [o, f] = std::mismatch(cut, old.end(), at, out.end(), same);
  if (o != old.end())
    fail(strf("资金流水重查与账本对不上: %s 起账本 %zu 行不是重查 %zu 行的开头, 第 %zu 行起不同: 账本 %s / 重查 %s", misc::dash(from).c_str(),
              static_cast<size_t>(old.end() - cut), static_cast<size_t>(out.end() - at), static_cast<size_t>(o - cut), show(*o).c_str(), f == out.end() ? "(无)" : show(*f).c_str()));
  check(out);
  return out;
}

// 流水累计的持仓: 代码 → 股 (不含 0)
std::map<std::string, double> holdings(const std::vector<CashFlow> &rows) {
  std::map<std::string, double> h;
  for (const CashFlow &r : rows)
    if (moves(r) & kHold)
      h[r.code] += r.qty;
  std::erase_if(h, [](const auto &x) { return std::fabs(x.second) < kShare; });
  return h;
}

// 流水累计的持仓 + 今日成交 对 券商持仓: 对不上的各只 "代码 流水/券商" (对上 → 空)
std::string reconcile(const std::vector<CashFlow> &rows, const Snapshot &s) {
  std::map<std::string, double> want = holdings(rows), got;
  for (const Traded &f : s.fills.by_code.list)
    want[f.code] += static_cast<double>(f.qty[kBuy] - f.qty[kSell]);
  for (const Position &p : s.pos.list)
    got[p.code] += static_cast<double>(p.balance);
  std::set<std::string> codes;
  for (const auto &[c, n] : want)
    codes.insert(c);
  for (const auto &[c, n] : got)
    codes.insert(c);
  std::string out;
  for (const std::string &c : codes)
    if (const double a = want[c], b = got[c]; std::fabs(a - b) >= kShare)
      out += strf("%s%s %.0f/%.0f", out.empty() ? "" : " · ", c.c_str(), a, b);
  return out;
}

void save(const Cache &c) {
  Pulled s{c.pulled, {{kRate, strf("%.6g", c.rate)}}, {}};
  for (const CashFlow &r : c.rows)
    s.rows.push_back({misc::dense(r.date), r.code, r.name, strf("%.4f", r.price), strf("%.2f", r.qty), r.kind, strf("%.2f", r.amount), strf("%.2f", r.balance)});
  Pulled d{c.pulled, {}, {}};
  for (const Deal &x : c.deals)
    d.rows.push_back({misc::dense(x.date), fmt_sec(x.sec), x.code, x.name, side_name(x.side), strf("%.4f", x.price), std::to_string(x.qty), strf("%.2f", x.amount), x.fill_id, x.order_id});
  save_pulled(disk::LiveDealsFile, kDealHead, d);
  save_pulled(disk::LiveFlowsFile, kHead, s);
  gen.fetch_add(1, std::memory_order_release);
}

} // namespace

uint8_t moves(const CashFlow &r) {
  const std::string_view k = r.kind;
  const auto b = std::ranges::find_if(kBiz, [&](const Biz &x) { return x.suffix ? k.ends_with(x.name) : k == x.name; });
  const uint8_t m = b == std::end(kBiz) ? 0 : b->moves;
  if (r.qty != 0 && !(m & (kHold | kPass)))
    fail(strf("资金流水 %d %s %s: 业务「%s」带股数 %.0f, 不认识", r.date, r.code.c_str(), r.name.c_str(), r.kind.c_str(), r.qty));
  return m;
}

void match(const std::vector<CashFlow> &rows, const std::vector<Deal> &deals) {
  std::map<std::pair<misc::Ymd, std::string>, std::pair<double, double>> by; // → (流水, 成交)
  for (const CashFlow &r : rows)
    if (moves(r) & kTrade)
      by[{r.date, r.code}].first += r.qty;
  for (const Deal &d : deals)
    by[{d.date, d.code}].second += static_cast<double>(d.side * d.qty);
  std::string off;
  for (const auto &[k, v] : by)
    if (std::fabs(v.first - v.second) >= kShare)
      off += strf("%s%d %s %.0f/%.0f", off.empty() ? "" : " · ", k.first, k.second.c_str(), v.first, v.second);
  need(off.empty(), "历史成交对不上资金流水的买卖 (日 代码 流水/成交): " + off);
}

Cache load() {
  const Pulled s = load_pulled(disk::LiveFlowsFile, kHead), t = load_pulled(disk::LiveDealsFile, kDealHead);
  assert(s.pulled == t.pulled && "资金流水 flows.csv 与历史成交 deals.csv 同拉同存 (拉于须同, 缺一不可)");
  Cache c{s.pulled, s.pulled ? std::stod(s.meta.at(kRate)) : 0, {}, {}};
  for (const std::vector<std::string> &f : s.rows) {
    c.rows.push_back({misc::parse_ymd(f[0]), f[1], f[2], std::stod(f[3]), std::stod(f[4]), f[5], std::stod(f[6]), std::stod(f[7])});
    assert(c.rows.back().date > 0 && "资金流水账本: 日期");
  }
  for (const std::vector<std::string> &f : t.rows) {
    Deal d;
    d.date = misc::parse_ymd(f[0]), d.sec = parse_sec(f[1]), d.code = f[2], d.name = f[3], d.mkt = market_of(f[2]);
    d.side = f[4] == side_name(kBuy) ? kBuy : kSell, d.price = std::stod(f[5]), d.qty = std::stoll(f[6]), d.amount = std::stod(f[7]), d.fill_id = f[8], d.order_id = f[9];
    assert(d.date > 0 && d.sec >= 0 && (f[4] == side_name(kBuy) || f[4] == side_name(kSell)) && "历史成交账本: 日期 / 时刻 / 方向");
    c.deals.push_back(std::move(d));
  }
  return c;
}

Cache pull(Broker &b) {
  Cache c = load();
  const misc::Ymd from = c.rows.empty() ? 0 : c.rows.back().date;
  Ledger l = b.ledger(from);
  const size_t n = l.flows.size();
  c.rows = merge(c.rows, from, std::move(l.flows));
  c.deals = splice("历史成交", c.deals, from, std::move(l.deals));
  match(c.rows, c.deals);
  {
    trace::Span sp("核对");
    trace::live("刷新持仓, 核对资金流水…");
    b.sync();
  }
  const std::shared_ptr<const Snapshot> s = b.snapshot();
  need(s != nullptr, "持仓 / 成交 / 信用账户没读到");
  const std::string off = reconcile(c.rows, *s);
  need(off.empty(), "资金流水累计的持仓 + 今日成交对不上券商持仓 (代码 流水/券商): " + off);
  c.pulled = misc::today_ymd();
  c.rate = s->acct.rate;
  trace::note("flows", strf("资金流水账本 %zu 条 (%s起重查 %zu 条), 持仓对上", c.rows.size(), from ? misc::dash(from).c_str() : "开户", n),
              {{"rows", c.rows.size()}, {"from", from}, {"fresh", n}});
  save(c);
  return c;
}

uint64_t generation() { return gen.load(std::memory_order_acquire); }

} // namespace live::flows
