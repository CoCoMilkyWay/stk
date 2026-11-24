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

namespace live::flows {

namespace {

constexpr const char *kRate = "rate";
constexpr const char *kHead = "date,code,name,price,qty,kind,amount,balance";
constexpr double kCent = 0.005; // 余额链容差 [元]

std::atomic<uint64_t> gen{1};

const std::set<std::string> kHolds = {"证券买入", "证券卖出", "融资买入", "担保品划入", "新股入账"};
const std::set<std::string> kPass = {"申购配号", "市值申购中签", "托管转出", "新股申购确认缴款"};

// 日序、余额链 (动资金的行: 上一行剩余 + 本行发生 = 本行剩余)、业务分类
void check(const std::vector<CashFlow> &rows) {
  const CashFlow *prev = nullptr;
  for (size_t i = 0; i < rows.size(); ++i) {
    const CashFlow &r = rows[i];
    if (i > 0 && rows[i - 1].date > r.date)
      fail(strf("资金流水第 %zu 行日期 %d 早于上一行 %d", i, r.date, rows[i - 1].date));
    holds(r);
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

// 账本并入一次重查 (重查覆盖 from 日起)
std::vector<CashFlow> merge(const std::vector<CashFlow> &old, misc::Ymd from, std::vector<CashFlow> fresh) {
  for (const CashFlow &r : fresh)
    if (r.date < from)
      fail(strf("资金流水重查 %s 起, 回来 %d 的行", misc::dash(from).c_str(), r.date));
  const auto cut = std::ranges::lower_bound(old, from, {}, &CashFlow::date);
  const auto [o, f] = std::mismatch(cut, old.end(), fresh.begin(), fresh.end(), same);
  if (o != old.end())
    fail(strf("资金流水重查与账本对不上: %s 起账本 %zu 行不是重查 %zu 行的开头, 第 %zu 行起不同: 账本 %s / 重查 %s", misc::dash(from).c_str(),
              static_cast<size_t>(old.end() - cut), fresh.size(), static_cast<size_t>(o - cut), show(*o).c_str(), f == fresh.end() ? "(无)" : show(*f).c_str()));
  std::vector<CashFlow> out(old.begin(), cut);
  out.insert(out.end(), std::make_move_iterator(fresh.begin()), std::make_move_iterator(fresh.end()));
  check(out);
  return out;
}

// 流水累计的持仓: 代码 → 股 (不含 0)
std::map<std::string, double> holdings(const std::vector<CashFlow> &rows) {
  std::map<std::string, double> h;
  for (const CashFlow &r : rows)
    if (holds(r))
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
  save_pulled(disk::LiveFlowsFile, kHead, s);
  gen.fetch_add(1, std::memory_order_release);
}

} // namespace

bool holds(const CashFlow &r) {
  if (kHolds.contains(r.kind))
    return true;
  if (r.qty != 0 && !kPass.contains(r.kind))
    fail(strf("资金流水 %d %s %s: 业务「%s」带股数 %.0f, 不认识", r.date, r.code.c_str(), r.name.c_str(), r.kind.c_str(), r.qty));
  return false;
}

Cache load() {
  const Pulled s = load_pulled(disk::LiveFlowsFile, kHead);
  Cache c{s.pulled, s.pulled ? std::stod(s.meta.at(kRate)) : 0, {}};
  for (const std::vector<std::string> &f : s.rows) {
    c.rows.push_back({misc::parse_ymd(f[0]), f[1], f[2], std::stod(f[3]), std::stod(f[4]), f[5], std::stod(f[6]), std::stod(f[7])});
    assert(c.rows.back().date > 0 && "资金流水账本: 日期");
  }
  return c;
}

Cache pull(Broker &b) {
  Cache c = load();
  const misc::Ymd from = c.rows.empty() ? 0 : c.rows.back().date;
  std::vector<CashFlow> fresh = b.flows(from);
  const size_t n = fresh.size();
  c.rows = merge(c.rows, from, std::move(fresh));
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
