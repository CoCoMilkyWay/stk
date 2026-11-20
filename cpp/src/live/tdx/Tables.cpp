#include "live/tdx/Tables.hpp"

#include "live/Trace.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <cstdlib>
#include <iterator>
#include <map>
#include <set>

namespace live::tdx::tables {

using cols::need_row;

// ---------- 行级校验 ----------

void Positions::check(const Row &r) {
  const double mv = r.market_value, px = r.price, bal = static_cast<double>(r.balance);
  if (mv && px && bal && !cols::isclose(px * bal, mv, 0.02, 1.0))
    fail(strf("%s %s 市值 (%.2f) ≠ 现价 × 余额 (%.2f), 数值列疑似错位", kName, r.code.c_str(), mv, px * bal));
}

void Fills::check(const Row &r) {
  need_row(kName, r.qty > 0 && r.price > 0 && !r.fill_id.empty(), r.code, "成交行数值异常");
  need_row(kName, cols::isclose(r.price * static_cast<double>(r.qty), r.amount, 1e-4, 0.02), r.code, "金额 ≠ 价 × 量");
}

void Pendings::check(const Row &r) {
  need_row(kName, !r.status.empty() && !r.order_id.empty(), r.code, "挂单状态 / 委托编号为空");
  need_row(kName, r.price > 0 && r.qty > 0 && 0 <= r.filled && r.filled < r.qty, r.code, "挂单量价异常");
}

void Debts::check(const Row &r) { need_row(kName, r.qty >= 0 && r.amount >= 0, r.code, "融资负债数值异常"); }

void Assets::check(const Row &r) { need_row(kName, !r.kv[0].first.empty(), "", "行首组名称为空"); }

void Margins::check(const Row &r) { need_row(kName, r.ratio > 0, r.code, "融资保证金比例不为正"); }

void Collats::check(const Row &r) { need_row(kName, 0 <= r.rate && r.rate <= 1, r.code, "担保折算率不在 [0, 1]"); }

void CashFlows::check(const Row &r) { need_row(kName, r.qty == 0 || !r.code.empty(), misc::dense(r.date) + " " + r.kind, "带股数却没有代码"); }

// ---------- 组合查询 ----------

namespace {

const std::string kNormal = "正常";
constexpr int kFlowSpan = 30, kFlowIdle = 6;
constexpr int kFlowFrom = 0x7e, kFlowTo = 0x7f;

double asset_val(const std::map<std::string, std::string> &c, const char *name) {
  const auto it = c.find(name);
  need(it != c.end(), strf("信用资产里没有「%s」", name));
  char *e = nullptr;
  const double v = std::strtod(it->second.c_str(), &e);
  need(!it->second.empty() && e && !*e, strf("信用资产「%s」读到「%s」", name, it->second.c_str()));
  return v;
}

template <class S>
auto pull_full() {
  trace::Span span(S::kName);
  trace::live(strf("直发查「%s」…", S::kName));
  auto t = read<S>();
  trace::note("rows", strf("「%s」%zu 行", S::kName, t.list.size()), {{"tab", S::kName}, {"n", t.list.size()}});
  return t;
}

Term term_of(const Margin *m, const Collat *c, Mkt mkt) {
  Term t;
  t.name = c ? c->name : m->name;
  t.mkt = mkt;
  if (m)
    t.ratio = m->ratio;
  t.financeable = m && m->remark.find("融资标的") != std::string::npos && m->status == kNormal;
  t.rate = c ? c->rate : 0;
  t.pledgeable = c && c->status == kNormal;
  return t;
}

std::vector<CashFlow> flow_window(misc::Ymd lo, misc::Ymd hi, size_t idx) {
  trace::Span span(strf("第 %zu 窗", idx), {{"lo", lo}, {"hi", hi}});
  std::vector<CashFlow> rows = read<CashFlows>({{kFlowFrom, misc::dense(lo)}, {kFlowTo, misc::dense(hi)}});
  for (const CashFlow &r : rows)
    need(lo <= r.date && r.date <= hi, strf("资金流水 %s ~ %s 的应答里有 %d 的行", misc::dash(lo).c_str(), misc::dash(hi).c_str(), r.date));
  std::string text = strf("资金流水 %s ~ %s: %zu 条", misc::dash(lo).c_str(), misc::dash(hi).c_str(), rows.size());
  trace::live(text);
  trace::note("window", std::move(text), {{"lo", lo}, {"hi", hi}, {"n", rows.size()}});
  return rows;
}

} // namespace

Account read_acct() {
  const auto c = read<Assets>();
  Account a;
  a.assets = asset_val(c, "担保资产");
  a.line = asset_val(c, "最高融资额度") - asset_val(c, "融资负债（含费用）");
  a.debt = asset_val(c, "负债总额");
  a.net = asset_val(c, "净资产");
  a.avail = asset_val(c, "可用保证金");
  a.cash = asset_val(c, "现金资产");
  a.rate = asset_val(c, "融资年利率");
  a.debts = read<Debts>();
  trace::ev("acct", {{"assets", a.assets}, {"line", a.line}, {"debt", a.debt}, {"net", a.net}, {"avail", a.avail}, {"cash", a.cash}, {"debts", a.debts.list.size()}});
  return a;
}

Terms read_terms() {
  const Keyed<Margin> mg = pull_full<Margins>();
  const Keyed<Collat> co = pull_full<Collats>();
  std::map<std::string, std::set<Mkt>> mkts;
  for (const Margin &r : mg.list)
    mkts[r.code].insert(r.mkt);
  for (const Collat &r : co.list)
    mkts[r.code].insert(r.mkt);
  Terms out;
  for (const auto &[code, ms] : mkts) {
    Mkt m = *ms.begin();
    if (ms.size() > 1) {
      m = market_of(code);
      need(ms.contains(m), code + " 跨市场同号, 按代码定不出市场");
    }
    const std::string k = sina_sym(m, code);
    out[code] = term_of(mg.find(k), co.find(k), m);
  }
  trace::ev("terms", {{"n", out.size()}});
  return out;
}

std::vector<CashFlow> read_flows(misc::Ymd from) {
  const misc::Ymd end = misc::add_days(misc::today_ymd(), -1);
  std::vector<std::vector<CashFlow>> wins;
  if (from) {
    for (misc::Ymd lo = from; lo <= end;) {
      const misc::Ymd hi = std::min(misc::add_days(lo, kFlowSpan - 1), end);
      wins.push_back(flow_window(lo, hi, wins.size() + 1));
      lo = misc::add_days(hi, 1);
    }
  } else {
    misc::Ymd hi = end;
    for (int idle = 0; idle < kFlowIdle;) {
      const misc::Ymd lo = misc::add_days(hi, 1 - kFlowSpan);
      wins.insert(wins.begin(), flow_window(lo, hi, wins.size() + 1));
      idle = wins.front().empty() ? idle + 1 : 0;
      hi = misc::add_days(lo, -1);
    }
  }
  std::vector<CashFlow> out;
  for (std::vector<CashFlow> &w : wins)
    out.insert(out.end(), std::make_move_iterator(w.begin()), std::make_move_iterator(w.end()));
  trace::ev("flows", {{"n", out.size()}});
  return out;
}

} // namespace live::tdx::tables
