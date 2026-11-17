#include "live/tdx/Credit.hpp"

#include "live/tdx/Tables.hpp"
#include "live/tdx/Trace.hpp"

#include <map>
#include <set>

namespace live::tdx::credit {

namespace {

const std::string kNormal = "正常";

// 直发读整表 S (翻页到底) → 索引; 进度记 log / live
template <class S>
mem::IndexOf<S> full(const Log &log, const Live &live) {
  trace::Span span(S::kShape.name);
  live(strf("直发查「%s」…", S::kShape.name));
  const std::vector<typename S::Row> rows = tables::read<S>();
  log("rows", {{"tab", S::kShape.name}, {"n", rows.size()}});
  trace::ev("rows", {{"tab", S::kShape.name}, {"n", rows.size()}});
  return mem::index<S>(rows);
}

Term term_of(const tables::Margin *m, const tables::Collat *c, Mkt mkt) {
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

// 两表 → 代码 → 条款; 跨市场同号的 (沪市债券与深市基金) 只留 market_of 那只
Terms merge(const Keyed<tables::Margin> &mg, const Keyed<tables::Collat> &co) {
  std::map<std::string, std::set<Mkt>> mkts;
  for (const tables::Margin &r : mg.list)
    mkts[r.code].insert(r.mkt);
  for (const tables::Collat &r : co.list)
    mkts[r.code].insert(r.mkt);
  Terms out;
  for (const auto &[code, ms] : mkts) {
    Mkt m = *ms.begin();
    if (ms.size() > 1) {
      m = market_of(code);
      if (!ms.contains(m))
        fail(code + " 跨市场同号, 按代码定不出市场");
    }
    const std::string k = sina_sym(m, code);
    out[code] = term_of(mg.find(k), co.find(k), m);
  }
  return out;
}

double value(const std::map<std::string, std::string> &c, const char *name) {
  const auto it = c.find(name);
  if (it == c.end())
    fail(strf("信用资产里没有「%s」", name));
  char *e = nullptr;
  const double v = std::strtod(it->second.c_str(), &e);
  if (it->second.empty() || *e)
    fail(strf("信用资产「%s」读到「%s」", name, it->second.c_str()));
  return v;
}

} // namespace

Terms pull(const Log &log, const Live &live) { return merge(full<tables::Margins>(log, live), full<tables::Collats>(log, live)); }

Account account(std::shared_ptr<const Keyed<Position>> held) {
  const auto c = mem::index<tables::Assets>(tables::read<tables::Assets>());
  const auto d = mem::index<tables::Debts>(tables::read<tables::Debts>());
  Account a;
  a.assets = value(c, "担保资产");
  a.line = value(c, "最高融资额度") - value(c, "融资负债（含费用）");
  a.debt = value(c, "负债总额");
  a.net = value(c, "净资产");
  a.avail = value(c, "可用保证金");
  a.cash = value(c, "现金资产");
  a.rate = value(c, "融资年利率");
  for (const tables::Debt &r : d.list)
    a.debts[r.code] = {r.qty, r.amount};
  a.held = std::move(held);
  return a;
}

} // namespace live::tdx::credit
