#include "live/tdx/Credit.hpp"

#include "live/tdx/Tables.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"

#include <map>
#include <set>

namespace live::tdx::credit {

namespace {

constexpr double kFullWait = 300; // 开页 / 查询全部最长等多久 [s] (担保品近万只, 客户端卡一两分钟)
constexpr long kAll = 454;        // 页上「查询全部」
const std::string kNormal = "正常";

// 开 func 页 (认页的列 col)、点查询全部, 等查完 (分页收齐、另拼出整块) 读整表, 行数须恰为页上条数.
// 进度: 开页、点查询全部、查完的条数记 log; 查着时每秒报 live 已等秒数 (界面线程忙, 不读界面)
template <class S>
mem::IndexOf<S> full(const mem::HeapP &heap, const Log &log, const Live &live, const std::string &func, const std::string &col) {
  trace::Span span(func);
  log("page", {{"func", func}});
  live("开「" + func + "」页, 等客户端重查…");
  const ui::Page pg = ui::open_page(func, col, kFullWait);
  log("all", {{"func", func}});
  const Clock::time_point t0 = Clock::now();
  ui::press(pg, kAll, kFullWait, [&] { live(strf("「%s」查询全部, 已等 %.0fs (客户端界面线程忙)", func.c_str(), Secs(Clock::now() - t0).count())); });
  const int n = pg.count.read();
  mem::IndexOf<S> rows = mem::Table<S>().read(heap).index;
  log("rows", {{"func", func}, {"n", n}, {"got", rows.list.size()}});
  trace::ev("rows", {{"n", n}, {"got", rows.list.size()}});
  if (rows.list.size() != static_cast<size_t>(n))
    fail(strf("「%s」查完 (页上 %d 条) 内存整表 %zu 行", func.c_str(), n, rows.list.size()));
  return rows;
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

Terms pull(const mem::HeapP &heap, const Log &log, const Live &live) {
  ui::show_bar();
  const auto mg = full<tables::Margins>(heap, log, live, "标的证券查询", "融资保证金比例");
  const auto co = full<tables::Collats>(heap, log, live, "担保品查询", "担保状态");
  ui::open_tab("持仓");
  return merge(mg, co);
}

Account account(mem::Chain &q, std::shared_ptr<const Keyed<Position>> held) {
  const auto c = q.ask<tables::Assets>([] { return ui::open_page("信用资产", "信用资金额度").count.read(); }).index;
  const auto d = q.ask<tables::Debts>([] { return ui::open_page("融资负债汇总", "当前负债金额").count.read(); }).index;
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
