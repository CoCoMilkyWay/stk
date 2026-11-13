#include "live/tdx/Credit.hpp"

#include "live/tdx/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"

#include <condition_variable>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <thread>

namespace live::tdx::credit {

namespace {

constexpr int kFullWait = 300; // 查询全部最长等多久 [s]
constexpr int kLiveGap = 1;    // 拼表期间读内存、报实时进度的间隔 [s]
const std::string kNormal = "正常";

double secs(Clock::time_point a, Clock::time_point b) { return Secs(b - a).count(); }

// 开 func 页、点查询全部, 等内存整表行数到页上条数. 进度: 开页、点查询全部、(页上条数, 内存行数) 每变一次记 log;
// 拼表期间每 kLiveGap 秒报 live (已等秒数 / 内存行数 / 页上条数 / 条数标签多久没读到).
// 条数标签经客户端界面线程读, 客户端卡着时读不动, 故另起线程读; 内存直接读, 不受卡顿影响
template <class S>
mem::IndexOf<S> full(mem::Table<S> &table, const mem::HeapP &heap, const Log &log, const Live &live, const std::string &func, const std::string &col) {
  trace::Span span(func);
  log("page", {{"func", func}});
  live("开「" + func + "」页, 等客户端重查…");
  const win::Hwnd dlg = ui::open_page(func, col);
  const win::Hwnd lbl = ui::count_label(dlg);
  win::command(dlg, ui::kPageAll);
  log("all", {{"func", func}});
  const Clock::time_point t0 = Clock::now();

  std::mutex mu;
  std::condition_variable cv;
  bool stop = false;
  std::optional<int> n;
  Clock::time_point at = t0;
  std::optional<std::string> err;
  std::thread label([&] {
    trace::Span labelling(func + "条数标签");
    try {
      std::unique_lock lk(mu);
      while (!stop) {
        lk.unlock();
        const std::optional<int> got = ui::page_count(lbl, kFullWait);
        lk.lock();
        n = got;
        at = Clock::now();
        cv.wait_for(lk, Secs(kLiveGap), [&] { return stop; });
      }
    } catch (const Fail &f) {
      std::lock_guard lk(mu);
      err = f.why;
    }
  });
  struct Join {
    std::thread &t;
    std::mutex &mu;
    std::condition_variable &cv;
    bool &stop;
    ~Join() {
      {
        std::lock_guard lk(mu);
        stop = true;
      }
      cv.notify_all();
      t.join();
    }
  } join{label, mu, cv, stop};

  std::optional<int> pn;
  size_t got = 0;
  std::optional<std::pair<std::optional<int>, size_t>> seen;
  const auto page = [&] { return pn ? std::to_string(*pn) : std::string("—"); };
  return std::move(*until(
      [&] { return strf("「%s」拼齐 (页上 %s 条, 内存 %zu 行)", func.c_str(), page().c_str(), got); },
      [&]() -> std::optional<mem::IndexOf<S>> {
        Clock::time_point pat;
        {
          std::lock_guard lk(mu);
          if (err)
            fail(*err);
          pn = n;
          pat = at;
        }
        mem::IndexOf<S> rows = table.read(heap).index;
        got = rows.list.size();
        const Clock::time_point now = Clock::now();
        const int waited = static_cast<int>(secs(t0, now) + 0.5), hung = static_cast<int>(secs(pat, now) + 0.5);
        live(strf("「%s」已等 %ds · 内存 %zu 行 / 页上 %s 条%s", func.c_str(), waited, got, page().c_str(),
                  hung >= 3 ? strf(" · 客户端卡着 %ds (界面线程忙, 内存照读)", hung).c_str() : ""));
        if (!seen || *seen != std::pair{pn, got}) {
          seen = std::pair{pn, got};
          log("rows", {{"func", func}, {"n", pn ? nlohmann::json(*pn) : nlohmann::json()}, {"got", got}});
          trace::ev("rows", {{"n", pn ? trace::Kw(*pn) : trace::Kw()}, {"got", got}});
        }
        if (pn && got == static_cast<size_t>(*pn))
          return rows;
        return std::nullopt;
      },
      kFullWait, kLiveGap));
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

Terms pull(Tables &t, const mem::HeapP &heap, const Log &log, const Live &live) {
  ui::show_bar();
  const auto mg = full(t.margins, heap, log, live, "标的证券查询", "融资保证金比例");
  const auto co = full(t.collats, heap, log, live, "担保品查询", "担保折算率");
  ui::click_tool("持仓");
  return merge(mg, co);
}

Account account(Tables &t, mem::Chain &q, const Keyed<Position> &pos) {
  const auto c = q.ask(t.assets, [] { ui::click_func("信用资产"); }).index;
  const auto d = q.ask(t.debts, [] { ui::click_func("融资负债汇总"); }).index;
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
  a.bal = balances(pos);
  for (const Position &p : pos.list)
    a.px[p.code] = p.price;
  return a;
}

} // namespace live::tdx::credit
