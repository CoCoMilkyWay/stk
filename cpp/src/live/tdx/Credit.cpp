#include "live/tdx/Credit.hpp"

#include "live/tdx/Ui.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <set>
#include <thread>

namespace live::tdx::credit {

namespace {

constexpr int kFullWait = 300; // 查询全部最长等多久 [s]
constexpr int kLiveGap = 1;    // 拼表期间读内存、报实时进度的间隔 [s]
const std::string kNormal = "正常";

using Clock = std::chrono::steady_clock;

double secs(Clock::time_point a, Clock::time_point b) { return std::chrono::duration<double>(b - a).count(); }

// 开 func 页、点查询全部, 等内存整表行数到页上条数. 进度: 开页、点查询全部、(页上条数, 内存行数) 每变一次记 log;
// 拼表期间每 kLiveGap 秒报 live (已等秒数 / 内存行数 / 页上条数 / 条数标签多久没读到).
// 条数标签经客户端界面线程读, 客户端卡着时读不动, 故另起线程读; 内存直接读, 不受卡顿影响
template <class S>
typename S::Index full(mem::Table<S> &table, int pid, const Log &log, const Live &live, const std::string &func, const std::string &col) {
  log("page", {{"func", func}});
  live("开「" + func + "」页, 等客户端重查…");
  const std::string dlg = ui::open_page(func, col);
  const std::string lbl = ui::count_label(dlg);
  ui::command(dlg, ui::kPageAll);
  log("all", {{"func", func}});
  const Clock::time_point t0 = Clock::now();

  std::mutex mu;
  std::condition_variable cv;
  bool stop = false;
  std::optional<int> n;
  Clock::time_point at = t0;
  std::optional<std::string> err;
  std::thread poll([&] {
    try {
      std::unique_lock lk(mu);
      while (!stop) {
        lk.unlock();
        const std::optional<int> got = ui::page_count(lbl, kFullWait);
        lk.lock();
        n = got;
        at = Clock::now();
        cv.wait_for(lk, std::chrono::seconds(kLiveGap), [&] { return stop; });
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
  } join{poll, mu, cv, stop};

  std::optional<std::pair<std::optional<int>, size_t>> seen;
  while (true) {
    std::optional<int> pn;
    Clock::time_point pat;
    {
      std::lock_guard lk(mu);
      if (err)
        fail(*err);
      pn = n;
      pat = at;
    }
    typename S::Index rows = table.read(pid).by_code;
    const Clock::time_point now = Clock::now();
    const int waited = static_cast<int>(secs(t0, now) + 0.5), hung = static_cast<int>(secs(pat, now) + 0.5);
    live(strf("「%s」已等 %ds · 内存 %zu 行 / 页上 %s 条%s", func.c_str(), waited, rows.size(), pn ? std::to_string(*pn).c_str() : "—",
              hung >= 3 ? strf(" · 客户端卡着 %ds (界面线程忙, 内存照读)", hung).c_str() : ""));
    if (!seen || *seen != std::pair{pn, rows.size()}) {
      seen = std::pair{pn, rows.size()};
      log("rows", {{"func", func}, {"n", pn ? nlohmann::json(*pn) : nlohmann::json()}, {"got", rows.size()}});
    }
    if (pn && rows.size() == static_cast<size_t>(*pn))
      return rows;
    if (secs(t0, now) >= kFullWait)
      fail(strf("%ds 内「%s」没拼齐 (页上 %s 条, 内存 %zu 行)", kFullWait, func.c_str(), pn ? std::to_string(*pn).c_str() : "—", rows.size()));
    std::this_thread::sleep_for(std::chrono::seconds(kLiveGap));
  }
}

Term term_of(const tables::MktCode &key, const tables::Margin::Index &mg, const tables::Collat::Index &co) {
  const auto mi = mg.find(key);
  const auto ci = co.find(key);
  const tables::MarginRow *m = mi == mg.end() ? nullptr : &mi->second;
  const tables::CollatRow *c = ci == co.end() ? nullptr : &ci->second;
  Term t;
  t.name = c ? c->name : m->name;
  t.mkt = key.first;
  if (m)
    t.ratio = m->ratio;
  t.financeable = m && m->remark.find("融资标的") != std::string::npos && m->status == kNormal;
  t.rate = c ? c->rate : 0;
  t.pledgeable = c && c->status == kNormal;
  return t;
}

// 两表 → 代码 → 条款; 跨市场同号的 (沪市债券与深市基金) 只留 market_of 那只
Terms merge(const tables::Margin::Index &mg, const tables::Collat::Index &co) {
  std::map<std::string, std::vector<tables::MktCode>> keys;
  std::set<tables::MktCode> all;
  for (const auto &[k, r] : mg)
    all.insert(k);
  for (const auto &[k, r] : co)
    all.insert(k);
  for (const tables::MktCode &k : all)
    keys[k.second].push_back(k);
  Terms out;
  for (auto &[code, ks] : keys) {
    if (ks.size() > 1) {
      std::erase_if(ks, [&](const tables::MktCode &k) { return k.first != market_of(code); });
      if (ks.size() != 1)
        fail(code + " 跨市场同号, 按代码定不出市场");
    }
    out[code] = term_of(ks[0], mg, co);
  }
  return out;
}

double value(const tables::Credit::Index &c, const char *name) {
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

Terms pull(Tables &t, int pid, const Log &log, const Live &live) {
  ui::show_bar();
  const auto mg = full(t.margin, pid, log, live, "标的证券查询", "融资保证金比例");
  const auto co = full(t.collat, pid, log, live, "担保品查询", "担保折算率");
  ui::click_tool("持仓");
  return merge(mg, co);
}

Account account(Tables &t, int pid, const Keyed<Position> &pos) {
  const auto c = t.credit.ask(pid, [] { ui::click_func("信用资产"); }).by_code;
  const auto d = t.debts.ask(pid, [] { ui::click_func("融资负债汇总"); }).by_code;
  Account a;
  a.at = clock_now();
  a.assets = value(c, "担保资产");
  a.line = value(c, "最高融资额度") - value(c, "融资负债（含费用）");
  a.debt = value(c, "负债总额");
  a.net = value(c, "净资产");
  a.avail = value(c, "可用保证金");
  a.cash = value(c, "现金资产");
  for (const auto &[code, r] : d)
    a.debts[code] = {r.qty, r.amount};
  a.bal = balances(pos);
  return a;
}

} // namespace live::tdx::credit
