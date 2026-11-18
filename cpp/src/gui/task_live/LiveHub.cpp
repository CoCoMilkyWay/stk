#include "gui/task_live/LiveHub.hpp"

#include "api/sina/client.hpp"
#include "disk/Disk.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "live/Terms.hpp"
#include "misc/parquet.hpp"
#include "shared/AssetInfo.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <functional>
#include <iterator>
#include <mutex>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

namespace GUI::Live {

using live::strf;

// 本地库的一只个股
struct Stock {
  std::string name;
  live::Mkt mkt = live::Mkt::SZ;
  bool listed = false; // 在市
  bool margin = false; // 两融明细最新一日有它 (交易所两融标的)
};
using Local = std::unordered_map<std::string, Stock>; // 代码 → 个股

struct LiveHub::Core {
  std::mutex mu;
  std::unique_ptr<live::Broker> broker; // connect 时建一次, 之后只读

  std::shared_ptr<const live::Quotes> quotes;
  uint64_t quotes_ver = 0;
  std::string quote_at;
  std::shared_ptr<const live::Account> px_acct; // px0 是哪份账户的读时现价
  std::shared_ptr<const live::rebal::Prices> px0;
  uint64_t px_ver = 0;

  std::shared_ptr<const live::terms::Cache> raw; // 券商的条款 (缓存); 还没拉过 = 空
  misc::Ymd auto_day = 0;                        // 上次自动拉条款的日子
  std::shared_ptr<const Local> local;            // 本地库的个股
  std::shared_ptr<const live::Terms> terms;      // raw 合 local; raw 空 = 空
  uint64_t terms_ver = 0;

  live::rebal::Cfg cfg;
  uint64_t cfg_ver = 0;
  std::vector<std::string> codes;
  std::string list_err;
  std::vector<live::Sec> buy; // 名单里可买的

  std::deque<OrderJob> jobs;
  uint64_t job_seq = 0;

  std::optional<Task> task;
  std::string task_stamp;
  double task_t0 = 0;
};

namespace {

using Core = LiveHub::Core;
using Lock = std::lock_guard<std::mutex>;

constexpr double kFillWait = 120, kFillGap = 1; // 等卖单成交: 最长秒数、两次只刷成交之间隔几秒

double mono() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

// ---------- 两融条款 ----------

constexpr const char *kMarginTable = "cn_stock_margin_trading_detail";

// 本地两融明细最新一日的代码 ("000001.SZ"): T 日盘后余额 T+1 盘前公布, 最新一日即上个交易日的交易所两融标的
std::unordered_set<std::string> margin_codes() {
  const std::vector<disk::Key> ks = disk::list(disk::FundShard, {.name = kMarginTable});
  for (auto k = ks.rbegin(); k != ks.rend(); ++k) {
    const std::filesystem::path p = disk::path(disk::FundShard, *k);
    const misc::Ymd last = misc::pq::max_yyyymmdd(p, "date");
    if (!last)
      continue;
    const misc::pq::TableView t(misc::pq::read_table(p, {"date", "instrument"}));
    const misc::pq::Col d = t.col("date"), ins = t.col("instrument");
    std::unordered_set<std::string> out;
    for (std::int64_t i = 0; i < t.rows(); ++i)
      if (d.yyyymmdd(i) == last)
        out.emplace(ins.str(i));
    return out;
  }
  assert(false && "本地库没有两融明细, 先在 Database 同步");
  return {};
}

// AssetInfo + 两融明细 → 本地库的个股; AssetInfo 还没建 → 空
std::shared_ptr<const Local> local_of(const AssetInfo &info) {
  auto out = std::make_shared<Local>();
  if (info.codes.empty())
    return out;
  const std::unordered_set<std::string> margin = margin_codes();
  for (size_t id = 0; id < info.codes.size(); ++id) {
    const StockInfo *s = info.find(id);
    if (!s)
      continue;
    const std::string &c = info.codes[id];
    assert(c.size() == 9 && c[6] == '.');
    (*out)[c.substr(0, 6)] = {s->name, live::mkt_named(std::string_view(c).substr(7)), s->out_date == 0, margin.contains(c)};
  }
  return out;
}

// 券商条款合本地库: 本地库有的个股名称 / 市场照本地, 融资标的 / 担保品两边都认才算; 本地库没有的照用券商的
live::Terms merged(const live::Terms &raw, const Local &local) {
  live::Terms out = raw;
  for (auto &[code, t] : out) {
    const auto i = local.find(code);
    if (i == local.end())
      continue;
    const Stock &s = i->second;
    t.name = s.name;
    t.mkt = s.mkt;
    t.financeable = t.financeable && s.margin;
    t.pledgeable = t.pledgeable && s.listed;
  }
  return out;
}

// 要用条款而缓存里没有的代码 (名单 / 持仓 / 负债); 本地库认得且北交所或已退市的本就不在券商表里, 不算
bool missing(const live::Terms &terms, const Local &local, const std::vector<std::string> &codes, const live::State *st) {
  const auto miss = [&](const std::string &code) {
    if (terms.contains(code))
      return false;
    const auto i = local.find(code);
    return i == local.end() || (i->second.listed && i->second.mkt != live::Mkt::BJ);
  };
  if (std::any_of(codes.begin(), codes.end(), miss))
    return true;
  if (st && st->pos && std::any_of(st->pos->list.begin(), st->pos->list.end(), [&](const live::Position &p) { return miss(p.code); }))
    return true;
  if (st && st->acct && std::any_of(st->acct->debts.list.begin(), st->acct->debts.list.end(), [&](const live::Debt &d) { return miss(d.code); }))
    return true;
  return false;
}

// 以下带 _locked 的须持 c.mu
void rebuy_locked(Core &c) { c.buy = c.terms ? live::rebal::buyable(c.codes, *c.terms) : std::vector<live::Sec>{}; }

void merge_locked(Core &c) {
  c.terms = c.raw ? std::make_shared<const live::Terms>(merged(c.raw->terms, *c.local)) : nullptr;
  ++c.terms_ver;
  rebuy_locked(c);
}

// ---------- 行情 ----------

// 持仓、今日成交、负债、名单里可买的, 按代码去重
std::vector<live::Sec> watch_set(Core &c, const live::State &st) {
  std::vector<live::Sec> v;
  std::unordered_set<std::string> seen;
  const auto add = [&](const live::Sec &s) {
    if (seen.insert(s.code).second)
      v.push_back(s);
  };
  if (st.pos)
    for (const live::Position &p : st.pos->list)
      add(p);
  if (st.fills)
    for (const live::Traded &f : st.fills->by_code.list)
      add(f);
  if (st.acct)
    for (const live::Debt &d : st.acct->debts.list)
      add(d);
  Lock lk(c.mu);
  for (const live::Sec &s : c.buy)
    add(s);
  return v;
}

// 行情 q 整份发布; 账户是新读到的且其各只都有了即记作读时现价
void take(Core &c, const live::State &st, std::shared_ptr<const live::Quotes> q) {
  std::string at;
  for (const auto &[code, x] : *q)
    at = std::max(at, live::quote_time(x));
  std::shared_ptr<live::rebal::Prices> px;
  if (st.acct) {
    px = std::make_shared<live::rebal::Prices>();
    bool all = true;
    const auto take = [&](const std::string &code) {
      const auto i = q->find(code);
      if (i == q->end())
        all = false;
      else
        (*px)[code] = i->second.price;
    };
    for (const live::Position &p : st.acct->held->list)
      take(p.code);
    for (const live::Debt &d : st.acct->debts.list)
      take(d.code);
    if (!all)
      px = nullptr;
  }
  Lock lk(c.mu);
  c.quotes = std::move(q);
  ++c.quotes_ver;
  c.quote_at = at;
  if (px && st.acct.v != c.px_acct) {
    c.px_acct = st.acct.v;
    c.px0 = px;
    ++c.px_ver;
  }
}

// 现取 secs 的行情 (事务要此刻的价, 不等订阅那一路) 发布
void fetch(Core &c, sina::Client &cl, const live::State &st, const std::vector<live::Sec> &secs) {
  auto q = std::make_shared<live::Quotes>();
  std::vector<std::string> syms;
  for (size_t i = 0; i < secs.size(); ++i) {
    syms.push_back(live::sina_sym(secs[i].mkt, secs[i].code));
    if (syms.size() == static_cast<size_t>(sina::kQuoteMax) || i + 1 == secs.size()) {
      for (const sina::Quote &x : cl.quotes(syms))
        (*q)[x.sym.substr(2)] = live::quote_of(x);
      syms.clear();
    }
  }
  take(c, st, std::move(q));
}

// ---------- 下单 ----------

// 有单在跑 → 0
uint64_t open_job(Core &c, const live::OrderReq &o, std::string label) {
  Lock lk(c.mu);
  if (std::any_of(c.jobs.begin(), c.jobs.end(), [](const OrderJob &j) { return j.st == JobSt::Run; }))
    return 0;
  OrderJob j{.id = ++c.job_seq, .t = live::clock_now(), .o = o, .label = std::move(label)};
  j.steps.push_back("行情");
  for (const std::string &s : c.broker->order_steps())
    j.steps.push_back(s);
  c.jobs.push_front(std::move(j));
  if (c.jobs.size() > kOrderCap)
    c.jobs.pop_back();
  return c.job_seq;
}

OrderJob put(Core &c, uint64_t id, const std::function<void(OrderJob &)> &f) {
  Lock lk(c.mu);
  const auto i = std::find_if(c.jobs.begin(), c.jobs.end(), [&](const OrderJob &j) { return j.id == id; });
  assert(i != c.jobs.end() && "在跑的单总在最前, 不会被挤掉");
  f(*i);
  return *i;
}

// ---------- 事务 ----------

std::string by_code(const nlohmann::json &want, const nlohmann::json &got) {
  std::string s;
  for (const auto &[code, n] : want.items())
    s += strf("%s%s %.0f/%.0f股", s.empty() ? "" : " · ", code.c_str(), got[code].get<double>(), n.get<double>());
  return s;
}

// 日志一条的文字
std::string describe(const std::string &k, const nlohmann::json &r) {
  if (k == "order") {
    const nlohmann::json &o = r["order"];
    const double qty = o["qty"].get<double>();
    return strf("%s %s %s %s %s", live::side_name(o["side"].get<int>()), o["credit"].get<std::string>().c_str(), o["code"].get<std::string>().c_str(),
                o["name"].get<std::string>().c_str(), qty > 0 ? strf("%.0f股", qty).c_str() : wan(o["size"].get<double>()).c_str());
  }
  if (k == "quote") {
    const nlohmann::json &q = r["quote"];
    const auto top = [](const nlohmann::json &side) { return side.empty() ? std::string("-") : strf("%.3f", side[0][0].get<double>()); };
    return strf("行情 %s 现价 %.3f 昨收 %.3f · 卖一 %s 买一 %s%s", q["at"].get<std::string>().c_str(), q["price"].get<double>(), q["last_close"].get<double>(),
                top(q["ask"]).c_str(), top(q["bid"]).c_str(), q["auction"].get<bool>() ? " · 竞价中" : "");
  }
  if (k == "job") {
    const nlohmann::json &j = r["job"];
    return strf("%s %s %s", j["state"].get<std::string>().c_str(), j["detail"].get<std::string>().c_str(), j["msg"].get<std::string>().c_str());
  }
  if (k == "plan") {
    const nlohmann::json &p = r["plan"];
    return strf("试算: 行使 %s (融资 %s) · %zu 单 · 成本 %.0f", wan(p["use"].get<double>()).c_str(), wan(p["financed"].get<double>()).c_str(),
                p["orders"].size(), p["cost"].get<double>());
  }
  if (k == "acct") {
    const nlohmann::json &a = r["acct"];
    return strf("账户: 现金 %s · 可用保证金 %s · 负债 %s", wan(a["cash"].get<double>()).c_str(), wan(a["avail"].get<double>()).c_str(),
                wan(a["debt"].get<double>()).c_str());
  }
  if (k == "fills")
    return "卖单成交 (到 / 要): " + by_code(r["want"], r["got"]);
  if (k == "pull")
    return "拉两融条款";
  if (k == "rows")
    return strf("「%s」%d 行", r["tab"].get<std::string>().c_str(), r["n"].get<int>());
  if (k == "terms")
    return strf("条款 %d 只 (融资标的 %d · 担保品 %d)", r["n"].get<int>(), r["financeable"].get<int>(), r["pledgeable"].get<int>());
  if (k == "window")
    return strf("资金流水 %d ~ %d: %d 条", r["lo"].get<int>(), r["hi"].get<int>(), r["n"].get<int>());
  if (k == "flows")
    return strf("资金流水账本 %d 条 (%d 起重查 %d 条), 持仓对上", r["rows"].get<int>(), r["from"].get<int>(), r["fresh"].get<int>());
  if (k == "err")
    return r["err"].get<std::string>();
  if (k == "ok")
    return "";
  return r.dump().substr(0, 300);
}

// 记一条: 落盘整条, 页面留文字
void note(Core &c, const char *kind, const nlohmann::json &kw) {
  std::string stamp;
  double t;
  {
    Lock lk(c.mu);
    stamp = c.task_stamp;
    t = mono() - c.task_t0;
  }
  nlohmann::json line = {{"t", std::round(t * 100) / 100}, {"kind", kind}};
  line.update(kw);
  disk::append(disk::RunLiveTaskLog, {.stamp = stamp}, line.dump());
  std::string text = describe(kind, kw);
  Lock lk(c.mu);
  c.task->log.push_back({t, kind, std::move(text)});
  if (c.task->log.size() > kLogTail)
    c.task->log.pop_front();
}
const nlohmann::json kNone = nlohmann::json::object();

nlohmann::json job_json(const OrderJob &j) {
  return {{"state", job_name(j.st)}, {"detail", j.detail}, {"msg", j.got.msg}, {"qty", j.got.qty}, {"px", j.got.px}};
}

// 事务走到哪一步
void stage(Core &c, std::string s) {
  Lock lk(c.mu);
  c.task->step = std::move(s);
}

// 事务的报告 (交给 broker 的长操作): 记一条 / 实时进度 / 走到哪一步
live::Report report_of(Core &c) {
  return {[&c](const char *k, nlohmann::json kw) { note(c, k, kw); },
          [&c](std::string line) {
            Lock lk(c.mu);
            c.task->live = std::move(line);
          },
          [&c](std::string s) { stage(c, std::move(s)); }};
}

nlohmann::json quote_json(const live::Quote &q) {
  const auto book = [](const std::vector<live::Level> &side) {
    nlohmann::json v = nlohmann::json::array();
    for (const live::Level &l : side)
      v.push_back({l.px, l.vol});
    return v;
  };
  return {{"at", live::quote_time(q)}, {"price", q.price}, {"last_close", q.last_close}, {"auction", live::auction(q)}, {"ask", book(q.book(live::kBuy))}, {"bid", book(q.book(live::kSell))}};
}

// 事务里下一单 (事务线程): 「行情」现取盘口记一条、按 mode 定委托价, 再交 broker 下; o 的量为 qty 股或 size 元 (恰给其一).
// 各步记进事务的步骤 (前缀 at, 如「卖出 2/5」); 结局记一条 job, 不成 → Fail
OrderJob exec(Core &c, live::OrderReq o, std::string label, live::PxMode mode, const std::string &at) {
  const uint64_t id = open_job(c, o, std::move(label));
  live::need(id != 0, "还有单在跑");
  const auto step = [&](size_t i) {
    std::string s;
    put(c, id, [&](OrderJob &j) {
      j.at = i;
      s = j.steps[i];
    });
    stage(c, at.empty() ? s : at + " " + s);
  };
  OrderJob out;
  try {
    step(0);
    sina::Client cl(sina::kQuoteHost);
    const std::vector<sina::Quote> got = cl.quotes({live::sina_sym(o.sec.mkt, o.sec.code)});
    live::need(!got.empty(), "新浪查无此码");
    const live::Quote q = live::quote_of(got[0]);
    note(c, "quote", {{"quote", quote_json(q)}});
    const std::optional<double> r = live::ref(q);
    live::need(r.has_value(), "无参考价");
    const std::optional<double> px = live::order_px(q, o.side, mode, o.size > 0 ? o.size : o.qty * *r);
    live::need(px.has_value(), "盘口为空 (竞价还没撮合出价 / 停牌), 定不了委托价");
    o.px = *px;
    put(c, id, [&](OrderJob &j) { j.o = o; });
    step(1);
    const live::Placed p = c.broker->place(o, [&](size_t i, const std::string &d) {
      step(1 + i);
      if (!d.empty())
        put(c, id, [&](OrderJob &j) { j.detail = d; });
    });
    out = put(c, id, [&](OrderJob &j) {
      j.st = JobSt::Ok;
      j.got = p;
    });
  } catch (const live::Fail &e) {
    out = put(c, id, [&](OrderJob &j) {
      j.st = JobSt::Err;
      j.got.msg = e.why;
    });
  }
  note(c, "job", {{"job", job_json(out)}});
  live::need(out.st == JobSt::Ok, strf("%s %s 下单失败: %s", o.sec.code.c_str(), o.sec.name.c_str(), out.got.msg.c_str()));
  return out;
}

void pull_terms(Core &c) {
  stage(c, "拉条款");
  note(c, "pull", kNone);
  auto x = std::make_shared<live::terms::Cache>(live::terms::Cache{misc::today_ymd(), c.broker->pull_terms(report_of(c))});
  int fin = 0, pl = 0;
  for (const auto &[code, t] : x->terms) {
    fin += t.financeable;
    pl += t.pledgeable;
  }
  note(c, "terms", {{"n", x->terms.size()}, {"financeable", fin}, {"pledgeable", pl}});
  live::terms::save(*x);
  Lock lk(c.mu);
  c.task->live.clear();
  c.raw = std::move(x);
  merge_locked(c);
}

// 刷新一轮 (成交、持仓、挂单、信用账户), 记下账户
std::shared_ptr<const live::State> sync(Core &c) {
  stage(c, "刷新");
  c.broker->sync(live::Part::All);
  auto st = c.broker->state();
  assert(st->pos && st->fills && st->orders && st->acct);
  const live::Account &a = *st->acct;
  nlohmann::json debts = nlohmann::json::object();
  for (const live::Debt &d : a.debts.list)
    debts[d.code] = {d.qty, d.amount};
  note(c, "acct", {{"acct", {{"at", st->acct.at}, {"assets", a.assets}, {"line", a.line}, {"debt", a.debt}, {"net", a.net}, {"avail", a.avail}, {"cash", a.cash}, {"debts", debts}}}});
  return st;
}

live::rebal::Plan plan_now(Core &c, sina::Client &cl) {
  stage(c, "试算");
  const auto st = c.broker->state();
  fetch(c, cl, *st, watch_set(c, *st));
  std::shared_ptr<const live::Terms> terms;
  std::shared_ptr<const live::rebal::Prices> px0;
  std::shared_ptr<const live::Quotes> q;
  live::rebal::Cfg cfg;
  std::vector<std::string> codes;
  {
    Lock lk(c.mu);
    live::need(c.terms != nullptr, "两融条款还没拉过");
    live::need(c.px_acct == st->acct.v, "账户各只的行情没取齐");
    terms = c.terms;
    px0 = c.px0;
    q = c.quotes;
    cfg = c.cfg;
    codes = c.codes;
  }
  const live::rebal::Result r = live::rebal::plan(*st->pos, *st->acct, *px0, *q, *terms, codes, cfg);
  live::need(r.plan.has_value(), r.wait);
  note(c, "plan", {{"cfg", live::rebal::to_json(cfg)}, {"codes", codes}, {"plan", live::rebal::to_json(*r.plan)}});
  return *r.plan;
}

OrderJob place_one(Core &c, const live::rebal::Order &o, live::PxMode mode, const std::string &at) {
  note(c, "order", {{"order", live::rebal::to_json(o)}});
  return exec(c, {o, o.side, o.credit, 0, o.qty, 0}, strf("%.0f股", o.qty), mode, at);
}

double sold(const live::State &st, const std::string &code) {
  const live::Traded *f = st.fills->by_code.find(code);
  return f ? f->qty[live::kSell] : 0;
}

// 只刷成交, 到成交表里各只卖量到 want [股]
void wait_fills(Core &c, const std::map<std::string, double> &want) {
  stage(c, "等成交");
  const double end = mono() + kFillWait;
  for (;;) {
    c.broker->sync(live::Part::Fills);
    const auto st = c.broker->state();
    std::map<std::string, double> got;
    for (const auto &[code, n] : want)
      got[code] = sold(*st, code);
    note(c, "fills", {{"want", want}, {"got", got}});
    if (std::all_of(want.begin(), want.end(), [&](const auto &w) { return got[w.first] >= w.second; }))
      return;
    live::need(mono() < end, strf("%.0fs 内卖单没成交完 (到 / 要): %s", kFillWait, by_code(want, got).c_str()));
    std::this_thread::sleep_for(std::chrono::duration<double>(kFillGap));
  }
}

void run_rebal(Core &c) {
  sina::Client cl(sina::kQuoteHost);
  bool have;
  {
    Lock lk(c.mu);
    have = c.raw != nullptr;
  }
  if (!have)
    pull_terms(c);
  const auto st = sync(c);
  if (!st->orders->list.empty()) {
    std::string s;
    for (const live::Pending &r : st->orders->list)
      s += strf("%s%s %s %.0f股", s.empty() ? "" : "、", r.code.c_str(), r.name.c_str(), r.qty - r.filled);
    live::fail(strf("还有 %zu 笔未成交委托 (%s), 撤掉或等成交后再调仓", st->orders->list.size(), s.c_str()));
  }
  live::PxMode mode;
  {
    Lock lk(c.mu);
    mode = c.cfg.px;
  }
  live::rebal::Plan p = plan_now(c, cl);
  std::vector<live::rebal::Order> sells;
  std::copy_if(p.orders.begin(), p.orders.end(), std::back_inserter(sells), [](const live::rebal::Order &o) { return o.side == live::kSell; });
  if (!sells.empty()) {
    std::map<std::string, double> want;
    for (const live::rebal::Order &o : sells)
      want[o.code] = sold(*st, o.code);
    c.broker->exclusive("调仓", [&] {
      for (size_t i = 0; i < sells.size(); ++i)
        want[sells[i].code] += place_one(c, sells[i], mode, strf("卖出 %zu/%zu", i + 1, sells.size())).got.qty;
    });
    wait_fills(c, want);
    sync(c);
    {
      Lock lk(c.mu);
      c.cfg.sells.clear();
      ++c.cfg_ver;
    }
    p = plan_now(c, cl);
  }
  c.broker->exclusive("调仓", [&] {
    for (size_t i = 0; i < p.orders.size(); ++i) {
      const live::rebal::Order &o = p.orders[i];
      live::need(o.side == live::kBuy, "去掉卖出后试算仍有卖单: " + o.code + " " + o.name);
      place_one(c, o, mode, strf("买入 %zu/%zu", i + 1, p.orders.size()));
    }
  });
  sync(c);
}

// 后台起一件事务; 已有事务在跑 → false
bool start(const std::shared_ptr<Core> &c, const char *name, std::function<void(Core &)> fn) {
  {
    Lock lk(c->mu);
    if (c->task && c->task->st == JobSt::Run)
      return false;
    c->task_stamp = disk::stamp_now();
    c->task_t0 = mono();
    c->task = Task{.name = name, .t = live::clock_now(), .file = disk::path(disk::RunLiveTaskLog, {.stamp = c->task_stamp}).string()};
  }
  const std::vector<disk::Key> old = disk::list(disk::RunLiveTaskLog);
  for (size_t i = 0; i + kTaskKeep <= old.size(); ++i)
    disk::remove(disk::RunLiveTaskLog, old[i]);
  std::thread([c, fn = std::move(fn)] {
    live::Watch w(*c->broker);
    JobSt st = JobSt::Ok;
    std::string msg;
    try {
      fn(*c);
      note(*c, "ok", kNone);
    } catch (const live::Fail &e) {
      st = JobSt::Err;
      msg = e.why;
      note(*c, "err", {{"err", e.why}});
    }
    Lock lk(c->mu);
    c->task->st = st;
    c->task->msg = std::move(msg);
    c->task->live.clear();
  }).detach();
  return true;
}

} // namespace

LiveHub::LiveHub() : c_(std::make_shared<Core>()) {
  {
    Lock lk(c_->mu);
    if (live::terms::Cache x = live::terms::load(); x.pulled)
      c_->raw = std::make_shared<const live::terms::Cache>(std::move(x));
    c_->local = std::make_shared<const Local>();
    merge_locked(*c_);
  }
  const std::vector<std::string> names = live::rebal::lists();
  if (!names.empty())
    set_list(names[0]);
  if (disk::exists(disk::LiveUiFile))
    priv_ = nlohmann::json::parse(disk::read(disk::LiveUiFile)).at("priv").get<bool>();
}

LiveHub::~LiveHub() = default;

void LiveHub::connect(size_t client) {
  assert(!connected() && client < live::clients().size());
  client_ = &live::clients()[client];
  c_->broker = client_->make();
}

const live::Client &LiveHub::client() const {
  assert(connected());
  return *client_;
}

void LiveHub::set_priv(bool on) {
  priv_ = on;
  disk::write(disk::LiveUiFile, {}, nlohmann::json{{"priv", on}}.dump(2));
}

live::Broker &LiveHub::broker() {
  assert(connected());
  return *c_->broker;
}

void LiveHub::peek() {
  Core &c = *c_;
  {
    Lock lk(c.mu);
    jobs_.assign(c.jobs.begin(), c.jobs.end());
    task_ = c.task;
  }
  if (c.broker)
    prog_ = c.broker->refresh();
}

void LiveHub::tick(const AssetInfo &info, Quote::QuoteHub &quote) {
  peek();
  Core &c = *c_;
  const std::array<int64_t, 3> ik = {info.pit_end, info.bar1m_end, static_cast<int64_t>(info.codes.size())};
  if (ik != info_key_) {
    info_key_ = ik;
    std::shared_ptr<const Local> l = local_of(info);
    Lock lk(c.mu);
    c.local = std::move(l);
    merge_locked(c);
  }
  if (c.broker) {
    c.broker->touch();
    st_ = c.broker->state();
    if (quote.started()) {
      const std::vector<live::Sec> secs = watch_set(c, *st_);
      std::vector<std::string> syms;
      for (const live::Sec &s : secs)
        syms.push_back(live::sina_sym(s.mkt, s.code));
      if (syms != watch_)
        quote.watch(watch_ = std::move(syms));
      if (const std::shared_ptr<const Quote::Live> &y = quote.live; y && y->watch && y->ver != live_ver_) {
        live_ver_ = y->ver;
        auto q = std::make_shared<live::Quotes>();
        for (const sina::Quote &x : *y->watch)
          (*q)[x.sym.substr(2)] = live::quote_of(x);
        take(c, *st_, std::move(q));
      }
    }
  }
  std::shared_ptr<const live::Quotes> q;
  std::shared_ptr<const live::Terms> terms;
  std::shared_ptr<const live::rebal::Prices> px0;
  std::shared_ptr<const live::Account> px_acct;
  uint64_t qv, tv, cv, pv;
  {
    Lock lk(c.mu);
    q = c.quotes;
    qv = c.quotes_ver;
    terms = c.terms;
    tv = c.terms_ver;
    cv = c.cfg_ver;
    px0 = c.px0;
    px_acct = c.px_acct;
    pv = c.px_ver;
    quote_at_ = c.quote_at;
    cfg_ = c.cfg;
    codes_ = c.codes;
    list_err_ = c.list_err;
    terms_n_ = terms ? std::optional<size_t>(terms->size()) : std::nullopt;
    terms_day_ = c.raw ? c.raw->pulled : 0;
  }
  if (!c.broker) {
    board_ = BoardView{.wait = "未连接"};
    plan_ = PlanView{.wait = "未连接"};
    return;
  }
  const live::State &st = *st_;

  if (board_key_[0] != st.ver || board_key_[1] != qv) {
    board_key_[0] = st.ver, board_key_[1] = qv;
    board_ = BoardView{.quotes = q};
    try {
      if (!st.pos || !st.fills)
        board_.wait = "还没读到持仓与成交 (等刷新)";
      else if (!q || !live::board::covered(*st.pos, *st.fills, *q))
        board_.wait = "等行情";
      else
        board_.frame = live::board::view(*st.pos, *st.fills, *q);
    } catch (const live::Fail &e) {
      board_.wait.clear();
      board_.err = e.why;
    }
  }

  const uint64_t key[5] = {st.ver, qv, cv, tv, pv};
  if (!std::equal(key, key + 5, plan_key_)) {
    std::copy(key, key + 5, plan_key_);
    plan_ = PlanView{};
    if (!terms)
      plan_.wait = "两融条款还没拉过 (更新两融名单)";
    else if (!st.pos || !st.acct)
      plan_.wait = "还没读到信用账户 (等刷新)";
    else if (px_acct != st.acct.v || !q)
      plan_.wait = "等账户各只的行情";
    else {
      try {
        live::rebal::Result r = live::rebal::plan(*st.pos, *st.acct, *px0, *q, *terms, codes_, cfg_);
        if (r.plan)
          plan_.plan = std::move(r.plan);
        else
          plan_.wait = r.wait;
      } catch (const live::Fail &e) {
        plan_.err = e.why;
      }
    }
    if (plan_.plan)
      plan_.now = plan_.plan->now;
    else if (st.acct)
      plan_.now = live::rebal::now_of(*st.acct);
  }
}

bool LiveHub::ordering() const {
  return std::any_of(jobs_.begin(), jobs_.end(), [](const OrderJob &j) { return j.st == JobSt::Run; });
}
bool LiveHub::busy() const { return task_ && task_->st == JobSt::Run; }

std::vector<std::string> LiveHub::lists() const { return live::rebal::lists(); }

void LiveHub::set_list(const std::string &name) {
  std::vector<std::string> codes;
  std::string err;
  try {
    codes = live::rebal::read_list(name);
  } catch (const live::Fail &e) {
    err = e.why;
  }
  Lock lk(c_->mu);
  c_->cfg.list = name;
  c_->codes = std::move(codes);
  c_->list_err = std::move(err);
  ++c_->cfg_ver;
  rebuy_locked(*c_);
}

void LiveHub::set_unit(double unit) {
  assert(unit >= 0);
  Lock lk(c_->mu);
  c_->cfg.unit = unit;
  ++c_->cfg_ver;
}

void LiveHub::set_pct(double pct) {
  assert(pct >= 0 && pct <= 100);
  Lock lk(c_->mu);
  c_->cfg.pct = pct;
  ++c_->cfg_ver;
}

void LiveHub::set_px(live::PxMode px) {
  Lock lk(c_->mu);
  c_->cfg.px = px;
  ++c_->cfg_ver;
}

void LiveHub::set_sell(const std::string &code, int n) {
  assert(n >= 0);
  Lock lk(c_->mu);
  if (n > 0)
    c_->cfg.sells[code] = n;
  else
    c_->cfg.sells.erase(code);
  ++c_->cfg_ver;
}

void LiveHub::submit(const std::string &code, int side, double size, live::Credit credit) {
  assert(st_ && size > 0);
  const live::Sec *sec = st_->pos ? st_->pos->find(code) : nullptr;
  if (!sec && st_->fills)
    sec = st_->fills->by_code.find(code);
  std::optional<live::Sec> buy;
  if (!sec) {
    Lock lk(c_->mu);
    const auto i = std::find_if(c_->buy.begin(), c_->buy.end(), [&](const live::Sec &s) { return s.code == code; });
    if (i != c_->buy.end())
      buy = *i;
  }
  assert(sec || (buy && side == live::kBuy));
  const live::OrderReq o{sec ? *sec : *buy, side, credit, 0, 0, size};
  start(c_, "下单", [o, mode = cfg_.px](Core &c) {
    note(c, "order", {{"order", {{"code", o.sec.code}, {"name", o.sec.name}, {"side", o.side}, {"credit", live::credit_name(o.credit)}, {"qty", 0}, {"size", o.size}, {"mode", live::kPxModeNames[static_cast<int>(mode)]}}}});
    exec(c, o, strf("%gW", o.size / 1e4), mode, "");
  });
}

void LiveHub::pull() {
  assert(connected());
  start(c_, "更新两融名单", pull_terms);
}

void LiveHub::run() {
  assert(connected());
  start(c_, "执行调仓", run_rebal);
}

bool LiveHub::job(const char *name, Job fn) {
  assert(connected());
  return start(c_, name, [fn = std::move(fn)](Core &c) { fn(*c.broker, report_of(c)); });
}

void LiveHub::auto_pull() {
  if (!connected())
    return;
  const misc::Ymd d = misc::today_ymd();
  {
    Lock lk(c_->mu);
    if (c_->auto_day == d || (c_->task && c_->task->st == JobSt::Run))
      return;
    if (c_->raw && misc::add_days(c_->raw->pulled, kTermsAge) > d && !missing(c_->raw->terms, *c_->local, c_->codes, st_.get()))
      return;
    c_->auto_day = d;
  }
  pull();
}

} // namespace GUI::Live
