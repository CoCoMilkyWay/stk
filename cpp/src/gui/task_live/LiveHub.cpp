#include "gui/task_live/LiveHub.hpp"

#include "disk/Disk.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "gui/task_live/Books.hpp"
#include "live/Flows.hpp"
#include "shared/AssetInfo.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <iterator>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_set>

namespace GUI::Live {

using live::strf;

struct LiveHub::Core {
  std::unique_ptr<live::Broker> broker; // connect 时建一次, 之后只读
  live::Steps steps;                    // 下单各步: 「行情」+ broker 的各步, 同 broker 建一次
  live::trace::Track track;             // 事务线程此刻在哪

  // mu: 三份快照 (改 = 抄一份改了换上)
  std::mutex mu;
  std::shared_ptr<const Inputs> in;
  std::shared_ptr<const Task> task;
  std::shared_ptr<const std::vector<OrderJob>> jobs = std::make_shared<const std::vector<OrderJob>>();
};

namespace {

using Core = LiveHub::Core;
using Lock = std::lock_guard<std::mutex>;

double mono() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

// 改核心的一份快照 m: 抄一份, f 改了换上
template <class T, class F>
void edit(Core &c, std::shared_ptr<const T> Core::*m, F &&f) {
  Lock lk(c.mu);
  auto x = std::make_shared<T>(*(c.*m));
  f(*x);
  c.*m = std::move(x);
}

// 改正下着的那单 (最近几单之首, 在跑): 同 edit → 改后的本单; 不在下单即不动 → 空
template <class F>
std::optional<OrderJob> ordering(Core &c, F &&f) {
  Lock lk(c.mu);
  if (c.jobs->empty() || c.jobs->front().st != JobSt::Run)
    return std::nullopt;
  auto v = std::make_shared<std::vector<OrderJob>>(*c.jobs);
  f(v->front());
  c.jobs = std::move(v);
  return c.jobs->front();
}

std::shared_ptr<const Inputs> inputs(Core &c) {
  Lock lk(c.mu);
  return c.in;
}

// 视图 (BoardView / PlanView) 换上重算的 next: 只是在等且已有上一份即留着上一份照显、只换 wait, 否则整份换
template <class V>
void settle(V &cur, V next) {
  if (next.wait.empty() || !cur.ready())
    cur = std::move(next);
  else
    cur.wait = std::move(next.wait);
}

// 显示名 (见 LiveHub::name_of): 本地库 l 有的个股 (交易所 + 代码对得上) 取本地简称, 否则 fallback
const std::string &local_name(const live::terms::Local &l, const std::string &code, const std::string &fallback) {
  const auto i = l.find(code);
  return i != l.end() && i->second.mkt == live::market_of(code) && !i->second.name.empty() ? i->second.name : fallback;
}

// 两组的派生: 条款 = 券商的合本地库; 名单里可买的 (随条款)
void derive(Inputs &x) {
  x.terms.merged = x.terms.raw ? std::make_shared<const live::Terms>(live::terms::merged(x.terms.raw->terms, *x.terms.local)) : nullptr;
  x.list.buy = x.terms.merged ? live::rebal::buyable(x.list.codes, *x.terms.merged) : std::vector<live::Sec>{};
}

// ---------- 行情 ----------

// 持仓、今日成交、负债、名单里可买的, 按代码去重
std::vector<live::Sec> watch_set(const live::Snapshot *snap, const Inputs &in) {
  std::vector<live::Sec> v;
  std::unordered_set<std::string> seen;
  const auto add = [&](const live::Sec &s) {
    if (seen.insert(s.code).second)
      v.push_back(s);
  };
  if (snap) {
    for (const live::Sec *s : live::held(*snap))
      add(*s);
    for (const live::Traded &f : snap->fills.by_code.list)
      add(f);
  }
  for (const live::Sec &s : in.list.buy)
    add(s);
  return v;
}

// 账户里各只 (live::held) 在 q 里的现价; 缺一只 → 空
std::shared_ptr<const live::rebal::Prices> prices_of(const live::Snapshot &s, const live::Quotes &q) {
  auto px = std::make_shared<live::rebal::Prices>();
  for (const live::Sec *x : live::held(s)) {
    const auto i = q.find(x->code);
    if (i == q.end())
      return nullptr;
    (*px)[x->code] = i->second.price;
  }
  return px;
}

// 行情 q 整份换上; 快照是新读到的且其各只都有了即记作读时现价
void take(Core &c, const std::shared_ptr<const live::Snapshot> &snap, std::shared_ptr<const live::Quotes> q) {
  int sec = -1;
  for (const auto &[code, x] : *q)
    sec = std::max(sec, x.sec);
  std::shared_ptr<const live::rebal::Prices> px = snap ? prices_of(*snap, *q) : nullptr;
  edit(c, &Core::in, [&](Inputs &x) {
    x.px.quotes = std::move(q);
    x.px.sec = sec;
    if (px && snap != x.px.snap) {
      x.px.snap = snap;
      x.px.px0 = std::move(px);
    }
  });
}

// 现取 secs 的行情 (事务要此刻的价, 不等订阅那一路) 换上; 取不到 → Fail (一试, 不卡在半路)
void fetch(Core &c, sina::Client &cl, const std::shared_ptr<const live::Snapshot> &snap, const std::vector<live::Sec> &secs) {
  std::optional<live::Quotes> q = quotes_of(cl, secs);
  live::need(q.has_value(), "新浪取不到行情 (%zu 只)", secs.size());
  take(c, snap, std::make_shared<const live::Quotes>(std::move(*q)));
}

// ---------- 事务 ----------

live::trace::Kw quote_json(const live::Quote &q) {
  const auto book = [](const std::vector<live::Level> &side) {
    nlohmann::json v = nlohmann::json::array();
    for (const live::Level &l : side)
      v.push_back({l.px, l.vol});
    return v;
  };
  return {{"at", live::fmt_sec(q.sec)}, {"price", q.price}, {"last_close", q.last_close}, {"auction", live::auction(q)}, {"ask", book(q.book[live::kBuy])}, {"bid", book(q.book[live::kSell])}};
}

std::string quote_text(const live::Quote &q) {
  const auto top = [](const std::vector<live::Level> &side) { return side.empty() ? std::string("-") : strf("%.3f", side[0].px); };
  return strf("行情 %s 现价 %.3f 昨收 %.3f · 卖一 %s 买一 %s%s", live::fmt_sec(q.sec).c_str(), q.price, q.last_close,
              top(q.book[live::kBuy]).c_str(), top(q.book[live::kSell]).c_str(), live::auction(q) ? " · 竞价中" : "");
}

// 走到哪一步 (事务的第二级标签; 批量下单时即本单在批里的位置, 如「卖出 2/5」)
void stage(Core &c, std::string s) {
  edit(c, &Core::task, [&](Task &x) { x.step = std::move(s); });
}

// 事务里下一单: 「行情」从 books 取盘口记一条、按 mode 定委托价 (yuan > 0 = 按额: 额定价、按委托价折股; 否则按 o.qty 股), 再交 broker 下.
// 本单进最近几单之首, 其各步 (段名即步名) 与实时一行经事务线程的 Hooks 同步进本单; 请求与结局各记一条.
// 成败看 st, 不抛: 本单出的任何错 (含应答畸形抛的) 都收在本单, 批量下单接着下后面的.
// 名称换成显示名 (本地库简称): 券商表给的可能是全称, 客户端切图表按标题里的简称认
OrderJob exec(Core &c, Books &books, live::OrderReq o, double yuan, live::PxMode mode, const std::string &at) {
  o.name = local_name(*inputs(c)->terms.local, o.code, o.name);
  const char *px_name = live::kPxModeNames[static_cast<int>(mode)];
  std::string label = yuan > 0 ? strf("%gW", yuan / 1e4) : strf("%lld股", static_cast<long long>(o.qty));
  live::trace::note("order", strf("%s %s %s %s %s · %s", live::side_name(o.side), live::credit_name(o.credit), o.code.c_str(), o.name.c_str(), label.c_str(), px_name),
                    {{"order", live::to_json(o)}, {"yuan", yuan}, {"mode", px_name}});
  stage(c, at);

  OrderJob j;
  j.sec = live::sec_now();
  j.o = o;
  j.label = std::move(label);
  edit(c, &Core::jobs, [&](std::vector<OrderJob> &v) {
    v.insert(v.begin(), std::move(j));
    if (v.size() > kOrderCap)
      v.pop_back();
  });

  live::Placed p;
  const std::string err = live::guard([&] {
    const std::string q = live::quiet(live::sec_now());
    live::need(q.empty(), q);
    {
      live::trace::Span sp("行情");
      const Books::Book b = books.get(o);
      const live::Quote &q = b.q;
      live::trace::note("quote", quote_text(q) + (b.fed ? strf(" · 流 %.1fs 前", b.age) : " · 现取"),
                        {{"quote", quote_json(q)}, {"fed", b.fed}, {"age", std::round(b.age * 100) / 100}});
      const std::optional<double> ref = live::ref(q);
      live::need(ref.has_value(), "无参考价");
      const std::optional<double> px = live::order_px(q, o.side, mode, yuan > 0 ? yuan : static_cast<double>(o.qty) * *ref);
      live::need(px.has_value(), "盘口为空 (竞价还没撮合出价 / 停牌), 定不了委托价");
      o.px = *px;
      if (yuan > 0)
        o.qty = live::shares_of(o.code, o.side, yuan, o.px);
      ordering(c, [&](OrderJob &y) { y.o = o; });
    }
    p = c.broker->place(o);
  });
  const std::optional<OrderJob> got = ordering(c, [&](OrderJob &y) {
    y.st = err.empty() ? JobSt::Ok : JobSt::Err;
    y.got = p;
    y.msg = err.empty() ? p.msg : err;
  });
  assert(got && "本单在跑");
  const OrderJob &out = *got;
  live::trace::note("job", strf("%s %s %s", job_name(out.st), out.live.c_str(), out.msg.c_str()),
                    {{"job", {{"state", job_name(out.st)}, {"live", out.live}, {"msg", out.msg}, {"qty", out.got.qty}, {"px", out.got.px}}}});
  return out;
}

std::string failed(const OrderJob &j) { return strf("%s %s 下单失败: %s", j.o.code.c_str(), j.o.name.c_str(), j.msg.c_str()); }

// 批里一单下完, 末了要不要重下一次 (空 = 不): 停在报出委托之前的原样重下 (客户端一时没跟上 / 钱券那时还没到账; 每次至多费几秒);
// 买单被最大可买封顶少买的重下余量 (够起买量): 前面的卖单陆续成交, 回款与腾出的保证金随之到账
std::optional<live::OrderReq> redo(const Core &c, const OrderJob &j) {
  if (j.st == JobSt::Err)
    return j.at < c.steps.unsent ? std::optional<live::OrderReq>(j.o) : std::nullopt;
  const int64_t rest = j.o.qty - j.got.qty;
  if (j.o.side != live::kBuy || rest < live::lot(j.o.code).first)
    return std::nullopt;
  live::OrderReq o = j.o;
  o.qty = rest;
  return o;
}

// 两样拉取 (同形, 下标 kPulls 序): miss = 拉过之后、快照 s 读到后另该拉的情形 (PullSpec::also; 空 = 无); run = 拉 → 存, 换上 → 拉于
bool terms_miss(Core &c, const live::Snapshot &s) {
  const std::shared_ptr<const Inputs> in = inputs(c);
  assert(in->terms.raw && "拉过才核缺");
  std::vector<std::string> codes = in->list.codes;
  for (const live::Sec *x : live::held(s))
    codes.push_back(x->code);
  return live::terms::missing(in->terms.raw->terms, *in->terms.local, codes);
}

misc::Ymd terms_run(Core &c) {
  auto x = std::make_shared<const live::terms::Cache>(live::terms::pull(*c.broker));
  const misc::Ymd d = x->pulled;
  edit(c, &Core::in, [&](Inputs &in) {
    in.terms.raw = std::move(x);
    derive(in);
  });
  return d;
}
misc::Ymd flows_run(Core &c) { return live::flows::pull(*c.broker).pulled; }

struct PullDef : PullSpec {
  bool (*miss)(Core &, const live::Snapshot &);
  misc::Ymd (*run)(Core &);
};
constexpr PullDef kPullDefs[] = {
    {{"更新两融名单", 7,
      "客户端重查融资标的与担保品两张全表 (慢, 期间独占客户端界面), 整份换掉缓存 output/live/terms.csv; 平时只用缓存\n"
      "个股的名称、融资标的、担保品另经本地库核 (Database 同步的两融明细 / 在市)",
      "名单 / 持仓 / 负债里有缓存没有的代码", "两融条款缓存拉于哪天 · 只数"},
     terms_miss,
     terms_run},
    {{"更新资金流水", 1,
      "客户端重查账本末日起的资金流水与历史成交 (慢, 一窗 30 天), 并入账本; 历史成交逐日逐只对上流水的买卖, 再刷新一轮核对 (流水累计的持仓 + 今日成交 = 券商持仓)\n"
      "才落 output/live/flows.csv · deals.csv; 账户净值 (Market 自选首行「账户」) 随账本重放, 历史成交另给日内回推与成交点",
      "", "资金流水账本拉于哪天 (账本截至其前一日)"},
     nullptr,
     flows_run},
};
static_assert(std::size(kPullDefs) == std::size(kPulls));

const PullDef &def(Pull p) { return kPullDefs[static_cast<int>(p)]; }

// p 该拉了 (今天 d, 快照 s 读到后): 还没拉过、拉过满 age 天, 或另该拉
bool due(Core &c, Pull p, const live::Snapshot &s, misc::Ymd d) {
  const misc::Ymd at = inputs(c)->pulled[static_cast<int>(p)];
  return !at || misc::add_days(at, def(p).age) <= d || (def(p).miss && def(p).miss(c, s));
}

// 拉 p (事务里), 记下拉于
void pull_run(Core &c, Pull p) {
  const misc::Ymd d = def(p).run(c);
  edit(c, &Core::in, [&](Inputs &x) { x.pulled[static_cast<int>(p)] = d; });
}

// 刷新一轮 (成交、持仓、挂单、信用账户), 记下账户
std::shared_ptr<const live::Snapshot> sync(Core &c) {
  live::trace::Span sp("刷新");
  c.broker->sync();
  auto snap = c.broker->snapshot();
  assert(snap);
  const live::Account &a = snap->acct;
  nlohmann::json debts = nlohmann::json::object();
  for (const live::Debt &d : snap->debts.list)
    debts[d.code] = {d.qty, d.amount};
  live::trace::note("acct", strf("账户: 现金 %s · 可用保证金 %s · 负债 %s", wan(a.cash).c_str(), wan(a.avail).c_str(), wan(a.debt).c_str()),
                    {{"acct", {{"at", live::fmt_sec(snap->at)}, {"assets", a.assets}, {"line", a.line}, {"debt", a.debt}, {"net", a.net}, {"avail", a.avail}, {"cash", a.cash}, {"debts", debts}}}});
  return snap;
}

live::rebal::Plan plan_now(Core &c, sina::Client &cl) {
  live::trace::Span sp("试算");
  const auto snap = c.broker->snapshot();
  assert(snap);
  fetch(c, cl, snap, watch_set(snap.get(), *inputs(c)));
  const std::shared_ptr<const Inputs> in = inputs(c);
  live::need(in->terms.err.empty(), "%s", in->terms.err.c_str());
  live::need(in->terms.merged != nullptr, "两融条款还没拉过");
  live::need(in->px.snap == snap, "账户各只的行情没取齐");
  const std::optional<live::rebal::Plan> p = live::rebal::plan(*snap, *in->px.px0, *in->px.quotes, *in->terms.merged, in->list.codes, in->cfg);
  live::need(p.has_value(), "新浪没给齐要的行情");
  live::trace::note("plan", strf("试算: 行使 %s (融资 %s) · %zu 单 · 成本 %.0f", wan(p->use).c_str(), wan(p->fin[live::kBuy]).c_str(), p->orders.size(), p->cost),
                    {{"cfg", live::rebal::to_json(in->cfg)}, {"list", in->list.name}, {"codes", in->list.codes}, {"plan", live::rebal::to_json(*p)}});
  return *p;
}

// 执行调仓. 要紧处:
//   · 只收单不撮合的静默段 (live::quiet) 不起: 盘口不作数; 各单下前另核一次 (批跨进静默段即其后各单失败)
//   · 试算现做一遍 (刷新后的账户 + 此刻行情), 页面上的那份只作预览; 各单的股数照它, 委托价下单时按新取的盘口再定
//   · 一口气下完, 不等成交: 卖出回款 / 腾出的保证金在卖单成交前还没到账, 紧跟着的融资买单由闪电窗的最大可买封顶 (少买的余量批末重下);
//     不等是有意的 —— 等成交会在半路卡住 (卖了一半没买), 留下方向与价格敞口
//   · 一单失败 (含任何意外) 只记下、接着下后面的; 批末把没报出就停下的与封顶少买的余量按原序重下一轮 (redo, 只一轮),
//     仍没下成的末了一并报失败 (该补哪几单); 取行情、等界面一律有界, 不会卡在某一单
//   · 下完即清掉卖出设置 (先于末了的刷新: 刷新失败也不留着): 再点执行不会重复卖
//   · 末了的刷新只为页面尽早见到结果, 失败只记进结局, 不盖掉各单的结局
void rebal_task(Core &c) {
  const std::string q = live::quiet(live::sec_now());
  live::need(q.empty(), q);
  sina::Client cl(sina::kQuoteHost);
  if (!inputs(c)->terms.raw)
    pull_run(c, Pull::Terms);
  const auto snap = sync(c);
  if (!snap->pends.list.empty()) {
    std::string s;
    for (const live::Pending &r : snap->pends.list)
      s += strf("%s%s %s %lld股", s.empty() ? "" : "、", r.code.c_str(), r.name.c_str(), static_cast<long long>(r.qty - r.filled));
    live::fail(strf("还有 %zu 笔未成交委托 (%s), 撤掉或等成交后再调仓", snap->pends.list.size(), s.c_str()));
  }
  const live::PxMode mode = inputs(c)->cfg.px;
  const live::rebal::Plan p = plan_now(c, cl);
  std::vector<std::string> fails; // 没下成的单: 记下接着下后面的, 末了一并报
  c.broker->batch([&] {
    Books books(std::vector<live::Sec>(p.orders.begin(), p.orders.end()));
    std::vector<live::OrderReq> again; // 末了重下的 (见 redo), 原序
    for (size_t i = 0; i < p.orders.size(); ++i) {
      const live::rebal::Order &o = p.orders[i];
      const OrderJob j = exec(c, books, o, 0, mode, strf("%s %zu/%zu", live::side_name(o.side), i + 1, p.orders.size()));
      if (std::optional<live::OrderReq> r = redo(c, j))
        again.push_back(std::move(*r));
      else if (j.st != JobSt::Ok)
        fails.push_back(failed(j));
    }
    for (size_t i = 0; i < again.size(); ++i) {
      const OrderJob j = exec(c, books, again[i], 0, mode, strf("重下 %zu/%zu", i + 1, again.size()));
      if (j.st != JobSt::Ok)
        fails.push_back(failed(j));
    }
  });
  edit(c, &Core::in, [](Inputs &x) { x.cfg.sells.clear(); });
  stage(c, "");
  const std::string after = live::guard([&] { sync(c); });
  if (!fails.empty()) {
    std::string s;
    for (const std::string &f : fails)
      s += (s.empty() ? "" : "; ") + f;
    live::fail(strf("%zu 单没下成, 其余照下: %s%s", fails.size(), s.c_str(), after.empty() ? "" : ("; 末了刷新失败: " + after).c_str()));
  }
  live::need(after.empty(), "各单已下, 末了刷新失败: %s", after.c_str());
}

// 现金还款: 刷新后的已清仓仍负债各只 (欠款少的先还), 共至多 cap 元 (∞ = 全部还清; 另以现金封顶); 各笔 broker 各记一条; 末了的刷新失败只记进结局
void repay_task(Core &c, double cap) {
  const auto snap = sync(c);
  std::vector<std::string> codes;
  double owe = 0;
  for (const live::Debt *d : live::orphans(*snap))
    codes.push_back(d->code), owe += d->amount;
  live::need(!codes.empty(), "没有已清仓仍负债的");
  live::need(snap->acct.cash >= 0.01, "现金 %s, 还不了", wan(snap->acct.cash).c_str());
  const bool full = std::isinf(cap);
  live::trace::note("repay", strf("已清仓仍负债 %zu 只共 %s, 现金还%s", codes.size(), wan(owe).c_str(), full ? "清" : ("至多 " + wan(cap)).c_str()),
                    {{"codes", codes}, {"owe", owe}, {"full", full}});
  const std::vector<live::Repaid> r = c.broker->repay(codes, full ? cap : std::min(cap, snap->acct.cash));
  double sum = 0;
  for (const live::Repaid &x : r)
    sum += x.amount;
  live::trace::note("repay", strf("已还 %zu 笔共 %s", r.size(), wan(sum).c_str()), {{"n", r.size()}, {"sum", sum}});
  const std::string after = live::guard([&] { sync(c); });
  live::need(after.empty(), "已还 %zu 笔共 %s, 末了刷新失败: %s", r.size(), wan(sum).c_str(), after.c_str());
}

// 后台起一件事务; 已有事务在跑 → 不收, false.
//   事务线程挂 Watch + Hooks: 段 / 实时一行 / 记一条照常经 live::trace 落交互日志, Hooks 另把记的各条落本件事务的日志文件
//   (output/run/live/task/<时刻>.jsonl, 留最近 kTaskKeep 个: 下单留底比交互日志留得久) 并留尾巴给页面, 正下着的那单同步其各步与实时一行.
//   事务的段在 Watch 之外 (进度只列其下的段, 名字已在事务名上)
bool start(const std::shared_ptr<Core> &c, const char *name, std::function<void(Core &)> fn) {
  const std::string stamp = disk::stamp_now();
  const double t0 = mono();
  {
    Lock lk(c->mu);
    if (c->task && c->task->st == JobSt::Run)
      return false;
    auto t = std::make_shared<Task>();
    t->sec = live::sec_now();
    t->name = name;
    t->file = disk::path(disk::RunLiveTaskLog, {.stamp = stamp}).string();
    c->task = std::move(t);
  }
  const std::vector<disk::Key> old = disk::list(disk::RunLiveTaskLog);
  for (size_t i = 0; i + kTaskKeep <= old.size(); ++i)
    disk::remove(disk::RunLiveTaskLog, old[i]);
  std::thread([c, name = std::string(name), fn = std::move(fn), stamp, t0] {
    const live::trace::Hooks hooks{
        .on_note = [&](const char *kind, std::string text, const live::trace::Kw &kw) {
          const double t = mono() - t0;
          live::trace::Kw line = {{"t", std::round(t * 100) / 100}, {"kind", kind}};
          for (const auto &e : kw.items())
            line[e.key()] = e.value();
          disk::append(disk::RunLiveTaskLog, {.stamp = stamp}, line.dump(-1, ' ', false, live::trace::Kw::error_handler_t::replace));
          edit(*c, &Core::task, [&](Task &x) {
            x.log.push_back({t, kind, std::move(text)});
            if (x.log.size() > kLogTail)
              x.log.pop_front();
          }); },
        .on_span = [&](std::string_view s) {
          const std::vector<std::string> &n = c->steps.names;
          if (const auto it = std::find(n.begin(), n.end(), s); it != n.end())
            ordering(*c, [&](OrderJob &j) { j.at = static_cast<size_t>(it - n.begin()); }); },
        .on_live = [&](std::string line) {
          ordering(*c, [&](OrderJob &j) { j.live = line; });
          edit(*c, &Core::task, [&](Task &x) { x.live = std::move(line); }); },
    };
    std::string msg;
    {
      live::trace::Span sp(name);
      const live::trace::Watch watch(c->track, &hooks);
      msg = live::guard([&] { fn(*c); });
      if (msg.empty())
        live::trace::note("ok", "");
      else
        live::trace::note("err", msg, {{"err", msg}});
    }
    edit(*c, &Core::task, [&](Task &t) {
      t.st = msg.empty() ? JobSt::Ok : JobSt::Err;
      t.msg = std::move(msg);
      t.step.clear();
    });
  }).detach();
  return true;
}

} // namespace

const PullSpec &spec(Pull p) { return def(p); }

LiveHub::LiveHub() : c_(std::make_shared<Core>()) {
  auto in = std::make_shared<Inputs>();
  if (live::terms::Cache x = live::terms::load(); x.pulled)
    in->terms.raw = std::make_shared<const live::terms::Cache>(std::move(x));
  in->terms.local = std::make_shared<const live::terms::Local>();
  derive(*in);
  in->pulled = {in->terms.raw ? in->terms.raw->pulled : 0, live::flows::load().pulled};
  c_->in = std::move(in);
  if (const std::vector<std::string> names = live::rebal::lists(); !names.empty())
    set_list(names[0]);
  if (disk::exists(disk::LiveUiFile))
    priv_ = nlohmann::json::parse(disk::read(disk::LiveUiFile)).at("priv").get<bool>();
  peek();
}

LiveHub::~LiveHub() = default;

void LiveHub::connect(size_t client) {
  assert(!connected() && client < live::clients().size());
  client_ = &live::clients()[client];
  c_->broker = std::make_unique<live::Broker>(client_->make());
  c_->steps = c_->broker->steps();
  c_->steps.names.insert(c_->steps.names.begin(), "行情");
  ++c_->steps.unsent;
  peek();
}

const std::vector<std::string> &LiveHub::steps() const {
  assert(connected());
  return c_->steps.names;
}

const live::Client &LiveHub::client() const {
  assert(connected());
  return *client_;
}

void LiveHub::set_priv(bool on) {
  priv_ = on;
  disk::write(disk::LiveUiFile, {}, nlohmann::json{{"priv", on}}.dump(2));
}

void LiveHub::peek() {
  {
    Lock lk(c_->mu);
    in_ = c_->in;
    task_ = c_->task;
    jobs_ = c_->jobs;
  }
  task_at_ = c_->track.now();
  if (c_->broker) {
    snap_ = c_->broker->snapshot();
    prog_ = c_->broker->refresh();
  }
}

void LiveHub::update() {
  peek();
  if (!connected() || !snap_)
    return;
  const misc::Ymd d = misc::today_ymd();
  for (const Pull p : kPulls) {
    misc::Ymd &tried = tried_[static_cast<int>(p)];
    if (tried < d && due(*c_, p, *snap_, d)) {
      if (start(c_, spec(p).name, [p](Core &c) { pull_run(c, p); }))
        tried = d;
      return;
    }
  }
}

const std::string &LiveHub::name_of(const std::string &code, const std::string &fallback) const { return local_name(*in_->terms.local, code, fallback); }

void LiveHub::tick(const AssetInfo &info, Quote::QuoteHub &quote) {
  Core &c = *c_;
  info_st_.update({info.pit_end, info.bar1m_end, static_cast<int64_t>(info.codes.size())}, [&](int &) {
    live::terms::Local l;
    std::string err = live::guard([&] { l = live::terms::local(info); });
    auto lp = std::make_shared<const live::terms::Local>(std::move(l));
    edit(c, &Core::in, [&](Inputs &x) {
      x.terms.local = std::move(lp);
      x.terms.err = std::move(err);
      derive(x);
    });
    in_ = inputs(c);
  });
  if (!c.broker) {
    board_.val = BoardView{.wait = "未连接"};
    plan_.val = PlanView{.wait = "未连接"};
    return;
  }
  c.broker->touch();
  if (quote.started()) {
    std::vector<std::string> syms;
    for (const live::Sec &s : watch_set(snap_.get(), *in_))
      syms.push_back(live::sina_sym(s.mkt, s.code));
    if (syms != watch_)
      quote.watch(watch_ = std::move(syms));
    if (const std::shared_ptr<const Quote::Live> &y = quote.live; y && y->watch && y->ver != live_ver_) {
      live_ver_ = y->ver;
      auto q = std::make_shared<live::Quotes>();
      merge(*q, *y->watch);
      take(c, snap_, std::move(q));
      in_ = inputs(c);
    }
  }
  const Inputs &in = *in_;

  board_.update({snap_.get(), in.px.quotes.get(), in.terms.local.get()}, [&](BoardView &cur) {
    BoardView b{.snap = snap_, .quotes = in.px.quotes};
    b.err = live::guard([&] {
      if (!snap_)
        b.wait = "还没读到持仓与成交 (等刷新)";
      else if (!in.px.quotes || !(b.frame = live::board::view(*snap_, *in.px.quotes)))
        b.wait = "等行情";
    });
    if (b.frame)
      for (live::board::Row &r : b.frame->rows)
        r.name = name_of(r.code, r.name);
    settle(cur, std::move(b));
  });

  plan_.update({snap_.get(), in_.get()}, [&](PlanView &cur) {
    PlanView p;
    if (!in.terms.err.empty())
      p.err = in.terms.err;
    else if (!in.terms.merged)
      p.wait = strf("两融条款还没拉过 (%s)", spec(Pull::Terms).name);
    else if (!snap_)
      p.wait = "还没读到信用账户 (等刷新)";
    else if (in.px.snap != snap_ || !in.px.quotes)
      p.wait = "等账户各只的行情";
    else
      p.err = live::guard([&] {
        if (!(p.plan = live::rebal::plan(*snap_, *in.px.px0, *in.px.quotes, *in.terms.merged, in.list.codes, in.cfg)))
          p.wait = "等行情";
      });
    if (p.plan)
      p.now = p.plan->now;
    else if (snap_)
      p.now = live::rebal::now_of(*snap_);
    settle(cur, std::move(p));
  });
}

void LiveHub::request() {
  assert(connected());
  c_->broker->request();
}

void LiveHub::every(double s) {
  assert(connected());
  c_->broker->every(s);
}

std::unique_ptr<live::Mirror> LiveHub::mirror() const {
  assert(connected());
  return c_->broker->mirror();
}

std::vector<std::string> LiveHub::lists() const { return live::rebal::lists(); }

void LiveHub::set_list(const std::string &name) {
  std::vector<std::string> codes;
  std::string err = live::guard([&] { codes = live::rebal::read_list(name); });
  edit(*c_, &Core::in, [&](Inputs &x) {
    x.list.name = name;
    x.list.codes = std::move(codes);
    x.list.err = std::move(err);
    derive(x);
  });
}

void LiveHub::edit_cfg(const std::function<void(live::rebal::Cfg &)> &f) {
  edit(*c_, &Core::in, [&](Inputs &x) {
    f(x.cfg);
    assert(live::rebal::valid(x.cfg) && x.cfg.unit >= 0 && x.cfg.pct >= 0 && x.cfg.pct <= 100);
  });
}

void LiveHub::submit(const std::string &code, int side, double size, live::Credit credit) {
  assert(size > 0);
  const live::Sec *sec = snap_ ? snap_->pos.find(code) : nullptr;
  if (!sec && snap_)
    sec = snap_->fills.by_code.find(code);
  if (!sec) {
    const std::vector<live::Sec> &buy = in_->list.buy;
    const auto i = std::find_if(buy.begin(), buy.end(), [&](const live::Sec &s) { return s.code == code; });
    assert(i != buy.end() && side == live::kBuy);
    sec = &*i;
  }
  const live::OrderReq o{*sec, side, credit};
  start(c_, "下单", [o, size, mode = in_->cfg.px](Core &c) {
    Books books(std::vector<live::Sec>{});
    const OrderJob j = exec(c, books, o, size, mode, "");
    live::need(j.st == JobSt::Ok, failed(j));
  });
}

void LiveHub::pull(Pull p) {
  assert(connected());
  start(c_, spec(p).name, [p](Core &c) { pull_run(c, p); });
}

void LiveHub::run() {
  assert(connected());
  start(c_, "执行调仓", rebal_task);
}

void LiveHub::repay(double cap) {
  assert(connected() && cap > 0);
  start(c_, "现金还款", [cap](Core &c) { repay_task(c, cap); });
}

} // namespace GUI::Live
