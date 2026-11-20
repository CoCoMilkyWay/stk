#include "gui/task_live/LiveHub.hpp"

#include "account/Ledger.hpp"
#include "api/sina/client.hpp"
#include "disk/Disk.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "shared/AssetInfo.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_set>

namespace GUI::Live {

using live::strf;

struct LiveHub::Core {
  std::unique_ptr<live::Broker> broker; // connect 时建一次, 之后只读

  // mu: 三份快照 (改 = 抄一份改了换上) 与事务日志的位置
  std::mutex mu;
  std::shared_ptr<const Inputs> in;
  std::shared_ptr<const Task> task;
  std::shared_ptr<const std::vector<OrderJob>> jobs = std::make_shared<const std::vector<OrderJob>>();
  misc::Ymd flows_pulled = 0; // 资金流水账本拉于
  std::string task_stamp;
  double task_t0 = 0;
};

namespace {

using Core = LiveHub::Core;
using Lock = std::lock_guard<std::mutex>;

const nlohmann::json kNone = nlohmann::json::object();

double mono() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

// 改核心的一份快照 m: 抄一份, f 改了换上
template <class T, class F>
void edit(Core &c, std::shared_ptr<const T> Core::*m, F &&f) {
  Lock lk(c.mu);
  auto x = std::make_shared<T>(*(c.*m));
  f(*x);
  c.*m = std::move(x);
}
std::shared_ptr<const Inputs> inputs(Core &c) {
  Lock lk(c.mu);
  return c.in;
}

// 由 raw / local / codes 派生的: 合并的条款、名单里可买的
void derive(Inputs &x) {
  x.terms = x.raw ? std::make_shared<const live::Terms>(live::terms::merged(x.raw->terms, *x.local)) : nullptr;
  x.buy = x.terms ? live::rebal::buyable(x.codes, *x.terms) : std::vector<live::Sec>{};
}

// ---------- 行情 ----------

// 持仓、今日成交、负债、名单里可买的, 按代码去重
std::vector<live::Sec> watch_set(const live::State &st, const Inputs &in) {
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
  for (const live::Sec &s : in.buy)
    add(s);
  return v;
}

// 账户各只 (持仓 / 负债) 在 q 里的现价; 缺一只 → 空
std::shared_ptr<const live::rebal::Prices> prices_of(const live::Account &a, const live::Quotes &q) {
  auto px = std::make_shared<live::rebal::Prices>();
  const auto take = [&](const std::string &code) {
    const auto i = q.find(code);
    if (i != q.end())
      (*px)[code] = i->second.price;
    return i != q.end();
  };
  const bool all = std::all_of(a.held->list.begin(), a.held->list.end(), [&](const live::Position &p) { return take(p.code); }) &&
                   std::all_of(a.debts.list.begin(), a.debts.list.end(), [&](const live::Debt &d) { return take(d.code); });
  return all ? px : nullptr;
}

// 行情 q 整份换上; 账户是新读到的且其各只都有了即记作读时现价
void take(Core &c, const live::State &st, std::shared_ptr<const live::Quotes> q) {
  std::string at;
  for (const auto &[code, x] : *q)
    at = std::max(at, live::quote_time(x));
  std::shared_ptr<const live::rebal::Prices> px = st.acct ? prices_of(*st.acct, *q) : nullptr;
  edit(c, &Core::in, [&](Inputs &x) {
    x.quotes = std::move(q);
    x.quote_at = at;
    if (px && st.acct.v != x.px_acct) {
      x.px_acct = st.acct.v;
      x.px0 = std::move(px);
    }
  });
}

// 现取 secs 的行情 (事务要此刻的价, 不等订阅那一路) 换上
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

// 下单用的盘口: 批量 = 后台一条常驻连接每 kBookGap 秒取一遍这批各只, 取用最新的一份; 收到已超 kBookStale 秒或还没这只 → 当场现取.
// 单笔 (secs 空) = 每次现取. 后台线程持 Feed 的 shared_ptr, 析构只置停 (在途的一问做完即退)
class Books {
public:
  struct Book {
    live::Quote q;
    double age = 0; // 收到距今 [s]; 现取 = 0
    bool fed = false;
  };
  explicit Books(const std::vector<live::Sec> &secs) {
    std::vector<std::string> syms;
    for (const live::Sec &s : secs)
      syms.push_back(live::sina_sym(s.mkt, s.code));
    std::sort(syms.begin(), syms.end());
    syms.erase(std::unique(syms.begin(), syms.end()), syms.end());
    assert(syms.size() <= static_cast<size_t>(sina::kQuoteMax) && "一批下单的码超出一次快照");
    if (syms.empty())
      return;
    std::thread([f = feed_, syms = std::move(syms)] {
      sina::Client cl(sina::kQuoteHost);
      std::unique_lock lk(f->mu);
      while (!f->stop) {
        lk.unlock();
        live::Quotes q;
        for (const sina::Quote &x : cl.quotes(syms))
          q[x.sym.substr(2)] = live::quote_of(x);
        lk.lock();
        f->q = std::move(q);
        f->at = mono();
        f->cv.wait_for(lk, std::chrono::duration<double>(kBookGap), [&] { return f->stop; });
      }
    }).detach();
  }
  ~Books() {
    {
      Lock lk(feed_->mu);
      feed_->stop = true;
    }
    feed_->cv.notify_all();
  }
  Books(const Books &) = delete;
  Books &operator=(const Books &) = delete;

  Book get(const live::Sec &s) {
    {
      Lock lk(feed_->mu);
      const auto i = feed_->q.find(s.code);
      if (const double age = mono() - feed_->at; i != feed_->q.end() && age <= kBookStale)
        return {i->second, age, true};
    }
    if (!cl_)
      cl_.emplace(sina::kQuoteHost);
    const std::vector<sina::Quote> got = cl_->quotes({live::sina_sym(s.mkt, s.code)});
    live::need(!got.empty(), "新浪查无此码");
    return {live::quote_of(got[0])};
  }

private:
  static constexpr double kBookGap = 0.5, kBookStale = 5; // [s]
  struct Feed {
    std::mutex mu;
    std::condition_variable cv;
    bool stop = false;
    live::Quotes q;
    double at = 0; // 收到于 (mono)
  };
  std::shared_ptr<Feed> feed_ = std::make_shared<Feed>();
  std::optional<sina::Client> cl_; // 现取的连接 (头一回现取才建)
};

// ---------- 事务 ----------

// 记一条: 落盘字段, 页面留文字
void note(Core &c, const char *kind, std::string text, const nlohmann::json &kw) {
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
  edit(c, &Core::task, [&](Task &x) {
    x.log.push_back({t, kind, std::move(text)});
    if (x.log.size() > kLogTail)
      x.log.pop_front();
  });
}
// 事务走到哪一步
void stage(Core &c, std::string s) {
  edit(c, &Core::task, [&](Task &x) { x.step = std::move(s); });
}
// 事务的报告 (交给 broker 的长操作): 记一条 / 实时进度 / 走到哪一步
live::Report report_of(Core &c) {
  return {[&c](const char *k, std::string text, nlohmann::json kw) { note(c, k, std::move(text), kw); },
          [&c](std::string line) { edit(c, &Core::task, [&](Task &x) { x.live = std::move(line); }); },
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
std::string quote_text(const live::Quote &q) {
  const auto top = [](const std::vector<live::Level> &side) { return side.empty() ? std::string("-") : strf("%.3f", side[0].px); };
  return strf("行情 %s 现价 %.3f 昨收 %.3f · 卖一 %s 买一 %s%s", live::quote_time(q).c_str(), q.price, q.last_close, top(q.book(live::kBuy)).c_str(),
              top(q.book(live::kSell)).c_str(), live::auction(q) ? " · 竞价中" : "");
}

// 事务里下一单: 「行情」从 books 取盘口记一条、按 mode 定委托价, 再交 broker 下; o 的量为 qty 股或 size 元 (恰给其一).
// 本单进最近几单之首, 各步记进本单与事务的步骤 (前缀 at, 如「卖出 2/5」); 请求与结局各记一条, 成败看 st (不抛)
OrderJob exec(Core &c, Books &books, live::OrderReq o, std::string label, live::PxMode mode, const std::string &at) {
  const char *px_name = live::kPxModeNames[static_cast<int>(mode)];
  note(c, "order", strf("%s %s %s %s %s · %s", live::side_name(o.side), live::credit_name(o.credit), o.sec.code.c_str(), o.sec.name.c_str(), label.c_str(), px_name),
       {{"order", live::to_json(o)}, {"mode", px_name}});
  OrderJob j{.t = live::clock_now(), .o = o, .label = std::move(label), .steps = {"行情"}};
  for (const std::string &s : c.broker->steps())
    j.steps.push_back(s);
  edit(c, &Core::jobs, [&](std::vector<OrderJob> &v) {
    v.insert(v.begin(), std::move(j));
    if (v.size() > kOrderCap)
      v.pop_back();
  });
  const auto put = [&](const auto &f) {
    OrderJob out;
    edit(c, &Core::jobs, [&](std::vector<OrderJob> &v) {
      f(v.front());
      out = v.front();
    });
    return out;
  };
  const auto step = [&](size_t i) {
    const OrderJob x = put([&](OrderJob &y) { y.at = i; });
    stage(c, at.empty() ? x.steps[i] : at + " " + x.steps[i]);
  };
  OrderJob out;
  try {
    step(0);
    const Books::Book b = books.get(o.sec);
    const live::Quote &q = b.q;
    note(c, "quote", quote_text(q) + (b.fed ? strf(" · 流 %.1fs 前", b.age) : " · 现取"), {{"quote", quote_json(q)}, {"fed", b.fed}, {"age", std::round(b.age * 100) / 100}});
    const std::optional<double> r = live::ref(q);
    live::need(r.has_value(), "无参考价");
    const std::optional<double> px = live::order_px(q, o.side, mode, o.size > 0 ? o.size : o.qty * *r);
    live::need(px.has_value(), "盘口为空 (竞价还没撮合出价 / 停牌), 定不了委托价");
    o.px = *px;
    put([&](OrderJob &y) { y.o = o; });
    step(1);
    const live::Placed p = c.broker->place(o, [&](size_t i, const std::string &d) {
      step(1 + i);
      if (!d.empty())
        put([&](OrderJob &y) { y.detail = d; });
    });
    out = put([&](OrderJob &y) {
      y.st = JobSt::Ok;
      y.got = p;
    });
  } catch (const live::Fail &e) {
    out = put([&](OrderJob &y) {
      y.st = JobSt::Err;
      y.got.msg = e.why;
    });
  }
  note(c, "job", strf("%s %s %s", job_name(out.st), out.detail.c_str(), out.got.msg.c_str()),
       {{"job", {{"state", job_name(out.st)}, {"detail", out.detail}, {"msg", out.got.msg}, {"qty", out.got.qty}, {"px", out.got.px}}}});
  return out;
}
std::string failed(const OrderJob &j) { return strf("%s %s 下单失败: %s", j.o.sec.code.c_str(), j.o.sec.name.c_str(), j.got.msg.c_str()); }

// 各件事务的身子; 两样拉取同形: 拉 → 存, 再换上
void terms_task(Core &c) {
  auto x = std::make_shared<const live::terms::Cache>(live::terms::pull(*c.broker, report_of(c)));
  edit(c, &Core::in, [&](Inputs &in) {
    in.raw = std::move(x);
    derive(in);
  });
}
void flows_task(Core &c) {
  const misc::Ymd d = account::pull(*c.broker, report_of(c)).pulled;
  Lock lk(c.mu);
  c.flows_pulled = d;
}

// 刷新一轮 (成交、持仓、挂单、信用账户), 记下账户
std::shared_ptr<const live::State> sync(Core &c) {
  stage(c, "刷新");
  c.broker->sync();
  auto st = c.broker->state();
  assert(st->pos && st->fills && st->orders && st->acct);
  const live::Account &a = *st->acct;
  nlohmann::json debts = nlohmann::json::object();
  for (const live::Debt &d : a.debts.list)
    debts[d.code] = {d.qty, d.amount};
  note(c, "acct", strf("账户: 现金 %s · 可用保证金 %s · 负债 %s", wan(a.cash).c_str(), wan(a.avail).c_str(), wan(a.debt).c_str()),
       {{"acct", {{"at", st->acct.at}, {"assets", a.assets}, {"line", a.line}, {"debt", a.debt}, {"net", a.net}, {"avail", a.avail}, {"cash", a.cash}, {"debts", debts}}}});
  return st;
}

live::rebal::Plan plan_now(Core &c, sina::Client &cl) {
  stage(c, "试算");
  const auto st = c.broker->state();
  fetch(c, cl, *st, watch_set(*st, *inputs(c)));
  const std::shared_ptr<const Inputs> in = inputs(c);
  live::need(in->terms != nullptr, "两融条款还没拉过");
  live::need(in->px_acct == st->acct.v, "账户各只的行情没取齐");
  const std::optional<live::rebal::Plan> p = live::rebal::plan(*st->pos, *st->acct, *in->px0, *in->quotes, *in->terms, in->codes, in->cfg);
  live::need(p.has_value(), "新浪没给齐要的行情");
  note(c, "plan", strf("试算: 行使 %s (融资 %s) · %zu 单 · 成本 %.0f", wan(p->use).c_str(), wan(p->financed).c_str(), p->orders.size(), p->cost),
       {{"cfg", live::rebal::to_json(in->cfg)}, {"codes", in->codes}, {"plan", live::rebal::to_json(*p)}});
  return *p;
}

// 批量下单期间 broker 不自起刷新 (不扫客户端), 出作用域即恢复
struct Hush {
  live::Broker &b;
  explicit Hush(live::Broker &x) : b(x) { b.hush(true); }
  ~Hush() { b.hush(false); }
  Hush(const Hush &) = delete;
  Hush &operator=(const Hush &) = delete;
};

// 执行调仓. 要紧处:
//   · 试算现做一遍 (刷新后的账户 + 此刻行情), 页面上的那份只作预览; 各单的股数照它, 委托价下单时按新取的盘口再定
//   · 一口气下完, 不等成交: 卖出回款 / 腾出的保证金在卖单成交前还没到账, 紧跟着的融资买单由闪电窗的最大可买封顶 (少买的事后人工补);
//     不等是有意的 —— 等成交会在半路卡住 (卖了一半没买), 留下方向与价格敞口
//   · 一单失败只记下、接着下后面的, 末了一并报失败 (该补哪几单)
//   · 下完即清掉卖出设置: 再点执行不会重复卖
void rebal_task(Core &c) {
  sina::Client cl(sina::kQuoteHost);
  if (!inputs(c)->raw)
    terms_task(c);
  const auto st = sync(c);
  if (!st->orders->list.empty()) {
    std::string s;
    for (const live::Pending &r : st->orders->list)
      s += strf("%s%s %s %.0f股", s.empty() ? "" : "、", r.code.c_str(), r.name.c_str(), r.qty - r.filled);
    live::fail(strf("还有 %zu 笔未成交委托 (%s), 撤掉或等成交后再调仓", st->orders->list.size(), s.c_str()));
  }
  const live::PxMode mode = inputs(c)->cfg.px;
  const live::rebal::Plan p = plan_now(c, cl);
  std::vector<std::string> fails; // 没下成的单: 记下接着下后面的, 末了一并报
  {
    const Hush hush(*c.broker);
    Books books(std::vector<live::Sec>(p.orders.begin(), p.orders.end()));
    for (size_t i = 0; i < p.orders.size(); ++i) {
      const live::rebal::Order &o = p.orders[i];
      const OrderJob j = exec(c, books, {o, o.side, o.credit, 0, o.qty, 0}, strf("%.0f股", o.qty), mode,
                              strf("%s %zu/%zu", live::side_name(o.side), i + 1, p.orders.size()));
      if (j.st != JobSt::Ok)
        fails.push_back(failed(j));
    }
  }
  sync(c);
  edit(c, &Core::in, [](Inputs &x) { x.cfg.sells.clear(); });
  if (!fails.empty()) {
    std::string s;
    for (const std::string &f : fails)
      s += (s.empty() ? "" : "; ") + f;
    live::fail(strf("%zu 单没下成, 其余照下: %s", fails.size(), s.c_str()));
  }
}

// 后台起一件事务; 已有事务在跑 → 不收, false
bool start(const std::shared_ptr<Core> &c, const char *name, std::function<void(Core &)> fn) {
  {
    Lock lk(c->mu);
    if (c->task && c->task->st == JobSt::Run)
      return false;
    c->task_stamp = disk::stamp_now();
    c->task_t0 = mono();
    c->task = std::make_shared<const Task>(Task{.name = name, .t = live::clock_now(), .file = disk::path(disk::RunLiveTaskLog, {.stamp = c->task_stamp}).string()});
  }
  const std::vector<disk::Key> old = disk::list(disk::RunLiveTaskLog);
  for (size_t i = 0; i + kTaskKeep <= old.size(); ++i)
    disk::remove(disk::RunLiveTaskLog, old[i]);
  std::thread([c, fn = std::move(fn)] {
    JobSt st = JobSt::Ok;
    std::string msg;
    try {
      fn(*c);
      note(*c, "ok", "", kNone);
    } catch (const live::Fail &e) {
      st = JobSt::Err;
      msg = e.why;
      note(*c, "err", e.why, {{"err", e.why}});
    }
    edit(*c, &Core::task, [&](Task &t) {
      t.st = st;
      t.msg = std::move(msg);
      t.live.clear();
    });
  }).detach();
  return true;
}

} // namespace

LiveHub::LiveHub() : c_(std::make_shared<Core>()) {
  auto in = std::make_shared<Inputs>();
  if (live::terms::Cache x = live::terms::load(); x.pulled)
    in->raw = std::make_shared<const live::terms::Cache>(std::move(x));
  in->local = std::make_shared<const live::terms::Local>();
  derive(*in);
  c_->in = std::move(in);
  c_->flows_pulled = account::load().pulled;
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
  peek();
}

const live::Client &LiveHub::client() const {
  assert(connected());
  return *client_;
}

live::Broker &LiveHub::broker() {
  assert(connected());
  return *c_->broker;
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
  if (c_->broker) {
    st_ = c_->broker->state();
    prog_ = c_->broker->refresh();
  }
}

void LiveHub::auto_pull() {
  if (!connected() || !st_ || !st_->acct)
    return;
  const misc::Ymd d = misc::today_ymd();
  const Inputs &x = *in_;
  bool flows;
  {
    Lock lk(c_->mu);
    flows = c_->flows_pulled < d;
  }
  if (tried_[0] < d && (!x.raw || misc::add_days(x.raw->pulled, kTermsAge) <= d || live::terms::missing(x.raw->terms, *x.local, x.codes, *st_))) {
    if (start(c_, "更新两融名单", terms_task))
      tried_[0] = d;
  } else if (tried_[1] < d && flows) {
    if (start(c_, "更新资金流水", flows_task))
      tried_[1] = d;
  }
}

void LiveHub::tick(const AssetInfo &info, Quote::QuoteHub &quote) {
  Core &c = *c_;
  if (const std::array<int64_t, 3> ik = {info.pit_end, info.bar1m_end, static_cast<int64_t>(info.codes.size())}; ik != info_key_) {
    info_key_ = ik;
    auto l = std::make_shared<const live::terms::Local>(live::terms::local(info));
    edit(c, &Core::in, [&](Inputs &x) {
      x.local = std::move(l);
      derive(x);
    });
  }
  peek();
  if (!c.broker) {
    board_ = BoardView{.wait = "未连接"};
    plan_ = PlanView{.wait = "未连接"};
    return;
  }
  c.broker->touch();
  if (quote.started()) {
    std::vector<std::string> syms;
    for (const live::Sec &s : watch_set(*st_, *in_))
      syms.push_back(live::sina_sym(s.mkt, s.code));
    if (syms != watch_)
      quote.watch(watch_ = std::move(syms));
    if (const std::shared_ptr<const Quote::Live> &y = quote.live; y && y->watch && y->ver != live_ver_) {
      live_ver_ = y->ver;
      auto q = std::make_shared<live::Quotes>();
      for (const sina::Quote &x : *y->watch)
        (*q)[x.sym.substr(2)] = live::quote_of(x);
      take(c, *st_, std::move(q));
      in_ = inputs(c);
    }
  }
  const live::State &st = *st_;
  const Inputs &in = *in_;

  if (board_key_[0] != st_ || board_key_[1] != in.quotes) {
    board_key_[0] = st_, board_key_[1] = in.quotes;
    BoardView b{.st = st_, .quotes = in.quotes};
    try {
      if (!st.pos || !st.fills)
        b.wait = "还没读到持仓与成交 (等刷新)";
      else if (!in.quotes || !(b.frame = live::board::view(*st.pos, *st.fills, *in.quotes)))
        b.wait = "等行情";
    } catch (const live::Fail &e) {
      b.err = e.why;
    }
    if (b.wait.empty() || !board_.frame)
      board_ = std::move(b);
    else
      board_.wait = std::move(b.wait);
  }

  if (plan_key_[0] != st_ || plan_key_[1] != in_) {
    plan_key_[0] = st_, plan_key_[1] = in_;
    PlanView p;
    if (!in.terms)
      p.wait = "两融条款还没拉过 (更新两融名单)";
    else if (!st.pos || !st.acct)
      p.wait = "还没读到信用账户 (等刷新)";
    else if (in.px_acct != st.acct.v || !in.quotes)
      p.wait = "等账户各只的行情";
    else {
      try {
        if (!(p.plan = live::rebal::plan(*st.pos, *st.acct, *in.px0, *in.quotes, *in.terms, in.codes, in.cfg)))
          p.wait = "等行情";
      } catch (const live::Fail &e) {
        p.err = e.why;
      }
    }
    if (p.plan)
      p.now = p.plan->now;
    else if (st.acct)
      p.now = live::rebal::now_of(*st.acct);
    if (p.wait.empty() || !plan_.plan)
      plan_ = std::move(p);
    else
      plan_.wait = std::move(p.wait);
  }
}

std::vector<std::string> LiveHub::lists() const { return live::rebal::lists(); }

void LiveHub::set_list(const std::string &name) {
  std::vector<std::string> codes;
  std::string err;
  try {
    codes = live::rebal::read_list(name);
  } catch (const live::Fail &e) {
    err = e.why;
  }
  edit(*c_, &Core::in, [&](Inputs &x) {
    x.cfg.list = name;
    x.codes = std::move(codes);
    x.list_err = std::move(err);
    derive(x);
  });
}

void LiveHub::edit_cfg(const std::function<void(live::rebal::Cfg &)> &f) {
  edit(*c_, &Core::in, [&](Inputs &x) {
    const std::string list = x.cfg.list;
    f(x.cfg);
    assert(x.cfg.list == list && "名单经 set_list");
    assert(x.cfg.unit >= 0 && x.cfg.pct >= 0 && x.cfg.pct <= 100);
  });
}

void LiveHub::submit(const std::string &code, int side, double size, live::Credit credit) {
  assert(size > 0);
  const live::Sec *sec = st_->pos ? st_->pos->find(code) : nullptr;
  if (!sec && st_->fills)
    sec = st_->fills->by_code.find(code);
  if (!sec) {
    const auto i = std::find_if(in_->buy.begin(), in_->buy.end(), [&](const live::Sec &s) { return s.code == code; });
    assert(i != in_->buy.end() && side == live::kBuy);
    sec = &*i;
  }
  const live::OrderReq o{*sec, side, credit, 0, 0, size};
  start(c_, "下单", [o, mode = in_->cfg.px](Core &c) {
    Books books(std::vector<live::Sec>{});
    const OrderJob j = exec(c, books, o, strf("%gW", o.size / 1e4), mode, "");
    live::need(j.st == JobSt::Ok, failed(j));
  });
}

void LiveHub::pull_terms() {
  assert(connected());
  start(c_, "更新两融名单", terms_task);
}

void LiveHub::run() {
  assert(connected());
  start(c_, "执行调仓", rebal_task);
}

} // namespace GUI::Live
