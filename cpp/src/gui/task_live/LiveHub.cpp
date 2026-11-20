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
#include <type_traits>
#include <unordered_set>

namespace GUI::Live {

using live::strf;

struct LiveHub::Core {
  std::unique_ptr<live::Broker> broker;
  live::trace::Track track;

  std::mutex mu;
  std::shared_ptr<const Inputs> in;
  std::shared_ptr<const Task> task;
  std::shared_ptr<const std::vector<OrderJob>> jobs = std::make_shared<const std::vector<OrderJob>>();
  misc::Ymd flows_pulled = 0;
  double task_t0 = 0;
  bool in_order = false;    // 当前正在跑一笔下单 (用于把 Span/live 同步进 jobs.front())
  std::string order_prefix; // 批量下单时当前单前缀 (如 "卖出 2/5")
};

namespace {

using Core = LiveHub::Core;
using Lock = std::lock_guard<std::mutex>;

double mono() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

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

void derive(Inputs &x) {
  x.terms = x.raw ? std::make_shared<const live::Terms>(live::terms::merged(x.raw->terms, *x.local)) : nullptr;
  x.buy = x.terms ? live::rebal::buyable(x.codes, *x.terms) : std::vector<live::Sec>{};
}

// ---------- 行情 ----------

std::vector<live::Sec> watch_set(const live::Snapshot *snap, const Inputs &in) {
  std::vector<live::Sec> v;
  std::unordered_set<std::string> seen;
  const auto add = [&](const live::Sec &s) {
    if (seen.insert(s.code).second)
      v.push_back(s);
  };
  if (snap) {
    for (const live::Position &p : snap->pos.list)
      add(p);
    for (const live::Traded &f : snap->fills.by_code.list)
      add(f);
    for (const live::Debt &d : snap->acct.debts.list)
      add(d);
  }
  for (const live::Sec &s : in.buy)
    add(s);
  return v;
}

std::shared_ptr<const live::rebal::Prices> prices_of(const live::Snapshot &s, const live::Quotes &q) {
  auto px = std::make_shared<live::rebal::Prices>();
  const auto take = [&](const std::string &code) {
    const auto i = q.find(code);
    if (i != q.end())
      (*px)[code] = i->second.price;
    return i != q.end();
  };
  const bool all = std::all_of(s.pos.list.begin(), s.pos.list.end(), [&](const live::Position &p) { return take(p.code); }) &&
                   std::all_of(s.acct.debts.list.begin(), s.acct.debts.list.end(), [&](const live::Debt &d) { return take(d.code); });
  return all ? px : nullptr;
}

void take(Core &c, const std::shared_ptr<const live::Snapshot> &snap, std::shared_ptr<const live::Quotes> q) {
  int sec = -1;
  for (const auto &[code, x] : *q)
    sec = std::max(sec, x.sec);
  std::shared_ptr<const live::rebal::Prices> px = snap ? prices_of(*snap, *q) : nullptr;
  edit(c, &Core::in, [&](Inputs &x) {
    x.quotes = std::move(q);
    x.quote_sec = sec;
    if (px && snap != x.px_snap) {
      x.px_snap = snap;
      x.px0 = std::move(px);
    }
  });
}

void fetch(Core &c, sina::Client &cl, const std::shared_ptr<const live::Snapshot> &snap, const std::vector<live::Sec> &secs) {
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
  take(c, snap, std::move(q));
}

// 下单盘口源: 批量 = 后台 0.5s 轮询流, 断流 > 5s 或单笔则现取
class Books {
public:
  struct Book {
    live::Quote q;
    double age = 0;
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
  static constexpr double kBookGap = 0.5, kBookStale = 5;
  struct Feed {
    std::mutex mu;
    std::condition_variable cv;
    bool stop = false;
    live::Quotes q;
    double at = 0;
  };
  std::shared_ptr<Feed> feed_ = std::make_shared<Feed>();
  std::optional<sina::Client> cl_;
};

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

OrderJob exec(Core &c, Books &books, live::OrderReq o, std::string label, live::PxMode mode, const std::string &at) {
  const char *px_name = live::kPxModeNames[static_cast<int>(mode)];
  live::trace::note("order", strf("%s %s %s %s %s · %s", live::side_name(o.side), live::credit_name(o.credit), o.sec.code.c_str(), o.sec.name.c_str(), label.c_str(), px_name),
                    {{"order", live::to_json(o)}, {"mode", px_name}});

  const auto bs = c.broker->steps();
  OrderJob j{.sec = live::sec_now(), .o = o, .label = std::move(label), .steps = {"行情"}};
  for (const std::string_view s : bs)
    j.steps.emplace_back(s);
  {
    Lock lk(c.mu);
    auto v = std::make_shared<std::vector<OrderJob>>(*c.jobs);
    v->insert(v->begin(), std::move(j));
    if (v->size() > kOrderCap)
      v->pop_back();
    c.jobs = std::move(v);
    c.in_order = true;
    c.order_prefix = at;
  }

  const auto put = [&](const auto &f) {
    OrderJob out;
    edit(c, &Core::jobs, [&](std::vector<OrderJob> &v) {
      f(v.front());
      out = v.front();
    });
    return out;
  };

  OrderJob out;
  try {
    {
      live::trace::Span sp("行情");
      const Books::Book b = books.get(o.sec);
      const live::Quote &q = b.q;
      live::trace::note("quote", quote_text(q) + (b.fed ? strf(" · 流 %.1fs 前", b.age) : " · 现取"),
                        {{"quote", quote_json(q)}, {"fed", b.fed}, {"age", std::round(b.age * 100) / 100}});
      const std::optional<double> ref = live::ref(q);
      live::need(ref.has_value(), "无参考价");
      const double notional = o.qty.is_shares() ? o.qty.v * *ref : o.qty.v;
      const std::optional<double> px = live::order_px(q, o.side, mode, notional);
      live::need(px.has_value(), "盘口为空 (竞价还没撮合出价 / 停牌), 定不了委托价");
      o.px = *px;
      put([&](OrderJob &y) { y.o = o; });
    }
    const live::Placed p = c.broker->place(o);
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
  {
    Lock lk(c.mu);
    c.in_order = false;
    c.order_prefix.clear();
  }
  live::trace::note("job", strf("%s %s %s", job_name(out.st), out.detail.c_str(), out.got.msg.c_str()),
                    {{"job", {{"state", job_name(out.st)}, {"detail", out.detail}, {"msg", out.got.msg}, {"qty", out.got.qty}, {"px", out.got.px}}}});
  return out;
}

std::string failed(const OrderJob &j) { return strf("%s %s 下单失败: %s", j.o.sec.code.c_str(), j.o.sec.name.c_str(), j.got.msg.c_str()); }

void terms_task(Core &c) {
  auto x = std::make_shared<const live::terms::Cache>(live::terms::pull(*c.broker));
  edit(c, &Core::in, [&](Inputs &in) {
    in.raw = std::move(x);
    derive(in);
  });
}

void flows_task(Core &c) {
  const misc::Ymd d = account::pull(*c.broker).pulled;
  Lock lk(c.mu);
  c.flows_pulled = d;
}

std::shared_ptr<const live::Snapshot> sync(Core &c) {
  live::trace::Span sp("刷新");
  c.broker->sync();
  auto snap = c.broker->snapshot();
  assert(snap);
  const live::Account &a = snap->acct;
  nlohmann::json debts = nlohmann::json::object();
  for (const live::Debt &d : a.debts.list)
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
  live::need(in->terms != nullptr, "两融条款还没拉过");
  live::need(in->px_snap == snap, "账户各只的行情没取齐");
  const std::optional<live::rebal::Plan> p = live::rebal::plan(*snap, *in->px0, *in->quotes, *in->terms, in->codes, in->cfg);
  live::need(p.has_value(), "新浪没给齐要的行情");
  live::trace::note("plan", strf("试算: 行使 %s (融资 %s) · %zu 单 · 成本 %.0f", wan(p->use).c_str(), wan(p->financed).c_str(), p->orders.size(), p->cost),
                    {{"cfg", live::rebal::to_json(in->cfg)}, {"codes", in->codes}, {"plan", live::rebal::to_json(*p)}});
  return *p;
}

void rebal_task(Core &c) {
  sina::Client cl(sina::kQuoteHost);
  if (!inputs(c)->raw)
    terms_task(c);
  const auto snap = sync(c);
  if (!snap->pends.list.empty()) {
    std::string s;
    for (const live::Pending &r : snap->pends.list)
      s += strf("%s%s %s %lld股", s.empty() ? "" : "、", r.code.c_str(), r.name.c_str(), static_cast<long long>(r.qty - r.filled));
    live::fail(strf("还有 %zu 笔未成交委托 (%s), 撤掉或等成交后再调仓", snap->pends.list.size(), s.c_str()));
  }
  const live::PxMode mode = inputs(c)->cfg.px;
  const live::rebal::Plan p = plan_now(c, cl);
  std::vector<std::string> fails;
  c.broker->batch([&] {
    Books books(std::vector<live::Sec>(p.orders.begin(), p.orders.end()));
    for (size_t i = 0; i < p.orders.size(); ++i) {
      const live::rebal::Order &o = p.orders[i];
      const OrderJob j = exec(c, books, {o, o.side, o.credit, 0, live::Qty::shares(o.qty)}, strf("%lld股", static_cast<long long>(o.qty)), mode,
                              strf("%s %zu/%zu", live::side_name(o.side), i + 1, p.orders.size()));
      if (j.st != JobSt::Ok)
        fails.push_back(failed(j));
    }
  });
  sync(c);
  edit(c, &Core::in, [](Inputs &x) { x.cfg.sells.clear(); });
  if (!fails.empty()) {
    std::string s;
    for (const std::string &f : fails)
      s += (s.empty() ? "" : "; ") + f;
    live::fail(strf("%zu 单没下成, 其余照下: %s", fails.size(), s.c_str()));
  }
}

// 后台起一件事务: 统一挂载 live::trace::Watch + Hooks, 零双写
bool start(const std::shared_ptr<Core> &c, const char *name, std::function<void(Core &)> fn) {
  {
    Lock lk(c->mu);
    if (c->task && c->task->st == JobSt::Run)
      return false;
    c->task_t0 = mono();
    c->task = std::make_shared<const Task>(Task{.name = name, .sec = live::sec_now(), .file = live::trace::file()});
  }
  std::thread([c, name = std::string(name), fn = std::move(fn)] {
    const live::trace::Hooks hooks{
        .on_note = [&](const char *kind, std::string text) {
          const double t = mono() - c->task_t0;
          const std::string f = live::trace::file();
          edit(*c, &Core::task, [&](Task &x) {
            if (x.file.empty())
              x.file = f;
            x.log.push_back({t, kind, std::move(text)});
            if (x.log.size() > kLogTail)
              x.log.pop_front();
          }); },
        .on_span = [&](std::string_view s) {
          bool in_ord = false;
          std::string pref;
          {
            Lock lk(c->mu);
            in_ord = c->in_order;
            pref = c->order_prefix;
          }
          if (in_ord) {
            edit(*c, &Core::jobs, [&](std::vector<OrderJob> &v) {
              if (v.empty())
                return;
              const auto it = std::find(v.front().steps.begin(), v.front().steps.end(), s);
              if (it != v.front().steps.end())
                v.front().at = static_cast<size_t>(it - v.front().steps.begin());
            });
          }
          std::string step_txt = pref.empty() ? std::string(s) : pref + " " + std::string(s);
          edit(*c, &Core::task, [&](Task &x) { x.step = std::move(step_txt); }); },
        .on_live = [&](std::string line) {
          bool in_ord = false;
          {
            Lock lk(c->mu);
            in_ord = c->in_order;
          }
          if (in_ord)
            edit(*c, &Core::jobs, [&](std::vector<OrderJob> &v) {
              if (!v.empty())
                v.front().detail = line;
            });
          edit(*c, &Core::task, [&](Task &x) { x.live = std::move(line); }); },
    };
    const live::trace::Watch watch(c->track, &hooks);
    live::trace::Span sp(name);
    JobSt st = JobSt::Ok;
    std::string msg;
    try {
      fn(*c);
      live::trace::note("ok", "");
    } catch (const live::Fail &e) {
      st = JobSt::Err;
      msg = e.why;
      live::trace::note("err", e.why, {{"err", e.why}});
    }
    const std::string f = live::trace::file();
    edit(*c, &Core::task, [&](Task &t) {
      if (t.file.empty())
        t.file = f;
      t.st = st;
      t.msg = std::move(msg);
      t.live.clear();
    });
  }).detach();
  return true;
}

struct Pull {
  const char *name;
  void (*run)(Core &);
};
constexpr Pull kPulls[] = {{"更新两融名单", terms_task}, {"更新资金流水", flows_task}};

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
  task_at_ = c_->track.now();
  if (c_->broker) {
    snap_ = c_->broker->snapshot();
    prog_ = c_->broker->refresh();
  }
}

void LiveHub::auto_pull() {
  if (!connected() || !snap_)
    return;
  const misc::Ymd d = misc::today_ymd();
  const Inputs &x = *in_;
  bool flows;
  {
    Lock lk(c_->mu);
    flows = c_->flows_pulled < d;
  }
  const bool due[] = {tried_[0] < d && (!x.raw || misc::add_days(x.raw->pulled, kTermsAge) <= d || live::terms::missing(x.raw->terms, *x.local, x.codes, snap_.get())),
                      tried_[1] < d && flows};
  static_assert(std::extent_v<decltype(due)> == std::size(kPulls));
  for (size_t i = 0; i < std::size(kPulls); ++i)
    if (due[i]) {
      if (start(c_, kPulls[i].name, kPulls[i].run))
        tried_[i] = d;
      return;
    }
}

void LiveHub::tick(const AssetInfo &info, Quote::QuoteHub &quote) {
  Core &c = *c_;
  info_st_.update({info.pit_end, info.bar1m_end, static_cast<int64_t>(info.codes.size())}, [&](int &) {
    auto l = std::make_shared<const live::terms::Local>(live::terms::local(info));
    edit(c, &Core::in, [&](Inputs &x) {
      x.local = std::move(l);
      derive(x);
    });
  });
  peek();
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
      for (const sina::Quote &x : *y->watch)
        (*q)[x.sym.substr(2)] = live::quote_of(x);
      take(c, snap_, std::move(q));
      in_ = inputs(c);
    }
  }
  const Inputs &in = *in_;

  board_.update({snap_.get(), in.quotes.get()}, [&](BoardView &cur) {
    BoardView b{.snap = snap_, .quotes = in.quotes};
    try {
      if (!snap_)
        b.wait = "还没读到持仓与成交 (等刷新)";
      else if (!in.quotes || !(b.frame = live::board::view(*snap_, *in.quotes)))
        b.wait = "等行情";
    } catch (const live::Fail &e) {
      b.err = e.why;
    }
    if (b.wait.empty() || !cur.frame)
      cur = std::move(b);
    else
      cur.wait = std::move(b.wait);
  });

  plan_.update({snap_.get(), in_.get()}, [&](PlanView &cur) {
    PlanView p;
    if (!in.terms)
      p.wait = "两融条款还没拉过 (更新两融名单)";
    else if (!snap_)
      p.wait = "还没读到信用账户 (等刷新)";
    else if (in.px_snap != snap_ || !in.quotes)
      p.wait = "等账户各只的行情";
    else {
      try {
        if (!(p.plan = live::rebal::plan(*snap_, *in.px0, *in.quotes, *in.terms, in.codes, in.cfg)))
          p.wait = "等行情";
      } catch (const live::Fail &e) {
        p.err = e.why;
      }
    }
    if (p.plan)
      p.now = p.plan->now;
    else if (snap_)
      p.now = live::rebal::now_of(snap_->acct);
    if (p.wait.empty() || !cur.plan)
      cur = std::move(p);
    else
      cur.wait = std::move(p.wait);
  });
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
  const live::Sec *sec = snap_ ? snap_->pos.find(code) : nullptr;
  if (!sec && snap_)
    sec = snap_->fills.by_code.find(code);
  if (!sec) {
    const auto i = std::find_if(in_->buy.begin(), in_->buy.end(), [&](const live::Sec &s) { return s.code == code; });
    assert(i != in_->buy.end() && side == live::kBuy);
    sec = &*i;
  }
  const live::OrderReq o{*sec, side, credit, 0, live::Qty::yuan(size)};
  start(c_, "下单", [o, mode = in_->cfg.px](Core &c) {
    Books books(std::vector<live::Sec>{});
    const OrderJob j = exec(c, books, o, strf("%gW", o.qty.v / 1e4), mode, "");
    live::need(j.st == JobSt::Ok, failed(j));
  });
}

void LiveHub::pull_terms() {
  assert(connected());
  start(c_, kPulls[0].name, kPulls[0].run);
}

void LiveHub::run() {
  assert(connected());
  start(c_, "执行调仓", rebal_task);
}

} // namespace GUI::Live
