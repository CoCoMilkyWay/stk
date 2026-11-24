#include "gui/task_live/LiveHub.hpp"

#include "api/sina/client.hpp"
#include "disk/Disk.hpp"
#include "gui/quote/QuoteHub.hpp"
#include "live/Flows.hpp"
#include "shared/AssetInfo.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <iterator>
#include <mutex>
#include <optional>
#include <thread>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

namespace GUI::Live {

using live::strf;

struct LiveHub::Core {
  std::unique_ptr<live::Broker> broker; // connect 时建一次, 之后只读
  std::vector<std::string> steps;       // 下单各步, 同 broker 建一次
  live::trace::Track track;             // 事务线程此刻在哪

  // mu: 三份快照 (改 = 抄一份改了换上) 与事务日志的位置
  std::mutex mu;
  std::shared_ptr<const Inputs> in;
  std::shared_ptr<const Task> task;
  std::shared_ptr<const std::vector<OrderJob>> jobs = std::make_shared<const std::vector<OrderJob>>();
  misc::Ymd flows_pulled = 0; // 资金流水账本拉于
  std::string task_stamp;     // 本件事务日志文件的时刻
  double task_t0 = 0;
  bool ordering = false; // 事务线程正下着最近几单之首那单: 其各步的段、实时一行同步进这单
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

// 快照各只 (持仓 / 负债) 在 q 里的现价; 缺一只 → 空
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

// 行情 q 整份换上; 快照是新读到的且其各只都有了即记作读时现价
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

constexpr int kQuoteTries = 2; // 事务现取行情: 网络错至多试几次 (有界, 不卡在半路)

// 现取 secs 的行情 (事务要此刻的价, 不等订阅那一路) 换上; 取不到 → Fail
void fetch(Core &c, sina::Client &cl, const std::shared_ptr<const live::Snapshot> &snap, const std::vector<live::Sec> &secs) {
  auto q = std::make_shared<live::Quotes>();
  std::vector<std::string> syms;
  for (size_t i = 0; i < secs.size(); ++i) {
    syms.push_back(live::sina_sym(secs[i].mkt, secs[i].code));
    if (syms.size() == static_cast<size_t>(sina::kQuoteMax) || i + 1 == secs.size()) {
      const std::optional<std::vector<sina::Quote>> got = cl.try_quotes(syms, kQuoteTries);
      live::need(got.has_value(), "新浪取不到行情 (%zu 只)", syms.size());
      for (const sina::Quote &x : *got)
        (*q)[x.sym.substr(2)] = live::quote_of(x);
      syms.clear();
    }
  }
  take(c, snap, std::move(q));
}

// 下单用的盘口: 批量 = 后台一条常驻连接每 kBookGap 秒取一遍这批各只 (超一次快照的分几批), 取用最新的一份;
//   收到已超 kBookStale 秒或还没这只 → 当场现取. 单笔 (secs 空) = 每次现取.
//   取行情一律有界 (try_quotes): 流取不到即留着上一份; 现取取不到即该单失败, 且 kBookStale 秒内不再现取 (断网时后面各单立刻失败、不排队等超时)
//   后台线程持 Feed 的 shared_ptr, 析构只置停 (在途的一问做完即退)
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
    if (syms.empty())
      return;
    std::thread([f = feed_, syms = std::move(syms)] {
      sina::Client cl(sina::kQuoteHost);
      std::unique_lock lk(f->mu);
      while (!f->stop) {
        lk.unlock();
        std::vector<live::Quote> got;
        std::ignore = live::guard([&] {
          for (size_t i = 0; i < syms.size(); i += static_cast<size_t>(sina::kQuoteMax)) {
            const std::vector<std::string> part(syms.begin() + static_cast<std::ptrdiff_t>(i),
                                                syms.begin() + static_cast<std::ptrdiff_t>(std::min(syms.size(), i + static_cast<size_t>(sina::kQuoteMax))));
            if (const std::optional<std::vector<sina::Quote>> q = cl.try_quotes(part, 1))
              for (const sina::Quote &x : *q)
                got.push_back(live::quote_of(x));
          }
        });
        const double now = mono();
        lk.lock();
        for (live::Quote &x : got) {
          const std::string code = x.sym.substr(2);
          f->q[code] = {std::move(x), now};
        }
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
      if (i != feed_->q.end())
        if (const double age = mono() - i->second.at; age <= kBookStale)
          return {i->second.q, age, true};
    }
    live::need(mono() - down_ > kBookStale, "新浪 %.0fs 内刚取不到行情, 本单不再现取", kBookStale);
    if (!cl_)
      cl_.emplace(sina::kQuoteHost);
    const std::optional<std::vector<sina::Quote>> got = cl_->try_quotes({live::sina_sym(s.mkt, s.code)}, 1);
    if (!got)
      down_ = mono();
    live::need(got.has_value(), "新浪取不到行情");
    live::need(!got->empty(), "新浪查无此码");
    return {live::quote_of((*got)[0])};
  }

private:
  static constexpr double kBookGap = 0.5, kBookStale = 5; // [s]
  struct Got {
    live::Quote q;
    double at = 0; // 收到于 (mono)
  };
  struct Feed {
    std::mutex mu;
    std::condition_variable cv;
    bool stop = false;
    std::unordered_map<std::string, Got> q; // 代码 → 最近收到的
  };
  std::shared_ptr<Feed> feed_ = std::make_shared<Feed>();
  std::optional<sina::Client> cl_; // 现取的连接 (头一回现取才建)
  double down_ = -1e9;             // 上一回现取取不到于 (mono)
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

// 走到哪一步 (事务的第二级标签; 批量下单时即本单在批里的位置, 如「卖出 2/5」)
void stage(Core &c, std::string s) {
  edit(c, &Core::task, [&](Task &x) { x.step = std::move(s); });
}

// 事务里下一单: 「行情」从 books 取盘口记一条、按 mode 定委托价 (yuan > 0 = 按额: 额定价、按委托价折股; 否则按 o.qty 股), 再交 broker 下.
// 本单进最近几单之首, 其各步 (段名即步名) 与实时一行经事务线程的 Hooks 同步进本单; 请求与结局各记一条.
// 成败看 st, 不抛: 本单出的任何错 (含应答畸形抛的) 都收在本单, 批量下单接着下后面的
OrderJob exec(Core &c, Books &books, live::OrderReq o, double yuan, live::PxMode mode, const std::string &at) {
  const char *px_name = live::kPxModeNames[static_cast<int>(mode)];
  std::string label = yuan > 0 ? strf("%gW", yuan / 1e4) : strf("%lld股", static_cast<long long>(o.qty));
  live::trace::note("order", strf("%s %s %s %s %s · %s", live::side_name(o.side), live::credit_name(o.credit), o.code.c_str(), o.name.c_str(), label.c_str(), px_name),
                    {{"order", live::to_json(o)}, {"yuan", yuan}, {"mode", px_name}});
  stage(c, at);

  OrderJob j{.sec = live::sec_now(), .o = o, .label = std::move(label)};
  {
    Lock lk(c.mu);
    auto v = std::make_shared<std::vector<OrderJob>>(*c.jobs);
    v->insert(v->begin(), std::move(j));
    if (v->size() > kOrderCap)
      v->pop_back();
    c.jobs = std::move(v);
    c.ordering = true;
  }
  const auto put = [&](const auto &f) {
    OrderJob out;
    edit(c, &Core::jobs, [&](std::vector<OrderJob> &v) {
      f(v.front());
      out = v.front();
    });
    return out;
  };

  live::Placed p;
  const std::string err = live::guard([&] {
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
      put([&](OrderJob &y) { y.o = o; });
    }
    p = c.broker->place(o);
  });
  const OrderJob out = put([&](OrderJob &y) {
    y.st = err.empty() ? JobSt::Ok : JobSt::Err;
    if (err.empty())
      y.got = p;
    else
      y.got.msg = err;
  });
  {
    Lock lk(c.mu);
    c.ordering = false;
  }
  live::trace::note("job", strf("%s %s %s", job_name(out.st), out.detail.c_str(), out.got.msg.c_str()),
                    {{"job", {{"state", job_name(out.st)}, {"detail", out.detail}, {"msg", out.got.msg}, {"qty", out.got.qty}, {"px", out.got.px}}}});
  return out;
}

std::string failed(const OrderJob &j) { return strf("%s %s 下单失败: %s", j.o.code.c_str(), j.o.name.c_str(), j.got.msg.c_str()); }

// 各件事务的身子; 两样拉取同形: 今天 d 该拉了 (快照 s 读到后) / 拉 → 存, 再换上
bool terms_due(Core &c, const live::Snapshot &s, misc::Ymd d) {
  const std::shared_ptr<const Inputs> in = inputs(c);
  return !in->raw || misc::add_days(in->raw->pulled, kTermsAge) <= d || live::terms::missing(in->raw->terms, *in->local, in->codes, &s);
}

void terms_task(Core &c) {
  auto x = std::make_shared<const live::terms::Cache>(live::terms::pull(*c.broker));
  edit(c, &Core::in, [&](Inputs &in) {
    in.raw = std::move(x);
    derive(in);
  });
}

bool flows_due(Core &c, const live::Snapshot &, misc::Ymd d) {
  Lock lk(c.mu);
  return c.flows_pulled < d;
}

void flows_task(Core &c) {
  const misc::Ymd d = live::flows::pull(*c.broker).pulled;
  Lock lk(c.mu);
  c.flows_pulled = d;
}

// 刷新一轮 (成交、持仓、挂单、信用账户), 记下账户
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
  live::need(in->local_err.empty(), "%s", in->local_err.c_str());
  live::need(in->terms != nullptr, "两融条款还没拉过");
  live::need(in->px_snap == snap, "账户各只的行情没取齐");
  const std::optional<live::rebal::Plan> p = live::rebal::plan(*snap, *in->px0, *in->quotes, *in->terms, in->codes, in->cfg);
  live::need(p.has_value(), "新浪没给齐要的行情");
  live::trace::note("plan", strf("试算: 行使 %s (融资 %s) · %zu 单 · 成本 %.0f", wan(p->use).c_str(), wan(p->financed).c_str(), p->orders.size(), p->cost),
                    {{"cfg", live::rebal::to_json(in->cfg)}, {"codes", in->codes}, {"plan", live::rebal::to_json(*p)}});
  return *p;
}

// 执行调仓. 要紧处:
//   · 试算现做一遍 (刷新后的账户 + 此刻行情), 页面上的那份只作预览; 各单的股数照它, 委托价下单时按新取的盘口再定
//   · 一口气下完, 不等成交: 卖出回款 / 腾出的保证金在卖单成交前还没到账, 紧跟着的融资买单由闪电窗的最大可买封顶 (少买的事后人工补);
//     不等是有意的 —— 等成交会在半路卡住 (卖了一半没买), 留下方向与价格敞口
//   · 一单失败 (含任何意外) 只记下、接着下后面的, 末了一并报失败 (该补哪几单); 取行情、等界面一律有界, 不会卡在某一单
//   · 下完即清掉卖出设置 (先于末了的刷新: 刷新失败也不留着): 再点执行不会重复卖
//   · 末了的刷新只为页面尽早见到结果, 失败只记进结局, 不盖掉各单的结局
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
  std::vector<std::string> fails; // 没下成的单: 记下接着下后面的, 末了一并报
  c.broker->batch([&] {
    Books books(std::vector<live::Sec>(p.orders.begin(), p.orders.end()));
    for (size_t i = 0; i < p.orders.size(); ++i) {
      const live::rebal::Order &o = p.orders[i];
      const OrderJob j = exec(c, books, o, 0, mode, strf("%s %zu/%zu", live::side_name(o.side), i + 1, p.orders.size()));
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

// 后台起一件事务; 已有事务在跑 → 不收, false.
//   事务线程挂 Watch + Hooks: 段 / 实时一行 / 记一条照常经 live::trace 落交互日志, Hooks 另把记的各条落本件事务的日志文件
//   (output/run/live/task/<时刻>.jsonl, 留最近 kTaskKeep 个: 下单留底比交互日志留得久) 并留尾巴给页面, 正下着的那单同步其各步与概要.
//   事务的段在 Watch 之外 (进度只列其下的段, 名字已在事务名上)
bool start(const std::shared_ptr<Core> &c, const char *name, std::function<void(Core &)> fn) {
  {
    Lock lk(c->mu);
    if (c->task && c->task->st == JobSt::Run)
      return false;
    c->task_stamp = disk::stamp_now();
    c->task_t0 = mono();
    c->task = std::make_shared<const Task>(Task{.name = name, .sec = live::sec_now(), .file = disk::path(disk::RunLiveTaskLog, {.stamp = c->task_stamp}).string()});
  }
  const std::vector<disk::Key> old = disk::list(disk::RunLiveTaskLog);
  for (size_t i = 0; i + kTaskKeep <= old.size(); ++i)
    disk::remove(disk::RunLiveTaskLog, old[i]);
  std::thread([c, name = std::string(name), fn = std::move(fn)] {
    const live::trace::Hooks hooks{
        .on_note = [&](const char *kind, std::string text, const live::trace::Kw &kw) {
          std::string stamp;
          double t;
          {
            Lock lk(c->mu);
            stamp = c->task_stamp;
            t = mono() - c->task_t0;
          }
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
          bool ordering;
          {
            Lock lk(c->mu);
            ordering = c->ordering;
          }
          if (const auto it = std::find(c->steps.begin(), c->steps.end(), s); ordering && it != c->steps.end())
            edit(*c, &Core::jobs, [&](std::vector<OrderJob> &v) { v.front().at = static_cast<size_t>(it - c->steps.begin()); }); },
        .on_live = [&](std::string line) {
          bool ordering;
          {
            Lock lk(c->mu);
            ordering = c->ordering;
          }
          if (ordering)
            edit(*c, &Core::jobs, [&](std::vector<OrderJob> &v) { v.front().detail = line; });
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
      t.live.clear();
      t.step.clear();
    });
  }).detach();
  return true;
}

// 两样拉取 (条款 / 流水; 同形: 该拉了 / 拉 → 存, 再换上), 下标同 LiveHub::tried_
struct Pull {
  const char *name;
  bool (*due)(Core &, const live::Snapshot &, misc::Ymd);
  void (*run)(Core &);
};
constexpr Pull kPulls[] = {{"更新两融名单", terms_due, terms_task}, {"更新资金流水", flows_due, flows_task}};

} // namespace

LiveHub::LiveHub() : c_(std::make_shared<Core>()) {
  auto in = std::make_shared<Inputs>();
  if (live::terms::Cache x = live::terms::load(); x.pulled)
    in->raw = std::make_shared<const live::terms::Cache>(std::move(x));
  in->local = std::make_shared<const live::terms::Local>();
  derive(*in);
  c_->in = std::move(in);
  c_->flows_pulled = live::flows::load().pulled;
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
  c_->steps = {"行情"};
  for (const std::string_view s : c_->broker->steps())
    c_->steps.emplace_back(s);
  peek();
}

const std::vector<std::string> &LiveHub::steps() const {
  assert(connected());
  return c_->steps;
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
  static_assert(std::size(kPulls) == std::extent_v<decltype(tried_)>);
  for (size_t i = 0; i < std::size(kPulls); ++i)
    if (tried_[i] < d && kPulls[i].due(*c_, *snap_, d)) {
      if (start(c_, kPulls[i].name, kPulls[i].run))
        tried_[i] = d;
      return;
    }
}

void LiveHub::tick(const AssetInfo &info, Quote::QuoteHub &quote) {
  Core &c = *c_;
  info_st_.update({info.pit_end, info.bar1m_end, static_cast<int64_t>(info.codes.size())}, [&](int &) {
    live::terms::Local l;
    std::string err = live::guard([&] { l = live::terms::local(info); });
    auto lp = std::make_shared<const live::terms::Local>(std::move(l));
    edit(c, &Core::in, [&](Inputs &x) {
      x.local = std::move(lp);
      x.local_err = std::move(err);
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
    b.err = live::guard([&] {
      if (!snap_)
        b.wait = "还没读到持仓与成交 (等刷新)";
      else if (!in.quotes || !(b.frame = live::board::view(*snap_, *in.quotes)))
        b.wait = "等行情";
    });
    if (b.wait.empty() || !cur.frame)
      cur = std::move(b);
    else
      cur.wait = std::move(b.wait);
  });

  plan_.update({snap_.get(), in_.get()}, [&](PlanView &cur) {
    PlanView p;
    if (!in.local_err.empty())
      p.err = in.local_err;
    else if (!in.terms)
      p.wait = "两融条款还没拉过 (更新两融名单)";
    else if (!snap_)
      p.wait = "还没读到信用账户 (等刷新)";
    else if (in.px_snap != snap_ || !in.quotes)
      p.wait = "等账户各只的行情";
    else
      p.err = live::guard([&] {
        if (!(p.plan = live::rebal::plan(*snap_, *in.px0, *in.quotes, *in.terms, in.codes, in.cfg)))
          p.wait = "等行情";
      });
    if (p.plan)
      p.now = p.plan->now;
    else if (snap_)
      p.now = live::rebal::now_of(*snap_);
    if (p.wait.empty() || !cur.plan)
      cur = std::move(p);
    else
      cur.wait = std::move(p.wait);
  });
}

std::vector<std::string> LiveHub::lists() const { return live::rebal::lists(); }

void LiveHub::set_list(const std::string &name) {
  std::vector<std::string> codes;
  std::string err = live::guard([&] { codes = live::rebal::read_list(name); });
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
  const live::OrderReq o{*sec, side, credit};
  start(c_, "下单", [o, size, mode = in_->cfg.px](Core &c) {
    Books books(std::vector<live::Sec>{});
    const OrderJob j = exec(c, books, o, size, mode, "");
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
