#include "live/Broker.hpp"

#include "live/Trace.hpp"
#include "misc/cross_platform.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <thread>

namespace live {

std::string strf(const char *fmt, ...) {
  va_list a, b;
  va_start(a, fmt);
  va_copy(b, a);
  const int n = std::vsnprintf(nullptr, 0, fmt, a);
  va_end(a);
  assert(n >= 0);
  std::string s(static_cast<size_t>(n), '\0');
  std::vsnprintf(s.data(), s.size() + 1, fmt, b);
  va_end(b);
  return s;
}

std::string clock_now() {
  const std::time_t t = std::time(nullptr);
  const std::tm tm = safe_localtime(&t);
  char buf[16];
  std::strftime(buf, sizeof buf, "%H:%M:%S", &tm);
  return buf;
}

Mkt market_of(std::string_view code) {
  assert(code.size() == 6);
  if (code.starts_with("4") || code.starts_with("8") || code.starts_with("92"))
    return Mkt::BJ;
  if (code.starts_with("5") || code.starts_with("6") || code.starts_with("9"))
    return Mkt::SH;
  return Mkt::SZ;
}

Mkt mkt_named(std::string_view s) {
  for (const Mkt m : {Mkt::SZ, Mkt::SH, Mkt::BJ})
    if (s == mkt_name(m))
      return m;
  assert(false && "市场名");
  return Mkt::SZ;
}

bool synced(const Position &h, const Traded *f) { return !(h.balance > 0) || h.traded == (f ? f->qty : BySide{}); }

nlohmann::json to_json(const OrderReq &o) {
  return {{"code", o.sec.code}, {"name", o.sec.name}, {"side", side_name(o.side)}, {"credit", credit_name(o.credit)}, {"px", o.px}, {"qty", o.qty}, {"size", o.size}};
}

// ---------- Broker ----------

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

constexpr auto kTick = 100ms;
constexpr double kEvery = 60;     // 定时刷新周期缺省 [s]
constexpr int kKicks[] = {1, 6};  // 下单成功后隔几秒各刷新一次 (限价单可能稍后才成交)
constexpr int kSyncTries = 2;     // 一轮里成交与持仓对不上 (两问之间又成交) 时最多连查几遍
constexpr auto kWatchLinger = 1s; // touch 之后算有人在看的时长

} // namespace

// mu: State 的底稿 (轮内逐样改) 与发布的一份 (轮末整份换上)、进度、轮次与起因
struct Broker::Core {
  std::unique_ptr<Driver> d;

  mutable std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<const State> st = std::make_shared<State>(), shown = st;
  Refresh prog{.every = kEvery};
  bool stop = false, hushed = false;
  uint64_t asked = 0, done = 0;          // 要过 / 刷完的轮号
  std::optional<Clock::time_point> last; // 上一轮结束时刻
  std::vector<Clock::time_point> kicks;
  Clock::time_point touched{};

  // 要一轮 → 轮号
  uint64_t want() {
    std::lock_guard lk(mu);
    const uint64_t n = ++asked;
    cv.notify_all();
    return n;
  }
  // 刚下过单: 隔几秒各刷一次 (Hush 着不排)
  void kick() {
    std::lock_guard lk(mu);
    if (hushed)
      return;
    const Clock::time_point now = Clock::now();
    for (const int k : kKicks)
      kicks.push_back(now + std::chrono::seconds(k));
    std::sort(kicks.begin(), kicks.end());
  }

  // 改底稿 (须持 mu)
  template <class F>
  void draft(F &&edit) {
    auto s = std::make_shared<State>(*st);
    edit(*s);
    st = std::move(s);
  }
  void note(std::string step) {
    std::lock_guard lk(mu);
    prog.step = std::move(step);
  }

  // 接上客户端; 换了进程即记下新的 who
  void connect() {
    trace::Span span("接入");
    std::string who = d->connect();
    std::lock_guard lk(mu);
    if (who != st->who)
      draft([&](State &x) { x.who = std::move(who); });
  }
  // 一件事务 (拉 / 写): 段 (who {kw}) + 接上客户端, 当前线程跑 fn
  template <class F>
  auto txn(const char *who, trace::Kw kw, F &&fn) {
    trace::Span span(who, std::move(kw));
    connect();
    return fn();
  }

  // ---------- 刷新线程 ----------

  // 该刷新一轮了 → 起因; 不该 → 空 (须持 mu)
  const char *due(Clock::time_point now) {
    if (asked > done)
      return "请求";
    if (hushed || now - touched >= kWatchLinger)
      return nullptr;
    if (!kicks.empty() && now >= kicks.front()) {
      kicks.erase(kicks.begin());
      return "下单后";
    }
    if (!last)
      return "首轮";
    if (prog.every > 0 && now - *last >= std::chrono::duration<double>(prog.every))
      return "定时";
    return nullptr;
  }

  // 读一样、记进底稿: fn() → 值
  template <class T, class F>
  void put(Got<T> State::*g, F &&fn) {
    note(State::name(g));
    trace::Span span(State::name(g));
    auto v = std::make_shared<const T>(fn());
    std::lock_guard lk(mu);
    draft([&](State &x) {
      (x.*g).v = std::move(v);
      (x.*g).at = clock_now();
    });
  }
  std::shared_ptr<const State> snap() const {
    std::lock_guard lk(mu);
    return st;
  }

  // 一轮: 成交、持仓两问之间又有成交即对不上, 连查至多 kSyncTries 遍, 仍不对留给上层标出; 再挂单、信用账户 (带同一轮的持仓)
  void round() {
    for (int i = 0; i < kSyncTries; ++i) {
      put(&State::fills, [&] { return d->fills(); });
      put(&State::pos, [&] { return d->pos(); });
      const std::shared_ptr<const State> s = snap();
      if (std::all_of(s->pos->list.begin(), s->pos->list.end(), [&](const Position &h) { return synced(h, s->fills->by_code.find(h.code)); }))
        break;
      trace::ev("unsynced", {{"try", i + 1}});
    }
    put(&State::pends, [&] { return d->pends(); });
    put(&State::acct, [&] {
      Account a = d->acct();
      a.held = snap()->pos.v;
      return a;
    });
  }

  void tick() {
    uint64_t n = 0;
    const char *why = nullptr;
    {
      std::unique_lock lk(mu);
      if (!cv.wait_for(lk, kTick, [&] { return stop || (why = due(Clock::now())) != nullptr; }) || stop)
        return;
      n = asked;
    }
    trace::Span span("刷新", {{"why", why}, {"n", n}});
    std::string down, msg;
    note("接入");
    try { // 错误边界: 接不上报在 down, 一轮失败报在 err, 都等下一个起因再刷
      connect();
    } catch (const Fail &f) {
      down = f.why;
    }
    if (down.empty()) {
      try {
        round();
      } catch (const Fail &f) {
        msg = f.why;
      }
    }
    std::lock_guard lk(mu);
    shown = st;
    last = Clock::now();
    done = n;
    prog.step.clear();
    prog.down = down;
    prog.err = down.empty() ? msg : "";
    cv.notify_all();
  }

  bool stopped() const {
    std::lock_guard lk(mu);
    return stop;
  }
};

Broker::Broker(std::unique_ptr<Driver> d) : c_(std::make_shared<Core>()) {
  c_->d = std::move(d);
  std::thread([c = c_] {
    while (!c->stopped())
      c->tick();
  }).detach();
}

Broker::~Broker() {
  {
    std::lock_guard lk(c_->mu);
    c_->stop = true;
  }
  c_->cv.notify_all();
}

std::shared_ptr<const State> Broker::state() const {
  std::lock_guard lk(c_->mu);
  return c_->shown;
}

Refresh Broker::refresh() const {
  std::lock_guard lk(c_->mu);
  return c_->prog;
}

void Broker::touch() {
  std::lock_guard lk(c_->mu);
  c_->touched = Clock::now();
}

void Broker::request() { c_->want(); }

void Broker::sync() {
  const uint64_t n = c_->want();
  std::unique_lock lk(c_->mu);
  c_->cv.wait(lk, [&] { return c_->done >= n || c_->stop; });
  if (c_->stop)
    fail("已断开");
  if (!c_->prog.down.empty())
    fail("接不上客户端: " + c_->prog.down);
  if (!c_->prog.err.empty())
    fail("刷新失败: " + c_->prog.err);
}

void Broker::every(double s) {
  assert(s >= 0);
  {
    std::lock_guard lk(c_->mu);
    c_->prog.every = s;
  }
  c_->cv.notify_all();
}

Broker::Hush::Hush(Broker &b) : c_(*b.c_) {
  std::lock_guard lk(c_.mu);
  assert(!c_.hushed);
  c_.hushed = true;
  c_.kicks.clear();
}

Broker::Hush::~Hush() {
  std::lock_guard lk(c_.mu);
  c_.hushed = false;
}

Terms Broker::terms(const Report &r) {
  return c_->txn("两融条款", trace::Kw::object(), [&] { return c_->d->terms(r); });
}

std::vector<CashFlow> Broker::flows(misc::Ymd from, const Report &r) {
  return c_->txn("资金流水", {{"from", from}}, [&] { return c_->d->flows(from, r); });
}

Placed Broker::place(const OrderReq &o, const Report &r) {
  const Placed p = c_->txn("下单", to_json(o), [&] { return c_->d->place(o, r); });
  c_->kick();
  return p;
}

const std::vector<std::string> &Broker::steps() const { return c_->d->steps(); }

} // namespace live
