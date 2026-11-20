#include "live/Broker.hpp"

#include "misc/cross_platform.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
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

int sec_now() {
  const std::time_t t = std::time(nullptr);
  const std::tm tm = safe_localtime(&t);
  return tm.tm_hour * 3600 + tm.tm_min * 60 + tm.tm_sec;
}

std::string fmt_sec(int sec) {
  if (sec < 0)
    return "—";
  return strf("%02d:%02d:%02d", sec / 3600, sec / 60 % 60, sec % 60);
}

int parse_sec(std::string_view s) {
  const auto dig = [](char c) { return std::isdigit(static_cast<unsigned char>(c)) != 0; };
  if (s.size() != 8 || !dig(s[0]) || !dig(s[1]) || s[2] != ':' || !dig(s[3]) || !dig(s[4]) || s[5] != ':' || !dig(s[6]) || !dig(s[7]))
    return -1;
  const int h = (s[0] - '0') * 10 + (s[1] - '0');
  const int m = (s[3] - '0') * 10 + (s[4] - '0');
  const int c = (s[6] - '0') * 10 + (s[7] - '0');
  if (h > 23 || m > 59 || c > 59)
    return -1;
  return h * 3600 + m * 60 + c;
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

bool synced(const Position &h, const Traded *f) { return h.balance <= 0 || h.traded == (f ? f->qty : Sides<int64_t>{}); }

nlohmann::json to_json(const OrderReq &o) {
  return {{"code", o.sec.code},
          {"name", o.sec.name},
          {"side", side_name(o.side)},
          {"credit", credit_name(o.credit)},
          {"px", o.px},
          {o.qty.is_shares() ? "qty" : "size", o.qty.v}};
}

// ---------- Broker ----------

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

constexpr auto kTick = 100ms;
constexpr double kEvery = 60;
constexpr int kKicks[] = {1, 6};
constexpr int kSyncTries = 2;
constexpr auto kWatchLinger = 1s;

} // namespace

struct Broker::Core {
  std::unique_ptr<Driver> d;
  trace::Track track;

  mutable std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<const Snapshot> shown;
  Refresh prog{.every = kEvery};
  bool stop = false;
  int hushed = 0; // > 0 = 批量事务期间抑制自起刷新
  uint64_t asked = 0, done = 0;
  std::optional<Clock::time_point> last;
  std::vector<Clock::time_point> kicks;
  Clock::time_point touched{};

  uint64_t want() {
    std::lock_guard lk(mu);
    const uint64_t n = ++asked;
    cv.notify_all();
    return n;
  }

  void kick() {
    std::lock_guard lk(mu);
    if (hushed > 0)
      return;
    const Clock::time_point now = Clock::now();
    for (const int k : kKicks)
      kicks.push_back(now + std::chrono::seconds(k));
    std::sort(kicks.begin(), kicks.end());
  }

  std::string connect() {
    trace::Span span("接入");
    std::string who = d->connect();
    std::lock_guard lk(mu);
    prog.who = who;
    return who;
  }

  template <class F>
  auto txn(const char *who, trace::Kw kw, F &&fn) {
    trace::Span span(who, std::move(kw));
    connect();
    return fn();
  }

  // ---------- 刷新线程 ----------

  const char *due(Clock::time_point now) {
    if (asked > done)
      return "请求";
    if (hushed > 0 || now - touched >= kWatchLinger)
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

  // 整轮读入局部 Snapshot, 轮末整份原子发布
  Snapshot round(std::string who) {
    Snapshot s;
    s.who = std::move(who);
    for (int i = 0; i < kSyncTries; ++i) {
      {
        trace::Span sp("成交");
        s.fills = d->fills();
      }
      {
        trace::Span sp("持仓");
        s.pos = d->pos();
      }
      if (std::all_of(s.pos.list.begin(), s.pos.list.end(), [&](const Position &h) { return synced(h, s.fills.by_code.find(h.code)); }))
        break;
      trace::ev("unsynced", {{"try", i + 1}});
    }
    {
      trace::Span sp("挂单");
      s.pends = d->pends();
    }
    {
      trace::Span sp("信用账户");
      s.acct = d->acct();
    }
    s.at = sec_now();
    return s;
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
    const trace::Watch watch(track);
    trace::Span span("刷新", {{"why", why}, {"n", n}});
    std::string down, msg;
    std::shared_ptr<const Snapshot> next;
    try {
      std::string who = connect();
      try {
        next = std::make_shared<const Snapshot>(round(std::move(who)));
      } catch (const Fail &f) {
        msg = f.why;
      }
    } catch (const Fail &f) {
      down = f.why;
    }
    std::lock_guard lk(mu);
    if (next)
      shown = std::move(next);
    last = Clock::now();
    done = n;
    prog.down = std::move(down);
    prog.err = prog.down.empty() ? std::move(msg) : "";
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

std::shared_ptr<const Snapshot> Broker::snapshot() const {
  std::lock_guard lk(c_->mu);
  return c_->shown;
}

Refresh Broker::refresh() const {
  Refresh r;
  {
    std::lock_guard lk(c_->mu);
    r = c_->prog;
  }
  r.at = c_->track.now();
  return r;
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
  need(!c_->stop, "已断开");
  need(c_->prog.down.empty(), "接不上客户端: " + c_->prog.down);
  need(c_->prog.err.empty(), "刷新失败: " + c_->prog.err);
}

void Broker::every(double s) {
  assert(s >= 0);
  {
    std::lock_guard lk(c_->mu);
    c_->prog.every = s;
  }
  c_->cv.notify_all();
}

Terms Broker::terms() {
  return c_->txn("两融条款", trace::Kw::object(), [&] { return c_->d->terms(); });
}

std::vector<CashFlow> Broker::flows(misc::Ymd from) {
  return c_->txn("资金流水", {{"from", from}}, [&] { return c_->d->flows(from); });
}

Placed Broker::place(const OrderReq &o) {
  const Placed p = c_->txn("下单", to_json(o), [&] { return c_->d->place(o); });
  c_->kick();
  return p;
}

void Broker::batch(const std::function<void()> &fn) {
  {
    std::lock_guard lk(c_->mu);
    ++c_->hushed;
    c_->kicks.clear();
  }
  struct Guard {
    Core &c;
    ~Guard() {
      std::lock_guard lk(c.mu);
      --c.hushed;
    }
  } g{*c_};
  fn();
}

std::span<const std::string_view> Broker::steps() const { return c_->d->steps(); }

std::unique_ptr<Mirror> Broker::mirror() const { return c_->d->mirror(); }

} // namespace live
