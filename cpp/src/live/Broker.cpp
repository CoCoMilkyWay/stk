#include "live/Broker.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace live {

bool synced(const Position &h, const Traded *f) { return h.balance <= 0 || h.traded == (f ? f->qty : Sides<int64_t>{}); }

nlohmann::json to_json(const OrderReq &o) {
  return {{"code", o.code}, {"name", o.name}, {"side", side_name(o.side)}, {"credit", credit_name(o.credit)}, {"px", o.px}, {"qty", o.qty}};
}

// ---------- Broker ----------

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

constexpr auto kTick = 100ms;
constexpr double kEvery = 60;     // 定时刷新周期缺省 [s]
constexpr int kKicks[] = {1, 6};  // 下单成功后隔几秒各刷新一次 (限价单可能稍后才成交)
constexpr int kSyncTries = 2;     // 一轮里成交与持仓对不上 (读的那一刻之间又成交) 时最多读几遍
constexpr auto kWatchLinger = 1s; // touch 之后算有人在看的时长

} // namespace

// mu: 发布的一份、进度与上轮结局、轮次与起因; 刷新线程此刻在哪另在 track
struct Broker::Core {
  std::unique_ptr<Driver> d;
  trace::Track track;

  mutable std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<const Snapshot> shown;
  Refresh prog{.every = kEvery};
  bool stop = false;
  bool hushed = false;                   // 批量下单期间, 不自起刷新 (事务一次一件, 不嵌套)
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

  // 刚下过单: 隔几秒各刷一次 (批量下单期间不排)
  void kick() {
    std::lock_guard lk(mu);
    if (hushed)
      return;
    const Clock::time_point now = Clock::now();
    for (const int k : kKicks)
      kicks.push_back(now + std::chrono::seconds(k));
    std::sort(kicks.begin(), kicks.end());
  }

  // 接上客户端, 记下是谁
  void connect() {
    trace::Span span("接入");
    std::string who = d->connect();
    std::lock_guard lk(mu);
    prog.who = std::move(who);
  }

  // 一件事务 (拉 / 写): 段 (name {kw}) + 接上客户端, 当前线程跑 fn
  template <class F>
  auto txn(const char *name, trace::Kw kw, F &&fn) {
    trace::Span span(name, std::move(kw));
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

  // 一轮读进一份局部的 Snapshot: 成交与持仓读的那一刻之间又有成交即对不上, 重读至多 kSyncTries 遍, 仍不对留给上层标出
  Snapshot round() {
    Snapshot s;
    for (int i = 1;; ++i) {
      {
        trace::Span sp("读");
        s = d->read();
      }
      const bool ok = std::all_of(s.pos.list.begin(), s.pos.list.end(), [&](const Position &h) { return synced(h, s.fills.by_code.find(h.code)); });
      if (!ok)
        trace::ev("unsynced", {{"try", i}});
      if (ok || i == kSyncTries)
        break;
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
    // 错误边界: 接不上报在 down, 一轮失败报在 err (保留上一份完整快照), 都等下一个起因再刷
    std::string msg;
    std::shared_ptr<const Snapshot> next;
    std::string down = guard([&] { connect(); });
    if (down.empty())
      msg = guard([&] { next = std::make_shared<const Snapshot>(round()); });
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
    assert(!c_->hushed && "批量下单不嵌套");
    c_->hushed = true;
    c_->kicks.clear();
  }
  struct Guard {
    Core &c;
    ~Guard() {
      std::lock_guard lk(c.mu);
      c.hushed = false;
    }
  } g{*c_};
  fn();
}

std::span<const std::string_view> Broker::steps() const { return c_->d->steps(); }

size_t Broker::unsent() const { return c_->d->unsent(); }

std::unique_ptr<Mirror> Broker::mirror() const { return c_->d->mirror(); }

} // namespace live
