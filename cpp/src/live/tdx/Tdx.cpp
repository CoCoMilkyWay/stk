#include "live/tdx/Tdx.hpp"

#include "live/Board.hpp"
#include "live/tdx/Credit.hpp"
#include "live/tdx/Flash.hpp"
#include "live/tdx/Flow.hpp"
#include "live/tdx/Heap.hpp"
#include "live/tdx/Req.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Tables.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"
#include "live/tdx/Win.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <iterator>
#include <mutex>
#include <optional>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>

namespace live::tdx {

namespace {

using namespace std::chrono_literals;

constexpr auto kTick = 100ms;
constexpr double kEvery = 60;     // 定时刷新周期缺省 [s]
constexpr int kKicks[] = {1, 6};  // 下单成功后隔几秒各刷新一次 (限价单可能稍后才成交)
constexpr int kSyncTries = 2;     // 一轮里成交与持仓对不上 (两问之间又成交) 时最多连查几遍
constexpr auto kWatchLinger = 1s; // touch 之后算有人在看的时长

constexpr size_t idx(Part p) { return static_cast<size_t>(p); }

// 界面锁: 动客户端界面的 (下单 / 独占 / 引导) 一次只许一路; who = 占着的那路 (空 = 空闲).
// 同一线程可重入 (调仓整批占着, 逐单再进), who 记最外层. 直发不碰界面, 不占
class Gate {
public:
  class Hold {
  public:
    Hold(Gate &g, const char *who) : g_(g) {
      if (!g_.mu_.try_lock()) {
        const std::string by = g_.who();
        const Clock::time_point t0 = Clock::now();
        g_.mu_.lock();
        trace::ev("gate", {{"by", by}, {"ms", trace::ms_since(t0)}});
      }
      std::lock_guard lk(g_.who_mu_);
      outer_ = g_.who_;
      if (g_.who_.empty())
        g_.who_ = who;
    }
    ~Hold() {
      {
        std::lock_guard lk(g_.who_mu_);
        g_.who_ = outer_;
      }
      g_.mu_.unlock();
    }
    Hold(const Hold &) = delete;
    Hold &operator=(const Hold &) = delete;

  private:
    Gate &g_;
    std::string outer_;
  };

  std::string who() const {
    std::lock_guard lk(who_mu_);
    return who_;
  }

private:
  std::recursive_mutex mu_;
  mutable std::mutex who_mu_;
  std::string who_;
};

// 刷新线程与 Broker 接口共用; 线程持一份 shared_ptr, Tdx 析构只置停 (在途的一问做完即退)
struct Core {
  Gate gate;

  // work: 进程堆与接入 (shim / 请求层); 序: gate → work → mu
  std::mutex work;
  mem::HeapP heap;

  // mu: 发布的 State、进度、轮次与起因
  mutable std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<const State> st = std::make_shared<State>();
  Refresh prog{.every = kEvery};
  bool stop = false;
  uint64_t asked[2] = {}, done[2] = {};  // 各范围 (下标 Part) 要过 / 刷完的轮号
  std::optional<Clock::time_point> last; // 上一个全部的一轮结束时刻
  std::vector<Clock::time_point> kicks;
  int holds = 0;
  Clock::time_point touched{};

  bool watched(Clock::time_point now) const { return holds > 0 || now - touched < kWatchLinger; }

  // 要一轮 → 该范围的轮号
  uint64_t want(Part p) {
    std::lock_guard lk(mu);
    const uint64_t n = ++asked[idx(p)];
    cv.notify_all();
    return n;
  }
  // 刚下过单: 隔几秒各刷一次
  void kick() {
    std::lock_guard lk(mu);
    const Clock::time_point now = Clock::now();
    for (const int k : kKicks)
      kicks.push_back(now + std::chrono::seconds(k));
    std::sort(kicks.begin(), kicks.end());
  }

  // 改发布的 State (须持 mu)
  template <class F>
  void publish(F &&edit) {
    auto s = std::make_shared<State>(*st);
    ++s->ver;
    edit(*s);
    st = std::move(s);
  }
  void note(std::string step) {
    std::lock_guard lk(mu);
    prog.step = std::move(step);
  }

  // 接上客户端: 没接过或进程换了即重开堆; shim 不在线即注入; 请求层钩上 (钩着即只重设记哪些槽)
  void connect() {
    std::lock_guard wk(work);
    trace::Span span("接入");
    if (!heap || heap->pid() != mem::find_pid()) {
      heap = mem::attach();
      std::lock_guard lk(mu);
      publish([&](State &x) { x.who = strf("tdxw.exe pid %d", heap->pid()); });
    }
    if (!shim::online())
      win::attach();
    req::arm(heap);
  }

  // 引导: 插件在本客户端进程里还没发过的表, 各经界面查一次 (其引导页), 末了回持仓页 (持仓缺也由它补; 占界面锁).
  // 刷新线程调的 (step) 把走到哪页记进刷新进度
  template <class... S>
  void prime(bool step = false) {
    std::vector<ui::Open> miss;
    ((tables::seen<S>() ? void() : miss.push_back(S::kOpen)), ...);
    if (miss.empty())
      return;
    std::erase_if(miss, [](const ui::Open &o) { return std::string_view(o.page) == tables::Positions::kOpen.page; });
    miss.push_back(tables::Positions::kOpen);
    if (step)
      note("引导");
    const Gate::Hold g(gate, "引导");
    trace::Span span("引导");
    ui::show_bar();
    for (const ui::Open &o : miss) {
      if (step)
        note(strf("引导「%s」", o.page));
      std::ignore = ui::open(o);
    }
    (need(tables::seen<S>(), strf("经界面查过「%s」, 插件仍没发过其功能", S::kName)), ...);
  }

  // 一界面事务 (下单 / 独占): 段 (who {kw}) + 界面锁 + 接上客户端, 当前线程跑 fn
  template <class F>
  auto txn(const char *who, trace::Kw kw, F &&fn) {
    trace::Span span(who, std::move(kw));
    const Gate::Hold g(gate, who);
    connect();
    return fn();
  }
  // 一次直发拉取 (两融条款 / 资金流水): 段 (who {kw}) + 接上客户端 + 引导表 S…, 当前线程跑 fn
  template <class... S, class F>
  auto pull(const char *who, trace::Kw kw, F &&fn) {
    trace::Span span(who, std::move(kw));
    connect();
    prime<S...>();
    return fn();
  }

  bool stopped() const {
    std::lock_guard lk(mu);
    return stop;
  }

  // ---------- 刷新线程 ----------

  // 该刷新一轮了 → (起因, 范围); 不该 → 无 (须持 mu)
  std::optional<std::pair<const char *, Part>> due(Clock::time_point now) {
    if (asked[idx(Part::All)] > done[idx(Part::All)])
      return std::pair{"请求", Part::All};
    if (asked[idx(Part::Fills)] > done[idx(Part::Fills)])
      return std::pair{"等成交", Part::Fills};
    if (!watched(now))
      return std::nullopt;
    if (!kicks.empty() && now >= kicks.front()) {
      kicks.erase(kicks.begin());
      return std::pair{"下单后", Part::All};
    }
    if (!last)
      return std::pair{"首轮", Part::All};
    if (prog.every > 0 && now - *last >= std::chrono::duration<double>(prog.every))
      return std::pair{"定时", Part::All};
    return std::nullopt;
  }

  // 读一样、发布: fn() → 值
  template <class T, class F>
  void put(Got<T> State::*g, F &&fn) {
    note(State::name(g));
    trace::Span span(State::name(g));
    auto v = std::make_shared<const T>(fn());
    std::lock_guard lk(mu);
    publish([&](State &x) {
      (x.*g).v = std::move(v);
      (x.*g).at = clock_now();
    });
  }
  bool synced() {
    std::lock_guard lk(mu);
    return board::synced(*st->pos, *st->fills);
  }
  std::shared_ptr<const Keyed<Position>> held() {
    std::lock_guard lk(mu);
    return st->pos.v;
  }

  // 一轮: 成交、持仓两问之间又有成交即对不上, 连查至多 kSyncTries 遍, 仍不对留给上层标出; 再挂单、信用账户
  void round() {
    prime<tables::Fills, tables::Positions, tables::Pendings, tables::Assets, tables::Debts>(true);
    for (int i = 0; i < kSyncTries; ++i) {
      put(&State::fills, [] { return tables::read<tables::Fills>(); });
      put(&State::pos, [] { return tables::read<tables::Positions>(); });
      if (synced())
        break;
      trace::ev("unsynced", {{"try", i + 1}});
    }
    put(&State::orders, [] { return tables::read<tables::Pendings>(); });
    put(&State::acct, [&] { return credit::account(held()); });
  }
  // 只刷成交 (等卖单成交时轮询)
  void round_fills() {
    prime<tables::Fills>(true);
    put(&State::fills, [] { return tables::read<tables::Fills>(); });
  }

  void tick() {
    uint64_t n[2];
    std::optional<std::pair<const char *, Part>> d;
    {
      std::unique_lock lk(mu);
      if (!cv.wait_for(lk, kTick, [&] { return stop || (d = due(Clock::now())).has_value(); }) || stop)
        return;
      std::copy(std::begin(asked), std::end(asked), n);
    }
    const auto [why, part] = *d;
    trace::Span span("刷新", {{"why", why}, {"n", n[idx(part)]}});
    std::string down, msg;
    note("接入");
    try { // 错误边界: 接不上报在 down, 一轮失败报在 err, 都等下一个起因再刷
      connect();
    } catch (const Fail &f) {
      down = f.why;
    }
    if (down.empty()) {
      try {
        part == Part::All ? round() : round_fills();
      } catch (const Fail &f) {
        msg = f.why;
      }
    }
    std::lock_guard lk(mu);
    if (part == Part::All) {
      last = Clock::now();
      done[idx(Part::All)] = n[idx(Part::All)];
    }
    done[idx(Part::Fills)] = n[idx(Part::Fills)]; // 全部的一轮也刷了成交
    prog.step.clear();
    prog.down = down;
    prog.err = down.empty() ? msg : "";
    cv.notify_all();
  }

  void refresh_loop() {
    while (!stopped())
      tick();
  }
};

class Tdx final : public Broker {
public:
  Tdx() : c_(std::make_shared<Core>()) {
    std::thread([c = c_] { c->refresh_loop(); }).detach();
  }
  ~Tdx() override {
    {
      std::lock_guard lk(c_->mu);
      c_->stop = true;
    }
    c_->cv.notify_all();
  }

  std::shared_ptr<const State> state() const override {
    std::lock_guard lk(c_->mu);
    return c_->st;
  }
  Refresh refresh() const override {
    std::lock_guard lk(c_->mu);
    return c_->prog;
  }

  void touch() override {
    std::lock_guard lk(c_->mu);
    c_->touched = Clock::now();
  }
  void hold(int d) override {
    {
      std::lock_guard lk(c_->mu);
      c_->holds += d;
      assert(c_->holds >= 0);
    }
    c_->cv.notify_all();
  }

  void request(Part p) override { c_->want(p); }
  void sync(Part p) override {
    const uint64_t n = c_->want(p);
    std::unique_lock lk(c_->mu);
    c_->cv.wait(lk, [&] { return c_->done[idx(p)] >= n || c_->stop; });
    if (c_->stop)
      fail("已断开");
    if (!c_->prog.down.empty())
      fail("接不上客户端: " + c_->prog.down);
    if (!c_->prog.err.empty())
      fail("刷新失败: " + c_->prog.err);
  }
  void every(double s) override {
    assert(s >= 0);
    {
      std::lock_guard lk(c_->mu);
      c_->prog.every = s;
    }
    c_->cv.notify_all();
  }

  const std::vector<std::string> &order_steps() const override { return flash::steps(); }
  Placed place(const OrderReq &o, const Step &step) override {
    const trace::Kw kw = {{"code", o.sec.code}, {"name", o.sec.name}, {"side", side_name(o.side)}, {"credit", credit_name(o.credit)}, {"px", o.px}, {"qty", o.qty}, {"size", o.size}};
    const Placed p = c_->txn("下单", kw, [&] { return flash::place(o, step); });
    c_->kick();
    return p;
  }
  void exclusive(const char *who, const std::function<void()> &fn) override { c_->txn(who, trace::Kw::object(), fn); }
  Terms pull_terms(const Report &r) override {
    return c_->pull<tables::Margins, tables::Collats>("两融条款", trace::Kw::object(), [&] { return credit::pull(r); });
  }
  std::vector<CashFlow> pull_flows(misc::Ymd from, const Report &r) override {
    return c_->pull<tables::CashFlows>("资金流水", {{"from", from}}, [&] { return flow::pull(from, r); });
  }

private:
  std::shared_ptr<Core> c_;
};

} // namespace

std::unique_ptr<Broker> make() { return std::make_unique<Tdx>(); }

} // namespace live::tdx
