#include "live/tdx/Tdx.hpp"

#include "live/Board.hpp"
#include "live/tdx/Console.hpp"
#include "live/tdx/Credit.hpp"
#include "live/tdx/Flash.hpp"
#include "live/tdx/Flow.hpp"
#include "live/tdx/Mem.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Tables.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Win.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <iterator>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

namespace live::tdx {

namespace {

using namespace std::chrono_literals;

constexpr auto kTick = 100ms;
constexpr auto kFullEvery = 60s;  // 全量扫描最长间隔
constexpr double kEvery = 60;     // 定时刷新周期缺省 [s]
constexpr int kKicks[] = {1, 6};  // 下单成功后隔几秒各刷新一次 (限价单可能稍后才成交)
constexpr int kSyncTries = 2;     // 一轮里成交与持仓对不上 (两次查询之间又成交) 时最多连查几遍
constexpr auto kWatchLinger = 1s; // touch 之后算有人在看的时长
constexpr double kQuiet = 60;     // 客户端这么久 [s] 内有真实键鼠输入即算有人在操作, 定时刷新推后

using PosSnap = mem::Snap<tables::Positions>;
using FillSnap = mem::Snap<tables::Fills>;

constexpr size_t idx(Part p) { return static_cast<size_t>(p); }

// 界面锁: 客户端界面自动化 (刷新 / 下单 / 两融条款 / 资金流水 / 调仓) 一次只许一路; who = 占着的那路 (空 = 空闲).
// 同一线程可重入 (调仓整批占着, 逐单再进), who 记最外层
class Gate {
public:
  class Hold {
  public:
    Hold(Gate &g, const char *who) : g_(g) {
      if (!g_.mu_.try_lock()) {
        const std::string by = g_.who();
        const Clock::time_point t0 = Clock::now();
        ++g_.waiting_;
        g_.mu_.lock();
        --g_.waiting_;
        ++g_.granted_;
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

    // 有别路在等即先让它占、做完再接着占 (刷新一轮在两步之间让下单等先走); 只许最外层调.
    // 让出期间界面任人改, 接着的一步须自己切到要的页. 锁不保证先等先得: 放开后等在等的那路占到了再去抢
    void yield() {
      assert(outer_.empty());
      if (g_.waiting_ == 0)
        return;
      std::string me;
      {
        std::lock_guard lk(g_.who_mu_);
        me = std::exchange(g_.who_, "");
      }
      const uint64_t n = g_.granted_;
      const Clock::time_point t0 = Clock::now();
      g_.mu_.unlock();
      poll([&] { return g_.granted_ != n; }, 1, 0.001);
      g_.mu_.lock();
      {
        std::lock_guard lk(g_.who_mu_);
        g_.who_ = std::move(me);
      }
      trace::ev("yield", {{"ms", trace::ms_since(t0)}});
    }

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
  std::atomic<int> waiting_{0};      // 在等的路数
  std::atomic<uint64_t> granted_{0}; // 等过才占到的次数
};

// 盯着的一样 (被动换代的表): 表、当前快照、交易栏页签、发布到 State 的哪样、快照 → 发布值
template <class S, class T>
struct Watched {
  using Snap = mem::Snap<S>;
  using Value = T;
  mem::Table<S> t;
  std::optional<Snap> snap;
  const char *tab;
  Got<T> State::*got;
  T (*view)(const Snap &);
};

// 页上的条数标签 (有才有): 清空 / 作应答到了的判据 (t 在 act 里才定, 故引用)
void clear(const std::optional<ui::Tab> &t) {
  if (t && t->count)
    t->count->clear();
}
mem::Count count_of(const std::optional<ui::Tab> &t) {
  return [&t]() -> std::optional<int> { return t && t->count ? t->count->read(1) : std::nullopt; };
}

// 两个后台线程与 Broker 接口共用; 线程各持一份 shared_ptr, Tdx 析构只置停 (在途的界面操作做完即退)
struct Core {
  Gate gate;

  // work: 盯着的两样与进程堆 (盯持仓的一步、刷新一轮的取数 / 并入都在其内); 序: work → mu
  std::mutex work;
  Watched<tables::Fills, Fills> fills{{}, {}, "成交", &State::fills, [](const FillSnap &s) { return Fills{s.rows, s.index}; }};
  Watched<tables::Positions, Keyed<Position>> pos{{}, {}, "持仓", &State::pos, [](const PosSnap &s) { return s.index; }};
  mem::HeapP heap;
  Clock::time_point scan_at{};

  // mu: 发布的 State、进度、轮次与起因
  mutable std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<const State> st = std::make_shared<State>();
  Refresh prog{.every = kEvery};
  bool lost = true; // 出过错: 下一步先接回客户端、全量重扫
  bool stop = false;
  uint64_t asked[2] = {}, done[2] = {};           // 各范围 (下标 Part) 要过 / 刷完的轮号
  std::optional<Clock::time_point> last;          // 上一个全部的一轮结束时刻
  std::shared_ptr<const Keyed<Position>> checked; // 上一个全部的一轮结束时的持仓
  std::vector<Clock::time_point> kicks;
  Clock::time_point quiet{}; // 有人在操作客户端: 此前不定时刷新
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

  // 改发布的 State (须持 mu)
  template <class F>
  void publish(F &&edit) {
    auto s = std::make_shared<State>(*st);
    ++s->ver;
    edit(*s);
    st = std::move(s);
  }
  // 一样并进 State (须持 mu): v 非空即换值; stamp = 客户端刚重查过, 记下读于几时
  template <class T>
  void put(Got<T> State::*g, std::shared_ptr<const T> v, bool stamp) {
    if (!v && !stamp)
      return;
    publish([&](State &x) {
      if (v)
        (x.*g).v = std::move(v);
      if (stamp)
        (x.*g).at = clock_now();
    });
  }
  // 盯着的一样换上快照 s (须持 work): 换代了才换值
  template <class W>
  void fresh(W &w, typename W::Snap s, bool stamp) {
    std::shared_ptr<const typename W::Value> v;
    if (!w.snap || !(*w.snap == s)) {
      trace::ev("gen", {{"tab", State::name(w.got)}, {"rows", s.rows.size()}, {"addr", mem::addrs(s.mark)}});
      v = std::make_shared<const typename W::Value>(w.view(s));
      w.snap = std::move(s);
    }
    std::lock_guard lk(mu);
    put(w.got, std::move(v), stamp);
  }
  void note(std::string step) {
    std::lock_guard lk(mu);
    prog.step = std::move(step);
  }

  // 接上客户端进程堆 (须持 work): 没接过或进程换了即重开
  const mem::HeapP &attach() {
    if (!heap || heap->pid() != mem::find_pid()) {
      heap = mem::attach();
      std::lock_guard lk(mu);
      publish([&](State &x) { x.who = strf("tdxw.exe pid %d", heap->pid()); });
    }
    return heap;
  }
  // 接上进程堆与 shim (须持 work): 客户端不在即 Fail, 不去空注入; 动界面的事务开头各调一次
  const mem::HeapP &connect() {
    attach();
    if (!shim::online()) {
      trace::Span attaching("接入");
      win::attach();
    }
    return heap;
  }
  mem::HeapP connected() {
    std::lock_guard wk(work);
    return connect();
  }

  // 一遍全量扫出成交与持仓 (并重找进程; 须持 work)
  void scan() {
    scan_at = Clock::now();
    const mem::HeapP &h = attach();
    const auto hits = h->scan({&tables::Fills::kShape, &tables::Positions::kShape});
    FillSnap f = fills.t.pick(h, hits[0]);
    PosSnap p = pos.t.pick(h, hits[1]);
    fresh(fills, std::move(f), false);
    fresh(pos, std::move(p), false);
  }
  void load() {
    if (!pos.snap)
      scan();
  }

  // ---------- 盯持仓线程 ----------

  // loud = false: 掉线期间的重试, 不记日志
  void step(bool loud) {
    const Clock::time_point now = Clock::now();
    bool was_lost;
    {
      std::lock_guard lk(mu);
      if (!watched(now))
        return;
      was_lost = lost;
    }
    trace::Span span("盯持仓", trace::Kw::object(), loud);
    std::lock_guard wk(work);
    if (was_lost)
      connect();
    if (was_lost || !pos.snap || now - scan_at >= kFullEvery) {
      scan();
      std::lock_guard lk(mu);
      lost = false;
      prog.down.clear();
    } else {
      PosSnap p = pos.t.follow(*pos.snap);
      if (!(p == *pos.snap))
        fresh(pos, std::move(p), true);
    }
  }

  bool stopped() const {
    std::lock_guard lk(mu);
    return stop;
  }

  std::string down() const {
    std::lock_guard lk(mu);
    return prog.down;
  }

  // 掉线期间原因不变的重试不记; 接回 (back) 与原因变了 (down) 各记一行
  void watch_loop() {
    while (!stopped()) {
      const std::string was = down();
      try {
        step(was.empty());
        if (!was.empty() && down().empty()) {
          trace::Span span("盯持仓");
          trace::ev("back", {{"down", was}});
        }
      } catch (const Fail &f) { // 错误边界: 记下原因, 下一步先接回客户端
        {
          std::lock_guard lk(mu);
          lost = true;
          prog.down = f.why;
        }
        if (!was.empty() && was != f.why) {
          trace::Span span("盯持仓");
          trace::ev("down", {{"why", f.why}});
        }
        std::this_thread::sleep_for(1s);
      }
      std::this_thread::sleep_for(kTick);
    }
  }

  // ---------- 刷新线程 ----------

  // 起因 + 范围; timed = 定时的 (有人在操作客户端即推后)
  struct Due {
    const char *why;
    Part part;
    bool timed;
  };
  // 该刷新一轮了 → 起因; 不该 → 无 (须持 mu)
  std::optional<Due> due(Clock::time_point now) {
    if (asked[idx(Part::All)] > done[idx(Part::All)])
      return Due{"请求", Part::All, false};
    if (asked[idx(Part::Fills)] > done[idx(Part::Fills)])
      return Due{"等成交", Part::Fills, false};
    if (!watched(now))
      return std::nullopt;
    if (!kicks.empty() && now >= kicks.front()) {
      kicks.erase(kicks.begin());
      return Due{"下单后", Part::All, false};
    }
    if (!last)
      return Due{"首轮", Part::All, false};
    if (now < quiet)
      return std::nullopt;
    if (prog.every > 0 && now - *last >= std::chrono::duration<double>(prog.every))
      return Due{"定时", Part::All, true};
    if (checked && st->pos && st->fills && st->pos.v != checked && !board::synced(*st->pos, *st->fills))
      return Due{"持仓与成交对不上", Part::All, true};
    return std::nullopt;
  }

  // 有人在操作客户端 (kQuiet 秒内有真实键鼠输入) → 定时刷新推到那之后; shim 不在线即不知, 由这一轮去接
  bool busy() {
    if (!shim::online())
      return false;
    const double s = win::idle();
    if (s >= kQuiet)
      return false;
    std::lock_guard lk(mu);
    quiet = Clock::now() + std::chrono::duration_cast<Clock::duration>(Secs(kQuiet - s));
    return true;
  }

  // 被动换代的一样重查: 切到其页签、清条数标签、点刷新, 等快照换代 (条数标签作不作数由读法定)
  template <class W>
  void refresh(W &w) {
    note(State::name(w.got));
    trace::Span span(State::name(w.got));
    const typename W::Snap old = [&] {
      std::lock_guard wk(work);
      return *w.snap;
    }();
    const std::optional<ui::Tab> tab = ui::open_tab(w.tab);
    clear(tab);
    ui::click_tool("刷新");
    typename W::Snap n = w.t.wait(old, count_of(tab));
    std::lock_guard wk(work);
    fresh(w, std::move(n), true);
  }
  // act 驱动的一样重查: fn() 让客户端重查并读回 → 发布值
  template <class T, class F>
  void ask(Got<T> State::*g, F &&fn) {
    note(State::name(g));
    trace::Span span(State::name(g));
    auto v = std::make_shared<const T>(fn());
    std::lock_guard lk(mu);
    put(g, std::move(v), true);
  }
  bool synced() {
    std::lock_guard lk(mu);
    return board::synced(*st->pos, *st->fills);
  }
  std::shared_ptr<const Keyed<Position>> held() {
    std::lock_guard lk(mu);
    return st->pos.v;
  }

  // 占界面锁 (别路占着即等它做完)
  Gate::Hold hold() {
    if (const std::string w = gate.who(); !w.empty())
      note("等界面 (" + w + ")");
    return Gate::Hold(gate, "刷新");
  }
  // 接上客户端、交易栏展开
  mem::HeapP enter() {
    mem::HeapP h;
    {
      std::lock_guard wk(work);
      h = connect();
      load();
    }
    ui::show_bar();
    return h;
  }

  // 一轮: 成交、持仓两次查询之间又有成交即对不上, 连查至多 kSyncTries 遍, 仍不对留给上层标出;
  // 挂单、信用资产、融资负债三问接成一串 (每问应答的那遍扫即下一问的问前); 查完回持仓页
  // (撤单页上另画着一个撤单动作键, 故切去时须在持仓页, 页签唯一).
  // 各样之间有别路 (下单等) 在等界面即先让它 (三问一串之内不让: 让出期间客户端可能自己重查, 新块就不是这问的应答了)
  void round() {
    Gate::Hold g = hold();
    const mem::HeapP h = enter();
    for (int i = 0; i < kSyncTries; ++i) {
      g.yield();
      refresh(fills);
      g.yield();
      refresh(pos);
      if (synced())
        break;
      trace::ev("unsynced", {{"try", i + 1}});
    }
    g.yield();
    mem::Chain q(h, {&tables::Pendings::kShape, &tables::Assets::kShape, &tables::Debts::kShape});
    ask(&State::orders, [&] {
      // 「撤单」页: 切页与刷新同在一问内 (切页自带一次查询, 其应答可能早于刷新); 等页出来再点刷新, 免得刷到原先那页
      std::optional<ui::Tab> tab;
      const auto act = [&] {
        tab = ui::open_tab("撤单");
        clear(tab);
        ui::click_tool("刷新");
      };
      return q.ask<tables::Pendings>(act, count_of(tab)).rows;
    });
    ask(&State::acct, [&, p = held()] {
      Account a = credit::account(q, p);
      trace::ev("put", {{"what", State::name(&State::acct)},
                        {"assets", a.assets},
                        {"line", a.line},
                        {"debt", a.debt},
                        {"net", a.net},
                        {"avail", a.avail},
                        {"cash", a.cash},
                        {"debts", a.debts.size()}});
      return a;
    });
    ui::click_tool("持仓");
  }

  // 只刷成交 (等卖单成交时轮询); 查完回持仓页
  void round_fills() {
    const Gate::Hold g = hold();
    enter();
    refresh(fills);
    ui::click_tool("持仓");
  }

  void tick() {
    uint64_t n[2];
    std::optional<Due> d;
    {
      std::unique_lock lk(mu);
      if (!cv.wait_for(lk, kTick, [&] { return stop || (d = due(Clock::now())).has_value(); }) || stop)
        return;
      std::copy(std::begin(asked), std::end(asked), n);
    }
    if (d->timed && busy()) {
      trace::Span span("刷新推后", {{"why", d->why}});
      trace::ev("user", {{"quiet", kQuiet}});
      return;
    }
    trace::Span span("刷新", {{"why", d->why}, {"n", n[idx(d->part)]}});
    std::string msg;
    try {
      d->part == Part::All ? round() : round_fills();
    } catch (const Fail &f) { // 错误边界: 报在进度里, 等下一个起因再刷
      msg = f.why;
    }
    std::lock_guard lk(mu);
    if (d->part == Part::All) {
      checked = st->pos.v;
      last = Clock::now();
      done[idx(Part::All)] = n[idx(Part::All)];
    }
    done[idx(Part::Fills)] = n[idx(Part::Fills)]; // 全部的一轮也刷了成交
    lost = lost || !msg.empty();
    prog.step.clear();
    prog.err = msg;
    cv.notify_all();
  }

  void refresh_loop() {
    while (!stopped()) {
      try {
        tick();
      } catch (const Fail &f) { // 错误边界: 起因判不了 (持仓与成交自相矛盾) 也只报在进度里, 原因变了才记
        bool fresh;
        {
          std::lock_guard lk(mu);
          fresh = prog.err != f.why;
          prog.step.clear();
          prog.err = f.why;
        }
        if (fresh) {
          trace::Span span("刷新");
          trace::ev("fail", {{"why", f.why}});
        }
        std::this_thread::sleep_for(1s);
      }
    }
  }
};

class Tdx final : public Broker {
public:
  Tdx() : c_(std::make_shared<Core>()) {
    std::thread([c = c_] { c->watch_loop(); }).detach();
    std::thread([c = c_] { c->refresh_loop(); }).detach();
  }
  ~Tdx() override {
    {
      std::lock_guard lk(c_->mu);
      c_->stop = true;
    }
    c_->cv.notify_all();
  }

  const char *name() const override { return "通达信"; }

  std::shared_ptr<const State> state() const override {
    std::lock_guard lk(c_->mu);
    return c_->st;
  }
  Refresh refresh() const override {
    std::lock_guard lk(c_->mu);
    Refresh r = c_->prog;
    r.user = Clock::now() < c_->quiet;
    return r;
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
  void kick() override {
    std::lock_guard lk(c_->mu);
    const Clock::time_point now = Clock::now();
    for (const int k : kKicks)
      c_->kicks.push_back(now + std::chrono::seconds(k));
    std::sort(c_->kicks.begin(), c_->kicks.end());
  }

  const std::vector<std::string> &order_steps() const override { return flash::steps(); }
  Placed place(const OrderReq &o, const Step &step) override {
    return txn("下单",
               {{"code", o.sec.code}, {"name", o.sec.name}, {"side", side_name(o.side)}, {"credit", credit_name(o.credit)}, {"px", o.px}, {"qty", o.qty}, {"size", o.size}},
               [&](const mem::HeapP &) {
                 Placed p = flash::place(o, step);
                 trace::ev("placed", {{"qty", p.qty}, {"px", p.px}, {"msg", p.msg}});
                 return p;
               });
  }
  void exclusive(const char *who, const std::function<void()> &fn) override {
    txn(who, trace::Kw::object(), [&](const mem::HeapP &) { fn(); });
  }
  Terms pull_terms(const Log &log, const Live &live) override {
    return txn("两融条款", trace::Kw::object(), [&](const mem::HeapP &h) {
      Terms t = credit::pull(h, log, live);
      trace::ev("terms", {{"n", t.size()}});
      return t;
    });
  }
  std::vector<CashFlow> pull_flows(misc::Ymd from, const Log &log, const Live &live) override {
    return txn("资金流水", {{"from", from}}, [&](const mem::HeapP &h) {
      std::vector<CashFlow> f = flow::pull(h, from, log, live);
      trace::ev("flows", {{"n", f.size()}});
      return f;
    });
  }

  void console() override {
    if (!console_)
      console_ = std::make_unique<tdx::console::Console>();
    console_->draw();
  }

private:
  // 一件界面事务: 段 (who {kw}) + 界面锁 + 接上客户端, 当前线程跑 fn(进程堆)
  template <class F>
  auto txn(const char *who, trace::Kw kw, F &&fn) {
    trace::Span span(who, std::move(kw));
    const Gate::Hold g(c_->gate, who);
    return fn(c_->connected());
  }

  std::shared_ptr<Core> c_;
  std::unique_ptr<tdx::console::Console> console_; // 首次打开 Client 页才建
};

} // namespace

std::unique_ptr<Broker> make() { return std::make_unique<Tdx>(); }

} // namespace live::tdx
