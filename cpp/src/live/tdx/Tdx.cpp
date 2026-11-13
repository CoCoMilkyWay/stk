#include "live/tdx/Tdx.hpp"

#include "live/Board.hpp"
#include "live/tdx/Console.hpp"
#include "live/tdx/Credit.hpp"
#include "live/tdx/Flash.hpp"
#include "live/tdx/Mem.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Tables.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Win.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>

namespace live::tdx {

namespace {

using namespace std::chrono_literals;

constexpr auto kTick = 100ms;
constexpr auto kFullEvery = 60s;  // 全量扫描最长间隔
constexpr double kEvery = 60;     // 定时刷新周期缺省 [s]
constexpr int kKicks[] = {1, 6};  // 下单成功后隔几秒各刷新一次 (限价单可能稍后才成交)
constexpr int kSyncTries = 2;     // 一轮里成交与持仓对不上 (两次查询之间又成交) 时最多连查几遍
constexpr auto kWatchLinger = 1s; // touch 之后算有人在看的时长

using PosSnap = mem::Snap<tables::Positions>;
using FillSnap = mem::Snap<tables::Fills>;

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

// 两个后台线程与 Broker 接口共用; 线程各持一份 shared_ptr, Tdx 析构只置停 (在途的界面操作做完即退)
struct Core {
  ui::Gate gate;

  // work: 盯着的两样与进程堆 (盯持仓的一步、刷新一轮的取数 / 并入都在其内); 序: work → mu
  std::mutex work;
  Watched<tables::Fills, Fills> fills{{}, {}, "成交", &State::fills, [](const FillSnap &s) { return Fills{s.rows, s.index}; }};
  Watched<tables::Positions, Keyed<Position>> pos{{}, {}, "持仓", &State::pos, [](const PosSnap &s) { return s.index; }};
  mem::Table<tables::Pendings> pendings;
  credit::Tables credit;
  mem::HeapP heap;
  Clock::time_point scan_at{};

  // mu: 发布的 State、进度、轮次与起因
  mutable std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<const State> st = std::make_shared<State>();
  Refresh prog{.every = kEvery};
  bool lost = true; // 出过错: 下一步先接回客户端、全量重扫
  bool stop = false;
  uint64_t asked = 0, done = 0;
  std::optional<Clock::time_point> last;          // 上一轮结束时刻
  std::shared_ptr<const Keyed<Position>> checked; // 上一轮结束时的持仓
  std::vector<Clock::time_point> kicks;
  int holds = 0;
  Clock::time_point touched{};

  bool watched(Clock::time_point now) const { return holds > 0 || now - touched < kWatchLinger; }

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

  // 该刷新一轮了 → 起因; 不该 → nullptr (须持 mu)
  const char *due(Clock::time_point now) {
    if (asked > done)
      return "请求";
    if (!watched(now))
      return nullptr;
    if (!kicks.empty() && now >= kicks.front()) {
      kicks.erase(kicks.begin());
      return "下单后";
    }
    if (!last)
      return "首轮";
    if (prog.every > 0 && now - *last >= std::chrono::duration<double>(prog.every))
      return "定时";
    if (checked && st->pos && st->fills && st->pos.v != checked && !board::synced(*st->pos, *st->fills))
      return "持仓与成交对不上";
    return nullptr;
  }

  // 重查盯着的一样: 切到其页签点刷新, 等快照换代
  template <class W>
  void refresh(W &w) {
    note(State::name(w.got));
    trace::Span span(State::name(w.got));
    const typename W::Snap old = [&] {
      std::lock_guard wk(work);
      return *w.snap;
    }();
    ui::click_tool(w.tab);
    ui::click_tool("刷新");
    typename W::Snap n = w.t.wait(old);
    std::lock_guard wk(work);
    fresh(w, std::move(n), true);
  }
  bool synced() {
    std::lock_guard lk(mu);
    return board::synced(*st->pos, *st->fills);
  }

  // 一轮: 成交、持仓两次查询之间又有成交即对不上, 连查至多 kSyncTries 遍, 仍不对留给上层标出;
  // 挂单、信用资产、融资负债三问接成一串 (每问应答的那遍扫即下一问的问前); 查完回持仓页
  // (撤单页上另画着一个撤单动作键, 故切去时须在持仓页, 页签唯一)
  void round() {
    if (const std::string w = gate.who(); !w.empty())
      note("等界面 (" + w + ")");
    ui::Gate::Hold g(gate, "刷新");
    mem::HeapP h;
    {
      std::lock_guard wk(work);
      h = connect();
      load();
    }
    ui::show_bar();
    for (int i = 0; i < kSyncTries; ++i) {
      refresh(fills);
      refresh(pos);
      if (synced())
        break;
      trace::ev("unsynced", {{"try", i + 1}});
    }
    mem::Chain q(h, {&tables::Pendings::kShape, &tables::Assets::kShape, &tables::Debts::kShape});
    note(State::name(&State::orders));
    std::shared_ptr<const Keyed<Position>> held;
    {
      trace::Span span(State::name(&State::orders));
      // 「撤单」页: 切页与刷新同在一问内 (切页自带一次查询, 其应答可能早于刷新)
      auto orders = q.ask(pendings, [] {
        ui::click_tool("撤单");
        ui::click_tool("刷新");
      });
      std::lock_guard lk(mu);
      put(&State::orders, std::make_shared<const std::vector<Pending>>(std::move(orders.rows)), true);
      held = st->pos.v;
    }
    note(State::name(&State::acct));
    {
      trace::Span span(State::name(&State::acct));
      auto a = std::make_shared<const Account>(credit::account(credit, q, *held));
      trace::ev("put", {{"what", State::name(&State::acct)},
                        {"assets", a->assets},
                        {"line", a->line},
                        {"debt", a->debt},
                        {"net", a->net},
                        {"avail", a->avail},
                        {"cash", a->cash},
                        {"debts", a->debts.size()}});
      std::lock_guard lk(mu);
      put(&State::acct, std::move(a), true);
    }
    ui::click_tool("持仓");
  }

  void tick() {
    uint64_t n;
    const char *why = nullptr;
    {
      std::unique_lock lk(mu);
      if (!cv.wait_for(lk, kTick, [&] { return stop || (why = due(Clock::now())) != nullptr; }) || stop)
        return;
      n = asked;
    }
    trace::Span span("刷新", {{"why", why}, {"n", n}});
    std::string msg;
    try {
      round();
    } catch (const Fail &f) { // 错误边界: 报在进度里, 等下一个起因再刷
      msg = f.why;
    }
    std::lock_guard lk(mu);
    checked = st->pos.v;
    last = Clock::now();
    done = n;
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

  uint64_t request() override {
    std::lock_guard lk(c_->mu);
    ++c_->asked;
    c_->cv.notify_all();
    return c_->asked;
  }
  void sync() override {
    const uint64_t n = request();
    std::unique_lock lk(c_->mu);
    c_->cv.wait(lk, [&] { return c_->done >= n || c_->stop; });
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
    trace::Span span("下单", {{"code", o.sec.code}, {"name", o.sec.name}, {"side", side_name(o.side)}, {"credit", credit_name(o.credit)}, {"px", o.px}, {"qty", o.qty}, {"size", o.size}});
    ui::Gate::Hold g(c_->gate, "下单");
    c_->connected();
    Placed p = flash::place(o, step);
    trace::ev("placed", {{"qty", p.qty}, {"px", p.px}, {"msg", p.msg}});
    return p;
  }
  void exclusive(const char *who, const std::function<void()> &fn) override {
    trace::Span span(who);
    ui::Gate::Hold g(c_->gate, who);
    c_->connected();
    fn();
  }
  Terms pull_terms(const Log &log, const Live &live) override {
    trace::Span span("两融条款");
    ui::Gate::Hold g(c_->gate, "两融条款");
    Terms t = credit::pull(c_->credit, c_->connected(), log, live);
    trace::ev("terms", {{"n", t.size()}});
    return t;
  }

  void console() override {
    if (!console_)
      console_ = std::make_unique<tdx::console::Console>();
    console_->draw();
  }

private:
  std::shared_ptr<Core> c_;
  std::unique_ptr<tdx::console::Console> console_; // 首次打开 Client 页才建
};

} // namespace

std::unique_ptr<Broker> make() { return std::make_unique<Tdx>(); }

} // namespace live::tdx
