#include "live/tdx/Tdx.hpp"

#include "live/Board.hpp"
#include "live/tdx/Console.hpp"
#include "live/tdx/Credit.hpp"
#include "live/tdx/Flash.hpp"
#include "live/tdx/Mem.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Tables.hpp"
#include "live/tdx/Ui.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>

namespace live::tdx {

namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

constexpr auto kTick = 100ms;
constexpr auto kFullEvery = 60s;  // 全量扫描最长间隔
constexpr double kEvery = 60;     // 定时刷新周期缺省 [s]
constexpr int kKicks[] = {1, 6};  // 下单成功后隔几秒各刷新一次 (限价单可能稍后才成交)
constexpr int kSyncTries = 2;     // 一轮里成交与持仓对不上 (两次查询之间又成交) 时最多连查几遍
constexpr auto kWatchLinger = 1s; // touch 之后算有人在看的时长

using PosSnap = mem::Snap<tables::Positions>;
using FillSnap = mem::Snap<tables::Fills>;

const std::vector<std::string> kRefresh = {"成交", "持仓", "挂单", "信用账户"};

// 两个后台线程与 Broker 接口共用; 线程各持一份 shared_ptr, Tdx 析构只置停 (在途的界面操作做完即退)
struct Core {
  ui::Gate gate;
  mem::Table<tables::Positions> pos_t;
  mem::Table<tables::Fills> fills_t;
  mem::Table<tables::Orders> orders_t;
  credit::Tables credit_t;

  // work: 快照与进程 (盯持仓的一步、刷新一轮的取数 / 并入都在其内); 序: work → mu
  std::mutex work;
  std::optional<PosSnap> pos;
  std::optional<FillSnap> fills;
  int pid = 0;
  Clock::time_point scan_at{};

  // mu: 发布的 State、进度、轮次与起因
  mutable std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<const State> st = std::make_shared<State>();
  Refresh prog{.steps = kRefresh, .every = kEvery};
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

  // 客户端重查过的一样并进 State (须持 work); stamp = 记下读于几时 (全量扫不记)
  void fresh_pos(PosSnap s, bool stamp) {
    const bool changed = !pos || !(*pos == s);
    if (changed)
      pos = std::move(s);
    std::lock_guard lk(mu);
    if (changed)
      publish([&](State &x) {
        x.pos = std::make_shared<const Keyed<Position>>(pos->by_code);
        x.who = strf("tdxw.exe pid %d", pos->pid);
      });
    if (stamp)
      prog.at["持仓"] = clock_now();
  }
  void fresh_fills(FillSnap s, bool stamp) {
    const bool changed = !fills || !(*fills == s);
    if (changed)
      fills = std::move(s);
    std::lock_guard lk(mu);
    if (changed)
      publish([&](State &x) { x.fills = std::make_shared<const Fills>(Fills{fills->rows, fills->by_code}); });
    if (stamp)
      prog.at["成交"] = clock_now();
  }
  template <class F>
  void fresh(const char *name, F &&edit) {
    std::lock_guard lk(mu);
    publish(edit);
    prog.at[name] = clock_now();
  }
  void note(std::string step) {
    std::lock_guard lk(mu);
    prog.step = std::move(step);
  }

  // 一遍全量扫出持仓与成交 (并重找进程; 须持 work)
  void scan() {
    scan_at = Clock::now();
    pid = mem::attach();
    const auto hits = mem::scan(pid, {&tables::Positions::kShape, &tables::Fills::kShape});
    PosSnap p = pos_t.pick(pid, hits[0]);
    FillSnap f = fills_t.pick(pid, hits[1]);
    fresh_fills(std::move(f), false);
    fresh_pos(std::move(p), false);
  }
  void load() {
    if (!pos)
      scan();
  }

  // ---------- 盯持仓线程 ----------

  void step() {
    const Clock::time_point now = Clock::now();
    bool was_lost;
    {
      std::lock_guard lk(mu);
      if (!watched(now))
        return;
      was_lost = lost;
    }
    std::lock_guard wk(work);
    if (was_lost) {
      mem::attach(); // 客户端不在即 Fail, 不去空注入
      if (!shim::online())
        ui::attach();
    }
    if (was_lost || !pos || now - scan_at >= kFullEvery) {
      scan();
      std::lock_guard lk(mu);
      lost = false;
      prog.down.clear();
    } else {
      PosSnap p = pos_t.follow(*pos);
      if (!(p == *pos))
        fresh_pos(std::move(p), true);
    }
  }

  bool stopped() const {
    std::lock_guard lk(mu);
    return stop;
  }

  void watch_loop() {
    while (!stopped()) {
      try {
        step();
      } catch (const Fail &f) { // 错误边界: 记下原因, 下一步先接回客户端
        {
          std::lock_guard lk(mu);
          lost = true;
          prog.down = f.why;
        }
        std::this_thread::sleep_for(1s);
      }
      std::this_thread::sleep_for(kTick);
    }
  }

  // ---------- 刷新线程 ----------

  // 该刷新一轮了 (须持 mu)
  bool due(Clock::time_point now) {
    if (asked > done)
      return true;
    if (!watched(now))
      return false;
    if (!kicks.empty() && now >= kicks.front()) {
      kicks.erase(kicks.begin());
      return true;
    }
    if (!last || (prog.every > 0 && now - *last >= std::chrono::duration<double>(prog.every)))
      return true;
    return checked && st->pos && st->fills && st->pos != checked && !board::synced(*st->pos, *st->fills);
  }

  void refresh_fills() {
    note("成交");
    FillSnap old;
    {
      std::lock_guard wk(work);
      old = *fills;
    }
    ui::click_tool("成交");
    ui::click_tool("刷新");
    FillSnap n = fills_t.wait(old);
    std::lock_guard wk(work);
    fresh_fills(std::move(n), true);
  }
  void refresh_pos() {
    note("持仓");
    PosSnap old;
    {
      std::lock_guard wk(work);
      old = *pos;
    }
    ui::click_tool("持仓");
    ui::click_tool("刷新");
    PosSnap n = pos_t.wait(old);
    std::lock_guard wk(work);
    fresh_pos(std::move(n), true);
  }
  bool synced() {
    std::lock_guard lk(mu);
    return board::synced(*st->pos, *st->fills);
  }

  // 一轮: 成交、持仓两次查询之间又有成交即对不上, 连查至多 kSyncTries 遍, 仍不对留给上层标出; 查完回持仓页
  // (撤单页上另画着一个撤单动作键, 故切去时须在持仓页, 页签唯一)
  void round() {
    if (const std::string w = gate.who(); !w.empty())
      note("等界面 (" + w + ")");
    ui::Gate::Hold g(gate, "刷新");
    int p;
    {
      std::lock_guard wk(work);
      load();
      p = pid;
    }
    ui::show_bar();
    for (int i = 0; i < kSyncTries; ++i) {
      refresh_fills();
      refresh_pos();
      if (synced())
        break;
    }
    note("挂单");
    // 「撤单」页: 切页与刷新同在 ask 内 (切页自带一次查询, 其应答可能早于刷新)
    auto orders = orders_t.ask(p, [] {
      ui::click_tool("撤单");
      ui::click_tool("刷新");
    });
    fresh("挂单", [&](State &x) { x.orders = std::make_shared<const std::vector<Pending>>(std::move(orders.rows)); });
    note("信用账户");
    std::shared_ptr<const Keyed<Position>> held;
    {
      std::lock_guard lk(mu);
      held = st->pos;
    }
    Account a = credit::account(credit_t, p, *held);
    fresh("信用账户", [&](State &x) { x.acct = std::make_shared<const Account>(std::move(a)); });
    ui::click_tool("持仓");
  }

  void tick() {
    uint64_t n;
    {
      std::unique_lock lk(mu);
      if (!cv.wait_for(lk, kTick, [&] { return stop || due(Clock::now()); }) || stop)
        return;
      n = asked;
    }
    std::string msg;
    try {
      round();
    } catch (const Fail &f) { // 错误边界: 报在进度里, 等下一个起因再刷
      msg = f.why;
    }
    std::lock_guard lk(mu);
    checked = st->pos;
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
      } catch (const Fail &f) { // 错误边界: 起因判不了 (持仓与成交自相矛盾) 也只报在进度里
        {
          std::lock_guard lk(mu);
          prog.step.clear();
          prog.err = f.why;
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
    ui::Gate::Hold g(c_->gate, "下单");
    return flash::place(o, step);
  }
  void exclusive(const char *who, const std::function<void()> &fn) override {
    ui::Gate::Hold g(c_->gate, who);
    fn();
  }
  Terms pull_terms(const Log &log, const Live &live) override {
    ui::Gate::Hold g(c_->gate, "两融条款");
    return credit::pull(c_->credit_t, mem::attach(), log, live);
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
