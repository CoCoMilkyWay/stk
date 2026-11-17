// shim test — 在线用例: 对着活客户端跑适配代码 (Ui / Mem / Tables / Credit / Flow 本身) 与客户端行为的假设 (TDX.md 实测的那些).
//   每例自成一段: 先展开交易栏, 末了回持仓页; 只做查询类动作. 一例一行 PASS / FAIL (原因), 用时与摘要; 有 FAIL 退出码 1.
//   失败的细节在交互日志 (output/run/live/tdx/log/<启动时刻>.jsonl) 的「用例 <名>」段里.
//   用例:
//     query   各页查一次 (页签 成交 / 持仓 / 撤单、功能树 信用资产 / 融资负债汇总 / 资金流水): 认页、等到查完、条数可读
//     busy    查着时切页签被吞: 切持仓、等其查询键禁用 (查起) 即投切撤单的命令, 撤单页 0.3s 内不显示
//     answer  查完即读: 成交 / 持仓 answered 行数对页上条数, 挂单 / 信用资产 / 融资负债接成 Chain 各问一次并解析信用账户
//     flow    资金流水近 14 天 (开页、敲起止日、查询、取块, 同 Broker::pull_flows)
//     terms   两融条款 (标的证券 / 担保品 查询全部, 约 1 分钟) — 慢, 须点名才跑
#include "live/tdx/shim/Tool.hpp"

#include "live/Broker.hpp"
#include "live/tdx/Credit.hpp"
#include "live/tdx/Flow.hpp"
#include "live/tdx/Mem.hpp"
#include "live/tdx/Tables.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"
#include "live/tdx/Win.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <cstdio>
#include <memory>

namespace live::tdx::tool {

namespace {

// 交易栏宿主与页签命令 (TDX.md「交易栏」): busy 要绕开 ui::open_tab 的等待, 直接投
constexpr long kHostId = 59649, kHold = 0x271D, kCancel = 0x2716;

std::string query() {
  std::string s;
  const auto one = [&](const char *name, const ui::Page &p) { s += strf("%s%s %d 条", s.empty() ? "" : " · ", name, p.count.read()); };
  for (const char *t : {"成交", "持仓", "撤单"})
    one(t, ui::open_tab(t));
  one("信用资产", ui::open_page("信用资产", "信用资金额度"));
  one("融资负债汇总", ui::open_page("融资负债汇总", "当前负债金额"));
  one("资金流水", ui::open_page("资金流水", "剩余金额"));
  return s;
}

std::string busy() {
  const ui::Page hold = ui::open_tab("持仓"), cancel = ui::open_tab("撤单");
  std::vector<win::Hwnd> hosts;
  for (const win::Node &n : win::find("AfxWnd100", kHostId))
    if (n.vis)
      hosts.push_back(n.hwnd);
  need(hosts.size() == 1, strf("交易栏页宿主可见的有 %zu 个", hosts.size()));
  const long n0 = win::disables(hold.key);
  win::post(hosts[0], win::kWmCommand, kHold);
  until(said("持仓页查起 (查询键禁用)"), [&] { return win::disables(hold.key) > n0; }, 3, 0.005);
  win::post(hosts[0], win::kWmCommand, kCancel);
  need(!poll([&] { return win::node(cancel.dlg).vis; }, 0.3, 0.01), "持仓查着时投的切撤单命令被执行了 (客户端不再吞切页?)");
  return "持仓查着时切撤单无效";
}

std::string answer() {
  const mem::HeapP h = mem::attach();
  mem::Table<tables::Positions> pos;
  mem::Table<tables::Fills> fills;
  pos.read(h);
  const int nf = ui::open_tab("成交").count.read();
  const size_t rf = fills.answered(h, nf).rows.size();
  const int np = ui::open_tab("持仓").count.read();
  const mem::Snap<tables::Positions> p = pos.answered(h, np);
  mem::Chain q(h, {&tables::Pendings::kShape, &tables::Assets::kShape, &tables::Debts::kShape});
  const size_t ro = q.ask<tables::Pendings>([] { return ui::open_tab("撤单").count.read(); }).rows.size();
  const Account a = credit::account(q, std::make_shared<const Keyed<Position>>(p.index));
  return strf("成交 %zu/%d · 持仓 %zu/%d · 挂单 %zu · 净资产 %.2f · 负债 %zu 只", rf, nf, p.rows.size(), np, ro, a.net, a.debts.size());
}

std::string flows() {
  const misc::Ymd from = misc::add_days(misc::today_ymd(), -14);
  const std::vector<CashFlow> f = flow::pull(mem::attach(), from, [](const char *, nlohmann::json) {}, [](std::string) {});
  return strf("%d 起 %zu 条", from, f.size());
}

std::string terms() {
  const Terms t = credit::pull(mem::attach(), [](const char *, nlohmann::json) {}, [](std::string) {});
  return strf("%zu 只", t.size());
}

struct Case {
  const char *name;
  bool slow;
  std::string (*fn)();
};
constexpr Case kCases[] = {{"query", false, query}, {"busy", false, busy}, {"answer", false, answer}, {"flow", false, flows}, {"terms", true, terms}};

// 一例: 段「用例 <名>」内展开交易栏、跑、回持仓页
bool run(const Case &c) {
  const Clock::time_point t0 = Clock::now();
  try {
    trace::Span span(std::string("用例 ") + c.name);
    ui::show_bar();
    const std::string s = c.fn();
    ui::open_tab("持仓");
    std::printf("PASS %-7s %6ldms  %s\n", c.name, trace::ms_since(t0), s.c_str());
    return true;
  } catch (const Fail &f) {
    std::printf("FAIL %-7s %6ldms  %s\n", c.name, trace::ms_since(t0), f.why.c_str());
    return false;
  }
}

} // namespace

int test(const std::vector<std::string> &names) {
  if (names == std::vector<std::string>{"list"}) {
    for (const Case &c : kCases)
      std::printf("%s%s\n", c.name, c.slow ? " (慢, 须点名)" : "");
    return 0;
  }
  for (const std::string &n : names)
    need(std::any_of(std::begin(kCases), std::end(kCases), [&](const Case &c) { return n == c.name; }), "没有用例「" + n + "」(shim test list)");
  bool ok = true;
  for (const Case &c : kCases)
    if (names.empty() ? !c.slow : std::find(names.begin(), names.end(), c.name) != names.end())
      ok = run(c) && ok;
  return ok ? 0 : 1;
}

} // namespace live::tdx::tool
