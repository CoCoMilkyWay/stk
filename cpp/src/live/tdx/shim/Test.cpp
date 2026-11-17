// shim test — 在线用例: 对着活客户端跑适配代码 (Ui / Mem / Tables / Credit / Flow 本身) 与客户端行为的假设 (TDX.md 实测的那些).
//   每例自成一段: 先展开交易栏, 末了回持仓页; 只做查询类动作. 一例一行 PASS / FAIL (原因), 用时与摘要; 有 FAIL 退出码 1.
//   失败的细节在交互日志 (output/run/live/tdx/log/<启动时刻>.jsonl) 的「用例 <名>」段里.
//   用例:
//     query   各页查一次 (页签 成交 / 持仓 / 撤单、功能树 信用资产 / 融资负债汇总 / 资金流水): 认页、等到查完、条数可读
//     busy    查着时切页签被吞: 切持仓、等其查询键禁用 (查起) 即投切撤单的命令, 撤单页 0.3s 内不显示
//     answer  查完即读: 成交 / 持仓 answered 行数对页上条数, 挂单 / 信用资产 / 融资负债接成 Chain 各问一次并解析信用账户
//     flow    资金流水近 14 天 (开页、敲起止日、查询、取块, 同 Broker::pull_flows)
//     direct  请求层直发对界面 + 内存: 持仓 / 挂单 / 信用资产 / 融资负债先经界面查一次 (插件发过, 入参才有得照搬),
//             再 req::ask 同一功能号, 行数与逐行键值须一致
//     fields  Tables.hpp 各表各布局: 列数 > 功能定义的出参数, 声明的各列号上的出参字段名恰为声明的 (协议字典, 不动界面)
//     terms   两融条款 (标的证券 / 担保品 查询全部, 约 1 分钟) — 慢, 须点名才跑
#include "live/tdx/Schema.hpp"
#include "live/tdx/shim/Tool.hpp"

#include "live/Broker.hpp"
#include "live/tdx/Credit.hpp"
#include "live/tdx/Flow.hpp"
#include "live/tdx/Mem.hpp"
#include "live/tdx/Req.hpp"
#include "live/tdx/Tables.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"
#include "live/tdx/Win.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <map>
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

// 直发功能 func 的应答按表 T 解析
template <class T>
std::vector<typename T::Row> asked(int func) {
  const req::Answer a = req::ask(func);
  return mem::parse_answer<T>(func, a.cols, a.rows);
}

// 两份行逐行同键同值 (键 / 值由 key 取; 顺序不论)
template <class Row, class K>
void same(const char *what, const std::vector<Row> &ui, const std::vector<Row> &direct, K key) {
  std::map<std::string, std::string> a, b;
  for (const Row &r : ui)
    a.insert(key(r));
  for (const Row &r : direct)
    b.insert(key(r));
  need(ui.size() == direct.size(), strf("%s: 界面 %zu 行, 直发 %zu 行", what, ui.size(), direct.size()));
  for (const auto &[k, v] : a)
    need(b.contains(k) && b.at(k) == v, strf("%s「%s」: 界面 %s, 直发 %s", what, k.c_str(), v.c_str(), b.contains(k) ? b.at(k).c_str() : "(没有)"));
}

std::string direct() {
  const mem::HeapP h = mem::attach();
  mem::Table<tables::Positions> pos;
  pos.read(h);
  const int np = ui::open_tab("持仓").count.read();
  const std::vector<Position> p = pos.answered(h, np).rows;
  same("持仓", p, asked<tables::Positions>(1228),
       [](const Position &r) { return std::pair{r.code, strf("%s %.0f/%.0f 成本 %.4f", r.name.c_str(), r.balance, r.available, r.cost)}; });
  mem::Chain q(h, {&tables::Pendings::kShape, &tables::Assets::kShape, &tables::Debts::kShape});
  same("挂单", q.ask<tables::Pendings>([] { return ui::open_tab("撤单").count.read(); }).rows, asked<tables::Pendings>(1100),
       [](const Pending &r) { return std::pair{r.order_id, strf("%s %.0f@%.3f %s", r.code.c_str(), r.qty, r.price, r.status.c_str())}; });
  same("信用资产", q.ask<tables::Assets>([] { return ui::open_page("信用资产", "信用资金额度").count.read(); }).rows, asked<tables::Assets>(3200),
       [](const tables::Asset &r) { return std::pair{r.kv[0].first, r.kv[0].second + " " + r.kv[1].first + " " + r.kv[1].second + " " + r.kv[2].first}; });
  same("融资负债", q.ask<tables::Debts>([] { return ui::open_page("融资负债汇总", "当前负债金额").count.read(); }).rows, asked<tables::Debts>(3264),
       [](const tables::Debt &r) { return std::pair{r.code, strf("%.0f %.0f %.2f", r.balance, r.qty, r.amount)}; });
  return strf("持仓 %zu · 挂单 · 信用资产 · 融资负债 逐行一致", p.size());
}

// 表 T 各布局对功能定义: 不符的各条记进 bad, 核过的列数记进 n
template <class T>
void fields_of(const Schema &s, int &n, std::string &bad) {
  for (size_t v = 0; v < T::kViews.size(); ++v) {
    const auto f = s.funcs.find(T::kFuncs[v]);
    if (f == s.funcs.end()) {
      bad += strf("%s: 没有功能 %d; ", T::kShape.name, T::kFuncs[v]);
      continue;
    }
    const std::vector<int> &out = f->second.out;
    if (static_cast<int>(out.size()) >= T::kViews[v])
      bad += strf("%s: 功能 %d 出参 %zu 个, 布局只 %d 列; ", T::kShape.name, T::kFuncs[v], out.size(), T::kViews[v]);
    std::apply(
        [&](const auto &...c) {
          (
              [&] {
                const int at = c.at[v];
                if (at == mem::kNone)
                  return;
                const std::string got = at < static_cast<int>(out.size()) ? s.name(out[static_cast<size_t>(at)]) : "(越界)";
                if (got != c.field)
                  bad += strf("%s 功能 %d 列 %d: 字典为「%s」, 声明「%s」; ", T::kShape.name, T::kFuncs[v], at, got.c_str(), c.field);
                ++n;
              }(),
              ...);
        },
        T::kCols);
  }
}

std::string fields() {
  const Schema s = schema(*mem::attach());
  int n = 0;
  std::string bad;
  fields_of<tables::Positions>(s, n, bad);
  fields_of<tables::Fills>(s, n, bad);
  fields_of<tables::Pendings>(s, n, bad);
  fields_of<tables::Debts>(s, n, bad);
  fields_of<tables::Assets>(s, n, bad);
  fields_of<tables::Margins>(s, n, bad);
  fields_of<tables::Collats>(s, n, bad);
  fields_of<tables::CashFlows>(s, n, bad);
  need(bad.empty(), bad);
  return strf("%s: 字段 %zu · 功能 %zu · 8 表 %d 列对上", std::filesystem::path(s.file).filename().c_str(), s.fields.size(), s.funcs.size(), n);
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
constexpr Case kCases[] = {{"query", false, query}, {"busy", false, busy}, {"answer", false, answer}, {"flow", false, flows}, {"direct", false, direct}, {"fields", false, fields}, {"terms", true, terms}};

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
