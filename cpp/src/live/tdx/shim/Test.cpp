// shim test — 在线用例: 对着活客户端跑适配代码与客户端行为假设, 零 sudo / 零 ptrace
#include "live/tdx/Schema.hpp"
#include "live/tdx/shim/Tool.hpp"

#include "live/Broker.hpp"
#include "live/Trace.hpp"
#include "live/tdx/Cols.hpp"
#include "live/tdx/Heap.hpp"
#include "live/tdx/Tables.hpp"
#include "live/tdx/Ui.hpp"
#include "live/tdx/Until.hpp"
#include "live/tdx/Win.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <map>

namespace live::tdx::tool {

namespace {

constexpr long kHostId = 59649, kHold = 0x271D, kCancel = 0x2716;

std::string query() {
  std::string s;
  std::apply([&](auto... t) { ((s += strf("%s%s %d 条", s.empty() ? "" : " · ", decltype(t)::kOpen.page, ui::open(decltype(t)::kOpen).count.read())), ...); },
             std::tuple_cat(tables::kReads, tables::kFlows));
  return s;
}

std::string busy() {
  const ui::Page hold = ui::open(tables::Positions::kOpen), cancel = ui::open(tables::Pendings::kOpen);
  std::vector<win::Hwnd> hosts;
  for (const win::Node &n : win::find("AfxWnd100", kHostId))
    if (n.vis)
      hosts.push_back(n.hwnd);
  need(hosts.size() == 1, "交易栏页宿主可见的有 %zu 个", hosts.size());
  const long n0 = win::disables(hold.key);
  win::post(hosts[0], win::kWmCommand, kHold);
  until(said("持仓页查起 (查询键禁用)"), [&] { return win::disables(hold.key) > n0; }, 3, 0.005);
  win::post(hosts[0], win::kWmCommand, kCancel);
  need(!poll([&] { return win::node(cancel.dlg).vis; }, 0.3, 0.01), "持仓查着时投的切撤单命令被执行了 (客户端不再吞切页?)");
  return "持仓查着时切撤单无效";
}

template <class S>
auto read_one() {
  trace::Span span(S::kName);
  const int n = ui::open(S::kOpen).count.read();
  std::vector<typename S::Row> rows = tables::rows<S>();
  need(rows.size() == static_cast<size_t>(n), "%s页上 %d 条, 直发 %zu 行", S::kName, n, rows.size());
  return S::Index::build(S::kName, std::move(rows));
}

std::string read() {
  const Fills f = read_one<tables::Fills>();
  const Keyed<Position> pos = read_one<tables::Positions>();
  const Keyed<Pending> o = read_one<tables::Pendings>();
  const Keyed<Debt> d = read_one<tables::Debts>();
  const std::map<std::string, std::string> c = read_one<tables::Assets>();
  // 一趟并发直发的五表与逐表直发的行数相同 (应答按序号认问, 不串)
  const Snapshot r = tables::reads();
  need(r.fills.rows.size() == f.rows.size() && r.pos.list.size() == pos.list.size() && r.pends.list.size() == o.list.size() && r.debts.list.size() == d.list.size(),
       "并发直发 成交 %zu · 持仓 %zu · 挂单 %zu · 融资负债 %zu, 与逐表的不符", r.fills.rows.size(), r.pos.list.size(), r.pends.list.size(), r.debts.list.size());
  return strf("成交 %zu · 持仓 %zu · 挂单 %zu · 融资负债 %zu · 信用资产 %zu 项 · 净资产 %.2f", f.rows.size(), pos.list.size(), o.list.size(), d.list.size(), c.size(), r.acct.net);
}

std::string flows() {
  std::apply([](auto... t) { ((std::ignore = ui::open(decltype(t)::kOpen)), ...); }, tables::kFlows);
  const misc::Ymd from = misc::add_days(misc::today_ymd(), -14);
  const std::vector<CashFlow> f = tables::flows(from);
  return strf("%d 起 %zu 条", from, f.size());
}

std::string terms() {
  std::apply([](auto... t) { ((std::ignore = ui::open(decltype(t)::kOpen)), ...); }, tables::kTerms);
  const Terms t = tables::terms();
  return strf("%zu 只", t.size());
}

template <class T>
void fields_of(const Schema &s, int &n, std::string &bad) {
  for (size_t v = 0; v < T::kFuncs.size(); ++v) {
    const int fn = T::kFuncs[v];
    const auto f = s.funcs.find(fn);
    if (f == s.funcs.end()) {
      bad += strf("%s: 没有功能 %d; ", T::kName, fn);
      continue;
    }
    const std::vector<int> &out = f->second.out;
    std::apply(
        [&](const auto &...c) {
          (
              [&] {
                const int at = c.at[v];
                if (at == cols::kNone)
                  return;
                const std::string got = at < static_cast<int>(out.size()) ? s.name(out[static_cast<size_t>(at)]) : "(越界)";
                if (got != c.field)
                  bad += strf("%s 功能 %d 列 %d: 字典为「%s」, 声明「%s」; ", T::kName, fn, at, got.c_str(), c.field);
                ++n;
              }(),
              ...);
        },
        T::kCols);
  }
}

std::string fields() {
  const int pid = mem::find_pid();
  need(pid != 0, "找不到 tdxw.exe 进程");
  const Schema s = schema(pid);
  int n = 0;
  std::string bad;
  std::apply([&](auto... t) { (fields_of<decltype(t)>(s, n, bad), ...); }, tables::kAll);
  need(bad.empty(), bad);
  return strf("%s: 字段 %zu · 功能 %zu · %zu 表 %d 列对上", std::filesystem::path(s.file).filename().c_str(), s.fields.size(), s.funcs.size(),
              std::tuple_size_v<decltype(tables::kAll)>, n);
}

struct Case {
  const char *name;
  std::string (*fn)();
};
constexpr Case kCases[] = {{"query", query}, {"busy", busy}, {"read", read}, {"flow", flows}, {"terms", terms}, {"fields", fields}};

bool run(const Case &c) {
  const Clock::time_point t0 = Clock::now();
  try {
    trace::Span span(std::string("用例 ") + c.name);
    ui::show_bar();
    const std::string s = c.fn();
    std::ignore = ui::open(tables::Positions::kOpen);
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
      std::printf("%s\n", c.name);
    return 0;
  }
  for (const std::string &n : names)
    need(std::any_of(std::begin(kCases), std::end(kCases), [&](const Case &c) { return n == c.name; }), "没有用例「" + n + "」(shim test list)");
  bool ok = true;
  for (const Case &c : kCases)
    if (names.empty() || std::find(names.begin(), names.end(), c.name) != names.end())
      ok = run(c) && ok;
  return ok ? 0 : 1;
}

} // namespace live::tdx::tool
