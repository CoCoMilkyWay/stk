#include "live/tdx/Ui.hpp"

#include "live/tdx/Shim.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Until.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string_view>

namespace live::tdx::ui {

using win::Hwnd;
using win::Item;
using win::Node;

namespace {

constexpr const char *kMainClass = "TdxW_MainFrame_Class";
// 交易栏左侧功能树 (查询 / 资产分析 / …), 右侧为所选功能的页; 树项只认真实光标处的点击, clickat 会钉光标
const std::string kFuncTreeClass = "SysTreeView32";
constexpr long kFuncTreeId = 59653;
constexpr long kHostId = 59649;                 // 交易栏下各页的宿主 (AfxWnd100), 页签命令也投给它
constexpr long kKeys[] = {1140, 144};           // 页的查询键 (「查 询」/「刷 新」/「回到第一页」), 查着时禁用
constexpr long kCounts[] = {1243, 166};         // 页的条数标签
constexpr long kMsgText = 7013, kMsgYes = 7015; // 确认框 / 提示框: 正文 Static、确认按钮
constexpr double kTick = 1;                     // 等查完期间报进度的间隔 [s]

// 交易栏页签: 认页的列 (只此页的表头有), 投给宿主切到它的命令
struct Tab {
  const char *name, *col;
  long cmd;
};
constexpr Tab kTabs[] = {{"持仓", "可卖数量", 0x271D}, {"成交", "成交编号", 0x271B}, {"撤单", "委托价格", 0x2716}};

const Tab &tab_of(const std::string &name) {
  for (const Tab &t : kTabs)
    if (name == t.name)
      return t;
  assert(false && "没有这个页签");
  return kTabs[0];
}

// 「第1--N条」/「共N条」→ N; 不是这形 → 无
std::optional<int> count_in(std::string_view v) {
  if (v.starts_with("第1--"))
    v.remove_prefix(std::string_view("第1--").size());
  else if (v.starts_with("共"))
    v.remove_prefix(std::string_view("共").size());
  else
    return std::nullopt;
  if (!v.ends_with("条"))
    return std::nullopt;
  v.remove_suffix(std::string_view("条").size());
  if (v.empty() || v.size() > 9 || !std::all_of(v.begin(), v.end(), [](unsigned char c) { return std::isdigit(c); }))
    return std::nullopt;
  return std::stoi(std::string(v));
}

std::vector<Node> func_trees() {
  std::vector<Node> out;
  for (Node &n : win::find(kFuncTreeClass, kFuncTreeId))
    if (n.vis)
      out.push_back(std::move(n));
  return out;
}

// ---------- 页 ----------
// 认页、取件只用 find / rect (不经界面线程, 客户端查大表卡着时照样答); 读表头的 detail 经界面线程

Hwnd host() {
  std::vector<Hwnd> hs;
  for (const Node &n : win::find("AfxWnd100", kHostId))
    if (n.vis)
      hs.push_back(n.hwnd);
  if (hs.size() != 1)
    fail(strf("交易栏页宿主 (AfxWnd100 %ld) 可见的有 %zu 个", kHostId, hs.size()));
  return hs[0];
}

std::set<Hwnd> pages(const Hwnd &host) {
  std::set<Hwnd> out;
  for (const Node &n : win::find("#32770"))
    if (n.parent == host)
      out.insert(n.hwnd);
  return out;
}

std::vector<Hwnd> shown(const Hwnd &host) {
  std::vector<Hwnd> out;
  for (const Node &n : win::find("#32770"))
    if (n.parent == host && n.vis)
      out.push_back(n.hwnd);
  return out;
}

// 页 dlg 的直接子窗口里类名 cls、id 在 ids 内的那一个 (至多一个)
std::optional<Hwnd> kid(const Hwnd &dlg, const char *cls, std::span<const long> ids) {
  std::vector<Hwnd> hs;
  for (const long id : ids)
    for (const Node &n : win::find(cls, id))
      if (n.parent == dlg)
        hs.push_back(n.hwnd);
  if (hs.size() > 1)
    fail(strf("页 %s 上的 %s (id %ld / %ld) 有 %zu 个", dlg.c_str(), cls, ids[0], ids[1], hs.size()));
  return hs.empty() ? std::nullopt : std::optional<Hwnd>(hs[0]);
}

Page page_of(const Hwnd &dlg) {
  const std::optional<Hwnd> k = kid(dlg, "Button", kKeys), c = kid(dlg, "Static", kCounts);
  if (!k || !c)
    fail(strf("页 %s 上没有查询键或条数标签", dlg.c_str()));
  return {dlg, *k, Count{*c}};
}

// 各页 → 其列表的表头
std::map<Hwnd, std::vector<Hwnd>> heads(const Hwnd &host) {
  const std::set<Hwnd> ps = pages(host);
  std::map<Hwnd, Hwnd> page_of_list;
  for (const Node &n : win::find("SysListView32"))
    if (ps.contains(n.parent))
      page_of_list[n.hwnd] = n.parent;
  std::map<Hwnd, std::vector<Hwnd>> out;
  for (const Node &n : win::find("SysHeader32"))
    if (const auto it = page_of_list.find(n.parent); it != page_of_list.end())
      out[it->second].push_back(n.hwnd);
  return out;
}

bool has_col(const std::vector<Hwnd> &hs, const std::string &col) {
  for (const Hwnd &h : hs)
    for (const Item &it : win::layer_of(shim::cmd("detail", h).at(0)).items)
      if (it.text && *it.text == col)
        return true;
  return false;
}

// 等客户端空闲: 恰一页显示着、其查询键可用 (没有查询键的页如下单表单不查, 即空闲).
// 顺带给界面线程装上禁用计数 (此后新建的页也记得)
void idle(const Hwnd &host, double timeout) {
  std::vector<Hwnd> s;
  until([&] { return strf("客户端空闲 (显示着 %zu 页%s)", s.size(), s.size() == 1 ? ", 查询键禁用着" : ""); },
        [&] {
          s = shown(host);
          if (s.size() != 1)
            return false;
          win::disables(host);
          const std::optional<Hwnd> k = kid(s[0], "Button", kKeys);
          return !k || win::node(*k).en;
        },
        timeout, 0.01);
}

// 等页 p 这一问查完: 查询键的禁用次数过了 n0 (查起过) 且此刻可用
void settle(const Page &p, long n0, double timeout, const std::function<void()> &tick, Clock::time_point t0) {
  const Clock::duration gap = std::chrono::duration_cast<Clock::duration>(Secs(kTick));
  Clock::time_point next = Clock::now() + gap;
  long n = n0;
  until([&] { return strf("页 %s 查完 (查询键禁用 %ld 次)", p.dlg.c_str(), n - n0); },
        [&] {
          if (tick && Clock::now() >= next) {
            tick();
            next += gap;
          }
          n = win::disables(p.key);
          return n > n0 && win::node(p.key).en;
        },
        timeout, 0.01);
  trace::ev("done", {{"page", p.dlg}, {"ms", trace::ms_since(t0)}});
}

} // namespace

Hwnd main_wnd() {
  const std::vector<Node> main = win::find(kMainClass);
  if (main.size() != 1)
    fail(strf("通达信主窗命中 %zu 个", main.size()));
  return main[0].hwnd;
}

// ---------- 交易栏 ----------

void show_bar() {
  if (!func_trees().empty())
    return;
  // 主窗顶栏的「交易」: 交易栏收起 (整个停靠条高 0、功能树与 MainViewBar 都不可见) 时点它展开
  win::click_text("AfxWnd100", "", "交易");
  until(said("点顶栏「交易」后交易栏展开"), [] { return !func_trees().empty(); }, 5);
}

void click_func(const std::string &name) {
  trace::Span span("功能树「" + name + "」");
  const std::vector<Node> trees = func_trees();
  if (trees.size() != 1)
    fail(strf("交易功能树命中 %zu 个", trees.size()));
  const Node &t = trees[0];
  win::send(t.hwnd, win::kWmVscroll, win::kSbTop, 0);
  std::optional<std::vector<std::string>> seen;
  while (true) {
    const std::vector<Item> items = win::layer_of(shim::cmd("detail", t.hwnd).at(0)).items;
    std::vector<std::string> names;
    for (const Item &it : items) {
      if (it.text && *it.text == name) {
        const auto [x, y] = win::center(it);
        win::clickat(t.hwnd, t.rect[0] + x, t.rect[1] + y);
        return;
      }
      names.push_back(it.text.value_or(""));
    }
    if (seen && names == *seen)
      fail("交易功能树里没有「" + name + "」");
    seen = std::move(names);
    win::send(t.hwnd, win::kWmVscroll, win::kSbPagedown, 0);
  }
}

// ---------- 页 ----------

int Count::read() const {
  const std::string t = win::text(h);
  const std::optional<int> n = count_in(t);
  if (!n)
    fail("条数标签读到「" + t + "」");
  return *n;
}

// 认目标页: act 前表头含 col 的页 (须至多一个) 记下其查询键的禁用次数; act 后显示出来的页是它即是,
// 是 act 前没有的页 (头一回开) 即等其表头 (界面线程卡着时 gettext 一直等) 再看, 禁用次数从 0 起
Page query(const std::string &col, const std::function<void()> &act, double timeout, const std::function<void()> &tick) {
  const Hwnd h = host();
  idle(h, timeout);
  const std::set<Hwnd> before = pages(h);
  std::map<Hwnd, long> cand;
  for (const auto &[dlg, hs] : heads(h))
    if (has_col(hs, col))
      cand[dlg] = win::disables(page_of(dlg).key);
  if (cand.size() > 1)
    fail(strf("表头含「%s」的页有 %zu 个", col.c_str(), cand.size()));
  const Clock::time_point t0 = Clock::now();
  act();
  long n0 = 0;
  const Hwnd dlg = *until(
      said("表头含「" + col + "」的页显示出来"),
      [&]() -> std::optional<Hwnd> {
        const std::vector<Hwnd> s = shown(h);
        if (s.size() != 1)
          return std::nullopt;
        if (const auto it = cand.find(s[0]); it != cand.end()) {
          n0 = it->second;
          return s[0];
        }
        if (before.contains(s[0]))
          return std::nullopt;
        win::text(s[0], timeout);
        return has_col(heads(h)[s[0]], col) ? std::optional<Hwnd>(s[0]) : std::nullopt;
      },
      timeout);
  const Page p = page_of(dlg);
  settle(p, n0, timeout, tick, t0);
  return p;
}

Page open_tab(const std::string &tab) {
  trace::Span span("页签「" + tab + "」");
  const Tab &t = tab_of(tab);
  return query(t.col, [&] { win::post(host(), win::kWmCommand, t.cmd); });
}

Page open_page(const std::string &func, const std::string &col, double timeout) {
  trace::Span span("开页「" + func + "」", {{"col", col}});
  return query(col, [&] { click_func(func); }, timeout);
}

void press(const Page &p, long key, double timeout, const std::function<void()> &tick) {
  trace::Span span(strf("按键 %ld", key));
  const Hwnd h = host();
  idle(h, timeout);
  if (shown(h) != std::vector<Hwnd>{p.dlg})
    fail(strf("页 %s 不在显示", p.dlg.c_str()));
  const long n0 = win::disables(p.key);
  const Clock::time_point t0 = Clock::now();
  win::command(p.dlg, key);
  settle(p, n0, timeout, tick, t0);
}

// ---------- 弹框 ----------

Hwnd popup(const std::vector<std::string> &titles, double timeout, const std::function<void()> &act) {
  trace::Span span("弹框", {{"titles", titles}});
  const win::Tops before = win::tops();
  act();
  std::string want;
  for (const std::string &t : titles)
    want += (want.empty() ? "" : "/") + t;
  const auto fresh = [&] {
    win::Tops out;
    for (auto &[h, n] : win::tops())
      if (!before.contains(h) && n.cls == "#32770")
        out.emplace(h, n);
    return out;
  };
  std::string title;
  const Hwnd box = *until(
      [&] {
        std::string s;
        for (const auto &[h, n] : fresh())
          s += (s.empty() ? "" : "; ") + ("「" + n.title + "」") + body(h);
        return "「" + want + "」, 期间新窗口: " + (s.empty() ? "无" : s);
      },
      [&]() -> std::optional<Hwnd> {
        std::vector<Hwnd> hits;
        for (const auto &[h, n] : fresh())
          if (std::find(titles.begin(), titles.end(), n.title) != titles.end()) {
            hits.push_back(h);
            title = n.title;
          }
        if (hits.size() > 1)
          fail(strf("「%s」同时出现 %zu 个", want.c_str(), hits.size()));
        return hits.empty() ? std::nullopt : std::optional<Hwnd>(hits[0]);
      },
      timeout);
  trace::ev("popup", {{"title", title}, {"hwnd", box}});
  return box;
}

std::string body(const Hwnd &box) {
  std::vector<Hwnd> hits;
  for (const Node &n : win::enum_(box))
    if (n.depth == 1 && n.id == kMsgText)
      hits.push_back(n.hwnd);
  return hits.size() == 1 ? win::squash(win::text(hits[0])) : "";
}

void yes(const Hwnd &box) { win::command(box, kMsgYes); }

} // namespace live::tdx::ui
