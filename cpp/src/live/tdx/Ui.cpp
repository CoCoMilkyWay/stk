#include "live/tdx/Ui.hpp"

#include "live/tdx/Shim.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Until.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <map>
#include <mutex>
#include <string_view>

namespace live::tdx::ui {

using win::Hwnd;
using win::Item;
using win::Node;

namespace {

const std::string kToolbarClass = "MHPToolBar", kToolbarTitle = "MainViewBar";
constexpr const char *kMainClass = "TdxW_MainFrame_Class";
// 交易栏左侧功能树 (查询 / 资产分析 / …), 右侧为所选功能的页; 树项只认真实光标处的点击, clickat 会钉光标
const std::string kFuncTreeClass = "SysTreeView32";
constexpr long kFuncTreeId = 59653;
constexpr double kReclick = 3;                  // 开页: 别的页一直挡着这么久 [s] 即再点一次功能树
constexpr long kPageAll = 454;                  // 查询页的「查询全部」按钮
constexpr long kMsgText = 7013, kMsgYes = 7015; // 确认框 / 提示框: 正文 Static、确认按钮

// 交易栏页签的页: 认页的记号 = 页上可见的列表 (表头含 col 的 SysHeader32 (列表的子窗口), 或 id 为 list 的 SysListView32;
//   持仓 / 成交的列表也是这个 id, 子表头含别的页签的 col 即不算)
struct Mark {
  const char *tab, *col;
  long list;
};
constexpr long kCancelList = 1567; // 撤单页的委托列表
constexpr Mark kMarks[] = {{"持仓", "可卖数量", 0}, {"成交", "成交编号", 0}, {"撤单", nullptr, kCancelList}};
constexpr double kTabWait = 2; // 切页签后等页出来 [s]
constexpr int kClimb = 3;      // 从列表往上找条数标签至多几层

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

// 可见的查询页 (id 为 count 的条数标签所在的对话框)
std::vector<Page> pages(long count) {
  std::vector<Page> out;
  for (const Node &n : win::find("Static", count))
    if (n.vis)
      out.push_back({n.parent, {n.hwnd}});
  return out;
}

// ---------- 页签 ----------

const Mark &mark_of(const std::string &tab) {
  for (const Mark &m : kMarks)
    if (tab == m.tab)
      return m;
  assert(false && "页签没有认页的记号");
  return kMarks[0];
}

bool header_has(const Hwnd &h, const char *col) {
  for (const Item &it : win::layer_of(shim::cmd("detail", h).at(0)).items)
    if (it.text && *it.text == col)
      return true;
  return false;
}

// 列表 list 的子表头含某页签的 col (= 那页签的列表)
bool foreign(const Hwnd &list) {
  for (const Node &n : win::enum_(list))
    if (n.depth == 1 && n.cls == "SysHeader32")
      for (const Mark &m : kMarks)
        if (m.col && header_has(n.hwnd, m.col))
          return true;
  return false;
}

// h 是 m 的记号控件: 表头须含 col, 列表须不是别的页签的
bool fits(const Mark &m, const Hwnd &h) { return m.col ? header_has(h, m.col) : !foreign(h); }

// 记号控件 h 此刻可见且是 m 的 (先看可见: 已销毁的窗口不可见, 不去 detail)
bool shown(const Mark &m, const Hwnd &h) { return win::node(h).vis && fits(m, h); }

// 此刻可见的记号控件 (须恰一个)
std::optional<Hwnd> mark_now(const Mark &m) {
  std::vector<Hwnd> hits;
  for (const Node &n : m.col ? win::find("SysHeader32") : win::find("SysListView32", m.list))
    if (n.vis && fits(m, n.hwnd))
      hits.push_back(n.hwnd);
  return hits.size() == 1 ? std::optional<Hwnd>(hits[0]) : std::nullopt;
}

// 从记号控件往上逐层看子树里可见的条数标签: 头一层有的, 恰一个即它所在的那层为页; 都没有 (或那层不止一个) → 页 = 记号的父窗口, 无标签.
// 标签只认此刻写着条数的 (正重查的清空着, 认不出, 下回再找)
Tab tab_of(const Hwnd &mark) {
  const Hwnd first = win::node(mark).parent;
  Hwnd a = first;
  for (int i = 0; i < kClimb && a != win::kNoWnd; ++i) {
    std::vector<Hwnd> lbls;
    for (const Node &n : win::tree(a))
      if (n.vis && n.cls == "Static" && count_in(n.title))
        lbls.push_back(n.hwnd);
    if (lbls.size() == 1)
      return {a, Count{lbls[0]}};
    if (lbls.size() > 1)
      break;
    a = win::node(a).parent;
  }
  return {first, std::nullopt};
}

std::string label_of(const Tab &t) { return t.count ? t.count->h : ""; }

// 认出的页签: 记号控件 + 页; 无 = 认不出 (此后只点不等). 按主窗记: 客户端重开即重认
struct Known {
  Hwnd mark;
  Tab tab;
};
struct Tabs {
  std::mutex mu;
  Hwnd main;
  std::map<std::string, std::optional<Known>> at;
};
Tabs &tabs() {
  static Tabs t;
  return t;
}

// ---------- 查询页 ----------

// 查询页列表的表头各列名; detail 在界面线程里只等 3s, 先 gettext (一直等) 把卡顿等过去
std::vector<std::string> cols(const Hwnd &dlg, double timeout) {
  win::text(dlg, timeout);
  std::vector<std::string> out;
  for (const Node &n : win::tree(dlg, timeout))
    if (n.vis && n.cls == "SysHeader32")
      for (const Item &it : win::layer_of(shim::cmd_wait(timeout, "detail", n.hwnd).at(0)).items)
        if (it.text)
          out.push_back(*it.text);
  return out;
}

} // namespace

Hwnd main_wnd() {
  const std::vector<Node> main = win::find(kMainClass);
  if (main.size() != 1)
    fail(strf("通达信主窗命中 %zu 个", main.size()));
  return main[0].hwnd;
}

// ---------- 交易栏 ----------

void click_tool(const std::string &txt) { win::click_text(kToolbarClass, kToolbarTitle, txt); }

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

// ---------- 条数标签 ----------

void Count::clear() const { win::settext(h, ""); }

std::optional<int> Count::read(double timeout) const {
  const std::string t = win::text(h, timeout);
  if (t.empty())
    return std::nullopt;
  const std::optional<int> n = count_in(t);
  if (!n)
    fail("条数标签读到「" + t + "」");
  return n;
}

// ---------- 页签 ----------

std::optional<Tab> open_tab(const std::string &tab) {
  trace::Span span("页签「" + tab + "」");
  const Mark &m = mark_of(tab);
  Tabs &c = tabs();
  std::lock_guard lk(c.mu);
  if (const Hwnd w = main_wnd(); w != c.main) {
    c.main = w;
    c.at.clear();
  }
  const auto it = c.at.find(tab);
  if (it == c.at.end()) {
    click_tool(tab);
    const std::optional<Hwnd> h = poll([&] { return mark_now(m); }, kTabWait);
    if (!h) {
      c.at[tab] = std::nullopt;
      trace::ev("tab", {{"known", false}});
      return std::nullopt;
    }
    const Known k{*h, tab_of(*h)};
    c.at[tab] = k;
    trace::ev("tab", {{"known", true}, {"mark", k.mark}, {"page", k.tab.page}, {"count", label_of(k.tab)}});
    return k.tab;
  }
  if (!it->second) {
    click_tool(tab);
    return std::nullopt;
  }
  Known &k = *it->second;
  if (!shown(m, k.mark)) {
    click_tool(tab);
    if (!poll([&] { return shown(m, k.mark); }, kTabWait)) {
      trace::ev("tab", {{"shown", false}});
      c.at.erase(it); // 页可能重建了: 下回重认
      return std::nullopt;
    }
  }
  if (!k.tab.count || !win::node(k.tab.count->h).vis) {
    k.tab = tab_of(k.mark);
    trace::ev("tab", {{"page", k.tab.page}, {"count", label_of(k.tab)}});
  }
  return k.tab;
}

// ---------- 查询页 ----------

Page open_page(const std::string &func, const std::string &col, long count, double timeout) {
  trace::Span span("开页「" + func + "」", {{"col", col}});
  click_func(func);
  Clock::time_point at = Clock::now();
  return *until(
      said("「" + func + "」页 (表头含「" + col + "」)"),
      [&]() -> std::optional<Page> {
        for (const Page &p : pages(count)) {
          const std::vector<std::string> c = cols(p.dlg, timeout);
          if (std::find(c.begin(), c.end(), col) != c.end())
            return p;
        }
        if (Clock::now() - at >= Secs(kReclick)) { // 排在前头还没处理完的点击 (如上一轮刚点的「持仓」) 会把页切回去
          click_func(func);
          at = Clock::now();
        }
        return std::nullopt;
      },
      timeout);
}

void query_all(const Page &p) { win::command(p.dlg, kPageAll); }

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
