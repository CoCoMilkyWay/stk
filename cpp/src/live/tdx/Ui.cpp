#include "live/tdx/Ui.hpp"

#include "live/tdx/Shim.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Until.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>

namespace live::tdx::ui {

using win::Hwnd;
using win::Item;
using win::Node;

namespace {

const std::string kToolbarClass = "MHPToolBar", kToolbarTitle = "MainViewBar";
constexpr const char *kMainWnd = "class=TdxW_MainFrame_Class";
// 交易栏左侧功能树 (查询 / 资产分析 / …), 右侧为所选功能的页; 树项只认真实光标处的点击, clickat 会钉光标
const std::string kFuncTreeClass = "SysTreeView32";
constexpr long kFuncTreeId = 59653;

int half(int a, int b) { return (a + b) >= 0 ? (a + b) / 2 : -((-(a + b) + 1) / 2); } // 向下取整

std::vector<Node> func_trees() {
  std::vector<Node> out;
  for (Node &n : win::tree())
    if (n.vis && n.cls == kFuncTreeClass && n.id == kFuncTreeId)
      out.push_back(std::move(n));
  return out;
}

// 可见的查询页 (id 为 count 的条数标签所在的对话框)
std::vector<Hwnd> pages(long count, double timeout) {
  const std::vector<Node> nodes = win::tree("", timeout);
  std::vector<Hwnd> out;
  for (size_t i = 0; i < nodes.size(); ++i) {
    const Node &n = nodes[i];
    if (!(n.vis && n.cls == "Static" && n.id == count))
      continue;
    size_t j = i;
    while (nodes[j].depth >= n.depth) {
      assert(j > 0);
      --j;
    }
    out.push_back(nodes[j].hwnd);
  }
  return out;
}

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

Gate::Hold::Hold(Gate &g, const char *who) : g_(g) {
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

Gate::Hold::~Hold() {
  {
    std::lock_guard lk(g_.who_mu_);
    g_.who_ = outer_;
  }
  g_.mu_.unlock();
}

std::string Gate::who() const {
  std::lock_guard lk(who_mu_);
  return who_;
}

Hwnd main_wnd() {
  const std::vector<Node> main = win::find_wnd(kMainWnd);
  if (main.size() != 1)
    fail("找不到通达信主窗");
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
        win::clickat(t.hwnd, t.rect[0] + half(it.l, it.r), t.rect[1] + half(it.t, it.b));
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

// ---------- 查询页 ----------

Hwnd open_page(const std::string &func, const std::string &col, long count, double timeout) {
  trace::Span span("开页「" + func + "」", {{"col", col}});
  click_func(func);
  return *until(
      said("「" + func + "」页 (表头含「" + col + "」)"),
      [&]() -> std::optional<Hwnd> {
        for (const Hwnd &d : pages(count, timeout)) {
          const std::vector<std::string> c = cols(d, timeout);
          if (std::find(c.begin(), c.end(), col) != c.end())
            return d;
        }
        return std::nullopt;
      },
      timeout);
}

Hwnd count_label(const Hwnd &dlg, long count) { return win::ctls(dlg, {{"n", count}}).at("n"); }

std::optional<int> page_count(const Hwnd &lbl, double timeout) {
  const std::string t = win::text(lbl, timeout);
  if (t.empty())
    return std::nullopt;
  std::string_view v = t;
  if (v.starts_with("第1--"))
    v.remove_prefix(std::string_view("第1--").size());
  else if (v.starts_with("共"))
    v.remove_prefix(std::string_view("共").size());
  else
    fail("条数标签读到「" + t + "」");
  if (!v.ends_with("条"))
    fail("条数标签读到「" + t + "」");
  v.remove_suffix(std::string_view("条").size());
  if (v.empty() || v.size() > 9 || !std::all_of(v.begin(), v.end(), [](unsigned char c) { return std::isdigit(c); }))
    fail("条数标签读到「" + t + "」");
  return std::stoi(std::string(v));
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
