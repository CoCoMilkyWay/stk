#include "live/tdx/Ui.hpp"

#include "live/Trace.hpp"
#include "live/tdx/Req.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Until.hpp"

#include <algorithm>
#include <cctype>
#include <iterator>
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
// 系统 MessageBox 的正文 Static (MSGBOX_IDTEXT); 客户端框里的 IDC_STATIC 也是此号, 故先认 kMsgText
constexpr long kBoxText = 0xFFFF;

// 交易栏页签: 投给宿主切到它的命令
struct Tab {
  std::string_view name;
  long cmd;
};
constexpr Tab kTabs[] = {{"持仓", 0x271D}, {"成交", 0x271B}, {"撤单", 0x2716}};

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
// 认页、取件只用 find / rect (不经界面线程, 客户端查大表卡着时照样答); 读表头的 detail 经界面线程.
// 页开过即一直留着 (隐藏), 其表头列名与查询键 / 条数标签不变: 每页头一回见时读一遍记下 (同一宿主下; 宿主换了即全忘),
//   此后认页、等空闲都不再读 (查完大表界面线程还要卡一阵画表, 那时的 detail 要等它; find 一次就要几十 ms). 调用方都持界面锁

// 记下的一页
struct Known {
  bool read = false;              // 表头读过
  std::vector<std::string> cols;  // 表头列名 (没有列表 = 空)
  bool probed = false;            // 查询键 / 条数标签找过
  std::optional<Hwnd> key, count; // 查询键 / 条数标签 (没有 = 空)
};
struct Seen {
  Hwnd host;                  // 交易栏页宿主
  std::map<Hwnd, Known> page; // 页 → 记下的
};
Seen &seen() {
  static Seen s;
  return s;
}

// 宿主: 记下的那个还在且可见即用它 (rect 一问), 否则重找; 换了即忘掉记下的各页
Hwnd host() {
  Seen &k = seen();
  if (!k.host.empty()) {
    const Node n = win::node(k.host);
    if (n.vis && n.cls == "AfxWnd100" && n.id == kHostId)
      return k.host;
  }
  std::vector<Hwnd> hs;
  for (const Node &n : win::find("AfxWnd100", kHostId))
    if (n.vis)
      hs.push_back(n.hwnd);
  if (hs.size() != 1)
    fail(strf("交易栏页宿主 (AfxWnd100 %ld) 可见的有 %zu 个", kHostId, hs.size()));
  if (hs[0] != k.host)
    k = {hs[0], {}};
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

// 页 dlg 记下的查询键 / 条数标签 (头一回即找)
const Known &probe(const Hwnd &dlg) {
  Known &k = seen().page[dlg];
  if (!k.probed) {
    k.key = kid(dlg, "Button", kKeys);
    k.count = kid(dlg, "Static", kCounts);
    k.probed = true;
  }
  return k;
}

Page page_of(const Hwnd &dlg) {
  const Known &k = probe(dlg);
  if (!k.key || !k.count)
    fail(strf("页 %s 上没有查询键或条数标签", dlg.c_str()));
  return {dlg, *k.key, Count{*k.count}};
}

// 宿主下各页 → 记下的; 还没读过表头的读一遍 (列表与表头一次找齐, 各表头经界面线程至多等 timeout), 不在了的忘掉
std::map<Hwnd, Known> &learn(const Hwnd &host, double timeout) {
  std::map<Hwnd, Known> &by = seen().page;
  const std::set<Hwnd> ps = pages(host);
  std::erase_if(by, [&](const auto &e) { return !ps.contains(e.first); });
  std::set<Hwnd> fresh;
  for (const Hwnd &p : ps)
    if (!by[p].read)
      fresh.insert(p);
  if (fresh.empty())
    return by;
  std::map<Hwnd, Hwnd> page_of_list;
  for (const Node &n : win::find("SysListView32"))
    if (fresh.contains(n.parent))
      page_of_list[n.hwnd] = n.parent;
  for (const Node &n : win::find("SysHeader32"))
    if (const auto it = page_of_list.find(n.parent); it != page_of_list.end())
      for (const Item &x : win::layer_of(shim::cmd_wait(timeout, "detail", n.hwnd).at(0)).items)
        if (x.text)
          by[it->second].cols.push_back(*x.text);
  for (const Hwnd &p : fresh)
    by[p].read = true;
  return by;
}

// 表头列名 cols 认得出 o 那页: 含 o.col 且不含 o.without
bool fits(const std::vector<std::string> &cols, const Open &o) {
  const auto has = [&](const char *c) { return std::find(cols.begin(), cols.end(), c) != cols.end(); };
  return has(o.col) && !(o.without && has(o.without));
}
std::string what(const Open &o) { return strf("表头含「%s」%s", o.col, o.without ? strf("不含「%s」", o.without).c_str() : ""); }

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
          const std::optional<Hwnd> &k = probe(s[0]).key;
          return !k || win::node(*k).en;
        },
        timeout, 0.01);
}

// 等页 p 这一问查完: 查询键的禁用次数过了 n0 (查起过) 且此刻可用
void settle(const Page &p, long n0, double timeout, Clock::time_point t0) {
  long n = n0;
  until([&] { return strf("页 %s 查完 (查询键禁用 %ld 次)", p.dlg.c_str(), n - n0); },
        [&] {
          n = win::disables(p.key);
          return n > n0 && win::node(p.key).en;
        },
        timeout, 0.01);
  trace::ev("done", {{"page", p.dlg}, {"ms", trace::ms_since(t0)}});
  req::flush();
}

// 无标题、无正文的对话框 = 客户端自弹的提示小窗 (如成交回报: 不模态, 关不掉, 过一会儿自己收起), 不挡界面, 一律不理;
// 每个窗口头一回见记下其子窗口 (认它用). 调用方都持界面锁
bool toast(const Hwnd &h, const Node &n) {
  if (!n.title.empty() || !body(h).empty())
    return false;
  static std::set<Hwnd> told;
  if (told.insert(h).second) {
    std::vector<std::string> kids;
    for (const Node &k : win::enum_(h))
      if (k.depth == 1)
        kids.push_back(strf("%s#%ld「%s」", k.cls.c_str(), k.id, win::squash(k.title).c_str()));
    trace::ev("toast", {{"hwnd", h}, {"rect", std::vector<int>(std::begin(n.rect), std::end(n.rect))}, {"kids", kids}});
  }
  return true;
}

// before 之后新出的可见对话框 (提示小窗除外)
win::Tops fresh(const win::Tops &before) {
  win::Tops out;
  for (auto &[h, n] : win::tops())
    if (!before.contains(h) && n.cls == "#32770" && !toast(h, n))
      out.emplace(h, n);
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

void show_bar() {
  if (!func_trees().empty())
    return;
  // 主窗顶栏的「交易」: 交易栏收起 (整个停靠条高 0、功能树与 MainViewBar 都不可见) 时点它展开.
  // 停靠条随即排好位, 功能树等界面线程空闲时才显示; 其间弹出的模态框不关, 界面线程一直在其里, 树就一直不显示
  const win::Tops before = win::tops();
  win::click_text("AfxWnd100", "", "交易");
  until(said("点顶栏「交易」后交易栏展开"), [&] {
    for (const auto &[h, n] : fresh(before))
      fail(strf("点顶栏「交易」后弹出「%s」%s, 交易栏等它关了才显示", n.title.c_str(), body(h).c_str()));
    return !func_trees().empty(); }, 5);
}

namespace {

// 功能树上找 name 并点它; 翻到底都没有 → false
bool click_item(const Node &t, const std::string &name) {
  win::send(t.hwnd, win::kWmVscroll, win::kSbTop, 0);
  std::optional<std::vector<std::string>> seen;
  while (true) {
    const std::vector<Item> items = win::layer_of(shim::cmd("detail", t.hwnd).at(0)).items;
    std::vector<std::string> names;
    for (const Item &it : items) {
      if (it.text && *it.text == name) {
        const auto [x, y] = win::center(it);
        win::clickat(t.hwnd, t.rect[0] + x, t.rect[1] + y);
        return true;
      }
      names.push_back(it.text.value_or(""));
    }
    if (seen && names == *seen)
      return false;
    seen = std::move(names);
    win::send(t.hwnd, win::kWmVscroll, win::kSbPagedown, 0);
  }
}

} // namespace

void click_func(const std::string &name, const std::string &parent) {
  trace::Span span("功能树「" + name + "」");
  const std::vector<Node> trees = func_trees();
  if (trees.size() != 1)
    fail(strf("交易功能树命中 %zu 个", trees.size()));
  const Node &t = trees[0];
  if (click_item(t, name))
    return;
  if (parent.empty() || !click_item(t, parent))
    fail("交易功能树里没有「" + name + "」");
  if (!poll([&] { return click_item(t, name); }, 2, 0.2))
    fail(strf("交易功能树点开「%s」后仍没有「%s」", parent.c_str(), name.c_str()));
}

// ---------- 页 ----------

int Count::read() const {
  const std::string t = win::text(h);
  const std::optional<int> n = count_in(t);
  if (!n)
    fail("条数标签读到「" + t + "」");
  return *n;
}

// 认目标页: act 前认得出的页 (列对得上且有查询键与条数标签, 下单页里嵌的同列持仓表不算; 须至多一个: 多了即分不清 act 后显示的是不是它)
//   记下其查询键的禁用次数; act 后显示出来的页是它即是,
// 是 act 前没有的页 (头一回开) 即禁用次数从 0 起等它查完再读表头核对 (查着时界面线程卡住, detail 等不到;
// 大表 (标的证券 / 担保品) 查询键恢复后界面线程还要卡一阵画表, 读表头的 detail 随之等, 至多 timeout)
Page query(const Open &o, const std::function<void()> &act, double timeout) {
  const Hwnd h = host();
  idle(h, timeout);
  std::set<Hwnd> before;
  std::map<Hwnd, long> cand;
  for (const auto &[dlg, k] : learn(h, timeout)) {
    before.insert(dlg);
    if (!fits(k.cols, o))
      continue;
    if (const Known &q = probe(dlg); q.key && q.count)
      cand[dlg] = win::disables(*q.key);
  }
  if (cand.size() > 1)
    fail(strf("%s的页有 %zu 个", what(o).c_str(), cand.size()));
  const Clock::time_point t0 = Clock::now();
  act();
  long n0 = 0;
  const Hwnd dlg = *until(
      said(what(o) + "的页显示出来"),
      [&]() -> std::optional<Hwnd> {
        const std::vector<Hwnd> s = shown(h);
        if (s.size() != 1)
          return std::nullopt;
        if (const auto it = cand.find(s[0]); it != cand.end()) {
          n0 = it->second;
          return s[0];
        }
        return before.contains(s[0]) ? std::nullopt : std::optional<Hwnd>(s[0]);
      },
      timeout);
  const Page p = page_of(dlg);
  settle(p, n0, timeout, t0);
  if (!cand.contains(dlg) && !fits(learn(h, timeout)[dlg].cols, o))
    fail(strf("新开的页 %s 不是%s的页", dlg.c_str(), what(o).c_str()));
  return p;
}

Page open(const Open &o, double timeout) {
  trace::Span span(strf("开页「%s」", o.page), {{"col", o.col}});
  const Tab *t = std::find_if(std::begin(kTabs), std::end(kTabs), [&](const Tab &x) { return x.name == o.page; });
  return query(o, [&] { t != std::end(kTabs) ? win::post(host(), win::kWmCommand, t->cmd) : click_func(o.page); }, timeout);
}

Hwnd form(const std::string &page, const std::string &parent, long mark, const std::string &text, double timeout) {
  trace::Span span("开页「" + page + "」", {{"mark", mark}, {"text", text}});
  const Hwnd h = host();
  idle(h, timeout);
  click_func(page, parent);
  return *until(
      said(strf("「%s」页 (控件 %ld「%s」) 显示出来", page.c_str(), mark, text.c_str())),
      [&]() -> std::optional<Hwnd> {
        const std::vector<Hwnd> s = shown(h);
        if (s.size() != 1)
          return std::nullopt;
        for (const Node &n : win::enum_(s[0]))
          if (n.depth == 1 && n.id == mark && n.vis && n.title == text)
            return s[0];
        return std::nullopt;
      },
      timeout, 0.05);
}

// ---------- 弹框 ----------

Hwnd popup(const std::vector<std::string> &titles, double timeout, const std::function<void()> &act, const std::vector<std::string> &again) {
  trace::Span span("弹框", {{"titles", titles}});
  const win::Tops before = win::tops();
  act();
  std::string want;
  for (const std::string &t : titles)
    want += (want.empty() ? "" : "/") + t;
  std::string title;
  const Hwnd box = *until(
      said("「" + want + "」"),
      [&]() -> std::optional<Hwnd> {
        std::vector<Hwnd> hits;
        for (const auto &[h, n] : fresh(before)) {
          if (std::find(titles.begin(), titles.end(), n.title) == titles.end()) {
            const std::string b = body(h);
            if (std::none_of(again.begin(), again.end(), [&](const std::string &s) { return b.find(s) != std::string::npos; }))
              fail(strf("等「%s」时先弹出「%s」%s", want.c_str(), n.title.c_str(), b.c_str()));
            trace::ev("again", {{"title", n.title}, {"body", b}});
            win::post(h, win::kWmClose, 0);
            until(said("「" + n.title + "」" + b + " 关掉"), [x = h] { return !win::tops().contains(x); }, 3);
            std::this_thread::sleep_for(Secs(kAgainGap));
            act();
            return std::nullopt;
          }
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
  std::map<long, std::vector<Hwnd>> hits;
  for (const Node &n : win::enum_(box))
    if (n.depth == 1 && (n.id == kMsgText || n.id == kBoxText))
      hits[n.id].push_back(n.hwnd);
  for (const long id : {kMsgText, kBoxText})
    if (const std::vector<Hwnd> &h = hits[id]; !h.empty())
      return h.size() == 1 ? win::squash(win::text(h[0])) : "";
  return "";
}

void yes(const Hwnd &box) { win::command(box, kMsgYes); }

// 每轮只关可用的 (模态框盖着的闪电窗此时禁用, 等上面的关了再关)
void close_new(const win::Tops &before) {
  trace::Span span("收尾");
  const auto left = [&] {
    std::vector<std::string> v;
    for (const auto &[h, n] : fresh(before))
      v.push_back(n.title);
    return v;
  };
  const bool done = poll(
      [&] {
        const win::Tops t = fresh(before);
        for (const auto &[h, n] : t)
          if (n.en) {
            trace::ev("close", {{"title", n.title}, {"hwnd", h}, {"body", body(h)}});
            win::post(h, win::kWmClose, 0);
          }
        return t.empty();
      },
      3, kAgainGap);
  if (!done)
    trace::ev("left", {{"titles", left()}});
}

} // namespace live::tdx::ui
