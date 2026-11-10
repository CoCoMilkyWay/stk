#include "live/tdx/Ui.hpp"

#include "live/tdx/Shim.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>

using nlohmann::json;

namespace live::tdx::ui {

namespace {

const std::string kToolbarClass = "MHPToolBar", kToolbarTitle = "MainViewBar";
// 交易栏左侧功能树 (查询 / 资产分析 / …), 右侧为所选功能的页; 树项只认真实光标处的点击, clickat 会钉光标
const std::string kFuncTreeClass = "SysTreeView32";
constexpr long kFuncTreeId = 59653;

int half(int a, int b) { return (a + b) >= 0 ? (a + b) / 2 : -((-(a + b) + 1) / 2); } // 向下取整

std::vector<Node> func_trees() {
  std::vector<Node> out;
  for (Node &n : tree())
    if (n.vis && n.cls == kFuncTreeClass && n.id == kFuncTreeId)
      out.push_back(std::move(n));
  return out;
}

// 可见的查询页 (条数标签所在的对话框)
std::vector<std::string> pages(double timeout) {
  const std::vector<Node> nodes = tree("", timeout);
  std::vector<std::string> out;
  for (size_t i = 0; i < nodes.size(); ++i) {
    const Node &n = nodes[i];
    if (!(n.vis && n.cls == "Static" && n.id == kPageCount))
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
std::vector<std::string> cols(const std::string &dlg, double timeout) {
  text(dlg, timeout);
  std::vector<std::string> out;
  for (const Node &n : tree(dlg, timeout))
    if (n.vis && n.cls == "SysHeader32")
      for (const Item &it : layer_of(shim::cmd_wait(timeout, "detail", n.hwnd).at(0)).items)
        if (it.text)
          out.push_back(*it.text);
  return out;
}

// 空白: ASCII 空白、不换行空格 U+00A0、全角空格 U+3000
size_t space_at(const std::string &s, size_t i) {
  const unsigned char c = static_cast<unsigned char>(s[i]);
  if (std::isspace(c))
    return 1;
  if (c == 0xC2 && i + 1 < s.size() && static_cast<unsigned char>(s[i + 1]) == 0xA0)
    return 2;
  if (c == 0xE3 && i + 2 < s.size() && static_cast<unsigned char>(s[i + 1]) == 0x80 && static_cast<unsigned char>(s[i + 2]) == 0x80)
    return 3;
  return 0;
}

} // namespace

Node node_of(const json &j) {
  Node n;
  n.depth = j.at("depth").get<int>();
  n.hwnd = j.at("hwnd").get<std::string>();
  n.id = j.at("id").get<long>();
  n.cls = j.at("class").get<std::string>();
  n.title = j.at("title").get<std::string>();
  const json &r = j.at("rect");
  for (int i = 0; i < 4; ++i)
    n.rect[i] = r.at(static_cast<size_t>(i)).get<int>();
  n.style = j.at("style").get<unsigned>();
  n.exstyle = j.at("exstyle").get<unsigned>();
  n.vis = j.at("vis").get<long>() != 0;
  n.en = j.at("en").get<long>() != 0;
  return n;
}

std::vector<Node> nodes_of(const std::vector<json> &recs) {
  std::vector<Node> out;
  for (const json &j : recs)
    out.push_back(node_of(j));
  return out;
}

Layer layer_of(const json &j) {
  Layer l;
  l.hwnd = j.at("hwnd").get<std::string>();
  l.off[0] = j.at("off").at(0).get<int>();
  l.off[1] = j.at("off").at(1).get<int>();
  for (const json &it : j.at("items")) {
    Item x{it.at(0).get<int>(), it.at(1).get<int>(), it.at(2).get<int>(), it.at(3).get<int>(), it.at(4).get<unsigned>(), std::nullopt};
    if (it.size() > 5)
      x.text = it.at(5).get<std::string>();
    l.items.push_back(std::move(x));
  }
  return l;
}

Gate::Hold::Hold(Gate &g, const char *who) : g_(g) {
  g_.mu_.lock();
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

void attach() {
  shim::ensure();
  for (const char *c : {"noraise", "gdi"}) {
    const std::vector<json> r = shim::cmd(c, "on");
    bool ok = !r.empty() && r.back().value("on", 0L) != 0;
    for (const json &x : r)
      ok = ok && !x.contains("skip");
    if (!ok)
      fail(strf("%s on: %s", c, json(r).dump().c_str()));
  }
}

std::vector<Node> enum_(const std::string &target) { return nodes_of(target.empty() ? shim::cmd("enum") : shim::cmd("enum", target)); }

std::vector<Node> tree(const std::string &target, double timeout) {
  return nodes_of(target.empty() ? shim::cmd_wait(timeout, "tree") : shim::cmd_wait(timeout, "tree", target));
}

std::vector<Node> find_wnd(const std::string &query) { return nodes_of(shim::cmd("find", query)); }

void post(const std::string &h, unsigned msg, long wp, const std::string &lp) {
  if (!shim::cmd("post", h, msg, wp, lp).at(0).at("ret").get<long>())
    fail(strf("PostMessage %s %#x 失败", h.c_str(), msg));
}

void send(const std::string &h, unsigned msg, long wp, long lp) { shim::cmd("send", h, msg, wp, lp); }

std::string title(const std::string &h) { return shim::cmd("rect", h).at(0).at("title").get<std::string>(); }

std::string text(const std::string &h, double timeout) { return shim::cmd_wait(timeout, "gettext", h).at(0).at("text").get<std::string>(); }

void settext(const std::string &h, const std::string &s) {
  if (!shim::cmd("settext", h, s).at(0).at("ret").get<long>())
    fail("写 " + h + " 失败");
}

unsigned regmsg(const char *name) { return shim::cmd("regmsg", name).at(0).at("msg").get<unsigned>(); }

void clickat(const std::string &h, int x, int y) { shim::cmd("clickat", h, x, y); }

std::map<std::string, std::string> ctls(const std::string &h, const std::vector<std::pair<std::string, long>> &ids) {
  std::vector<Node> kids;
  for (Node &n : enum_(h))
    if (n.depth == 1)
      kids.push_back(std::move(n));
  std::map<std::string, std::string> out;
  for (const auto &[k, cid] : ids) {
    std::vector<std::string> hits;
    for (const Node &n : kids)
      if (n.id == cid)
        hits.push_back(n.hwnd);
    if (hits.size() != 1)
      fail(strf("%s「%s」控件 %s(%ld) 命中 %zu 个", h.c_str(), title(h).c_str(), k.c_str(), cid, hits.size()));
    out[k] = hits[0];
  }
  return out;
}

void command(const std::string &dlg, long cid) { post(dlg, kWmCommand, cid, ctls(dlg, {{"btn", cid}}).at("btn")); }

void click_text(const std::string &cls, const std::string &ttl, const std::string &txt) {
  std::map<std::string, Node> wins;
  for (Node &n : tree())
    if (n.vis && (cls.empty() || n.cls == cls) && (ttl.empty() || n.title == ttl))
      wins.emplace(n.hwnd, std::move(n));
  struct Hit {
    const Node *n;
    int ox, oy;
    Item it;
  };
  std::vector<Hit> hits;
  for (const json &d : shim::cmd("gdidump")) {
    const Layer l = layer_of(d);
    const auto w = wins.find(l.hwnd);
    if (w == wins.end())
      continue;
    for (const Item &it : l.items)
      if (it.text && squash(*it.text) == txt)
        hits.push_back({&w->second, l.off[0], l.off[1], it});
  }
  if (hits.size() != 1)
    fail(strf("「%s」(%s %s) 命中 %zu 处, 需唯一", txt.c_str(), cls.c_str(), ttl.c_str(), hits.size()));
  const Hit &h = hits[0];
  clickat(h.n->hwnd, h.n->rect[0] + h.ox + half(h.it.l, h.it.r), h.n->rect[1] + h.oy + half(h.it.t, h.it.b));
}

void click_tool(const std::string &txt) { click_text(kToolbarClass, kToolbarTitle, txt); }

void show_bar() {
  if (!func_trees().empty())
    return;
  // 主窗顶栏的「交易」: 交易栏收起 (整个停靠条高 0、功能树与 MainViewBar 都不可见) 时点它展开
  click_text("AfxWnd100", "", "交易");
  wait(said("点顶栏「交易」后交易栏展开"), [] { return !func_trees().empty(); }, 5);
}

void click_func(const std::string &name) {
  const std::vector<Node> trees = func_trees();
  if (trees.size() != 1)
    fail(strf("交易功能树命中 %zu 个", trees.size()));
  const Node &t = trees[0];
  send(t.hwnd, kWmVscroll, kSbTop, 0);
  std::optional<std::vector<std::string>> seen;
  while (true) {
    const std::vector<Item> items = layer_of(shim::cmd("detail", t.hwnd).at(0)).items;
    std::vector<std::string> names;
    for (const Item &it : items) {
      if (it.text && *it.text == name) {
        clickat(t.hwnd, t.rect[0] + half(it.l, it.r), t.rect[1] + half(it.t, it.b));
        return;
      }
      names.push_back(it.text.value_or(""));
    }
    if (seen && names == *seen)
      fail("交易功能树里没有「" + name + "」");
    seen = std::move(names);
    send(t.hwnd, kWmVscroll, kSbPagedown, 0);
  }
}

std::string open_page(const std::string &func, const std::string &col, double timeout) {
  click_func(func);
  return *wait(
      said("「" + func + "」页 (表头含「" + col + "」)"),
      [&]() -> std::optional<std::string> {
        for (const std::string &d : pages(timeout)) {
          const std::vector<std::string> c = cols(d, timeout);
          if (std::find(c.begin(), c.end(), col) != c.end())
            return d;
        }
        return std::nullopt;
      },
      timeout);
}

std::string count_label(const std::string &dlg) { return ctls(dlg, {{"n", kPageCount}}).at("n"); }

std::optional<int> page_count(const std::string &lbl, double timeout) {
  const std::string t = text(lbl, timeout);
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

std::string bare(const std::string &s) {
  std::string out;
  for (size_t i = 0; i < s.size();) {
    if (const size_t k = space_at(s, i)) {
      i += k;
      continue;
    }
    out += s[i++];
  }
  return out;
}

std::string squash(const std::string &s) {
  std::string out;
  bool gap = false;
  for (size_t i = 0; i < s.size();) {
    if (const size_t k = space_at(s, i)) {
      gap = true;
      i += k;
      continue;
    }
    if (gap && !out.empty())
      out += ' ';
    gap = false;
    out += s[i++];
  }
  return out;
}

} // namespace live::tdx::ui
