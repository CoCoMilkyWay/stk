#include "live/tdx/Win.hpp"

#include "live/Broker.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/Trace.hpp"
#include "live/tdx/Until.hpp"

#include <cassert>
#include <cctype>

using nlohmann::json;

namespace live::tdx::win {

namespace {

constexpr double kRedraw = 2; // click_text: 一处没有时等绘制层画回来 [s]

int half(int a, int b) { return (a + b) >= 0 ? (a + b) / 2 : -((-(a + b) + 1) / 2); } // 向下取整

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

Node node_of(const json &j) {
  Node n;
  n.depth = j.at("depth").get<int>();
  n.hwnd = j.at("hwnd").get<std::string>();
  n.parent = j.at("parent").get<std::string>();
  n.id = j.at("id").get<long>();
  n.cls = j.at("class").get<std::string>();
  n.title = j.at("title").get<std::string>();
  const json &r = j.at("rect");
  for (int i = 0; i < 4; ++i)
    n.rect[i] = r.at(static_cast<size_t>(i)).get<int>();
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

} // namespace

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

std::pair<int, int> center(const Item &it) { return {half(it.l, it.r), half(it.t, it.b)}; }

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
  shim::cmd("detours");
}

// ---------- 窗口 ----------

std::vector<Node> enum_(const Hwnd &target) { return nodes_of(target.empty() ? shim::cmd("enum") : shim::cmd("enum", target)); }

std::vector<Node> find(const std::string &cls, std::optional<long> id, const std::string &title) {
  assert(!cls.empty() && cls.find(' ') == std::string::npos);
  std::string line = "find class=" + cls;
  if (id)
    line += " id=" + std::to_string(*id);
  if (!title.empty())
    line += " title=" + title;
  return nodes_of(shim::call(line));
}

Node node(const Hwnd &h) { return node_of(shim::cmd("rect", h).at(0)); }

Tops tops() {
  Tops out;
  for (Node &n : enum_())
    if (n.depth == 0 && n.vis)
      out.emplace(n.hwnd, std::move(n));
  return out;
}

long disables(const Hwnd &h) { return shim::cmd("disables", h).at(0).at("n").get<long>(); }

// ---------- 消息 ----------

void post(const Hwnd &h, unsigned msg, long wp, const std::string &lp) {
  if (!shim::cmd("post", h, msg, wp, lp).at(0).at("ret").get<long>())
    fail(strf("PostMessage %s %#x 失败", h.c_str(), msg));
}

void send(const Hwnd &h, unsigned msg, long wp, long lp) { shim::cmd("send", h, msg, wp, lp); }

std::string title(const Hwnd &h) { return shim::cmd("rect", h).at(0).at("title").get<std::string>(); }

std::string text(const Hwnd &h, double timeout) { return shim::cmd_wait(timeout, "gettext", h).at(0).at("text").get<std::string>(); }

void settext(const Hwnd &h, const std::string &s) {
  if (!shim::cmd("settext", h, s).at(0).at("ret").get<long>())
    fail("写 " + h + " 失败");
}

unsigned regmsg(const char *name) { return shim::cmd("regmsg", name).at(0).at("msg").get<unsigned>(); }

void clickat(const Hwnd &h, int x, int y) { shim::cmd("clickat", h, x, y); }

// ---------- 控件 ----------

std::map<std::string, Hwnd> ctls(const Hwnd &h, const std::vector<std::pair<std::string, long>> &ids) {
  std::vector<Node> kids;
  for (Node &n : enum_(h))
    if (n.depth == 1)
      kids.push_back(std::move(n));
  std::map<std::string, Hwnd> out;
  for (const auto &[k, cid] : ids) {
    std::vector<Hwnd> hits;
    for (const Node &n : kids)
      if (n.id == cid)
        hits.push_back(n.hwnd);
    if (hits.size() != 1)
      fail(strf("%s「%s」控件 %s(%ld) 命中 %zu 个", h.c_str(), title(h).c_str(), k.c_str(), cid, hits.size()));
    out[k] = hits[0];
  }
  return out;
}

void command(const Hwnd &dlg, long cid) { post(dlg, kWmCommand, cid, ctls(dlg, {{"btn", cid}}).at("btn")); }

// 候选窗口恰一个即只取它的图层, 否则取全部再按候选筛; 不恰一处时先把候选窗口上画着的文字记进日志
void click_text(const std::string &cls, const std::string &ttl, const std::string &txt) {
  trace::Span span("点「" + txt + "」");
  std::map<Hwnd, Node> wins;
  struct Hit {
    const Node *n;
    int ox, oy;
    Item it;
  };
  std::vector<Hit> hits;
  trace::Kw seen;
  poll(
      [&] {
        wins.clear();
        hits.clear();
        seen = trace::Kw::object();
        for (Node &n : find(cls, std::nullopt, ttl))
          if (n.vis)
            wins.emplace(n.hwnd, std::move(n));
        for (const json &d : wins.size() == 1 ? shim::cmd("gdidump", wins.begin()->first) : shim::cmd("gdidump")) {
          const Layer l = layer_of(d);
          const auto w = wins.find(l.hwnd);
          if (w == wins.end())
            continue;
          trace::Kw &texts = seen[l.hwnd + " " + w->second.cls + "「" + squash(w->second.title) + "」"] = trace::Kw::array();
          for (const Item &it : l.items) {
            const std::string t = it.text ? squash(*it.text) : "";
            if (t.empty())
              continue;
            if (t == txt)
              hits.push_back({&w->second, l.off[0], l.off[1], it});
            texts.push_back(t);
          }
        }
        return !hits.empty();
      },
      kRedraw, 0.1);
  if (hits.size() != 1) {
    trace::ev("texts", {{"wins", wins.size()}, {"seen", std::move(seen)}});
    fail(strf("「%s」(%s %s) 命中 %zu 处, 需唯一", txt.c_str(), cls.c_str(), ttl.c_str(), hits.size()));
  }
  const Hit &h = hits[0];
  const auto [x, y] = center(h.it);
  clickat(h.n->hwnd, h.n->rect[0] + h.ox + x, h.n->rect[1] + h.oy + y);
}

// ---------- 文字 ----------

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

} // namespace live::tdx::win
