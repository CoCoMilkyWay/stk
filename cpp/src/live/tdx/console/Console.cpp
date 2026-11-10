#include "live/tdx/Console.hpp"

#include "live/Broker.hpp"
#include "live/tdx/Shim.hpp"
#include "live/tdx/console/Model.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <thread>

namespace live::tdx::console {

namespace {

using Lock = std::lock_guard<std::mutex>;

constexpr ImVec4 kHot(0xd7 / 255.f, 0xba / 255.f, 0x7d / 255.f, 1), kAcc(0x4e / 255.f, 0xc9 / 255.f, 0xb0 / 255.f, 1),
    kRed(0xf1 / 255.f, 0x4c / 255.f, 0x4c / 255.f, 1);

int resize_cb(ImGuiInputTextCallbackData *d) {
  if (d->EventFlag == ImGuiInputTextFlags_CallbackResize) {
    auto *s = static_cast<std::string *>(d->UserData);
    s->resize(static_cast<size_t>(d->BufTextLen));
    d->Buf = s->data();
  }
  return 0;
}

void ping(Core &c) {
  std::optional<long> pid;
  try {
    pid = shim::cmd("ping").at(0).at("pid").get<long>();
  } catch (const Fail &) {
  }
  {
    Lock lk(c.mu);
    c.pid = pid;
  }
  if (pid)
    noraise(c, "");
}

// 拉树 + 子项: 树变了才发布、才清已销毁窗口的绘制层 (只清拉树前就到的: 树发出后才建的窗口不在这棵树里,
// 其图层可能先到, 清了就再也不会重推); 列表 / 勾选等内容变化不发 WinEvent, 子项每回都拉, 变了才发布
void pull_tree(Core &c, std::string &tree_key, std::string &detail_key) {
  const Clock::time_point t0 = Clock::now();
  std::vector<json> recs;
  try {
    recs = shim::cmd("tree");
  } catch (const Fail &f) {
    Lock lk(c.mu);
    c.tree = make_tree({}, f.why);
    tree_key.clear();
    return;
  }
  std::string key = json(recs).dump();
  std::shared_ptr<const Tree> t;
  if (key != tree_key) {
    tree_key = std::move(key);
    t = make_tree(win::nodes_of(recs));
    Lock lk(c.mu);
    c.tree = t;
    auto g = std::make_shared<Gdis>(*c.gdi);
    std::erase_if(*g, [&](const auto &kv) { return !t->find(kv.first) && kv.second->at < t0; });
    c.gdi = std::move(g);
  } else {
    Lock lk(c.mu);
    t = c.tree;
  }
  json all = json::array();
  auto d = std::make_shared<Layers>();
  for (const win::Node &n : t->nodes) {
    if (!needs_detail(n))
      continue;
    try {
      const json r = shim::cmd("detail", n.hwnd).at(0);
      d->emplace(n.hwnd, win::layer_of(r));
      all.push_back(r);
    } catch (const Fail &f) {
      all.push_back(f.why);
    }
  }
  key = all.dump();
  if (key == detail_key)
    return;
  detail_key = std::move(key);
  Lock lk(c.mu);
  c.detail = std::move(d);
}

// 树的触发: 推送 / 手动 (want_tree)、轮询 (自动更新且 fps > 0; Wine 9 改标题不发 WinEvent, 推送收不到)、交互后 (soon);
// 另每 3s ping 一次 (连带不抬窗状态), Spy 自动时每 1s 抓一次
void poll_loop(const std::shared_ptr<Core> &cp) {
  Core &c = *cp;
  Clock::time_point next_poll{}, next_spy{}, next_ping{};
  std::string tree_key, detail_key;
  while (true) {
    bool pull, soon, spy_now, ping_now;
    {
      std::unique_lock lk(c.mu);
      c.cv.wait_for(lk, 50ms, [&] { return c.stop || (c.want_tree && c.active(Clock::now())); });
      if (c.stop)
        return;
      const Clock::time_point now = Clock::now();
      if (!c.active(now))
        continue;
      soon = c.soon && now >= *c.soon;
      if (soon)
        c.soon.reset();
      const bool poll = c.auto_upd && c.fps > 0 && now >= next_poll;
      if (poll)
        next_poll = now + std::chrono::milliseconds(1000 / c.fps);
      pull = c.want_tree || poll || soon;
      c.want_tree = false;
      spy_now = c.spy_auto && now >= next_spy;
      if (spy_now)
        next_spy = now + 1s;
      ping_now = now >= next_ping;
      if (ping_now)
        next_ping = now + 3s;
    }
    if (ping_now)
      ping(c);
    if (pull)
      pull_tree(c, tree_key, detail_key);
    if (soon)
      noraise(c, "");
    if (spy_now)
      spy(c, "dump", "");
  }
}

// 长连接 treewatch (来 {gen} 即要拉树) / gdiwatch (来一层即记下); 页面停画即断, 断了隔 1s 重连 (重连即先推全量)
void watch_loop(const std::shared_ptr<Core> &cp, bool gdi) {
  Core &c = *cp;
  while (true) {
    {
      std::unique_lock lk(c.mu);
      c.cv.wait_for(lk, 300ms, [&] { return c.stop; });
      if (c.stop)
        return;
      if (!c.active(Clock::now()))
        continue;
    }
    try {
      shim::watch(gdi ? "gdiwatch" : "treewatch", [&](const json &r) {
        Lock lk(c.mu);
        if (c.stop || !c.active(Clock::now()))
          return false;
        if (r.is_null())
          return true;
        if (gdi) {
          auto g = std::make_shared<Gdis>(*c.gdi);
          win::Layer l = win::layer_of(r);
          std::string h = l.hwnd;
          (*g)[std::move(h)] = std::make_shared<const Gdi>(Gdi{std::move(l), Clock::now()});
          c.gdi = std::move(g);
        } else if (c.auto_upd) {
          c.want_tree = true;
          c.cv.notify_all();
        }
        return true;
      });
    } catch (const Fail &) {
      std::this_thread::sleep_for(1s);
    }
  }
}

// 输入串成一条队列, 到达客户端的顺序与操作顺序一致; 每条之后稍等客户端处理完补拉一次树 (其间再交互并入这一次)
void input_loop(const std::shared_ptr<Core> &cp) {
  Core &c = *cp;
  while (true) {
    Core::Input in;
    {
      std::unique_lock lk(c.mu);
      c.cv.wait(lk, [&] { return c.stop || !c.inputs.empty(); });
      if (c.stop)
        return;
      in = std::move(c.inputs.front());
      c.inputs.pop_front();
    }
    std::string err;
    try {
      shim::call(in.line);
    } catch (const Fail &f) {
      err = json{{"err", f.why}}.dump(1);
    }
    Lock lk(c.mu);
    c.dropping[in.drop] = false;
    if (!err.empty())
      c.sel_out = std::move(err);
    if (!c.soon)
      c.soon = Clock::now() + kSoon;
    c.cv.notify_all();
  }
}

void act_loop(const std::shared_ptr<Core> &cp) {
  Core &c = *cp;
  while (true) {
    std::function<void()> fn;
    {
      std::unique_lock lk(c.mu);
      c.cv.wait(lk, [&] { return c.stop || !c.acts.empty(); });
      if (c.stop)
        return;
      fn = std::move(c.acts.front());
      c.acts.pop_front();
    }
    fn();
  }
}

void header(Core &c, View &v) {
  if (v.pid)
    ImGui::TextColored(kAcc, "shim 在线 · pid=%ld", *v.pid);
  else {
    ImGui::TextColored(kRed, "shim 离线");
    ImGui::SameLine();
    if (ImGui::Button("接入"))
      act(c, [&c] {
        try {
          win::attach();
          Lock lk(c.mu);
          c.status.clear();
        } catch (const Fail &f) {
          Lock lk(c.mu);
          c.status = f.why;
        }
        ping(c);
      });
    ImGui::SetItemTooltip("编译 + 注入 shim, 再开不抬窗与绘制层");
  }
  if (!v.status.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(kRed, "✗ %s", v.status.c_str());
  }
  ImGui::SameLine(0, ImGui::GetFontSize() * 2);
  bool on = v.noraise;
  if (ImGui::Checkbox("不抬窗", &on))
    act(c, [&c, on] { noraise(c, on ? "on" : "off"); });
  ImGui::SetItemTooltip("拦客户端顶层窗口的 z 序变化 (弹窗不抢到前台)");
  ImGui::SameLine();
  ImGui::TextDisabled("已拦 %ld 次", v.blocked);
  ImGui::SameLine();
  if (ImGui::Button("刷新窗口树")) {
    Lock lk(c.mu);
    c.want_tree = true;
    c.cv.notify_all();
  }
  ImGui::SameLine();
  if (ImGui::Button("ping"))
    act(c, [&c] { ping(c); });
}

} // namespace

const win::Node *Tree::find(const std::string &hwnd) const {
  const auto i = at.find(hwnd);
  return i == at.end() ? nullptr : &nodes[i->second];
}

std::shared_ptr<const Tree> make_tree(std::vector<win::Node> nodes, std::string err) {
  auto t = std::make_shared<Tree>();
  t->nodes = std::move(nodes);
  t->err = std::move(err);
  t->kids.resize(t->nodes.size());
  std::vector<size_t> stack;
  for (size_t i = 0; i < t->nodes.size(); ++i) {
    const win::Node &n = t->nodes[i];
    t->at.emplace(n.hwnd, i);
    while (!stack.empty() && t->nodes[stack.back()].depth >= n.depth)
      stack.pop_back();
    (stack.empty() ? t->roots : t->kids[stack.back()]).push_back(i);
    stack.push_back(i);
  }
  return t;
}

void act(Core &c, std::function<void()> fn) {
  Lock lk(c.mu);
  c.acts.push_back(std::move(fn));
  c.cv.notify_all();
}

void input(Core &c, std::string line, int drop) {
  assert(drop >= 0 && drop < 3);
  Lock lk(c.mu);
  if (drop && c.dropping[drop])
    return;
  c.dropping[drop] = drop != 0;
  c.inputs.push_back({std::move(line), drop});
  c.cv.notify_all();
}

std::string shown(const std::function<std::vector<json>()> &fn) {
  try {
    return json(fn()).dump(1);
  } catch (const Fail &f) {
    return json{{"err", f.why}}.dump(1);
  }
}

void noraise(Core &c, const std::string &arg) {
  try {
    const std::vector<json> r = arg.empty() ? shim::cmd("noraise") : shim::cmd("noraise", arg);
    Lock lk(c.mu);
    c.noraise = r.at(0).at("on").get<long>() != 0;
    c.blocked = r.at(0).at("blocked").get<long>();
  } catch (const Fail &f) {
    Lock lk(c.mu);
    c.sel_out = json{{"err", f.why}}.dump(1);
  }
}

void spy(Core &c, const std::string &action, const std::string &filter) {
  try {
    const std::vector<json> r = action == "on" ? shim::cmd("spy", action, filter) : shim::cmd("spy", action);
    Lock lk(c.mu);
    if (action != "dump") {
      c.spy_st = "spy " + action + " ✓";
      return;
    }
    c.spy.clear();
    for (const json &e : r)
      c.spy.push_back({e.at("msg").get<std::string>(), e.at("hwnd").get<std::string>(), e.at("class").get<std::string>(), e.at("wparam").get<std::string>(),
                       e.at("lparam").get<std::string>()});
    c.spy_st = strf("抓到 %zu 条 · %s", r.size(), clock_now().c_str());
  } catch (const Fail &f) {
    Lock lk(c.mu);
    c.spy_st = "✗ " + f.why;
  }
}

std::string trim(std::string_view s) {
  const auto sp = [](char ch) { return std::isspace(static_cast<unsigned char>(ch)) != 0; };
  while (!s.empty() && sp(s.front()))
    s.remove_prefix(1);
  while (!s.empty() && sp(s.back()))
    s.remove_suffix(1);
  return std::string(s);
}

std::string cut(const std::string &s, size_t n) {
  size_t i = 0;
  for (size_t k = 0; i < s.size(); ++i)
    if ((static_cast<unsigned char>(s[i]) & 0xC0) != 0x80 && k++ == n)
      break;
  return s.substr(0, i);
}

std::string lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return s;
}

bool Filter::match(const win::Node &n) const {
  return text.empty() || lower(n.cls + " " + n.title + " " + n.hwnd + " " + std::to_string(n.id)).find(text) != std::string::npos;
}
bool Filter::keep(const win::Node &n) const {
  return !(only_vis && !n.vis) && !(hide_zero && (n.rect[0] == n.rect[2] || n.rect[1] == n.rect[3])) && match(n);
}
Filter filter_of(const View &v) { return {lower(win::squash(v.filter)), v.only_vis, v.hide_zero}; }

std::optional<Pick> pick_of(const View &v, const std::string &id) {
  if (id.empty())
    return std::nullopt;
  const std::string host = id.substr(0, id.find('#'));
  const win::Node *n = v.tree->find(host);
  if (!n)
    return std::nullopt;
  if (host == id)
    return Pick{*n, ""};
  for (Pick &e : elems_of(v, *n))
    if (e.hwnd == id)
      return std::move(e);
  return std::nullopt;
}

void select(View &v, const Pick &p) {
  v.sel = p;
  v.m_target = p.host.empty() ? p.hwnd : p.host;
}

bool input_text(const char *id, std::string &s, const char *hint, ImGuiInputTextFlags flags) {
  return ImGui::InputTextWithHint(id, hint, s.data(), s.capacity() + 1, flags | ImGuiInputTextFlags_CallbackResize, resize_cb, &s);
}

void out_box(const char *id, const std::string &s, float lines) {
  ImGui::InputTextMultiline(id, const_cast<char *>(s.c_str()), s.size() + 1, ImVec2(-1, lines > 0 ? ImGui::GetTextLineHeight() * lines : -1),
                            ImGuiInputTextFlags_ReadOnly);
}

Console::Console() : c_(std::make_shared<Core>()), v_(std::make_unique<View>()) {
  std::thread([c = c_] { poll_loop(c); }).detach();
  std::thread([c = c_] { watch_loop(c, false); }).detach();
  std::thread([c = c_] { watch_loop(c, true); }).detach();
  std::thread([c = c_] { input_loop(c); }).detach();
  std::thread([c = c_] { act_loop(c); }).detach();
}

Console::~Console() {
  Lock lk(c_->mu);
  c_->stop = true;
  c_->rec_halt = true;
  c_->cv.notify_all();
}

void Console::draw() {
  Core &c = *c_;
  View &v = *v_;
  {
    Lock lk(c.mu);
    c.seen = Clock::now();
    c.auto_upd = v.auto_upd;
    c.fps = v.fps;
    c.spy_auto = v.spy_auto;
    c.cv.notify_all();
    v.tree = c.tree;
    v.detail = c.detail;
    v.gdi = c.gdi;
    v.pid = c.pid;
    v.noraise = c.noraise;
    v.blocked = c.blocked;
    v.status = c.status;
    v.sel_out = c.sel_out;
    v.msg_out = c.msg_out;
    v.spy_st = c.spy_st;
    v.req_st = c.req_st;
    v.spy = c.spy;
    v.rec_on = c.rec_on;
    v.rec_line = !c.rec_st.empty()    ? "✗ " + c.rec_st
                 : c.rec_file.empty() ? std::string("未录制")
                                      : strf("%s · %ld 条 · %s", c.rec_on ? "录制中" : "已停", c.rec_n, c.rec_file.c_str());
    if (v.rec_ver != c.rec_ver) {
      v.rec_ver = c.rec_ver;
      v.rec_tail = c.rec_tail;
    }
    if (v.probe_ver != c.probe_ver) {
      v.probe_ver = c.probe_ver;
      v.probe_out = c.probe_out;
    }
    v.probing = c.probing;
  }
  v.hover = std::move(v.hover_next);
  v.hover_map = v.hover_map_next;
  v.hover_next.clear();
  v.hover_map_next = false;

  header(c, v);
  const float h = ImGui::GetContentRegionAvail().y;
  if (v.top_h <= 0)
    v.top_h = h * 0.6f;
  v.top_h = std::clamp(v.top_h, std::min(160.f, h / 2), std::max(h - 160.f, h / 2));
  ImGui::BeginChild("##top", ImVec2(0, v.top_h), ImGuiChildFlags_None, ImGuiWindowFlags_NoScrollbar | ImGuiWindowFlags_NoScrollWithMouse);
  draw_map(c, v);
  ImGui::EndChild();

  ImGui::InvisibleButton("##gutter", ImVec2(-1, 6));
  if (ImGui::IsItemActive())
    v.top_h += ImGui::GetIO().MouseDelta.y;
  if (ImGui::IsItemHovered() || ImGui::IsItemActive())
    ImGui::SetMouseCursor(ImGuiMouseCursor_ResizeNS);
  const ImVec2 g0 = ImGui::GetItemRectMin(), g1 = ImGui::GetItemRectMax();
  ImGui::GetWindowDrawList()->AddLine(ImVec2(g0.x, (g0.y + g1.y) / 2), ImVec2(g1.x, (g0.y + g1.y) / 2),
                                      ImGui::GetColorU32(ImGui::IsItemActive() ? kHot : ImGui::GetStyleColorVec4(ImGuiCol_Separator)));

  ImGui::BeginChild("##bot");
  if (ImGui::BeginTabBar("##bottabs")) {
    if (ImGui::BeginTabItem("工具")) {
      draw_tools(c, v);
      ImGui::EndTabItem();
    }
    if (ImGui::BeginTabItem("列表")) {
      draw_list(c, v);
      ImGui::EndTabItem();
    }
    if (ImGui::BeginTabItem(v.rec_on ? "录制 ●###rec" : "录制###rec")) {
      draw_rec(c_, v);
      ImGui::EndTabItem();
    }
    if (ImGui::BeginTabItem("探内存")) {
      draw_probe(c_, v);
      ImGui::EndTabItem();
    }
    ImGui::EndTabBar();
  }
  ImGui::EndChild();
}

} // namespace live::tdx::console
