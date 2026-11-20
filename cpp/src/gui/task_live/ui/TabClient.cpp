#include "gui/task_live/ui/TabClient.hpp"

#include "gui/util/Tip.hpp"
#include "live/Clients.hpp"

#include "imgui.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <set>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

namespace GUI::Live {

namespace {

using live::DrawItem;
using live::DrawLayer;
using live::Mirror;
using live::MirrorFrame;
using live::strf;
using live::WinNode;
using Clock = std::chrono::steady_clock;

// Win32 COLORREF (0x00BBGGRR) → ImU32
ImU32 colorref(unsigned v, unsigned alpha = 255) {
  return IM_COL32(v & 0xFFu, (v >> 8) & 0xFFu, (v >> 16) & 0xFFu, alpha);
}

bool has_detail_cls(std::string_view cls) {
  return cls == "SysListView32" || cls == "SysHeader32" || cls == "SysTreeView32" || cls == "ListBox" || cls == "ComboBox";
}

// ImGuiKey → Win32 VK_* (常用功能键与编辑键)
unsigned vk_of(ImGuiKey k) {
  switch (k) {
  case ImGuiKey_Tab:
    return 0x09;
  case ImGuiKey_Enter:
  case ImGuiKey_KeypadEnter:
    return 0x0D;
  case ImGuiKey_Escape:
    return 0x1B;
  case ImGuiKey_Backspace:
    return 0x08;
  case ImGuiKey_Delete:
    return 0x2E;
  case ImGuiKey_Insert:
    return 0x2D;
  case ImGuiKey_LeftArrow:
    return 0x25;
  case ImGuiKey_UpArrow:
    return 0x26;
  case ImGuiKey_RightArrow:
    return 0x27;
  case ImGuiKey_DownArrow:
    return 0x28;
  case ImGuiKey_PageUp:
    return 0x21;
  case ImGuiKey_PageDown:
    return 0x22;
  case ImGuiKey_Home:
    return 0x24;
  case ImGuiKey_End:
    return 0x23;
  case ImGuiKey_F1:
    return 0x70;
  case ImGuiKey_F2:
    return 0x71;
  case ImGuiKey_F3:
    return 0x72;
  case ImGuiKey_F4:
    return 0x73;
  case ImGuiKey_F5:
    return 0x74;
  case ImGuiKey_F6:
    return 0x75;
  case ImGuiKey_F7:
    return 0x76;
  case ImGuiKey_F8:
    return 0x77;
  case ImGuiKey_F9:
    return 0x78;
  case ImGuiKey_F10:
    return 0x79;
  case ImGuiKey_F11:
    return 0x7A;
  case ImGuiKey_F12:
    return 0x7B;
  default:
    return 0;
  }
}

struct InputEv {
  enum Kind : uint8_t { Mouse,
                        Key,
                        Char } kind = Mouse;
  const char *m_kind = nullptr;
  int x = 0, y = 0, delta = 0;
  unsigned mk = 0, code = 0;
};

// 树节点构建父子索引 (兄弟按先序 = Win32 Z 序顶→底)
struct TreeView {
  std::vector<std::vector<size_t>> kids;
  std::vector<size_t> roots;

  explicit TreeView(const std::vector<WinNode> &ns) : kids(ns.size()) {
    std::vector<size_t> stk;
    for (size_t i = 0; i < ns.size(); ++i) {
      const int d = ns[i].depth;
      while (static_cast<int>(stk.size()) > d)
        stk.pop_back();
      if (stk.empty())
        roots.push_back(i);
      else
        kids[stk.back()].push_back(i);
      stk.push_back(i);
    }
  }
};

bool icontains(std::string_view hay, std::string_view ndl) {
  if (ndl.empty())
    return true;
  auto it = std::search(hay.begin(), hay.end(), ndl.begin(), ndl.end(),
                        [](char a, char b) { return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b)); });
  return it != hay.end();
}

} // namespace

struct ClientState::Impl {
  std::unique_ptr<Mirror> mirror;

  mutable std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<const MirrorFrame> frame;
  std::string err;
  bool stop = false;
  bool live = true;
  bool want_detail = true;
  bool refresh_req = false;
  Clock::time_point touched{};

  std::mutex in_mu;
  std::condition_variable in_cv;
  std::deque<InputEv> inputs;

  void start(std::unique_ptr<Mirror> m) {
    assert(!mirror && m);
    mirror = std::move(m);
    auto self = shared_self.lock();
    assert(self);

    // 线程 1: GDI 增量推送流 + 定时检查窗口树代数
    std::thread([c = self] { c->loop_pull(); }).detach();
    // 线程 2: 鼠键透传队列 (与画面拉取解耦, 不互等)
    std::thread([c = self] { c->loop_input(); }).detach();
  }

  std::weak_ptr<Impl> shared_self;

  bool active() const {
    std::lock_guard lk(mu);
    return !stop && live && (Clock::now() - touched) < std::chrono::seconds(2);
  }

  void push_input(InputEv ev) {
    {
      std::lock_guard lk(in_mu);
      if (inputs.size() > 128)
        inputs.pop_front();
      inputs.push_back(ev);
    }
    in_cv.notify_one();
  }

  void loop_input() {
    while (true) {
      InputEv ev;
      {
        std::unique_lock lk(in_mu);
        in_cv.wait(lk, [&] {
          std::lock_guard slk(mu);
          return stop || !inputs.empty();
        });
        {
          std::lock_guard slk(mu);
          if (stop)
            return;
        }
        ev = inputs.front();
        inputs.pop_front();
      }
      try {
        if (ev.kind == InputEv::Mouse)
          mirror->mouse(ev.m_kind, ev.x, ev.y, ev.mk, ev.delta);
        else if (ev.kind == InputEv::Key)
          mirror->key(ev.code);
        else
          mirror->ch(ev.code);
      } catch (const live::Fail &) {
      }
    }
  }

  void pull_full(MirrorFrame &draft, bool with_detail) {
    draft.nodes = mirror->tree();
    draft.gen = mirror->tree_gen();
    draft.gdi.clear();
    for (DrawLayer &l : mirror->gdi_dump())
      draft.gdi[l.hwnd] = std::move(l);
    draft.detail.clear();
    if (with_detail) {
      for (const WinNode &n : draft.nodes)
        if (n.vis && has_detail_cls(n.cls) && n.rect[2] > n.rect[0] && n.rect[3] > n.rect[1]) {
          try {
            draft.detail[n.hwnd] = mirror->detail(n.hwnd);
          } catch (const live::Fail &) {
          }
        }
    }
  }

  void publish(const MirrorFrame &draft, std::string e = {}) {
    auto snap = std::make_shared<const MirrorFrame>(draft);
    std::lock_guard lk(mu);
    frame = std::move(snap);
    err = std::move(e);
  }

  void loop_pull() {
    MirrorFrame draft;
    while (true) {
      bool force = false, with_det = true;
      {
        std::unique_lock lk(mu);
        cv.wait_for(lk, std::chrono::milliseconds(150), [&] {
          return stop || refresh_req || (live && (Clock::now() - touched) < std::chrono::seconds(2));
        });
        if (stop)
          return;
        force = refresh_req;
        refresh_req = false;
        with_det = want_detail;
        if (!force && (!live || (Clock::now() - touched) >= std::chrono::seconds(2)))
          continue;
      }

      try {
        mirror->ensure();
        pull_full(draft, with_det);
        publish(draft);
        if (!active())
          continue;

        // 进入实时增量流: 收到 GDI 图层即局部更新并发布; 每 250ms 核一次窗口树代数
        Clock::time_point last_tree = Clock::now();
        mirror->watch_gdi([&](DrawLayer layer) -> bool {
          bool req = false, det = true;
          {
            std::lock_guard lk(mu);
            if (stop || !live || (Clock::now() - touched) >= std::chrono::seconds(2))
              return false;
            req = refresh_req;
            refresh_req = false;
            det = want_detail;
          }
          bool changed = false;
          if (!layer.hwnd.empty()) {
            draft.gdi[layer.hwnd] = std::move(layer);
            changed = true;
          }
          const Clock::time_point now = Clock::now();
          if (req || now - last_tree >= std::chrono::milliseconds(250)) {
            last_tree = now;
            const int64_t g = mirror->tree_gen();
            if (req || g != draft.gen) {
              pull_full(draft, det);
              changed = true;
            }
          }
          if (changed)
            publish(draft);
          return true;
        });
      } catch (const live::Fail &f) {
        {
          std::lock_guard lk(mu);
          err = f.why;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }
    }
  }
};

ClientState::ClientState() : impl(std::make_shared<Impl>()) { impl->shared_self = impl; }
ClientState::~ClientState() { stop(); }
ClientState::ClientState(ClientState &&) noexcept = default;
ClientState &ClientState::operator=(ClientState &&) noexcept = default;

void ClientState::stop() {
  if (!impl)
    return;
  {
    std::lock_guard lk(impl->mu);
    impl->stop = true;
  }
  impl->cv.notify_all();
  impl->in_cv.notify_all();
}

namespace {

// 递归绘制一棵可见子树 (父先于子, 同级逆序 = Win32 Z 序底→顶)
struct PaintCtx {
  const MirrorFrame &fr;
  const TreeView &tv;
  const ClientState &st;
  ImDrawList *dl = nullptr;
  ImVec2 origin{};
  int bx = 0, by = 0; // 客户端包围盒左上 (屏幕坐标)
  float scale = 1.0f;
  std::string hover_hwnd;
  ImVec2 mouse{};
  bool mouse_in = false;

  ImVec2 pt(int x, int y) const { return {origin.x + static_cast<float>(x - bx) * scale, origin.y + static_cast<float>(y - by) * scale}; }

  void draw_layer(const DrawLayer &l, const WinNode &n, bool is_detail) {
    const int ox = n.rect[0] + l.off[0], oy = n.rect[1] + l.off[1];
    const float fs = std::clamp(13.0f * scale, 8.0f, 22.0f);
    for (size_t i = 0; i < l.items.size(); ++i) {
      const DrawItem &it = l.items[i];
      const ImVec2 a = pt(ox + it.l, oy + it.t), b = pt(ox + it.r, oy + it.b);
      if (b.x <= a.x || b.y <= a.y)
        continue;
      if (is_detail) {
        // 标准控件子项: 选中态高亮, 网格浅描边 + 文字
        const bool sel = (it.v & 2u) != 0;
        if (sel)
          dl->AddRectFilled(a, b, IM_COL32(45, 95, 160, 140));
        dl->AddRect(a, b, IM_COL32(90, 95, 110, 80));
        if (it.text && !it.text->empty()) {
          dl->PushClipRect(a, b, true);
          dl->AddText(nullptr, fs, ImVec2(a.x + 2, a.y + std::max(0.0f, (b.y - a.y - fs) * 0.5f)), IM_COL32(230, 232, 240, 255), it.text->c_str());
          dl->PopClipRect();
        }
      } else {
        // GDI 绘制层: 无 text = 色块填充, 有 text = 文本渲染
        if (!it.text) {
          dl->AddRectFilled(a, b, colorref(it.v));
        } else if (!it.text->empty()) {
          dl->PushClipRect(a, b, true);
          dl->AddText(nullptr, fs, ImVec2(a.x, a.y + std::max(0.0f, (b.y - a.y - fs) * 0.5f)), colorref(it.v), it.text->c_str());
          dl->PopClipRect();
        }
        if (n.hwnd == st.sel_hwnd && static_cast<int>(i) == st.sel_item)
          dl->AddRect(a, b, IM_COL32(255, 220, 40, 255), 0, 0, 2.0f);
      }
    }
  }

  void paint(size_t idx, const ImVec2 &clip_a, const ImVec2 &clip_b) {
    const WinNode &n = fr.nodes[idx];
    if (!n.vis)
      return;
    const ImVec2 a = pt(n.rect[0], n.rect[1]), b = pt(n.rect[2], n.rect[3]);
    if (b.x <= a.x || b.y <= a.y)
      return;
    const ImVec2 ca(std::max(a.x, clip_a.x), std::max(a.y, clip_a.y));
    const ImVec2 cb(std::min(b.x, clip_b.x), std::min(b.y, clip_b.y));
    if (cb.x <= ca.x || cb.y <= ca.y)
      return;

    if (mouse_in && mouse.x >= ca.x && mouse.x <= cb.x && mouse.y >= ca.y && mouse.y <= cb.y)
      hover_hwnd = n.hwnd;

    dl->PushClipRect(ca, cb, true);

    const auto gi = fr.gdi.find(n.hwnd);
    const auto di = fr.detail.find(n.hwnd);
    const bool has_gdi = st.show_gdi && gi != fr.gdi.end() && !gi->second.items.empty();
    const bool has_det = st.show_detail && di != fr.detail.end() && !di->second.items.empty();

    // 标准控件兜底背景 (没有 GDI 填充时给暗底 + 标题)
    if (!has_gdi) {
      if (n.depth == 0 || n.cls == "#32770")
        dl->AddRectFilled(a, b, IM_COL32(28, 30, 36, 230));
      else if (n.cls == "Edit" || n.cls == "ComboBox")
        dl->AddRectFilled(a, b, IM_COL32(20, 22, 28, 240));
      else if (n.cls == "Button")
        dl->AddRectFilled(a, b, IM_COL32(55, 62, 74, 230));
      else if (has_det)
        dl->AddRectFilled(a, b, IM_COL32(22, 24, 30, 235));
    }

    if (has_gdi)
      draw_layer(gi->second, n, false);
    if (has_det)
      draw_layer(di->second, n, true);

    if (!has_gdi && !has_det && !n.title.empty() && (n.cls == "Button" || n.cls == "Static" || n.cls == "Edit" || n.cls == "ComboBox" || n.depth == 0)) {
      const float fs = std::clamp(13.0f * scale, 8.0f, 20.0f);
      dl->AddText(nullptr, fs, ImVec2(a.x + 3, a.y + 2), IM_COL32(220, 224, 232, 255), n.title.c_str());
    }

    if (st.show_box || n.cls == "Button" || n.cls == "Edit" || n.depth == 0)
      dl->AddRect(a, b, n.depth == 0 ? IM_COL32(110, 140, 190, 200) : IM_COL32(85, 92, 108, 120));

    // 子节点逆序画 (Win32 兄弟先序 = Z 序顶→底, 逆序即底→顶)
    const auto &ch = tv.kids[idx];
    for (auto it = ch.rbegin(); it != ch.rend(); ++it)
      paint(*it, ca, cb);

    if (n.hwnd == st.sel_hwnd)
      dl->AddRect(a, b, IM_COL32(255, 190, 50, 255), 0, 0, 2.0f);

    dl->PopClipRect();
  }
};

void draw_tree_node(const MirrorFrame &fr, const TreeView &tv, size_t idx, ClientState &st, std::string_view filt) {
  const WinNode &n = fr.nodes[idx];
  if (st.only_vis && !n.vis)
    return;
  const bool hit = icontains(n.cls, filt) || icontains(n.title, filt) || icontains(n.hwnd, filt);
  const auto &ch = tv.kids[idx];
  const auto gi = fr.gdi.find(n.hwnd);
  const size_t gn = gi == fr.gdi.end() ? 0 : gi->second.items.size();
  const auto di = fr.detail.find(n.hwnd);
  const size_t dn = di == fr.detail.end() ? 0 : di->second.items.size();

  if (!filt.empty() && !hit) {
    for (const size_t c : ch)
      draw_tree_node(fr, tv, c, st, filt);
    return;
  }

  ImGuiTreeNodeFlags fl = ImGuiTreeNodeFlags_OpenOnArrow | ImGuiTreeNodeFlags_SpanAvailWidth;
  if (ch.empty())
    fl |= ImGuiTreeNodeFlags_Leaf | ImGuiTreeNodeFlags_NoTreePushOnOpen;
  if (n.hwnd == st.sel_hwnd)
    fl |= ImGuiTreeNodeFlags_Selected;
  if (n.depth == 0)
    fl |= ImGuiTreeNodeFlags_DefaultOpen;

  std::string lbl = strf("%s#%ld", n.cls.c_str(), n.id);
  if (!n.title.empty())
    lbl += " 「" + n.title + "」";
  if (gn)
    lbl += strf(" [G:%zu]", gn);
  if (dn)
    lbl += strf(" [D:%zu]", dn);
  if (!n.vis)
    lbl += " (隐)";

  if (!n.vis)
    ImGui::PushStyleColor(ImGuiCol_Text, ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
  const bool open = ImGui::TreeNodeEx(n.hwnd.c_str(), fl, "%s", lbl.c_str());
  if (!n.vis)
    ImGui::PopStyleColor();
  if (ImGui::IsItemClicked() && !ImGui::IsItemToggledOpen())
    st.sel_hwnd = n.hwnd;
  if (open && !ch.empty()) {
    for (const size_t c : ch)
      draw_tree_node(fr, tv, c, st, filt);
    ImGui::TreePop();
  }
}

} // namespace

void RenderTabClient(LiveHub &hub, ClientState &st) {
  if (!hub.connected()) {
    ImGui::TextUnformatted("接券商客户端:");
    for (size_t i = 0; i < live::clients().size(); ++i) {
      const live::Client &c = live::clients()[i];
      if (ImGui::Button(strf("连接%s", c.name).c_str()))
        hub.connect(i);
      ImGui::SameLine();
      ImGui::TextWrapped("%s", c.about);
    }
    return;
  }

  ClientState::Impl &im = *st.impl;
  if (!im.mirror) {
    std::unique_ptr<Mirror> m = hub.broker().mirror();
    if (!m) {
      ImGui::TextDisabled("当前客户端 (%s) 不支持窗口复现", hub.client().name);
      return;
    }
    im.start(std::move(m));
  }

  // 每帧 touch 与同步选项
  std::shared_ptr<const MirrorFrame> fr;
  std::string err;
  {
    std::lock_guard lk(im.mu);
    im.touched = Clock::now();
    im.live = st.live;
    im.want_detail = st.show_detail;
    fr = im.frame;
    err = im.err;
  }

  // ---------- 顶栏 ----------
  if (ImGui::SmallButton("刷新")) {
    {
      std::lock_guard lk(im.mu);
      im.refresh_req = true;
    }
    im.cv.notify_all();
  }
  ImGui::SameLine();
  ImGui::Checkbox("实时", &st.live);
  if (ImGui::IsItemHovered())
    tip("走 gdiwatch 增量推流 + 窗口树代数变化自动更新");
  ImGui::SameLine();
  if (st.interact)
    ImGui::PushStyleColor(ImGuiCol_CheckMark, ImVec4(1.0f, 0.35f, 0.35f, 1.0f));
  ImGui::Checkbox("交互透传", &st.interact);
  if (st.interact)
    ImGui::PopStyleColor();
  if (ImGui::IsItemHovered())
    tip("勾选后, 画布上的鼠标点击/滚轮与键盘输入直接投递给客户端 (不动真实光标)");
  ImGui::SameLine();
  ImGui::Checkbox("GDI层", &st.show_gdi);
  ImGui::SameLine();
  ImGui::Checkbox("控件项", &st.show_detail);
  ImGui::SameLine();
  ImGui::Checkbox("边框", &st.show_box);
  ImGui::SameLine();
  ImGui::Checkbox("自适应", &st.fit);
  ImGui::SameLine();
  ImGui::Checkbox("检查器", &st.inspector);

  if (fr) {
    // 顶层窗口选择
    ImGui::SameLine();
    ImGui::SetNextItemWidth(ImGui::GetFontSize() * 14);
    std::string cur_top = "全部可见顶层";
    if (!st.top_hwnd.empty()) {
      for (const WinNode &n : fr->nodes)
        if (n.hwnd == st.top_hwnd)
          cur_top = n.title.empty() ? n.cls : n.title;
    }
    if (ImGui::BeginCombo("##top", cur_top.c_str())) {
      if (ImGui::Selectable("全部可见顶层", st.top_hwnd.empty()))
        st.top_hwnd.clear();
      for (const WinNode &n : fr->nodes)
        if (n.depth == 0 && n.vis && n.rect[2] > n.rect[0] && n.rect[3] > n.rect[1]) {
          const std::string lbl = strf("%s %s (%dx%d)##%s", n.cls.c_str(), n.title.c_str(), n.rect[2] - n.rect[0], n.rect[3] - n.rect[1], n.hwnd.c_str());
          if (ImGui::Selectable(lbl.c_str(), st.top_hwnd == n.hwnd))
            st.top_hwnd = n.hwnd;
        }
      ImGui::EndCombo();
    }

    size_t gdi_items = 0;
    for (const auto &[h, l] : fr->gdi)
      gdi_items += l.items.size();
    ImGui::SameLine();
    ImGui::TextDisabled("窗 %zu · GDI %zu层/%zu项 · gen %lld", fr->nodes.size(), fr->gdi.size(), gdi_items, static_cast<long long>(fr->gen));
  }
  if (!err.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(ImVec4(1, 0.35f, 0.35f, 1), "✗ %s", err.c_str());
  }

  if (!fr || fr->nodes.empty()) {
    ImGui::TextDisabled("正在拉取客户端窗口树与 GDI 绘制层…");
    return;
  }

  const TreeView tv(fr->nodes);

  // 计算展示的顶层列表与包围盒
  std::vector<size_t> tops;
  int min_x = std::numeric_limits<int>::max(), min_y = std::numeric_limits<int>::max();
  int max_x = std::numeric_limits<int>::min(), max_y = std::numeric_limits<int>::min();
  for (const size_t r : tv.roots) {
    const WinNode &n = fr->nodes[r];
    if (!n.vis || n.rect[2] <= n.rect[0] || n.rect[3] <= n.rect[1])
      continue;
    if (!st.top_hwnd.empty() && n.hwnd != st.top_hwnd)
      continue;
    tops.push_back(r);
    min_x = std::min(min_x, n.rect[0]);
    min_y = std::min(min_y, n.rect[1]);
    max_x = std::max(max_x, n.rect[2]);
    max_y = std::max(max_y, n.rect[3]);
  }
  if (tops.empty()) {
    st.top_hwnd.clear();
    ImGui::TextDisabled("没有可见顶层窗口");
    return;
  }

  const float avail_w = ImGui::GetContentRegionAvail().x;
  const float left_w = st.inspector ? avail_w * st.split : avail_w;

  // ---------- 左区: GDI 复现画布 ----------
  ImGui::BeginChild("##client_canvas", ImVec2(left_w, 0), ImGuiChildFlags_Borders, st.fit ? ImGuiWindowFlags_NoScrollbar : ImGuiWindowFlags_HorizontalScrollbar);
  const ImVec2 view = ImGui::GetContentRegionAvail();
  const float src_w = static_cast<float>(std::max(1, max_x - min_x));
  const float src_h = static_cast<float>(std::max(1, max_y - min_y));
  const float scale = st.fit ? std::min(view.x / src_w, view.y / src_h) : 1.0f;
  const ImVec2 canvas_sz(src_w * scale, src_h * scale);
  const ImVec2 origin = ImGui::GetCursorScreenPos();

  ImGui::InvisibleButton("##screen", ImVec2(std::max(canvas_sz.x, 1.0f), std::max(canvas_sz.y, 1.0f)),
                         ImGuiButtonFlags_MouseButtonLeft | ImGuiButtonFlags_MouseButtonRight);
  const bool hovered = ImGui::IsItemHovered();
  const bool focused = hovered || ImGui::IsItemActive();
  const ImGuiIO &io = ImGui::GetIO();

  ImDrawList *dl = ImGui::GetWindowDrawList();
  dl->AddRectFilled(origin, ImVec2(origin.x + canvas_sz.x, origin.y + canvas_sz.y), IM_COL32(16, 18, 22, 255));

  PaintCtx ctx{.fr = *fr, .tv = tv, .st = st, .dl = dl, .origin = origin, .bx = min_x, .by = min_y, .scale = scale, .mouse = io.MousePos, .mouse_in = hovered};
  const ImVec2 clip_b(origin.x + canvas_sz.x, origin.y + canvas_sz.y);
  for (auto it = tops.rbegin(); it != tops.rend(); ++it)
    ctx.paint(*it, origin, clip_b);

  // 交互透传 vs 检查点选
  if (hovered) {
    const int sx = min_x + static_cast<int>(std::lround((io.MousePos.x - origin.x) / scale));
    const int sy = min_y + static_cast<int>(std::lround((io.MousePos.y - origin.y) / scale));
    if (st.interact) {
      unsigned mk = 0;
      if (io.KeyShift)
        mk |= 0x0004;
      if (io.KeyCtrl)
        mk |= 0x0008;
      if (ImGui::IsMouseDoubleClicked(0))
        im.push_input({InputEv::Mouse, "ldbl", sx, sy, 0, mk | 0x0001, 0});
      else if (ImGui::IsMouseClicked(0))
        im.push_input({InputEv::Mouse, "ldown", sx, sy, 0, mk | 0x0001, 0});
      if (ImGui::IsMouseReleased(0))
        im.push_input({InputEv::Mouse, "lup", sx, sy, 0, mk, 0});
      if (ImGui::IsMouseDoubleClicked(1))
        im.push_input({InputEv::Mouse, "rdbl", sx, sy, 0, mk | 0x0002, 0});
      else if (ImGui::IsMouseClicked(1))
        im.push_input({InputEv::Mouse, "rdown", sx, sy, 0, mk | 0x0002, 0});
      if (ImGui::IsMouseReleased(1))
        im.push_input({InputEv::Mouse, "rup", sx, sy, 0, mk, 0});
      if (io.MouseWheel != 0)
        im.push_input({InputEv::Mouse, "wheel", sx, sy, static_cast<int>(io.MouseWheel * 120), mk, 0});
    } else if (ImGui::IsMouseClicked(0) && !ctx.hover_hwnd.empty()) {
      st.sel_hwnd = ctx.hover_hwnd;
    }
    if (!st.interact && !ctx.hover_hwnd.empty()) {
      for (const WinNode &n : fr->nodes)
        if (n.hwnd == ctx.hover_hwnd) {
          const auto gi = fr->gdi.find(n.hwnd);
          tip("%s  %s#%ld\n标题: %s\n矩形: [%d,%d,%d,%d] (%dx%d)\nGDI 图元: %zu · 坐标 (%d, %d)", n.hwnd.c_str(), n.cls.c_str(), n.id,
              n.title.empty() ? "(无)" : n.title.c_str(), n.rect[0], n.rect[1], n.rect[2], n.rect[3], n.rect[2] - n.rect[0], n.rect[3] - n.rect[1],
              gi == fr->gdi.end() ? 0 : gi->second.items.size(), sx, sy);
          break;
        }
    }
  }
  if (st.interact && focused) {
    for (int k = ImGuiKey_NamedKey_BEGIN; k < ImGuiKey_NamedKey_END; ++k) {
      const unsigned vk = vk_of(static_cast<ImGuiKey>(k));
      if (vk && ImGui::IsKeyPressed(static_cast<ImGuiKey>(k)))
        im.push_input({InputEv::Key, nullptr, 0, 0, 0, 0, vk});
    }
    for (const ImWchar cp : io.InputQueueCharacters)
      if (cp >= 0x20)
        im.push_input({InputEv::Char, nullptr, 0, 0, 0, 0, static_cast<unsigned>(cp)});
  }
  ImGui::EndChild();

  if (!st.inspector)
    return;

  // ---------- 右区: 窗口树 + GDI 绘制层图元检查器 ----------
  ImGui::SameLine();
  ImGui::BeginChild("##client_inspect", ImVec2(0, 0), ImGuiChildFlags_Borders);
  ImGui::SetNextItemWidth(-ImGui::GetFontSize() * 5);
  ImGui::InputTextWithHint("##flt", "过滤 类/标题/hwnd", st.filter, sizeof(st.filter));
  ImGui::SameLine();
  ImGui::Checkbox("仅可见", &st.only_vis);

  const float h_half = ImGui::GetContentRegionAvail().y * 0.48f;
  ImGui::BeginChild("##win_tree", ImVec2(0, h_half), ImGuiChildFlags_Borders);
  for (const size_t r : tv.roots)
    draw_tree_node(*fr, tv, r, st, st.filter);
  ImGui::EndChild();

  st.sel_item = -1;
  ImGui::BeginChild("##gdi_items", ImVec2(0, 0), ImGuiChildFlags_Borders);
  const WinNode *sel = nullptr;
  for (const WinNode &n : fr->nodes)
    if (n.hwnd == st.sel_hwnd) {
      sel = &n;
      break;
    }
  if (!sel) {
    ImGui::TextDisabled("点击画布或窗口树选一个窗口查看其 GDI 绘制层");
  } else {
    ImGui::Text("%s  %s#%ld  %s", sel->hwnd.c_str(), sel->cls.c_str(), sel->id, sel->title.c_str());
    ImGui::TextDisabled("rect [%d,%d,%d,%d] (%dx%d) · %s · %s", sel->rect[0], sel->rect[1], sel->rect[2], sel->rect[3],
                        sel->rect[2] - sel->rect[0], sel->rect[3] - sel->rect[1], sel->vis ? "vis" : "hid", sel->en ? "en" : "dis");
    const auto gi = fr->gdi.find(sel->hwnd);
    if (gi == fr->gdi.end() || gi->second.items.empty()) {
      ImGui::TextDisabled("该窗口无 GDI 绘制层图元");
    } else {
      const DrawLayer &l = gi->second;
      ImGui::TextDisabled("GDI 绘制层: off [%d,%d] · %zu 项", l.off[0], l.off[1], l.items.size());
      if (ImGui::BeginTable("##gdi_tbl", 4, ImGuiTableFlags_BordersInnerV | ImGuiTableFlags_RowBg | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_ScrollY)) {
        ImGui::TableSetupColumn("#");
        ImGui::TableSetupColumn("色");
        ImGui::TableSetupColumn("区域");
        ImGui::TableSetupColumn("文字", ImGuiTableColumnFlags_WidthStretch);
        ImGui::TableHeadersRow();
        for (size_t i = 0; i < l.items.size(); ++i) {
          const DrawItem &it = l.items[i];
          ImGui::TableNextRow();
          ImGui::TableSetColumnIndex(0);
          ImGui::Selectable(strf("%zu##gi%zu", i, i).c_str(), false, ImGuiSelectableFlags_SpanAllColumns);
          if (ImGui::IsItemHovered())
            st.sel_item = static_cast<int>(i);
          ImGui::TableSetColumnIndex(1);
          const unsigned r = it.v & 0xFFu, g = (it.v >> 8) & 0xFFu, b = (it.v >> 16) & 0xFFu;
          ImGui::ColorButton(strf("##c%zu", i).c_str(), ImVec4(r / 255.f, g / 255.f, b / 255.f, 1.f),
                             ImGuiColorEditFlags_NoTooltip | ImGuiColorEditFlags_NoPicker, ImVec2(12, 12));
          ImGui::TableSetColumnIndex(2);
          ImGui::TextDisabled("%d,%d-%d,%d", it.l, it.t, it.r, it.b);
          ImGui::TableSetColumnIndex(3);
          if (it.text)
            ImGui::TextUnformatted(it.text->c_str());
          else
            ImGui::TextDisabled("(色块)");
        }
        ImGui::EndTable();
      }
    }
  }
  ImGui::EndChild();
  ImGui::EndChild();
}

} // namespace GUI::Live
