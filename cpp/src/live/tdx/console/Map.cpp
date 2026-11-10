// 布局图: 按屏幕坐标、按 Win32 z 序还原窗口树 (画家算法: 先画父, 子窗口裁在父矩形内, 兄弟从下层往上层画; 过滤不中的只调暗自身)
//   标准控件按类 (kClasses) 画底板皮肤与装饰 (detail 子项: 列表行 / 表头 / 树项 / 菜单项 / 勾选态); 表外的类即自绘窗口,
//   皮肤 web / self (叶子打斜纹) / frame, 有绘制层即按其色块与文字画 (字框宽是客户端按原字体量的: 文字横向压到同宽, 位置才对得上),
//   其文字段即绘制元素 (可悬停 / 选中 / 点击)
#include "live/tdx/console/Model.hpp"

#include <algorithm>
#include <cfloat>
#include <cmath>

namespace live::tdx::console {

namespace {

constexpr unsigned kNone = ~0u;
constexpr unsigned kBg = 0x141414, kFg = 0xdddddd, kGray = 0x777777, kSelfLbl = 0x666666, kSelBar = 0x094771, kSep = 0x555555, kHatchBg = 0x1c1c1c,
                   kHatch = 0x2a2a2a, kHot = 0xd7ba7d, kAcc = 0x4ec9b0;

ImU32 rgb(unsigned c, float a = 1) { return IM_COL32((c >> 16) & 255, (c >> 8) & 255, c & 255, static_cast<int>(a * 255)); }
ImU32 bgr(unsigned c, float a) { return IM_COL32(c & 255, (c >> 8) & 255, (c >> 16) & 255, static_cast<int>(a * 255)); } // COLORREF 0x00BBGGRR

struct Look {
  unsigned fill, stroke;
};
// 窗口底板皮肤
enum Skin : uint8_t { kFrame,
                      kBtn,
                      kBare,
                      kGroup,
                      kField,
                      kHdr,
                      kMenu,
                      kSelf,
                      kWeb };
constexpr Look kSkins[] = {{0x1e1e1e, 0x333333}, {0x3a3a3a, 0x5a5a5a}, {kNone, kNone}, {kNone, 0x555555}, {0x111111, 0x444444}, {0x2d2d2d, 0x4a4a4a}, {0x2b2b2b, 0x777777}, {kHatchBg, 0x3a3a3a}, {0x18222c, 0x2d4256}};
constexpr Look kBtnFace = kSkins[kBtn], kHdrItem = kSkins[kHdr], kPressed = {0x1f1f1f, 0x222222};

// 标准控件: 类名 → 画法
enum class Kind : uint8_t { Button,
                            Static,
                            Edit,
                            Combo,
                            List,
                            TreeItems,
                            Header,
                            Menu };
struct Cls {
  std::string_view name;
  Kind kind;
};
constexpr Cls kClasses[] = {{"Button", Kind::Button}, {"Static", Kind::Static}, {"Edit", Kind::Edit}, {"ComboBox", Kind::Combo}, {"ListBox", Kind::List}, {"ComboLBox", Kind::List}, {"SysListView32", Kind::List}, {"SysTreeView32", Kind::TreeItems}, {"SysHeader32", Kind::Header}, {"#32768", Kind::Menu}};

std::optional<Kind> kind_of(const std::string &cls) {
  for (const Cls &c : kClasses)
    if (c.name == cls)
      return c.kind;
  return std::nullopt;
}

int btn_type(const win::Node &n) { return static_cast<int>(n.style & 0xf); } // BS_TYPEMASK
bool check_like(const win::Node &n) {                                        // 勾选 / 单选类, 非 BS_PUSHLIKE
  const int t = btn_type(n);
  return (t == 2 || t == 3 || t == 4 || t == 5 || t == 6 || t == 9) && !(n.style & 0x1000);
}
bool is_web(const win::Node &n) { return n.cls.starts_with("Chrome_") || n.cls.starts_with("Cef"); }
bool on_map(const win::Node &n) { return n.vis && n.rect[2] > n.rect[0] && n.rect[3] > n.rect[1]; } // IsWindowVisible 已含祖先

Skin skin_of(const win::Node &n, bool leaf) {
  if (const std::optional<Kind> k = kind_of(n.cls))
    switch (*k) {
    case Kind::Button:
      return btn_type(n) == 7 ? kGroup : check_like(n) ? kBare
                                                       : kBtn;
    case Kind::Static:
      return kBare;
    case Kind::Header:
      return kHdr;
    case Kind::Menu:
      return kMenu;
    default:
      return kField;
    }
  return is_web(n) ? kWeb : leaf ? kSelf
                                 : kFrame;
}

std::string mnem(const std::string &s) { // 去助记符 &X / &&
  std::string out;
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '&' && i + 1 < s.size())
      ++i;
    out += s[i];
  }
  return out;
}

size_t cps(const std::string &s) { // UTF-8 码点数
  return static_cast<size_t>(std::count_if(s.begin(), s.end(), [](char ch) { return (static_cast<unsigned char>(ch) & 0xC0) != 0x80; }));
}

struct R {
  float l, t, r, b;
};
R rect_of(const int (&r)[4]) { return {static_cast<float>(r[0]), static_cast<float>(r[1]), static_cast<float>(r[2]), static_cast<float>(r[3])}; }
R shift(const win::Item &it, int ox, int oy) {
  return {static_cast<float>(it.l + ox), static_cast<float>(it.t + oy), static_cast<float>(it.r + ox), static_cast<float>(it.b + oy)};
}
ImVec4 isect(const ImVec4 &a, const ImVec4 &b) { return {std::max(a.x, b.x), std::max(a.y, b.y), std::min(a.z, b.z), std::min(a.w, b.w)}; }

enum Align { kStart,
             kMiddle,
             kEnd };

// 世界坐标 (= 屏幕坐标) → 画布; a = 当前节点不透明度
struct Painter {
  ImDrawList *dl;
  ImVec2 o; // 世界 (x0, y0) 在画布上的位置
  float s, x0, y0;
  ImFont *font;
  float a = 1;

  ImVec2 at(float x, float y) const { return {o.x + (x - x0) * s, o.y + (y - y0) * s}; }
  ImVec4 screen(const R &r) const {
    const ImVec2 p = at(r.l, r.t), q = at(r.r, r.b);
    return {p.x, p.y, q.x, q.y};
  }
  ImVec4 clip() const {
    const ImVec2 p = dl->GetClipRectMin(), q = dl->GetClipRectMax();
    return {p.x, p.y, q.x, q.y};
  }
  void box(const R &r, const Look &k) const {
    if (r.r <= r.l || r.b <= r.t)
      return;
    if (k.fill != kNone)
      dl->AddRectFilled(at(r.l, r.t), at(r.r, r.b), rgb(k.fill, a));
    if (k.stroke != kNone)
      dl->AddRect(at(r.l, r.t), at(r.r, r.b), rgb(k.stroke, a));
  }
  void fill(const R &r, ImU32 col) const {
    if (r.r > r.l && r.b > r.t)
      dl->AddRectFilled(at(r.l, r.t), at(r.r, r.b), col);
  }
  // 斜纹: 世界坐标里 x + y = 6√2·k 的 45° 线
  void hatch(const R &r) const {
    const float d = 6 * std::sqrt(2.f);
    dl->PushClipRect(at(r.l, r.t), at(r.r, r.b), true);
    for (float k = std::ceil((r.l + r.t) / d) * d; k <= r.r + r.b; k += d)
      dl->AddLine(at(k - r.t, r.t), at(k - r.b, r.b), rgb(kHatch, a), std::max(1.f, s));
    dl->PopClipRect();
  }
  void line(float x1, float y1, float x2, float y2, unsigned col) const { dl->AddLine(at(x1, y1), at(x2, y2), rgb(col, a)); }
  // 矩形内单行文字, 超出裁掉
  void text_in(const R &r, const std::string &str, Align al = kStart, unsigned col = kFg, float size = 12) const {
    const float fs = size * s;
    if (str.empty() || r.r <= r.l || r.b <= r.t || fs < 2)
      return;
    const float w = font->CalcTextSizeA(fs, FLT_MAX, 0, str.c_str()).x;
    const ImVec4 c = screen(r);
    const float x = al == kMiddle ? (c.x + c.z - w) / 2 : al == kEnd ? at(r.r - 3, 0).x - w
                                                                     : at(r.l + 3, 0).x;
    dl->AddText(font, fs, ImVec2(x, (c.y + c.w - fs) / 2), rgb(col, a), str.c_str(), nullptr, 0, &c);
  }
  // 绘制层文字: 字号取框高 0.8 (至少 6), 横向压到框宽
  void text_fit(const R &r, const std::string &str, ImU32 col) const {
    const float fs = std::max(6.f, (r.b - r.t) * 0.8f) * s;
    if (str.empty() || fs < 2)
      return;
    const float w = font->CalcTextSizeA(fs, FLT_MAX, 0, str.c_str()).x;
    if (w <= 0)
      return;
    static const ImVec4 kAll(-FLT_MAX, -FLT_MAX, FLT_MAX, FLT_MAX); // 先不裁 (压宽后再由裁剪框裁)
    const ImVec2 p = at(r.l, (r.t + r.b) / 2);
    const int v0 = dl->VtxBuffer.Size;
    dl->AddText(font, fs, ImVec2(p.x, p.y - fs / 2), col, str.c_str(), nullptr, 0, &kAll);
    const float k = std::max(1.f, r.r - r.l) * s / w;
    for (int i = v0; i < dl->VtxBuffer.Size; ++i)
      dl->VtxBuffer[i].pos.x = p.x + (dl->VtxBuffer[i].pos.x - p.x) * k;
  }
};

// 命中框 (画布坐标, 已裁), 按画序; 后画的在上
struct Hit {
  ImVec4 box;
  std::string id;
};

struct Ctx {
  const View &v;
  Painter p;
  Filter f;
  std::vector<Hit> hits;
};

// 子项已换到屏幕坐标: items 的 [l,t,r,b] + (ox, oy)
struct Sub {
  const std::vector<win::Item> &items;
  int ox, oy;
  R at(const win::Item &it) const { return shift(it, ox, oy); }
};

void draw_button(const Painter &p, const win::Node &n, const Sub &d) {
  const unsigned st = d.items.empty() ? 0 : d.items[0].v;
  const std::string text = mnem(win::squash(n.title));
  const unsigned col = n.en ? kFg : kGray;
  const R box = rect_of(n.rect);
  if (btn_type(n) == 7) // 分组框
    return p.text_in({box.l + 6, box.t, box.r, box.t + 14}, text, kStart, col);
  if (check_like(n)) {
    const bool radio = btn_type(n) == 4 || btn_type(n) == 9, on = st & kStCheck;
    return p.text_in(box, std::string(radio ? (on ? "◉" : "○") : on ? "☑"
                                                                    : "☐") +
                              " " + text,
                     kStart, col);
  }
  if (st & (kStSel | kStCheck))
    p.box(box, kPressed);
  p.text_in(box, text, kMiddle, col);
}

void draw_combo(const Painter &p, const win::Node &n) {
  const R box = rect_of(n.rect);
  const float a = std::min(box.b - box.t, 18.f);
  p.box({box.r - a, box.t, box.r, box.b}, kBtnFace);
  p.text_in({box.r - a, box.t, box.r, box.b}, "▾", kMiddle);
  p.text_in({box.l, box.t, box.r - a, box.b}, win::squash(n.title), kStart, n.en ? kFg : kGray);
}

// ListBox / ComboLBox / SysListView32: 行 (kStRow) 先铺选中底色, 其后各格文字
void draw_rows(const Painter &p, const Sub &d) {
  for (const win::Item &it : d.items) {
    const R r = d.at(it);
    if (it.v & kStSel)
      p.fill(r, rgb(kSelBar, p.a));
    if (!(it.v & kStRow))
      p.text_in(r, it.text.value_or(""), kStart, it.v & kStGray ? kGray : kFg);
  }
}

void draw_header(const Painter &p, const Sub &d) {
  for (const win::Item &it : d.items) {
    p.box(d.at(it), kHdrItem);
    p.text_in(d.at(it), it.text.value_or(""));
  }
}

void draw_tree_items(const Painter &p, const win::Node &n, const Sub &d) {
  for (const win::Item &it : d.items) {
    const R r = d.at(it);
    if (it.v & kStSel)
      p.fill(r, rgb(kSelBar, p.a));
    if (it.v & kStKids)
      p.text_in({r.l - 14, r.t, r.l, r.b}, it.v & kStOpen ? "▾" : "▸", kMiddle, kGray);
    p.text_in({r.l, r.t, static_cast<float>(n.rect[2]), r.b}, it.text.value_or("")); // 文字矩形按原字体量的, 放宽到右边界免得被裁
  }
}

void draw_menu(const Painter &p, const Sub &d) {
  for (const win::Item &it : d.items) {
    const R r = d.at(it);
    if (it.v & kStSep) {
      p.line(r.l + 2, (r.t + r.b) / 2, r.r - 2, (r.t + r.b) / 2, kSep);
      continue;
    }
    if (it.v & kStSel)
      p.fill(r, rgb(kSelBar, p.a));
    const unsigned col = it.v & kStGray ? kGray : kFg;
    const std::string s = mnem(it.text.value_or(""));
    const size_t tab = s.find('\t');
    if (it.v & kStCheck)
      p.text_in({r.l, r.t, r.l + 18, r.b}, "✓", kMiddle, col);
    p.text_in({r.l + 18, r.t, r.r - 14, r.b}, s.substr(0, tab), kStart, col);
    if (tab != std::string::npos)
      p.text_in({r.l, r.t, r.r - 14, r.b}, s.substr(tab + 1, s.find('\t', tab + 1) - tab - 1), kEnd, col);
    if (it.v & kStSub)
      p.text_in({r.r - 14, r.t, r.r, r.b}, "▸", kMiddle, col);
  }
}

void draw_other(const Painter &p, const win::Node &n, bool leaf) {
  const R box = rect_of(n.rect);
  if (!leaf || box.r - box.l < 30 || box.b - box.t < 10)
    return;
  if (is_web(n))
    return p.text_in(box, "网页 (CEF)", kMiddle, kGray);
  const std::string t = win::squash(n.title);
  p.text_in({box.l, box.t, box.r, std::min(box.b, box.t + 12)}, t.empty() ? n.cls : t, kStart, kSelfLbl, 9);
}

void draw_static(const Painter &p, const win::Node &n) {
  const unsigned a = n.style & 0x1f;
  p.text_in(rect_of(n.rect), mnem(win::squash(n.title)), a == 1 ? kMiddle : a == 2 ? kEnd
                                                                                  : kStart,
            n.en ? kFg : kGray);
}

void draw_edit(const Painter &p, const win::Node &n) {
  std::string s;
  if (n.style & 0x20) // ES_PASSWORD
    for (size_t i = cps(n.title); i > 0; --i)
      s += "●";
  else
    s = win::squash(n.title);
  p.text_in(rect_of(n.rect), s);
}

// 一个窗口: 底板 (接命中) + 按类装饰 (有绘制层则由它代替); 绘制元素的命中框盖在其画面上
void draw_node(Ctx &m, const win::Node &n, bool leaf) {
  const Painter &p = m.p;
  const R box = rect_of(n.rect);
  const Skin sk = skin_of(n, leaf);
  p.box(box, kSkins[sk]);
  if (sk == kSelf) {
    p.hatch(box);
    p.box(box, {kNone, kSkins[kSelf].stroke});
  }
  const ImVec4 clip = isect(p.screen(box), p.clip());
  m.hits.push_back({clip, n.hwnd});
  const win::Layer *g = gdi_of(m.v, n);
  if (!g) {
    static const std::vector<win::Item> kNoItems;
    const auto d = m.v.detail->find(n.hwnd);
    const Sub sub = d == m.v.detail->end() ? Sub{kNoItems, 0, 0} : Sub{d->second.items, n.rect[0] + d->second.off[0], n.rect[1] + d->second.off[1]};
    const std::optional<Kind> k = kind_of(n.cls);
    if (!k)
      return draw_other(p, n, leaf);
    switch (*k) {
    case Kind::Button:
      return draw_button(p, n, sub);
    case Kind::Static:
      return draw_static(p, n);
    case Kind::Edit:
      return draw_edit(p, n);
    case Kind::Combo:
      return draw_combo(p, n);
    case Kind::List:
      return draw_rows(p, sub);
    case Kind::TreeItems:
      return draw_tree_items(p, n, sub);
    case Kind::Header:
      return draw_header(p, sub);
    case Kind::Menu:
      return draw_menu(p, sub);
    }
    return;
  }
  const int ox = n.rect[0] + g->off[0], oy = n.rect[1] + g->off[1];
  p.dl->PushClipRect(p.at(box.l, box.t), p.at(box.r, box.b), true);
  for (const win::Item &it : g->items) {
    const R r = shift(it, ox, oy);
    if (!it.text)
      p.fill(r, bgr(it.v, p.a));
    else
      p.text_fit(r, *it.text, bgr(it.v, p.a));
  }
  p.dl->PopClipRect();
  if (n.vis && !std_class(n.cls))
    for (const win::Item &it : g->items)
      if (it.text && !trim(*it.text).empty())
        m.hits.push_back({isect(p.screen(shift(it, ox, oy)), clip), strf("%s#%d,%d", n.hwnd.c_str(), it.l, it.t)});
}

void draw_tree(Ctx &m, const Tree &t, size_t i) {
  const win::Node &n = t.nodes[i];
  if (!on_map(n))
    return;
  std::vector<size_t> sub;
  for (const size_t k : t.kids[i])
    if (on_map(t.nodes[k]))
      sub.push_back(k);
  m.p.a = m.f.match(n) ? 1 : 0.3f;
  draw_node(m, n, sub.empty());
  m.p.a = 1;
  if (sub.empty())
    return;
  m.p.dl->PushClipRect(m.p.at(n.rect[0], n.rect[1]), m.p.at(n.rect[2], n.rect[3]), true);
  for (auto k = sub.rbegin(); k != sub.rend(); ++k)
    draw_tree(m, t, *k);
  m.p.dl->PopClipRect();
}

std::string hit_at(const std::vector<Hit> &hits, ImVec2 q) {
  for (auto h = hits.rbegin(); h != hits.rend(); ++h)
    if (q.x >= h->box.x && q.x < h->box.z && q.y >= h->box.y && q.y < h->box.w)
      return h->id;
  return {};
}

// 浮框 (悬停 / 选中): 按对象矩形摆在最上层, 不被子窗口遮住
void mark(const Painter &p, const View &v, const std::string &id, bool hl) {
  const std::optional<Pick> n = pick_of(v, id);
  if (!n)
    return;
  const ImVec2 a = p.at(n->rect[0], n->rect[1]), b = p.at(n->rect[2], n->rect[3]);
  if (hl)
    p.dl->AddRectFilled(a, b, rgb(kHot, 0.15f));
  p.dl->AddRect(a, b, hl ? rgb(kHot) : IM_COL32_WHITE, 0, 0, 2);
}

// 非可打印键 → 虚拟键码; 可打印字符走 char (shim 内按 VkKeyScan 决定投键还是 WM_CHAR)
struct Vk {
  ImGuiKey key;
  int vk;
};
constexpr Vk kVks[] = {{ImGuiKey_Backspace, 8}, {ImGuiKey_Tab, 9}, {ImGuiKey_Enter, 13}, {ImGuiKey_KeypadEnter, 13}, {ImGuiKey_Escape, 27}, {ImGuiKey_PageUp, 33}, {ImGuiKey_PageDown, 34}, {ImGuiKey_End, 35}, {ImGuiKey_Home, 36}, {ImGuiKey_LeftArrow, 37}, {ImGuiKey_UpArrow, 38}, {ImGuiKey_RightArrow, 39}, {ImGuiKey_DownArrow, 40}, {ImGuiKey_Insert, 45}, {ImGuiKey_Delete, 46}, {ImGuiKey_F1, 112}, {ImGuiKey_F2, 113}, {ImGuiKey_F3, 114}, {ImGuiKey_F4, 115}, {ImGuiKey_F5, 116}, {ImGuiKey_F6, 117}, {ImGuiKey_F7, 118}, {ImGuiKey_F8, 119}, {ImGuiKey_F9, 120}, {ImGuiKey_F10, 121}, {ImGuiKey_F11, 122}, {ImGuiKey_F12, 123}};

// 画布: 画、命中、交互. 交互开: 左 / 右键按下抬起分开投 (可拖拽, 第二下按下投双击), 滚轮投 WM_MOUSEWHEEL (±120 / 格),
// 键盘投给焦点窗口; 交互关: 左键 = 选中 + 闪烁, 滚轮缩放. Ctrl+滚轮恒缩放 (围绕光标), 中键拖拽平移、单击 (未拖动) 选中
void canvas(Core &c, View &v, const Tree &t, const std::vector<size_t> &tops, const R &world) {
  const ImGuiIO &io = ImGui::GetIO();
  const float s = *v.scale;
  if (v.reset) {
    ImGui::SetScrollX(0);
    ImGui::SetScrollY(0);
    v.reset = false;
  }
  const ImVec2 o = ImGui::GetCursorScreenPos();
  const ImVec2 size((world.r - world.l) * s, (world.b - world.t) * s);
  ImGui::Dummy(size);
  Ctx m{v, Painter{ImGui::GetWindowDrawList(), o, s, world.l, world.t, ImGui::GetFont()}, filter_of(v), {}};
  for (auto i = tops.rbegin(); i != tops.rend(); ++i)
    draw_tree(m, t, *i);
  if (v.sel)
    mark(m.p, v, v.sel->hwnd, false);
  if (!v.hover.empty())
    mark(m.p, v, v.hover, true);
  if (ImGui::IsWindowFocused()) {
    const ImVec2 wp = ImGui::GetWindowPos(), ws = ImGui::GetWindowSize();
    ImGui::GetForegroundDrawList()->AddRect(wp, ImVec2(wp.x + ws.x, wp.y + ws.y), rgb(kAcc));
  }

  const ImVec2 q = io.MousePos;
  const bool inside = ImGui::IsWindowHovered() && q.x >= o.x && q.y >= o.y && q.x < o.x + size.x && q.y < o.y + size.y;
  const std::string hit = inside ? hit_at(m.hits, q) : std::string();
  const std::optional<Pick> hp = pick_of(v, hit);
  if (!hit.empty()) {
    v.hover_next = hit;
    v.hover_map_next = true;
    if (hp && hp->host.empty() && !v.held && !v.pan)
      ImGui::SetTooltip("%s id=%ld %s \"%s\" [%d,%d,%d,%d]", hp->hwnd.c_str(), hp->id, hp->cls.c_str(), win::squash(hp->title).c_str(), hp->rect[0], hp->rect[1],
                        hp->rect[2], hp->rect[3]);
  }
  const int wx = static_cast<int>(std::lround(world.l + (q.x - o.x) / s)), wy = static_cast<int>(std::lround(world.t + (q.y - o.y) / s));
  // Win32 MK_*: LBUTTON 1 / RBUTTON 2 / SHIFT 4 / CONTROL 8 / MBUTTON 0x10
  const unsigned mk = (io.MouseDown[0] ? 1 : 0) | (io.MouseDown[1] ? 2 : 0) | (io.KeyShift ? 4 : 0) | (io.KeyCtrl ? 8 : 0) | (io.MouseDown[2] ? 0x10 : 0);
  const auto mouse = [&](const std::string &kind, int drop = 0, const std::string &extra = "") {
    input(c, strf("mouse %s %d %d %u%s", kind.c_str(), wx, wy, mk, extra.c_str()), drop);
  };
  for (const int b : {0, 1}) {
    const int bit = b == 0 ? 1 : 2;
    const std::string lr = b == 0 ? "l" : "r";
    if (inside && ImGui::IsMouseClicked(b)) {
      if (!v.interact) {
        if (hp && bit == 1) {
          select(v, *hp);
          act_sel(c, v, "highlight");
        }
      } else {
        v.held |= bit;
        mouse(lr + (ImGui::IsMouseDoubleClicked(b) ? "dbl" : "down"));
        if (hp && bit == 1)
          select(v, *hp);
      }
    }
    if ((v.held & bit) && ImGui::IsMouseReleased(b)) {
      v.held &= ~bit;
      mouse(lr + "up");
    }
  }
  if (v.held && (io.MouseDelta.x != 0 || io.MouseDelta.y != 0))
    mouse("move", 1);

  if (inside && io.MouseWheel != 0) {
    if (v.interact && !io.KeyCtrl)
      mouse("wheel", 2, io.MouseWheel > 0 ? " 120" : " -120");
    else {
      const float ns = std::clamp(s * (io.MouseWheel > 0 ? 1.15f : 1 / 1.15f), 0.15f, 20.f);
      const float k = ns / s - 1;
      ImGui::SetScrollX(ImGui::GetScrollX() + (q.x - o.x) * k);
      ImGui::SetScrollY(ImGui::GetScrollY() + (q.y - o.y) * k);
      v.scale = ns;
    }
  }

  if (inside && ImGui::IsMouseClicked(ImGuiMouseButton_Middle))
    v.pan = View::Pan{q, ImVec2(ImGui::GetScrollX(), ImGui::GetScrollY())};
  if (v.pan) {
    const ImVec2 d(q.x - v.pan->at.x, q.y - v.pan->at.y);
    v.pan->moved = v.pan->moved || std::fabs(d.x) + std::fabs(d.y) > 3;
    ImGui::SetScrollX(v.pan->scroll.x - d.x);
    ImGui::SetScrollY(v.pan->scroll.y - d.y);
    if (ImGui::IsMouseReleased(ImGuiMouseButton_Middle)) {
      if (!v.pan->moved && hp)
        select(v, *hp);
      v.pan.reset();
    }
  }

  if (v.interact && ImGui::IsWindowFocused() && !io.KeyCtrl && !io.KeyAlt && !io.KeySuper) {
    for (const ImWchar ch : io.InputQueueCharacters)
      if (ch >= 0x20 && ch != 0x7f)
        input(c, strf("char %u", static_cast<unsigned>(ch)));
    for (const Vk &k : kVks)
      if (ImGui::IsKeyPressed(k.key))
        input(c, strf("key %d", k.vk));
  }
}

} // namespace

bool std_class(const std::string &cls) { return kind_of(cls).has_value(); }

bool needs_detail(const win::Node &n) {
  if (!on_map(n))
    return false;
  const std::optional<Kind> k = kind_of(n.cls);
  return k && (*k == Kind::Button ? check_like(n) : *k != Kind::Static && *k != Kind::Edit && *k != Kind::Combo);
}

const win::Layer *gdi_of(const View &v, const win::Node &n) {
  if (!v.gdi_on)
    return nullptr;
  const auto g = v.gdi->find(n.hwnd);
  return g == v.gdi->end() || g->second->layer.items.empty() ? nullptr : &g->second->layer;
}

std::vector<Pick> elems_of(const View &v, const win::Node &n) {
  std::vector<Pick> out;
  const win::Layer *g = n.vis && !std_class(n.cls) ? gdi_of(v, n) : nullptr;
  if (!g)
    return out;
  const int ox = n.rect[0] + g->off[0], oy = n.rect[1] + g->off[1];
  for (const win::Item &it : g->items) {
    const std::string t = it.text ? trim(*it.text) : std::string();
    if (t.empty())
      continue;
    Pick e;
    e.depth = n.depth + 1;
    e.hwnd = strf("%s#%d,%d", n.hwnd.c_str(), it.l, it.t);
    e.host = n.hwnd;
    e.cls = "绘";
    e.title = t;
    e.rect[0] = it.l + ox, e.rect[1] = it.t + oy, e.rect[2] = it.r + ox, e.rect[3] = it.b + oy;
    e.vis = e.en = true;
    out.push_back(std::move(e));
  }
  return out;
}

void draw_map(Core &c, View &v) {
  if (ImGui::Button("复位")) {
    v.scale.reset();
    v.reset = true;
  }
  ImGui::SameLine();
  ImGui::TextDisabled("×%.2f", v.scale.value_or(0));
  ImGui::SameLine();
  ImGui::Checkbox("自动更新", &v.auto_upd);
  ImGui::SameLine();
  ImGui::Checkbox("交互", &v.interact);
  ImGui::SameLine();
  ImGui::Checkbox("绘制层", &v.gdi_on);
  ImGui::SetItemTooltip("按客户端真实绘制的文字 / 色块画 (shim gdi)");
  ImGui::SameLine();
  ImGui::TextDisabled("(点图后可键盘输入 · Ctrl+滚轮缩放)");
  ImGui::SameLine();
  ImGui::TextUnformatted("轮询");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 5);
  if (ImGui::InputInt("fps", &v.fps))
    v.fps = std::clamp(v.fps, 0, 10);
  ImGui::SetNextItemWidth(-ImGui::GetFontSize() * 14);
  input_text("##filter", v.filter, "过滤 class/title/hwnd/id…");
  ImGui::SameLine();
  ImGui::Checkbox("只看可见", &v.only_vis);
  ImGui::SameLine();
  ImGui::Checkbox("隐藏零矩形", &v.hide_zero);

  // 排除跑到屏外的顶层窗口: 最小化时 Win32 把窗口挪到 -32000 一带 (right / bottom 为负)
  const Tree &t = *v.tree;
  std::vector<size_t> vis, tops;
  for (const size_t i : t.roots)
    if (on_map(t.nodes[i])) {
      vis.push_back(i);
      if (t.nodes[i].rect[2] > 0 && t.nodes[i].rect[3] > 0)
        tops.push_back(i);
    }
  R world{FLT_MAX, FLT_MAX, -FLT_MAX, -FLT_MAX};
  for (const size_t i : tops) {
    const R r = rect_of(t.nodes[i].rect);
    world = {std::min(world.l, r.l), std::min(world.t, r.t), std::max(world.r, r.r), std::max(world.b, r.b)};
  }
  const ImVec2 avail = ImGui::GetContentRegionAvail();
  if (!tops.empty() && !v.scale) {
    const ImVec2 pad = ImGui::GetStyle().WindowPadding;
    v.scale = std::min((avail.x - pad.x * 2 - 2) / (world.r - world.l), (avail.y - pad.y * 2 - 2) / (world.b - world.t));
  }
  if (!tops.empty())
    ImGui::SetNextWindowContentSize(ImVec2((world.r - world.l) * *v.scale, (world.b - world.t) * *v.scale));
  ImGui::PushStyleColor(ImGuiCol_ChildBg, rgb(kBg));
  ImGui::BeginChild("##mapwrap", ImVec2(0, 0), ImGuiChildFlags_Borders, ImGuiWindowFlags_HorizontalScrollbar | ImGuiWindowFlags_NoScrollWithMouse);
  ImGui::PopStyleColor();
  if (!t.err.empty())
    ImGui::TextColored(ImGui::ColorConvertU32ToFloat4(rgb(0xf14c4c)), "✗ %s", t.err.c_str());
  else if (tops.empty())
    ImGui::TextUnformatted(vis.empty() ? "无可见控件（切到列表或刷新窗口树）" : "主窗口已最小化 / 不在前台（切回客户端即恢复）");
  else
    canvas(c, v, t, tops, world);
  ImGui::EndChild();
}

} // namespace live::tdx::console
