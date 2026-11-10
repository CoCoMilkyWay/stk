// 列表: 窗口树逐行 (先序, 缩进 = depth), 绘制元素排在所画窗口之后、子窗口之前, 随它折叠; 有过滤词时不折叠 (要能搜到深处).
//   与布局图同一悬停 (金框) / 选中 (蓝底); 点 ▸/▾ 折叠, 点行选中, 双击选中并闪烁
#include "live/tdx/console/Model.hpp"

#include <algorithm>

namespace live::tdx::console {

namespace {

constexpr ImU32 kAcc = IM_COL32(0x4e, 0xc9, 0xb0, 255), kHot = IM_COL32(0xd7, 0xba, 0x7d, 255), kMut = IM_COL32(0x88, 0x88, 0x88, 255),
                kSel = IM_COL32(0x09, 0x47, 0x71, 255), kHover = IM_COL32(0x2d, 0x2d, 0x2d, 255), kHl = IM_COL32(0x3a, 0x33, 0x20, 255);

struct Row {
  Pick n;
  bool kids;
};

std::vector<Row> rows_of(const View &v) {
  const Tree &t = *v.tree;
  const Filter f = filter_of(v);
  std::vector<Row> out;
  std::optional<int> hide_depth; // 折叠时跳过比它更深的
  for (size_t i = 0; i < t.nodes.size(); ++i) {
    const win::Node &n = t.nodes[i];
    const bool kids = i + 1 < t.nodes.size() && t.nodes[i + 1].depth > n.depth;
    const bool folded = v.collapsed.contains(n.hwnd);
    if (f.text.empty()) {
      if (hide_depth) {
        if (n.depth > *hide_depth)
          continue;
        hide_depth.reset();
      }
      if (folded && kids)
        hide_depth = n.depth;
    }
    std::vector<Pick> els = elems_of(v, n);
    if (f.keep(n))
      out.push_back({Pick{n, ""}, kids || !els.empty()});
    if (!f.text.empty() || !folded)
      for (Pick &e : els)
        if (f.match(e))
          out.push_back({std::move(e), false});
  }
  return out;
}

ImU32 fade(ImU32 c, float a) { return (c & ~IM_COL32_A_MASK) | (static_cast<ImU32>(((c >> IM_COL32_A_SHIFT) & 255) * a) << IM_COL32_A_SHIFT); }

} // namespace

void draw_list(Core &c, View &v) {
  ImGui::TextUnformatted("窗口树列表（点 ▸/▾ 折叠）");
  ImGui::BeginChild("##tree", ImVec2(0, 0), ImGuiChildFlags_None, ImGuiWindowFlags_HorizontalScrollbar);
  const std::vector<Row> rows = rows_of(v);
  const float row_h = ImGui::GetTextLineHeightWithSpacing(), fs = ImGui::GetFontSize();
  const ImGuiStyle &st = ImGui::GetStyle();

  if (v.hover_map && !v.hover.empty()) { // 悬停来自布局图: 滚到该行 (就近)
    const auto it = std::find_if(rows.begin(), rows.end(), [&](const Row &r) { return r.n.hwnd == v.hover; });
    if (it != rows.end()) {
      const float y = ImGui::GetCursorStartPos().y + static_cast<float>(it - rows.begin()) * row_h, h = ImGui::GetWindowHeight();
      if (y < ImGui::GetScrollY())
        ImGui::SetScrollY(y - st.WindowPadding.y);
      else if (y + row_h > ImGui::GetScrollY() + h)
        ImGui::SetScrollY(y + row_h - h + st.WindowPadding.y);
    }
  }

  ImGuiListClipper clip;
  clip.Begin(static_cast<int>(rows.size()), row_h);
  while (clip.Step())
    for (int i = clip.DisplayStart; i < clip.DisplayEnd; ++i) {
      const Row &r = rows[static_cast<size_t>(i)];
      const Pick &n = r.n;
      ImGui::PushID(i);
      const ImVec2 p = ImGui::GetCursorScreenPos();
      ImGui::PushStyleColor(ImGuiCol_Header, IM_COL32(0, 0, 0, 0));
      ImGui::PushStyleColor(ImGuiCol_HeaderHovered, IM_COL32(0, 0, 0, 0));
      ImGui::PushStyleColor(ImGuiCol_HeaderActive, IM_COL32(0, 0, 0, 0));
      ImGui::Selectable("##row", false, ImGuiSelectableFlags_AllowDoubleClick, ImVec2(0, ImGui::GetTextLineHeight()));
      ImGui::PopStyleColor(3);
      const ImVec2 a = ImGui::GetItemRectMin(), b = ImGui::GetItemRectMax();
      const float fold_x = p.x + 4 + static_cast<float>(n.depth) * 14;
      const bool on_fold = ImGui::GetIO().MousePos.x >= fold_x && ImGui::GetIO().MousePos.x < fold_x + 12;
      const bool hovered = ImGui::IsItemHovered();
      if (hovered) {
        v.hover_next = n.hwnd;
        ImGui::SetTooltip("%s | %s", n.cls.c_str(), n.title.c_str());
      }
      if (ImGui::IsItemClicked()) {
        if (!on_fold)
          select(v, n);
        else if (r.kids && !v.collapsed.erase(n.hwnd))
          v.collapsed.insert(n.hwnd);
      }
      if (hovered && !on_fold && ImGui::IsMouseDoubleClicked(ImGuiMouseButton_Left)) {
        select(v, n);
        act_sel(c, v, "highlight");
      }

      ImDrawList *dl = ImGui::GetWindowDrawList();
      if (v.hover == n.hwnd) {
        dl->AddRectFilled(a, b, kHl, 3);
        dl->AddRect(a, b, kHot, 3);
      } else if (v.sel && v.sel->hwnd == n.hwnd)
        dl->AddRectFilled(a, b, kSel, 3);
      else if (hovered)
        dl->AddRectFilled(a, b, kHover, 3);

      const float al = n.vis ? 1 : 0.45f;
      float x = fold_x;
      const auto put = [&](const std::string &s, ImU32 col) {
        dl->AddText(ImVec2(x, a.y), fade(col, al), s.c_str());
        x += ImGui::CalcTextSize(s.c_str()).x;
      };
      const ImU32 fg = ImGui::GetColorU32(ImGuiCol_Text);
      const std::string rect = strf("[%d,%d,%d,%d]", n.rect[0], n.rect[1], n.rect[2], n.rect[3]);
      const std::string ttl = n.title.empty() ? "" : " \"" + cut(win::squash(n.title), 60) + "\"";
      if (r.kids)
        dl->AddText(ImVec2(fold_x + (12 - ImGui::CalcTextSize(v.collapsed.contains(n.hwnd) ? "▸" : "▾").x) / 2, a.y), fade(kMut, al),
                    v.collapsed.contains(n.hwnd) ? "▸" : "▾");
      x += 12;
      if (n.host.empty()) {
        put(strf("%s id=%ld", n.hwnd.c_str(), n.id), kMut);
        put(" ", fg);
      }
      put(n.cls, kAcc);
      put(ttl, kHot);
      put(" " + rect + (n.host.empty() ? strf(" v%de%d", n.vis ? 1 : 0, n.en ? 1 : 0) : ""), kMut);
      ImGui::SameLine(0, 0);
      ImGui::Dummy(ImVec2(std::max(0.f, x - b.x + fs), 0)); // 横向滚动范围跟着文字宽
      ImGui::PopID();
    }
  ImGui::EndChild();
}

} // namespace live::tdx::console
