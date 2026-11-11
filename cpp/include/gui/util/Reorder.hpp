#pragma once

// 拖动调序 (各页的子池列表 / 自选同一份): 左键 / 中键在可动项上按住拖过阈值 = 拖起; 落点 = 本列表所在窗内指针最近的可动项,
//   按其上 / 下半落到它前 / 后 (画插入线); 松开 = 移过去, 拖起前松开 = 普通点击
//   用法: 每帧一个 Reorder rd("##id"); 每项画完 (上一个 item = 该项, 项自上而下) 调 rd.item(i, 可动); 末了 rd.end() = 本帧落定的移动
//   项的左键单击用 rd.tapped(i) 判 (松开时: 按下在本项, 没拖起, 按的不是开合箭头), 不用 IsItemClicked (按下即算, 与拖起冲突)
#include "imgui.h"
#include "imgui_internal.h" // IsMouseDragPastThreshold

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <optional>
#include <utility>
#include <vector>

namespace GUI {

// 一次移动: 下标 from 的项移到下标 to (移后的位置)
struct Move {
  int from, to;
};

template <class T>
void apply_move(std::vector<T> &v, const Move &m) {
  assert(m.from >= 0 && m.to >= 0 && static_cast<size_t>(std::max(m.from, m.to)) < v.size());
  T x = std::move(v[static_cast<size_t>(m.from)]);
  v.erase(v.begin() + m.from);
  v.insert(v.begin() + m.to, std::move(x));
}

class Reorder {
public:
  explicit Reorder(const char *id) : id_(ImGui::GetID(id)) {
    Press &p = press();
    if (p.list == id_ && !ImGui::IsMouseDown(p.button) && !ImGui::IsMouseReleased(p.button)) // 松开那帧本列表没画
      p = {};
  }

  void item(int i, bool movable) {
    Press &p = press();
    if (ImGui::IsItemHovered())
      for (const ImGuiMouseButton b : {ImGuiMouseButton_Left, ImGuiMouseButton_Middle})
        if (ImGui::IsMouseClicked(b) && (movable || b == ImGuiMouseButton_Left))
          p = {id_, i, b, movable, b == ImGuiMouseButton_Left && !ImGui::IsItemToggledOpen()};
    if (!movable || !moving() || !ImGui::IsWindowHovered(ImGuiHoveredFlags_AllowWhenBlockedByActiveItem))
      return;
    const ImVec2 a = ImGui::GetItemRectMin(), b = ImGui::GetItemRectMax();
    const float y = ImGui::GetMousePos().y, c = 0.5f * (a.y + b.y);
    if (std::abs(y - c) >= dist_)
      return;
    dist_ = std::abs(y - c), to_ = i, after_ = y > c, y_ = after_ ? b.y : a.y;
    x0_ = ImGui::GetWindowPos().x, x1_ = x0_ + ImGui::GetWindowWidth();
  }

  bool tapped(int i) const {
    const Press &p = press();
    return p.list == id_ && p.from == i && p.tap && ImGui::IsMouseReleased(ImGuiMouseButton_Left) &&
           !ImGui::IsMouseDragPastThreshold(ImGuiMouseButton_Left) && ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenBlockedByActiveItem);
  }

  std::optional<Move> end() {
    Press &p = press();
    if (p.list != id_)
      return std::nullopt;
    const bool drag = moving();
    if (drag) {
      ImGui::SetMouseCursor(ImGuiMouseCursor_ResizeNS);
      if (to_ >= 0)
        ImGui::GetForegroundDrawList()->AddLine(ImVec2(x0_, y_), ImVec2(x1_, y_), ImGui::GetColorU32(ImGuiCol_DragDropTarget), 2.0f);
    }
    if (ImGui::IsMouseDown(p.button))
      return std::nullopt;
    const int from = p.from;
    p = {};
    if (!drag || to_ < 0)
      return std::nullopt;
    const int at = to_ + (after_ ? 1 : 0), to = at > from ? at - 1 : at;
    return to == from ? std::nullopt : std::optional<Move>(Move{from, to});
  }

private:
  // 按下 (全局一份: 同时只有一处在按)
  struct Press {
    ImGuiID list = 0;
    int from = -1;
    ImGuiMouseButton button = ImGuiMouseButton_Left;
    bool movable = false, tap = false;
  };
  static Press &press() {
    static Press p;
    return p;
  }
  bool moving() const {
    const Press &p = press();
    return p.list == id_ && p.movable && ImGui::IsMouseDragPastThreshold(p.button);
  }

  ImGuiID id_;
  int to_ = -1; // 落点项 (−1 = 无)
  bool after_ = false;
  float dist_ = FLT_MAX, y_ = 0.f, x0_ = 0.f, x1_ = 0.f; // 落点中线离指针 / 插入线
};

} // namespace GUI
