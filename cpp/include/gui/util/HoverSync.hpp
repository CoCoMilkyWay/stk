#pragma once

// 图 ↔ 右表的悬停联动 (Beta→Style / Market 行情页同一份); 键 K = 行的身份 (一行可对图上几条线):
//   图上悬停 = 鼠标 x 处纵向最近 (kNearPx 内) 的线 → 右表该行底色亮 + 滚到居中 (悬停起记下原位, 悬停止回原位);
//   右表悬停一行 → 图上该行的线白粗线画在最上
//   每帧: begin → 各图 (先画) 读 hot() 画白粗线, 写 plot → 右表 (后画) 读 plot (follow / 底色), 写 row

#include "imgui.h"
#include "implot.h"

#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>

namespace GUI {

inline constexpr float kNearPx = 8.0f;                   // 图上悬停: 离鼠标纵向这么近的线才算
inline constexpr ImVec4 kHotCol(1.0f, 1.0f, 1.0f, 1.0f); // 悬停的线: 白粗线
inline constexpr float kHotW = 3.0f;

template <class K>
struct HoverSync {
  std::optional<K> plot; // 本帧图上悬停的 (各图画完才定)
  std::optional<K> was;  // 上帧图上悬停的 (先画的图看后画的图上的悬停)
  std::optional<K> row;  // 上帧右表悬停的
  int synced = -1;       // 右表已滚到的表行 (换线 / 行序变才再滚)
  float back_y = -1.0f;  // 图上悬停起时右表的滚动位置 (−1 = 未在悬停)

  // 帧首 (各图之前)
  void begin() {
    was = plot;
    plot.reset();
  }
  // 图上画白粗线的行: 图上悬停 (本帧 / 上帧) 优先, 否则上帧右表悬停的
  const std::optional<K> &hot() const { return plot ? plot : was ? was
                                                                 : row; }
  // 右表内 (BeginTable 之后, 画行之前): k = plot 所在的表行 (−1 = 图上无悬停 / 不在表里) → 本帧要滚到居中的表行 (−1 = 不滚);
  // 图上悬停止 → 回原位
  int follow(int k) {
    if (plot) {
      if (back_y < 0.0f)
        back_y = ImGui::GetScrollY();
      if (k == synced)
        return -1;
      return synced = k;
    }
    if (back_y >= 0.0f) {
      ImGui::SetScrollY(back_y);
      back_y = -1.0f, synced = -1;
    }
    return -1;
  }
};

// 当前表行 (TableNextRow 之后) 底色 = 图上悬停的行
inline void hot_row() { ImGui::TableSetBgColor(ImGuiTableBgTarget_RowBg1, ImGui::GetColorU32(ImGuiCol_HeaderHovered)); }

// 当前图内: n 条线里在鼠标 x 处离鼠标纵向最近 (kNearPx 内) 的下标; y(i, x) = 线 i 在 x 处的值 (不可算 = NaN); 图未悬停 / 无 → −1
template <class Y>
int near_line(size_t n, const Y &y) {
  if (!ImPlot::IsPlotHovered())
    return -1;
  const double mx = ImPlot::GetPlotMousePos().x;
  const float my = ImGui::GetMousePos().y;
  float best = kNearPx;
  int near = -1;
  for (size_t i = 0; i < n; ++i)
    if (const double v = y(i, mx); std::isfinite(v))
      if (const float dy = std::abs(ImPlot::PlotToPixels(mx, v).y - my); dy < best)
        best = dy, near = static_cast<int>(i);
  return near;
}

// 折线 (n 点, x 升序; pt(i) = 第 i 点 ImPlotPoint) 在 x 处的值: 相邻两点线性插值; 首末点之外 → NaN
template <class Pt>
double line_at(size_t n, double x, const Pt &pt) {
  constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
  if (n == 0 || x < pt(0).x || x > pt(n - 1).x)
    return kNaN;
  size_t lo = 0, hi = n - 1; // pt(lo).x ≤ x ≤ pt(hi).x
  while (hi - lo > 1) {
    const size_t m = (lo + hi) / 2;
    (pt(m).x <= x ? lo : hi) = m;
  }
  const ImPlotPoint a = pt(lo), b = pt(hi);
  return b.x > a.x ? a.y + (b.y - a.y) * (x - a.x) / (b.x - a.x) : a.y;
}

} // namespace GUI
