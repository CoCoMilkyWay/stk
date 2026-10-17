#pragma once

// 表格格内迷你图 (Features 预览 Dist / PSD 列, Factors 因子表 rIC / x 分布列共用)

#include "imgui.h"

#include <algorithm>
#include <cassert>
#include <vector>

inline constexpr float kSparkWidth = 96.0f;                 // 迷你图列宽 (px)
inline const ImVec4 kSparkPosColor{1.0f, 0.3f, 0.3f, 1.0f}; // 分布: x > 0 段 (红)
inline const ImVec4 kSparkNegColor{0.2f, 0.8f, 0.4f, 1.0f}; // 分布: x < 0 段 (绿)

// 迷你折线: 当前光标处画 size 大小的 polyline (x/y 各自按数据范围归一化, 铺满格子),
// 返回是否 hover. neg_color 非空 → 以 x = 0 为界两色 (左 neg_color, 右 color),
// 同一条 polyline 用裁剪矩形分段, 不切数组.
// pt_colors 非空 ([n]) → 逐段渐变色 (段色 = 起点色), 覆盖 color/neg_color.
// x_domain / y_domain 非空 ([2] = {lo, hi}) → 该轴按固定域归一化 (不铺满, 全表同尺可比);
// 点先夹到域内再画 (域外值贴边, 不穿到邻行)
inline bool sparkline(const char *id, const float *xs, const float *ys, int n, ImVec2 size,
                      const ImVec4 &color, const ImVec4 *neg_color = nullptr,
                      const ImU32 *pt_colors = nullptr, const float *x_domain = nullptr,
                      const float *y_domain = nullptr) {
  assert(n >= 2);
  const ImVec2 p0 = ImGui::GetCursorScreenPos();
  ImGui::InvisibleButton(id, size);
  const bool hovered = ImGui::IsItemHovered();

  float ymin = ys[0], ymax = ys[0];
  if (y_domain) {
    ymin = y_domain[0];
    ymax = y_domain[1];
  } else {
    for (int i = 1; i < n; ++i) {
      ymin = std::min(ymin, ys[i]);
      ymax = std::max(ymax, ys[i]);
    }
  }
  const float xlo = x_domain ? x_domain[0] : xs[0];
  const float xr = (x_domain ? x_domain[1] : xs[n - 1]) - xlo;
  const float inv_x = xr > 0.0f ? 1.0f / xr : 0.0f;
  const float yr = ymax - ymin;
  const float inv_y = yr > 0.0f ? 1.0f / yr : 0.0f;

  static std::vector<ImVec2> pts; // GUI 单线程, 帧内复用
  pts.resize(n);
  for (int i = 0; i < n; ++i) {
    const float u = std::clamp((xs[i] - xlo) * inv_x, 0.0f, 1.0f);
    const float v = yr > 0.0f ? std::clamp((ys[i] - ymin) * inv_y, 0.0f, 1.0f) : 0.5f;
    pts[i] = ImVec2(p0.x + u * size.x, p0.y + (1.0f - v) * (size.y - 2.0f) + 1.0f);
  }

  ImDrawList *dl = ImGui::GetWindowDrawList();
  if (pt_colors) {
    for (int i = 0; i + 1 < n; ++i)
      dl->AddLine(pts[i], pts[i + 1], pt_colors[i], 1.0f);
  } else if (!neg_color) {
    dl->AddPolyline(pts.data(), n, ImGui::GetColorU32(color), 0, 1.0f);
  } else {
    const float xz = p0.x + std::clamp((0.0f - xlo) * inv_x, 0.0f, 1.0f) * size.x; // x = 0 的像素位
    dl->PushClipRect(ImVec2(p0.x, p0.y), ImVec2(xz, p0.y + size.y), true);
    dl->AddPolyline(pts.data(), n, ImGui::GetColorU32(*neg_color), 0, 1.0f);
    dl->PopClipRect();
    dl->PushClipRect(ImVec2(xz, p0.y), ImVec2(p0.x + size.x, p0.y + size.y), true);
    dl->AddPolyline(pts.data(), n, ImGui::GetColorU32(color), 0, 1.0f);
    dl->PopClipRect();
  }
  return hovered;
}
