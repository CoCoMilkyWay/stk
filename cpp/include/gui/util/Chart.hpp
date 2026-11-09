#pragma once

// 图表公共件 (ASSET 行情 / Market 净值同一份): 涨跌色, 档位按钮条, 线尾小字, 现价十字

#include "imgui.h"
#include "implot.h"

#include <algorithm>
#include <cfloat>
#include <cmath>
#include <string>
#include <vector>

namespace GUI {

// 涨跌色: 涨红 / 跌绿 / 平灰
inline constexpr ImVec4 kUp(0.945f, 0.298f, 0.298f, 1.f), kDn(0.416f, 0.600f, 0.333f, 1.f), kFlat(0.80f, 0.80f, 0.80f, 1.f);
inline ImVec4 tint(double v, double ref) { return v > ref ? kUp : v < ref ? kDn
                                                                          : kFlat; }

// 一排小按钮单选 (选中的按下色): 点 = cur 换成它; 返回是否换了
inline bool button_bar(const char *const names[], int n, int &cur) {
  bool changed = false;
  for (int i = 0; i < n; ++i) {
    if (i)
      ImGui::SameLine(0, 2);
    const bool on = cur == i;
    if (on)
      ImGui::PushStyleColor(ImGuiCol_Button, ImGui::GetStyleColorVec4(ImGuiCol_ButtonActive));
    if (ImGui::SmallButton(names[i]) && !on) {
      cur = i;
      changed = true;
    }
    if (on)
      ImGui::PopStyleColor();
  }
  return changed;
}

// 线尾小字: at = 线末点的像素位置
struct EndTag {
  ImVec2 at;
  ImU32 col;
  std::string text;
};
inline constexpr float kTagGap = 4.0f; // 离末点的间距 (px)
// 小字 (字号 = 字体 × scale) 占的最宽像素, 含两侧间距
inline float end_tags_width(const std::vector<EndTag> &tags, float scale) {
  ImFont *font = ImGui::GetFont();
  const float fs = ImGui::GetFontSize() * scale;
  float w = 0.0f;
  for (const EndTag &t : tags)
    w = std::max(w, font->CalcTextSizeA(fs, FLT_MAX, 0.0f, t.text.c_str()).x);
  return w + 2.0f * kTagGap;
}
// 当前图内画小字: 按像素 y 排开不重叠 (自上而下推开, 再自下而上收进图底), 贴末点右侧, 右缘不过 right
inline void draw_end_tags(std::vector<EndTag> &tags, float scale, float right) {
  std::sort(tags.begin(), tags.end(), [](const EndTag &a, const EndTag &b) { return a.at.y < b.at.y; });
  ImFont *font = ImGui::GetFont();
  const float fs = ImGui::GetFontSize() * scale, bottom = ImPlot::GetPlotPos().y + ImPlot::GetPlotSize().y;
  for (size_t i = 1; i < tags.size(); ++i)
    tags[i].at.y = std::max(tags[i].at.y, tags[i - 1].at.y + fs);
  for (size_t i = tags.size(); i-- > 0;)
    tags[i].at.y = std::min(tags[i].at.y, i + 1 < tags.size() ? tags[i + 1].at.y - fs : bottom - 0.5f * fs);
  ImDrawList *dl = ImPlot::GetPlotDrawList();
  ImPlot::PushPlotClipRect();
  for (const EndTag &t : tags) {
    const ImVec2 sz = font->CalcTextSizeA(fs, FLT_MAX, 0.0f, t.text.c_str());
    dl->AddText(font, fs, ImVec2(std::min(t.at.x + kTagGap, right - sz.x), t.at.y - 0.5f * sz.y), t.col, t.text.c_str());
  }
  ImPlot::PopPlotClipRect();
}

// 一段横 / 竖虚线 [px] (kDashOn 实 / kDashOff 空)
inline constexpr float kDashOn = 4.f, kDashOff = 3.f;
inline void dash(ImDrawList *dl, ImVec2 a, const ImVec2 &b, ImU32 col) {
  const float len = std::abs(b.x - a.x) + std::abs(b.y - a.y);
  const ImVec2 u((b.x - a.x) / len, (b.y - a.y) / len);
  for (float t = 0; t < len; t += kDashOn + kDashOff) {
    const float e = std::min(t + kDashOn, len);
    dl->AddLine(ImVec2(a.x + u.x * t, a.y + u.y * t), ImVec2(a.x + u.x * e, a.y + u.y * e), col);
  }
}

// 现价十字: 过末点 at [px] 的横 / 竖虚线 (色 col), 右壁标读数 y, 底壁标时刻 x (色底黑字)
struct NowCross {
  ImVec2 at;
  ImU32 col;
  std::string y, x;
};
inline constexpr ImVec2 kCrossPad(2.f, 1.f); // 标签框内边距
// 右壁标签占的宽 (字号 = 字体 × scale)
inline float now_cross_width(const NowCross &c, float scale) {
  return ImGui::GetFont()->CalcTextSizeA(ImGui::GetFontSize() * scale, FLT_MAX, 0.f, c.y.c_str()).x + 2 * kCrossPad.x;
}
// 当前图内画十字; 右壁标签右缘 = right
inline void draw_now_cross(const NowCross &c, float scale, float right) {
  ImDrawList *dl = ImPlot::GetPlotDrawList();
  ImFont *font = ImGui::GetFont();
  const float fs = ImGui::GetFontSize() * scale;
  const ImVec2 pos = ImPlot::GetPlotPos(), psz = ImPlot::GetPlotSize();
  const float top = pos.y, bottom = pos.y + psz.y, px = std::round(c.at.x) + 0.5f, py = std::round(c.at.y) + 0.5f;
  const ImU32 ink = IM_COL32(17, 17, 17, 255);
  ImPlot::PushPlotClipRect();
  dash(dl, ImVec2(pos.x, py), ImVec2(pos.x + psz.x, py), c.col);
  dash(dl, ImVec2(px, top), ImVec2(px, bottom), c.col);
  const auto label = [&](const std::string &s, float x, float y) { // (x, y) = 底框左上角
    const ImVec2 sz = font->CalcTextSizeA(fs, FLT_MAX, 0.f, s.c_str());
    dl->AddRectFilled(ImVec2(x, y), ImVec2(x + sz.x + 2 * kCrossPad.x, y + sz.y + 2 * kCrossPad.y), c.col);
    dl->AddText(font, fs, ImVec2(x + kCrossPad.x, y + kCrossPad.y), ink, s.c_str());
  };
  const float bw = now_cross_width(c, scale), bh = fs + 2 * kCrossPad.y;
  label(c.y, right - bw, std::clamp(py - bh * 0.5f, top, bottom - bh));
  const float tw = font->CalcTextSizeA(fs, FLT_MAX, 0.f, c.x.c_str()).x + 2 * kCrossPad.x;
  label(c.x, std::clamp(px - tw * 0.5f, pos.x, pos.x + psz.x - tw), bottom - bh);
  ImPlot::PopPlotClipRect();
}

} // namespace GUI
