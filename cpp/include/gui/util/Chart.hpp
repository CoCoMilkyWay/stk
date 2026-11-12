#pragma once

// 图表公共件 (ASSET 行情 / Market 净值 / Beta 风格表同一份): 涨跌色, 名次色 + 截面名次 + 表头排序口径, 横向框选, 色环, 右表宽, 开关按钮 / 档位按钮条, 线尾小字, 现价十字, 区间条

#include "imgui.h"
#include "implot.h"
#include "misc/date.hpp"

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdio>
#include <limits>
#include <numbers>
#include <string>
#include <utility>
#include <vector>

namespace GUI {

// 涨跌色: 涨红 / 跌绿 / 平灰
inline constexpr ImVec4 kUp(0.945f, 0.298f, 0.298f, 1.f), kDn(0.416f, 0.600f, 0.333f, 1.f), kFlat(0.80f, 0.80f, 0.80f, 1.f);
inline ImVec4 tint(double v, double ref) { return v > ref ? kUp : v < ref ? kDn
                                                                          : kFlat; }

inline constexpr ImVec4 kHead(0.4f, 0.8f, 1.0f, 1.0f);       // 节标题
inline constexpr ImVec4 kStratCol(1.0f, 0.55f, 0.15f, 1.0f); // 强调 (策略线 / 新选)

// 好 → 差色: u ∈ [0, 1] (0 = 最便宜 / 名次最好 绿 → 1 = 最贵 / 最差 红) 按色相渐变 (VAL band / 各右表名次底色同一份)
inline ImVec4 band_col(double u, float a) {
  ImVec4 c(0.f, 0.f, 0.f, a);
  ImGui::ColorConvertHSVtoRGB(0.33f * static_cast<float>(1.0 - u), 0.8f, 0.9f, c.x, c.y, c.z);
  return c;
}

// 截面名次 (各表的名次底色 / 截面色同一核): n 个值 x(i) 在有限值里排名 → put(i, u), u ∈ [0, 1] (0 = 最好, 即 band_col 的绿端;
//   并列取中, 只一个 = 0.5); good = +1 → x 高为好, −1 → x 低为好; 非有限的不排 (不 put). 排序一遍 O(n log n)
template <class X, class Put>
void rank_by(size_t n, int good, const X &x, const Put &put) {
  std::vector<std::pair<double, size_t>> v; // (−good · x, i)
  for (size_t i = 0; i < n; ++i)
    if (const double y = x(i); std::isfinite(y))
      v.push_back({-good * y, i});
  std::sort(v.begin(), v.end());
  const size_t m = v.size();
  for (size_t a = 0, b = 0; a < m; a = b) {
    while (b < m && v[b].first == v[a].first)
      ++b;
    const double u = m > 1 ? (static_cast<double>(a) + 0.5 * static_cast<double>(b - a - 1)) / static_cast<double>(m - 1) : 0.5;
    for (size_t k = a; k < b; ++k)
      put(v[k].second, u);
  }
}

// 表头排序的比较 (各表同一口径): 非有限值恒在末, 其余按 desc 降 / 升
inline bool sort_less(double x, double y, bool desc) {
  if (!std::isfinite(x) || !std::isfinite(y))
    return std::isfinite(x) && !std::isfinite(y);
  return desc ? x > y : x < y;
}

// OKLCH (感知均匀: 同 L / C 的色环上两色的感知距离 = OKLab 弦长, 小角度 ∝ 色相差) → sRGB; 分量截到 [0, 1] 只吸收舍入
inline ImVec4 oklch(float L, float C, float hue) {
  const float h = 2.0f * std::numbers::pi_v<float> * hue, a = C * std::cos(h), b = C * std::sin(h);
  const float l = std::pow(L + 0.3963377774f * a + 0.2158037573f * b, 3.0f), m = std::pow(L - 0.1055613458f * a - 0.0638541728f * b, 3.0f),
              s = std::pow(L - 0.0894841775f * a - 1.2914855480f * b, 3.0f);
  const auto gamma = [](float x) {
    x = std::clamp(x, 0.0f, 1.0f);
    return x <= 0.0031308f ? 12.92f * x : 1.055f * std::pow(x, 1.0f / 2.4f) - 0.055f;
  };
  return {gamma(4.0767416621f * l - 3.3077115913f * m + 0.2309699292f * s), gamma(-1.2684380046f * l + 2.6097574011f * m - 0.3413193965f * s),
          gamma(-0.0041960863f * l - 0.7034186147f * m + 1.7076147010f * s), 1.0f};
}
inline constexpr float kRowL = 0.75f, kRowC = 0.125f; // 行色的 OKLCH 亮度 / 艳度: 该亮度下整环都在 sRGB 色域内的艳度上限附近 (环不被截变形)

// 右表宽 (Market 各页与 Beta→Style 同宽, 不随内容: 字符格数, 表格超宽横滚) / 至多占整宽的比例; avail = 页内可用宽
inline float sheet_width(float avail) {
  constexpr float kCells = 100.0f, kMaxFrac = 0.5f;
  const ImGuiStyle &st = ImGui::GetStyle();
  return std::min(kCells * ImGui::CalcTextSize("0").x + 2.0f * (st.WindowPadding.x + st.ChildBorderSize), avail * kMaxFrac);
}

// 开关小按钮 (各页的开 / 关与档位同一种): 开 = 按下色, 关 = 底色; 悬停 / 按住各按自身态提亮, 两态始终分得开; 返回是否点了
inline bool toggle_button(const char *label, bool on) {
  const ImVec4 c = ImGui::GetStyleColorVec4(on ? ImGuiCol_ButtonActive : ImGuiCol_Button);
  const ImVec4 lit = on ? ImVec4(c.x + (1.f - c.x) * 0.25f, c.y + (1.f - c.y) * 0.25f, c.z + (1.f - c.z) * 0.25f, c.w) : ImVec4(c.x, c.y, c.z, std::min(1.f, c.w * 1.6f));
  ImGui::PushStyleColor(ImGuiCol_Button, c);
  ImGui::PushStyleColor(ImGuiCol_ButtonHovered, lit);
  ImGui::PushStyleColor(ImGuiCol_ButtonActive, lit);
  const bool hit = ImGui::SmallButton(label);
  ImGui::PopStyleColor(3);
  return hit;
}

// 一排小按钮单选 (toggle_button, 选中的开): 点 = cur 换成它; 返回是否换了
inline bool button_bar(const char *const names[], int n, int &cur) {
  bool changed = false;
  for (int i = 0; i < n; ++i) {
    if (i)
      ImGui::SameLine(0, 2);
    if (toggle_button(names[i], cur == i) && cur != i) {
      cur = i;
      changed = true;
    }
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

// 横向框选 (ASSET 行情两图 / VAL 主图 / CTA 净值图同一种): 左键在图上按下 begin(x) 起拖, 拖动中各图 draw(x) 画竖带;
//   松手 end(x) 落定: 屏上拖过 ≥ kPx 才算, 给出选段 [lo, hi] (轴坐标). x = 鼠标 x 截到图区 (轴坐标), 调用方给
struct XZoom {
  static constexpr float kPx = 4.f;                       // 生效的最小拖动 [px]
  double from = std::numeric_limits<double>::quiet_NaN(); // 起点 x; NaN = 未在框选
  float from_px = 0;                                      // 起点屏幕 x
  bool on() const { return !std::isnan(from); }
  void begin(double x) { from = x, from_px = ImGui::GetMousePos().x; }
  void cancel() { from = std::numeric_limits<double>::quiet_NaN(); }
  // 当前图里画 from → x 的竖带 (BeginPlot 之后)
  void draw(double x) const {
    assert(on());
    const ImVec2 pos = ImPlot::GetPlotPos(), sz = ImPlot::GetPlotSize();
    const float a = ImPlot::PlotToPixels(std::min(from, x), 0.0).x, b = ImPlot::PlotToPixels(std::max(from, x), 0.0).x;
    ImDrawList *dl = ImPlot::GetPlotDrawList();
    ImPlot::PushPlotClipRect();
    dl->AddRectFilled(ImVec2(a, pos.y), ImVec2(b, pos.y + sz.y), IM_COL32(86, 156, 214, 40));
    for (const float e : {a, b})
      dl->AddLine(ImVec2(e, pos.y), ImVec2(e, pos.y + sz.y), IM_COL32(86, 156, 214, 160));
    ImPlot::PopPlotClipRect();
  }
  // 框选中每帧调 (图之后亦可: 光标盖掉 ImPlot 的); 左键松开 → 结束, 拖够了返回 true (lo ≤ hi)
  bool end(double x, double &lo, double &hi) {
    if (!on())
      return false;
    if (!ImGui::IsMouseReleased(ImGuiMouseButton_Left)) {
      ImGui::SetMouseCursor(ImGuiMouseCursor_ResizeEW);
      return false;
    }
    const bool ok = std::abs(ImGui::GetMousePos().x - from_px) >= kPx;
    lo = std::min(from, x), hi = std::max(from, x);
    cancel();
    return ok;
  }
  // 单图: draw + end
  bool track(double x, double &lo, double &hi) {
    if (!on())
      return false;
    draw(x);
    return end(x, lo, hi);
  }
};
// 当前图里鼠标 x 截到 x 视野 (轴坐标; XZoom 的 x)
inline double plot_mouse_x() {
  const ImPlotRange r = ImPlot::GetPlotLimits().X;
  return std::clamp(ImPlot::GetPlotMousePos().x, r.Min, r.Max);
}

// 区间条的日段 [d0, d1) (日轴下标) + 拖动态
struct DayRange {
  int d0 = 0, d1 = 0;
  int drag = 0;                 // 拖动中: 0 无 / 1 左把手 / 2 右把手 / 3 段内平移
  double grab_day = 0.0;        // 按下时鼠标所在日 (平移按与它的差算)
  int grab_d0 = 0, grab_d1 = 0; // 按下时的 [d0, d1) (平移保持日数)
};
// 当前图内的区间条 (调用方已 BeginPlot / 设轴 / 画底线; x = 日下标 · day_w): 选段 [d0, d1), 拖动中即生效;
//   左右边 = 竖把手 (拖改起止, 吸附到月初 = 每月首个交易日, 右边 = 下月月初 / 末尾; 双击拉到头); 段内拖 = 平移 (日数不变), 段内双击 = 回全段;
//   悬停 / 拖动时把手旁标起止日
inline void range_bar(DayRange &rg, const std::vector<misc::Ymd> &dates, double day_w) {
  const int D = static_cast<int>(dates.size());
  if (D == 0)
    return;
  std::vector<int> ms; // 吸附点 = 月初下标, 末尾补 D
  for (int i = 0; i < D; ++i)
    if (i == 0 || dates[i] / 100 != dates[i - 1] / 100)
      ms.push_back(i);
  ms.push_back(D);
  const int K = static_cast<int>(ms.size()) - 1;
  const auto near_k = [&](double day, int lo, int hi) { // ms[lo..hi] 里离 day 最近的那个的下标
    int best = lo;
    for (int k = lo + 1; k <= hi; ++k)
      if (std::abs(ms[k] - day) < std::abs(ms[best] - day))
        best = k;
    return best;
  };
  const ImPlotRect lim = ImPlot::GetPlotLimits();
  const auto px_of = [&](int day) { return ImPlot::PlotToPixels(day * day_w, lim.Y.Min).x; };
  const ImVec2 pp = ImPlot::GetPlotPos(), ps = ImPlot::GetPlotSize(), mp = ImGui::GetMousePos();
  const double mday = ImPlot::GetPlotMousePos().x / day_w;
  constexpr float kGrab = 7.0f; // 把手抓取半宽 (px)
  int over = 0;                 // 鼠标下: 1 左把手 / 2 右把手 / 3 段内
  if (mp.x >= pp.x && mp.x <= pp.x + ps.x && mp.y >= pp.y && mp.y <= pp.y + ps.y && ImGui::IsWindowHovered(ImGuiHoveredFlags_AllowWhenBlockedByActiveItem)) {
    const float x0 = px_of(rg.d0), x1 = px_of(rg.d1);
    over = std::abs(mp.x - x0) <= kGrab ? 1 : std::abs(mp.x - x1) <= kGrab ? 2
                                          : (mp.x > x0 && mp.x < x1)       ? 3
                                                                           : 0;
  }
  assert(0 <= rg.d0 && rg.d0 < rg.d1 && rg.d1 <= D);
  if (over != 0 && ImGui::IsMouseDoubleClicked(0)) { // 左 / 右把手 = 该边拉到头, 段内 = 回全段
    if (over != 2)
      rg.d0 = 0;
    if (over != 1)
      rg.d1 = D;
  } else if (rg.drag == 0 && over != 0 && ImGui::IsMouseClicked(0)) {
    rg.drag = over, rg.grab_day = mday, rg.grab_d0 = rg.d0, rg.grab_d1 = rg.d1;
  }
  if (rg.drag != 0 && !ImGui::IsMouseDown(0))
    rg.drag = 0;
  if (rg.drag == 1) { // 左边取 < d1 的月初
    const int hi = static_cast<int>(std::ranges::lower_bound(ms, rg.d1) - ms.begin()) - 1;
    rg.d0 = ms[near_k(mday, 0, hi)];
  } else if (rg.drag == 2) { // 右边取 > d0 的月初 / 末尾
    const int lo = static_cast<int>(std::ranges::upper_bound(ms, rg.d0) - ms.begin());
    rg.d1 = ms[near_k(mday, lo, K)];
  } else if (rg.drag == 3) {
    const int w = rg.grab_d1 - rg.grab_d0;
    rg.d0 = std::clamp(rg.grab_d0 + static_cast<int>(std::lround(mday - rg.grab_day)), 0, D - w), rg.d1 = rg.d0 + w;
  }
  const int act = rg.drag != 0 ? rg.drag : over;
  if (act == 1 || act == 2)
    ImGui::SetMouseCursor(ImGuiMouseCursor_ResizeEW);
  else if (act == 3)
    ImGui::SetMouseCursor(ImGuiMouseCursor_Hand);

  // 画: 段外压暗 + 段内淡色 + 两边竖把手 (带握纹, 悬停 / 拖动高亮)
  const float x0 = px_of(rg.d0), x1 = px_of(rg.d1), yb = pp.y + ps.y;
  ImDrawList &dl = *ImPlot::GetPlotDrawList();
  ImPlot::PushPlotClipRect();
  dl.AddRectFilled(pp, ImVec2(x0, yb), IM_COL32(0, 0, 0, 120));
  dl.AddRectFilled(ImVec2(x1, pp.y), ImVec2(pp.x + ps.x, yb), IM_COL32(0, 0, 0, 120));
  dl.AddRectFilled(ImVec2(x0, pp.y), ImVec2(x1, yb), IM_COL32(102, 204, 255, 28));
  const auto handle = [&](float x, bool hot) {
    const ImU32 c = hot ? IM_COL32(190, 240, 255, 255) : IM_COL32(102, 204, 255, 255);
    dl.AddLine(ImVec2(x, pp.y), ImVec2(x, yb), c, hot ? 3.0f : 2.0f);
    constexpr float hw = 5.0f;
    const float cx = std::clamp(x, pp.x + hw, pp.x + ps.x - hw), cy = pp.y + ps.y * 0.5f, hh = ps.y * 0.28f;
    dl.AddRectFilled(ImVec2(cx - hw, cy - hh), ImVec2(cx + hw, cy + hh), c, 3.0f);
    for (const float gx : {cx - 1.5f, cx + 1.5f})
      dl.AddLine(ImVec2(gx, cy - hh * 0.5f), ImVec2(gx, cy + hh * 0.5f), IM_COL32(20, 50, 70, 255), 1.0f);
  };
  handle(x0, act == 1 || act == 3);
  handle(x1, act == 2 || act == 3);
  if (act != 0) { // 起止日贴把手外侧 (放不下改内侧)
    char s0[16], s1[16];
    const misc::Ymd a = dates[rg.d0], b = dates[rg.d1 - 1];
    std::snprintf(s0, sizeof(s0), "%d/%02d/%02d", a / 10000, a / 100 % 100, a % 100);
    std::snprintf(s1, sizeof(s1), "%d/%02d/%02d", b / 10000, b / 100 % 100, b % 100);
    const ImVec2 t0 = ImGui::CalcTextSize(s0), t1 = ImGui::CalcTextSize(s1);
    float lx = x0 - 9.0f - t0.x, rx = x1 + 9.0f;
    if (lx < pp.x)
      lx = x0 + 9.0f;
    if (rx + t1.x > pp.x + ps.x)
      rx = x1 - 9.0f - t1.x;
    const auto label = [&](float tx, const char *s, const ImVec2 &ts) {
      dl.AddRectFilled(ImVec2(tx - 3.0f, pp.y + 2.0f), ImVec2(tx + ts.x + 3.0f, pp.y + 4.0f + ts.y), IM_COL32(10, 20, 30, 220), 3.0f);
      dl.AddText(ImVec2(tx, pp.y + 3.0f), IM_COL32(255, 255, 255, 255), s);
    };
    label(lx, s0, t0);
    label(rx, s1, t1);
  }
  ImPlot::PopPlotClipRect();
}

} // namespace GUI
