// Market 行情页主图 (NAV 资金曲线 ↔ VAL 估值 band) + 区间条 — 见 LinesCommon.hpp
#include "gui/task_market/ui/LinesCommon.hpp"

#include "gui/util/Chart.hpp" // tint / EndTag / NowCross / range_bar
#include "gui/util/HoverSync.hpp"
#include "gui/util/Tip.hpp"
#include "imgui.h"
#include "implot.h"
#include "implot_internal.h" // GetItem (主图例显隐 / 线色), ButtonBehavior (图例矩阵)
#include "misc/date.hpp"     // dash (现价十字时刻) / 日历日运算 (区间档位)

#include <algorithm>
#include <array>
#include <cassert>
#include <cfloat>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <iterator>
#include <map>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <vector>

namespace GUI::Market {

namespace {

// 横轴刻度 (主图 / 区间条): x = 日下标 · kDayW + 槽 → YY/MM/DD, 只到日 (user_data = 日并集轴)
int day_formatter(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<misc::Ymd> *>(ud);
  if (dates.empty() || v < 0)
    return std::snprintf(buf, size, " ");
  const misc::Ymd y = dates[std::min(static_cast<size_t>(v) / kDayW, dates.size() - 1)];
  return std::snprintf(buf, size, "%02d/%02d/%02d", y / 10000 % 100, y / 100 % 100, y % 100);
}
// 横轴刻度位: 只落在交易日收盘 x_of(d, kClose) (日线点位所在); 疏密 = kTickSteps 里首个刻度数不超过 图宽 / (kTickGap 个标签宽) 的档
//   档 = 每 k 个交易日 / 每 k 月的首个交易日 / 每 k 年的首个交易日 (日并集轴首日算所在月 / 年的首日)
enum TickUnit : int { kTickDay,
                      kTickMonth,
                      kTickYear };
struct TickStep {
  TickUnit unit;
  int k;
};
constexpr TickStep kTickSteps[] = {{kTickDay, 1}, {kTickDay, 2}, {kTickDay, 5}, {kTickDay, 10}, {kTickMonth, 1}, {kTickMonth, 2}, {kTickMonth, 3}, {kTickMonth, 6}, {kTickYear, 1}, {kTickYear, 2}, {kTickYear, 5}, {kTickYear, 10}};
constexpr float kTickGap = 2.0f;
int tick_period(misc::Ymd y, TickUnit u) { return u == kTickMonth ? y / 10000 * 12 + y / 100 % 100 - 1 : y / 10000; }
bool on_step(const std::vector<misc::Ymd> &dates, size_t d, TickStep s) {
  if (s.unit == kTickDay)
    return d % static_cast<size_t>(s.k) == 0;
  const int p = tick_period(dates[d], s.unit);
  return (d == 0 || tick_period(dates[d - 1], s.unit) != p) && p % s.k == 0;
}
void day_locator(ImPlotTicker &t, const ImPlotRange &r, float px, bool, ImPlotFormatter fmt, void *ud) {
  const auto &dates = *static_cast<const std::vector<misc::Ymd> *>(ud);
  const double D = static_cast<double>(dates.size()), W = static_cast<double>(kDayW), c = static_cast<double>(kClose);
  const double lo = std::max(0.0, std::ceil((r.Min - c) / W)), hi = std::min(D, std::floor((r.Max - c) / W) + 1.0); // 视野内的日 [lo, hi)
  if (!(lo < hi))
    return;
  const size_t d0 = static_cast<size_t>(lo), d1 = static_cast<size_t>(hi);
  const double cap = px / (kTickGap * ImGui::CalcTextSize("00/00/00").x);
  constexpr size_t kSteps = std::size(kTickSteps);
  size_t k = 0;
  for (; k + 1 < kSteps; ++k) {
    size_t n = 0;
    for (size_t d = d0; d < d1; ++d)
      n += on_step(dates, d, kTickSteps[k]);
    if (static_cast<double>(n) <= cap)
      break;
  }
  for (size_t d = d0; d < d1; ++d)
    if (on_step(dates, d, kTickSteps[k]))
      t.AddTick(x_of(d, kClose), true, 0, true, fmt, ud);
}
// 横轴 = 日并集轴 (主图 / 区间条同一份): 刻度位 day_locator + 标签 day_formatter
void setup_day_axis(const std::vector<misc::Ymd> &dates) {
  ImPlot::SetupAxisFormat(ImAxis_X1, day_formatter, const_cast<std::vector<misc::Ymd> *>(&dates));
  ImPlot::GetCurrentPlot()->Axes[ImAxis_X1].Locator = day_locator;
}

// 主图 y 恒贴合视野内进 fit 的线 (AutoFit | RangeFit), 上下各留跨度 × kFitMargin (log 轴按对数跨度)
constexpr double kFitMargin = 0.2;
// 本帧 x 在 fit (切片画全段 / 小字不按视野筛)
bool fit_x() { return ImPlot::GetCurrentPlot()->Axes[ImAxis_X1].FitThisFrame; }
// y 的留白: 须在本帧进 fit 的 item 都画完之后
void fit_margin(bool log) {
  const ImPlotRange &e = ImPlot::GetCurrentPlot()->Axes[ImAxis_Y1].FitExtents;
  if (!(e.Min < e.Max))
    return;
  assert(!log || e.Min > 0.0);
  const auto f = [&](double v) { return log ? std::log10(v) : v; };
  const auto g = [&](double t) { return log ? std::pow(10.0, t) : t; };
  const double a = f(e.Min), b = f(e.Max), m = kFitMargin * (b - a);
  ImPlot::FitPointY(g(a - m));
  ImPlot::FitPointY(g(b + m));
}

// 绘图区正中的灰字
void plot_hint(const char *s) {
  const ImVec2 p = ImPlot::GetPlotPos(), sz = ImPlot::GetPlotSize(), t = ImGui::CalcTextSize(s);
  ImPlot::PushPlotClipRect();
  ImPlot::GetPlotDrawList()->AddText(ImVec2(p.x + 0.5f * (sz.x - t.x), p.y + 0.5f * (sz.y - t.y)), ImGui::GetColorU32(ImGuiCol_TextDisabled), s);
  ImPlot::PopPlotClipRect();
}

// 组线宽: 强调组 (kGroupDefs::accent) 加粗
float row_width(int g) { return kGroupDefs[g].accent ? 2.0f : 1.0f; }
// 成对线表 [口径][PairLine] (口径 = pair_kind: 0 = 有行情键 (篮子 / 成分 / 账户), 1 = 因子 / 策略): 主图例口径列 / 线尾小字后缀
constexpr const char *kPairName[2][kPairLines] = {{"可成交", "理想"}, {"有冲击", "无冲击"}};
constexpr const char *kPairTag[2][kPairLines] = {{" 可成交", " 理想"}, {"", " 无冲击"}};
int pair_kind(const LinesRow &r) { return !r.ref.ok(); }

// VAL 线表 (标的线色 = 该行的 row_color): band 填色 / 线 / 线尾刻度的 alpha
constexpr float kBandFillA = 0.12f, kBandLineA = 0.35f, kBandTagA = 0.9f;
// VAL 线尾刻度: 价格线末点 × (1 + r) 的点位 (对照 band 线看离各档还差多少); 色 = band_col(u) (涨绿, 跌越多越红)
struct ValMark {
  double r, u;
};
constexpr ValMark kValMarks[] = {{0.10, 0.0}, {-0.05, 0.6}, {-0.10, 0.7}, {-0.20, 0.85}, {-0.30, 1.0}};

// 估值值的显示 (收益率 %, 小数位按 kVal), 不可算 → "-"
void fmt_val(int b, double x, char *buf, size_t n) {
  if (std::isfinite(x))
    std::snprintf(buf, n, "%.*f%%", kVal[b].prec, 100.0 * x);
  else
    std::snprintf(buf, n, "-");
}

// VAL 绝对估值等值线: 篮子全史倍数 m (= 1 / 收益率) 区间 kContours 等分, y = unit · m (unit = P · v, 见 LinesBand); 很淡的灰, 悬停 kContourHit px 内最近一条高亮
constexpr float kContourHit = 6.0f; // 悬停命中半宽 (px)
const ImVec4 kContourCol(0.6f, 0.6f, 0.6f, 0.22f), kContourHotCol(1.0f, 1.0f, 1.0f, 0.9f);
// 一条等值线 (零拷贝: 按下标回调取点; u 的 NaN 处断开)
struct ContourLine {
  const double *x, *u;
  double m;
};
ImPlotPoint contour_pt(int i, void *p) {
  const ContourLine &l = *static_cast<const ContourLine *>(p);
  return {l.x[i], l.u[i] * l.m};
}

// 图例矩阵 (legend) 的格按钮: 叠在图上 (AllowOverlap), 按下即触发; hov = 悬停
bool lg_button(const ImRect &r, const char *id, bool &hov) {
  bool held = false;
  const ImGuiID gid = ImGui::GetID(id);
  ImGui::KeepAliveID(gid);
  return ImGui::ButtonBehavior(r, gid, &hov, &held, ImGuiButtonFlags_AllowOverlap | ImGuiButtonFlags_PressedOnClick);
}
// 图例矩阵的开关格: R 行 × C 列, item(r, c) = 该格的 item; 整行 / 整列 (r0 / c0 = −1 = 不限): 集合里有显示的 → 全隐, 否则全显
template <class Item>
bool lg_any(int R, int C, int r0, int c0, const Item &item) {
  for (int r = 0; r < R; ++r)
    for (int c = 0; c < C; ++c)
      if ((r0 < 0 || r == r0) && (c0 < 0 || c == c0) && item(r, c)->Show)
        return true;
  return false;
}
template <class Item>
void lg_flip(int R, int C, int r0, int c0, const Item &item) {
  const bool any = lg_any(R, C, r0, c0, item);
  for (int r = 0; r < R; ++r)
    for (int c = 0; c < C; ++c)
      if ((r0 < 0 || r == r0) && (c0 < 0 || c == c0))
        item(r, c)->Show = !any;
}

// 主图左上角 (对仗日内栏): 区间档位条贴绘图区左上 (内缩 kBarPad), 图例矩阵接在其下 (lg_origin)
constexpr float kBarPad = 4.0f;
ImVec2 lg_origin() {
  const ImVec2 p = ImPlot::GetPlotPos();
  return ImVec2(p.x + kBarPad, p.y + 2.f * kBarPad + ImGui::GetTextLineHeight());
}
// 图例矩阵的字: 字号 = 字体 × kLgScale, 行高 = 字号; 框内边距 kLgPad
constexpr float kLgScale = 0.8f;
constexpr ImVec2 kLgPad(4.f, 2.f);
struct LgFont {
  ImFont *font = ImGui::GetFont();
  float fs = ImGui::GetFontSize() * kLgScale;
  float w(const char *s) const { return font->CalcTextSizeA(fs, FLT_MAX, 0.f, s).x; }
  void text(ImDrawList &dl, const ImVec2 &at, ImU32 col, const char *s) const { dl.AddText(font, fs, at, col, s); }
};

// ---- 图例矩阵 (NAV / VAL 同一件, 两页只填表), 绘图区左上 (lg_origin): 行 = 行头 (名) + 各列一格; 列 = 文本列 | 开关列 (格 = ImPlot item(行, 开关号));
//      开关格 = 色块 (实心 = 显示, 空框 = 隐; 格有字 → 色块贴左缘, 否则居中), 点格切该项; 点行头 / 开关列头切整行 / 整列 (有显示的 → 全隐, 否则全显);
//      列头右对齐 (只色块的列居中), 文本列头灰; 字右对齐, 字色 0 = 随显隐 (开关格 = 该项, 文本格 = 行有显示 → 字色, 否则灰). 返回悬停 + 框 ----
struct LgCol {
  const char *head;
  int toggle = -1; // 开关号 (−1 = 文本列)
};
struct LgRow {
  const char *name;
  ImU32 col; // 名色 (行有显示时)
};
struct LgCell {
  std::string text; // 空 = 无字
  ImU32 col = 0;    // 字色 (0 = 随显隐)
  ImVec4 sw[2]{};   // 开关格色块: 左 / 右 (同色 = 实心)
};
// 悬停: on = 在行头 / 开关列头 / 开关格上 (r / t = 其行 / 开关号, −1 = 整列 / 整行); row = 所在行 (框内任意处; −1 = 列头行 / 框外)
struct LgHover {
  bool on = false;
  int r = -1, t = -1;
  int row = -1;
  ImRect box;
  bool at(int r0, int t0) const { return on && (r < 0 || r == r0) && (t < 0 || t == t0); }
};
// 格 = cells [行][列]
template <class Item>
LgHover legend(const char *id, std::span<const LgCol> cols, std::span<const LgRow> rows, std::span<const LgCell> cells, const Item &item) {
  LgHover h;
  const int R = static_cast<int>(rows.size()), C = static_cast<int>(cols.size());
  assert(cells.size() == rows.size() * cols.size());
  if (R == 0)
    return h;
  const int T = static_cast<int>(std::ranges::count_if(cols, [](const LgCol &k) { return k.toggle >= 0; }));
  std::vector<ImPlotItem *> its(static_cast<size_t>(R * T)); // 先于下面的 PushID 取齐: item 的 ID 随当前 ID 栈 (须同画线处)
  for (int r = 0; r < R; ++r)
    for (int t = 0; t < T; ++t)
      its[static_cast<size_t>(r * T + t)] = item(r, t);
  const auto item_at = [&](int r, int t) { return its[static_cast<size_t>(r * T + t)]; };
  const auto cell = [&](int r, int c) -> const LgCell & { return cells[static_cast<size_t>(r * C + c)]; };
  const LgFont lf;
  const float th = lf.fs, sw = th * 0.7f, gx = ImGui::GetStyle().ItemSpacing.x, sg = 0.3f * th;
  float w_head = 0.f;
  for (const LgRow &r : rows)
    w_head = std::max(w_head, lf.w(r.name));
  std::vector<float> w(static_cast<size_t>(C)), xc(static_cast<size_t>(C));
  std::vector<uint8_t> bare(static_cast<size_t>(C)); // 只色块的开关列
  const ImVec2 o = lg_origin();
  float x = o.x + kLgPad.x + w_head;
  for (int c = 0; c < C; ++c) {
    const size_t k = static_cast<size_t>(c);
    bare[k] = cols[k].toggle >= 0;
    w[k] = lf.w(cols[k].head);
    for (int r = 0; r < R; ++r) {
      const LgCell &e = cell(r, c);
      bare[k] &= e.text.empty();
      w[k] = std::max(w[k], (cols[k].toggle < 0 ? 0.f : e.text.empty() ? sw
                                                                       : sw + sg) +
                                lf.w(e.text.c_str()));
    }
    xc[k] = x + gx;
    x = xc[k] + w[k];
  }
  h.box = ImRect(o, ImVec2(x + kLgPad.x, o.y + 2.f * kLgPad.y + static_cast<float>(R + 1) * th));
  const auto y_row = [&](int r) { return h.box.Min.y + kLgPad.y + static_cast<float>(r + 1) * th; }; // r = −1 → 列头行
  const auto any = [&](int r0, int t0) { return lg_any(R, T, r0, t0, item_at); };
  const auto hover = [&](int r, int t) { h.on = true, h.r = r, h.t = t; };
  ImGui::PushID(id);
  bool bg = false;
  lg_button(h.box, "##bg", bg);
  ImDrawList &dl = *ImPlot::GetPlotDrawList();
  ImPlot::PushPlotClipRect();
  dl.AddRectFilled(h.box.Min, h.box.Max, ImPlot::GetStyleColorU32(ImPlotCol_LegendBg));
  dl.AddRect(h.box.Min, h.box.Max, ImPlot::GetStyleColorU32(ImPlotCol_LegendBorder));
  const ImU32 c_txt = ImGui::GetColorU32(ImGuiCol_Text), c_dis = ImGui::GetColorU32(ImGuiCol_TextDisabled),
              c_hov = ImGui::GetColorU32(ImGuiCol_PlotLinesHovered);
  for (int c = 0; c < C; ++c) {
    const size_t k = static_cast<size_t>(c);
    const LgCol &col = cols[k];
    const float tw = lf.w(col.head);
    const ImRect rc(ImVec2(xc[k], y_row(-1)), ImVec2(xc[k] + w[k], y_row(-1) + th));
    const ImVec2 at(bare[k] ? rc.Min.x + (w[k] - tw) * 0.5f : rc.Max.x - tw, rc.Min.y);
    if (col.toggle < 0) {
      lf.text(dl, at, c_dis, col.head);
      continue;
    }
    ImGui::PushID(c);
    bool hov = false;
    if (lg_button(rc, "##c", hov))
      lg_flip(R, T, -1, col.toggle, item_at);
    ImGui::PopID();
    if (hov)
      hover(-1, col.toggle);
    lf.text(dl, at, hov ? c_hov : any(-1, col.toggle) ? c_txt
                                                      : c_dis,
            col.head);
  }
  for (int r = 0; r < R; ++r) {
    const float y = y_row(r);
    ImGui::PushID(r);
    { // 行头
      const ImRect rr(ImVec2(h.box.Min.x + kLgPad.x, y), ImVec2(h.box.Min.x + kLgPad.x + w_head, y + th));
      bool hov = false;
      if (lg_button(rr, "##r", hov))
        lg_flip(R, T, r, -1, item_at);
      if (hov)
        hover(r, -1);
      lf.text(dl, rr.Min, hov ? c_hov : any(r, -1) ? rows[static_cast<size_t>(r)].col
                                                   : c_dis,
              rows[static_cast<size_t>(r)].name);
    }
    for (int c = 0; c < C; ++c) {
      const size_t k = static_cast<size_t>(c);
      const LgCell &e = cell(r, c);
      bool on = cols[k].toggle < 0 && any(r, -1);
      if (const int t = cols[k].toggle; t >= 0) { // 整格可点
        ImPlotItem *it = item_at(r, t);
        ImGui::PushID(c);
        bool hov = false;
        if (lg_button(ImRect(ImVec2(xc[k], y), ImVec2(xc[k] + w[k], y + th)), "##t", hov))
          it->Show = !it->Show;
        ImGui::PopID();
        if (hov)
          hover(r, t);
        const ImVec2 s0(bare[k] ? xc[k] + (w[k] - sw) * 0.5f : xc[k], y + (th - sw) * 0.5f), s1(s0.x + sw, s0.y + sw);
        if (!it->Show) {
          dl.AddRect(s0, s1, hov ? c_txt : c_dis);
        } else {
          ImVec4 a = e.sw[0], b = e.sw[1];
          a.w *= hov ? 0.75f : 1.f, b.w *= hov ? 0.75f : 1.f;
          const ImU32 ua = ImGui::ColorConvertFloat4ToU32(a), ub = ImGui::ColorConvertFloat4ToU32(b);
          dl.AddRectFilledMultiColor(s0, s1, ua, ub, ub, ua);
        }
        on = it->Show;
      }
      if (!e.text.empty())
        lf.text(dl, ImVec2(xc[k] + w[k] - lf.w(e.text.c_str()), y), e.col ? e.col : on ? c_txt
                                                                                       : c_dis,
                e.text.c_str());
    }
    ImGui::PopID();
  }
  ImPlot::PopPlotClipRect();
  ImGui::PopID();
  if (h.on)
    h.row = h.r;
  else if (const int i = static_cast<int>(std::floor((ImGui::GetMousePos().y - y_row(0)) / th)); bg && i >= 0 && i < R)
    h.row = i;
  return h;
}
// 图例格的数: v 不可算 → "-"
std::string lg_num(const char *fmt, double v) {
  if (!std::isfinite(v))
    return "-";
  char s[24];
  std::snprintf(s, sizeof(s), fmt, v);
  return s;
}

// 档 s (kSpanDefs) 的区间起点 (dates 非空): 推到的那日之后的首个交易日 (起点前收 = 那日及之前的末个交易日); 日轴不够长 → 0, 至多末日
int span_d0(const std::vector<misc::Ymd> &dates, const SpanDef &s) {
  namespace ch = std::chrono;
  ch::year_month_day y{misc::to_days(misc::add_days(dates.back(), -s.days))};
  y -= ch::months{s.months};
  if (!y.ok())
    y = y.year() / y.month() / ch::last;
  const misc::Ymd from = misc::from_days(ch::sys_days{y});
  return std::min(static_cast<int>(std::ranges::upper_bound(dates, from) - dates.begin()), static_cast<int>(dates.size()) - 1);
}
// 档位条: 当前区间正是某档 → 该钮按下
void span_bar(LinesRange &rg, const std::vector<misc::Ymd> &dates) {
  const int D = static_cast<int>(dates.size());
  ImGui::BeginDisabled(D == 0);
  for (const SpanDef &s : kSpanDefs) {
    if (&s != kSpanDefs)
      ImGui::SameLine(0, 2);
    const int d0 = D > 0 ? span_d0(dates, s) : 0;
    if (toggle_button(s.name, D > 0 && rg.d0 == d0 && rg.d1 == D))
      rg.d0 = d0, rg.d1 = D;
    if (D > 0 && ImGui::IsItemHovered())
      tip("区间 = 近%s: %s ~ %s (区间条同步; 双击区间条段内回全段)", s.name, misc::dash(dates[d0]).c_str(), misc::dash(dates.back()).c_str());
  }
  ImGui::EndDisabled();
}

// n 点线 (pt(i) = 第 i 点, x 升序) 视野内末点; 无 (或不可算) → false
template <class Pt>
bool end_point(const ImPlotRect &lim, size_t n, const Pt &pt, ImPlotPoint &p) {
  const auto r = std::views::iota(size_t{0}, n);
  const size_t e = static_cast<size_t>(std::ranges::partition_point(r, [&](size_t i) { return pt(i).x <= lim.X.Max; }) - r.begin());
  if (e == 0)
    return false;
  p = pt(e - 1);
  return p.x >= lim.X.Min && std::isfinite(p.y);
}
template <class Pt>
void push_tag(std::vector<EndTag> &tags, const ImPlotRect &lim, size_t n, const Pt &pt, ImU32 col, std::string text) {
  ImPlotPoint p;
  if (end_point(lim, n, pt, p))
    tags.push_back({ImPlot::PlotToPixels(p), col, std::move(text)});
}

ImPlotPoint slice_pt(const LinesSlice &s, size_t i) { return {s.x(i), s.y(i)}; }
// 线的日末视图点 (PlotLineG; p = Line, 共 e.size() 点): 0 = 起点 (前收), i = 活跃日 f + i − 1 的末点 (x = 该日末根)
ImPlotPoint day_end_pt(int i, void *p) {
  const Line &c = *static_cast<const Line *>(p);
  return i == 0 ? ImPlotPoint(c.x[0], c.y[0]) : ImPlotPoint(x_of(static_cast<size_t>(c.f + i - 1), kClose), c.e[static_cast<size_t>(i)]);
}
void push_tag(std::vector<EndTag> &tags, const ImPlotRect &lim, const LinesSlice &s, ImU32 col, std::string text) {
  push_tag(tags, lim, s.size(), [&](size_t i) { return slice_pt(s, i); }, col, std::move(text));
}
constexpr float kTagScale = 0.95f; // 线尾小字 / 刻度字号 (× 字体)
// 线尾小字右缘 (图区右壁内 2 px)
float tags_right() { return ImPlot::GetPlotPos().x + ImPlot::GetPlotSize().x - 2.0f; }
constexpr float kMarkTick = 6.0f; // 刻度的短横长 (px)
// 刻度占的像素宽 (短横 + 字; 无 → 0)
float marks_width(const std::vector<EndTag> &marks) { return marks.empty() ? 0.0f : end_tags_width(marks, kTagScale) + kMarkTick; }
// 刻度: 真 y 处短横贴点右侧 (同线尾小字, 出界则左移进 right 内), 字左对齐接在短横后, 按像素 y 排开不重叠
void draw_marks(std::vector<EndTag> &marks, float right) {
  if (marks.empty())
    return;
  std::sort(marks.begin(), marks.end(), [](const EndTag &a, const EndTag &b) { return a.at.y < b.at.y; });
  ImFont *font = ImGui::GetFont();
  const float fs = ImGui::GetFontSize() * kTagScale;
  ImDrawList *dl = ImPlot::GetPlotDrawList();
  ImPlot::PushPlotClipRect();
  float y = -FLT_MAX;
  for (const EndTag &m : marks) {
    const ImVec2 sz = font->CalcTextSizeA(fs, FLT_MAX, 0.0f, m.text.c_str());
    const float x0 = std::min(m.at.x + kTagGap, right - marks_width(marks) + kTagGap), x1 = x0 + kMarkTick; // 短横 [x0, x1); 字左缘 = x1 + kTagGap
    dl->AddLine(ImVec2(x0, m.at.y), ImVec2(x1, m.at.y), m.col);
    y = std::max(m.at.y, y + fs);
    dl->AddText(font, fs, ImVec2(x1 + kTagGap, y - 0.5f * sz.y), m.col, m.text.c_str());
  }
  ImPlot::PopPlotClipRect();
}

// 主图一帧的公共件 (NAV / VAL 同一份): 小字筛选视野 + 线尾小字 + 线尾刻度 (VAL) + 显示的线 (只一条 → 现价十字) + 显隐签名 (下帧进 fit 键)
struct PlotFrame {
  ImPlotRect lim;
  std::vector<EndTag> tags;
  std::vector<EndTag> marks;
  std::vector<const LinesSlice *> lines;
  std::string shown;
  uint64_t view = 0; // 视图 gen (切片的身份随它)
  // 图例项 label 的显隐记进 shown; 显示 → 该项 (Color = 实际线色), 否则 nullptr
  const ImPlotItem *item(const char *label) {
    const ImPlotItem *it = ImPlot::GetItem(label);
    assert(it);
    shown += it->Show ? '1' : '0';
    return it->Show ? it : nullptr;
  }
};

// ---- 切片画线: 按像素列抽稀 (每列留 首 / 低 / 高 / 末, 与逐点画逐像素一致; 分钟线全史数十万点), 一列至多 4 点的段即原样;
//      x 段 = 视野 (fit 帧 = 切片全段: 极值都在, fit 不偏), 段外各留一点接线. 抽好的点按 label 留到下帧, (视图, 切片, x 段, 图宽) 同 → 复用 ----
struct Thinned {
  uint64_t view = 0;
  const LinesSlice *s = nullptr;
  double x0 = 0, x1 = 0;
  float w = 0;
  int frame = 0; // 末次画到的帧
  std::vector<double> x, y;
};
std::map<std::string, Thinned> s_thin;
void thin(const LinesSlice &s, double x0, double x1, float w, Thinned &t) {
  t.x.clear(), t.y.clear();
  const size_t n = s.size();
  const auto r = std::views::iota(size_t{0}, n);
  size_t i = static_cast<size_t>(std::ranges::partition_point(r, [&](size_t k) { return s.x(k) < x0; }) - r.begin());
  size_t e = static_cast<size_t>(std::ranges::partition_point(r, [&](size_t k) { return s.x(k) <= x1; }) - r.begin());
  i = i > 0 ? i - 1 : 0, e = std::min(e + 1, n);
  const double bw = x1 > x0 ? (x1 - x0) / static_cast<double>(w) : 0.0;
  while (i < e) {
    size_t j = i + 1, lo = i, hi = i;
    if (bw > 0)
      for (const double b = std::floor((s.x(i) - x0) / bw); j < e && std::floor((s.x(j) - x0) / bw) == b; ++j) {
        if (s.y(j) < s.y(lo))
          lo = j;
        if (s.y(j) > s.y(hi))
          hi = j;
      }
    std::array<size_t, 4> k{i, lo, hi, j - 1};
    std::sort(k.begin(), k.end());
    for (size_t q = 0; q < k.size(); ++q)
      if (q == 0 || k[q] != k[q - 1])
        t.x.push_back(s.x(k[q])), t.y.push_back(s.y(k[q]));
    i = j;
  }
}
void plot_slice(const PlotFrame &pf, const char *label, const LinesSlice &s, ImPlotLineFlags flags = 0) {
  const bool fit = fit_x() && s.size() > 0;
  const double x0 = fit ? s.x(0) : pf.lim.X.Min, x1 = fit ? s.x(s.size() - 1) : pf.lim.X.Max;
  const float w = std::max(1.0f, ImPlot::GetPlotSize().x);
  Thinned &t = s_thin[label];
  if (t.view != pf.view || t.s != &s || t.x0 != x0 || t.x1 != x1 || t.w != w) {
    t.view = pf.view, t.s = &s, t.x0 = x0, t.x1 = x1, t.w = w;
    thin(s, x0, x1, w, t);
  }
  t.frame = ImGui::GetFrameCount();
  ImPlot::PlotLine(label, t.x.data(), t.y.data(), static_cast<int>(t.x.size()), flags);
}
// 本帧没画到的 label 的抽稀点丢掉
void thin_prune() {
  std::erase_if(s_thin, [](const auto &kv) { return kv.second.frame != ImGui::GetFrameCount(); });
}
// 现价十字 (同日内栏): 只显示一条线时, 其视野内末点; 读数 = 区间涨跌 (切片起点 = 1), 时刻 = 该点所在日. 无 → false
bool now_cross(const PlotFrame &pf, const std::vector<misc::Ymd> &dates, NowCross &c) {
  if (pf.lines.size() != 1)
    return false;
  const LinesSlice &s = *pf.lines.front();
  ImPlotPoint p;
  if (!end_point(pf.lim, s.size(), [&](size_t i) { return slice_pt(s, i); }, p))
    return false;
  char y[24];
  std::snprintf(y, sizeof(y), "%+.2f%%", 100.0 * (p.y - 1.0));
  const misc::Ymd day = dates[std::min(static_cast<size_t>(p.x) / kDayW, dates.size() - 1)];
  c = {ImPlot::PlotToPixels(p), ImGui::ColorConvertFloat4ToU32(tint(p.y, 1.0)), y, misc::dash(day)};
  return true;
}

// ---- NAV 主图例 (legend): 行 = 选中行 (名 = 线色), 列 = 可成交 | 夏普 | 理想 | 夏普 | 年换手 (因子 / 策略行的两条 = 有冲击 / 无冲击;
//      年化 / 夏普按线随区间, 年换手按行); 线格 = 开关 + 区间年化; 悬停一行 = 联动 (ui.hover), 名格另出行的悬停提示; 返回矩阵框 ----
struct NavRow {
  int g;
  const LinesRow *row;
  const LinesSlice *s[kPairLines];
  ImVec4 col[kPairLines];
  std::string lb[kPairLines]; // 各线 item label (须有可显示的名, 同 val_item; ###线 ID 定 ID)
};
ImRect nav_legend(LinesState &ui, const std::vector<NavRow> &rows) {
  static constexpr LgCol kCols[] = {{kPairName[0][kTrade], kTrade}, {"夏普"}, {kPairName[0][kIdeal], kIdeal}, {"夏普"}, {"年换手"}};
  std::vector<LgRow> lr;
  std::vector<LgCell> cells;
  for (const NavRow &r : rows) {
    lr.push_back({r.row->cn.c_str(), ImGui::ColorConvertFloat4ToU32(r.col[kTrade])});
    for (int l = 0; l < kPairLines; ++l) {
      cells.push_back({lg_num("%+.1f%%", r.s[l]->ann), 0, {r.col[l], r.col[l]}});
      cells.push_back({lg_num("%+.2f", r.s[l]->sharpe)});
    }
    cells.push_back({lg_num("%.1f倍", r.row->line[kTrade]->turn)});
  }
  const LgHover h = legend("##nav_lg", kCols, lr, cells, [&](int i, int l) {
    ImPlotItem *it = ImPlot::GetItem(rows[static_cast<size_t>(i)].lb[l].c_str()); // 本帧刚 Plot 过, 必在池里
    assert(it);
    return it;
  });
  if (h.on && h.r < 0)
    tip("%s线 (因子 / 策略行 = %s): 色块 = 显隐, 数 = 区间年化\n点 = 整列开关", kPairName[0][h.t], kPairName[1][h.t]);
  else if (h.on && h.t < 0)
    row_tip(*rows[static_cast<size_t>(h.r)].row);
  else if (h.on)
    tip("%s %s: 点 = 显 / 隐", rows[static_cast<size_t>(h.r)].row->cn.c_str(), kPairName[pair_kind(*rows[static_cast<size_t>(h.r)].row)][h.t]);
  if (h.row >= 0)
    ui.hover.plot = rows[static_cast<size_t>(h.row)].row->id;
  return h.box;
}

// ---- NAV 主图: 画选中行的两条线, 按组表画层 (for_each_layer; 策略在最上层) + 图例矩阵 (最上层, nav_legend);
//      日内形未到的行 (minute_wait) 整行不画、不进图例, 到了一次画成分钟线;
//      行刚选中 → ui.reveal 强制回默认显隐一次 (理想列显, 可成交列隐);
//      悬停联动 (ui.hover): 显示的线里鼠标处最近的一条 → 其行 (+ 行的悬停提示; 鼠标在图例矩阵上不算), hot 行显示的线白粗线画在最上 ----
void plot_nav(LinesState &ui, const LinesDerived &d, const LinesView &v, PlotFrame &pf) {
  static ImRect s_box;      // 上帧图例矩阵 (悬停找线避开)
  std::vector<NavRow> rows; // 画序, 只含选中行的
  for_each_layer([&](int g) {
    for (size_t r = 0; r < d.rows[g].size(); ++r)
      if (const LinesRow &row = d.rows[g][r]; picked(ui, kPageNav, row) && !d.minute_wait(row)) {
        NavRow x{g, &row, {}, {}, {}};
        for (int l = 0; l < kPairLines; ++l) {
          x.s[l] = &v.slice[g][r][l];
          x.col[l] = row_color(d, g, r, l);
          x.lb[l] = row.cn + " " + kPairName[pair_kind(row)][l] + "###" + line_id(row, l);
        }
        rows.push_back(std::move(x));
      }
  });
  struct On {
    const LinesRow *row;
    const LinesSlice *s;
  };
  std::vector<On> on; // 显示的线
  for (const NavRow &x : rows)
    for (int l = 0; l < kPairLines; ++l) {
      const char *lb = x.lb[l].c_str();
      if (ui.reveal.erase(line_id(*x.row, l)))
        ImPlot::HideNextItem(l == kTrade, ImPlotCond_Always);
      ImPlot::SetNextLineStyle(x.col[l], row_width(x.g));
      plot_slice(pf, lb, *x.s[l]);
      if (pf.item(lb)) {
        push_tag(pf.tags, pf.lim, *x.s[l], ImGui::ColorConvertFloat4ToU32(x.col[l]), x.row->tag + kPairTag[pair_kind(*x.row)][l]);
        pf.lines.push_back(x.s[l]);
        on.push_back({x.row, x.s[l]});
      }
    }
  const int k = s_box.Contains(ImGui::GetMousePos()) ? -1 : near_line(on.size(), [&](size_t j, double x) {
    const LinesSlice &s = *on[j].s;
    return line_at(s.size(), x, [&](size_t i) { return slice_pt(s, i); });
  });
  if (k >= 0) {
    const LinesRow &r = *on[static_cast<size_t>(k)].row;
    ui.hover.plot = r.id;
    row_tip(r);
  }
  const std::optional<std::string> &hot = ui.hover.hot();
  for (size_t j = 0; j < on.size(); ++j)
    if (on[j].row->id == hot) {
      char id[24];
      std::snprintf(id, sizeof(id), "##pf_hot%zu", j);
      ImPlot::SetNextLineStyle(kHotCol, kHotW);
      plot_slice(pf, id, *on[j].s, ImPlotItemFlags_NoLegend | ImPlotItemFlags_NoFit);
    }
  s_box = nav_legend(ui, rows);
}

// ---- VAL 主图例 (legend; 对仗 Inspect 期限结构图例): 行 = 口 (EP / BP / SP / CP / DY), 列 = 当前收益率 | 当前倍数 | N 年估值分位 | Band | 等值线;
//      分位按便宜绿 → 贵红着色; Band 色块 = band 渐变; 悬停格 / 头 → 对应线高亮 (下帧) ----
enum ValLgCol : int { kLgBand,
                      kLgContour };
constexpr float kHiScale = 2.0f; // 图例悬停高亮: 线宽倍数 (等值线改用 kContourHotA)
constexpr float kContourHotA = 0.6f;
// 开关列 c 口 b 的 item label (Band / 等值线). 须有可显示的名: ImPlot 对无名 item 每帧强制 Show = true, 开关不住; 图本身 NoLegend, 名不显示
std::string val_item(int c, int b) {
  return std::string(kVal[b].name) + (c == kLgBand ? " Band###pf_band" : " 等值线###pf_contour") + std::to_string(b);
}

// 返回悬停 (r = 口, t = ValLgCol) + 框 (悬停等值线避开)
LgHover val_legend(const LinesView &v, int years) {
  char yrs[24];
  std::snprintf(yrs, sizeof(yrs), "%d年分位", years);
  const LgCol cols[] = {{"当前"}, {"倍数"}, {yrs}, {"Band", kLgBand}, {"等值线", kLgContour}};
  const ImU32 c_txt = ImGui::GetColorU32(ImGuiCol_Text), c_dis = ImGui::GetColorU32(ImGuiCol_TextDisabled);
  const ImVec4 contour(kContourCol.x, kContourCol.y, kContourCol.z, 0.8f);
  std::vector<LgRow> lr;
  std::vector<LgCell> cells;
  for (int b = 0; b < kPoolVals; ++b) {
    const LinesBand &bd = v.band[b];
    char a[24] = "-", m[24] = "-", p[16] = "-";
    if (bd.ok) {
      fmt_val(b, bd.now, a, sizeof(a));
      if (bd.now > 0.0)
        std::snprintf(m, sizeof(m), "%s %.2f", kVal[b].inv, 1.0 / bd.now);
      std::snprintf(p, sizeof(p), "%3.0f%%", bd.pct);
    }
    const ImU32 ink = bd.ok ? c_txt : c_dis;
    lr.push_back({kVal[b].name, c_txt});
    cells.push_back({a, ink});
    cells.push_back({m, ink});
    cells.push_back({p, bd.ok ? ImGui::ColorConvertFloat4ToU32(band_col(bd.pct / 100.0, 1.0f)) : c_dis});
    cells.push_back({{}, 0, {band_col(0.0, 1.0f), band_col(1.0, 1.0f)}});
    cells.push_back({{}, 0, {contour, contour}});
  }
  const LgHover h = legend("##pf_val_lg", cols, lr, cells, [](int b, int c) {
    ImPlotItem *it = ImPlot::GetItem(val_item(c, b).c_str()); // 本帧刚 Plot 过, 必在池里
    assert(it);
    return it;
  });
  if (h.on && h.r >= 0 && h.t >= 0) {
    const int b = h.r;
    const std::vector<double> &lv = v.band[b].levels;
    if (h.t == kLgBand)
      tip("%s Band: 过去 %d 年估值分位换算的价格线 (点 = 开关)", kVal[b].name, years);
    else if (!lv.empty())
      tip("%s 等值线: %s %.2f ~ %.2f 倍 %d 等分 (该标的全史倍数区间, 两头各剔 %.0f%%)\n点 = 开关; 悬停主图里的线看该档", kVal[b].name, kVal[b].inv, lv.front(),
          lv.back(), kContours - 1, 100.0 * kContourTrim);
    else
      tip("%s 等值线: 全史不可算 (无收益率 > 0 的日子)", kVal[b].name);
  }
  return h;
}

// VAL 悬停等值线: 鼠标 x 处 (相邻两点插值) 各开着的口里离鼠标最近的一档, kContourHit px 内 → 高亮 (最上层) + 提示数据; 鼠标在图例矩阵上不算
void hover_contour(const LinesDerived &d, const LinesView &v, const bool *on, const ImRect &legend) {
  const double *bx = v.band_x.data();
  const size_t n = v.band_x.size();
  if (!ImPlot::IsPlotHovered() || legend.Contains(ImGui::GetMousePos()))
    return;
  const ImPlotPoint mp = ImPlot::GetPlotMousePos();
  const size_t j = static_cast<size_t>(std::upper_bound(bx, bx + n, mp.x) - bx);
  if (j == 0 || j >= n)
    return;
  const double t = (mp.x - bx[j - 1]) / (bx[j] - bx[j - 1]);
  const auto lerp = [&](double a, double c) { return a + (c - a) * t; };
  const float my = ImGui::GetMousePos().y;
  int hb = -1;
  double m = 0.0, hu = 0.0;
  float best = kContourHit;
  for (int b = 0; b < kPoolVals; ++b) {
    const std::vector<double> &u = v.band[b].unit;
    if (!on[b] || !std::isfinite(u[j - 1]) || !std::isfinite(u[j]))
      continue;
    const double uu = lerp(u[j - 1], u[j]);
    for (const double lv : v.band[b].levels)
      if (const float dy = std::abs(ImPlot::PlotToPixels(mp.x, uu * lv).y - my); dy < best)
        best = dy, hb = b, m = lv, hu = uu;
  }
  if (hb < 0)
    return;
  const double px = lerp(v.band_p[j - 1], v.band_p[j]), now = px / hu;
  ContourLine l{bx, v.band[hb].unit.data(), m};
  ImPlot::SetNextLineStyle(kContourHotCol, 1.5f);
  ImPlot::PlotLineG("##pf_contour_hot", contour_pt, &l, static_cast<int>(n), ImPlotItemFlags_NoFit | ImPlotItemFlags_NoLegend);
  const misc::Ymd day = d.dates[std::min(static_cast<size_t>(bx[t < 0.5 ? j - 1 : j]) / kDayW, d.dates.size() - 1)];
  const ValMeta &vm = kVal[hb];
  char y_line[24], y_now[24];
  fmt_val(hb, 1.0 / m, y_line, sizeof(y_line));
  fmt_val(hb, 1.0 / now, y_now, sizeof(y_now));
  tip("%s 等值线: %s = %.2f 倍 (%s %s)\n"
      "• %d/%02d/%02d: 线上点位 %.3f, 指数 %.3f (%s %.2f 倍, %s %s)\n"
      "• 指数到该线 %+.1f%%",
      vm.name, vm.inv, m, vm.name, y_line, day / 10000, day / 100 % 100, day % 100, hu * m, px, vm.inv, now, vm.name, y_now, 100.0 * (m / now - 1.0));
}

// VAL 横向框选 (XZoom): 左键拖出一段 x → 松手 = 区间改成该段覆盖的日 [d0, d1) (区间条同一份, 主图随之贴合); 双击 = 区间回全段; 图例矩阵上不起
void zoom_x(LinesRange &rg, int D, const ImRect &legend) {
  assert(D > 0);
  const double x = plot_mouse_x();
  if (ImPlot::IsPlotHovered() && !legend.Contains(ImGui::GetMousePos())) {
    if (ImGui::IsMouseDoubleClicked(ImGuiMouseButton_Left)) {
      rg.d0 = 0, rg.d1 = D;
      rg.zoom.cancel();
      return;
    }
    if (ImGui::IsMouseClicked(ImGuiMouseButton_Left))
      rg.zoom.begin(x);
  }
  double lo, hi;
  if (!rg.zoom.track(x, lo, hi))
    return;
  const auto day = [](double v) { return static_cast<int>(std::floor(v / static_cast<double>(kDayW))); };
  rg.d0 = std::clamp(day(lo), 0, D - 1);
  rg.d1 = std::clamp(day(hi) + 1, rg.d0 + 1, D);
}

// 滚轮缩放 (NAV / VAL 同一份; 主图 x 恒 = 区间, ImPlot 自带的缩放不起): 右端钉在区间末 d1 (默认 = 末日), 只挪左端;
// 一格 = 日数 × (1 + kWheelRate)^∓1 (上滚 = 放大), 至少挪一日, 至少留一日; 改的是区间本身 ⇒ 区间条 / 档位条同步
constexpr double kWheelRate = 0.05;
void wheel_x(LinesRange &rg) {
  if (!ImPlot::IsPlotHovered())
    return;
  ImGui::SetKeyOwner(ImGuiKey_MouseWheelY, ImPlot::GetCurrentPlot()->ID); // 占住滚轮 (不滚外层窗口)
  const float w = ImGui::GetIO().MouseWheel;
  if (w == 0.0f || rg.d1 <= 0)
    return;
  const int n = rg.d1 - rg.d0;
  int m = static_cast<int>(std::lround(n * std::pow(1.0 + kWheelRate, -w)));
  if (m == n)
    m += w > 0 ? -1 : 1;
  rg.d0 = rg.d1 - std::clamp(m, 1, rg.d1);
}

// ---- VAL 主图: 标的各口滚动 band (日末点; 填色 + kBandQ 条线共一个 item, 一键开关; 首次默认只开 BP)
//      + 各口绝对估值等值线 (同口各档共一个 item, 首次默认关) + 标的理想线 (常显; 有分钟价则画到分钟) + 悬停等值线高亮 + 图例矩阵 (最上层) + 横向框选;
//      标的日内形未到 (minute_wait) 整图不画, 到了一次画成;
//      价格线 + 开着的 band 线进 fit, band 填色 (到图顶 / 底) / 等值线不进; 框选改 rg ----
void plot_val(LinesState &ui, LinesRange &rg, const LinesDerived &d, const LinesView &v, const LinesAt &pin, PlotFrame &pf) {
  static LgHover s_hl; // 上帧图例矩阵
  const LinesSlice &sl = v.slice[pin.g][pin.r][kIdeal];
  const double *bx = v.band_x.data();
  const int bn = static_cast<int>(v.band_x.size());
  for (int b = 0; b < kPoolVals; ++b) {
    const LinesBand &bd = v.band[b];
    const std::string lbs = val_item(kLgBand, b);
    const char *lb = lbs.c_str();
    const float hw = s_hl.at(b, kLgBand) ? kHiScale : 1.0f;
    if (b != kValBp)
      ImPlot::HideNextItem(true, ImPlotCond_Once);
    // 色块 c ∈ [0, kBandQ]: 0 = 最便宜档以下 (到图底), kBandQ = 最贵档以上 (到图顶), 其余 = 相邻两档之间
    for (const auto &[i0, i1] : bd.segs) {
      const int n = i1 - i0;
      const double *x = bx + i0;
      for (int c = 0; c <= kBandQ; ++c) {
        ImPlot::SetNextFillStyle(band_col(static_cast<double>(c) / kBandQ, 1.0f), kBandFillA);
        if (c == 0)
          ImPlot::PlotShaded(lb, x, bd.y[0].data() + i0, n, -HUGE_VAL, ImPlotItemFlags_NoFit);
        else if (c == kBandQ)
          ImPlot::PlotShaded(lb, x, bd.y[kBandQ - 1].data() + i0, n, HUGE_VAL, ImPlotItemFlags_NoFit);
        else
          ImPlot::PlotShaded(lb, x, bd.y[c - 1].data() + i0, bd.y[c].data() + i0, n, ImPlotItemFlags_NoFit);
      }
    }
    for (int j = 0; j < kBandQ; ++j) {
      ImPlot::SetNextLineStyle(band_col((j + 0.5) / kBandQ, kBandLineA), (j == kBandQ / 2 ? 1.5f : 1.0f) * hw);
      ImPlot::PlotLine(lb, bx, bd.y[j].data(), bn, ImPlotLineFlags_SkipNaN);
    }
  }
  // 等值线: 每档一条 (同一 item; 无档也登记)
  bool on[kPoolVals];
  for (int b = 0; b < kPoolVals; ++b) {
    const std::vector<double> &lv = v.band[b].levels;
    const std::string lbs = val_item(kLgContour, b);
    const char *lb = lbs.c_str();
    const ImVec4 col(kContourCol.x, kContourCol.y, kContourCol.z, s_hl.at(b, kLgContour) ? kContourHotA : kContourCol.w);
    ImPlot::HideNextItem(true, ImPlotCond_Once);
    if (lv.empty())
      ImPlot::PlotLineG(lb, contour_pt, nullptr, 0, ImPlotItemFlags_NoFit);
    for (const double m : lv) {
      ContourLine l{bx, v.band[b].unit.data(), m};
      ImPlot::SetNextLineStyle(col, 1.0f);
      ImPlot::PlotLineG(lb, contour_pt, &l, bn, ImPlotItemFlags_NoFit);
    }
    const ImPlotItem *it = ImPlot::GetItem(lb);
    assert(it);
    on[b] = it->Show;
  }
  ImPlot::SetNextLineStyle(row_color(d, pin, kTrade), 2.0f);
  plot_slice(pf, "##pf_val_px", sl);
  if (const ImPlotItem *it = pf.item("##pf_val_px")) {
    push_tag(pf.tags, pf.lim, sl, it->Color, d.row(pin).tag);
    pf.lines.push_back(&sl);
    ImPlotPoint p;
    if (end_point(pf.lim, sl.size(), [&](size_t i) { return slice_pt(sl, i); }, p))
      for (const ValMark &m : kValMarks) {
        char t[16];
        std::snprintf(t, sizeof(t), "%+.0f%%", 100.0 * m.r);
        pf.marks.push_back({ImPlot::PlotToPixels(p.x, p.y * (1.0 + m.r)), ImGui::ColorConvertFloat4ToU32(band_col(m.u, kBandTagA)), t});
      }
  }
  hover_contour(d, v, on, s_hl.box);
  s_hl = val_legend(v, ui.roll.key.years);
  zoom_x(rg, static_cast<int>(d.dates.size()), s_hl.box);
}

} // namespace

LinesRange DefaultLinesRange(int page, const std::vector<misc::Ymd> &dates) {
  const int D = static_cast<int>(dates.size());
  const SpanId s = kPageDefs[page].span;
  return LinesRange{{.d0 = D > 0 && s != kSpanAll ? span_d0(dates, kSpanDefs[s]) : 0, .d1 = D}};
}

// ---- 主图: NAV (plot_nav) ↔ VAL (plot_val) 同一框: 贴边 (窄内边距, 不标轴名); y 恒贴合 + 留白 (fit_margin);
//      x: 恒贴合 (视野只随区间; 滚轮 = 钉住右端缩放区间, wheel_x); x 右端留出线尾小字 / 刻度 / 十字标签;
//      左上角 = 区间档位条 (span_bar) + 其下图例矩阵 ----
void RenderLinesPlot(LinesState &ui, int page, const LinesAt &pin, const ImVec2 &size) {
  const LinesDerived &d = ui.der.val;
  const LinesView &v = ui.view.val;
  const bool nav = page == kPageNav;
  LinesRange &range = ui.range[page];
  const LinesRange rg = range;                                    // 视图所切的区间 (VAL 框选本帧改 range, 下帧才进视图)
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(2, 2)); // 贴边: 图区让给线
  ImPlot::PushStyleVar(ImPlotStyleVar_LabelPadding, ImVec2(2, 1));
  LinesFitKey fk{d.hist_gen, ui.roll.gen, ui.pct.gen, pin, rg.d0, rg.d1, nav, ui.log_y, {}, ui.shown_key};
  bool wait = !nav && pin.g >= 0 && d.minute_wait(d.row(pin)); // 有线在等日内形
  if (nav)
    for (const std::vector<LinesRow> &rs : d.rows)
      for (const LinesRow &r : rs) {
        const bool p = picked(ui, kPageNav, r);
        fk.rows += p ? '1' : '0';
        wait = wait || (p && d.minute_wait(r));
      }
  if (fk != ui.fit_key)
    ImPlot::SetNextAxesToFit();
  if (ImPlot::BeginPlot(nav ? "##portfolio" : "##pf_val", size, ImPlotFlags_NoLegend | ImPlotFlags_NoBoxSelect)) {
    ui.fit_key = fk;
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit | ImPlotAxisFlags_RangeFit);
    setup_day_axis(d.dates);
    if (ui.log_y)
      ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    // x fit 帧的视野还是上帧的 (区间刚变时可能整段在外): 小字不按视野筛, 全收来量留白宽度
    const ImPlotRect cur = ImPlot::GetPlotLimits(); // 先锁 setup, FitThisFrame 才定
    PlotFrame pf{fit_x() ? ImPlotRect(-DBL_MAX, DBL_MAX, -DBL_MAX, DBL_MAX) : cur, {}, {}, {}, {}, ui.view.gen};
    if (nav)
      plot_nav(ui, d, v, pf);
    else if (pin.g >= 0 && !wait)
      plot_val(ui, range, d, v, pin, pf);
    wheel_x(range);
    if (wait)
      plot_hint("分钟线载入中…");
    fit_margin(ui.log_y);
    thin_prune();
    ui.shown_key = std::move(pf.shown);
    NowCross cross{};
    const bool crossed = now_cross(pf, d.dates, cross);
    const float cw = crossed ? now_cross_width(cross, kTagScale) + kTagGap : 0.0f; // 十字右壁标签占的宽 (小字 / 刻度让开)
    // 本帧 x fit (自动 / 双击): x 右端再扩一段, 让区间末点右侧留出线尾小字 / 刻度 (同贴末点, 取宽者) + 十字标签的宽度
    if (fit_x() && !pf.tags.empty() && rg.d1 > rg.d0) {
      const float tw = std::max(end_tags_width(pf.tags, kTagScale), marks_width(pf.marks)) + cw, W = ImPlot::GetPlotSize().x;
      const double x0 = x_of(static_cast<size_t>(rg.d0), kOpen), x1 = x_of(static_cast<size_t>(rg.d1), kOpen);
      if (W > 2.0f * tw)
        ImPlot::FitPointX(x1 + (x1 - x0) * tw / (W - tw));
    }
    draw_end_tags(pf.tags, kTagScale, tags_right() - cw);
    draw_marks(pf.marks, tags_right() - cw);
    if (crossed)
      draw_now_cross(cross, kTagScale, tags_right());
    const ImVec2 corner = ImPlot::GetPlotPos();
    ImPlot::EndPlot();
    const ImVec2 next = ImGui::GetCursorScreenPos(); // 档位条叠在图上 (图之后提交, 抢得到悬停), 画完光标回图下
    ImGui::SetCursorScreenPos(ImVec2(corner.x + kBarPad, corner.y + kBarPad));
    span_bar(range, d.dates);
    ImGui::SetCursorScreenPos(next);
  }
  ImPlot::PopStyleVar(2);
}

// ---- 区间条 (range_bar): 底线 = 全段组合 (有冲击) / VAL 标的理想线 (价格指数 / 后复权收盘), 只画日末视图; VAL 主图横向框选同改它 (任意日) ----
void RenderLinesRange(LinesState &ui, int page, const LinesAt &pin, const ImVec2 &size) {
  const LinesDerived &d = ui.der.val;
  const bool nav = page == kPageNav;
  const int D = static_cast<int>(d.dates.size());
  if (!ImPlot::BeginPlot("##pf_range", size,
                         ImPlotFlags_NoTitle | ImPlotFlags_NoLegend | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoMouseText))
    return;
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_None, ImPlotAxisFlags_NoDecorations | ImPlotAxisFlags_AutoFit);
  if (D > 0)
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, x_of(d.dates.size(), kOpen), ImPlotCond_Always);
  setup_day_axis(d.dates);
  if (ui.log_y)
    ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
  const Line none;
  const Line &rc = nav ? *d.rows[kGroupStrat][0].line[kTrade] : pin.g >= 0 ? *d.row(pin).line[kIdeal]
                                                                           : none;
  ImPlot::SetNextLineStyle(nav || pin.g < 0 ? kStratCol : row_color(d, pin, kTrade), 1.0f);
  ImPlot::PlotLineG("##pf_range_line", day_end_pt, const_cast<Line *>(&rc), static_cast<int>(rc.e.size()));
  range_bar(ui.range[page], d.dates, static_cast<double>(kDayW));
  ImPlot::EndPlot();
}

} // namespace GUI::Market