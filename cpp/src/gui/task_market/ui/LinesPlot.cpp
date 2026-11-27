// Market 行情页主图 (NAV 资金曲线 ↔ VAL 估值 / MARGIN 两融 band) + 区间条 — 见 LinesCommon.hpp
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
#include <map>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <vector>

namespace GUI::Market {

namespace {

// 主图横轴 = 日并集轴 (x = 日下标 · kDayW + 槽; 读数只到日): 日界 = 首根前半格 (前收点位所在)
DayAxis day_axis(const std::vector<misc::Ymd> &dates) { return {dates, static_cast<double>(kDayW), -0.5}; }

// 主图 y 恒贴合视野内进 fit 的线 (AutoFit | RangeFit), 上下各留跨度 × kFitMargin (log 轴按对数跨度)
constexpr double kFitMargin = 0.2;
// 本帧 x 在 fit (切片画全段 / 小字不按视野筛)
bool fit_x() { return ImPlot::GetCurrentPlot()->Axes[ImAxis_X1].FitThisFrame; }
// y 轴 y 的留白: 须在本帧进 fit 的 item 都画完之后 (之后当前 y 轴 = y)
void fit_margin(ImAxis y, bool log) {
  ImPlot::SetAxis(y);
  const ImPlotRange &e = ImPlot::GetCurrentPlot()->Axes[y].FitExtents;
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
// 成对线表 [口径][PairLine] (口径 = pair_kind: 0 = 有行情键 (篮子 / 成分 / 账户), 1 = 因子 / 策略 = 只有可成交账, 理想位即主线 = 同一条): 主图例口径列 / 线尾小字后缀
constexpr const char *kPairName[2][kPairLines] = {{"可成交", "理想"}, {"可成交", "可成交"}};
constexpr const char *kPairTag[2][kPairLines] = {{" 可成交", " 理想"}, {"", ""}};
int pair_kind(const LinesRow &r) { return !r.ref.ok(); }

// VAL 线尾刻度的 alpha (band 填色 / 线的见 LinesCommon.hpp)
constexpr float kBandTagA = 0.9f;
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

// 主图左上角 (对仗日内栏): 区间档位条贴绘图区左上 (内缩 kBarPad), 图例矩阵接在其下 (lg_origin)
constexpr float kBarPad = 4.0f;
ImVec2 lg_origin() {
  const ImVec2 p = ImPlot::GetPlotPos();
  return ImVec2(p.x + kBarPad, p.y + 2.f * kBarPad + ImGui::GetTextLineHeight());
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
const Thinned *plot_slice(const PlotFrame &pf, const char *label, const LinesSlice &s, ImPlotLineFlags flags = 0) {
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
  return &t;
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
  const LgHover h = legend("##nav_lg", lg_origin(), kCols, lr, cells, [&](int i, int l) {
    ImPlotItem *it = ImPlot::GetItem(rows[static_cast<size_t>(i)].lb[l].c_str()); // 本帧刚 Plot 过, 必在池里
    assert(it);
    return it;
  });
  if (h.on && h.r < 0)
    tip("%s线 (可成交 = 账扣成本, 理想 = 价格; 因子 / 策略行只有可成交, 两列同一条): 色块 = 显隐, 数 = 区间年化\n点 = 整列开关", kPairName[0][h.t]);
  else if (h.on && h.t < 0)
    row_tip(*rows[static_cast<size_t>(h.r)].row);
  else if (h.on)
    tip("%s %s: 点 = 显 / 隐", rows[static_cast<size_t>(h.r)].row->cn.c_str(), kPairName[pair_kind(*rows[static_cast<size_t>(h.r)].row)][h.t]);
  if (h.row >= 0)
    ui.hover.plot = rows[static_cast<size_t>(h.row)].row->id;
  return h.box;
}

// 当前图数据坐标 → 相对鼠标的像素偏移 (支持 log_y; NAV 邻域选线 / 选点共用)
constexpr float kPickPx = 6.0f;      // 选点水平搜索半宽 [px]
constexpr double kPickXWeight = 1.5; // 选点横距权: |dy| + kPickXWeight · |dx| (震荡挤在一起时优先贴近鼠标价格水平)
struct PlotScale {
  double mx = 0.0, ym = 0.0, sx = 0.0, sy = 0.0, x_lo = 0.0, x_hi = 0.0;
  bool log = false;
  static double map_y(double y, bool log) { return log ? std::log(std::max(y, 1e-30)) : y; }
  PlotScale(bool log_, float radius_px) : log(log_) {
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    const ImVec2 sz = ImPlot::GetPlotSize();
    const ImPlotPoint mp = ImPlot::GetPlotMousePos();
    mx = mp.x, ym = map_y(mp.y, log);
    const double dx = lim.X.Max - lim.X.Min, dy = map_y(lim.Y.Max, log) - map_y(lim.Y.Min, log);
    sx = dx > 0.0 ? sz.x / dx : 0.0;
    sy = dy > 0.0 ? sz.y / dy : 0.0;
    const double rx = sx > 0.0 ? radius_px / sx : 0.0;
    x_lo = mx - rx, x_hi = mx + rx;
  }
  double px_dx(double x) const { return (x - mx) * sx; }
  double px_dy(double y) const { return (map_y(y, log) - ym) * sy; }
};
// 原点 (鼠标) 到像素线段 (ax, ay)-(bx, by) 的距离
double seg_dist(double ax, double ay, double bx, double by) {
  const double ux = bx - ax, uy = by - ay, uu = ux * ux + uy * uy;
  const double t = uu > 0.0 ? std::clamp(-(ax * ux + ay * uy) / uu, 0.0, 1.0) : 0.0;
  return std::hypot(ax + t * ux, ay + t * uy);
}
// 抽稀线 t 在鼠标 ±kNearPx 像素内到鼠标的最近像素距 (震荡挤成竖带 / 稀疏斜线皆准)
double thin_dist(const Thinned &t, const PlotScale &sc) {
  const size_t m = t.x.size();
  if (m == 0)
    return DBL_MAX;
  if (m == 1)
    return std::hypot(sc.px_dx(t.x[0]), sc.px_dy(t.y[0]));
  const size_t lo = static_cast<size_t>(std::ranges::lower_bound(t.x, sc.x_lo) - t.x.begin());
  const size_t hi = static_cast<size_t>(std::ranges::upper_bound(t.x, sc.x_hi) - t.x.begin());
  const size_t a = lo > 0 ? lo - 1 : 0, b = std::min(hi + 1, m);
  double best = DBL_MAX;
  for (size_t q = a; q + 1 < b; ++q)
    best = std::min(best, seg_dist(sc.px_dx(t.x[q]), sc.px_dy(t.y[q]), sc.px_dx(t.x[q + 1]), sc.px_dy(t.y[q + 1])));
  return best;
}

// 账户悬停读数 (NAV 主图: 单账户线全图带十字线吸附, 多线近线触发):
//   在鼠标 ±kPickPx 像素内选价格水平最贴近鼠标的采样点 (zoom out 震荡挤在一起时稳选波峰 / 波谷 / 目标价位; 稀疏时退回按 x 最近);
//   显示前复权净资产 (从此刻净资产往前推) + 区间复权 / 真正回撤 (起点 = 当前缩放起点)
void acct_hover(const LinesDerived &d, const LinesView &v, size_t n_lim, bool cross, bool log) {
  const LinesSlice &as = v.slice[kGroupAccount][0][kIdeal];
  const size_t n = std::min(n_lim, as.size());
  if (n == 0)
    return;
  const PlotScale sc(log, kPickPx);
  const auto ix = std::views::iota(size_t{0}, n);
  const size_t lo = static_cast<size_t>(std::ranges::lower_bound(ix, sc.x_lo, {}, [&](size_t j) { return as.x(j); }) - ix.begin());
  const size_t hi = static_cast<size_t>(std::ranges::upper_bound(ix, sc.x_hi, {}, [&](size_t j) { return as.x(j); }) - ix.begin());
  size_t i = 0;
  if (lo < hi) {
    double best = DBL_MAX;
    for (size_t j = lo; j < hi; ++j)
      if (const double s = std::abs(sc.px_dy(as.y(j))) + kPickXWeight * std::abs(sc.px_dx(as.x(j))); s < best)
        best = s, i = j;
  } else {
    i = lo;
    if (i == n || (i > 0 && as.x(i) - sc.mx > sc.mx - as.x(i - 1)))
      --i;
  }
  const double px = as.x(i), py = as.y(i);
  if (cross) {
    ImPlot::SetNextLineStyle(ImVec4(0.85f, 0.85f, 0.85f, 0.5f), 1.0f);
    ImPlot::PlotInfLines("##acct_cx", &px, 1, ImPlotItemFlags_NoFit | ImPlotItemFlags_NoLegend);
    ImPlot::SetNextLineStyle(ImVec4(0.85f, 0.85f, 0.85f, 0.35f), 1.0f);
    ImPlot::PlotInfLines("##acct_cy", &py, 1, ImPlotInfLinesFlags(ImPlotInfLinesFlags_Horizontal) | ImPlotItemFlags_NoFit | ImPlotItemFlags_NoLegend);
  }
  {
    const ImVec2 pt = ImPlot::PlotToPixels(px, py);
    ImDrawList *dl = ImPlot::GetPlotDrawList();
    ImPlot::PushPlotClipRect();
    dl->AddCircleFilled(pt, 4.0f, ImGui::ColorConvertFloat4ToU32(row_color(d, kGroupAccount, 0, kTrade)));
    dl->AddCircle(pt, 4.0f, IM_COL32(255, 255, 255, 230), 0, 1.5f);
    ImPlot::PopPlotClipRect();
  }
  const size_t j = as.j0 + i;
  char when[32];
  if (j == 0) {
    std::snprintf(when, sizeof(when), "%s 前收", misc::dash(d.dates[static_cast<size_t>(as.c.c->f)]).c_str());
  } else {
    const int day_i = as.day(i);
    assert(day_i >= 0 && static_cast<size_t>(day_i) < d.dates.size());
    const int mn = Quote::slot_label(std::clamp(static_cast<int>(std::lround(as.c.x(j))) % static_cast<int>(kDayW), 0, static_cast<int>(kDayW) - 1));
    std::snprintf(when, sizeof(when), "%s %02d:%02d", misc::dash(d.dates[static_cast<size_t>(day_i)]).c_str(), mn / 60, mn % 60);
  }
  const int f0 = as.day(0);
  const std::string from = f0 >= 0 ? misc::dash(d.dates[static_cast<size_t>(f0)]) : misc::dash(d.dates[static_cast<size_t>(as.c.c->f)]) + " 前收";
  const double seg_chg = py - 1.0, seg_pnl = v.acct[i].pnl - v.acct[0].pnl;
  ImGui::BeginTooltip();
  ImGui::TextUnformatted(when);
  ImGui::SameLine();
  ImGui::TextDisabled("起 %s", from.c_str());
  EquityTip(v.acct[i]);
  ImGui::TextDisabled("区间");
  ImGui::SameLine(0, 4);
  ImGui::TextColored(tint(seg_chg, 0.0), "%+.2f%%", 100.0 * seg_chg);
  ImGui::SameLine();
  ImGui::TextDisabled("盈亏");
  ImGui::SameLine(0, 4);
  ImGui::TextColored(tint(seg_pnl, 0.0), "%s", money_text(seg_pnl, true).c_str());
  ImGui::EndTooltip();
}

// ---- NAV 主图: 画选中行的两条线, 按组表画层 (for_each_layer; 策略在最上层) + 图例矩阵 (最上层, nav_legend);
//      日内形未到的行 (minute_wait) 整行不画、不进图例, 到了一次画成分钟线;
//      行刚选中 → ui.reveal 强制回默认显隐一次 (理想列显, 可成交列隐);
//      悬停联动 (ui.hover): 显示的线里鼠标处最近的一条 → 其行 (+ 行的悬停提示; 鼠标在图例矩阵上不算), hot 行显示的线白粗线画在最上;
//      账户线上叠成交点 (trades: 30 分钟一档, 红 / 绿小圆点; 空 = 不叠), 鼠标近一点 → 提示该档成交 (代替账户线悬停);
//      账户线悬停 (只显账户线时全图带十字线吸附, 多线时近线触发) 显示前复权净资产与区间复权 / 真正回撤 ----
void plot_nav(LinesState &ui, const LinesDerived &d, const LinesView &v, PlotFrame &pf, const Quote::Trades *trades) {
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
    const Thinned *thin;
  };
  std::vector<On> on; // 显示的线
  for (const NavRow &x : rows)
    for (int l = 0; l < kPairLines; ++l) {
      const char *lb = x.lb[l].c_str();
      if (ui.reveal.erase(line_id(*x.row, l)))
        ImPlot::HideNextItem(l == kTrade, ImPlotCond_Always);
      ImPlot::SetNextLineStyle(x.col[l], row_width(x.g));
      const Thinned *th = plot_slice(pf, lb, *x.s[l]);
      if (pf.item(lb)) {
        push_tag(pf.tags, pf.lim, *x.s[l], ImGui::ColorConvertFloat4ToU32(x.col[l]), x.row->tag + kPairTag[pair_kind(*x.row)][l]);
        pf.lines.push_back(x.s[l]);
        on.push_back({x.row, x.s[l], th});
      }
    }
  const bool in_box = s_box.Contains(ImGui::GetMousePos());
  int k = -1;
  if (ImPlot::IsPlotHovered() && !in_box) {
    const PlotScale sc(ui.log_y, kNearPx);
    double best = kNearPx;
    for (size_t j = 0; j < on.size(); ++j)
      if (const double dd = thin_dist(*on[j].thin, sc); dd < best)
        best = dd, k = static_cast<int>(j);
  }
  const bool only_acct = !on.empty() && std::ranges::all_of(on, [](const On &x) { return x.row->ref.is(Quote::kAccount); });
  const bool hit_acct = k >= 0 && on[static_cast<size_t>(k)].row->ref.is(Quote::kAccount);
  if (k >= 0) {
    const LinesRow &r = *on[static_cast<size_t>(k)].row;
    ui.hover.plot = r.id;
    if (!hit_acct || v.acct.empty())
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
  bool on_dot = false;
  if (const auto a = std::ranges::find_if(on, [](const On &x) { return x.row->ref.is(Quote::kAccount); }); trades && a != on.end()) {
    const std::vector<TradeDot> dots = TradeDots(*trades, d.dates, a->s->size(), [&](size_t i) { return slice_pt(*a->s, i); });
    if (const TradeDot *dot = PlotTradeDots(dots, true); dot && !in_box) {
      on_dot = true;
      ImGui::BeginTooltip();
      TradeTip(*dot->m);
      ImGui::EndTooltip();
    }
  }
  if (!v.acct.empty() && ImPlot::IsPlotHovered() && !in_box && !on_dot && (only_acct || hit_acct))
    acct_hover(d, v, hit_acct ? on[static_cast<size_t>(k)].s->size() : v.slice[kGroupAccount][0][kIdeal].size(), only_acct, ui.log_y);
  s_box = nav_legend(ui, rows);
}

// ---- VAL / MARGIN 主图例 (legend; 对仗 Inspect 期限结构图例): 行 = 口集 vs 的口 (EP / BP / SP / CP / DY | 融资 / 融券),
//      列 = 当前收益率 (占比) | 当前倍数 (两融 = 余额) | N 年分位 | Band (两融 = 占比线 + Band, 两口单选) | 等值线 (两融无);
//      分位按低绿 → 高红着色; Band 色块 = band 渐变 (下 → 上); 悬停格 / 头 → 对应线高亮 (下帧) ----
enum ValLgCol : int { kLgBand,
                      kLgContour };
constexpr float kHiScale = 2.0f; // 图例悬停高亮: 线宽倍数 (等值线改用 kContourHotA)
constexpr float kContourHotA = 0.6f;
// 开关列 c 口 b 的 item label (Band / 等值线). 须有可显示的名: ImPlot 对无名 item 每帧强制 Show = true, 开关不住; 图本身 NoLegend, 名不显示
std::string val_item(int c, int b) {
  return std::string(kVal[b].name) + (c == kLgBand ? " Band###pf_band" : " 等值线###pf_contour") + std::to_string(b);
}

// 返回悬停 (r = 口集内第几口, t = ValLgCol) + 框 (悬停等值线避开)
LgHover val_legend(const LinesView &v, int years, const ValSet &vs) {
  char yrs[24];
  std::snprintf(yrs, sizeof(yrs), "%d年分位", years);
  const bool mg = vs.margin();
  const LgCol val_cols[] = {{"当前"}, {"倍数"}, {yrs}, {"Band", kLgBand}, {"等值线", kLgContour}};
  const LgCol mg_cols[] = {{"当前"}, {"余额"}, {yrs}, {"Band", kLgBand}};
  const std::span<const LgCol> cols = mg ? std::span<const LgCol>(mg_cols) : std::span<const LgCol>(val_cols);
  const ImU32 c_txt = ImGui::GetColorU32(ImGuiCol_Text), c_dis = ImGui::GetColorU32(ImGuiCol_TextDisabled);
  const ImVec4 contour(kContourCol.x, kContourCol.y, kContourCol.z, 0.8f);
  std::vector<LgRow> lr;
  std::vector<LgCell> cells;
  for (int b = vs.lo; b < vs.hi; ++b) {
    const LinesBand &bd = v.band[b];
    char a[24] = "-", m[24] = "-", p[16] = "-";
    if (bd.ok) {
      fmt_val(b, bd.now, a, sizeof(a));
      if (mg && std::isfinite(bd.bal))
        std::snprintf(m, sizeof(m), "%s", yi_text(bd.bal).c_str());
      else if (!mg && bd.now > 0.0)
        std::snprintf(m, sizeof(m), "%s %.2f", kVal[b].inv, 1.0 / bd.now);
      std::snprintf(p, sizeof(p), "%3.0f%%", bd.pct);
    }
    const ImU32 ink = bd.ok ? c_txt : c_dis;
    lr.push_back({kVal[b].name, c_txt});
    cells.push_back({a, ink});
    cells.push_back({m, ink});
    cells.push_back({p, bd.ok ? ImGui::ColorConvertFloat4ToU32(band_col(bd.pct / 100.0, 1.0f)) : c_dis});
    cells.push_back({{}, 0, {band_col(0.0, 1.0f), band_col(1.0, 1.0f)}});
    if (!mg)
      cells.push_back({{}, 0, {contour, contour}});
  }
  const LgHover h = legend(mg ? "##pf_margin_lg" : "##pf_val_lg", lg_origin(), cols, lr, cells, [&](int r, int c) {
    ImPlotItem *it = ImPlot::GetItem(val_item(c, vs.lo + r).c_str()); // 本帧刚 Plot 过, 必在池里
    assert(it);
    return it;
  });
  if (h.on && h.r >= 0 && h.t >= 0) {
    const int b = vs.lo + h.r;
    const std::vector<double> &lv = v.band[b].levels;
    if (h.t == kLgBand && mg)
      tip("%s: 余额占比线 (左轴 %%) + 过去 %d 年滚动分位 Band (占比低绿 → 高红); 两口单选, 点 = 切到该口", kVal[b].name, years);
    else if (h.t == kLgBand)
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
  const ValSet &vs = kValSets[kSetVal];
  const LinesSlice &sl = v.slice[pin.g][pin.r][kIdeal];
  const double *bx = v.band_x.data();
  const int bn = static_cast<int>(v.band_x.size());
  for (int b = vs.lo; b < vs.hi; ++b) {
    const std::string lbs = val_item(kLgBand, b);
    if (b != vs.show)
      ImPlot::HideNextItem(true, ImPlotCond_Once);
    PlotBand(lbs.c_str(), v.band[b].lines, bx, s_hl.at(b - vs.lo, kLgBand) ? kHiScale : 1.0f);
  }
  // 等值线: 每档一条 (同一 item; 无档也登记)
  bool on[kPoolVals] = {};
  for (int b = vs.lo; b < vs.hi; ++b) {
    const std::vector<double> &lv = v.band[b].levels;
    const std::string lbs = val_item(kLgContour, b);
    const char *lb = lbs.c_str();
    const ImVec4 col(kContourCol.x, kContourCol.y, kContourCol.z, s_hl.at(b - vs.lo, kLgContour) ? kContourHotA : kContourCol.w);
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
  s_hl = val_legend(v, ui.roll.key.years, vs);
  zoom_x(rg, static_cast<int>(d.dates.size()), s_hl.box);
}

// ---- MARGIN 主图: 两轴 (余额不随价格走, 不换算成价格) — 右轴 Y2 = 标的理想线 (背景淡线, 同 NAV 画到分钟; log 开关作用于它),
//      左轴 Y1 = 当前口的余额占比 (%) + 其滚动分位 band (占比空间; 下 = 占比低绿 → 上 = 高红);
//      口 = 两口单选 (各口 占比线 + band 共一个 item, 主图例 Band 列切; 首次默认融资, ui.margin_on 记着; 左轴刻度按其小数位);
//      价格线 / 占比线 / 开着的 band 线各进各轴的 fit, 填色不进; 线尾小字 = 价格 (右轴) + 占比 (视野内末个可算点); 无现价十字 / 刻度;
//      框选 / 滚轮同 VAL ----
constexpr float kMarginPxA = 0.35f; // 背景价格线的 alpha
void plot_margin(LinesState &ui, LinesRange &rg, const LinesDerived &d, const LinesView &v, const LinesAt &pin, PlotFrame &pf) {
  static LgHover s_hl; // 上帧图例矩阵
  const ValSet &vs = kValSets[kSetMargin];
  const LinesSlice &sl = v.slice[pin.g][pin.r][kIdeal];
  ImVec4 pc = row_color(d, pin, kTrade);
  pc.w = kMarginPxA;
  ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
  ImPlot::SetNextLineStyle(pc, 1.0f);
  plot_slice(pf, "##pf_margin_px", sl);
  if (pf.item("##pf_margin_px"))
    push_tag(pf.tags, pf.lim, sl, ImGui::ColorConvertFloat4ToU32(pc), d.row(pin).tag);
  ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
  const double *bx = v.band_x.data();
  const int bn = static_cast<int>(v.band_x.size());
  const ImVec4 ink = ImGui::GetStyleColorVec4(ImGuiCol_Text);
  const auto item = [](int b) { // 本帧刚 Plot 过, 必在池里 (池会扩容: 用时现取)
    ImPlotItem *it = ImPlot::GetItem(val_item(kLgBand, b).c_str());
    assert(it);
    return it;
  };
  for (int b = vs.lo; b < vs.hi; ++b) {
    const LinesBand &bd = v.band[b];
    const std::string lbs = val_item(kLgBand, b);
    const char *lb = lbs.c_str();
    const float hw = s_hl.at(b - vs.lo, kLgBand) ? kHiScale : 1.0f;
    if (b != vs.show)
      ImPlot::HideNextItem(true, ImPlotCond_Once);
    PlotBand(lb, bd.lines, bx, hw);
    ImPlot::SetNextLineStyle(ink, 1.5f * hw);
    ImPlot::PlotLine(lb, bx, bd.ratio.data(), bn, ImPlotLineFlags_SkipNaN);
    if (!item(b)->Show)
      continue;
    for (size_t i = static_cast<size_t>(bn); i-- > 0;)
      if (bx[i] <= pf.lim.X.Max && std::isfinite(bd.ratio[i])) {
        if (bx[i] >= pf.lim.X.Min)
          pf.tags.push_back({ImPlot::PlotToPixels(bx[i], bd.ratio[i]), ImGui::ColorConvertFloat4ToU32(ink), std::string(kVal[b].name) + "占比"});
        break;
      }
  }
  s_hl = val_legend(v, ui.roll.key.years, vs);
  // 两口单选: 图例刚点开另一口 → 换到它 (原口隐); 都关 → −1
  int pick = -1;
  for (int b = vs.lo; b < vs.hi; ++b)
    if (item(b)->Show && (pick < 0 || pick == ui.margin_on))
      pick = b;
  for (int b = vs.lo; b < vs.hi; ++b)
    item(b)->Show = b == pick;
  ui.margin_on = pick;
  zoom_x(rg, static_cast<int>(d.dates.size()), s_hl.box);
}

} // namespace

void PlotBand(const char *lb, const BandLines &bd, const double *x, float hw) {
  for (const auto &[i0, i1] : bd.segs) {
    const int n = i1 - i0;
    const double *xs = x + i0;
    for (int c = 0; c <= kBandQ; ++c) {
      ImPlot::SetNextFillStyle(band_col(static_cast<double>(c) / kBandQ, 1.0f), kBandFillA);
      if (c == 0)
        ImPlot::PlotShaded(lb, xs, bd.y[0].data() + i0, n, -HUGE_VAL, ImPlotItemFlags_NoFit);
      else if (c == kBandQ)
        ImPlot::PlotShaded(lb, xs, bd.y[kBandQ - 1].data() + i0, n, HUGE_VAL, ImPlotItemFlags_NoFit);
      else
        ImPlot::PlotShaded(lb, xs, bd.y[c - 1].data() + i0, bd.y[c].data() + i0, n, ImPlotItemFlags_NoFit);
    }
  }
  for (int j = 0; j < kBandQ; ++j) {
    ImPlot::SetNextLineStyle(band_col((j + 0.5) / kBandQ, kBandLineA), (j == kBandQ / 2 ? 1.5f : 1.0f) * hw);
    ImPlot::PlotLine(lb, x, bd.y[j].data(), static_cast<int>(bd.size()), ImPlotLineFlags_SkipNaN);
  }
}

LinesRange DefaultLinesRange(int page, const std::vector<misc::Ymd> &dates) {
  const int D = static_cast<int>(dates.size());
  const SpanId s = kPageDefs[page].span;
  return LinesRange{{.d0 = D > 0 && s != kSpanAll ? span_d0(dates, kSpanDefs[s]) : 0, .d1 = D}};
}

// ---- 主图: NAV (plot_nav) ↔ VAL (plot_val) ↔ MARGIN (plot_margin: 左轴占比 + 右轴价格) 同一框: 贴边 (窄内边距, 不标轴名); y 恒贴合 + 留白 (fit_margin, 各轴各留);
//      x: 恒贴合 (视野只随区间; 滚轮 = 钉住右端缩放区间, wheel_x); x 右端留出线尾小字 / 刻度 / 十字标签;
//      左上角 = 区间档位条 (span_bar) + 其下图例矩阵 ----
void RenderLinesPlot(LinesState &ui, int page, const LinesAt &pin, const ImVec2 &size, const Quote::Trades *trades, const std::string &doing) {
  const LinesDerived &d = ui.der.val;
  const LinesView &v = ui.view.val;
  const ValSetId vals = kPageDefs[page].vals;
  const bool nav = vals == kSetNone, mg = vals == kSetMargin;
  LinesRange &range = ui.range[page];
  const LinesRange rg = range;                                    // 视图所切的区间 (VAL / MARGIN 框选本帧改 range, 下帧才进视图)
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(2, 2)); // 贴边: 图区让给线
  ImPlot::PushStyleVar(ImPlotStyleVar_LabelPadding, ImVec2(2, 1));
  LinesFitKey fk{d.hist_gen, ui.roll.gen, ui.pct.gen, pin, rg.d0, rg.d1, page, ui.log_y, {}, ui.shown_key};
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
  if (ImPlot::BeginPlot(nav ? "##portfolio" : mg ? "##pf_margin"
                                                 : "##pf_val",
                        size, ImPlotFlags_NoLegend | ImPlotFlags_NoBoxSelect)) {
    ui.fit_key = fk;
    constexpr ImPlotAxisFlags kFitY = ImPlotAxisFlags_AutoFit | ImPlotAxisFlags_RangeFit;
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_AutoFit, kFitY);
    DayAxis ax = day_axis(d.dates);
    setup_day_axis(ax);
    const ImAxis px = mg ? ImAxis_Y2 : ImAxis_Y1; // 价格 / 净值所在的 y 轴 (log 开关作用于它)
    if (mg) {                                     // 左轴 = 占比 (%, 按在画的口的小数位), 右轴 = 价格
      char f[16];
      std::snprintf(f, sizeof(f), "%%.%df%%%%", ui.margin_on >= 0 ? kVal[ui.margin_on].prec : 2);
      ImPlot::SetupAxisFormat(ImAxis_Y1, f);
      ImPlot::SetupAxis(ImAxis_Y2, nullptr, ImPlotAxisFlags_AuxDefault | kFitY);
    }
    if (ui.log_y)
      ImPlot::SetupAxisScale(px, ImPlotScale_Log10);
    // x fit 帧的视野还是上帧的 (区间刚变时可能整段在外): 小字不按视野筛, 全收来量留白宽度
    const ImPlotRect cur = ImPlot::GetPlotLimits(); // 先锁 setup, FitThisFrame 才定
    PlotFrame pf{fit_x() ? ImPlotRect(-DBL_MAX, DBL_MAX, -DBL_MAX, DBL_MAX) : cur, {}, {}, {}, {}, ui.view.gen};
    if (nav)
      plot_nav(ui, d, v, pf, trades);
    else if (pin.g >= 0 && !wait && mg)
      plot_margin(ui, range, d, v, pin, pf);
    else if (pin.g >= 0 && !wait)
      plot_val(ui, range, d, v, pin, pf);
    wheel_x(range);
    if (wait)
      plot_hint(doing.empty() ? "分钟线载入中…" : ("分钟线载入中… · " + doing).c_str());
    fit_margin(px, ui.log_y);
    if (mg)
      fit_margin(ImAxis_Y1, false);
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
  const Line none;
  const Line &rc = nav ? *d.rows[kGroupStrat][0].line[kTrade] : pin.g >= 0 ? *d.row(pin).line[kIdeal]
                                                                           : none;
  range_plot("##pf_range", ui.range[page], d.dates, static_cast<double>(kDayW), size, ui.log_y, [&] {
    ImPlot::SetNextLineStyle(nav || pin.g < 0 ? kStratCol : row_color(d, pin, kTrade), 1.0f);
    ImPlot::PlotLineG("##pf_range_line", day_end_pt, const_cast<Line *>(&rc), static_cast<int>(rc.e.size()));
  });
}

} // namespace GUI::Market