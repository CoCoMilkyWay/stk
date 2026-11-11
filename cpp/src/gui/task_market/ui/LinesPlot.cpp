// Market 行情页主图 (NAV 资金曲线 ↔ VAL 估值 band) + 区间条 — 见 LinesCommon.hpp
#include "gui/task_market/ui/LinesCommon.hpp"

#include "gui/util/Chart.hpp" // tint / EndTag / NowCross / range_bar
#include "gui/util/Tip.hpp"
#include "imgui.h"
#include "implot.h"
#include "implot_internal.h" // GetItem (主图例显隐 / 线色), ButtonBehavior (图例矩阵)
#include "misc/date.hpp"     // dash (现价十字时刻)

#include <algorithm>
#include <array>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <iterator>
#include <ranges>
#include <string>
#include <vector>

namespace GUI::Market {

namespace {

// 横轴刻度 (主图 / 区间条): x = 日下标 · kDayW + 分钟 → YY/MM/DD, 只到日 (user_data = 日并集轴)
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

// 组线宽: 强调组 (kGroupDefs::accent) 加粗
float row_width(int g) { return kGroupDefs[g].accent ? 2.0f : 1.0f; }
// 成对线表 [口径][PairLine] (口径 = pair_kind: 0 = 有行情键 (篮子 / 成分), 1 = 因子 / 策略): 主图例口径列 / 线尾小字后缀
constexpr const char *kPairName[2][kPairLines] = {{"可成交", "理想"}, {"有冲击", "无冲击"}};
constexpr const char *kPairTag[2][kPairLines] = {{" 可成交", " 理想"}, {"", " 无冲击"}};
int pair_kind(const LinesRow &r) { return !r.q.ok(); }

// VAL 线表 (标的线色 = 该行的 row_color): band 填色 / 线 / 线尾刻度的 alpha
constexpr float kBandFillA = 0.12f, kBandLineA = 0.35f, kBandTagA = 0.9f;
// VAL 主图 y 上限 = 价格线最高点 × (1 + kValHeadroom) (band 不进 fit)
constexpr double kValHeadroom = 0.2;
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

// 右补空格到 px 宽 (等宽字体, 中文占两格: 按实测像素补, 列对齐)
std::string pad_to(const char *s, float px) {
  const float w = ImGui::CalcTextSize(s).x, sp = ImGui::CalcTextSize(" ").x;
  return s + std::string(static_cast<size_t>(std::lround(std::max(0.f, px - w) / sp)), ' ');
}
// NAV 主图例多列排版: 第 c 列起点 = 前 c 列各自全行最宽之和 + 列间隔; 按已拼前缀的实测宽补空格 (取整误差不跨列累积); 空格子不补
template <size_t N>
std::vector<std::string> align_cols(const std::vector<std::array<std::string, N>> &rows) {
  float w[N] = {};
  for (const std::array<std::string, N> &r : rows)
    for (size_t c = 0; c < N; ++c)
      w[c] = std::max(w[c], ImGui::CalcTextSize(r[c].c_str()).x);
  const float gap = ImGui::CalcTextSize("  ").x;
  std::vector<std::string> out;
  out.reserve(rows.size());
  for (const std::array<std::string, N> &r : rows) {
    std::string s;
    float x = 0.f;
    for (size_t c = 0; c < N; ++c) {
      if (c > 0)
        x += w[c - 1] + gap;
      if (!r[c].empty())
        s = pad_to(s.c_str(), x) + r[c];
    }
    out.push_back(std::move(s));
  }
  return out;
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

// ---- 切片画线 (零拷贝: ImPlot 按下标回调取点) ----
ImPlotPoint slice_pt(const LinesSlice &s, size_t i) { return {s.x(i), s.y(i)}; }
ImPlotPoint slice_getter(int i, void *s) { return slice_pt(*static_cast<const LinesSlice *>(s), static_cast<size_t>(i)); }
void plot_slice(const char *label, const LinesSlice &s) {
  ImPlot::PlotLineG(label, slice_getter, const_cast<LinesSlice *>(&s), static_cast<int>(s.size()));
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
  // 图例项 label 的显隐记进 shown; 显示 → 该项 (Color = 实际线色), 否则 nullptr
  const ImPlotItem *item(const char *label) {
    const ImPlotItem *it = ImPlot::GetItem(label);
    assert(it);
    shown += it->Show ? '1' : '0';
    return it->Show ? it : nullptr;
  }
};
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

// ---- NAV 主图: 画选中行的两条线, 按组表画层 (for_each_layer; 策略在最上层). 主图例 = cn | 口径 | 区间年化 | 区间夏普 | 年换手 (align_cols; 悬停一项看 hover), ###线 ID 定 ID (数值变了不换 item);
//      点图例单独隐 / 显一条 (行刚选中 → ui.reveal 强制回默认显隐一次: 理想列显, 可成交列隐) ----
void plot_nav(LinesUIState &ui, const LinesDerived &d, const LinesView &v, PlotFrame &pf) {
  struct Draw {
    int g, l;
    const LinesRow *row;
    const LinesSlice *s;
    ImVec4 col;
  };
  std::vector<Draw> ds; // 画序, 只含选中行的
  for_each_layer([&](int g) {
    for (size_t r = 0; r < d.rows[g].size(); ++r)
      if (picked(ui, kPageNav, d.rows[g][r]))
        for (int l = 0; l < kPairLines; ++l)
          ds.push_back({g, l, &d.rows[g][r], &v.slice[g][r][l], row_color(d, g, r, l)});
  });
  std::vector<std::array<std::string, 5>> rows;
  for (const Draw &x : ds) {
    char ann[48] = "", shp[48] = "", turn[48] = "";
    if (std::isfinite(x.s->ann))
      std::snprintf(ann, sizeof(ann), "(年化=%+5.1f%%)", x.s->ann);
    if (std::isfinite(x.s->sharpe))
      std::snprintf(shp, sizeof(shp), "(夏普=%+5.2f)", x.s->sharpe);
    if (const double t = x.row->line[kTrade]->turn; std::isfinite(t))
      std::snprintf(turn, sizeof(turn), "(年换手=%5.1f倍)", t);
    rows.push_back({x.row->cn, kPairName[pair_kind(*x.row)][x.l], ann, shp, turn});
  }
  const std::vector<std::string> texts = align_cols(rows);
  for (size_t i = 0; i < ds.size(); ++i) {
    const Draw &x = ds[i];
    const std::string id = line_id(*x.row, x.l), lb = texts[i] + "###" + id;
    if (ui.reveal.erase(id))
      ImPlot::HideNextItem(x.l == kTrade, ImPlotCond_Always);
    ImPlot::SetNextLineStyle(x.col, row_width(x.g));
    plot_slice(lb.c_str(), *x.s);
    if (ImPlot::IsLegendEntryHovered(lb.c_str()))
      row_tip(*x.row);
    if (pf.item(lb.c_str())) {
      push_tag(pf.tags, pf.lim, *x.s, ImGui::ColorConvertFloat4ToU32(x.col), x.row->tag + kPairTag[pair_kind(*x.row)][x.l]);
      pf.lines.push_back(x.s);
    }
  }
}

// ---- VAL 主图例 = 自绘矩阵 (对仗 Inspect 期限结构图例), 绘图区左上: 行 = 口 (EP / BP / SP / CP / DY), 列 = 当前收益率 | 当前倍数 | N 年估值分位 | Band | 等值线;
//      开关列的格 = 色块 (实心 = 显示, 空框 = 隐), 点格切该项, 点行头 / 列头切整行 / 整列 (有显示的 → 全隐, 否则全显); 悬停格 / 头 → 对应线高亮 (下帧) ----
enum ValLgCol : int { kLgBand,
                      kLgContour };
constexpr int kLgToggles = 2;
constexpr float kHiScale = 2.0f; // 图例悬停高亮: 线宽倍数 (等值线改用 kContourHotA)
constexpr float kContourHotA = 0.6f;
// 上帧图例矩阵的悬停 (b = −1 整列, c = −1 整行) + 框 (悬停等值线避开); 本图唯一实例
struct ValLegendHl {
  bool on = false;
  int b = -1, c = -1;
  ImRect box;
  bool at(int b0, int c0) const { return on && (b < 0 || b == b0) && (c < 0 || c == c0); }
};
// 开关列 c 口 b 的 item label (Band / 等值线). 须有可显示的名: ImPlot 对无名 item 每帧强制 Show = true, 开关不住; 图本身 NoLegend, 名不显示
std::string val_item(int c, int b) {
  return std::string(kVal[b].name) + (c == kLgBand ? " Band###pf_band" : " 等值线###pf_contour") + std::to_string(b);
}

void val_legend(const LinesView &v, int years, ValLegendHl &hl) {
  constexpr int kTexts = 3, kCols = kTexts + kLgToggles;
  const ImPlotStyle &ps = ImPlot::GetStyle();
  const ImVec2 inner = ps.LegendInnerPadding;
  const float th = ImGui::GetTextLineHeight(), sw = th * 0.7f, gx = 2.f * ImGui::GetStyle().ItemSpacing.x;
  char yrs[24];
  std::snprintf(yrs, sizeof(yrs), "%d年分位", years);
  const char *head[kCols] = {"当前", "倍数", yrs, "Band", "等值线"};
  std::array<std::array<std::string, kTexts>, kPoolVals> txt;
  for (int b = 0; b < kPoolVals; ++b) {
    const LinesBand &bd = v.band[b];
    char a[24] = "-", m[24] = "-", p[16] = "-";
    if (bd.ok) {
      fmt_val(b, bd.now, a, sizeof(a));
      if (bd.now > 0.0)
        std::snprintf(m, sizeof(m), "%s %.2f", kVal[b].inv, 1.0 / bd.now);
      std::snprintf(p, sizeof(p), "%3.0f%%", bd.pct);
    }
    txt[static_cast<size_t>(b)] = {a, m, p};
  }
  float w_head = 0.f, w[kCols], xc[kCols];
  for (const ValMeta &vm : kVal)
    w_head = std::max(w_head, ImGui::CalcTextSize(vm.name).x);
  const ImVec2 p0 = ImPlot::GetPlotPos(), o(p0.x + ps.LegendPadding.x, p0.y + ps.LegendPadding.y);
  float x = o.x + inner.x + w_head;
  for (int c = 0; c < kCols; ++c) {
    w[c] = std::max(ImGui::CalcTextSize(head[c]).x, c < kTexts ? 0.f : sw);
    if (c < kTexts)
      for (const auto &r : txt)
        w[c] = std::max(w[c], ImGui::CalcTextSize(r[static_cast<size_t>(c)].c_str()).x);
    xc[c] = x + gx;
    x = xc[c] + w[c];
  }
  const ImRect box(o, ImVec2(x + inner.x, o.y + 2.f * inner.y + static_cast<float>(kPoolVals + 1) * th));
  const auto y_row = [&](int b) { return box.Min.y + inner.y + static_cast<float>(b + 1) * th; }; // b = −1 → 列头行
  const auto button = [&](const ImRect &r, const char *id, bool &hov) {
    bool held = false;
    const ImGuiID gid = ImGui::GetID(id);
    ImGui::KeepAliveID(gid);
    return ImGui::ButtonBehavior(r, gid, &hov, &held, ImGuiButtonFlags_AllowOverlap | ImGuiButtonFlags_PressedOnClick);
  };
  const auto item_of = [&](int c, int b) {
    ImPlotItem *it = ImPlot::GetItem(val_item(c, b).c_str()); // 本帧刚 Plot 过, 必在池里
    assert(it);
    return it;
  };
  // 整行 / 整列: 集合里有显示的 → 全隐, 否则全显 (b0 / c0 = −1 = 不限)
  const auto any_shown = [&](int b0, int c0) {
    for (int b = 0; b < kPoolVals; ++b)
      for (int c = 0; c < kLgToggles; ++c)
        if ((b0 < 0 || b == b0) && (c0 < 0 || c == c0) && item_of(c, b)->Show)
          return true;
    return false;
  };
  const auto toggle_set = [&](int b0, int c0) {
    const bool any = any_shown(b0, c0);
    for (int b = 0; b < kPoolVals; ++b)
      for (int c = 0; c < kLgToggles; ++c)
        if ((b0 < 0 || b == b0) && (c0 < 0 || c == c0))
          item_of(c, b)->Show = !any;
  };
  bool bg_hov = false;
  button(box, "##pf_val_legend", bg_hov);
  ImDrawList &dl = *ImPlot::GetPlotDrawList();
  ImPlot::PushPlotClipRect();
  dl.AddRectFilled(box.Min, box.Max, ImPlot::GetStyleColorU32(ImPlotCol_LegendBg));
  dl.AddRect(box.Min, box.Max, ImPlot::GetStyleColorU32(ImPlotCol_LegendBorder));
  const ImU32 c_txt = ImGui::GetColorU32(ImGuiCol_Text), c_dis = ImGui::GetColorU32(ImGuiCol_TextDisabled),
              c_hov = ImGui::GetColorU32(ImGuiCol_PlotLinesHovered);
  ValLegendHl nx;
  nx.box = box;
  char id[32];
  for (int c = 0; c < kCols; ++c) { // 列头 (开关列可点)
    const ImRect r(ImVec2(xc[c], y_row(-1)), ImVec2(xc[c] + w[c], y_row(-1) + th));
    const float tw = ImGui::CalcTextSize(head[c]).x;
    const ImVec2 at(c < kTexts ? r.Max.x - tw : r.Min.x + (w[c] - tw) * 0.5f, r.Min.y);
    if (c < kTexts) {
      dl.AddText(at, c_dis, head[c]);
      continue;
    }
    std::snprintf(id, sizeof(id), "##pf_lg_c%d", c);
    bool hov = false;
    if (button(r, id, hov))
      toggle_set(-1, c - kTexts);
    if (hov)
      nx.on = true, nx.b = -1, nx.c = c - kTexts;
    dl.AddText(at, hov ? c_hov : any_shown(-1, c - kTexts) ? c_txt
                                                           : c_dis,
               head[c]);
  }
  for (int b = 0; b < kPoolVals; ++b) {
    const float yb = y_row(b);
    { // 行头
      const ImRect r(ImVec2(box.Min.x + inner.x, yb), ImVec2(box.Min.x + inner.x + w_head, yb + th));
      std::snprintf(id, sizeof(id), "##pf_lg_r%d", b);
      bool hov = false;
      if (button(r, id, hov))
        toggle_set(b, -1);
      if (hov)
        nx.on = true, nx.b = b, nx.c = -1;
      dl.AddText(r.Min, hov ? c_hov : any_shown(b, -1) ? c_txt
                                                       : c_dis,
                 kVal[b].name);
    }
    const LinesBand &bd = v.band[b];
    for (int c = 0; c < kTexts; ++c) { // 数值列右对齐; 分位按便宜绿 → 贵红着色
      const std::string &t = txt[static_cast<size_t>(b)][static_cast<size_t>(c)];
      const ImU32 col = !bd.ok ? c_dis : c == 2 ? ImGui::ColorConvertFloat4ToU32(band_col(bd.pct / 100.0, 1.0f))
                                                : c_txt;
      dl.AddText(ImVec2(xc[c] + w[c] - ImGui::CalcTextSize(t.c_str()).x, yb), col, t.c_str());
    }
    for (int c = 0; c < kLgToggles; ++c) {
      const float xq = xc[kTexts + c], wq = w[kTexts + c];
      const ImRect r(ImVec2(xq, yb), ImVec2(xq + wq, yb + th));
      const ImVec2 s0(xq + (wq - sw) * 0.5f, yb + (th - sw) * 0.5f), s1(s0.x + sw, s0.y + sw);
      std::snprintf(id, sizeof(id), "##pf_lg_%d_%d", b, c);
      bool hov = false;
      ImPlotItem *it = item_of(c, b);
      if (button(r, id, hov))
        it->Show = !it->Show;
      if (hov) {
        nx.on = true, nx.b = b, nx.c = c;
        const std::vector<double> &lv = bd.levels;
        if (c == kLgBand)
          tip("%s Band: 过去 %d 年估值分位换算的价格线 (点 = 开关)", kVal[b].name, years);
        else if (!lv.empty())
          tip("%s 等值线: %s %.2f ~ %.2f 倍 %d 等分 (该标的全史倍数区间, 两头各剔 %.0f%%)\n点 = 开关; 悬停主图里的线看该档", kVal[b].name, kVal[b].inv, lv.front(),
              lv.back(), kContours - 1, 100.0 * kContourTrim);
        else
          tip("%s 等值线: 全史不可算 (无收益率 > 0 的日子)", kVal[b].name);
      }
      const float a = hov ? 0.75f : 1.f;
      if (!it->Show)
        dl.AddRect(s0, s1, hov ? c_txt : c_dis);
      else if (c == kLgBand) // 色块 = band 渐变 (便宜绿 → 贵红)
        dl.AddRectFilledMultiColor(s0, s1, ImGui::ColorConvertFloat4ToU32(band_col(0.0, a)), ImGui::ColorConvertFloat4ToU32(band_col(1.0, a)),
                                   ImGui::ColorConvertFloat4ToU32(band_col(1.0, a)), ImGui::ColorConvertFloat4ToU32(band_col(0.0, a)));
      else
        dl.AddRectFilled(s0, s1, ImGui::ColorConvertFloat4ToU32(ImVec4(kContourCol.x, kContourCol.y, kContourCol.z, 0.8f * a)));
    }
  }
  ImPlot::PopPlotClipRect();
  hl = nx;
}

// VAL 悬停等值线: 鼠标 x 处 (相邻两点插值) 各开着的口里离鼠标最近的一档, kContourHit px 内 → 高亮 (最上层) + 提示数据; 鼠标在图例矩阵上不算
void hover_contour(const LinesDerived &d, const LinesView &v, const LinesSlice &sl, const bool *on, const ImRect &legend) {
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
  const double px = lerp(sl.y(j - 1), sl.y(j)), now = px / hu;
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

// VAL 横向框选: 左键拖出一段 x (竖带示意) → 松手 = 区间改成该段覆盖的日 [d0, d1) (区间条同一份, 主图随之贴合); 双击 = 区间回全段; 图例矩阵上不起
constexpr float kZoomMin = 4.0f; // 框选生效的最小宽 (px)
void zoom_x(LinesRange &rg, int D, const ImRect &legend) {
  assert(D > 0);
  const ImVec2 mp = ImGui::GetMousePos();
  if (ImPlot::IsPlotHovered() && !legend.Contains(mp)) {
    if (ImGui::IsMouseDoubleClicked(0)) {
      rg.d0 = 0, rg.d1 = D, rg.zooming = false;
      return;
    }
    if (ImGui::IsMouseClicked(0))
      rg.zooming = true, rg.zoom_x = ImPlot::GetPlotMousePos().x;
  }
  if (!rg.zooming)
    return;
  const ImVec2 pp = ImPlot::GetPlotPos(), ps = ImPlot::GetPlotSize();
  const float p0 = ImPlot::PlotToPixels(ImPlotPoint(rg.zoom_x, 1.0)).x;
  const float a = std::clamp(std::min(p0, mp.x), pp.x, pp.x + ps.x), b = std::clamp(std::max(p0, mp.x), pp.x, pp.x + ps.x);
  if (ImGui::IsMouseDown(0)) {
    ImDrawList &dl = *ImPlot::GetPlotDrawList();
    ImPlot::PushPlotClipRect();
    dl.AddRectFilled(ImVec2(a, pp.y), ImVec2(b, pp.y + ps.y), IM_COL32(102, 204, 255, 40));
    for (const float x : {a, b})
      dl.AddLine(ImVec2(x, pp.y), ImVec2(x, pp.y + ps.y), IM_COL32(102, 204, 255, 160));
    ImPlot::PopPlotClipRect();
    ImGui::SetMouseCursor(ImGuiMouseCursor_ResizeEW);
    return;
  }
  rg.zooming = false;
  if (b - a < kZoomMin)
    return;
  const auto day = [&](float px) { return static_cast<int>(std::floor(ImPlot::PixelsToPlot(px, pp.y).x / static_cast<double>(kDayW))); };
  rg.d0 = std::clamp(day(a), 0, D - 1);
  rg.d1 = std::clamp(day(b) + 1, rg.d0 + 1, D);
}

// ---- VAL 主图: 标的各口滚动 band (填色 + kBandQ 条线共一个 item, 一键开关; 首次默认只开 BP)
//      + 各口绝对估值等值线 (同口各档共一个 item, 首次默认关) + 标的可成交线 (常显) + 悬停等值线高亮 + 图例矩阵 (最上层) + 横向框选;
//      只价格线进 fit (y 上限另抬 kValHeadroom), band / 等值线都不进 ----
void plot_val(LinesUIState &ui, const LinesDerived &d, const LinesView &v, const LinesAt &pin, PlotFrame &pf) {
  static ValLegendHl s_hl;
  const LinesSlice &sl = v.slice[pin.g][pin.r][kTrade];
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
      ImPlot::PlotLine(lb, bx, bd.y[j].data(), bn, ImPlotLineFlags{ImPlotLineFlags_SkipNaN} | ImPlotItemFlags_NoFit);
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
  plot_slice("##pf_val_px", sl);
  if (ImPlot::FitThisFrame()) {
    double hi = -HUGE_VAL;
    for (size_t i = 0; i < sl.size(); ++i)
      if (const double y = sl.y(i); std::isfinite(y))
        hi = std::max(hi, y);
    if (std::isfinite(hi))
      ImPlot::FitPointY(hi * (1.0 + kValHeadroom));
  }
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
  hover_contour(d, v, sl, on, s_hl.box);
  val_legend(v, ui.roll.key.years, s_hl);
  zoom_x(ui.range, static_cast<int>(d.dates.size()), s_hl.box);
}

} // namespace

// ---- 主图: NAV (plot_nav) ↔ VAL (plot_val) 同一框: 贴边 (窄内边距, 不标轴名); NAV = fit 键变 → 本帧 fit, VAL = 恒贴合 (不平移 / 缩放, 视野只随区间);
//      x 右端留出线尾小字 / 刻度 / 十字标签 ----
void RenderLinesPlot(LinesUIState &ui, const LinesAt &pin, bool nav, const ImVec2 &size) {
  const LinesDerived &d = ui.der.val;
  const LinesView &v = ui.view.val;
  const LinesRange rg = ui.range;                                 // 视图所切的区间 (VAL 框选本帧改 ui.range, 下帧才进视图)
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(2, 2)); // 贴边: 图区让给线
  ImPlot::PushStyleVar(ImPlotStyleVar_LabelPadding, ImVec2(2, 1));
  LinesFitKey fk{ui.view.gen, nav, ui.log_y, {}, ui.shown_key};
  if (nav)
    for (const std::vector<LinesRow> &rs : d.rows)
      for (const LinesRow &r : rs)
        fk.rows += picked(ui, kPageNav, r) ? '1' : '0';
  if (fk != ui.fit_key)
    ImPlot::SetNextAxesToFit();
  if (ImPlot::BeginPlot(nav ? "##portfolio" : "##pf_val", size, nav ? ImPlotFlags_None : ImPlotFlags_NoLegend | ImPlotFlags_NoBoxSelect)) {
    ui.fit_key = fk;
    // 线性: y 下限钉在 0 (净值 / 点位恒正), fit / 缩放只动上限; log: 正常
    const ImPlotAxisFlags fit = nav ? ImPlotAxisFlags_None : ImPlotAxisFlags_AutoFit;
    ImPlot::SetupAxes(nullptr, nullptr, fit, fit | (ui.log_y ? ImPlotAxisFlags_None : ImPlotAxisFlags_LockMin));
    setup_day_axis(d.dates);
    if (ui.log_y)
      ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    else
      ImPlot::GetCurrentPlot()->Axes[ImAxis_Y1].SetMin(0.0, /*force=*/true);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest);
    // fit 帧的视野还是上帧的 (区间刚变时可能整段在外): 小字不按视野筛, 全收来量留白宽度
    const ImPlotRect cur = ImPlot::GetPlotLimits(); // 先锁 setup, FitThisFrame 才定
    PlotFrame pf{ImPlot::FitThisFrame() ? ImPlotRect(-DBL_MAX, DBL_MAX, -DBL_MAX, DBL_MAX) : cur, {}, {}, {}, {}};
    if (nav)
      plot_nav(ui, d, v, pf);
    else if (pin.g >= 0)
      plot_val(ui, d, v, pin, pf);
    ui.shown_key = std::move(pf.shown);
    NowCross cross{};
    const bool crossed = now_cross(pf, d.dates, cross);
    const float cw = crossed ? now_cross_width(cross, kTagScale) + kTagGap : 0.0f; // 十字右壁标签占的宽 (小字 / 刻度让开)
    // 本帧 fit (自动 / 双击): x 右端再扩一段, 让区间末点右侧留出线尾小字 / 刻度 (同贴末点, 取宽者) + 十字标签的宽度
    if (ImPlot::FitThisFrame() && !pf.tags.empty() && rg.d1 > rg.d0) {
      const float tw = std::max(end_tags_width(pf.tags, kTagScale), marks_width(pf.marks)) + cw, W = ImPlot::GetPlotSize().x;
      const double x0 = x_of(static_cast<size_t>(rg.d0), kOpen), x1 = x_of(static_cast<size_t>(rg.d1), kOpen);
      if (W > 2.0f * tw)
        ImPlot::FitPointX(x1 + (x1 - x0) * tw / (W - tw));
    }
    draw_end_tags(pf.tags, kTagScale, tags_right() - cw);
    draw_marks(pf.marks, tags_right() - cw);
    if (crossed)
      draw_now_cross(cross, kTagScale, tags_right());
    ImPlot::EndPlot();
  }
  ImPlot::PopStyleVar(2);
}

// ---- 区间条 (range_bar): 底线 = 全段组合 (有冲击) / VAL 标的可成交线 (可成交指数 / 后复权收盘); VAL 主图横向框选同改它 (任意日) ----
void RenderLinesRange(LinesUIState &ui, const LinesAt &pin, bool nav, const ImVec2 &size) {
  const LinesDerived &d = ui.der.val;
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
  const Line &rc = nav ? *d.rows[kGroupStrat][0].line[kTrade] : pin.g >= 0 ? *d.row(pin).line[kTrade]
                                                                           : none;
  ImPlot::SetNextLineStyle(nav || pin.g < 0 ? kStratCol : row_color(d, pin, kTrade), 1.0f);
  ImPlot::PlotLine("##pf_range_line", rc.x.data(), rc.y.data(), static_cast<int>(rc.x.size()));
  range_bar(ui.range, d.dates, static_cast<double>(kDayW));
  ImPlot::EndPlot();
}

} // namespace GUI::Market