// Layers — 见头文件
#include "gui/util/Layers.hpp"
#include "gui/util/Chart.hpp" // DayAxis (日轴刻度)

#include "define/Annual.hpp"
#include "factor/Stat/Contract.hpp"
#include "implot_internal.h" // GImPlot (按 ID 查上帧 item 的显隐)

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <limits>
#include <string>
#include <unordered_map>

namespace GUI::Layer {

namespace {

constexpr int K = factor::stat::kGroups;
using Engine::kTails;
using Engine::layer_tail;
using factor::stat::end_group;
using factor::stat::kBot;
using factor::stat::kEnds;
using factor::stat::kTop;
// 组 k 是多空两端之一 (G20 / G1; LS 图例项 hover 时高亮)
constexpr bool is_end(int k) { return k == end_group(kTop) || k == end_group(kBot); }

constexpr ImVec4 kColNeutral(0.50f, 0.50f, 0.52f, 1.f);         // 中段
constexpr float kTailLayerWeight[kTails] = {1.0f, 0.75f, 0.5f}; // 尾部分层线宽 (组线 1.2): 越极端越细
constexpr float kTailLsWeight[kTails] = {2.0f, 1.5f, 1.0f};     // 尾部 LS 线宽 (LS 2.5): 同上
constexpr float kTailFillAlpha[kTails] = {0.08f, 0.15f, 0.24f}; // 相邻尾部分层线间色带 alpha: 越极端越重, 但不盖线
constexpr float kColEdge = 2.f;                                 // 两端各留几组显色
constexpr float kColGain = 8.f;                                 // tanh 增益: 越大台阶越陡 (8 → 台阶上相邻组 ~0.84 / ~0.5 / ~0.16)
constexpr float kColRamp = 0.3f;                                // 线性底坡占比: 中段色度 ≈ kColRamp·|s|, 中心灰起渐变

// y 范围: 显示中的分层簇 (簇 0 = 20 组恒显 + 开着的尾部级) 与显示中的 LS 线同一口径, 合并 min / max 加边; show[i] = LS 线 i 显示 (i ≥ 1 同管簇 i)
struct LayerRange {
  float y_lo, y_hi;
};
LayerRange layer_range(const LayerDerived &d, const bool *show) {
  float lo = d.lay_lo[0], hi = d.lay_hi[0];
  for (int i = 0; i < kLsLines; ++i)
    if (show[i]) {
      lo = std::min(lo, d.ls_lo[i]), hi = std::max(hi, d.ls_hi[i]);
      if (i)
        lo = std::min(lo, d.lay_lo[i]), hi = std::max(hi, d.lay_hi[i]);
    }
  const float margin = (hi > lo ? hi - lo : 1.f) * 0.03f;
  return {lo - margin, hi + margin};
}

// 右 1/4: 光标日截面 = 各分层线在光标日的值 (与左图同一量) 的横柱, 在共享 y 范围里均匀排 (G1 下 … G20 上); 柱色按组号, 柱长非单调即分层破绽.
// 开着的尾部级 (show[1 + j]) 两端各加一柱: 底端排 G1 之下, 顶端排 G20 之上, 越极端越靠外; hov[i] = 左图图例项 i hover → 该项的柱描白边.
// 须在 plot_layers 的子图内调 (y 由 LinkRows 跟左图)
void plot_section(const LayerDerived &d, const LayerRange &yr, const bool *show, const bool *hov, int cursor_day) {
  if (!ImPlot::BeginPlot("##section", ImVec2(), ImPlotFlags_NoMenus | ImPlotFlags_NoLegend))
    return;
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoTickMarks | ImPlotAxisFlags_NoGridLines,
                    ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoTickMarks);
  struct Slot {
    int line; // lay_ann 下标
    ImVec4 col;
    float w; // 柱宽系数
    bool hl;
    char tag[24];
  };
  Slot s[kLayerLines];
  int ns = 0;
  const auto tail = [&](int e, int j) {
    Slot &t = s[ns++];
    t = Slot{layer_tail(e, j), e == kTop ? kColTop : kColBottom, kTailLayerWeight[j], hov[1 + j], {}};
    std::snprintf(t.tag, sizeof(t.tag), "G%d 尾 %g%%", end_group(e) + 1, static_cast<double>(factor::stat::tail_frac(j)) * 100.0);
  };
  for (int j = kTails - 1; j >= 0; --j)
    if (show[1 + j])
      tail(kBot, j);
  for (int k = 0; k < K; ++k) {
    Slot &t = s[ns++];
    t = Slot{k, group_color(k), 1.f, hov[0] && is_end(k), {}};
    std::snprintf(t.tag, sizeof(t.tag), "G%d", k + 1);
  }
  for (int j = 0; j < kTails; ++j)
    if (show[1 + j])
      tail(kTop, j);
  float v[kLayerLines], v_lo = 0.f, v_hi = 0.f; // 含 0 (柱从 0 起)
  for (int i = 0; i < ns; ++i) {
    v[i] = d.lay_ann[s[i].line][static_cast<size_t>(cursor_day)];
    v_lo = std::min(v_lo, v[i]), v_hi = std::max(v_hi, v[i]);
  }
  const float margin = (v_hi > v_lo ? v_hi - v_lo : 1.f) * 0.03f;
  follow_limits(ImAxis_X1, v_lo - margin, v_hi + margin);
  const double zero = 0.0;
  ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
  ImPlot::PlotInfLines("##zero", &zero, 1);
  const double slot = (yr.y_hi - yr.y_lo) / ns; // y 范围均分 ns 槽
  for (int i = 0; i < ns; ++i) {                // 逐线一柱: PlotBars 一次只能一色; 已爆仓的线无柱
    if (std::isnan(v[i]))
      continue;
    const double x = v[i], y = yr.y_lo + (i + 0.5) * slot;
    ImPlot::SetNextFillStyle(s[i].col);
    if (s[i].hl)
      ImPlot::SetNextLineStyle(kColLS, kHiScale);
    char id[16];
    std::snprintf(id, sizeof(id), "##b%d", s[i].line);
    ImPlot::PlotBars(id, &x, &y, 1, slot * 0.8 * s[i].w, ImPlotBarsFlags_Horizontal);
  }
  if (ImPlot::IsPlotHovered()) {
    const int i = std::clamp(static_cast<int>(std::floor((ImPlot::GetPlotMousePos().y - yr.y_lo) / slot)), 0, ns - 1);
    if (std::isnan(v[i]))
      ImGui::SetTooltip("%s  爆仓 (光标日前某日收益 ≤ −100%%)", s[i].tag);
    else
      ImGui::SetTooltip("%s  %+.3f  (光标日前 %d 日对数累计, 按 %d 日折年, %%)", s[i].tag, v[i], d.win, d.win);
  }
  ImPlot::EndPlot();
}

} // namespace

ImVec4 lerp_color(float t) {
  constexpr float mid = 1.f - kColEdge * 2.f / static_cast<float>(K - 1); // 台阶中心 = 从边数第 kColEdge + 1 组的 |s|
  const auto step = [](float a) { return 0.5f * (1.f + std::tanh(kColGain * (a - mid))); };
  const float s = (t - 0.5f) * 2.f, a = std::abs(s);
  const float w = kColRamp * a + (1.f - kColRamp) * (step(a) - step(0.f)) / (step(1.f) - step(0.f));
  const ImVec4 &c = s < 0.f ? kColBottom : kColTop;
  return ImVec4(kColNeutral.x + (c.x - kColNeutral.x) * w, kColNeutral.y + (c.y - kColNeutral.y) * w, kColNeutral.z + (c.z - kColNeutral.z) * w, 1.f);
}
ImVec4 group_color(int k) { return lerp_color(static_cast<float>(k) / static_cast<float>(K - 1)); }

CompactLegend::CompactLegend() {
  ImPlot::PushStyleVar(ImPlotStyleVar_LegendPadding, ImVec2(4.f, 2.f));
  ImPlot::PushStyleVar(ImPlotStyleVar_LegendInnerPadding, ImVec2(2.f, 2.f));
  ImPlot::PushStyleVar(ImPlotStyleVar_LegendSpacing, ImVec2(2.f, 0.f));
}
CompactLegend::~CompactLegend() { ImPlot::PopStyleVar(3); }

void follow_limits(ImAxis ax, double lo, double hi) {
  static std::unordered_map<ImGuiID, ImPlotRange> last;
  assert(GImPlot->CurrentPlot);
  ImPlotRange &r = last[ImHashData(&ax, sizeof(ax), GImPlot->CurrentPlot->ID)];
  const bool changed = r.Min != lo || r.Max != hi;
  r = ImPlotRange(lo, hi);
  ImPlot::SetupAxisLimits(ax, lo, hi, changed ? ImPlotCond_Always : ImPlotCond_Once);
}

int date_formatter(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<misc::Ymd> *>(ud);
  if (dates.empty())
    return std::snprintf(buf, size, "%g", v);
  const int i = std::clamp(static_cast<int>(std::lround(v)), 0, static_cast<int>(dates.size()));
  if (i == 0)
    return std::snprintf(buf, size, "起点");
  return std::snprintf(buf, size, "%06d", dates[static_cast<size_t>(i) - 1] % 1000000);
}

void empty_plot(const char *title, const ImVec2 &size, const char *hint) {
  if (ImPlot::BeginPlot(title, size, ImPlotFlags_NoLegend | ImPlotFlags_NoMenus)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoGridLines, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoGridLines);
    ImPlot::SetupAxesLimits(0, 1, 0, 1, ImPlotCond_Always);
    ImPlot::PlotText(hint, 0.5, 0.5);
    ImPlot::EndPlot();
  }
}

// 前缀对数和 cum[c] = Σ_{d<c} ln(1 + r_d); 某日 r_d ≤ −100% (单只级尾部 / LS 空头暴涨) = 爆仓, 该线此后 NaN 不画; LS Sharpe = 日收益 Daily (define/Annual.hpp, 全仓同一口径)
void derive_layers(LayerDerived &d, std::vector<misc::Ymd> dates, const float *const *lay, const float *const *ls, const float *turn) {
  d = LayerDerived{};
  d.days = static_cast<int>(dates.size());
  assert(d.days >= 1);
  d.dates = std::move(dates);
  d.x_day.resize(static_cast<size_t>(d.days) + 1);
  for (int i = 0; i <= d.days; ++i)
    d.x_day[static_cast<size_t>(i)] = static_cast<float>(i);
  const auto cum_of = [&](const float *v, std::vector<double> &cum) {
    cum.assign(static_cast<size_t>(d.days) + 1, 0.0);
    for (int day = 0; day < d.days; ++day) {
      assert(!std::isnan(v[day]) && "日收益 NaN");
      if (v[day] <= -1.f) {
        std::fill(cum.begin() + day + 1, cum.end(), std::numeric_limits<double>::quiet_NaN());
        return;
      }
      cum[static_cast<size_t>(day) + 1] = cum[static_cast<size_t>(day)] + std::log1p(static_cast<double>(v[day]));
    }
  };
  for (int i = 0; i < kLayerLines; ++i)
    cum_of(lay[i], d.lay_cum[i]);
  for (int i = 0; i < kLsLines; ++i) {
    cum_of(ls[i], d.ls_cum[i]);
    d.ls_turn[i] = turn[i];
    Daily m;
    for (int day = 0; day < d.days; ++day)
      m.add(ls[i][day]);
    d.ls_sharpe[i] = static_cast<float>(m.sharpe());
  }
}

// 第 c 日 = 窗 [max(0, c − win), c) 对数和按 win 日复利年化 × 100 % (ann_log; 起点前不足 win 日按 0 补, 全部线同一折算; win = days 即从起点累计)
void roll_layers(LayerDerived &d, int win) {
  assert(d.days >= 1 && win >= 1 && win <= d.days);
  if (d.win == win)
    return;
  d.win = win;
  const size_t n = static_cast<size_t>(d.days) + 1, w = static_cast<size_t>(win);
  const auto roll = [&](const std::vector<double> &cum, std::vector<float> &ann, float &lo, float &hi) {
    ann.resize(n);
    for (size_t c = 0; c < n; ++c) {
      const float v = static_cast<float>(100.0 * ann_log(cum[c] - cum[c < w ? 0 : c - w], win));
      ann[c] = v;
      lo = std::min(lo, v), hi = std::max(hi, v); // NaN (爆仓) 不进极值
    }
  };
  std::fill(std::begin(d.lay_lo), std::end(d.lay_lo), 0.f);
  std::fill(std::begin(d.lay_hi), std::end(d.lay_hi), 0.f);
  std::fill(std::begin(d.ls_lo), std::end(d.ls_lo), 0.f);
  std::fill(std::begin(d.ls_hi), std::end(d.ls_hi), 0.f);
  for (int k = 0; k < K; ++k)
    roll(d.lay_cum[k], d.lay_ann[k], d.lay_lo[0], d.lay_hi[0]);
  for (int j = 0; j < kTails; ++j)
    for (int e = 0; e < kEnds; ++e)
      roll(d.lay_cum[layer_tail(e, j)], d.lay_ann[layer_tail(e, j)], d.lay_lo[1 + j], d.lay_hi[1 + j]);
  for (int i = 0; i < kLsLines; ++i)
    roll(d.ls_cum[i], d.ls_ann[i], d.ls_lo[i], d.ls_hi[i]);
}

// 左 3/4 时序: 20 组 + 尾部 2 × kTails 条 + 相邻尾部线间色带 + LS 系 (顶底 + kTails 条尾部), 同一 y 轴 (layer_range 按图例状态, 开关图例下一帧即重贴);
//   win < days 时先画光标日窗口 [光标 − win, 光标] 的淡色竖带 (= 右侧截面柱的取值区间); 一级尾部一个图例项 (默认隐藏) 管该级分层 + 色带 + LS + 右图截面柱;
//   光标 DragLineX 锚在交易日 (cursor_day ∈ [0, days], 整数吸附; 双击落点; 越界 → 末日); 右 1/4 = plot_section(光标日)
void plot_layers(const LayerDerived &d, bool absolute, int &cursor_day, const ImVec2 &size) {
  assert(d.win >= 1 && "先 roll_layers");
  if (cursor_day < 0 || cursor_day > d.days)
    cursor_day = d.days;
  char title[128];
  const char *mode = absolute ? "(绝对: lv)" : "(超额: lv − mkt)";
  if (d.win < d.days)
    std::snprintf(title, sizeof(title), "分层滚动 %d 日对数累计 %s###ts", d.win, mode);
  else
    std::snprintf(title, sizeof(title), "分层对数累计 %s###ts", mode);
  const int n = d.days + 1;
  float col_ratios[2] = {3.f, 1.f};
  LayerRange yr{};
  bool show[kLsLines] = {}, hov[kLsLines] = {};
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(0.f, ImPlot::GetStyle().PlotPadding.y));
  if (ImPlot::BeginSubplots("##layers", 1, 2, size, ImPlotSubplotFlags_NoTitle | ImPlotSubplotFlags_NoMenus | ImPlotSubplotFlags_NoResize | ImPlotSubplotFlags_LinkRows,
                            nullptr, col_ratios)) {
    ImVec4 legend_bg = ImPlot::GetStyleColorVec4(ImPlotCol_LegendBg); // 图例压在线上: 底色更透
    legend_bg.w *= 0.4f;
    ImPlot::PushStyleColor(ImPlotCol_LegendBg, legend_bg);
    if (ImPlot::BeginPlot(title, ImVec2(), ImPlotFlags_NoMenus)) {
      CompactLegend cl;
      { // 图例状态 → y 范围. item 按 "###id" 查 (与下面 Plot 的 label 同 ID); 不存在 = 首帧, 取默认显隐.
        // ImPlot 在 SetupFinish 才 PushOverrideID(Items.ID) 作 item 哈希种子, 这里还在 Setup 阶段, GetItem(label) 会用错种子 → 直接按同一种子哈希
        // hover = 上帧 EndPlot 记的 LegendHovered (ImPlot 自己 BeginItem 加粗同名线也读它); ImPlot 只管同名 item 的线, 组线 / 色带 / 右图截面柱按 hov 自己高亮
        ImPlotItemGroup &items = *GImPlot->CurrentItems;
        const auto look = [&](const char *id, bool dflt, bool &sh, bool &hv) {
          const ImPlotItem *it = items.GetItem(ImHashStr(id, 0, items.ID));
          sh = it ? it->Show : dflt;
          hv = it && it->Show && it->LegendHovered;
        };
        look("###ls", true, show[0], hov[0]);
        for (int j = 0; j < kTails; ++j) {
          char id[16];
          std::snprintf(id, sizeof(id), "###tls%d", j);
          look(id, false, show[1 + j], hov[1 + j]);
        }
        yr = layer_range(d, show);
      }
      ImPlot::SetupAxes(nullptr, absolute ? "对数年化(%)" : "超额对数年化(%)", ImPlotAxisFlags_None, ImPlotAxisFlags_None);
      ImPlot::SetupAxisFormat(ImAxis_Y1, kYTickFmt);
      DayAxis day_ax{d.dates, 1.0, 0.0, {}, date_formatter, const_cast<std::vector<misc::Ymd> *>(&d.dates)}; // 日 i 的日界 = 前一日收 x = i
      setup_day_axis(day_ax);
      follow_limits(ImAxis_X1, 0, std::max(1, d.days) * (1.0 + kCursorPad));
      follow_limits(ImAxis_Y1, yr.y_lo, yr.y_hi);
      ImPlot::SetupLegend(ImPlotLocation_SouthWest, ImPlotLegendFlags_None);
      ImPlot::SetupFinish();
      if (d.win < d.days) { // 光标日窗口带 (先画, 不遮线)
        const ImPlotRect lim = ImPlot::GetPlotLimits();
        ImPlot::PushPlotClipRect();
        const ImVec2 p0 = ImPlot::PlotToPixels(static_cast<double>(std::max(0, cursor_day - d.win)), lim.Y.Max),
                     p1 = ImPlot::PlotToPixels(static_cast<double>(cursor_day), lim.Y.Min);
        ImPlot::GetPlotDrawList()->AddRectFilled(p0, p1, IM_COL32(255, 128, 0, 28));
        ImPlot::PopPlotClipRect();
      }
      ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 1.2f);
      for (int k = 0; k < K; ++k) { // 顶底组线 = 顶底 LS 的分层: 随 LS 图例项高亮
        const bool hl = hov[0] && is_end(k);
        ImPlot::SetNextLineStyle(group_color(k), hl ? 1.2f * kHiScale : IMPLOT_AUTO);
        char id[16];
        std::snprintf(id, sizeof(id), "##g%d", k);
        ImPlot::PlotLine(id, d.x_day.data(), d.lay_ann[k].data(), n);
      }
      ImPlot::PopStyleVar();
      // 图例 (对仗 Portfolio, 等宽字体逐列定宽): "LS <tag:4>  年化=<+8.2f>%  夏普=<+6.2f>  年换手=<7.1f>倍", 值恒为全段 (不随窗口):
      // ann_log(ls_cum 末点, days) / ls_sharpe / ls_turn; tag = 占组比例 (顶底 = 100%, 整组); 末点 NaN = 爆仓 (某日收益 ≤ −100%)
      char ll[128];
      const auto ls_label = [&](const char *tag, int i, const char *id) {
        const double a = 100.0 * ann_log(d.ls_cum[i].back(), d.days);
        if (std::isnan(a))
          std::snprintf(ll, sizeof(ll), "LS %4s  年化=  爆仓    夏普=%+6.2f  年换手=%7.1f倍###%s", tag, d.ls_sharpe[i], d.ls_turn[i], id);
        else
          std::snprintf(ll, sizeof(ll), "LS %4s  年化=%+8.2f%%  夏普=%+6.2f  年换手=%7.1f倍###%s", tag, a, d.ls_sharpe[i], d.ls_turn[i], id);
      };
      ls_label("100%", 0, "ls");
      ImPlot::SetNextLineStyle(kColLS, 2.5f);
      ImPlot::PlotLine(ll, d.x_day.data(), d.ls_ann[0].data(), n);
      const ImVec4 end_col[kEnds] = {kColTop, kColBottom};
      for (int j = 0; j < kTails; ++j) { // 尾部: 一级一个图例项 (默认隐藏) 同时管该级的色带 + 两端分层线 + LS (同名 = 同一 item, 一起显隐; 图例色 = 最后画的 LS 白)
        char tag[8], id[8];
        std::snprintf(tag, sizeof(tag), "%g%%", static_cast<double>(factor::stat::tail_frac(j)) * 100.0);
        std::snprintf(id, sizeof(id), "tls%d", j);
        ls_label(tag, 1 + j, id);
        ImPlot::HideNextItem(true, ImPlotCond_Once);
        for (int e = 0; e < kEnds; ++e) { // 色带: 本级线与上一级线 (j = 0 时是该端的组线) 之间, 越极端越重
          const float *inner = (j ? d.lay_ann[layer_tail(e, j - 1)] : d.lay_ann[end_group(e)]).data();
          ImPlot::SetNextFillStyle(end_col[e], kTailFillAlpha[j] * (hov[1 + j] ? kHiScale : 1.f));
          ImPlot::PlotShaded(ll, d.x_day.data(), inner, d.lay_ann[layer_tail(e, j)].data(), n);
        }
        for (int e = 0; e < kEnds; ++e) { // 分层: 饱和顶 / 底色, 越极端越细
          ImPlot::SetNextLineStyle(end_col[e], kTailLayerWeight[j]);
          ImPlot::PlotLine(ll, d.x_day.data(), d.lay_ann[layer_tail(e, j)].data(), n);
        }
        ImPlot::SetNextLineStyle(kColLS, kTailLsWeight[j]); // LS: 同色, 越极端越细
        ImPlot::PlotLine(ll, d.x_day.data(), d.ls_ann[1 + j].data(), n);
      }
      { // 光标 (对仗 OrderFlow 的锚): 拖动跟手并吸附整日, 双击落点; 标注光标日期 + LS 线在该日的值
        double cx = cursor_day;
        if (ImPlot::DragLineX(0, &cx, kColCursor, 2.f))
          cursor_day = std::clamp(static_cast<int>(std::lround(cx)), 0, d.days);
        else if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0))
          cursor_day = std::clamp(static_cast<int>(std::lround(ImPlot::GetPlotMousePos().x)), 0, d.days);
        const float ls_c = d.ls_ann[0][static_cast<size_t>(cursor_day)];
        const std::string when = cursor_day ? misc::dense(d.dates[static_cast<size_t>(cursor_day) - 1]) : "起点";
        if (std::isnan(ls_c))
          ImPlot::Annotation(static_cast<double>(cursor_day), yr.y_lo, kColCursor, ImVec2(5, -15), true, "%s  LS 爆仓", when.c_str());
        else
          ImPlot::Annotation(static_cast<double>(cursor_day), ls_c, kColCursor, ImVec2(5, -15), true, "%s  LS %+.2f", when.c_str(), ls_c);
      }
      ImPlot::EndPlot();
    }
    ImPlot::PopStyleColor();
    plot_section(d, yr, show, hov, cursor_day);
    ImPlot::EndSubplots();
  }
  ImPlot::PopStyleVar();
}

} // namespace GUI::Layer
