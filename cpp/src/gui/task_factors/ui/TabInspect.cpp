// Tab Inspect — 见头文件
#include "gui/task_factors/ui/TabInspect.hpp"
#include "gui/Tasks.hpp" // StatusColor

#include "imgui.h"
#include "imgui_internal.h" // ArrowButtonEx (窄箭头)
#include "implot.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <mutex>
#include <string>

namespace GUI::Factors {

namespace {

constexpr int K = factor::stat::kGroups;

// 横轴刻度: 交易日下标 → YYMMDD (user_data = InspectDerived::dates)
int date_formatter(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<std::string> *>(ud);
  if (dates.empty())
    return std::snprintf(buf, size, "%g", v);
  const int i = std::clamp(static_cast<int>(std::floor(v)), 0, static_cast<int>(dates.size()) - 1);
  return std::snprintf(buf, size, "%s", dates[static_cast<size_t>(i)].c_str() + 2);
}

// 分层色: 两端两色 (G1 冷蓝 / G20 暖橙), 中间线性渐变 —— 单调, 一眼分出谁高谁低 (Jet 在中段来回翻色, 相邻组交织分不清)
constexpr ImVec4 kColBottom(0.25f, 0.55f, 1.00f, 1.f), kColTop(1.00f, 0.45f, 0.20f, 1.f), kColLS(1.f, 1.f, 1.f, 1.f);
ImVec4 lerp_color(float t) { // t ∈ [0, 1]: 0 = 冷蓝 (G1 / 最负), 1 = 暖橙 (G20 / 最正)
  return ImVec4(kColBottom.x + (kColTop.x - kColBottom.x) * t, kColBottom.y + (kColTop.y - kColBottom.y) * t,
                kColBottom.z + (kColTop.z - kColBottom.z) * t, 1.f);
}
ImVec4 group_color(int k) { return lerp_color(static_cast<float>(k) / static_cast<float>(K - 1)); }

// 小巧箭头: 窄 (0.6 字高) × 行高 (与同行 Button 对齐)
bool arrow(const char *id, ImGuiDir dir) { return ImGui::ArrowButtonEx(id, dir, ImVec2(ImGui::GetFontSize() * 0.6f, ImGui::GetFrameHeight())); }

// 紧凑切换控件 (替代下拉): "label ◂ value ▸", 两头箭头循环切; 只有两项时省掉箭头, 点值文字直接 toggle. 返回是否切了.
// 整体一个 Group, 调用方紧接着 IsItemHovered() 对整个控件生效
bool cycler(const char *label, int &idx, int n, const char *value) {
  assert(n >= 2 && idx >= 0 && idx < n);
  ImGui::PushID(label);
  ImGui::BeginGroup();
  ImGui::AlignTextToFramePadding();
  ImGui::TextDisabled("%s", label);
  ImGui::SameLine(0.f, 4.f);
  int delta = 0;
  if (n > 2) {
    if (arrow("##prev", ImGuiDir_Left))
      delta = -1;
    ImGui::SameLine(0.f, 2.f);
    ImGui::TextUnformatted(value);
    ImGui::SameLine(0.f, 2.f);
    if (arrow("##next", ImGuiDir_Right))
      delta = 1;
  } else if (ImGui::SmallButton(value)) {
    delta = 1;
  }
  if (delta)
    idx = (idx + delta + n) % n;
  ImGui::EndGroup();
  ImGui::PopID();
  return delta != 0;
}

// 单调三次 Hermite (Fritsch–Carlson) 把节点 (x 严格升序) 稠密采样成折线: 圆滑且不过冲 (期限结构点少, 直连折线生硬)
void pchip_dense(const std::vector<double> &x, const std::vector<double> &y, std::vector<double> &xs, std::vector<double> &ys) {
  const int n = static_cast<int>(x.size());
  assert(y.size() == x.size());
  xs.clear();
  ys.clear();
  if (n < 2) {
    xs = x;
    ys = y;
    return;
  }
  std::vector<double> h(static_cast<size_t>(n) - 1), del(static_cast<size_t>(n) - 1), m(static_cast<size_t>(n), 0.0);
  for (int i = 0; i + 1 < n; ++i) {
    h[static_cast<size_t>(i)] = x[static_cast<size_t>(i) + 1] - x[static_cast<size_t>(i)];
    assert(h[static_cast<size_t>(i)] > 0.0);
    del[static_cast<size_t>(i)] = (y[static_cast<size_t>(i) + 1] - y[static_cast<size_t>(i)]) / h[static_cast<size_t>(i)];
  }
  if (n == 2) {
    m[0] = m[1] = del[0];
  } else {
    for (int i = 1; i + 1 < n; ++i) {
      const double d0 = del[static_cast<size_t>(i) - 1], d1 = del[static_cast<size_t>(i)];
      if (d0 * d1 <= 0.0)
        continue; // 极值点斜率 0
      const double w1 = 2.0 * h[static_cast<size_t>(i)] + h[static_cast<size_t>(i) - 1], w2 = h[static_cast<size_t>(i)] + 2.0 * h[static_cast<size_t>(i) - 1];
      m[static_cast<size_t>(i)] = (w1 + w2) / (w1 / d0 + w2 / d1);
    }
    auto endpoint = [](double h0, double h1, double d0, double d1) { // 三点外推 + 保形钳位
      const double s = ((2.0 * h0 + h1) * d0 - h0 * d1) / (h0 + h1);
      if (s * d0 <= 0.0)
        return 0.0;
      if (d0 * d1 <= 0.0 && std::abs(s) > 3.0 * std::abs(d0))
        return 3.0 * d0;
      return s;
    };
    m[0] = endpoint(h[0], h[1], del[0], del[1]);
    m[static_cast<size_t>(n) - 1] = endpoint(h[static_cast<size_t>(n) - 2], h[static_cast<size_t>(n) - 3], del[static_cast<size_t>(n) - 2], del[static_cast<size_t>(n) - 3]);
  }
  constexpr int kPerSeg = 16;
  xs.reserve(static_cast<size_t>(n - 1) * kPerSeg + 1);
  ys.reserve(xs.capacity());
  for (int i = 0; i + 1 < n; ++i) {
    const size_t u = static_cast<size_t>(i);
    for (int j = 0; j < kPerSeg; ++j) {
      const double t = static_cast<double>(j) / kPerSeg, t2 = t * t, t3 = t2 * t;
      const double h00 = 2 * t3 - 3 * t2 + 1, h10 = t3 - 2 * t2 + t, h01 = -2 * t3 + 3 * t2, h11 = t3 - t2;
      xs.push_back(x[u] + t * h[u]);
      ys.push_back(h00 * y[u] + h10 * h[u] * m[u] + h01 * y[u + 1] + h11 * h[u] * m[u + 1]);
    }
  }
  xs.push_back(x.back());
  ys.push_back(y.back());
}

// 一级 Row[T] (选定 hold) → 日级序列 + IC 分布. 累计按 1/h 折算: 相邻 h 行的标签是同一段收益, Σ_t r_t / h = h 个相位非重叠链的平均;
// 再按回测长度折成等效年化 (× kDaysPerYear / days, 单位 %): 末点 = 年化收益, 不同区间长度 / 持有期可比;
// IC 分布 = ok 行的 ic 逐行进 KLL (与 summarize 的 ic_mean/std/skew/kurt 同一组样本)
void derive(const InspectResult &res, int sel, bool absolute, InspectDerived &d) {
  d = InspectDerived{};
  if (res.key.empty() || !res.error.empty() || res.rows.empty())
    return;
  assert(sel >= 0 && sel < res.hd.n);
  const int T = res.scope.T, days = res.scope.days, S = factor::kSegLen;
  assert(T == days * S && days >= 1);
  const int hold = res.hd.h[sel];
  const double h = factor::stat::hold_minutes(hold);
  const double ann = 100.0 * factor::stat::kDaysPerYear / days;
  const factor::stat::Row *r = res.rows.data() + static_cast<size_t>(sel) * T;
  d.days = days;
  d.dates = res.dates;
  d.x_day.resize(static_cast<size_t>(days) + 1);
  for (int i = 0; i <= days; ++i)
    d.x_day[static_cast<size_t>(i)] = static_cast<float>(i);
  for (int k = 0; k < K; ++k)
    d.grp_cum[k].assign(static_cast<size_t>(days) + 1, 0.f);
  d.ls_cum.assign(static_cast<size_t>(days) + 1, 0.f);
  std::vector<float> ics;
  ics.reserve(static_cast<size_t>(T));
  double gc[K] = {}, lc = 0.0;
  for (int day = 0; day < days; ++day) {
    double g[K] = {}, ls = 0.0;
    for (int t = day * S; t < (day + 1) * S; ++t) {
      const factor::stat::Row &w = r[t];
      if (!w.ok)
        continue;
      ics.push_back(w.ic);
      ls += w.ls;
      for (int k = 0; k < K; ++k)
        if (w.cnt[k]) // TS 口径组可空: 该刻无仓, 贡献 0
          g[k] += w.grp[k] / w.cnt[k] + (absolute ? w.mkt : 0.0);
    }
    for (int k = 0; k < K; ++k) {
      gc[k] += g[k] / h;
      d.grp_cum[k][static_cast<size_t>(day) + 1] = static_cast<float>(gc[k] * ann);
    }
    lc += ls / h;
    d.ls_cum[static_cast<size_t>(day) + 1] = static_cast<float>(lc * ann);
  }
  // y 范围: 分层线 min / max (含起点 0); LS 不参与定范围, 而是平移到最低点贴 lo (ls_off), 跨度若超过分层跨度则把 hi 抬到装下
  float lo = 0.f, hi = 0.f, ls_lo = 0.f, ls_hi = 0.f;
  for (int k = 0; k < K; ++k)
    for (const float v : d.grp_cum[k])
      lo = std::min(lo, v), hi = std::max(hi, v);
  for (const float v : d.ls_cum)
    ls_lo = std::min(ls_lo, v), ls_hi = std::max(ls_hi, v);
  hi = std::max(hi, lo + (ls_hi - ls_lo));
  d.ls_off = lo - ls_lo;
  const float margin = (hi > lo ? hi - lo : 1.f) * 0.03f;
  d.y_lo = lo - margin;
  d.y_hi = hi + margin;
  KLLcache kll(analysis::kAggKllCapacity, analysis::kAggKllResolution);
  kll.addBatch(ics);
  d.ic_pdf.fill(kll, 3); // n < 3 与 summarize 同口径: 无 stat 也无 PDF
  d.hs = res.hold[sel];
  d.valid = true;
}

// 空图占位 (统一样式)
void empty_plot(const char *title, const ImVec2 &size, const char *hint) {
  if (ImPlot::BeginPlot(title, size, ImPlotFlags_NoLegend | ImPlotFlags_NoMenus)) {
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoGridLines, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoGridLines);
    ImPlot::SetupAxesLimits(0, 1, 0, 1, ImPlotCond_Always);
    ImPlot::PlotText(hint, 0.5, 0.5);
    ImPlot::EndPlot();
  }
}

// 左上: 1×2 子图 (LinkRows 共 y 轴, 横向 PlotPadding 置 0 → 两图无缝相接), 刻度单位 % (derive 已 ×100, 标在 y 轴 label)
//   左 3/4 时序: 分层累计 + LS. y 范围按分层线定 (d.y_lo / y_hi); LS 平移 +ls_off, 最低点贴到范围底 (跨度更大时范围已抬高装下), 刚好填满
//   右 1/4 期末截面: 各组末日累计的横柱, 按组号在共享 y 范围里均匀排 (G1 下 … G20 上), 长 = 末值, x 范围 = y 范围 (只同步 range, 长度与左图同尺);
//   同色, 不标组号 (颜色即顺序)
void plot_layers(const InspectDerived &d, bool absolute, const ImVec2 &size) {
  char title[128];
  std::snprintf(title, sizeof(title), "分层累计 (等效年化) %s  h=%s  mono %+.3f###ts", absolute ? "(绝对 lv)" : "(超额 lv − mkt)",
                factor::stat::hold_name(d.hs.hold).c_str(), d.hs.mono);
  const int n = d.days + 1;
  float col_ratios[2] = {3.f, 1.f};
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(0.f, ImPlot::GetStyle().PlotPadding.y));
  if (ImPlot::BeginSubplots("##layers", 1, 2, size, ImPlotSubplotFlags_NoTitle | ImPlotSubplotFlags_NoMenus | ImPlotSubplotFlags_NoResize | ImPlotSubplotFlags_LinkRows,
                            nullptr, col_ratios)) {
    if (ImPlot::BeginPlot(title, ImVec2(), ImPlotFlags_NoMenus)) {
      ImPlot::SetupAxes(nullptr, absolute ? "年化 Σ ret / h  (%)" : "年化 Σ excess / h  (%)", ImPlotAxisFlags_None, ImPlotAxisFlags_None);
      ImPlot::SetupAxisFormat(ImAxis_X1, date_formatter, const_cast<std::vector<std::string> *>(&d.dates));
      ImPlot::SetupAxisLimits(ImAxis_X1, 0, std::max(1, d.days), ImPlotCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, d.y_lo, d.y_hi, ImPlotCond_Always);
      ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
      ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 1.2f);
      for (int k = 0; k < K; ++k) {
        ImPlot::SetNextLineStyle(group_color(k));
        char id[16];
        std::snprintf(id, sizeof(id), "##g%d", k);
        ImPlot::PlotLine(id, d.x_day.data(), d.grp_cum[k].data(), n);
      }
      ImPlot::PopStyleVar();
      char ls[64];
      std::snprintf(ls, sizeof(ls), "LS  SR %+.2f  t %+.2f###ls", d.hs.sharpe, d.hs.ls_t);
      struct Shift {
        const float *y;
        double off;
      } shift{d.ls_cum.data(), d.ls_off};
      ImPlot::SetNextLineStyle(kColLS, 2.5f);
      ImPlot::PlotLineG(
          ls,
          [](int i, void *ud) {
            const Shift &s = *static_cast<const Shift *>(ud);
            return ImPlotPoint(i, s.y[i] + s.off);
          },
          &shift, n);
      if (ImPlot::IsPlotHovered()) { // 鼠标所在日: 各组 / LS 累计值 (LS 显示原值, 不含平移)
        const ImPlotPoint mp = ImPlot::GetPlotMousePos();
        const int di = std::clamp(static_cast<int>(std::floor(mp.x)), 0, d.days - 1);
        ImGui::BeginTooltip();
        ImGui::Text("%s  (day %d)  年化 %%", d.dates[static_cast<size_t>(di)].c_str(), di + 1);
        ImGui::Text("LS  %+.3f", d.ls_cum[static_cast<size_t>(di) + 1]);
        ImGui::Separator();
        for (int k = K - 1; k >= 0; --k)
          ImGui::TextColored(group_color(k), "G%-2d %+.3f", k + 1, d.grp_cum[k][static_cast<size_t>(di) + 1]);
        ImGui::EndTooltip();
      }
      ImPlot::EndPlot();
    }
    if (ImPlot::BeginPlot("##end", ImVec2(), ImPlotFlags_NoMenus | ImPlotFlags_NoLegend)) {
      ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoTickMarks | ImPlotAxisFlags_NoGridLines,
                        ImPlotAxisFlags_NoTickLabels | ImPlotAxisFlags_NoTickMarks);
      ImPlot::SetupAxisLimits(ImAxis_X1, d.y_lo, d.y_hi, ImPlotCond_Always); // x 范围 = y 范围 (y 由 LinkRows 跟左图)
      const double zero = 0.0;
      ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
      ImPlot::PlotInfLines("##zero", &zero, 1);
      const double slot = (d.y_hi - d.y_lo) / K;     // y 范围均分 K 槽, G1 最下 … G20 最上
      float v_lo = d.grp_cum[0].back(), v_hi = v_lo; // 柱色按取值: 最负 = G1 色, 最正 = G20 色, 中间渐变 (左图按组号着色; 两边对照直接看单调性破绽)
      for (int k = 1; k < K; ++k)
        v_lo = std::min(v_lo, d.grp_cum[k].back()), v_hi = std::max(v_hi, d.grp_cum[k].back());
      for (int k = 0; k < K; ++k) { // 逐组一柱: PlotBars 一次只能一色
        const double v = d.grp_cum[k].back(), y = d.y_lo + (k + 0.5) * slot;
        ImPlot::SetNextFillStyle(lerp_color(v_hi > v_lo ? (static_cast<float>(v) - v_lo) / (v_hi - v_lo) : 0.5f));
        char id[16];
        std::snprintf(id, sizeof(id), "##b%d", k);
        ImPlot::PlotBars(id, &v, &y, 1, slot * 0.8, ImPlotBarsFlags_Horizontal);
      }
      if (ImPlot::IsPlotHovered()) {
        const int k = std::clamp(static_cast<int>(std::floor((ImPlot::GetPlotMousePos().y - d.y_lo) / slot)), 0, K - 1);
        ImGui::SetTooltip("G%d  %+.3f  (年化 %%, 期末)", k + 1, d.grp_cum[k].back());
      }
      ImPlot::EndPlot();
    }
    ImPlot::EndSubplots();
  }
  ImPlot::PopStyleVar();
}

// 右上: 期限结构 = 全部持有期的 L (G20 做多超额) / S (G1 做多超额) / LS 均值, PCHIP 圆滑折线 + 节点圆点; L / S 默认隐藏, 点图例切.
// 口径 = 全 universe 全时刻池化 (HoldStat::grp = Σ 组内超额 / Σ 组样本数; ls_mean = 逐 t ls 的时间均值), 不是逐资产信号平均.
// hs[n_hold] 来自 Inspect 结果或文件 stat; n < 3 的持有期不作节点
void plot_term(const factor::stat::HoldStat *hs, int n_hold, int sel_hold, const char *source, const ImVec2 &size) {
  char title[128];
  std::snprintf(title, sizeof(title), "期限结构: 年化池化均值超额 / 持有期  (%s)###term", source);
  if (!ImPlot::BeginPlot(title, size, ImPlotFlags_NoMenus))
    return;
  std::vector<double> pos(static_cast<size_t>(n_hold));
  std::vector<std::string> names(static_cast<size_t>(n_hold));
  std::vector<const char *> labels(static_cast<size_t>(n_hold));
  std::vector<double> xk, yk[3]; // 节点: [L / S / LS]
  for (int i = 0; i < n_hold; ++i) {
    pos[static_cast<size_t>(i)] = i;
    names[static_cast<size_t>(i)] = factor::stat::hold_name(hs[i].hold);
    labels[static_cast<size_t>(i)] = names[static_cast<size_t>(i)].c_str();
    if (hs[i].n < 3)
      continue;
    xk.push_back(i);
    const double ann = 100.0 * factor::stat::periods_per_year(hs[i].hold); // 每期均值 × 每年期数 → 年化 %, 跨持有期可比
    yk[0].push_back(hs[i].grp[K - 1] * ann);
    yk[1].push_back(hs[i].grp[0] * ann);
    yk[2].push_back(hs[i].ls_mean * ann);
  }
  ImPlot::SetupAxes("hold", "年化均值超额 (%)", ImPlotAxisFlags_None, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupAxisTicks(ImAxis_X1, pos.data(), n_hold, labels.data());
  ImPlot::SetupAxisLimits(ImAxis_X1, -0.6, n_hold - 0.4, ImPlotCond_Always);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  const char *items[3] = {"L (G20 long)", "S (G1 long)", "LS"};
  const ImVec4 cols[3] = {kColTop, kColBottom, kColLS};
  std::vector<double> xs, ys;
  for (int c = 0; c < 3; ++c) {
    pchip_dense(xk, yk[c], xs, ys);
    if (c < 2)
      ImPlot::HideNextItem(true, ImPlotCond_Once); // 默认只显 LS, 图例可点开
    ImPlot::SetNextLineStyle(cols[c], c == 2 ? 2.5f : 1.5f);
    ImPlot::PlotLine(items[c], xs.data(), ys.data(), static_cast<int>(xs.size()));
    ImPlot::SetNextMarkerStyle(ImPlotMarker_Circle, 3.f, cols[c], IMPLOT_AUTO, cols[c]);
    ImPlot::PlotScatter(items[c], xk.data(), yk[c].data(), static_cast<int>(xk.size())); // 同名 → 同一图例项, 一起显隐
  }
  if (sel_hold >= 0 && sel_hold < n_hold)
    ImPlot::TagX(static_cast<double>(sel_hold), ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "sel");
  if (ImPlot::IsPlotHovered()) {
    const ImPlotPoint mp = ImPlot::GetPlotMousePos();
    const int i = static_cast<int>(std::lround(mp.x));
    if (i >= 0 && i < n_hold) {
      const factor::stat::HoldStat &h = hs[i];
      ImGui::BeginTooltip();
      const double ann = 100.0 * factor::stat::periods_per_year(h.hold);
      if (h.n < 3)
        ImGui::TextDisabled("h=%s  n=%d (不足)", labels[static_cast<size_t>(i)], h.n);
      else
        ImGui::Text("h=%-5s n=%d/%d  rIC %+.4f IR %+.3f t %+.2f pos %.2f | LS %+.5f t %+.2f pos %.2f SR %+.2f β %+.3f | mono %+.3f | rAC %+.3f\n"
                    "年化 %%:  L %+.2f  S %+.2f  LS %+.2f",
                    labels[static_cast<size_t>(i)], h.n, h.n_ac, h.ic_mean, h.icir, h.ic_t, h.ic_pos, h.ls_mean, h.ls_t, h.ls_pos, h.sharpe,
                    h.beta, h.mono, h.rank_ac, h.grp[K - 1] * ann, h.grp[0] * ann, h.ls_mean * ann);
      ImGui::EndTooltip();
    }
  }
  ImPlot::EndPlot();
}

// 左下: IC 分布 — 逐行 rank IC 当随机变量, KLL 导出的 PDF (阴影 + 线) + 0 线 + 均值线; 标题的矩与顶部 stat 行同一组样本
void plot_ic(const InspectDerived &d, const ImVec2 &size) {
  char title[160];
  std::snprintf(title, sizeof(title), "IC 分布 (逐行 rank IC)  mean %+.4f  std %.4f  skew %+.2f  kurt %+.2f  pos %.2f  n=%d###ic", d.hs.ic_mean, d.hs.ic_std,
                d.hs.ic_skew, d.hs.ic_kurt, d.hs.ic_pos, d.hs.n);
  if (!ImPlot::BeginPlot(title, size, ImPlotFlags_NoMenus))
    return;
  ImPlot::SetupAxes("IC", "density", ImPlotAxisFlags_None, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupAxisLimits(ImAxis_X1, -0.5, 0.5, ImPlotCond_Always); // 固定 x 范围, 0 居中: 换持有期 / 因子时分布位置可直接比
  ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_None);
  const analysis::AggPdf &p = d.ic_pdf;
  const int np = static_cast<int>(p.n_pts);
  if (np >= 2) {
    const ImVec4 col(0.4f, 0.7f, 1.0f, 1.0f);
    ImPlot::SetNextFillStyle(col, 0.35f);
    ImPlot::PlotShaded("PDF", p.x.data(), p.y.data(), np, 0.0);
    ImPlot::SetNextLineStyle(col, 2.0f);
    ImPlot::PlotLine("PDF", p.x.data(), p.y.data(), np);
  }
  const double zero = 0.0, mean = d.hs.ic_mean;
  ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
  ImPlot::PlotInfLines("##zero", &zero, 1);
  ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.75f, 0.3f, 1.0f), 1.5f);
  ImPlot::PlotInfLines("mean", &mean, 1);
  if (np >= 2 && ImPlot::IsPlotHovered()) {
    const ImPlotPoint mp = ImPlot::GetPlotMousePos();
    const float *lo = std::lower_bound(p.x.data(), p.x.data() + np, static_cast<float>(mp.x));
    const int i = std::clamp(static_cast<int>(lo - p.x.data()), 0, np - 1);
    ImGui::SetTooltip("IC %+.4f  density %.3f", p.x[static_cast<size_t>(i)], p.y[static_cast<size_t>(i)]);
  }
  ImPlot::EndPlot();
}

// 高光行 (fui.view_file) 短锁拷出 (rows 由 worker 重扫时整体替换); 顺带给出下一个有效行的文件名 (箭头循环切因子用). 找不到 → false
bool view_row(FactorsService &fsvc, const std::string &file, FactorRow &row, std::string *prev_file = nullptr, std::string *next_file = nullptr) {
  std::lock_guard<std::mutex> lock(fsvc.mutex);
  const auto &rows = fsvc.rows;
  const int n = static_cast<int>(rows.size());
  int cur = -1;
  for (int i = 0; i < n; ++i)
    if (rows[static_cast<size_t>(i)].file == file) {
      cur = i;
      break;
    }
  if (cur < 0)
    return false;
  row = rows[static_cast<size_t>(cur)];
  const auto neighbor = [&](int dir, std::string &out) { // 从相邻行起按 dir 绕一圈找第一个有效 alpha 行; 没有 → 空
    out.clear();
    for (int step = 1; step < n; ++step) {
      const FactorRow &r = rows[static_cast<size_t>(((cur + dir * step) % n + n) % n)];
      if (r.error.empty()) {
        out = r.file;
        return;
      }
    }
  };
  if (prev_file)
    neighbor(-1, *prev_file);
  if (next_file)
    neighbor(+1, *next_file);
  return true;
}

} // namespace

bool InspectAutoRequest(FactorsService &fsvc, InspectService &isvc, const FactorsUIState &fui, InspectUIState &ui, const FactorsUIContext &ctx) {
  if (fui.view_file.empty() || !ctx.axis_ready || isvc.feats().labels.empty())
    return false;
  const InspectStatus st = isvc.status();
  if (st == InspectStatus::Loading || st == InspectStatus::Running)
    return false;
  FactorRow row;
  if (!view_row(fsvc, fui.view_file, row) || !row.error.empty())
    return false;
  InspectRequest probe;
  probe.file = row.file, probe.expr = row.expr, probe.frame = row.frame;
  probe.universe = ctx.universe, probe.start_date = ctx.start_date, probe.end_date = ctx.end_date;
  probe.impact_amt = ui.impact_amt, probe.sell_impact = ui.impact_amt > 0 ? ctx.sell_impact : 0.0;
  const std::string key = probe.key();
  if (ui.last_req_key == key) // 取消过 / 已发过的 key 不反复起 (Compute 手动)
    return false;
  {
    std::lock_guard<std::mutex> lock(isvc.mutex);
    if (isvc.result.key == key)
      return false;
  }
  ui.last_req_key = key;
  ui.req_row = row;
  ui.req_reload = false;
  return true;
}

int RenderTabInspect(FactorsService &fsvc, InspectService &isvc, FactorsUIState &fui, InspectUIState &ui, const FactorsUIContext &ctx) {
  int action = 0;
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Factor:");
  ImGui::SameLine();
  if (fui.view_file.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "(无) 在 Factors 页点一行高光");
    return 0;
  }
  // sel = 选中行 (view_file; 箭头 / 自动起算 / Compute 的对象); row = 显示行 = 现结果的因子 (切因子期间整页继续画旧因子, 新结果到了一次性换),
  // 无结果 (或结果因子已不在表里) 时 = sel
  FactorRow sel;
  std::string prev_file, next_file;
  if (!view_row(fsvc, fui.view_file, sel, &prev_file, &next_file)) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s (重扫中 / 已不在)", fui.view_file.c_str());
    return 0;
  }
  FactorRow row = sel;
  {
    std::string res_file;
    {
      std::lock_guard<std::mutex> lock(isvc.mutex);
      if (isvc.result.error.empty())
        res_file = isvc.result.file;
    }
    FactorRow r;
    if (!res_file.empty() && res_file != sel.file && view_row(fsvc, res_file, r))
      row = r;
  }
  const bool switching = row.file != sel.file;
  if (prev_file.empty())
    ImGui::BeginDisabled();
  if (arrow("##prev_factor", ImGuiDir_Left)) // 不切页换因子: 按表序循环到相邻有效行 (自动起算随之)
    fui.view_file = prev_file;
  if (prev_file.empty())
    ImGui::EndDisabled();
  ImGui::SameLine(0.f, 2.f);
  if (next_file.empty())
    ImGui::BeginDisabled();
  if (arrow("##next_factor", ImGuiDir_Right))
    fui.view_file = next_file;
  if (next_file.empty())
    ImGui::EndDisabled();
  ImGui::SameLine();
  ImGui::Text("%s  %s", row.name_cn.c_str(), row.name_en.c_str());
  if (ImGui::IsItemHovered())
    tip("%s/%s\n• ◂ ▸ 按 Factors 表序循环切相邻有效因子", ctx.factor_dir.c_str(), row.file.c_str());
  if (switching) {
    ImGui::SameLine();
    if (sel.error.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "→ %s  %s (计算中…)", sel.name_cn.c_str(), sel.name_en.c_str());
    else
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "→ %s BROKEN: %s", sel.name_en.c_str(), sel.error.c_str());
  }
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  if (!row.error.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "BROKEN: %s", row.error.c_str());
    return 0;
  }
  ImGui::Text("[%s] %s", factor::stat::frame_name(row.frame), row.expr.c_str());
  if (!row.note.empty())
    ImGui::TextDisabled("%s", row.note.c_str());

  // ---- 控件行 ----
  const FeatureTable &ft = isvc.feats();
  const InspectStatus st = isvc.status();
  const bool busy = st == InspectStatus::Loading || st == InspectStatus::Running;
  const int n_hold = static_cast<int>(ft.labels.size());
  ui.hold_idx = std::clamp(ui.hold_idx, 0, std::max(0, n_hold - 1));
  const int cur_hold = n_hold ? ft.labels[static_cast<size_t>(ui.hold_idx)].hold : 0;
  {
    bool impact_ok = ui.impact_amt == 0; // 字段表没有该档 (特征库换过) → 回毛
    for (const CostCol &cc : ft.costs)
      impact_ok |= cc.amt == ui.impact_amt;
    if (!impact_ok)
      ui.impact_amt = 0;
  }
  if (n_hold >= 2)
    cycler("Hold", ui.hold_idx, n_hold, factor::stat::hold_name(cur_hold).c_str());
  else
    ImGui::TextDisabled("Hold %s", n_hold ? factor::stat::hold_name(cur_hold).c_str() : "-");
  if (ImGui::IsItemHovered())
    tip("分层累计 / IC 分布 / 顶部 stat 看哪个持有期\n"
        "• 只影响显示, 一次算全部持有期");
  ImGui::SameLine();
  { // 档位 0 = 无, i ≥ 1 = costs[i − 1]
    int imp_idx = 0;
    for (int i = 0; i < static_cast<int>(ft.costs.size()); ++i)
      if (ft.costs[static_cast<size_t>(i)].amt == ui.impact_amt)
        imp_idx = i + 1;
    const std::string cur = ui.impact_amt ? std::to_string(ui.impact_amt) + "w" : "无";
    if (ft.costs.empty())
      ImGui::TextDisabled("冲击 无");
    else if (cycler("冲击", imp_idx, static_cast<int>(ft.costs.size()) + 1, cur.c_str()))
      ui.impact_amt = imp_idx ? ft.costs[static_cast<size_t>(imp_idx) - 1].amt : 0;
  }
  if (ImGui::IsItemHovered())
    tip("标签口径\n"
        "• 无: 价格收益, entry 中间价 → exit 分钟 VWAP (默认, 与 Factors Run 同)\n"
        "• <amt>w: 再扣建仓冲击 (lb_cost_buy/sell_<amt>w: 同一盘口吃 amt 万的 VWAP 偏离中间价) + 平仓固定冲击 (Config %.4f)\n"
        "• 净标签即兴算不缓存: 换档只重算 Stat, 不重读库; 吃不到的格 (NaN) 无效",
        ctx.sell_impact);
  ImGui::SameLine();
  {
    int abs_idx = ui.absolute ? 1 : 0;
    if (cycler("分层", abs_idx, 2, ui.absolute ? "绝对" : "超额"))
      ui.absolute = abs_idx == 1;
  }
  if (ImGui::IsItemHovered())
    tip("分层累计图的口径 (只影响显示)\n"
        "• 超额: 组均 (lv − mkt), 围着 0 看形状\n"
        "• 绝对: 组均 lv, 含市场\n"
        "• LS = top(lv) + bottom(sv), 本就市场中性, 两种口径同一条");
  ImGui::SameLine();
  const bool can_run = !busy && ctx.axis_ready && n_hold > 0 && sel.error.empty();
  if (!can_run)
    ImGui::BeginDisabled();
  if (ImGui::Button("Compute", ImVec2(80, 0))) { // 算的是选中行
    action = 1;
    ui.req_row = sel;
    ui.req_reload = true;
  }
  if (!can_run)
    ImGui::EndDisabled();
  if (ImGui::IsItemHovered())
    tip(ctx.axis_ready ? "弃缓存整体重算这一个因子: 重读特征库 → eval (CPU) → Stat, 留一级 Row[H][T]\n"
                         "• 换因子会自动起算, 只补缺的特征; 此按钮 = 强制全量"
                       : "资产轴未就绪, 读不了特征库\n"
                         "• 先在 Database 页扫描");
  ImGui::SameLine();
  if (!busy)
    ImGui::BeginDisabled();
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    action = -1;
  if (!busy)
    ImGui::EndDisabled();
  ImGui::SameLine();
  ImGui::Text("Status:");
  ImGui::SameLine();
  const int done = isvc.done(), total = isvc.total();
  switch (st) {
  case InspectStatus::Idle:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
    break;
  case InspectStatus::Loading:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "loading %d/%d days", done, total);
    break;
  case InspectStatus::Running:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "running %d/%d nodes", done, total);
    break;
  case InspectStatus::Done:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "done");
    break;
  case InspectStatus::Cancelled:
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelled %d/%d", done, total);
    break;
  }

  // ---- 结果快照 (持锁派生, 不拷 rows) ----
  InspectRequest probe;
  probe.file = sel.file, probe.expr = sel.expr, probe.frame = sel.frame;
  probe.universe = ctx.universe, probe.start_date = ctx.start_date, probe.end_date = ctx.end_date;
  probe.impact_amt = ui.impact_amt, probe.sell_impact = ui.impact_amt > 0 ? ctx.sell_impact : 0.0;
  const std::string key = probe.key();
  const int sel_hold = ui.hold_idx;
  std::string message, res_error, res_key, res_file;
  int res_impact = 0;
  StatScope res_scope;
  float res_valid = 0.f;
  double res_ms = 0.0;
  factor::stat::HoldStat res_holds[factor::stat::kMaxHold]; // 全部持有期 (期限结构)
  bool res_has = false;
  {
    std::lock_guard<std::mutex> lock(isvc.mutex);
    message = isvc.message;
    const InspectResult &res = isvc.result;
    res_key = res.key;
    res_error = res.error;
    if (!res.key.empty() && res.error.empty()) {
      assert(res.n_hold == n_hold && "Inspect 与 FeatureTable 的标签组不一致");
      res_has = true;
      res_file = res.file;
      res_impact = res.impact_amt;
      res_scope = res.scope;
      res_valid = res.valid_pct;
      res_ms = res.eval_ms;
      for (int i = 0; i < n_hold; ++i)
        res_holds[i] = res.hold[i];
    }
    const uint64_t ep = isvc.epoch();
    if (ep != ui.derived_epoch || sel_hold != ui.derived_sel || ui.absolute != ui.derived_abs) {
      ui.derived_epoch = ep;
      ui.derived_sel = sel_hold;
      ui.derived_abs = ui.absolute;
      if (res_has && sel_hold < res.hd.n)
        derive(res, sel_hold, ui.absolute, ui.der);
      else
        ui.der = InspectDerived{};
    }
  }
  if (!message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "| %s", message.c_str());
  }
  if (action == 1) // 手动 Compute (自动起算在 InspectAutoRequest, 两页都每帧调)
    ui.last_req_key = key;

  // ---- 作用域 + 选定持有期的 stat 行 ----
  // match = 结果就是当前请求 (选中行 + 作用域 + 冲击档); 否则仍整页画旧结果 (服务不清旧 result), 新结果发布一次性换; 头部显示行 row 已随结果
  const bool match = res_has && res_key == key;
  const bool shown = res_has;
  assert(!shown || res_file == row.file || row.file == sel.file); // 显示行 = 结果因子 (结果因子已不在表里时退回选中行)
  if (!res_error.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "算不了: %s", res_error.c_str());
  } else if (shown) {
    ImGui::TextDisabled("%s  %s..%s  %d 天 × %d 资产 (T=%d)  %s %s | valid %.2f%%  eval %.1f ms | 标签 %s%s", res_scope.universe.c_str(),
                        res_scope.start_date.c_str(), res_scope.end_date.c_str(), res_scope.days, res_scope.A, res_scope.T, res_scope.backend.c_str(),
                        res_scope.time.c_str(), res_valid, res_ms,
                        res_impact ? ("扣冲击 " + std::to_string(res_impact) + "w + 平仓 " + std::to_string(ctx.sell_impact)).c_str() : "毛 (价格收益)",
                        match ? "" : "  (旧结果, 新请求计算中…)");
    const factor::stat::HoldStat &h = ui.der.hs;
    if (ui.der.valid && h.n >= 3)
      ImGui::Text("h=%-5s n=%d/%d  rIC %+.4f std %.4f IR %+.3f t %+.2f pos %.2f skew %+.2f kurt %+.2f | LS %+.5f t %+.2f pos %.2f SR %+.2f β %+.3f "
                  "| mono %+.3f | rAC %+.3f",
                  factor::stat::hold_name(h.hold).c_str(), h.n, h.n_ac, h.ic_mean, h.ic_std, h.icir, h.ic_t, h.ic_pos, h.ic_skew, h.ic_kurt, h.ls_mean,
                  h.ls_t, h.ls_pos, h.sharpe, h.beta, h.mono, h.rank_ac);
    else
      ImGui::TextDisabled("h=%s  n=%d (有效行不足, 无 stat)", factor::stat::hold_name(cur_hold).c_str(), h.n);
  } else {
    ImGui::TextDisabled("%s", busy ? "计算中…" : (ctx.axis_ready ? "尚未计算 (自动起算 / Compute)" : "资产轴未就绪, 先在 Database 页扫描"));
  }
  ImGui::Separator();

  // ---- 2×2 图 ----
  const ImVec2 avail = ImGui::GetContentRegionAvail();
  const float sp = ImGui::GetStyle().ItemSpacing.x;
  const ImVec2 cell(std::max(100.f, (avail.x - sp) * 0.5f), std::max(100.f, (avail.y - ImGui::GetStyle().ItemSpacing.y) * 0.5f));
  const char *wait_hint = busy ? "计算中…" : "Compute 后可见";

  if (shown && ui.der.valid)
    plot_layers(ui.der, ui.absolute, cell);
  else
    empty_plot("分层累计###layers", cell, wait_hint);
  ImGui::SameLine();
  // 期限结构: Inspect 结果优先, 否则文件 stat (Factors 表的 row.hold)
  if (shown)
    plot_term(res_holds, n_hold, ui.hold_idx, res_impact ? "Inspect (扣冲击)" : "Inspect", cell);
  else if (row.has_stat && row.n_hold == n_hold && ui.impact_amt == 0) // 文件 stat 只有毛口径
    plot_term(row.hold, n_hold, ui.hold_idx, row.stat_from_file ? "文件 stat" : "Factors Run", cell);
  else
    empty_plot("期限结构###term", cell, "无 stat (Factors 页 Run 或此处 Compute)");

  if (shown && ui.der.valid)
    plot_ic(ui.der, cell);
  else
    empty_plot("IC 分布###ic", cell, wait_hint);
  ImGui::SameLine();
  empty_plot("留位###reserved", cell, "Markowitz CDF 仓位映射 (待做)");
  return action;
}

} // namespace GUI::Factors
