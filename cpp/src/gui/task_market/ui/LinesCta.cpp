// Market→CTA 页: 左 = 两图 (价格与中线 / 档线, 底色 = 仓位 / 净值, 共一条 x), 其右 = 中栏 (设定 / 参数轨迹 / 光标处参数 / 全期指标; 与右表分右栏) — 见 TabLines.hpp
#include "gui/task_market/ui/LinesCommon.hpp"

#include "gui/Tasks.hpp"      // StatusColor
#include "gui/util/Chart.hpp" // kUp / kDn
#include "gui/util/Tip.hpp"
#include "imgui.h"
#include "implot.h"
#include "implot_internal.h" // GetItem (图例显隐)
#include "misc/date.hpp"     // dash

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdio>
#include <initializer_list>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Market {

namespace {

constexpr size_t kS = Quote::kSlots;
constexpr float kPriceFrac = 0.62f;                                                                                  // 上图占两图高的比例
constexpr float kPosA = 0.35f;                                                                                       // 满仓仓位色的不透明度 (多 = kUp, 空 = kDn)
constexpr ImVec4 kMidCol(1.0f, 0.6f, 0.2f, 1.0f), kHoldCol(0.6f, 0.6f, 0.6f, 1.0f), kPinCol(1.0f, 0.5f, 0.0f, 1.0f); // kPinCol = 光标 (同 OrderFlow 锚线)
// 成本带色 (序同 cta::kCosts) + 常态 / 悬停的不透明度
constexpr ImVec4 kCostCol[] = {{0.35f, 0.60f, 0.95f, 1.0f}, {0.70f, 0.50f, 0.95f, 1.0f}, {0.95f, 0.65f, 0.25f, 1.0f}, {0.30f, 0.80f, 0.75f, 1.0f}, {0.95f, 0.40f, 0.55f, 1.0f}};
static_assert(std::size(kCostCol) == cta::kNC, "成本带色");
constexpr float kCostA = 0.22f, kCostHotA = 0.6f;

// 有效根 i 所在日 / 槽
misc::Ymd day_of(const CtaResult &r, size_t i) { return r.dates[r.se->t[i] / kS]; }
int slot_of(const CtaResult &r, size_t i) { return static_cast<int>(r.se->t[i] % kS); }
// 有效根 i 的时刻 "YYYY-MM-DD hh:mm" (标号 = 根结束分钟)
std::string when(const CtaResult &r, size_t i) {
  const int m = Quote::slot_label(slot_of(r, i));
  char s[24];
  std::snprintf(s, sizeof(s), " %02d:%02d", m / 60, m % 60);
  return misc::dash(day_of(r, i)) + s;
}

// 横轴刻度: x = 有效根下标 → YY/MM/DD; 视野不足几日 → MM/DD hh:mm
struct XFmt {
  const CtaResult *r;
  bool time;
};
int x_formatter(double v, char *buf, int size, void *ud) {
  const XFmt &f = *static_cast<const XFmt *>(ud);
  const double n = static_cast<double>(f.r->se->c.size());
  if (v < 0 || v > n - 1)
    return std::snprintf(buf, static_cast<size_t>(size), " ");
  const size_t i = static_cast<size_t>(std::lround(v));
  const misc::Ymd y = day_of(*f.r, i);
  if (!f.time)
    return std::snprintf(buf, static_cast<size_t>(size), "%02d/%02d/%02d", y / 10000 % 100, y / 100 % 100, y % 100);
  const int m = Quote::slot_label(slot_of(*f.r, i));
  return std::snprintf(buf, static_cast<size_t>(size), "%02d/%02d %02d:%02d", y / 100 % 100, y % 100, m / 60, m % 60);
}

// 视野 [x0, x1] 内每 step 根取末根 (两端各多一点, 线连到图边); 下标升序
std::vector<size_t> sample(size_t n, double x0, double x1, size_t step) {
  std::vector<size_t> o;
  const double lo = std::max(0.0, std::floor(x0) - static_cast<double>(step)), hi = std::ceil(x1) + static_cast<double>(step);
  for (size_t b = static_cast<size_t>(lo) / step * step; b < n && static_cast<double>(b) <= hi; b += step)
    o.push_back(std::min(b + step - 1, n - 1));
  return o;
}

// 有效根 i 所在日 (下标同 Track::days)
size_t dix(const CtaResult &r, size_t i) { return r.se->t[i] / kS - r.se->t[0] / kS; }

// ---- 两图: 上 = 仓位色 (多 = 收盘线以上, 空 = 以下, 越满越浓) + 收盘 + 中线 + 档线 (分层的 kUnits 份各一条, 分层时另有起点一条) + 买 / 卖标记 (每点合成);
//      下 = 策略 / 持有净值 + 成本带 (y 轴 log / 绝对, 右上角切换). x 同一视野 (链到 ui), 每像素取末根; 光标 = 可拖竖线 (两图同一条)
//      左键框选放大 (价格图连 y 一起; 净值图只 x), 右键拖动平移, 滚轮缩放, 双击回全段;
//      光标 = 橙色竖线 (同 OrderFlow 锚线): 参数轨迹展开时才出现 (初放视野正中), 只能拖动, 标注时刻 ----
void plots(CtaState &c, const CtaResult &r, const char *name, const ImVec2 &size) {
  const cta::Series &se = *r.se;
  const cta::Track &tr = r.tr;
  const size_t n = se.c.size();
  if (c.fit != r.ref) { // 换标的: 回全段, 光标回末根
    c.fit = r.ref;
    c.x0 = -0.5, c.x1 = static_cast<double>(n) - 0.5, c.fit_y = true, c.cursor = -1;
  }
  const size_t step = std::max<size_t>(1, static_cast<size_t>((c.x1 - c.x0) / std::max(1.0f, size.x)));
  const std::vector<size_t> at = sample(n, c.x0, c.x1, step);
  const auto take = [&](const auto &v) {
    std::vector<double> o(at.size());
    for (size_t k = 0; k < at.size(); ++k)
      o[k] = v[at[k]];
    return o;
  };
  std::vector<double> xs(at.size());
  for (size_t k = 0; k < at.size(); ++k)
    xs[k] = static_cast<double>(at[k]);
  const cta::Col &w = tr.strat.run.w;
  const std::vector<double> close = take(se.c), mid = take(tr.cut.mid), nav = take(tr.strat.nav), hold = take(tr.hold.nav);
  std::vector<double> wm(at.size(), 0); // wm[k] = 点 k → k + 1 这段所持的平均仓位 (根 at[k] .. at[k + 1] − 1 收盘调到的)
  for (size_t k = 0; k + 1 < at.size(); ++k) {
    for (size_t i = at[k]; i < at[k + 1]; ++i)
      wm[k] += w[i];
    wm[k] /= static_cast<double>(at[k + 1] - at[k]);
  }

  // 买 / 卖标记: 每点 (根 at[k] / step · step .. at[k]) 的买 / 卖各合成一个 = 合计 |Δw|, 标在其中最大一笔那根 (逐根 = 各根本身); 次数 = 视野内逐根计
  const size_t lo = static_cast<size_t>(std::clamp(std::ceil(c.x0), 0.0, static_cast<double>(n - 1)));
  const size_t hi = static_cast<size_t>(std::clamp(std::floor(c.x1), 0.0, static_cast<double>(n - 1)));
  const auto dw = [&](size_t i) { return w[i] - (i ? w[i - 1] : 0.0f); };
  struct Mark {
    size_t i = 0;
    float d = 0, top = 0; // 合计 Δw / 最大一笔 |Δw|
  };
  std::vector<Mark> marks;
  int buys = 0, sells = 0;
  for (const size_t e : at) {
    Mark m[2]; // 买 / 卖
    for (size_t i = e / step * step; i <= e; ++i) {
      const float d = dw(i);
      if (std::fabs(d) <= 1e-6f)
        continue;
      Mark &x = m[d < 0];
      x.d += d;
      if (std::fabs(d) > x.top)
        x.top = std::fabs(d), x.i = i;
      if (i >= lo && i <= hi)
        ++(d > 0 ? buys : sells);
    }
    for (const Mark &x : m)
      if (x.top > 0)
        marks.push_back(x);
  }

  // 标题: 标的 (高亮) · 模型 · 窗 · 视野 · 粒度
  const std::string model = std::string(cta::kCenters[r.spec.kind].name) + " / " + cta::kDirs[r.spec.dir].name;
  char gran[64];
  if (step == 1)
    std::snprintf(gran, sizeof(gran), "逐根, 买 %d 次 / 卖 %d 次", buys, sells);
  else
    std::snprintf(gran, sizeof(gran), "每点 %zu 根 (买 / 卖各合成一个), 买 %d 次 / 卖 %d 次", step, buys, sells);
  ImGui::TextColored(kHead, "%s", name ? name : "");
  ImGui::SameLine();
  ImGui::TextDisabled("%s  ·  W %d 日  ·  %s ~ %s  ·  %s", model.c_str(), r.spec.roll, misc::dash(day_of(r, lo)).c_str(),
                      misc::dash(day_of(r, hi)).c_str(), gran);

  const ImGuiStyle &st = ImGui::GetStyle();
  const float ph = ImGui::GetContentRegionAvail().y - st.ItemSpacing.y, h1 = std::floor(ph * kPriceFrac);
  XFmt fmt{&r, c.x1 - c.x0 < 3.0 * kS};
  const double pad = static_cast<double>(n) * 0.02 + 10.0;
  bool pinned = c.path_open && c.cursor < 0, reset = false;
  double pin = c.cursor >= 0 ? c.cursor : std::clamp((c.x0 + c.x1) / 2, 0.0, static_cast<double>(n - 1));
  const auto setup_x = [&](ImPlotAxisFlags f) {
    ImPlot::SetupAxis(ImAxis_X1, nullptr, f);
    ImPlot::SetupAxisLinks(ImAxis_X1, &c.x0, &c.x1);
    ImPlot::SetupAxisFormat(ImAxis_X1, x_formatter, &fmt);
    ImPlot::SetupAxisLimitsConstraints(ImAxis_X1, -pad, static_cast<double>(n) + pad);
  };
  const auto track = [&](int id) { // 双击 = 回全段; 光标 (同 OrderFlow 锚线): 左键拖动, 松手停住
    reset |= ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(ImGuiMouseButton_Left);
    if (c.path_open)
      pinned |= ImPlot::DragLineX(id, &pin, kPinCol, 2.0f, ImPlotDragToolFlags_NoFit);
  };
  const ImPlotInputMap keep = ImPlot::GetInputMap(); // 本页两图: 右键平移; 左键 = 价格图框选 (x + y) / 净值图横向框选 (XZoom); 画完还原
  ImPlot::MapInputReverse();
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(2, 2));
  ImPlot::PushStyleVar(ImPlotStyleVar_LabelPadding, ImVec2(2, 1));

  if (c.boxing && ImGui::IsMouseReleased(ImGuiMouseButton_Left)) // 框选松手: 本帧起用框的 y
    c.fit_y = false;
  if (c.fit_y)
    ImPlot::SetNextAxisToFit(ImAxis_Y1);
  if (ImPlot::BeginPlot("##cta_px", ImVec2(size.x, h1), ImPlotFlags_NoTitle | ImPlotFlags_NoMenus)) {
    setup_x(ImPlotAxisFlags_NoTickLabels);
    ImPlot::SetupAxis(ImAxis_Y1, nullptr, ImPlotAxisFlags_RangeFit);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_Horizontal);
    { // 仓位色: 每段 (点 k → k + 1) 一块梯形, 多 = 收盘线到图顶, 空 = 收盘线到图底; 浓度 ∝ |平均仓位|; 关填充抗锯齿, 相邻块不留缝
      const float y0 = ImPlot::GetPlotPos().y, y1 = y0 + ImPlot::GetPlotSize().y;
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      const ImDrawListFlags keep_flags = dl->Flags;
      dl->Flags &= ~ImDrawListFlags_AntiAliasedFill;
      ImPlot::PushPlotClipRect();
      for (size_t k = 0; k + 1 < at.size(); ++k) {
        if (std::fabs(wm[k]) <= 1e-3)
          continue;
        const ImVec2 a = ImPlot::PlotToPixels(xs[k], close[k]), b = ImPlot::PlotToPixels(xs[k + 1], close[k + 1]);
        const float edge = wm[k] > 0 ? y0 : y1, ya = std::clamp(a.y, y0, y1), yb = std::clamp(b.y, y0, y1);
        ImVec4 col = wm[k] > 0 ? kUp : kDn;
        col.w = kPosA * static_cast<float>(std::min(1.0, std::fabs(wm[k])));
        dl->AddQuadFilled(ImVec2(a.x, edge), ImVec2(b.x, edge), ImVec2(b.x, yb), ImVec2(a.x, ya), ImGui::GetColorU32(col));
      }
      ImPlot::PopPlotClipRect();
      dl->Flags = keep_flags;
    }
    { // 档线: 自中线一侧起逐条 (Cuts::rungs_at); 满仓 = 随中线走, 分层 = 已减份的冻结 + 起点 + 持有份的随 ATR 伸缩
      std::vector<std::vector<double>> y(cta::kUnits + 1, std::vector<double>(xs.size(), cta::kNaN));
      for (size_t k = 0; k < at.size(); ++k)
        tr.cut.rungs_at(at[k], [&](int j, float p) { y[static_cast<size_t>(j)][k] = p; });
      for (const std::vector<double> &v : y) {
        ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.35f), 1.0f);
        ImPlot::PlotLine("档线", xs.data(), v.data(), static_cast<int>(xs.size()));
      }
    }
    ImPlot::SetNextLineStyle(kMidCol, 1.2f);
    ImPlot::PlotLine("中线", xs.data(), mid.data(), static_cast<int>(xs.size()));
    ImPlot::SetNextLineStyle(ImGui::GetStyleColorVec4(ImGuiCol_Text), 1.2f);
    ImPlot::PlotLine("收盘", xs.data(), close.data(), static_cast<int>(xs.size()));
    { // 买 ▲ (收盘下方) / 卖 ▼ (上方), 边长 ∝ 合计 |Δw| (截到 1), 细描边
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      ImPlot::PushPlotClipRect();
      const float sc = ImGui::GetFontSize() / 13.0f;
      for (const Mark &m : marks) {
        const float d = m.d;
        const ImVec2 p = ImPlot::PlotToPixels(static_cast<double>(m.i), se.c[m.i]);
        const float s = (5.0f + 14.0f * std::min(1.0f, std::fabs(d))) * sc / 2, y = d > 0 ? p.y + s * 1.2f : p.y - s * 1.2f, t = d > 0 ? -s : s;
        const ImVec2 a(p.x, y + t), b(p.x - s, y - t), e(p.x + s, y - t);
        dl->AddTriangleFilled(a, b, e, ImGui::GetColorU32(d > 0 ? kUp : kDn));
        dl->AddTriangle(a, b, e, IM_COL32(0, 0, 0, 160), 1.0f);
      }
      ImPlot::PopPlotClipRect();
    }
    track(0);
    if (c.path_open) {
      const size_t i = static_cast<size_t>(std::clamp(std::lround(pin), 0L, static_cast<long>(n - 1)));
      ImPlot::Annotation(static_cast<double>(i), se.c[i], kPinCol, ImVec2(5, -15), true, "%s", when(r, i).c_str());
    }
    c.boxing = ImPlot::IsPlotSelected();
    ImPlot::EndPlot();
  }
  ImVec2 corner(-1, -1); // 净值图右上角 (轴切换按钮)
  double zlo = 0, zhi = 0;
  bool zoomed = false; // 净值图框选落定 (x 视野在 EndPlot 回写链接之后改)
  if (ImPlot::BeginPlot("##cta_nav", ImVec2(size.x, ph - h1), ImPlotFlags_NoTitle | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect)) {
    setup_x(ImPlotAxisFlags_None);
    ImPlot::SetupAxis(ImAxis_Y1, nullptr, ImPlotAxisFlags_AutoFit | ImPlotAxisFlags_RangeFit);
    ImPlot::SetupAxisScale(ImAxis_Y1, c.nav_log ? ImPlotScale_Log10 : ImPlotScale_Linear);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_Horizontal);
    // 成本带: 逐项叠 (带下沿 = 叠回前几项, 上沿 = 再叠回本项), 带厚 = 该项把净值压下去的量; 悬停的带高亮 + 说明
    //   基线 = 策略净值; 图例隐去策略净值时 = 1 (各沿除以策略净值, 只看成本本身)
    const cta::Book &sb = tr.strat;
    const bool hovered = ImPlot::IsPlotHovered(); // 锁定 setup 之后 item 才能按 label 查
    const ImPlotItem *nav_it = ImPlot::GetItem("策略净值");
    const bool on_nav = !nav_it || nav_it->Show;
    const auto edge = [&](size_t j, size_t i) { // 第 j 层的下沿 (j = 0 = 基线; j = kNC = 毛)
      const double v = j ? sb.lift[j - 1][i] : sb.nav[i];
      return on_nav ? v : v / sb.nav[i];
    };
    int band = -1;
    size_t bi = 0;
    if (hovered) {
      const ImPlotPoint mp = ImPlot::GetPlotMousePos();
      bi = static_cast<size_t>(std::clamp(std::lround(mp.x), 0L, static_cast<long>(n - 1)));
      for (size_t j = 0; j < cta::kNC && band < 0; ++j) {
        const double y0 = edge(j, bi), y1 = edge(j + 1, bi);
        if (y1 > y0 && mp.y >= y0 && mp.y <= y1)
          band = static_cast<int>(j);
      }
    }
    std::vector<double> lo(xs.size()), hi(xs.size());
    for (size_t k = 0; k < at.size(); ++k)
      lo[k] = edge(0, at[k]);
    for (size_t j = 0; j < cta::kNC; ++j) {
      for (size_t k = 0; k < at.size(); ++k)
        hi[k] = edge(j + 1, at[k]);
      ImPlot::SetNextFillStyle(kCostCol[j], band == static_cast<int>(j) ? kCostHotA : kCostA);
      ImPlot::PlotShaded(cta::kCosts[j].name, xs.data(), lo.data(), hi.data(), static_cast<int>(xs.size()));
      std::swap(lo, hi);
    }
    ImPlot::SetNextLineStyle(kHoldCol, 1.0f);
    ImPlot::PlotLine("持有净值", xs.data(), hold.data(), static_cast<int>(xs.size()));
    ImPlot::SetNextLineStyle(ImGui::GetStyleColorVec4(ImGuiCol_Text), 1.5f);
    ImPlot::PlotLine("策略净值", xs.data(), nav.data(), static_cast<int>(xs.size()));
    track(1);
    const double zx = plot_mouse_x();
    if (hovered && ImGui::IsMouseClicked(ImGuiMouseButton_Left) && !ImGui::IsAnyItemActive()) // 拖光标时不起
      c.zoom.begin(zx);
    zoomed = c.zoom.track(zx, zlo, zhi) && zhi > zlo;
    if (band >= 0) {
      const cta::CostItem &ci = cta::kCosts[band];
      const size_t j = static_cast<size_t>(band);
      const double y0 = edge(j, bi), y1 = edge(j + 1, bi);
      tip("%s\n截至 %s: 不扣这项, 净值高 %.2f%% (%.4f → %.4f)\n全期年均 %.2f%% (占净值)", ci.name, when(r, bi).c_str(), (y1 / y0 - 1) * 100, y0, y1,
          sb.run.cost.*ci.f / sb.st.years * 100);
    }
    corner = ImVec2(ImPlot::GetPlotPos().x + ImPlot::GetPlotSize().x, ImPlot::GetPlotPos().y);
    ImPlot::EndPlot();
  }
  if (corner.x >= 0) { // 净值图 y 轴切换: 叠在右上角
    static const char *const kScale[] = {"log", "绝对"};
    float bw = 2.0f;
    for (const char *s : kScale)
      bw += ImGui::CalcTextSize(s).x + 2 * st.FramePadding.x;
    ImGui::SetCursorScreenPos(ImVec2(corner.x - bw - 4, corner.y + 4));
    int cur = c.nav_log ? 0 : 1;
    if (button_bar(kScale, 2, cur))
      c.nav_log = cur == 0;
  }
  ImPlot::PopStyleVar(2);
  ImPlot::GetInputMap() = keep;
  if (pinned)
    c.cursor = static_cast<int>(std::clamp(std::lround(pin), 0L, static_cast<long>(n - 1)));
  if (zoomed)
    c.x0 = zlo, c.x1 = zhi;
  if (reset) {
    c.x0 = -0.5, c.x1 = static_cast<double>(n) - 0.5, c.fit_y = true;
    c.zoom.cancel();
  }
}

// ---- 中栏: 状态 + 设定 (改了下帧即重发) + 光标处所用参数 + 全期指标 ----
// 全期指标表 [行]: 名 / 字段 / 格式 / 倍数 / 说明
struct StatRow {
  const char *name;
  double cta::Stat::*f;
  const char *fmt;
  double mul;
  const char *tip;
};
const StatRow kStatRows[] = {
    {"夏普", &cta::Stat::sharpe, "%.2f", 1, "逐根净收益 均值 / 标准差 × √每年根数"},
    {"索提诺", &cta::Stat::sortino, "%.2f", 1, "逐根净收益 均值 / 下行波动 √E[min(r, 0)²] × √每年根数"},
    {"年化", &cta::Stat::cagr, "%+.1f%%", 100, "全期净值按年数复利折算"},
    {"最大回撤", &cta::Stat::mdd, "%.1f%%", 100, "净值自前高的最大回落"},
    {"年化波动", &cta::Stat::vol, "%.1f%%", 100, "逐根净收益标准差 × √每年根数"},
    {"平均仓位", &cta::Stat::expo, "%.2f", 1, "逐根 |仓位| 的均值 (满多 / 满空 = 1)"},
    {"年换手", &cta::Stat::turn, "%.1f", 1, "单边: Σ |Δ仓位| / 年数"},
    {"年成本", &cta::Stat::cost, "%.2f%%", 100, "佣金 + 印花税 + 冲击 + 震荡 + 跳空, 每年占净值"},
    {"  佣金", &cta::Stat::commission, "%.2f%%", 100, "佣金率 × Σ |Δ仓位| / 年数"},
    {"  印花税", &cta::Stat::stamp, "%.2f%%", 100, "印花税率 × Σ 卖出量 (多头减仓 / 融券卖出) / 年数"},
    {"  冲击", &cta::Stat::impact, "%.2f%%", 100, "冲击率 × Σ |Δ仓位| / 年数"},
    {"  震荡", &cta::Stat::chop, "%.2f%%", 100,
     "越线差的震荡部分: 分层按收盘成交比按档线价成交多付的差, 每份都按离收盘最近那档的差计 (只越过一档 = 全归震荡); Σ / 年数"},
    {"  跳空", &cta::Stat::gap, "%.2f%%", 100, "越线差的跳空部分: 一根越过几档时, 远处的档比最近那档多付的部分; Σ / 年数"},
    {"上涨捕获", &cta::Stat::up, "%.2f", 1, "资产上涨的根上 Σ 策略毛收益 / Σ 资产收益 (满多 = 1, 满空 = −1, 随机择时 ≈ 平均净仓位)"},
    {"下跌捕获", &cta::Stat::dn, "%.2f", 1, "资产下跌的根上 Σ 策略毛收益 / Σ 资产收益 = 承受的下跌 (做空为负 = 下跌里挣的)"},
    {"Edge", &cta::Stat::edge, "%+.2f", 1, "上涨捕获 − 下跌捕获 = 择时本身挣的 (不含成本)"},
    {"年在场段数", &cta::Stat::trades, "%.1f", 1, "在场段 = 仓位同号非零的连续根 (建仓根到清仓 / 翻向根)"},
    {"胜率", &cta::Stat::win, "%.0f%%", 100, "段收益 Π(1 + 净) − 1 > 0 的段占比"},
    {"盈亏比", &cta::Stat::payoff, "%.2f", 1, "盈利段均盈 / 亏损段均亏"},
    {"平均持有", &cta::Stat::hold, "%.0f 根", 1, "在场段的平均根数"},
};

// 单元格内右对齐; dim = 灰显
void right(const char *s, bool dim) {
  ImGui::SetCursorPosX(ImGui::GetCursorPosX() + std::max(0.0f, ImGui::GetContentRegionAvail().x - ImGui::CalcTextSize(s).x));
  if (dim)
    ImGui::TextDisabled("%s", s);
  else
    ImGui::TextUnformatted(s);
}

// 名 | 策略 | 持有 (灰, 作对照); 数值右对齐
void stat_table(const CtaResult &r) {
  if (!ImGui::BeginTable("##cta_stat", 3, ImGuiTableFlags_RowBg | ImGuiTableFlags_SizingStretchProp))
    return;
  ImGui::TableSetupColumn("##name", ImGuiTableColumnFlags_WidthStretch, 1.4f);
  ImGui::TableSetupColumn("##st", ImGuiTableColumnFlags_WidthStretch, 1.0f);
  ImGui::TableSetupColumn("##sh", ImGuiTableColumnFlags_WidthStretch, 1.0f);
  ImGui::TableNextRow();
  ImGui::TableNextColumn();
  for (const char *h : {"策略", "持有"}) {
    ImGui::TableNextColumn();
    right(h, true);
  }
  for (const StatRow &x : kStatRows) {
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(x.name);
    if (ImGui::IsItemHovered())
      tip("%s = %s", x.name, x.tip);
    for (const cta::Stat *s : {&r.tr.strat.st, &r.tr.hold.st}) {
      ImGui::TableNextColumn();
      const double v = s->*x.f;
      char t[32] = "-";
      if (std::isfinite(v))
        std::snprintf(t, sizeof(t), x.fmt, v * x.mul);
      right(t, s == &r.tr.hold.st);
    }
  }
  ImGui::EndTable();
}

// 光标处 (−1 = 末根) 所在日 → v = 当日所用参数 (阻尼后, 轴的单位; 未选参 = nullptr); 返回说明
std::string cursor_at(const CtaResult &r, int cursor, const std::vector<double> *&v) {
  const size_t n = r.se->c.size(), i = cursor >= 0 ? static_cast<size_t>(cursor) : n - 1;
  const std::vector<double> &u = r.tr.use[dix(r, i)];
  const std::string head = cursor >= 0 ? "光标 " + when(r, i) : "末根";
  v = u.empty() ? nullptr : &u;
  return head + (u.empty() ? ": 买入持有 (首日 / 窗内数据不足 / 中线未就绪, 未选参)" : ": 当日所用 (阻尼后)");
}

// 模型下拉: 选项 xs (各有 name / tip) 的下标 cur; 悬停 = head + 各模型说明
template <class Xs>
void model_combo(const char *id, const Xs &xs, size_t &cur, const std::string &head) {
  if (ImGui::BeginCombo(id, xs[cur].name)) {
    for (size_t k = 0; k < std::size(xs); ++k)
      if (ImGui::Selectable(xs[k].name, k == cur))
        cur = k;
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered()) {
    std::string s = head;
    for (const auto &x : xs)
      s += std::string("\n• ") + x.name + " = " + x.tip;
    tip("%s", s.c_str());
  }
}

// ---- 参数轨迹: 逐日各轴参数 ÷ 首个选参日的值 (都从 1 起, log 轴; 只看相对变化): 实线 = 阻尼后所用, 淡线 = 曲面最高点 (跳 = 岭回归选参不稳);
//      x 链到两图 (同一视野, 双击回全段), 未选参日断开; 光标 = 橙色竖线; 悬停看当日参数 ----

// 参数 v (轴的单位): 中线 模型  轴 = 值 …
std::string params_of(const cta::Grid &g, const std::vector<double> &v) {
  std::string s = std::string("中线 ") + cta::kCenters[g.kind].name;
  for (size_t a = 0; a < g.ax.size(); ++a) {
    char b[48];
    std::snprintf(b, sizeof(b), "  %s = %.3g%s", g.ax[a]->name, v[a], g.ax[a]->day ? " 日" : "");
    s += b;
  }
  return s;
}

void path(CtaState &c, const CtaResult &r) {
  const cta::Track &tr = r.tr;
  const cta::Grid &g = tr.grid;
  const size_t n = r.se->c.size(), na = g.ax.size();
  const auto first = std::ranges::find_if(tr.use, [](const std::vector<double> &v) { return !v.empty(); });
  if (first == tr.use.end()) {
    ImGui::TextDisabled("全期未选参 (买入持有)");
    return;
  }
  const std::vector<double> &v0 = *first;
  // 阶梯: 每日两点 (首根, 末根), 日间的竖跳 = 换参; 未选参日 = NaN 断开
  std::vector<double> xs;
  std::vector<std::vector<double>> ys(2 * na); // [轴] 所用 / [na + 轴] 最高点
  for (size_t d = 0; d < tr.days.size(); ++d) {
    const size_t b = tr.days.off[d], e = tr.days.off[d + 1];
    if (b == e)
      continue;
    for (const size_t i : {b, e - 1}) {
      xs.push_back(static_cast<double>(i));
      for (size_t a = 0; a < na; ++a) {
        ys[a].push_back(tr.use[d].empty() ? cta::kNaN : tr.use[d][a] / v0[a]);
        ys[na + a].push_back(tr.raw[d].empty() ? cta::kNaN : tr.raw[d][a] / v0[a]);
      }
    }
  }
  // y 范围 = 视野内各日 (与视野相交) 的 log 跨度, 上下各留 20%; 跨度为 0 按一格 (kStep) 计
  double lo = INFINITY, hi = -INFINITY;
  for (size_t k = 0; k + 1 < xs.size(); k += 2)
    if (xs[k] <= c.x1 && xs[k + 1] >= c.x0)
      for (const std::vector<double> &y : ys)
        if (std::isfinite(y[k]))
          lo = std::min(lo, std::log10(y[k])), hi = std::max(hi, std::log10(y[k]));
  if (!(lo <= hi))
    lo = hi = 0;
  const double span = hi > lo ? hi - lo : std::log10(cta::kStep);
  const float w = ImGui::GetContentRegionAvail().x;
  XFmt fmt{&r, c.x1 - c.x0 < 3.0 * kS};
  if (ImPlot::BeginPlot("##cta_path", ImVec2(w, std::floor(w * 0.6f)), ImPlotFlags_NoTitle | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect)) {
    ImPlot::SetupAxis(ImAxis_X1, nullptr, ImPlotAxisFlags_None);
    ImPlot::SetupAxisLinks(ImAxis_X1, &c.x0, &c.x1);
    ImPlot::SetupAxisFormat(ImAxis_X1, x_formatter, &fmt);
    ImPlot::SetupAxis(ImAxis_Y1, nullptr, ImPlotAxisFlags_None);
    ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    ImPlot::SetupAxisLimits(ImAxis_Y1, std::pow(10.0, lo - 0.2 * span), std::pow(10.0, hi + 0.2 * span), ImPlotCond_Always);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_Horizontal);
    for (size_t a = 0; a < na; ++a) {
      const ImVec4 col = ImPlot::GetColormapColor(static_cast<int>(a));
      const std::string top = std::string(g.ax[a]->name) + " 最高点";
      ImPlot::SetNextLineStyle(ImVec4(col.x, col.y, col.z, 0.35f), 1.0f);
      ImPlot::PlotLine(top.c_str(), xs.data(), ys[na + a].data(), static_cast<int>(xs.size()));
      ImPlot::SetNextLineStyle(col, 1.5f);
      ImPlot::PlotLine(g.ax[a]->name, xs.data(), ys[a].data(), static_cast<int>(xs.size()));
    }
    if (c.cursor >= 0) {
      const double x = c.cursor;
      ImPlot::SetNextLineStyle(kPinCol, 2.0f);
      ImPlot::PlotInfLines("##cta_path_pin", &x, 1);
    }
    if (ImPlot::IsPlotHovered()) {
      const size_t i = static_cast<size_t>(std::clamp(std::lround(ImPlot::GetPlotMousePos().x), 0L, static_cast<long>(n - 1))), d = dix(r, i);
      if (tr.use[d].empty())
        tip("%s\n未选参 (买入持有)", misc::dash(day_of(r, i)).c_str());
      else
        tip("%s\n所用 (阻尼后): %s\n最高点: %s", misc::dash(day_of(r, i)).c_str(), params_of(g, tr.use[d]).c_str(), params_of(g, tr.raw[d]).c_str());
    }
    ImPlot::EndPlot();
  }
  ImGui::TextDisabled("首个选参日: %s", params_of(g, v0).c_str());
}

void panel(const CtaService &svc, const Progress &lib, CtaState &c, const cta::Rates &rates, const CtaResult *r, const std::string &lack) {
  // 状态: 价格库合成 / 进度 / 算不了的原因 / 全期跨度
  const Phase ph = svc.phase.load(std::memory_order_acquire);
  const size_t done = svc.done.load(std::memory_order_relaxed), total = svc.total.load(std::memory_order_relaxed);
  const size_t lib_total = lib.total.load(std::memory_order_relaxed);
  const bool syncing = lib.phase.load(std::memory_order_acquire) == Phase::Loading && lib_total > 0;
  if (syncing)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "合成价格 %zu/%zu 日", lib.done.load(std::memory_order_relaxed), lib_total);
  else if (ph == Phase::Running)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "滚动选参 %zu/%zu 日", done, total);
  else if (!lack.empty())
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s", lack.c_str());
  else if (r) {
    const size_t n = r->se->c.size();
    ImGui::TextDisabled("%s ~ %s  ·  %zu 根", misc::dash(day_of(*r, 0)).c_str(), misc::dash(day_of(*r, n - 1)).c_str(), n);
  } else
    ImGui::TextDisabled("未算");
  if (syncing && ImGui::IsItemHovered())
    tip("首次一趟合成全部篮子线与个股的 1 分钟价, 存在 all.bar 库目录 cache/price/ 下 (十年全市场约 40 GB); 之后只补新日, 退出不丢已合成的");

  // 设定: 分组 (优化目标 / 中线 / 分层 / 引擎), 组内一行一项, 名左值右
  CtaSpec &sp = c.spec;
  const float x0 = ImGui::GetCursorPosX();
  float lw = 0.0f; // 名列宽 (控件 / 灰显参数都从这里起)
  for (const char *s : {"滚动窗 (日)", "网格配置数", "模型", "方向", "费率 (bp)", "目标年化波动", "不交易带"})
    lw = std::max(lw, ImGui::CalcTextSize(s).x);
  lw += ImGui::GetStyle().ItemSpacing.x;
  const auto label = [&](const char *s) {
    ImGui::AlignTextToFramePadding();
    ImGui::TextUnformatted(s);
    ImGui::SameLine(x0 + lw);
    ImGui::SetNextItemWidth(-FLT_MIN);
  };
  const auto hint = [](const char *s) {
    ImGui::PushStyleColor(ImGuiCol_Text, ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
    ImGui::TextWrapped("%s", s);
    ImGui::PopStyleColor();
  };
  ImGui::SeparatorText("优化目标");
  label("模型");
  model_combo("##cta_goal", cta::kGoals, sp.goal, "优化目标: 滚动选参所最大化的分数");
  label("滚动窗 (日)");
  ImGui::SliderInt("##cta_roll", &sp.roll, static_cast<int>(cta::kMinRoll), static_cast<int>(cta::kMaxRoll), "%d", ImGuiSliderFlags_Logarithmic);
  sp.roll = std::clamp(sp.roll, static_cast<int>(cta::kMinRoll), static_cast<int>(cta::kMaxRoll));
  c.editing = ImGui::IsItemActive();
  if (ImGui::IsItemHovered())
    tip("每日以此前至多 W 日 (%zu ~ %zu, 对数滑条; 不足 W 日 = 自首日起全部, expanding) 网格各配置的目标值拟岭回归曲面, 取最高点 (平原顶) 用到当日; "
        "首日 / 窗内根数不足窗长一半 / 窗内各配置的中线都未就绪 = 买入持有 (与基准同口径)\n"
        "• 松手生效 (Ctrl + 点击可直接输入)",
        cta::kMinRoll, cta::kMaxRoll);
  label("网格配置数");
  ImGui::InputInt("##cta_grid", &sp.grid, 10, 50);
  sp.grid = std::clamp(sp.grid, 3, 1000);
  c.editing |= ImGui::IsItemActive();
  if (ImGui::IsItemHovered())
    tip("参数轴 = 自 v0 向两侧铺的格点, 相邻之比 %.3g, 间距不小于各轴基本单位 (小值处退成等差), 收进各轴硬边界\n"
        "网格 = 以当前最优为中点、各多值轴 m 个相邻格点的全部组合 (碰到硬边界则整格平移进界); 各轴的 m 轮流分得 (每次给格点最少的轴加一个), 积不超过此数\n"
        "每日取曲面的连续最高点 (参数可为小数, 相邻格点间对数插值); 最近的格点落在网格边上就以它为中心移网格、补算新格点再拟 (当日至多 %d 次), 中心留到次日\n"
        "• 编辑完生效",
        cta::kStep, cta::kHops);
  hint("中线只选模型, 参数每日以此前至多 W 日的网格曲面最高点选出, 网格随最高点移动; 所用参数 = 最高点的 5 日阻尼, 中线按所用参数逐根递推 (换参不跳线)");
  c.path_open = r && ImGui::CollapsingHeader("参数轨迹");
  if (r && ImGui::IsItemHovered())
    tip("逐日参数, 每轴 ÷ 首个选参日的值 (都从 1 起, log 轴), 只看相对变化: 实线 = 阻尼后所用, 淡线 = 曲面最高点; 淡线跳得频 / 跳得大 = 岭回归选参不稳\n"
        "• 阻尼: 所用参数 (对数) 每日朝最高点走 1 / %.0f\n"
        "• x 与左侧两图同一视野; 展开时左图出现光标 (橙色竖线), 中栏参数看光标所在日",
        cta::kDamp);
  if (c.path_open)
    path(c, *r);

  const std::vector<double> *at = nullptr;
  const std::string where = r ? cursor_at(*r, c.path_open ? c.cursor : -1, at) : std::string{};
  ImGui::SeparatorText("中线");
  label("模型");
  model_combo("##cta_center", cta::kCenters, sp.kind, "中线 (周期 n): 顺向侧满仓, 逆向侧交给分层");
  const cta::Kind &shown = cta::kCenters[r ? r->spec.kind : sp.kind]; // 灰显参数按结果的模型
  for (size_t a = 0; a < shown.axes.size(); ++a) {
    const cta::Axis &ax = shown.axes[a];
    char s[32];
    std::snprintf(s, sizeof(s), "%s%s", ax.name, ax.day ? " (日)" : "");
    ImGui::TextUnformatted(s);
    ImGui::SameLine(x0 + lw);
    if (at)
      ImGui::TextDisabled("%.3g", (*at)[a]);
    else
      ImGui::TextDisabled("-");
  }
  if (r) {
    hint(where.c_str());
    if (ImGui::IsItemHovered())
      tip("灰显 = 光标 (参数轨迹展开时图上可拖的竖线; 收起 = 末根) 处所用的参数; 周期轴以日计");
  }

  ImGui::SeparatorText("分层");
  {
    char s[160];
    std::snprintf(s, sizeof(s), "固定: 层距 = %zu 日 ATR × %.2g, 一份 = %.0f%%; 逆向每走一层减一份, 顺向每走一层加一份, 以上一笔成交为基准", cta::kAtrDays,
                  cta::kSpace, 100.0 / cta::kUnits);
    hint(s);
    if (ImGui::IsItemHovered())
      tip("只认档线: 满仓时档线 = 中线 − j × 层距, 随中线走; 逆向走到 中线 − 层距 时冻结当根中线作起点, 之后基准 = 上一笔成交的档线\n"
          "• 逆向走到 基准 − 层距 减一份, 该价冻结成一条档线; 顺向回到上一条冻结档线加一份, 最后一份在起点加回\n"
          "• 加满之前中线不起作用 (价格回到中线以上也不一次补满); 加满后档线重新随中线走\n"
          "• 一根越过几档成交几档, 份数到 0 后基准照移 (跌透之后反弹一档即加回)\n"
          "• 图上: 已减份的档线与起点横住 (宽度 = 各自成交时的层距), 持有份的档线挂在基准下方, 宽度随当日 ATR 变\n"
          "• ATR = 海龟 N: 日真实波幅的 %zu 日平滑, 盘中用前一交易日收盘时的值; 中线 / ATR 未就绪 = 空仓, 就绪首根按满仓起点落到对应档",
          cta::kAtrDays);
  }

  ImGui::SeparatorText("引擎");
  {
    constexpr int nd = static_cast<int>(std::size(cta::kDirs));
    const char *names[nd];
    std::string s = "方向: 顺向 = 所押的一侧";
    for (int k = 0; k < nd; ++k)
      names[k] = cta::kDirs[k].name, s += std::string("\n• ") + cta::kDirs[k].name + " = " + cta::kDirs[k].tip;
    label("方向");
    ImGui::BeginGroup();
    button_bar(names, nd, sp.dir);
    ImGui::EndGroup();
    if (ImGui::IsItemHovered())
      tip("%s", s.c_str());
  }
  label("费率 (bp)");
  ImGui::TextDisabled("佣金 %.3g / 印花税 %.3g / 冲击 %.3g", rates.commission * 1e4, rates.stamp * 1e4, rates.impact * 1e4);
  if (ImGui::IsItemHovered())
    tip("系统设置的税佣与固定冲击 (Settings), 各页同一份; 改了即重算\n"
        "• 佣金 = 佣金率 × |Δ仓位|, 买卖各一次\n"
        "• 印花税 = 印花税率 × 卖出量 (多头减仓 / 融券卖出), 买入不收\n"
        "• 冲击 = 固定冲击 × |Δ仓位|, 买卖各一次, 不按金额变\n"
        "• 震荡 / 跳空另计: 分层按收盘成交比按档线价成交多付的差; 最近那档的差 = 震荡, 一根越过几档多出的部分 = 跳空");
  label("目标年化波动");
  ImGui::InputFloat("##cta_vol", &sp.vol, 0.05f, 0.1f, "%.2f");
  sp.vol = std::max(sp.vol, 0.0f);
  c.editing |= ImGui::IsItemActive();
  if (ImGui::IsItemHovered())
    tip("波动率定仓: 仓位 × min(1, 目标 / 近 %zu 根日内年化波动 (隔夜跳空不计)); 只作用于策略, 持有 = 满仓不动; 0 = 不调; 编辑完生效", cta::kVolSpan);
  label("不交易带");
  ImGui::InputFloat("##cta_buf", &sp.buffer, 0.05f, 0.1f, "%.2f");
  sp.buffer = std::clamp(sp.buffer, 0.0f, 1.0f);
  c.editing |= ImGui::IsItemActive();
  if (ImGui::IsItemHovered())
    tip("前后同向持仓且 |Δ仓位| < 带宽时不调 (省换手); 0 = 不用; 编辑完生效");
  if (r) {
    ImGui::SeparatorText("全期指标");
    stat_table(*r);
  }
}

} // namespace

void RenderLinesCta(CtaService &svc, const Quote::QuoteHub &mk, LinesState &ui, const cta::Rates &rates, const char *name, const ImVec2 &plots_size,
                    float panel_w) {
  CtaState &c = ui.cta;
  const std::map<std::string, Quote::Ref> &one = ui.pick.rows[kPickOne];
  const Quote::Ref q = one.empty() ? Quote::Ref{} : one.begin()->second;
  const Quote::PriceView *v = mk.view.get();
  const bool ready = v && v->lack.empty();
  const Quote::Series *se = ready && q.ok() ? v->series(q) : nullptr;
  const std::shared_ptr<const Quote::Series> m = se && se->minute ? std::shared_ptr<const Quote::Series>(mk.view, se) : nullptr;
  const bool off = ready && q.ok() && !se && v->covers(q, true); // 个股在库轴外 (篮子恒在)
  if (m && !c.editing)
    c.sent.send(svc, {.ref = q, .spec = c.spec, .rates = rates, .stamp = v->stamp, .dates = v->dates, .series = m});
  const std::shared_ptr<const CtaResult> r = latest(svc);
  const bool mine = q.ok() && r && r->ref == q; // 本标的的 (设定改了时新的还在算, 先显旧的)
  const CtaResult *show = mine && r->lack.empty() ? r.get() : nullptr;

  ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
  ImGui::BeginChild("##cta_plots", plots_size);
  ImGui::PopStyleVar();
  if (!q.ok())
    ImGui::TextDisabled("右表单选一个指数 / 行业 / 概念 / 成分股 (与 VAL 同一份选中)");
  else if (show)
    plots(c, *show, name, ImGui::GetContentRegionAvail());
  else if (!ready)
    ImGui::TextDisabled("等价格库…");
  else if (!m && !off)
    ImGui::TextDisabled("读分钟价…");
  else if (m && (!mine || r->gen != c.sent.req.gen))
    ImGui::TextDisabled("计算中…");
  ImGui::EndChild();
  ImGui::SameLine();
  ImGui::BeginChild("##cta_panel", ImVec2(panel_w, plots_size.y), ImGuiChildFlags_Borders);
  const std::string lack = v && !v->lack.empty() ? v->lack
                           : off                 ? "该股不在 all.bar 库轴上 (库建成之后上市 / 进池; 全量重算 all.bar 即并入)."
                           : mine                ? r->lack
                                                 : std::string{};
  panel(svc, mk.progress(), c, rates, show, lack);
  ImGui::EndChild();
}

} // namespace GUI::Market