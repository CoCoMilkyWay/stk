// Market→CTA 页: 左 = 两图 (价格与带, 底色 = 仓位 / 净值, 共一条 x), 其右 = 中栏 (设定 / 参数点云 / 光标处参数 / 全期指标; 与右表分右栏) — 见 TabLines.hpp
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
#include <mutex>
#include <numeric>
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

// 带间填色: 两线都有值的连续段各画一段 (同一 item)
void shaded(const char *label, const std::vector<double> &xs, const std::vector<double> &lo, const std::vector<double> &hi, const ImVec4 &col) {
  const auto ok = [&](size_t k) { return std::isfinite(lo[k]) && std::isfinite(hi[k]); };
  for (size_t b = 0; b < xs.size();) {
    while (b < xs.size() && !ok(b))
      ++b;
    size_t e = b;
    while (e < xs.size() && ok(e))
      ++e;
    if (e > b) {
      ImPlot::SetNextFillStyle(col, 0.10f);
      ImPlot::PlotShaded(label, xs.data() + b, lo.data() + b, hi.data() + b, static_cast<int>(e - b));
    }
    b = e;
  }
}

// 有效根 i 所在的选参段: pts 下标 (−1 = 未选参) + 段起根
std::pair<int, size_t> seg_at(const cta::Track &tr, size_t i) {
  const auto it = std::upper_bound(tr.seg.begin(), tr.seg.end(), i, [](size_t v, const std::pair<size_t, int> &s) { return v < s.first; });
  assert(it != tr.seg.begin());
  return {std::prev(it)->second, std::prev(it)->first};
}

// ---- 两图: 上 = 仓位色 (多 = 收盘线以上, 空 = 以下, 越满越浓) + 收盘 + 上线 / 下线 (带间填色) + 中线 + 分层线 (等差 / 等比) + 逐根时的买 / 卖标记;
//      下 = 策略 / 持有净值 + 成本带 (y 轴 log / 绝对, 右上角切换). x 同一视野 (链到 ui), 每像素取末根; 光标 = 可拖竖线 (两图同一条)
//      左键框选放大 (价格图连 y 一起; 净值图只 x), 右键拖动平移, 滚轮缩放, 双击回全段;
//      光标 = 橙色竖线 (同 OrderFlow 锚线): 参数点云展开时才出现 (初放视野正中), 只能拖动, 标注时刻 ----
void plots(CtaUIState &c, const CtaResult &r, const char *name, const ImVec2 &size) {
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
  const std::vector<double> close = take(se.c), up = take(tr.cut.up), mid = take(tr.cut.mid), dn = take(tr.cut.dn), nav = take(tr.strat.nav),
                            hold = take(tr.hold.nav);
  std::vector<double> wm(at.size(), 0); // wm[k] = 点 k → k + 1 这段所持的平均仓位 (根 at[k] .. at[k + 1] − 1 收盘调到的)
  for (size_t k = 0; k + 1 < at.size(); ++k) {
    for (size_t i = at[k]; i < at[k + 1]; ++i)
      wm[k] += w[i];
    wm[k] /= static_cast<double>(at[k + 1] - at[k]);
  }

  // 视野内逐根的买 / 卖 (只 step = 1 标)
  const size_t lo = static_cast<size_t>(std::clamp(std::ceil(c.x0), 0.0, static_cast<double>(n - 1)));
  const size_t hi = static_cast<size_t>(std::clamp(std::floor(c.x1), 0.0, static_cast<double>(n - 1)));
  const auto dw = [&](size_t i) { return w[i] - (i ? w[i - 1] : 0.0f); };
  int buys = 0, sells = 0;
  if (step == 1)
    for (size_t i = lo; i <= hi; ++i)
      buys += dw(i) > 1e-6f, sells += dw(i) < -1e-6f;

  // 标题: 标的 (高亮) · 模型 · 窗 · 视野 · 粒度
  std::string model;
  for (size_t j = 0; j < cta::kNP; ++j)
    model += std::string(j ? " / " : "") + cta::kParts[j].kinds[r.spec.kind[j]].name;
  char gran[64];
  if (step == 1)
    std::snprintf(gran, sizeof(gran), "逐根, 买 %d 次 / 卖 %d 次", buys, sells);
  else
    std::snprintf(gran, sizeof(gran), "每点 %zu 根 (放大到逐根看买卖)", step);
  ImGui::TextColored(kHead, "%s", name ? name : "");
  ImGui::SameLine();
  ImGui::TextDisabled("%s  ·  W %d 日  ·  %s ~ %s  ·  %s", model.c_str(), r.spec.roll, misc::dash(day_of(r, lo)).c_str(),
                      misc::dash(day_of(r, hi)).c_str(), gran);

  const ImGuiStyle &st = ImGui::GetStyle();
  const float ph = ImGui::GetContentRegionAvail().y - st.ItemSpacing.y, h1 = std::floor(ph * kPriceFrac);
  XFmt fmt{&r, c.x1 - c.x0 < 3.0 * kS};
  const double pad = static_cast<double>(n) * 0.02 + 10.0;
  bool pinned = c.cloud_open && c.cursor < 0, reset = false;
  double pin = c.cursor >= 0 ? c.cursor : std::clamp((c.x0 + c.x1) / 2, 0.0, static_cast<double>(n - 1));
  const auto setup_x = [&](ImPlotAxisFlags f) {
    ImPlot::SetupAxis(ImAxis_X1, nullptr, f);
    ImPlot::SetupAxisLinks(ImAxis_X1, &c.x0, &c.x1);
    ImPlot::SetupAxisFormat(ImAxis_X1, x_formatter, &fmt);
    ImPlot::SetupAxisLimitsConstraints(ImAxis_X1, -pad, static_cast<double>(n) + pad);
  };
  const auto track = [&](int id) { // 双击 = 回全段; 光标 (同 OrderFlow 锚线): 左键拖动, 松手停住
    reset |= ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(ImGuiMouseButton_Left);
    if (c.cloud_open)
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
    shaded("带", xs, dn, up, kMidCol);
    { // 分层线: 中线到上线 / 下线各 N 层 (Ladder::line, 与越线差同一份); 等差 = 价格等分 / 等比 = 对数价等分
      const cta::Ladder &lad = tr.lad;
      std::vector<double> y(xs.size());
      for (const double side : {1.0, -1.0})
        for (int j = 1; j < lad.N; ++j) {
          const double s = side * j / lad.N;
          for (size_t k = 0; k < xs.size(); ++k)
            y[k] = lad.line(s, tr.cut(at[k]));
          ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.25f), 1.0f);
          ImPlot::PlotLine("##分层", xs.data(), y.data(), static_cast<int>(xs.size()));
        }
    }
    ImPlot::SetNextLineStyle(kMidCol, 1.0f);
    ImPlot::PlotLine("中线", xs.data(), mid.data(), static_cast<int>(xs.size()));
    ImPlot::SetNextLineStyle(kDn, 1.2f);
    ImPlot::PlotLine("下线", xs.data(), dn.data(), static_cast<int>(xs.size()));
    ImPlot::SetNextLineStyle(kUp, 1.2f);
    ImPlot::PlotLine("上线", xs.data(), up.data(), static_cast<int>(xs.size()));
    ImPlot::SetNextLineStyle(ImGui::GetStyleColorVec4(ImGuiCol_Text), 1.2f);
    ImPlot::PlotLine("收盘", xs.data(), close.data(), static_cast<int>(xs.size()));
    if (step == 1) { // 买 ▲ (收盘下方) / 卖 ▼ (上方), 边长 ∝ |Δw| (翻向截到 1), 细描边
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      ImPlot::PushPlotClipRect();
      const float sc = ImGui::GetFontSize() / 13.0f;
      for (size_t i = lo; i <= hi; ++i) {
        const float d = dw(i);
        if (std::fabs(d) <= 1e-6f)
          continue;
        const ImVec2 p = ImPlot::PlotToPixels(static_cast<double>(i), se.c[i]);
        const float s = (7.0f + 14.0f * std::min(1.0f, std::fabs(d))) * sc / 2, y = d > 0 ? p.y + s * 1.2f : p.y - s * 1.2f, t = d > 0 ? -s : s;
        const ImVec2 a(p.x, y + t), b(p.x - s, y - t), e(p.x + s, y - t);
        dl->AddTriangleFilled(a, b, e, ImGui::GetColorU32(d > 0 ? kUp : kDn));
        dl->AddTriangle(a, b, e, IM_COL32(0, 0, 0, 160), 1.0f);
      }
      ImPlot::PopPlotClipRect();
    }
    track(0);
    if (c.cloud_open) {
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
     "越线差的震荡部分: 收盘价穿过层线 (前根收与本根收在线两侧) 时按收盘成交比按线价成交多付的差, 每条越过的线都按离收盘最近那条线的差计 (只越过一条线 = 全归震荡); Σ / 年数"},
    {"  跳空", &cta::Stat::gap, "%.2f%%", 100, "越线差的跳空部分: 一根越过几条线时, 远处的线比最近那条线多越的整档; Σ / 年数"},
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

// 光标处 (−1 = 末根) 所在选参段 → q = 所用配置的格点号 (未选参 = nullptr); 返回段说明
std::string cursor_at(const CtaResult &r, int cursor, const cta::Q *&q) {
  const size_t n = r.se->c.size(), i = cursor >= 0 ? static_cast<size_t>(cursor) : n - 1;
  const auto [x, from] = seg_at(r.tr, i);
  const std::string head = cursor >= 0 ? "光标 " + when(r, i) : "末根";
  q = x < 0 ? nullptr : &r.tr.pts[static_cast<size_t>(x)];
  return head + (x < 0 ? ": 买入持有 (首日 / 窗内数据不足 / 带未就绪, 未选参)" : ": 自 " + misc::dash(day_of(r, from)) + " 起用");
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

// ---- 参数点云: 至光标日打过分的配置在行为坐标 (cloud_of: 其选参窗内逐日目标值的前 3 主成分) 里的 3 维散点;
//      当日网格着色 = 窗内目标值 (拟合 / 实际), 圈 = 岭最优点; 其余 = 历史点, 淡画垫底; 正交投影, 左键拖动旋转, 悬停看配置 ----

// 配置 q 的参数: 一组一行 (组名 模型  轴 = 值 …)
std::string params_of(const cta::Grid &g, const cta::Q &q) {
  std::string s;
  size_t a = 0;
  for (size_t j = 0; j < cta::kNP; ++j) {
    const cta::Kind &k = cta::kParts[j].kinds[g.kinds[j]];
    s += std::string(j ? "\n" : "") + cta::kParts[j].title + " " + k.name;
    for (const cta::Axis &ax : k.axes) {
      char b[48];
      std::snprintf(b, sizeof(b), "  %s = %.3g%s", ax.name, g.value(a, q[a]), ax.day ? " 日" : "");
      s += b;
      ++a;
    }
  }
  return s;
}

void cloud(CtaUIState &c, const CtaResult &r) {
  const cta::Track &tr = r.tr;
  const size_t n = r.se->c.size();
  const size_t i = c.cursor >= 0 ? static_cast<size_t>(c.cursor) : n - 1, d = r.se->t[i] / kS - r.se->t[0] / kS;
  assert(d < tr.pick.size());

  static const char *const kShade[] = {"拟合", "实际"};
  int shade = c.cloud_fit ? 0 : 1;
  if (button_bar(kShade, 2, shade))
    c.cloud_fit = shade == 0;
  ImGui::SameLine();
  const cta::Pick &pk = tr.pick[d];
  if (pk.pt < 0) {
    ImGui::TextDisabled("%s  ·  该日未选参 (买入持有)", misc::dash(day_of(r, i)).c_str());
    return;
  }
  const std::vector<cta::Q> g = tr.grid.around(pk.center);
  const size_t ng = g.size(), d0 = pk.from;
  std::vector<uint32_t> id(ng); // [点] pts 下标: 当日网格 [0, ng) + 历史点 (至当日打过分、不在当日网格的配置) [ng, ns)
  std::vector<char> now(pk.seen, 0);
  std::vector<double> real(ng);
  size_t best = ng;
  for (size_t x = 0; x < ng; ++x) {
    id[x] = tr.index.at(g[x]), now[id[x]] = 1, real[x] = tr.acc[id[x]].over(d0, d);
    if (static_cast<int>(id[x]) == pk.pt)
      best = x;
  }
  assert(best < ng && "当日所用配置在当日网格内");
  for (uint32_t k = 0; k < pk.seen; ++k)
    if (!now[k])
      id.push_back(k);
  const size_t ns = id.size();
  const std::vector<double> fit = tr.grid.ridge.fit(real);
  if (c.cloud_gen != r.gen || c.cloud_day != d) { // 坐标: 窗内逐日目标值 → 前 3 主成分; 各成分朝当日网格拟合值升的一侧
    std::vector<float> score((d - d0) * ns);
    for (size_t e = d0; e < d; ++e)
      for (size_t x = 0; x < ns; ++x)
        score[(e - d0) * ns + x] = static_cast<float>(tr.acc[id[x]].over(e, e + 1));
    c.cloud = cta::cloud_of(score, ns), c.cloud_gen = r.gen, c.cloud_day = d;
    const double fm = std::accumulate(fit.begin(), fit.end(), 0.0) / static_cast<double>(ng);
    for (size_t k = 0; k < 3; ++k) {
      double s = 0;
      for (size_t x = 0; x < ng; ++x)
        s += c.cloud.at[x][k] * (fit[x] - fm);
      if (s < 0)
        for (cta::P3 &p : c.cloud.at)
          p[k] = -p[k];
    }
  }
  const cta::Cloud &cl = c.cloud;
  ImGui::TextDisabled("%s  ·  主成分 %.0f / %.0f / %.0f%%", misc::dash(day_of(r, i)).c_str(), cl.share[0] * 100, cl.share[1] * 100, cl.share[2] * 100);
  if (ImGui::IsItemHovered())
    tip("彩点 = 光标日 (主图可拖竖线) 的参数网格各配置; 淡点 = 历史点: 至当日打过分 (网格移动留下)、不在当日网格的配置\n"
        "坐标 = 两者一起在当日选参窗内逐日目标值序列的前 3 主成分 (表现相近的配置挨在一起; 各成分朝当日网格拟合值升的一侧),\n"
        "百分比 = 各主成分占配置间总方差; 着色 = 窗内目标值: 拟合 = 岭回归曲面 / 实际 = 逐点; 圈 = 岭最优点 (当日所用)\n"
        "• 左键拖动旋转, 悬停看配置");
  const std::vector<double> &v = c.cloud_fit ? fit : real;
  const auto [vlo, vhi] = std::minmax_element(v.begin(), v.end());
  const double lo = *vlo, span = *vhi - *vlo;

  // 画布 + 旋转
  const float w = ImGui::GetContentRegionAvail().x, h = std::floor(w * 0.8f);
  ImGui::InvisibleButton("##cta_cloud", ImVec2(w, h));
  const bool hovered = ImGui::IsItemHovered();
  if (ImGui::IsItemActive() && ImGui::IsMouseDragging(ImGuiMouseButton_Left, 0.0f)) {
    const ImVec2 dm = ImGui::GetIO().MouseDelta;
    c.yaw += dm.x * 0.01f, c.pitch = std::clamp(c.pitch + dm.y * 0.01f, -1.5f, 1.5f);
  }
  const ImVec2 p0 = ImGui::GetItemRectMin(), p1 = ImGui::GetItemRectMax(), o((p0.x + p1.x) / 2, (p0.y + p1.y) / 2);
  ImDrawList *dl = ImGui::GetWindowDrawList();
  dl->AddRectFilled(p0, p1, ImGui::GetColorU32(ImGuiCol_FrameBg));
  dl->PushClipRect(p0, p1, true);

  // 坐标 → 中心化, 三轴同一尺度 (保持主成分间的比例) → 绕竖轴 yaw, 俯仰 pitch → 屏幕 (x, y) + 深度 z (大 = 远)
  cta::P3 mid{}, half{};
  for (size_t k = 0; k < 3; ++k) {
    const auto [a, b] = std::minmax_element(cl.at.begin(), cl.at.end(), [k](const cta::P3 &p, const cta::P3 &q) { return p[k] < q[k]; });
    mid[k] = ((*a)[k] + (*b)[k]) / 2, half[k] = ((*b)[k] - (*a)[k]) / 2;
  }
  const float s = std::max({half[0], half[1], half[2], 1e-12f}), scale = 0.36f * std::min(w, h);
  const float cy = std::cos(c.yaw), sy = std::sin(c.yaw), cp = std::cos(c.pitch), sp = std::sin(c.pitch);
  struct Px {
    ImVec2 p;
    float z;
  };
  const auto proj = [&](float a, float b, float e) {
    const float x = a * cy - b * sy, q = a * sy + b * cy;
    return Px{ImVec2(o.x + x * scale, o.y - (e * cp - q * sp) * scale), q * cp + e * sp};
  };
  const auto at = [&](size_t x) { return proj((cl.at[x][0] - mid[0]) / s, (cl.at[x][1] - mid[1]) / s, (cl.at[x][2] - mid[2]) / s); };

  { // 外框 (各轴实际跨度) + 轴名
    const ImU32 edge = ImGui::GetColorU32(ImGuiCol_TextDisabled, 0.35f);
    const float hx[3] = {half[0] / s, half[1] / s, half[2] / s};
    const auto corner = [&](int m) { return proj(m & 1 ? hx[0] : -hx[0], m & 2 ? hx[1] : -hx[1], m & 4 ? hx[2] : -hx[2]).p; };
    for (int m = 0; m < 8; ++m)
      for (int b = 1; b < 8; b <<= 1)
        if (!(m & b))
          dl->AddLine(corner(m), corner(m | b), edge);
    static const char *const kAxis[] = {"PC1", "PC2", "PC3"};
    for (int k = 0; k < 3; ++k)
      dl->AddText(corner(1 << k), ImGui::GetColorU32(ImGuiCol_TextDisabled), kAxis[k]);
  }

  // 点: 历史点垫底 (淡, 小, 无描边); 当日网格远的先画, 近大远小; 悬停取屏幕上最近的当日点
  std::vector<Px> px(ns);
  std::vector<size_t> ord(ng);
  for (size_t x = 0; x < ns; ++x)
    px[x] = at(x);
  for (size_t x = 0; x < ng; ++x)
    ord[x] = x;
  std::sort(ord.begin(), ord.end(), [&](size_t a, size_t b) { return px[a].z > px[b].z; });
  const float r0 = std::max(3.0f, ImGui::GetFontSize() * 0.32f);
  const ImU32 faint = ImGui::GetColorU32(ImGuiCol_TextDisabled, 0.2f);
  for (size_t x = ng; x < ns; ++x)
    dl->AddCircleFilled(px[x].p, 0.6f * r0 * (1.0f - 0.25f * px[x].z), faint);
  const ImVec2 mouse = ImGui::GetIO().MousePos;
  int hot = -1;
  float dmin = FLT_MAX;
  for (const size_t x : ord) {
    const float rad = r0 * (1.0f - 0.25f * px[x].z);
    const ImVec4 col = ImPlot::SampleColormap(span > 0 ? static_cast<float>((v[x] - lo) / span) : 0.5f, ImPlotColormap_Viridis);
    dl->AddCircleFilled(px[x].p, rad, ImGui::GetColorU32(col));
    dl->AddCircle(px[x].p, rad, IM_COL32(0, 0, 0, 140));
    if (x == best)
      dl->AddCircle(px[x].p, rad + 4, ImGui::GetColorU32(ImGuiCol_Text), 0, 2.0f);
    const float dx = px[x].p.x - mouse.x, dy = px[x].p.y - mouse.y, e = dx * dx + dy * dy;
    if (hovered && e <= (rad + 3) * (rad + 3) && e < dmin)
      dmin = e, hot = static_cast<int>(x);
  }
  dl->PopClipRect();
  if (hot >= 0) {
    const size_t x = static_cast<size_t>(hot);
    tip("%s\n拟合 %.4f  ·  实际 %.4f%s", params_of(tr.grid, g[x]).c_str(), fit[x], real[x], x == best ? "\n岭最优点 (当日所用)" : "");
  }

  // 色标: 低 → 高
  const float bh = ImGui::GetTextLineHeight() * 0.5f;
  const ImVec2 b0 = ImGui::GetCursorScreenPos();
  constexpr int kSeg = 32;
  for (int k = 0; k < kSeg; ++k) {
    const float x0 = b0.x + w * static_cast<float>(k) / kSeg, x1 = b0.x + w * static_cast<float>(k + 1) / kSeg;
    dl->AddRectFilled(ImVec2(x0, b0.y), ImVec2(x1, b0.y + bh), ImGui::GetColorU32(ImPlot::SampleColormap((static_cast<float>(k) + 0.5f) / kSeg, ImPlotColormap_Viridis)));
  }
  ImGui::Dummy(ImVec2(w, bh));
  char a[32], b[32];
  std::snprintf(a, sizeof(a), "%.4f", lo);
  std::snprintf(b, sizeof(b), "%.4f", lo + span);
  ImGui::TextDisabled("%s", a);
  ImGui::SameLine();
  right(b, true);
}

void panel(const CtaService &svc, CtaUIState &c, const CtaResult *r, const std::string &lack) {
  // 状态: 进度 / 算不了的原因 / 全期跨度
  const Phase ph = svc.phase.load(std::memory_order_acquire);
  const size_t done = svc.done.load(std::memory_order_relaxed), total = svc.total.load(std::memory_order_relaxed);
  if (ph == Phase::Loading)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "合成价格 %zu/%zu 日", done, total);
  else if (ph == Phase::Running)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "滚动选参 %zu/%zu 日", done, total);
  else if (!lack.empty())
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s", lack.c_str());
  else if (r) {
    const size_t n = r->se->c.size();
    ImGui::TextDisabled("%s ~ %s  ·  %zu 根", misc::dash(day_of(*r, 0)).c_str(), misc::dash(day_of(*r, n - 1)).c_str(), n);
  } else
    ImGui::TextDisabled("未算");
  if (ph == Phase::Loading && ImGui::IsItemHovered())
    tip("首次一趟合成全部篮子线与个股的 1 分钟价, 存在 all.bar 库目录 cta/ 下 (十年全市场约 30 GB); 之后只补新日, 取消不丢已合成的");

  // 设定: 分组 (优化目标 / 三组部件 / 引擎), 组内一行一项, 名左值右
  CtaSpec &sp = c.spec;
  const float x0 = ImGui::GetCursorPosX();
  float lw = 0.0f; // 名列宽 (控件 / 灰显参数都从这里起)
  for (const char *s : {"滚动窗 (日)", "网格配置数", "模型", "方向", "佣金 (bp)", "印花税 (bp)", "冲击 (bp)", "目标年化波动", "不交易带"})
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
  ImGui::InputInt("##cta_roll", &sp.roll, 50, 250);
  sp.roll = std::max(sp.roll, static_cast<int>(cta::kMinRoll));
  c.editing = ImGui::IsItemActive();
  if (ImGui::IsItemHovered())
    tip("每日以此前至多 W 日 (≥ %zu; 不足 W 日 = 自首日起全部, expanding) 网格各配置的目标值拟岭回归曲面, 取最高点 (平原顶) 用到当日; "
        "首日 / 窗内根数不足窗长一半 / 窗内各配置的带都未就绪 = 买入持有 (与基准同口径)\n"
        "• 编辑完生效",
        cta::kMinRoll);
  label("网格配置数");
  ImGui::InputInt("##cta_grid", &sp.grid, 10, 50);
  sp.grid = std::clamp(sp.grid, 8, 1000);
  c.editing |= ImGui::IsItemActive();
  if (ImGui::IsItemHovered())
    tip("参数轴 = 自 v0 向两侧铺的格点, 相邻之比 %.3g, 间距不小于各轴基本单位 (小值处退成等差), 收进各轴硬边界\n"
        "网格 = 以当前最优为中点、各多值轴 m 个相邻格点的全部组合 (碰到硬边界则整格平移进界); 各轴的 m 轮流分得 (每次给格点最少的轴加一个), 积不超过此数\n"
        "每日最高点落在网格边上就以它为中心移网格、补算新格点再拟 (当日至多 %d 次), 中心留到次日\n"
        "• 编辑完生效",
        cta::kStep, cta::kHops);
  hint("各组只选模型, 参数每日以此前至多 W 日的网格曲面最高点选出, 网格随最高点移动");
  c.cloud_open = r && ImGui::CollapsingHeader("参数点云");
  if (c.cloud_open)
    cloud(c, *r);

  const cta::Q *at = nullptr;
  const std::string where = r ? cursor_at(*r, c.cloud_open ? c.cursor : -1, at) : std::string{};
  const cta::Kinds &shown = r ? r->spec.kind : sp.kind; // 灰显参数按结果的模型
  for (size_t j = 0, a = 0; j < cta::kNP; ++j) {
    const cta::Part &p = cta::kParts[j];
    ImGui::SeparatorText(p.title);
    label("模型");
    model_combo(("##cta_part" + std::to_string(j)).c_str(), p.kinds, sp.kind[j], std::string(p.title) + ": " + p.tip);
    for (const cta::Axis &ax : p.kinds[shown[j]].axes) {
      char s[32];
      std::snprintf(s, sizeof(s), "%s%s", ax.name, ax.day ? " (日)" : "");
      ImGui::TextUnformatted(s);
      ImGui::SameLine(x0 + lw);
      if (at)
        ImGui::TextDisabled("%.3g", r->tr.grid.value(a, (*at)[a]));
      else
        ImGui::TextDisabled("-");
      ++a;
    }
  }
  if (r) {
    hint(where.c_str());
    if (ImGui::IsItemHovered())
      tip("灰显 = 光标 (参数点云展开时图上可拖的竖线; 收起 = 末根) 处所用的参数; 周期轴以日计, k = 半宽倍数, N = 每侧层数");
  }

  ImGui::SeparatorText("引擎");
  {
    constexpr int nd = static_cast<int>(std::size(cta::kDirs));
    const char *names[nd];
    std::string s = "方向: 仓位按方向截取, 方向之外的半边空仓";
    for (int k = 0; k < nd; ++k)
      names[k] = cta::kDirs[k].name, s += std::string("\n• ") + cta::kDirs[k].name + " = " + cta::kDirs[k].tip;
    label("方向");
    ImGui::BeginGroup();
    button_bar(names, nd, sp.dir);
    ImGui::EndGroup();
    if (ImGui::IsItemHovered())
      tip("%s", s.c_str());
  }
  const auto bp = [&](const char *name, const char *id, float &v, const char *s) {
    label(name);
    ImGui::InputFloat(id, &v, 0.5f, 1.0f, "%.1f");
    v = std::max(v, 0.0f);
    c.editing |= ImGui::IsItemActive();
    if (ImGui::IsItemHovered())
      tip("%s; 编辑完生效", s);
  };
  bp("佣金 (bp)", "##cta_commission", sp.commission_bp, "佣金 = 佣金率 × |Δ仓位|, 买卖各一次");
  bp("印花税 (bp)", "##cta_stamp", sp.stamp_bp, "印花税 = 印花税率 × 卖出量 (多头减仓 / 融券卖出), 买入不收");
  bp("冲击 (bp)", "##cta_impact", sp.impact_bp,
     "冲击 = 冲击率 × |Δ仓位|, 固定, 不按金额变\n• 震荡 / 跳空另计: 收盘价穿过层线时按收盘成交比按线价成交多付的差; 最近那条线的差 = 震荡, 一根越过几条线多出的整档 = 跳空");
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

void RenderLinesCta(CtaService &svc, LinesUIState &ui, const char *name, const ImVec2 &plots_size, float panel_w) {
  CtaUIState &c = ui.cta;
  const std::map<std::string, Quote::Ref> &one = ui.pick.rows[kPickOne];
  const Quote::Ref q = one.empty() ? Quote::Ref{} : one.begin()->second;
  if (q.ok() && !c.editing && !(c.sent.gen && c.sent.ref == q && c.sent.spec == c.spec)) {
    c.sent = {c.sent.gen + 1, q, c.spec};
    svc.Request(c.sent);
  } else if (!q.ok() && !c.sent.gen) { // 首次打开未选标的: 先备价格库
    c.sent = {1, {}, c.spec};
    svc.Request(c.sent);
  }
  std::shared_ptr<const CtaResult> r;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    r = svc.result;
  }
  const bool mine = q.ok() && r && r->ref == q; // 本标的的 (设定改了时新的还在算, 先显旧的)
  const CtaResult *show = mine && r->lack.empty() ? r.get() : nullptr;

  ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
  ImGui::BeginChild("##cta_plots", plots_size);
  ImGui::PopStyleVar();
  if (!q.ok())
    ImGui::TextDisabled("右表单选一个指数 / 行业 / 概念 / 成分股 (与 VAL 同一份选中)");
  else if (show)
    plots(c, *show, name, ImGui::GetContentRegionAvail());
  else if (!mine || r->gen != c.sent.gen)
    ImGui::TextDisabled("计算中…");
  ImGui::EndChild();
  ImGui::SameLine();
  ImGui::BeginChild("##cta_panel", ImVec2(panel_w, plots_size.y), ImGuiChildFlags_Borders);
  panel(svc, c, show, mine ? r->lack : std::string{});
  ImGui::EndChild();
}

} // namespace GUI::Market