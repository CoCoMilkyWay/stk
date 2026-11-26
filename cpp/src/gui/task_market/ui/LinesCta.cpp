// Market→CTA 页: 左 = 三图 (价格与中线 / 档线, 底色 = 仓位 / 净值 / 回撤, 共一条 x) + 区间条, 其右 = 中栏 (设定 / 参数轨迹 / 光标处参数 / 视野内指标; 与右表分右栏) — 见 TabLines.hpp
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
#include <iterator>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Market {

namespace {

constexpr size_t kS = Quote::kSlots;
constexpr float kPriceFrac = 0.55f, kDdFrac = 0.15f;                                                                 // 价格图 / 回撤图占三图高的比例 (余 = 净值图)
constexpr ImVec4 kMidCol(1.0f, 0.6f, 0.2f, 1.0f), kHoldCol(0.6f, 0.6f, 0.6f, 1.0f), kPinCol(1.0f, 0.5f, 0.0f, 1.0f); // kPinCol = 光标 (同 OrderFlow 锚线)
constexpr ImVec4 kFollowSellCol(0.25f, 0.70f, 0.95f, 1.0f);                                                          // 跟随卖出线 (减仓线 = kDn, 跟随买入线 = kUp)
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

// 横轴读数: x = 有效根下标 → YY/MM/DD; 视野不足几日 → MM/DD hh:mm
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
// 横轴刻度的日轴: 有根的日 + 各日首根前半格的日界 (x 按根不等宽) + 各日末根
struct XDays {
  std::vector<misc::Ymd> dates;
  std::vector<double> xs;
  std::vector<size_t> last;
};
XDays x_days(const CtaResult &r) {
  const cta::Days &ds = r.tr->days;
  const size_t d0 = r.se->t[0] / kS;
  XDays o;
  for (size_t d = 0; d < ds.size(); ++d)
    if (ds.off[d] < ds.off[d + 1])
      o.dates.push_back(r.dates[d0 + d]), o.xs.push_back(static_cast<double>(ds.off[d]) - 0.5), o.last.push_back(ds.off[d + 1] - 1);
  return o;
}

// 标的 BP Band 的来源 (VAL 标的同一份): 日并集轴 dates 上, 轴日 k 的估值账行 at[k] (−1 = 无), BP 收益率 v [账行] (nullptr = 无估值腿),
// 滚动分位 roll (窗 years 年); 轴日 k 的 band_j 偏离 = v / q_j(k) (× 价格 = 该档价)
struct PbBand {
  const std::vector<misc::Ymd> &dates;
  const std::vector<int32_t> &at;
  const float *v;
  const LinesRoll &roll;
  int years;
};

// ---- 区间条 (range_bar, 同 NAV / VAL): x = 有根的日 (XDays 下标), 底线 = 各日末根收盘; 选段 [d0, d1) ↔ 两图 x 视野:
//      每帧由视野定 (含该视野的整日段), 拖了 → 视野改成该整日段, 价格 y 回跟随;
//      净值图恒以视野首根的前一根为 1 (见 plots) ----
int range_formatter(double v, char *buf, int size, void *ud) {
  const auto &dates = *static_cast<const std::vector<misc::Ymd> *>(ud);
  const misc::Ymd y = dates[static_cast<size_t>(std::clamp(v, 0.0, static_cast<double>(dates.size() - 1)))];
  return std::snprintf(buf, static_cast<size_t>(size), "%02d/%02d/%02d", y / 10000 % 100, y / 100 % 100, y % 100);
}
void range(CtaState &c, const CtaResult &r, const XDays &xd, const ImVec2 &size) {
  const int K = static_cast<int>(xd.dates.size());
  assert(K > 0);
  std::vector<double> b(xd.xs); // 日界 [K + 1]: 日 k 占 x ∈ [b[k], b[k + 1])
  b.push_back(static_cast<double>(r.se->c.size()) - 0.5);
  DayRange &rg = c.range;
  rg.d0 = std::clamp(static_cast<int>(std::ranges::upper_bound(b, c.x0) - b.begin()) - 1, 0, K - 1);
  rg.d1 = std::clamp(static_cast<int>(std::ranges::lower_bound(b, c.x1) - b.begin()), rg.d0 + 1, K);
  const int d0 = rg.d0, d1 = rg.d1;
  push_range_style();
  if (!ImPlot::BeginPlot("##cta_range", size, ImPlotFlags_NoTitle | ImPlotFlags_NoLegend | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoMouseText)) {
    ImPlot::PopStyleVar();
    return;
  }
  ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoTickLabels, ImPlotAxisFlags_NoDecorations | ImPlotAxisFlags_AutoFit);
  ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, static_cast<double>(K), ImPlotCond_Always);
  DayAxis ax{xd.dates, 1.0, 0.0, {}, range_formatter, const_cast<std::vector<misc::Ymd> *>(&xd.dates)};
  setup_day_axis(ax);
  std::vector<double> xs(xd.last.size()), ys(xd.last.size());
  for (size_t k = 0; k < xd.last.size(); ++k)
    xs[k] = static_cast<double>(k) + 0.5, ys[k] = r.se->c[xd.last[k]];
  ImPlot::SetNextLineStyle(ImGui::GetStyleColorVec4(ImGuiCol_Text), 1.0f);
  ImPlot::PlotLine("##cta_range_line", xs.data(), ys.data(), static_cast<int>(xs.size()));
  x_ticks_inside();
  range_bar(rg, xd.dates, 1.0);
  ImPlot::EndPlot();
  ImPlot::PopStyleVar();
  if (rg.d0 != d0 || rg.d1 != d1)
    c.x0 = b[static_cast<size_t>(rg.d0)], c.x1 = b[static_cast<size_t>(rg.d1)], c.fit_y = true;
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

// ---- 三图: 上 = 收盘 (按仓位着色: 多 → kUp / 空 → kDn, 越满越纯) + 中线 + 档线 (每份一条: 减仓线 / 跟随买入线 / 跟随卖出线) + 买 / 卖标记 (横轴刻度进到日档才画)
//        + PB Band (同 VAL: 偏离 v / q_j 乘本图当日末根收盘, 每日一点在该日末根; 填色 + 各档线共一个图例项, 默认关);
//      中 = 策略 / 持有净值 + 成本带 (y 轴 log / 绝对, 右上角切换); 下 = 策略 / 持有回撤 (%); 底 = 区间条.
//      窄边框, x / y 刻度标签都画在图区内 (x 只回撤图标, 三图同一套刻度线; y 在各图右边), 三图 + 区间条绘图区同宽;
//      x 同一视野 (链到 ui), 每像素取末根; 光标 = 可拖竖线 (三图同一条)
//      左键框选放大 (价格图连 y 一起; 净值 / 回撤图只 x, 两图同画), 右键拖动平移, 滚轮缩放, 双击回全段;
//      光标 = 橙色竖线 (同 OrderFlow 锚线): 常显 (初放视野正中), 只能拖动, 标注时刻;
//      净值图 (策略 / 持有 / 成本带) 恒以视野首根的前一根 = 1, 回撤自该根起算: 视野怎么变 (框选 / 滚轮 / 平移 / 区间条 / 双击) 都随之重新对齐 ----
void plots(CtaState &c, const CtaResult &r, const char *name, const PbBand &pb, const ImVec2 &size) {
  const cta::Series &se = *r.se;
  const cta::Track &tr = *r.tr;
  const size_t n = se.c.size();
  if (c.fit != r.ref) { // 换标的: 回全段, 光标重放
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
  const XDays xd = x_days(r);
  const size_t first = static_cast<size_t>(std::clamp(std::ceil(c.x0), 0.0, static_cast<double>(n - 1)));
  const size_t anc = first ? first - 1 : 0; // 对齐根: 视野首根的前一根 (视野自首根起 = 首根); 净值 / 成本带在此 = 1
  const cta::Col &w = tr.strat.run.w;
  const std::vector<double> close = take(se.c), mid = take(tr.cut.mid);
  std::vector<double> nav = take(tr.strat.nav[0]), hold = take(tr.hold.nav[0]);
  for (size_t k = 0; k < at.size(); ++k)
    nav[k] /= tr.strat.nav[0][anc], hold[k] /= tr.hold.nav[0][anc];
  const auto drawdown = [&](const auto &v) { // 回撤 (%): 净值 / 自对齐根起逐根的最高 − 1, 取样点读; 对齐根之前 = 0
    std::vector<double> o(at.size(), 0);
    double top = v[anc];
    for (size_t k = 0, i = anc; k < at.size(); ++k) {
      if (at[k] < anc)
        continue;
      for (; i <= at[k]; ++i)
        top = std::max<double>(top, v[i]);
      o[k] = (v[at[k]] / top - 1) * 100;
    }
    return o;
  };
  const std::vector<double> dd = drawdown(tr.strat.nav[0]), dd_hold = drawdown(tr.hold.nav[0]);
  std::vector<double> wm(at.size(), 0); // wm[k] = 点 k → k + 1 这段所持的平均仓位 (根 at[k] .. at[k + 1] − 1 收盘调到的)
  for (size_t k = 0; k + 1 < at.size(); ++k) {
    for (size_t i = at[k]; i < at[k + 1]; ++i)
      wm[k] += w[i];
    wm[k] /= static_cast<double>(at[k + 1] - at[k]);
  }

  // 买 / 卖: 视野内逐根计次; 横轴刻度进到日档 (同 day_locator 的选档) 即画标记, 一根一个 (在该根的 x)
  XFmt fmt{&r, c.x1 - c.x0 < 3.0 * kS};
  DayAxis ax{xd.dates, 1.0, 0.0, xd.xs, x_formatter, &fmt};
  const bool daily = day_ticks(ax, {c.x0, c.x1}, size.x).u == kTickDay;
  const size_t lo = static_cast<size_t>(std::clamp(std::ceil(c.x0), 0.0, static_cast<double>(n - 1)));
  const size_t hi = static_cast<size_t>(std::clamp(std::floor(c.x1), 0.0, static_cast<double>(n - 1)));
  const auto dw = [&](size_t i) { return w[i] - (i ? w[i - 1] : 0.0f); };
  std::vector<size_t> marks;
  int buys = 0, sells = 0;
  for (size_t i = lo; i <= hi; ++i) {
    const float d = dw(i);
    if (std::fabs(d) <= 1e-6f)
      continue;
    ++(d > 0 ? buys : sells);
    if (daily)
      marks.push_back(i);
  }

  // 标题: 标的 (高亮) · 模型 · 窗 · 视野 · 粒度
  const std::string model = std::string(cta::Band::kinds[r.spec.band.kind].name) + " / " + cta::Reduce::kinds[r.spec.reduce.kind].name + " / " +
                            cta::Follow::kinds[r.spec.follow.kind].name + " / " + cta::kDirs[r.spec.dir].name;
  char gran[96];
  if (step == 1)
    std::snprintf(gran, sizeof(gran), "逐根, 买 %d 次 / 卖 %d 次", buys, sells);
  else
    std::snprintf(gran, sizeof(gran), daily ? "每点 %zu 根, 买 %d 次 / 卖 %d 次" : "每点 %zu 根 (放大到日刻度才标买卖), 买 %d 次 / 卖 %d 次", step, buys, sells);
  ImGui::TextColored(kHead, "%s", name ? name : "");
  ImGui::SameLine();
  ImGui::TextDisabled("%s  ·  窗 %d / %d / %d 年  ·  %s ~ %s  ·  %s", model.c_str(), r.spec.band.win, r.spec.reduce.win, r.spec.follow.win,
                      misc::dash(day_of(r, lo)).c_str(), misc::dash(day_of(r, hi)).c_str(), gran);

  const ImGuiStyle &st = ImGui::GetStyle();
  const float ph = ImGui::GetContentRegionAvail().y - 3 * st.ItemSpacing.y - kRangeH, h1 = std::floor(ph * kPriceFrac), h3 = std::floor(ph * kDdFrac);
  // PB Band: 有根的日各一点 (该日末根), 价 = 末根收盘 × v / q_j (同 VAL 断开: 轴上无该日 / v ≤ 0 / 窗内有无解点 / q_j ≤ 0 = NaN); 填色只画各档全有限的连续段
  const size_t nk = xd.dates.size();
  std::vector<double> pb_x(nk), pb_y[kBandQ];
  std::vector<std::pair<int, int>> pb_segs;
  for (std::vector<double> &y : pb_y)
    y.assign(nk, kNaN);
  assert(!pb.v || (pb.at.size() == pb.dates.size() && pb.roll.band.size() == pb.dates.size()));
  for (size_t k = 0; k < nk; ++k) {
    pb_x[k] = static_cast<double>(xd.last[k]);
    const auto it = std::ranges::lower_bound(pb.dates, xd.dates[k]);
    if (!pb.v || it == pb.dates.end() || *it != xd.dates[k])
      continue;
    const size_t j = static_cast<size_t>(it - pb.dates.begin());
    const double v = pb.at[j] >= 0 ? pb.v[pb.at[j]] : kNaN;
    if (!(v > 0.0) || !pb.roll.band[j])
      continue;
    for (int q = 0; q < kBandQ; ++q)
      if (const float qk = pb.roll.q[q][j]; qk > 0.f)
        pb_y[q][k] = se.c[xd.last[k]] * v / qk;
    if (!(pb.roll.q[kBandQ - 1][j] > 0.f)) // q 降序: 最贵档 > 0 ⇔ 各档全有限
      continue;
    const int ik = static_cast<int>(k);
    if (pb_segs.empty() || pb_segs.back().second != ik)
      pb_segs.push_back({ik, ik + 1});
    else
      ++pb_segs.back().second;
  }
  char pb_lb[48];
  std::snprintf(pb_lb, sizeof(pb_lb), "PB %d年 Band###cta_pb", pb.years);
  const double pad = static_cast<double>(n) * 0.02 + 10.0;
  bool pinned = c.cursor < 0, reset = false;
  double pin = c.cursor >= 0 ? c.cursor : std::clamp((c.x0 + c.x1) / 2, 0.0, static_cast<double>(n - 1));
  const auto setup_x = [&] { // 刻度标签不占轴下一行 (回撤图由 x_ticks_inside 画进图区), 三图同一套刻度线
    ImPlot::SetupAxis(ImAxis_X1, nullptr, ImPlotAxisFlags_NoTickLabels);
    ImPlot::SetupAxisLinks(ImAxis_X1, &c.x0, &c.x1);
    setup_day_axis(ax);
    ImPlot::SetupAxisLimitsConstraints(ImAxis_X1, -pad, static_cast<double>(n) + pad);
  };
  const auto track = [&](int id) { // 双击 = 回全段; 光标 (同 OrderFlow 锚线): 左键拖动, 松手停住
    reset |= ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(ImGuiMouseButton_Left);
    pinned |= ImPlot::DragLineX(id, &pin, kPinCol, 2.0f, ImPlotDragToolFlags_NoFit);
  };
  double zx = 0;         // 净值 / 回撤图横向框选的鼠标 x (两图对齐, 同值)
  const auto box = [&] { // 净值 / 回撤图: 左键按下起框 (拖光标时不起), 框选中两图同画
    zx = plot_mouse_x();
    if (ImPlot::IsPlotHovered() && ImGui::IsMouseClicked(ImGuiMouseButton_Left) && !ImGui::IsAnyItemActive())
      c.zoom.begin(zx);
    if (c.zoom.on())
      c.zoom.draw(zx);
  };
  const ImPlotInputMap keep = ImPlot::GetInputMap(); // 本页三图: 右键平移; 左键 = 价格图框选 (x + y) / 净值 / 回撤图横向框选 (XZoom); 画完还原
  ImPlot::MapInputReverse();
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(2, 1)); // 窄边框 (同区间条); x / y 刻度标签都画在图区内, 三图 + 区间条绘图区同宽
  ImPlot::PushStyleVar(ImPlotStyleVar_LabelPadding, ImVec2(2, 1));

  if (c.boxing && ImGui::IsMouseReleased(ImGuiMouseButton_Left)) // 框选松手: 本帧起用框的 y
    c.fit_y = false;
  if (c.fit_y)
    ImPlot::SetNextAxisToFit(ImAxis_Y1);
  if (ImPlot::BeginPlot("##cta_px", ImVec2(size.x, h1), ImPlotFlags_NoTitle | ImPlotFlags_NoMenus)) {
    setup_x();
    ImPlot::SetupAxis(ImAxis_Y1, nullptr, ImPlotAxisFlags_RangeFit | ImPlotAxisFlags_NoTickLabels);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_Horizontal);
    { // PB Band (同 VAL 主图): 色块 c ∈ [0, kBandQ]: 0 = 最便宜档以下 (到图底), kBandQ = 最贵档以上 (到图顶), 其余 = 相邻两档之间; 线进 fit, 填色不进
      ImPlot::HideNextItem(true, ImPlotCond_Once);
      for (const auto &[i0, i1] : pb_segs) {
        const int m = i1 - i0;
        const double *x = pb_x.data() + i0;
        for (int q = 0; q <= kBandQ; ++q) {
          ImPlot::SetNextFillStyle(band_col(static_cast<double>(q) / kBandQ, 1.0f), kBandFillA);
          if (q == 0)
            ImPlot::PlotShaded(pb_lb, x, pb_y[0].data() + i0, m, -HUGE_VAL, ImPlotItemFlags_NoFit);
          else if (q == kBandQ)
            ImPlot::PlotShaded(pb_lb, x, pb_y[kBandQ - 1].data() + i0, m, HUGE_VAL, ImPlotItemFlags_NoFit);
          else
            ImPlot::PlotShaded(pb_lb, x, pb_y[q - 1].data() + i0, pb_y[q].data() + i0, m, ImPlotItemFlags_NoFit);
        }
      }
      for (int q = 0; q < kBandQ; ++q) {
        ImPlot::SetNextLineStyle(band_col((q + 0.5) / kBandQ, kBandLineA), q == kBandQ / 2 ? 1.5f : 1.0f);
        ImPlot::PlotLine(pb_lb, pb_x.data(), pb_y[q].data(), static_cast<int>(nk), ImPlotLineFlags_SkipNaN);
      }
    }
    { // 档线 (Cuts::rungs_at) = 生效的线: 一份一条连续线 (恒为份数条), 段色按段末的类别: 减仓线绿 / 跟随买入线红 / 跟随卖出线蓝 (同一不透明度);
      //   图例: 实际减仓线 (持有的: 生效的预期卖出线 / 锁定的减仓线 = 绿 + 跟随卖出线 = 蓝) / 跟随买入线 (红) / 预期减仓线 (未生效的预期卖出线)
      //   悬停: 鼠标处离它最近的那份 (kRungHit px 内) 的线 (各段) 一起高亮, 提示该份
      //   + 预期减仓线 (静态档线, 未生效): 第 j 条 = 中线 − j × band = 第 j 份的预期, 更淡, 不进 fit, 不参与高亮; 独立图例项, 默认关
      static const char *const kName[] = {"实际减仓线", "跟随买入线"}; // 图例项; 类别 → 项: 减仓线 / 跟随卖出线 → 0, 跟随买入线 → 1
      const ImVec4 kCol[] = {kDn, kUp, kFollowSellCol};                // 下标 = 类别 (cta::kReduce / kFollowBuy / kFollowSell)
      const auto group = [](uint8_t type) { return type == cta::kFollowBuy ? 1 : 0; };
      constexpr float kRungHit = 6.0f, kRungA = 0.8f;
      const size_t U = static_cast<size_t>(tr.cut.units);
      const size_t K = at.size();
      { // 预期减仓线
        const std::vector<double> hb = take(tr.cut.h);
        std::vector<double> ey(K);
        ImPlot::HideNextItem(true, ImPlotCond_Once);
        for (size_t j = 0; j < U; ++j) {
          for (size_t k = 0; k < K; ++k)
            ey[k] = mid[k] - tr.cut.s * static_cast<double>(j) * hb[k];
          ImPlot::SetNextLineStyle(ImVec4(kDn.x, kDn.y, kDn.z, 0.1f), 1.0f);
          ImPlot::PlotLine("预期减仓线", xs.data(), ey.data(), static_cast<int>(K), ImPlotItemFlags_NoFit);
        }
      }
      std::vector<float> p(U * K, cta::kNaN); // [份][点] 价
      std::vector<uint8_t> pol(p.size(), 0);  // [份][点] 类别
      for (size_t k = 0; k < K; ++k)
        tr.cut.rungs_at(at[k], [&](int j, float v, uint8_t type) { p[static_cast<size_t>(j) * K + k] = v, pol[static_cast<size_t>(j) * K + k] = type; });
      bool show[std::size(kName)];
      for (size_t a = 0; a < std::size(kName); ++a) {
        ImPlot::SetNextLineStyle(kCol[a]);
        ImPlot::PlotDummy(kName[a]);
        const ImPlotItem *it = ImPlot::GetItem(kName[a]);
        show[a] = !it || it->Show;
      }
      int hot = -1; // 悬停的份
      size_t hk = 0;
      if (ImPlot::IsPlotHovered() && K) {
        hk = std::min(K - 1, static_cast<size_t>(std::ranges::lower_bound(xs, ImPlot::GetPlotMousePos().x) - xs.begin()));
        const float my = ImGui::GetMousePos().y;
        float best = kRungHit;
        for (size_t j = 0; j < U; ++j)
          if (const size_t e = j * K + hk; !std::isnan(p[e]) && show[group(pol[e])])
            if (const float dy = std::fabs(ImPlot::PlotToPixels(xs[hk], p[e]).y - my); dy < best)
              best = dy, hot = static_cast<int>(j);
      }
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      ImPlot::PushPlotClipRect();
      const auto draw = [&](size_t j, bool hl) {
        for (size_t k = 0; k + 1 < K; ++k) {
          const size_t e = j * K + k + 1;
          const float y0 = p[e - 1], y1 = p[e];
          if (std::isnan(y0) || std::isnan(y1) || !show[group(pol[e])])
            continue;
          ImVec4 col = kCol[pol[e]];
          col.w = hl ? 1.0f : kRungA;
          dl->AddLine(ImPlot::PlotToPixels(xs[k], y0), ImPlot::PlotToPixels(xs[k + 1], y1), ImGui::GetColorU32(col), hl ? 2.5f : 1.0f);
        }
      };
      for (size_t j = 0; j < U; ++j)
        if (static_cast<int>(j) != hot)
          draw(j, false);
      if (hot >= 0) {
        const size_t j = static_cast<size_t>(hot), e = j * K + hk;
        draw(j, true);
        static const char *const kType[] = {"减仓线", "跟随买入线", "跟随卖出线"};
        tip("第 %zu 份 (自上而下, 0 起): %s %.4g\n其预期卖出线 = 中线 − %zu × band", j, kType[pol[e]], p[e], j);
      }
      ImPlot::PopPlotClipRect();
    }
    ImPlot::SetNextLineStyle(kMidCol, 1.2f);
    ImPlot::PlotLine("中线", xs.data(), mid.data(), static_cast<int>(xs.size()));
    ImPlot::SetNextLineStyle(ImGui::GetStyleColorVec4(ImGuiCol_Text), 1.2f);
    ImPlot::PlotLine("收盘", xs.data(), close.data(), static_cast<int>(xs.size()));
    if (const ImPlotItem *it = ImPlot::GetItem("收盘"); !it || it->Show) { // 仓位着色: 有仓的段 (点 k → k + 1, 平均仓位) 盖在收盘线上, 由文字色向 kUp / kDn 按 |仓位| 插值
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      ImPlot::PushPlotClipRect();
      const ImVec4 base = ImGui::GetStyleColorVec4(ImGuiCol_Text);
      for (size_t k = 0; k + 1 < at.size(); ++k) {
        if (std::fabs(wm[k]) <= 1e-3)
          continue;
        const ImVec4 to = wm[k] > 0 ? kUp : kDn;
        const float f = static_cast<float>(std::min(1.0, std::fabs(wm[k])));
        const ImVec4 col(base.x + (to.x - base.x) * f, base.y + (to.y - base.y) * f, base.z + (to.z - base.z) * f, 1.0f);
        dl->AddLine(ImPlot::PlotToPixels(xs[k], close[k]), ImPlot::PlotToPixels(xs[k + 1], close[k + 1]), ImGui::GetColorU32(col), 1.2f);
      }
      ImPlot::PopPlotClipRect();
    }
    { // 买 ▲ (收盘下方) / 卖 ▼ (上方), 边长 ∝ |Δw| (截到 1), 细描边
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      ImPlot::PushPlotClipRect();
      const float sc = ImGui::GetFontSize() / 13.0f;
      for (const size_t i : marks) {
        const float d = dw(i);
        const ImVec2 p = ImPlot::PlotToPixels(static_cast<double>(i), se.c[i]);
        const float s = (5.0f + 14.0f * std::min(1.0f, std::fabs(d))) * sc / 2, y = d > 0 ? p.y + s * 1.2f : p.y - s * 1.2f, t = d > 0 ? -s : s;
        const ImVec2 a(p.x, y + t), b(p.x - s, y - t), e(p.x + s, y - t);
        dl->AddTriangleFilled(a, b, e, ImGui::GetColorU32(d > 0 ? kUp : kDn));
        dl->AddTriangle(a, b, e, IM_COL32(0, 0, 0, 160), 1.0f);
      }
      ImPlot::PopPlotClipRect();
    }
    y_ticks_inside();
    track(0);
    {
      const size_t i = static_cast<size_t>(std::clamp(std::lround(pin), 0L, static_cast<long>(n - 1)));
      ImPlot::Annotation(static_cast<double>(i), se.c[i], kPinCol, ImVec2(5, -15), true, "%s", when(r, i).c_str());
    }
    c.boxing = ImPlot::IsPlotSelected();
    ImPlot::EndPlot();
  }
  ImVec2 corner(-1, -1); // 净值图右上角 (轴切换按钮)
  if (ImPlot::BeginPlot("##cta_nav", ImVec2(size.x, ph - h1 - h3), ImPlotFlags_NoTitle | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect)) {
    setup_x();
    ImPlot::SetupAxis(ImAxis_Y1, nullptr, ImPlotAxisFlags_AutoFit | ImPlotAxisFlags_RangeFit | ImPlotAxisFlags_NoTickLabels);
    ImPlot::SetupAxisScale(ImAxis_Y1, c.nav_log ? ImPlotScale_Log10 : ImPlotScale_Linear);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_Horizontal);
    // 成本带: 逐项叠 (带下沿 = 叠回前几项, 上沿 = 再叠回本项), 带厚 = 该项把净值压下去的量; 悬停的带高亮 + 说明
    //   基线 = 策略净值; 图例隐去策略净值时 = 1 (各沿除以策略净值, 只看成本本身); 各沿在对齐根 = 1 (自对齐根起不扣前几项的净值)
    const cta::Book &sb = tr.strat;
    const bool hovered = ImPlot::IsPlotHovered(); // 锁定 setup 之后 item 才能按 label 查
    const ImPlotItem *nav_it = ImPlot::GetItem("策略净值");
    const bool on_nav = !nav_it || nav_it->Show;
    const auto lift = [&](size_t j, size_t i) { return sb.nav[j][i] / sb.nav[j][anc]; };
    const auto edge = [&](size_t j, size_t i) { // 第 j 层的下沿 (j = 0 = 基线; j = kNC = 毛)
      return on_nav ? lift(j, i) : lift(j, i) / lift(0, i);
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
    y_ticks_inside();
    track(1);
    box();
    if (band >= 0) {
      const size_t j = static_cast<size_t>(band);
      const double y0 = edge(j, bi), y1 = edge(j + 1, bi);
      tip("%s\n截至 %s: 不扣这项, 净值高 %.2f%% (%.4f → %.4f)\n全期年均 %.2f%% (占净值)", cta::kCosts[j].name, when(r, bi).c_str(), (y1 / y0 - 1) * 100, y0, y1,
          sb.st.costs[j] * 100);
    }
    corner = ImVec2(ImPlot::GetPlotPos().x + ImPlot::GetPlotSize().x, ImPlot::GetPlotPos().y);
    ImPlot::EndPlot();
  }
  if (ImPlot::BeginPlot("##cta_dd", ImVec2(size.x, h3), ImPlotFlags_NoTitle | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect)) {
    setup_x();
    ImPlot::SetupAxis(ImAxis_Y1, nullptr, ImPlotAxisFlags_AutoFit | ImPlotAxisFlags_RangeFit | ImPlotAxisFlags_NoTickLabels);
    ImPlot::SetupAxisFormat(ImAxis_Y1, "%g%%");
    ImPlot::SetupLegend(ImPlotLocation_SouthWest, ImPlotLegendFlags_Horizontal);
    const int m = static_cast<int>(xs.size());
    ImPlot::SetNextLineStyle(kHoldCol, 1.0f);
    ImPlot::PlotLine("持有回撤", xs.data(), dd_hold.data(), m);
    ImPlot::SetNextFillStyle(ImGui::GetStyleColorVec4(ImGuiCol_Text), 0.15f);
    ImPlot::PlotShaded("策略回撤", xs.data(), dd.data(), m, 0.0);
    ImPlot::SetNextLineStyle(ImGui::GetStyleColorVec4(ImGuiCol_Text), 1.2f);
    ImPlot::PlotLine("策略回撤", xs.data(), dd.data(), m);
    x_ticks_inside();
    y_ticks_inside();
    track(2);
    box();
    ImPlot::EndPlot();
  }
  double zlo = 0, zhi = 0;
  const bool zoomed = c.zoom.end(zx, zlo, zhi) && zhi > zlo; // 框选落定 (x 视野在 EndPlot 回写链接之后改)
  if (corner.x >= 0) {                                       // 净值图 y 轴切换: 叠在右上角, 画完光标回图下
    static const char *const kScale[] = {"log", "绝对"};
    float bw = 2.0f;
    for (const char *s : kScale)
      bw += ImGui::CalcTextSize(s).x + 2 * st.FramePadding.x;
    const ImVec2 next = ImGui::GetCursorScreenPos();
    ImGui::SetCursorScreenPos(ImVec2(corner.x - bw - 4, corner.y + 4));
    int cur = c.nav_log ? 0 : 1;
    if (button_bar(kScale, 2, cur))
      c.nav_log = cur == 0;
    ImGui::SetCursorScreenPos(next);
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
  range(c, r, xd, ImVec2(size.x, kRangeH));
}

// ---- 中栏: 状态 + 设定 (改了下帧即重发) + 光标处所用参数 + 视野内指标 ----
// 指标表 [行]: 名 / 字段 / 格式 / 倍数 / 优向 (+1 = 越高越好, −1 = 越低越好, 0 = 不论优劣) / 说明; 年成本行可展开各分项 (cta::kCosts, 默认折叠, 同格式同优向)
struct StatRow {
  const char *name;
  double cta::Stat::*f;
  const char *fmt;
  double mul;
  int better;
  const char *tip;
};
const StatRow kStatRows[] = {
    {"年换手", &cta::Stat::turn, "%.1f", 1, 0, "单边: Σ |Δ仓位| / 年数"},
    {"平均仓位", &cta::Stat::expo, "%.2f", 1, 0, "逐根 |仓位| 的均值 (满多 / 满空 = 1)"},
    {"年化波动", &cta::Stat::vol, "%.1f%%", 100, -1, "逐根净收益标准差 × √每年根数"},
    {"夏普", &cta::Stat::sharpe, "%.2f", 1, 1, "逐根净收益 均值 / 标准差 × √每年根数"},
    {"索提诺", &cta::Stat::sortino, "%.2f", 1, 1, "逐根净收益 均值 / 下行波动 √E[min(r, 0)²] × √每年根数"},
    {"年化", &cta::Stat::cagr, "%+.1f%%", 100, 1, "视野内净值按年数复利折算"},
    {"最大回撤", &cta::Stat::mdd, "%.1f%%", 100, 1, "净值自前高的最大回落"},
    {"年成本", &cta::Stat::cost, "%.2f%%", 100, -1, "佣金 + 印花税 + 冲击 + 震荡 + 跳空, 每年占净值; 展开看各分项"},
};

// 单元格内右对齐, 以 col 色显
void right(const char *s, const ImVec4 &col) {
  ImGui::SetCursorPosX(ImGui::GetCursorPosX() + std::max(0.0f, ImGui::GetContentRegionAvail().x - ImGui::CalcTextSize(s).x));
  ImGui::TextColored(col, "%s", s);
}

// 名 | 持有 (灰, 作对照) | 策略 (比持有好 → kUp / 差 → kDn, 不论优劣的行 = 文字色); 数值右对齐
//   指标 = 视野 [x0, x1] 内的根段 (同左侧三图 / 区间条), 每年根数沿用全期的 (根数 / 年数); 段变了才重算 (CtaState::seg 缓存)
void stat_table(CtaState &c, const CtaResult &r) {
  const size_t n = r.se->c.size();
  const size_t i0 = static_cast<size_t>(std::clamp(std::ceil(c.x0), 0.0, static_cast<double>(n - 1)));
  const size_t i1 = static_cast<size_t>(std::clamp(std::floor(c.x1), static_cast<double>(i0), static_cast<double>(n - 1))) + 1;
  if (c.seg.of != &r || c.seg.gen != r.gen || c.seg.i0 != i0 || c.seg.i1 != i1) {
    const double bpy = static_cast<double>(n) / r.tr->strat.st.years;
    c.seg = {&r, r.gen, i0, i1, cta::stat_of(r.tr->hold.run, i0, i1, bpy), cta::stat_of(r.tr->strat.run, i0, i1, bpy)};
  }
  constexpr int nc = 2; // 数值列: 持有 / 策略
  if (!ImGui::BeginTable("##cta_stat", 1 + nc, ImGuiTableFlags_RowBg | ImGuiTableFlags_SizingStretchProp))
    return;
  ImGui::TableSetupColumn("##name", ImGuiTableColumnFlags_WidthStretch, 1.4f);
  for (int k = 0; k < nc; ++k)
    ImGui::TableSetupColumn("##v", ImGuiTableColumnFlags_WidthStretch, 1.0f);
  const ImVec4 dim = ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled), text = ImGui::GetStyleColorVec4(ImGuiCol_Text);
  ImGui::TableNextRow();
  ImGui::TableNextColumn();
  static const char *const kHead[] = {"持有", "策略"};
  for (int k = 0; k < nc; ++k) {
    ImGui::TableNextColumn();
    right(kHead[k], dim);
  }
  // tree = 名作可展开的节点 (默认折叠), 返回是否展开
  const auto row = [&](const char *name, const std::string &note, const StatRow &x, auto get, bool tree = false) {
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    bool open = false;
    if (tree)
      open = ImGui::TreeNodeEx(name, ImGuiTreeNodeFlags_SpanAvailWidth);
    else
      ImGui::TextUnformatted(name);
    if (ImGui::IsItemHovered())
      tip("%s = %s", name, note.c_str());
    const double h = get(c.seg.hold), vs[] = {get(c.seg.strat)};
    for (int k = 0; k < nc; ++k) {
      ImGui::TableNextColumn();
      const double u = k ? vs[k - 1] : h;
      char t[32] = "-";
      if (std::isfinite(u))
        std::snprintf(t, sizeof(t), x.fmt, u * x.mul);
      right(t, !k ? dim : x.better && std::isfinite(h) && std::isfinite(u) ? tint(x.better * u, x.better * h)
                                                                           : text);
    }
    return open;
  };
  for (const StatRow &x : kStatRows) {
    const bool cost = x.f == &cta::Stat::cost;
    if (!row(x.name, x.tip, x, [&](const cta::Stat &s) { return s.*x.f; }, cost))
      continue;
    for (size_t j = 0; j < cta::kNC; ++j)
      row(cta::kCosts[j].name, std::string(cta::kCosts[j].tip) + "; Σ / 年数", x, [&](const cta::Stat &s) { return s.costs[j]; });
    ImGui::TreePop();
  }
  ImGui::EndTable();
}

// 减仓 / 跟随 (下标 0 / 1)
const cta::Path &path_of(const CtaResult &r, size_t k) { return k ? r.tr->follow : r.tr->reduce; }
constexpr const char *kPathName[] = {"减仓", "跟随"};

// 光标处 (−1 = 末根) 所在日 → v[k] = 当日所用参数 (阻尼后, 轴的单位; 减仓未选参 = 买入持有, 两者都 nullptr; 跟随未选参 = 沿用的上一个选参日, 尚无 = nullptr); 返回说明
std::string cursor_at(const CtaResult &r, int cursor, const cta::P *(&v)[2]) {
  const size_t n = r.se->c.size(), i = cursor >= 0 ? static_cast<size_t>(cursor) : n - 1;
  const size_t d = dix(r, i);
  const std::string head = cursor >= 0 ? "光标 " + when(r, i) : "末根";
  v[0] = v[1] = nullptr;
  if (!r.tr->reduce.sel[d])
    return head + ": 买入持有 (首日 / 窗内数据不足 / 减仓线未就绪, 未选参)";
  v[0] = &r.tr->reduce.use[d];
  const cta::Path &f = r.tr->follow;
  for (size_t e = d + 1; e-- > 0 && !v[1];)
    if (f.sel[e])
      v[1] = &f.use[e];
  return head + ": 当日所用 (阻尼后)" + (f.sel[d] ? "" : v[1] ? "; 跟随窗内无笔, 沿用上一个选参日的"
                                                              : "; 跟随尚未选参, 用初始中心");
}

// 模型下拉: 选项 xs (各有 name / tip) 的下标 cur; 悬停 = head + 当前模型的说明, 下拉里各项悬停 = 该项的说明
template <class Xs>
void model_combo(const char *id, const Xs &xs, size_t &cur, const std::string &head) {
  if (ImGui::BeginCombo(id, xs[cur].name)) {
    for (size_t k = 0; k < std::size(xs); ++k) {
      if (ImGui::Selectable(xs[k].name, k == cur))
        cur = k;
      if (ImGui::IsItemHovered())
        tip("%s = %s", xs[k].name, xs[k].tip);
    }
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    tip("%s\n• %s = %s", head.c_str(), xs[cur].name, xs[cur].tip);
}

// 模型 k 在网格配置数 n 下的参数网格: 维数 + 各轴格点数 (之积 = 每日评的组合数)
std::string grid_note(const cta::Kind &k, int n) {
  if (!k.na)
    return "无参数, 不调参";
  const cta::At m = cta::split(k, static_cast<size_t>(n));
  std::string names, counts;
  size_t prod = 1;
  for (size_t a = 0; a < k.na; ++a)
    names += (a ? " × " : "") + std::string(k.ax[a].name), counts += (a ? " × " : "") + std::to_string(m[a]), prod *= m[a];
  return "参数网格 " + std::to_string(k.na) + " 维: " + names + " = " + counts + (k.na > 1 ? " = " + std::to_string(prod) : "") + " 个组合";
}

// ---- 参数轨迹: 一张图, 各参数逐日 ÷ 各自首个有值日的值 (都从 1 起, log 轴; 只看相对变化): 实线 = 阻尼后所用, 淡线 = 曲面最高点 (跳 = 岭回归选参不稳);
//      band 行 = band ÷ 首个有值日的 band; 图例矩阵 (同 NAV): 行 = band / 减仓 / 跟随, 列 = 各自的参数, 格 = 该参数的线 (色块 + 光标处的值; 默认全关);
//      x 链到左侧三图 (同一视野), 未选参日断开; 窄边框, 刻度画在图内; 光标 = 橙色竖线; 悬停看当日值 ----
void path(CtaState &c, const CtaResult &r) {
  const cta::Track &tr = *r.tr;
  const cta::Series &se = *r.se;
  const size_t n = se.c.size();
  std::vector<double> xs; // 每个有根的日两点 (首根, 末根)
  std::vector<size_t> ds; // [日两点 / 2] 日下标
  for (size_t d = 0; d < tr.days.size(); ++d)
    if (tr.days.off[d] < tr.days.off[d + 1])
      xs.push_back(static_cast<double>(tr.days.off[d])), xs.push_back(static_cast<double>(tr.days.off[d + 1] - 1)), ds.push_back(d);
  const size_t dc = dix(r, c.cursor >= 0 ? static_cast<size_t>(c.cursor) : n - 1); // 光标所在日 (未放 = 末根)
  const auto close_of = [&](size_t d) { return se.c[tr.days.off[d + 1] - 1]; };    // 日 d 的末根收盘 (d 须有根)
  // 一条线 = 一个模型的一个参数: item 名 / 轴名 / 说明 / [日两点] 所用 / 最高点 (空 = 无) ÷ 首个有值日的值 / 光标日的值
  struct Line {
    std::string id;
    const char *axis, *tip;
    std::vector<double> use, raw;
    std::string now;
  };
  constexpr size_t kRows = 3; // band / 减仓 / 跟随
  std::vector<Line> ls[kRows];
  std::string names[kRows];
  names[0] = std::string("band ") + cta::Band::kinds[r.spec.band.kind].name;
  {
    Line l{"band###cta_path_band", "band", "band ÷ 首个有值日的 band (档线间距的相对变化; 人选, 不调)", std::vector<double>(xs.size(), cta::kNaN), {}, "-"};
    double v0 = cta::kNaN;
    for (size_t j = 0; j < ds.size(); ++j)
      if (const double b = tr.band[ds[j]]; std::isfinite(b)) {
        v0 = std::isnan(v0) ? b : v0;
        l.use[2 * j] = l.use[2 * j + 1] = b / v0;
      }
    if (std::isfinite(tr.band[dc]) && tr.days.off[dc] < tr.days.off[dc + 1]) {
      char b[32];
      std::snprintf(b, sizeof(b), "%.2f%%", tr.band[dc] / close_of(dc) * 100);
      l.now = b;
    }
    ls[0].push_back(std::move(l));
  }
  for (size_t k = 0; k < 2; ++k) {
    const cta::Path &p = path_of(r, k);
    const cta::Kind &kd = *p.grid.kind;
    names[1 + k] = std::string(kPathName[k]) + " " + kd.name;
    const size_t first = static_cast<size_t>(std::ranges::find(p.sel, 1) - p.sel.begin());
    for (size_t a = 0; a < kd.na; ++a) {
      const cta::Axis &x = kd.ax[a];
      Line l{std::string(kPathName[k]) + " " + x.name + "###cta_path_" + std::to_string(k) + "_" + std::to_string(a), x.name, x.tip,
             std::vector<double>(xs.size(), cta::kNaN), std::vector<double>(xs.size(), cta::kNaN), "-"};
      if (first < p.sel.size())
        for (size_t j = 0; j < ds.size(); ++j)
          if (const size_t d = ds[j]; p.sel[d])
            l.use[2 * j] = l.use[2 * j + 1] = p.use[d][a] / p.use[first][a], l.raw[2 * j] = l.raw[2 * j + 1] = p.raw[d][a] / p.use[first][a];
      if (p.sel[dc]) {
        char b[32];
        std::snprintf(b, sizeof(b), "%.3g%s", p.use[dc][a], x.day ? "日" : "");
        l.now = b;
      }
      ls[1 + k].push_back(std::move(l));
    }
  }
  if (std::ranges::find(tr.reduce.sel, 1) == tr.reduce.sel.end()) {
    ImGui::TextDisabled("全期未选参 (买入持有)");
    return;
  }
  XFmt fmt{&r, c.x1 - c.x0 < 3.0 * kS};
  const XDays xd = x_days(r);
  DayAxis ax{xd.dates, 1.0, 0.0, xd.xs, x_formatter, &fmt};
  const float w = ImGui::GetContentRegionAvail().x;
  ImPlot::PushStyleVar(ImPlotStyleVar_PlotPadding, ImVec2(2, 1));
  if (ImPlot::BeginPlot("##cta_path", ImVec2(w, std::floor(w * 0.6f)), ImPlotFlags_NoTitle | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect | ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxis(ImAxis_X1, nullptr, ImPlotAxisFlags_NoTickLabels);
    ImPlot::SetupAxisLinks(ImAxis_X1, &c.x0, &c.x1);
    setup_day_axis(ax);
    ImPlot::SetupAxis(ImAxis_Y1, nullptr, ImPlotAxisFlags_AutoFit | ImPlotAxisFlags_RangeFit | ImPlotAxisFlags_NoTickLabels);
    ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    const auto color = [&](size_t row, size_t a) { return ImPlot::GetColormapColor(static_cast<int>(row * cta::kAxes + a)); };
    for (size_t row = 0; row < kRows; ++row)
      for (size_t a = 0; a < ls[row].size(); ++a) {
        const Line &l = ls[row][a];
        const ImVec4 col = color(row, a);
        ImPlot::HideNextItem(true, ImPlotCond_Once);
        ImPlot::SetNextLineStyle(col, 1.5f);
        ImPlot::PlotLine(l.id.c_str(), xs.data(), l.use.data(), static_cast<int>(xs.size()));
        if (const ImPlotItem *it = ImPlot::GetItem(l.id.c_str()); !l.raw.empty() && it && it->Show) {
          ImPlot::SetNextLineStyle(ImVec4(col.x, col.y, col.z, 0.35f), 1.0f);
          ImPlot::PlotLine(("##raw" + l.id).c_str(), xs.data(), l.raw.data(), static_cast<int>(xs.size()), ImPlotItemFlags_NoLegend);
        }
      }
    if (c.cursor >= 0) {
      const double x = c.cursor;
      ImPlot::SetNextLineStyle(kPinCol, 2.0f);
      ImPlot::PlotInfLines("##cta_path_pin", &x, 1);
    }
    x_ticks_inside();
    y_ticks_inside();
    // 图例矩阵: 行 = 模型, 列 = 参数 (格 = 色块 + 轴名 光标处的值)
    const LgCol cols[] = {{"参数 1", 0}, {"参数 2", 1}};
    static_assert(cta::kAxes == 2, "图例列 = 参数轴数上限");
    const ImU32 c_txt = ImGui::GetColorU32(ImGuiCol_Text);
    std::vector<LgRow> lr;
    std::vector<LgCell> cells;
    for (size_t row = 0; row < kRows; ++row) {
      lr.push_back({names[row].c_str(), c_txt});
      for (size_t a = 0; a < cta::kAxes; ++a)
        if (a < ls[row].size())
          cells.push_back({std::string(ls[row][a].axis) + " " + ls[row][a].now, 0, {color(row, a), color(row, a)}});
        else
          cells.push_back({});
    }
    const ImVec2 o = ImPlot::GetPlotPos();
    const LgHover h = legend("##cta_path_lg", ImVec2(o.x + 4.f, o.y + 4.f), cols, lr, cells, [&](int row, int t) -> ImPlotItem * {
      const std::vector<Line> &v = ls[static_cast<size_t>(row)];
      return static_cast<size_t>(t) < v.size() ? ImPlot::GetItem(v[static_cast<size_t>(t)].id.c_str()) : nullptr;
    });
    if (h.on && h.r >= 0 && h.t >= 0 && static_cast<size_t>(h.t) < ls[static_cast<size_t>(h.r)].size()) {
      const Line &l = ls[static_cast<size_t>(h.r)][static_cast<size_t>(h.t)];
      tip("%s = %s; 点 = 开关", l.axis, l.tip);
    } else if (ImPlot::IsPlotHovered() && !h.box.Contains(ImGui::GetMousePos())) {
      const size_t i = static_cast<size_t>(std::clamp(std::lround(ImPlot::GetPlotMousePos().x), 0L, static_cast<long>(n - 1))), d = dix(r, i);
      std::string s = misc::dash(day_of(r, i));
      if (std::isfinite(tr.band[d])) {
        char b[64];
        std::snprintf(b, sizeof(b), "\nband = %.4g (收盘的 %.2f%%)", tr.band[d], tr.band[d] / se.c[i] * 100);
        s += b;
      }
      for (size_t k = 0; k < 2; ++k) {
        const cta::Path &p = path_of(r, k);
        const cta::Kind &kd = *p.grid.kind;
        if (!p.sel[d]) {
          s += std::string("\n") + kPathName[k] + " 未选参" + (k ? "" : " (买入持有)");
          continue;
        }
        for (size_t a = 0; a < kd.na; ++a) {
          char b[96];
          std::snprintf(b, sizeof(b), "\n%s %s: 所用 %.3g / 最高点 %.3g%s", kPathName[k], kd.ax[a].name, p.use[d][a], p.raw[d][a], kd.ax[a].day ? " 日" : "");
          s += b;
        }
      }
      tip("%s", s.c_str());
    }
    ImPlot::EndPlot();
  }
  ImPlot::PopStyleVar();
}

void panel(const CtaService &svc, const Progress &lib, CtaState &c, const Fee &rates, const CtaResult *r, const std::string &lack) {
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
    tip("首次合成全部 1 分钟价 (all.bar 库 cache/price/), 之后只补新日");

  // 设定: 分组 (band / 减仓 / 跟随 / 分层 / 引擎), 一行两项 (名 控件 | 名 控件, 各占半行); 改了即全部重算
  cta::Spec &sp = c.spec;
  const float x0 = ImGui::GetCursorPosX();
  const auto pair = [&](const char *a, auto fa, const char *b, auto fb) {
    const float g = ImGui::GetStyle().ItemSpacing.x, half = (ImGui::GetContentRegionAvail().x - g) / 2;
    ImGui::AlignTextToFramePadding();
    ImGui::TextUnformatted(a);
    ImGui::SameLine();
    ImGui::SetNextItemWidth(half - ImGui::CalcTextSize(a).x - g);
    fa();
    ImGui::SameLine(x0 + half + g);
    ImGui::TextUnformatted(b);
    ImGui::SameLine();
    ImGui::SetNextItemWidth(-FLT_MIN);
    fb();
  };
  const auto hint = [](const char *s) {
    ImGui::PushStyleColor(ImGuiCol_Text, ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
    ImGui::TextWrapped("%s", s);
    ImGui::PopStyleColor();
  };
  c.editing = false;
  const auto years = [&](const char *id, int &y, const char *what) { // 窗 (整年): 填数 + −/+ 按钮
    ImGui::InputInt(id, &y, 1, 1);
    y = std::clamp(y, cta::kMinYears, cta::kMaxYears);
    c.editing |= ImGui::IsItemActive();
    if (ImGui::IsItemHovered())
      tip("%s (%d ~ %d 年)", what, cta::kMinYears, cta::kMaxYears);
  };
  const cta::P *at[2] = {};
  const std::string where = r ? cursor_at(*r, c.cursor, at) : std::string{};
  const auto head = [&](const char *name, const cta::Kind &k) { // 节标题 = 名 (参数维度); 悬停 = 参数网格的分法
    std::string t = std::string(name) + " (";
    for (size_t a = 0; a < k.na; ++a)
      t += (a ? ", " : "") + std::string(k.ax[a].name);
    t += k.na ? ")" : "无参数)";
    ImGui::SeparatorText(t.c_str());
    if (ImGui::IsItemHovered())
      tip("%s", grid_note(k, sp.rule.units).c_str());
  };
  const auto axes = [&](const cta::Kind &k, const cta::P *v) { // 各轴一项 名: 最小|光标处所用|最大 (v = nullptr: 所用记 -); 各项单独悬停 = 该轴的说明
    for (size_t a = 0; a < k.na; ++a) {
      const cta::Axis &ax = k.ax[a];
      char y[16] = "-";
      if (v)
        std::snprintf(y, sizeof(y), "%.3g", (*v)[a]);
      if (a)
        ImGui::SameLine();
      ImGui::TextDisabled("%s: %.3g|%s|%.3g", ax.name, ax.lo, y, ax.hi);
      if (ImGui::IsItemHovered())
        tip("%s = %s%s (最小 | 光标处所用 | 最大)", ax.name, ax.tip, ax.day ? ", 以日计" : "");
    }
  };

  const auto pick = [&](const char *id, std::span<const cta::Kind> kinds, cta::Pick &pk, const char *model, const char *win) { // 模型 | 窗 (年) 一行
    ImGui::PushID(id);
    pair("模型", [&] { model_combo("##model", kinds, pk.kind, model); }, "窗 (年)", [&] { years("##win", pk.win, win); });
    ImGui::PopID();
  };
  const auto tuned = [&](const char *name, std::span<const cta::Kind> kinds, cta::Pick &pk, size_t k, const char *model, const char *win) { // 节标题 + 各轴 + 模型 | 窗
    head(name, kinds[pk.kind]);
    axes(kinds[pk.kind], r && (k ? r->spec.follow : r->spec.reduce).kind == pk.kind ? at[k] : nullptr);
    pick(name, kinds, pk, model, win);
  };

  // 机制 (悬停只讲模型含义, 细节见 cta/Model.hpp · Tune.hpp · Trace.hpp):
  //   band = 档线间距, 日频, 日内线性过渡到前一交易日收盘时的值; 止损 / 跟随线漂移都以它计
  //   减仓 = 快线 (固定 5 日, 不调) 往逆向挪一个 band 为中线, 第 j 份的预期卖出线 = 中线 − j × band; 选参窗只管就绪 (窗内根数不足一半 = 买入持有)
  //   跟随 = 跟随买入 / 卖出线朝价漂的模型 (两向镜像, 各份成交时定参); 每日以窗内卖出各笔的中位夏普拟岭回归曲面选参, 5 日阻尼
  //   分层: 每份恰一条线 (减仓线 / 跟随买入线 / 跟随卖出线); 首个止损起其余减仓线锁定, 最下面的跟随卖出线带着下面锁定的线上移 / 横着;
  //     跟随卖出线挂出时预期在上 = 上漂去追, 否则横着等, 追到 / 被预期上穿 = 回到随预期; 全部收复 = 锁定的回到随预期 (差额一日内消化)
  //   份数 = 满仓份数, 也是每日评的参数组合数上限; 止损距 = 成交一份后新线离成交线几个 band
  ImGui::SeparatorText("band");
  pick("band", cta::Band::kinds, sp.band, "band = 档线间距 (价格单位), 人选, 不调; 宽度随估值缩放", "band 的窗");
  const auto wide = [&](const char *id, float &x, const char *what) {
    ImGui::InputFloat(id, &x, 0.1f, 0.5f, "%.2f");
    x = std::clamp(x, 0.1f, 10.0f);
    c.editing |= ImGui::IsItemActive();
    if (ImGui::IsItemHovered())
      tip("%s; 之间按价线性 (估值线 = PB Band 首末两档, 窗 = Band 滚动窗)", what);
  };
  pair("高估宽", [&] { wide("##cta_wide_hi", sp.wide_hi, "价在估值贵线时 band × 此数; 默认 1"); }, "低估宽", [&] { wide("##cta_wide_lo", sp.wide_lo, "价在估值便宜线时 band × 此数 (宽 = 不易触发); 默认 2"); });
  tuned("减仓", cta::Reduce::kinds, sp.reduce, 0, "减仓模型: 快线 (5 日) 往下挪一个 band 为中线, 第 j 份的预期卖出线 = 中线 − j × band", "减仓的窗: 窗内数据不足 = 买入持有");
  tuned("跟随", cta::Follow::kinds, sp.follow, 1, "跟随模型: 卖出后的跟随买入线往下漂 (抄底), 买回后的跟随卖出线往上漂 (移动止损)",
        "跟随选参的滚动窗");
  if (r)
    hint(where.c_str());
  if (r) { // 参数轨迹 (默认折叠)
    const bool open = ImGui::CollapsingHeader("参数轨迹");
    if (ImGui::IsItemHovered())
      tip("各参数逐日 ÷ 首个有值日的值; 实线 = 所用, 淡线 = 曲面最高点");
    if (open)
      path(c, *r);
  }

  ImGui::SeparatorText("分层");
  if (ImGui::IsItemHovered())
    tip("每份一条线: 减仓线 → 跌到即卖成跟随买入线 → 涨到即买回成跟随卖出线 → 赶上预期回到减仓线");
  pair(
      "份数",
      [&] {
        ImGui::InputInt("##cta_units", &sp.rule.units, 1, 5);
        sp.rule.units = std::clamp(sp.rule.units, 1, 100);
        c.editing |= ImGui::IsItemActive();
        if (ImGui::IsItemHovered())
          tip("满仓分几份 (= 档线条数); 默认 %d", cta::kUnits);
      },
      "止损", [&] {
        ImGui::InputFloat("##cta_stop", &sp.rule.stop, 0.1f, 0.5f, "%.2f");
        sp.rule.stop = std::clamp(sp.rule.stop, 0.0f, 10.0f);
        c.editing |= ImGui::IsItemActive();
        if (ImGui::IsItemHovered())
          tip("成交一份后, 新挂的跟随线离成交线几个 band; 默认 %.2g", cta::kStop); });

  ImGui::SeparatorText("引擎");
  {
    constexpr int nd = static_cast<int>(std::size(cta::kDirs));
    const char *names[nd];
    std::string s = "方向: 顺向 = 所押的一侧";
    for (int k = 0; k < nd; ++k)
      names[k] = cta::kDirs[k].name, s += std::string("\n• ") + cta::kDirs[k].name + " = " + cta::kDirs[k].tip;
    ImGui::AlignTextToFramePadding();
    ImGui::TextUnformatted("方向");
    ImGui::SameLine();
    ImGui::BeginGroup();
    button_bar(names, nd, sp.dir);
    ImGui::EndGroup();
    if (ImGui::IsItemHovered())
      tip("%s", s.c_str());
  }
  ImGui::AlignTextToFramePadding();
  ImGui::TextUnformatted("费率 (bp)");
  ImGui::SameLine();
  ImGui::TextDisabled("佣金 %.3g / 过户费 %.3g / 印花税 %.3g / 冲击 %.3g", rates.commission * 1e4, rates.transfer * 1e4, rates.stamp * 1e4, rates.impact * 1e4);
  if (ImGui::IsItemHovered())
    tip("系统设置 (Settings) 的税佣与固定冲击");
  if (r) {
    ImGui::SeparatorText("视野内指标");
    if (ImGui::IsItemHovered())
      tip("随左侧视野重算; 双击图回全段 = 全期");
    stat_table(c, *r);
  }
}

// 估值线 [日脊] (band 宽度随估值): BP 收益率 ÷ 窗内最贵 / 最便宜档的分位收益率 = 贵线 / 便宜线对当日收盘的倍数 (同价格图 PB Band 的首末两档; 无 = NaN);
//   键 (日脊 / 派生代 / 滚动分位代) 变了才重建, 请求里的指针不变 = 不重发
void gate_of(CtaState &c, const PbBand &pb, const std::shared_ptr<const std::vector<misc::Ymd>> &spine, uint64_t der, uint64_t roll) {
  if (c.gate.up && c.gate.spine == spine.get() && c.gate.der == der && c.gate.roll == roll)
    return;
  const size_t n = spine->size();
  auto up = std::make_shared<std::vector<float>>(n, cta::kNaN), dn = std::make_shared<std::vector<float>>(n, cta::kNaN);
  for (size_t k = 0; pb.v && k < n; ++k) {
    const auto it = std::ranges::lower_bound(pb.dates, (*spine)[k]);
    if (it == pb.dates.end() || *it != (*spine)[k])
      continue;
    const size_t j = static_cast<size_t>(it - pb.dates.begin());
    const double v = pb.at[j] >= 0 ? pb.v[pb.at[j]] : kNaN;
    if (!(v > 0.0) || !pb.roll.band[j])
      continue;
    if (const float q = pb.roll.q[kBandQ - 1][j]; q > 0.f)
      (*up)[k] = static_cast<float>(v / q);
    if (const float q = pb.roll.q[0][j]; q > 0.f)
      (*dn)[k] = static_cast<float>(v / q);
  }
  c.gate = {spine.get(), der, roll, std::move(up), std::move(dn)};
}

} // namespace

void RenderLinesCta(CtaService &svc, const Quote::QuoteHub &mk, LinesState &ui, const Fee &rates, const char *name, const ImVec2 &plots_size,
                    float panel_w) {
  CtaState &c = ui.cta;
  const std::map<std::string, Quote::Ref> &one = ui.pick.rows[kPickOne];
  const Quote::Ref q = one.empty() ? Quote::Ref{} : one.begin()->second;
  const Quote::PriceView *v = mk.view.get();
  const bool ready = v && v->lack.empty();
  const Quote::Series *se = ready && q.ok() ? v->series(q) : nullptr;
  const std::shared_ptr<const Quote::Series> m = se && se->minute ? std::shared_ptr<const Quote::Series>(mk.view, se) : nullptr;
  const bool off = ready && q.ok() && !se && v->covers(q, true); // 个股在库轴外 (篮子恒在)
  const LinesLeg &lg = ui.roll.key.leg;                          // 滚动分位所属的估值腿 (= 本页标的的)
  const PbBand pb{ui.der.val.dates, ui.der.val.val_at, lg.b ? leg_val(lg, kValBp) : nullptr, ui.roll.val[kValBp], ui.roll.key.years};
  if (m)
    gate_of(c, pb, v->dates, ui.der.gen, ui.roll.gen);
  if (m && !c.editing)
    c.sent.send(svc, {.ref = q,
                      .spec = c.spec,
                      .rates = rates,
                      .stamp = v->stamp,
                      .dates = v->dates,
                      .series = m,
                      .up = c.gate.up,
                      .dn = c.gate.dn});
  const std::shared_ptr<const CtaResult> r = latest(svc);
  const bool mine = q.ok() && r && r->ref == q; // 本标的的 (设定改了时新的还在算, 先显旧的)
  const CtaResult *show = mine && r->lack.empty() ? r.get() : nullptr;

  ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
  ImGui::BeginChild("##cta_plots", plots_size);
  ImGui::PopStyleVar();
  if (!q.ok())
    ImGui::TextDisabled("右表单选一个指数 / 行业 / 概念 / 成分股 (与 VAL 同一份选中)");
  else if (show) {
    plots(c, *show, name, pb, ImGui::GetContentRegionAvail());
  } else if (!ready)
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