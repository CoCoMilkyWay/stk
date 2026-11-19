// Market→CTA 页: 左 = 两图 (价格与中线 / 档线, 底色 = 仓位 / 净值, 共一条 x) + 区间条, 其右 = 中栏 (设定 / 参数轨迹 / 光标处参数 / 全期指标; 与右表分右栏) — 见 TabLines.hpp
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
#include <utility>
#include <vector>

namespace GUI::Market {

namespace {

constexpr size_t kS = Quote::kSlots;
constexpr float kPriceFrac = 0.62f;                                                                                  // 上图占两图高的比例
constexpr ImVec4 kMidCol(1.0f, 0.6f, 0.2f, 1.0f), kHoldCol(0.6f, 0.6f, 0.6f, 1.0f), kPinCol(1.0f, 0.5f, 0.0f, 1.0f); // kPinCol = 光标 (同 OrderFlow 锚线)
// 成本带色 (序同 cta::kCosts) + 常态 / 悬停的不透明度
constexpr ImVec4 kCostCol[] = {{0.35f, 0.60f, 0.95f, 1.0f}, {0.70f, 0.50f, 0.95f, 1.0f}, {0.95f, 0.65f, 0.25f, 1.0f}, {0.30f, 0.80f, 0.75f, 1.0f}, {0.95f, 0.40f, 0.55f, 1.0f}};
static_assert(std::size(kCostCol) == cta::kNC, "成本带色");
constexpr float kCostA = 0.22f, kCostHotA = 0.6f;
constexpr float kBandA0 = 0.8f, kBandA1 = 0.06f; // 档线不透明度: 同极性离收盘最近 / 最远那条

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
  const cta::Days &ds = r.tr.days;
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
//      左把手松手 / 双击改了左端 (段内平移不算) → 净值图锚定到 d0 ----
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
  const int was = rg.drag;
  range_bar(rg, xd.dates, 1.0);
  ImPlot::EndPlot();
  ImPlot::PopStyleVar();
  if (rg.drag == 0 && (was == 1 || (was == 0 && rg.d0 != d0)))
    c.anchor = rg.d0;
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

// ---- 两图: 上 = 收盘 (按仓位着色: 多 → kUp / 空 → kDn, 越满越纯) + 中线 + 档线 (每份一条: 减仓线 / 加仓线) + 买 / 卖标记 (逐根时才画)
//        + PB Band (同 VAL: 偏离 v / q_j 乘本图当日末根收盘, 每日一点在该日末根; 填色 + 各档线共一个图例项, 默认关);
//      下 = 策略 / 持有净值 + 成本带 (y 轴 log / 绝对, 右上角切换); 底 = 区间条. x 同一视野 (链到 ui), 每像素取末根; 光标 = 可拖竖线 (两图同一条)
//      左键框选放大 (价格图连 y 一起; 净值图只 x), 右键拖动平移, 滚轮缩放, 双击回全段;
//      光标 = 橙色竖线 (同 OrderFlow 锚线): 参数轨迹展开时才出现 (初放视野正中), 只能拖动, 标注时刻 ----
void plots(CtaState &c, const CtaResult &r, const char *name, const PbBand &pb, const ImVec2 &size) {
  const cta::Series &se = *r.se;
  const cta::Track &tr = r.tr;
  const size_t n = se.c.size();
  if (c.fit != r.ref) { // 换标的: 回全段, 光标回末根, 锚回首日
    c.fit = r.ref;
    c.x0 = -0.5, c.x1 = static_cast<double>(n) - 0.5, c.fit_y = true, c.cursor = -1, c.anchor = 0;
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
  assert(c.anchor >= 0 && static_cast<size_t>(c.anchor) < xd.dates.size());
  const size_t anc = c.anchor ? xd.last[static_cast<size_t>(c.anchor) - 1] : 0; // 锚根: 锚定日前一日末根 (锚 = 首日则首根); 净值 / 成本带在此 = 1
  const cta::Col &w = tr.strat.run.w;
  const std::vector<double> close = take(se.c), mid = take(tr.cut.mid);
  std::vector<double> nav = take(tr.strat.nav), hold = take(tr.hold.nav);
  for (size_t k = 0; k < at.size(); ++k)
    nav[k] /= tr.strat.nav[anc], hold[k] /= tr.hold.nav[anc];
  std::vector<double> wm(at.size(), 0); // wm[k] = 点 k → k + 1 这段所持的平均仓位 (根 at[k] .. at[k + 1] − 1 收盘调到的)
  for (size_t k = 0; k + 1 < at.size(); ++k) {
    for (size_t i = at[k]; i < at[k + 1]; ++i)
      wm[k] += w[i];
    wm[k] /= static_cast<double>(at[k + 1] - at[k]);
  }

  // 买 / 卖: 视野内逐根计次; 标记只在逐根 (step = 1) 时画, 一根一个
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
    if (step == 1)
      marks.push_back(i);
  }

  // 标题: 标的 (高亮) · 模型 · 窗 · 视野 · 粒度
  const std::string model = std::string(cta::kBandKinds[r.spec.band].name) + " / " + cta::kReduceKinds[r.spec.reduce].name + " / " +
                            cta::kAddKinds[r.spec.add].name + " / " + cta::kDirs[r.spec.dir].name;
  char gran[96];
  if (step == 1)
    std::snprintf(gran, sizeof(gran), "逐根, 买 %d 次 / 卖 %d 次", buys, sells);
  else
    std::snprintf(gran, sizeof(gran), "每点 %zu 根 (放大到逐根才标买卖), 买 %d 次 / 卖 %d 次", step, buys, sells);
  ImGui::TextColored(kHead, "%s", name ? name : "");
  ImGui::SameLine();
  ImGui::TextDisabled("%s  ·  窗 %.2g / %.2g / %.2g 年  ·  %s ~ %s  ·  %s", model.c_str(), r.spec.band_win, r.spec.reduce_win, r.spec.add_win,
                      misc::dash(day_of(r, lo)).c_str(), misc::dash(day_of(r, hi)).c_str(), gran);

  const ImGuiStyle &st = ImGui::GetStyle();
  const float ph = ImGui::GetContentRegionAvail().y - 2 * st.ItemSpacing.y - kRangeH, h1 = std::floor(ph * kPriceFrac);
  XFmt fmt{&r, c.x1 - c.x0 < 3.0 * kS};
  DayAxis ax{xd.dates, 1.0, 0.0, xd.xs, x_formatter, &fmt};
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
  bool pinned = c.path_open && c.cursor < 0, reset = false;
  double pin = c.cursor >= 0 ? c.cursor : std::clamp((c.x0 + c.x1) / 2, 0.0, static_cast<double>(n - 1));
  const auto setup_x = [&](ImPlotAxisFlags f) {
    ImPlot::SetupAxis(ImAxis_X1, nullptr, f);
    ImPlot::SetupAxisLinks(ImAxis_X1, &c.x0, &c.x1);
    setup_day_axis(ax);
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
    { // 档线 (Cuts::rungs_at): 一份一条连续线 (恒为份数条), 段色按段末的极性: 减仓线绿 / 加仓线红; 每点在同色里按离收盘的远近排名, 越近越浓 (逐段画);
      //   图例只管两色的显隐
      static const char *const kName[] = {"减仓线", "加仓线"};
      const ImVec4 kCol[] = {kDn, kUp};
      const size_t U = static_cast<size_t>(tr.cut.units);
      const size_t K = at.size();
      std::vector<float> p(U * K, cta::kNaN); // [份][点] 价
      std::vector<uint8_t> pol(p.size(), 0);  // [份][点] 1 = 加仓线
      std::vector<uint8_t> rk(p.size(), 0);   // [份][点] 同极性里离收盘的名次 (0 = 最近)
      for (size_t k = 0; k < K; ++k) {
        tr.cut.rungs_at(at[k], [&](int j, float v, bool add) { p[static_cast<size_t>(j) * K + k] = v, pol[static_cast<size_t>(j) * K + k] = add; });
        for (size_t j = 0; j < U; ++j) {
          const float dj = std::fabs(p[j * K + k] - static_cast<float>(close[k]));
          for (size_t i = 0; i < U; ++i) {
            const float di = std::fabs(p[i * K + k] - static_cast<float>(close[k]));
            rk[j * K + k] += pol[i * K + k] == pol[j * K + k] && (di < dj || (di == dj && i < j)); // NaN 比较恒假
          }
        }
      }
      bool show[2];
      for (size_t a = 0; a < 2; ++a) {
        ImPlot::SetNextLineStyle(kCol[a]);
        ImPlot::PlotDummy(kName[a]);
        const ImPlotItem *it = ImPlot::GetItem(kName[a]);
        show[a] = !it || it->Show;
      }
      ImDrawList *dl = ImPlot::GetPlotDrawList();
      ImPlot::PushPlotClipRect();
      for (size_t j = 0; j < U; ++j)
        for (size_t k = 0; k + 1 < K; ++k) {
          const size_t e = j * K + k + 1;
          const float y0 = p[e - 1], y1 = p[e];
          if (std::isnan(y0) || std::isnan(y1) || !show[pol[e]])
            continue;
          ImVec4 col = kCol[pol[e]];
          col.w = kBandA0 + (kBandA1 - kBandA0) * static_cast<float>(rk[e]) / static_cast<float>(std::max<size_t>(1, U - 1));
          dl->AddLine(ImPlot::PlotToPixels(xs[k], y0), ImPlot::PlotToPixels(xs[k + 1], y1), ImGui::GetColorU32(col), 1.0f);
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
    //   基线 = 策略净值; 图例隐去策略净值时 = 1 (各沿除以策略净值, 只看成本本身); 各沿在锚根 = 1 (自锚根起不扣前几项的净值)
    const cta::Book &sb = tr.strat;
    const bool hovered = ImPlot::IsPlotHovered(); // 锁定 setup 之后 item 才能按 label 查
    const ImPlotItem *nav_it = ImPlot::GetItem("策略净值");
    const bool on_nav = !nav_it || nav_it->Show;
    const auto lift = [&](size_t j, size_t i) { return j ? sb.lift[j - 1][i] / sb.lift[j - 1][anc] : sb.nav[i] / sb.nav[anc]; };
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
    if (c.anchor > 0) { // 锚点: 锚定日的日界
      const double x = xd.xs[static_cast<size_t>(c.anchor)];
      ImPlot::SetNextLineStyle(kHoldCol, 1.0f);
      ImPlot::PlotInfLines("##cta_anchor", &x, 1, ImPlotItemFlags_NoFit);
    }
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
  if (corner.x >= 0) { // 净值图 y 轴切换: 叠在右上角, 画完光标回图下
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

// 减仓线 / 加仓线 (下标 0 / 1)
const cta::Path &path_of(const CtaResult &r, size_t k) { return k ? r.tr.add : r.tr.reduce; }
constexpr const char *kPathName[] = {"减仓线", "加仓线"};

// 光标处 (−1 = 末根) 所在日 → v[k] = 当日所用参数 (阻尼后, 轴的单位; 减仓线未选参 = 买入持有, 两者都 nullptr; 加仓线未选参 = 沿用的上一个选参日, 尚无 = nullptr); 返回说明
std::string cursor_at(const CtaResult &r, int cursor, const cta::V *(&v)[2]) {
  const size_t n = r.se->c.size(), i = cursor >= 0 ? static_cast<size_t>(cursor) : n - 1;
  const size_t d = dix(r, i);
  const std::string head = cursor >= 0 ? "光标 " + when(r, i) : "末根";
  v[0] = v[1] = nullptr;
  if (!r.tr.reduce.sel[d])
    return head + ": 买入持有 (首日 / 窗内数据不足 / 减仓线未就绪, 未选参)";
  v[0] = &r.tr.reduce.use[d];
  const cta::Path &a = r.tr.add;
  for (size_t e = d + 1; e-- > 0 && !v[1];)
    if (a.sel[e])
      v[1] = &a.use[e];
  return head + ": 当日所用 (阻尼后)" + (a.sel[d] ? "" : v[1] ? "; 加仓线窗内无笔, 沿用上一个选参日的"
                                                              : "; 加仓线尚未选参, 用初始中心");
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

// ---- 参数轨迹: 减仓线 / 加仓线逐日各轴参数 ÷ 各自首个选参日的值 (都从 1 起, log 轴; 只看相对变化): 实线 = 阻尼后所用, 淡线 = 曲面最高点 (跳 = 岭回归选参不稳);
//      x 链到两图 (同一视野, 双击回全段), 未选参日断开; 光标 = 橙色竖线; 悬停看当日参数 ----

// 参数 v (轴的单位): 头 模型  轴 = 值 …
std::string params_of(const char *head, const cta::Grid &g, const cta::V &v) {
  std::string s = std::string(head) + " " + g.model->name;
  for (size_t a = 0; a < g.na; ++a) {
    char b[48];
    std::snprintf(b, sizeof(b), "  %s = %.3g%s", g.ax[a]->name, v[a], g.ax[a]->day ? " 日" : "");
    s += b;
  }
  return s;
}

// 模型 k 在网格配置数 n 下的参数网格: 维数 + 各轴格点数 (之积 = 每日评的组合数)
std::string grid_note(const cta::Kind &k, int n) {
  if (k.axes.empty())
    return "无参数, 不调参";
  const cta::At m = cta::split(k, static_cast<size_t>(n));
  std::string names, counts;
  size_t prod = 1;
  for (size_t a = 0; a < k.axes.size(); ++a)
    names += (a ? " × " : "") + std::string(k.axes[a].name), counts += (a ? " × " : "") + std::to_string(m[a]), prod *= m[a];
  return "参数网格 " + std::to_string(k.axes.size()) + " 维: " + names + " = " + counts + (k.axes.size() > 1 ? " = " + std::to_string(prod) : "") + " 个组合";
}

void path(CtaState &c, const CtaResult &r) {
  const cta::Track &tr = r.tr;
  const size_t n = r.se->c.size();
  // 一条线 = 一个模型的一个轴: 名 / [点] 所用 / 最高点 (÷ 首个选参日的值)
  struct Line {
    std::string name;
    std::vector<double> use, raw;
  };
  std::vector<Line> ls;
  std::vector<double> xs; // 阶梯: 每日两点 (首根, 末根), 日间的竖跳 = 换参; 未选参日 = NaN 断开
  for (size_t d = 0; d < tr.days.size(); ++d)
    if (tr.days.off[d] < tr.days.off[d + 1])
      xs.push_back(static_cast<double>(tr.days.off[d])), xs.push_back(static_cast<double>(tr.days.off[d + 1] - 1));
  size_t first[2] = {};
  for (size_t k = 0; k < 2; ++k) {
    const cta::Path &p = path_of(r, k);
    first[k] = static_cast<size_t>(std::ranges::find(p.sel, 1) - p.sel.begin());
    if (first[k] == p.sel.size())
      continue;
    const cta::V &v0 = p.use[first[k]];
    for (size_t a = 0; a < p.grid.na; ++a) {
      Line l{std::string(kPathName[k]) + " " + p.grid.ax[a]->name, {}, {}};
      for (size_t d = 0; d < tr.days.size(); ++d)
        if (tr.days.off[d] < tr.days.off[d + 1])
          for (int twice = 0; twice < 2; ++twice) {
            l.use.push_back(p.sel[d] ? p.use[d][a] / v0[a] : cta::kNaN);
            l.raw.push_back(p.sel[d] ? p.raw[d][a] / v0[a] : cta::kNaN);
          }
      ls.push_back(std::move(l));
    }
  }
  if (first[0] == tr.reduce.sel.size()) {
    ImGui::TextDisabled("全期未选参 (买入持有)");
    return;
  }
  // y 范围 = 视野内各日 (与视野相交) 的 log 跨度, 上下各留 20%; 跨度为 0 按一格 (kStep) 计
  double lo = INFINITY, hi = -INFINITY;
  for (size_t k = 0; k + 1 < xs.size(); k += 2)
    if (xs[k] <= c.x1 && xs[k + 1] >= c.x0)
      for (const Line &l : ls)
        for (const std::vector<double> *y : {&l.use, &l.raw})
          if (std::isfinite((*y)[k]))
            lo = std::min(lo, std::log10((*y)[k])), hi = std::max(hi, std::log10((*y)[k]));
  if (!(lo <= hi))
    lo = hi = 0;
  const double span = hi > lo ? hi - lo : std::log10(cta::kStep);
  const float w = ImGui::GetContentRegionAvail().x;
  XFmt fmt{&r, c.x1 - c.x0 < 3.0 * kS};
  const XDays xd = x_days(r);
  DayAxis ax{xd.dates, 1.0, 0.0, xd.xs, x_formatter, &fmt};
  if (ImPlot::BeginPlot("##cta_path", ImVec2(w, std::floor(w * 0.6f)), ImPlotFlags_NoTitle | ImPlotFlags_NoMenus | ImPlotFlags_NoBoxSelect)) {
    ImPlot::SetupAxis(ImAxis_X1, nullptr, ImPlotAxisFlags_None);
    ImPlot::SetupAxisLinks(ImAxis_X1, &c.x0, &c.x1);
    setup_day_axis(ax);
    ImPlot::SetupAxis(ImAxis_Y1, nullptr, ImPlotAxisFlags_None);
    ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
    ImPlot::SetupAxisLimits(ImAxis_Y1, std::pow(10.0, lo - 0.2 * span), std::pow(10.0, hi + 0.2 * span), ImPlotCond_Always);
    ImPlot::SetupLegend(ImPlotLocation_NorthWest, ImPlotLegendFlags_Horizontal);
    for (size_t a = 0; a < ls.size(); ++a) {
      const ImVec4 col = ImPlot::GetColormapColor(static_cast<int>(a));
      const std::string top = ls[a].name + " 最高点";
      ImPlot::SetNextLineStyle(ImVec4(col.x, col.y, col.z, 0.35f), 1.0f);
      ImPlot::PlotLine(top.c_str(), xs.data(), ls[a].raw.data(), static_cast<int>(xs.size()));
      ImPlot::SetNextLineStyle(col, 1.5f);
      ImPlot::PlotLine(ls[a].name.c_str(), xs.data(), ls[a].use.data(), static_cast<int>(xs.size()));
    }
    if (c.cursor >= 0) {
      const double x = c.cursor;
      ImPlot::SetNextLineStyle(kPinCol, 2.0f);
      ImPlot::PlotInfLines("##cta_path_pin", &x, 1);
    }
    if (ImPlot::IsPlotHovered()) {
      const size_t i = static_cast<size_t>(std::clamp(std::lround(ImPlot::GetPlotMousePos().x), 0L, static_cast<long>(n - 1))), d = dix(r, i);
      std::string s = misc::dash(day_of(r, i));
      for (size_t k = 0; k < 2; ++k) {
        const cta::Path &p = path_of(r, k);
        s += p.sel[d] ? "\n所用 (阻尼后): " + params_of(kPathName[k], p.grid, p.use[d]) + "\n最高点: " + params_of(kPathName[k], p.grid, p.raw[d])
                      : std::string("\n") + kPathName[k] + " 未选参" + (k ? "" : " (买入持有)");
      }
      tip("%s", s.c_str());
    }
    ImPlot::EndPlot();
  }
  for (size_t k = 0; k < 2; ++k) {
    const cta::Path &p = path_of(r, k);
    if (first[k] < p.sel.size())
      ImGui::TextDisabled("首个选参日: %s", params_of(kPathName[k], p.grid, p.use[first[k]]).c_str());
  }
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

  // 设定: 分组 (band / 减仓线 / 加仓线 / 分层 / 引擎), 组内一行一项, 名左值右; 改了即全部重算
  cta::Spec &sp = c.spec;
  const float x0 = ImGui::GetCursorPosX();
  float lw = 0.0f; // 名列宽 (控件 / 灰显参数都从这里起)
  for (const char *s : {"模型", "窗 (年)", "网格配置数", "滚动窗 (年)", "份数", "止损 (间距)", "方向", "费率 (bp)"})
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
  c.editing = false;
  const auto years = [&](const char *id, float &y, const char *what) { // 窗 (年): 对数滑条, 松手生效
    ImGui::SliderFloat(id, &y, cta::kMinYears, cta::kMaxYears, "%.2g 年", ImGuiSliderFlags_Logarithmic);
    y = std::clamp(y, cta::kMinYears, cta::kMaxYears);
    c.editing |= ImGui::IsItemActive();
    if (ImGui::IsItemHovered())
      tip("%s\n• %.2g ~ %.2g 年 (1 年 = %.0f 交易日), 默认 1 年; 不足窗长 = 自首日起全部 (expanding)\n• 松手生效 (Ctrl + 点击可直接输入)", what, cta::kMinYears,
          cta::kMaxYears, cta::kYearDays);
  };
  const cta::V *at[2] = {};
  const std::string where = r ? cursor_at(*r, c.path_open ? c.cursor : -1, at) : std::string{};
  const auto params = [&](const cta::Kind &k, const cta::V *v) { // 灰显参数 (按结果的模型): 光标处所用
    for (size_t a = 0; a < k.axes.size(); ++a) {
      const cta::Axis &ax = k.axes[a];
      char s[32];
      std::snprintf(s, sizeof(s), "%s%s", ax.name, ax.day ? " (日)" : "");
      ImGui::TextUnformatted(s);
      ImGui::SameLine(x0 + lw);
      if (v)
        ImGui::TextDisabled("%.3g", (*v)[a]);
      else
        ImGui::TextDisabled("-");
    }
  };

  ImGui::SeparatorText("band");
  label("模型");
  model_combo("##cta_band", cta::kBandKinds, sp.band, "band = 档线间距 (价格单位, 每日重算, 盘中用前一交易日收盘时的值; 人选, 不调); 止损与加仓线的下移都以它计");
  label("窗 (年)");
  years("##cta_band_win", sp.band_win, "band 的窗; 不足窗长 = 用已有的日 (增量窗)");
  label("网格配置数");
  ImGui::InputInt("##cta_grid", &sp.grid, 1, 10);
  sp.grid = std::clamp(sp.grid, 3, 1000);
  c.editing |= ImGui::IsItemActive();
  if (ImGui::IsItemHovered())
    tip("减仓线 / 加仓线各自独立选参时, 每日在当前最优点周围评的参数组合数上限 (两者共用此数, 各自按自己的参数轴分; 分法见各模型下的注释); 默认 %d\n"
        "• 参数轴 = 自 v0 向两侧铺的格点, 相邻之比 %.3g, 间距不小于各轴基本单位 (小值处退成等差), 收进各轴硬边界\n"
        "• 各轴的格点数轮流分得 (每次给格点最少的轴加一个), 积不超过此数; 网格 = 以当前最优为中点的全部组合 (碰到硬边界则整格平移进界)\n"
        "• 每日拟岭回归曲面取连续最高点 (参数可为小数, 相邻格点间对数插值); 最近的格点落在网格边上就以它为中心移网格、补算新格点再拟 (当日至多 %d 次), 中心留到次日\n"
        "• 编辑完生效",
        cta::kGrid, cta::kStep, cta::kHops);

  ImGui::SeparatorText("减仓线");
  label("模型");
  model_combo("##cta_reduce", cta::kReduceKinds, sp.reduce,
              "减仓线模型: 满仓时链顶随它, 往下每隔一个 band 一条减仓线; 目的是尽量保留顺向敞口\n"
              "调参目标 = 保敞口: 它当静态档线 (第 r 份 = 线 − r × band, 跌破即出 / 涨回即入, 与加仓线无关), 分 = (顺向 − 翻转成本) / 总振幅");
  hint(grid_note(cta::kReduceKinds[sp.reduce], sp.grid).c_str());
  label("滚动窗 (年)");
  years("##cta_reduce_win", sp.reduce_win, "减仓线选参的滚动窗: 每日以此前这么长的窗内网格各配置的分拟曲面, 取最高点用到当日; 窗内根数不足窗长一半 = 买入持有");
  params(cta::kReduceKinds[r ? r->spec.reduce : sp.reduce], at[0]);

  ImGui::SeparatorText("加仓线");
  label("模型");
  model_combo("##cta_add", cta::kAddKinds, sp.add,
              "加仓线模型: 已减的份的加仓线每日收盘怎么下移 (不低于 收盘 + 止损距), 回撤够深后下移做网格抄底\n"
              "调参目标 = 每笔中位夏普: 在当日减仓线下跑完整分层, 窗内卖出的各笔 (ln(卖价 / 买回价) − 成本) / (band / 卖价 × √持续日) 的中位; 窗末未买回的按窗末收盘计");
  hint(grid_note(cta::kAddKinds[sp.add], sp.grid).c_str());
  label("滚动窗 (年)");
  years("##cta_add_win", sp.add_win, "加仓线选参的滚动窗: 每日以此前这么长的窗内卖出的各笔打分, 拟曲面取最高点用到当日; 窗内无笔 = 沿用上一日的");
  params(cta::kAddKinds[r ? r->spec.add : sp.add], at[1]);
  if (r) {
    hint(where.c_str());
    if (ImGui::IsItemHovered())
      tip("灰显 = 光标 (参数轨迹展开时图上可拖的竖线; 收起 = 末根) 处所用的参数; 以日计的轴单位为日");
  }
  c.path_open = r && ImGui::CollapsingHeader("参数轨迹");
  if (r && ImGui::IsItemHovered())
    tip("减仓线 / 加仓线逐日参数, 每轴 ÷ 各自首个选参日的值 (都从 1 起, log 轴), 只看相对变化: 实线 = 阻尼后所用, 淡线 = 曲面最高点; 淡线跳得频 / 跳得大 = 岭回归选参不稳\n"
        "• 阻尼: 所用参数 (对数) 每日朝最高点走 1 / %.0f\n"
        "• x 与左侧两图同一视野; 展开时左图出现光标 (橙色竖线), 中栏参数看光标所在日",
        cta::kDamp);
  if (c.path_open)
    path(c, *r);

  ImGui::SeparatorText("分层");
  hint("减仓线链: 链顶锚定上一个触发点, 往下每条隔一个 band (每日变); 加仓线每日按加仓线模型下移");
  if (ImGui::IsItemHovered())
    tip("持有的份挂减仓线 (逆向跌到即减一份), 已减的份各挂一条加仓线 (顺向涨到即加回该份); 成交按档线价\n"
        "• 减仓线链: 链顶锚定上一个触发点, 往下每条 = 上一条 − 当日 band (锚定不变, band 每日变)\n"
        "• 链顶: 满仓 = 减仓线模型; 卖出 @a 后 = a − 当日 band; 买回 @L 后 = L − 止损 × 当日 band\n"
        "• 满仓 (只剩减仓线) 且减仓线模型上穿链顶 = 回到随它\n"
        "• 加仓线: 卖出 @a 即挂 a + 止损距; 每日收盘按加仓线模型下移, 不低于 收盘 + 止损距 (不会自行触发); 减仓线不下移\n"
        "• 一根越过几条成交几条 (先近后远)\n"
        "• 图上: 一份一条连续线 (恒为份数条), 绿 = 减仓线, 红 = 加仓线, 成交处换色; 同色里离收盘越近越浓\n"
        "• 减仓线 / band 尚无值 (如首日) = 买入持有 (与基准同口径)");
  label("份数");
  ImGui::InputInt("##cta_units", &sp.rule.units, 1, 5);
  sp.rule.units = std::clamp(sp.rule.units, 1, 100);
  c.editing |= ImGui::IsItemActive();
  if (ImGui::IsItemHovered())
    tip("满仓分几份 (= 档线条数), 一份 = 1 / 份数 的仓位; 默认 %d; 编辑完生效", cta::kUnits);
  label("止损 (间距)");
  ImGui::InputFloat("##cta_stop", &sp.rule.stop, 0.1f, 0.5f, "%.2f");
  sp.rule.stop = std::clamp(sp.rule.stop, 0.0f, 10.0f);
  c.editing |= ImGui::IsItemActive();
  if (ImGui::IsItemHovered())
    tip("成交一份, 该份换极性并止损 × 当日 band: 卖出 = 加仓线挂在成交价上方这么远, 买回 = 链顶挂在买入价下方这么远; 默认 %.2g; 编辑完生效", cta::kStop);

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
  else if (show) {
    const LinesLeg &lg = ui.roll.key.leg; // 滚动分位所属的估值腿 (= 本页标的的)
    plots(c, *show, name, {ui.der.val.dates, ui.der.val.val_at, lg.b ? lg.b->val_of(lg.line, kValBp) : nullptr, ui.roll.val[kValBp], ui.roll.key.years},
          ImGui::GetContentRegionAvail());
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