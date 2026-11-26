// eval_factor 的展示 (头几节与原文对仗; 原文末尾的累计分层 / 分年 rIC 正负柱图这里换成下面的图): 候选 (最佳权重的具体式, 负号前置 / 思路) + 频率 · 预测周期 + 各权重统计 (最佳接门槛; 无权重 = 结局一行) + 分年
//   → 左栏三图 (各参数点的窗目标线 / 写进的权重的累计 rIC / 分层图, Inspect 同一份) | 右栏参数点表 (缺省 score 降序; 窗目标线图 ↔ 表悬停联动)
//   → 20 组柱 (只在没有逐日序列 = 载入的会话时)
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/tools/Trial.hpp"
#include "gui/task_agent/ui/Widgets.hpp"
#include "gui/util/Chart.hpp"     // DayAxis / band_col
#include "gui/util/HoverSync.hpp" // near_line / hot_row / kHotCol
#include "gui/util/Layers.hpp"

#include "implot.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <limits>
#include <vector>

namespace GUI::Agent {

using namespace ui;

namespace {

constexpr float kSweepH = 200.0f;                                                       // 窗目标线图的高
constexpr float kCumH = 150.0f;                                                         // 累计 rIC 图的高
constexpr float kLayersH = 300.0f;                                                      // 分层图的高
constexpr float kLineA = 0.55f;                                                         // 非悬停线的不透明度
constexpr float kBestW = 2.0f;                                                          // score 最高的点 (= 写进的权重) 线宽
constexpr const char *kLineName = factor::stat::kMonoTarget ? "窗 mono" : "窗中位 rIC"; // 窗目标线 (Stat/Roll.hpp roll_of line)

int date_fmt(double v, char *buf, int size, void *ud) {
  const std::vector<int> &ds = *static_cast<const std::vector<int> *>(ud);
  if (ds.empty() || v < 0)
    return std::snprintf(buf, static_cast<size_t>(size), " ");
  const int y = ds[std::min(static_cast<size_t>(v), ds.size() - 1)];
  return std::snprintf(buf, static_cast<size_t>(size), "%02d/%02d/%02d", y / 10000 % 100, y / 100 % 100, y % 100);
}

// 线的首个有值日 (首个计入的窗之前 NaN)
size_t first_of(const std::vector<float> &line) {
  size_t d = 0;
  while (d < line.size() && std::isnan(line[d]))
    ++d;
  return d;
}

// 名次色 (pts 已按 score 降序: 下标即名次, 0 = 最好 绿 → 末 = 红)
ImVec4 rank_col(size_t i, size_t n, float a) { return band_col(n > 1 ? static_cast<double>(i) / static_cast<double>(n - 1) : 0.0, a); }

// 左: 各点的窗目标线 (图例只列写进的权重; 纵轴含 0, 画 0 线); 悬停 (本表或上帧图) 的点白粗线画在最上; 返回图上悬停的点 (−1 = 无)
int sweep_plot(const Sweep &sw, int hot, const ImVec2 &size) {
  const size_t n = sw.pts.size(), D = sw.dates.size();
  float lo = 0.f, hi = 0.f;
  for (const Point &p : sw.pts)
    for (const float x : p.line)
      if (!std::isnan(x))
        lo = std::min(lo, x), hi = std::max(hi, x);
  if (!ImPlot::BeginPlot("##sweep_ric", size, ImPlotFlags_NoTitle | ImPlotFlags_NoMenus))
    return -1;
  ImPlot::SetupAxes(nullptr, kLineName, ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_None);
  const float m = (hi > lo ? hi - lo : 1.f) * 0.05f;
  ImPlot::SetupAxisLimits(ImAxis_Y1, lo - m, hi + m, ImPlotCond_Always);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest);
  DayAxis ax{sw.dates, 1.0, 0.0, {}, date_fmt, const_cast<std::vector<int> *>(&sw.dates)};
  setup_day_axis(ax);
  const double zero = 0.0;
  ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
  ImPlot::PlotInfLines("##zero", &zero, 1, ImPlotInfLinesFlags_Horizontal);
  std::vector<size_t> from(n);
  for (size_t i = 0; i < n; ++i)
    from[i] = first_of(sw.pts[i].line);
  const auto draw = [&](size_t i, const ImVec4 &col, float w, const char *label) {
    if (from[i] >= D)
      return;
    char id[32];
    std::snprintf(id, sizeof(id), "%s##p%zu", label, i);
    ImPlot::SetNextLineStyle(col, w);
    ImPlot::PlotLine(id, sw.pts[i].line.data() + from[i], static_cast<int>(D - from[i]), 1.0, static_cast<double>(from[i]));
  };
  for (size_t i = n; i-- > 0;) // 好的画在上
    if (static_cast<int>(i) != hot)
      draw(i, rank_col(i, n, i == 0 ? 1.0f : kLineA), i == 0 ? kBestW : 1.0f, i == 0 ? "权重" : "");
  if (hot >= 0 && static_cast<size_t>(hot) < n)
    draw(static_cast<size_t>(hot), kHotCol, kHotW, "");
  const int near = near_line(n, [&](size_t i, double x) {
    const long d = std::lround(x);
    return d >= static_cast<long>(from[i]) && d < static_cast<long>(D) ? static_cast<double>(sw.pts[i].line[static_cast<size_t>(d)])
                                                                       : std::numeric_limits<double>::quiet_NaN();
  });
  ImPlot::EndPlot();
  return near;
}

// 写进的权重的累计 rIC (逐日 rIC 逐日累加; 图例带年化 IR; 色同窗目标线图里的权重线)
void cum_plot(const DayTrace &d, size_t n_pts) {
  if (!ImPlot::BeginPlot("##cum_ric", ImVec2(-1, kCumH), ImPlotFlags_NoTitle | ImPlotFlags_NoMenus))
    return;
  ImPlot::SetupAxes(nullptr, "累计 rIC", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest);
  DayAxis ax{d.dates, 1.0, 0.0, {}, date_fmt, const_cast<std::vector<int> *>(&d.dates)};
  setup_day_axis(ax);
  char label[48];
  std::snprintf(label, sizeof(label), "权重  IR=%+.2f", d.ir);
  ImPlot::SetNextLineStyle(rank_col(0, n_pts, 1.0f), kBestW);
  ImPlot::PlotLine(label, d.cum.data(), static_cast<int>(d.cum.size()));
  ImPlot::EndPlot();
}

// 写进的权重的分层图 (与 Inspect 左上同一份, 恒全段): 分层对数累计 / 光标日截面
void layers_view(const TrialRes &k) {
  const DayTrace &d = *k.st.res.daily;
  assert(d.lay.size() == size_t{Layer::kLayerLines} && d.ls.size() == size_t{Layer::kLsLines} && d.turn.size() == size_t{Layer::kLsLines});
  if (!k.lay) {
    const float *lay[Layer::kLayerLines], *ls[Layer::kLsLines];
    for (int i = 0; i < Layer::kLayerLines; ++i)
      lay[i] = d.lay[static_cast<size_t>(i)].data();
    for (int i = 0; i < Layer::kLsLines; ++i)
      ls[i] = d.ls[static_cast<size_t>(i)].data();
    k.lay = std::make_shared<Layer::LayerDerived>();
    Layer::derive_layers(*k.lay, d.dates, lay, ls, d.turn.data());
  }
  Layer::roll_layers(*k.lay, k.lay->days);
  Layer::plot_layers(*k.lay, false, k.cursor, ImVec2(-1, kLayersH));
}

// 右: 参数点表 (列头点了排序, 缺省 score 降序); plot_hot = 图上悬停的点 (该行底色亮); 返回表上悬停的点 (−1 = 无)
int sweep_table(const Sweep &sw, int plot_hot, float h) {
  const size_t P = sw.slots.size(), n = sw.pts.size();
  constexpr ImGuiTableFlags kFlags = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollX |
                                     ImGuiTableFlags_ScrollY | ImGuiTableFlags_Sortable | ImGuiTableFlags_NoSavedSettings;
  if (!ImGui::BeginTable("##sweep_tbl", static_cast<int>(P) + 1, kFlags, ImVec2(-1, h)))
    return -1;
  ImGui::TableSetupScrollFreeze(static_cast<int>(P), 1);
  for (const std::string &s : sw.slots)
    ImGui::TableSetupColumn(s.c_str());
  ImGui::TableSetupColumn(factor::stat::kScoreName, ImGuiTableColumnFlags_DefaultSort | ImGuiTableColumnFlags_PreferSortDescending);
  ImGui::TableHeadersRow();
  const auto val = [&](const Point &p, int c) { return c < static_cast<int>(P) ? p.v[static_cast<size_t>(c)] : p.score; };
  std::vector<size_t> ord(n);
  for (size_t i = 0; i < n; ++i)
    ord[i] = i;
  if (const ImGuiTableSortSpecs *ss = ImGui::TableGetSortSpecs(); ss && ss->SpecsCount > 0) {
    const ImGuiTableColumnSortSpecs &s = ss->Specs[0];
    const bool up = s.SortDirection == ImGuiSortDirection_Ascending;
    std::stable_sort(ord.begin(), ord.end(), [&](size_t a, size_t b) {
      const double x = val(sw.pts[a], s.ColumnIndex), y = val(sw.pts[b], s.ColumnIndex);
      return up ? x < y : x > y;
    });
  }
  int hot = -1;
  for (const size_t i : ord) {
    const Point &p = sw.pts[i];
    ImGui::TableNextRow();
    if (static_cast<int>(i) == plot_hot)
      hot_row();
    ImGui::PushID(static_cast<int>(i));
    for (size_t j = 0; j < P; ++j) {
      ImGui::TableNextColumn();
      const double v = p.v[j];
      if (j == 0) {
        const ImVec2 at = ImGui::GetCursorPos();
        ImGui::Selectable("##row", false, ImGuiSelectableFlags_SpanAllColumns | ImGuiSelectableFlags_AllowOverlap);
        if (ImGui::IsItemHovered())
          hot = static_cast<int>(i);
        ImGui::SetCursorPos(at);
      }
      ImGui::TextColored(rank_col(i, n, 1.0f), v == std::round(v) ? "%.0f" : "%.4g", v);
    }
    cell_num(p.score, "%.4f", true);
    ImGui::PopID();
  }
  ImGui::EndTable();
  return hot;
}

// 左 = 窗目标线 / 累计 rIC / 分层 三图上下叠, 右 = 参数点表 (与左栏等高); 无自由参数只画左栏两图
void charts_view(const TrialRes &k) {
  const Result &r = k.st.res;
  const auto days = [&] {
    if (r.daily)
      cum_plot(*r.daily, r.sweep ? r.sweep->pts.size() : 0), layers_view(k);
  };
  if (!r.sweep || r.sweep->slots.empty() || r.sweep->pts.size() < 2 || r.sweep->dates.empty())
    return days();
  const Sweep &sw = *r.sweep;
  heading(SweepHead(sw));
  if (!ImGui::BeginTable("##charts", 2, ImGuiTableFlags_SizingStretchProp | ImGuiTableFlags_BordersInnerV))
    return;
  ImGui::TableSetupColumn("图", ImGuiTableColumnFlags_WidthStretch, 3.0f);
  ImGui::TableSetupColumn("表", ImGuiTableColumnFlags_WidthStretch, 1.0f);
  ImGui::TableNextRow();
  ImGui::TableNextColumn();
  const float y0 = ImGui::GetCursorPosY();
  const int near = sweep_plot(sw, k.hot, ImVec2(-1, kSweepH));
  days();
  const float h = ImGui::GetCursorPosY() - y0 - ImGui::GetStyle().ItemSpacing.y;
  ImGui::TableNextColumn();
  const int row = sweep_table(sw, near, h);
  ImGui::EndTable();
  k.hot = near >= 0 ? near : row;
}

} // namespace

void TrialRes::draw() const {
  const Result &r = st.res;
  ImGui::PushID(this);
  const Weight *best = shown();
  heading(st.cand.name + "  " + st.cand.name_cn);
  wrapped(kColExpr, best ? Formula(*best) : st.cand.expr);
  if (!st.cand.note.empty())
    wrapped_dim("思路: " + st.cand.note);
  const TaskStatus::Kind kind = r.verdict == Verdict::Weights   ? (GateMiss(gate, r).empty() ? TaskStatus::Kind::Ready : TaskStatus::Kind::None)
                                : r.verdict == Verdict::Null    ? TaskStatus::Kind::Warn
                                : r.verdict == Verdict::Pending ? TaskStatus::Kind::Muted
                                                                : TaskStatus::Kind::Error;
  if (!best) {
    wrapped(color(kind), Brief(r));
  } else {
    wrapped(ScopeLine(*this));
    wrapped(color(kind), WeightLine(*this, *best));
    for (const Weight &w : r.ws)
      if (&w != best)
        wrapped(WeightLine(*this, w));
    if (!best->years.empty())
      wrapped(YearsLine(best->years));
  }
  if (r.visible) {
    charts_view(*this);
    if (best && !best->grp.empty() && !r.daily)
      groups_plot((GroupsHead() + "##grp").c_str(), best->grp, 180.0f);
  }
  ImGui::PopID();
}

} // namespace GUI::Agent
