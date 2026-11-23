// eval_factor 的展示: 候选 (模板 / 思路) + 结局 + 各权重表
//   + 参数扫描 (左 = 各参数点的窗 rIC 复合图, 右 = 参数点表, 缺省中位 rIC 降序; 图 ↔ 表悬停联动)
//   + 写进的权重的累计 rIC + 分层图 (Inspect 同一份; 没有逐日序列 = 载入的会话 → 20 组柱) + 分年
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

constexpr float kSweepH = 320.0f;  // 复合图 / 点表的高
constexpr float kCumH = 220.0f;    // 累计 rIC 图的高
constexpr float kLayersH = 420.0f; // 分层图的高
constexpr float kLineA = 0.55f;    // 非悬停线的不透明度
constexpr float kBestW = 2.0f;     // 中位 rIC 最高的点 (= 写进的权重) 线宽

int date_fmt(double v, char *buf, int size, void *ud) {
  const std::vector<int> &ds = *static_cast<const std::vector<int> *>(ud);
  if (ds.empty() || v < 0)
    return std::snprintf(buf, static_cast<size_t>(size), " ");
  const int y = ds[std::min(static_cast<size_t>(v), ds.size() - 1)];
  return std::snprintf(buf, static_cast<size_t>(size), "%02d/%02d/%02d", y / 10000 % 100, y / 100 % 100, y % 100);
}

// 线 p 的首个有值日 (首个计入的窗之前 NaN)
size_t first_of(const Point &p) {
  size_t d = 0;
  while (d < p.line.size() && std::isnan(p.line[d]))
    ++d;
  return d;
}

// 名次色 (pts 已按中位 rIC 降序: 下标即名次, 0 = 最好 绿 → 末 = 红)
ImVec4 rank_col(size_t i, size_t n, float a) { return band_col(n > 1 ? static_cast<double>(i) / static_cast<double>(n - 1) : 0.0, a); }

// 左: 各点的窗 rIC (图例只列写进的权重); 悬停 (本表或上帧图) 的点白粗线画在最上; 返回图上悬停的点 (−1 = 无)
int sweep_plot(const Sweep &sw, int hot, const ImVec2 &size) {
  const size_t n = sw.pts.size(), D = sw.dates.size();
  if (!ImPlot::BeginPlot("##sweep_ric", size, ImPlotFlags_NoTitle | ImPlotFlags_NoMenus))
    return -1;
  ImPlot::SetupAxes(nullptr, "窗 rIC", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest);
  DayAxis ax{sw.dates, 1.0, 0.0, {}, date_fmt, const_cast<std::vector<int> *>(&sw.dates)};
  setup_day_axis(ax);
  std::vector<size_t> from(n);
  for (size_t i = 0; i < n; ++i)
    from[i] = first_of(sw.pts[i]);
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

// 写进的权重的累计 rIC (逐日 ok 行 rIC 之和沿日累加; 色同参数扫描里的权重线)
void cum_plot(const Daily &d, size_t n_pts) {
  std::vector<float> cum(d.ric.size());
  double s = 0.0;
  for (size_t i = 0; i < d.ric.size(); ++i)
    cum[i] = static_cast<float>(s += d.ric[i]);
  if (!ImPlot::BeginPlot("##cum_ric", ImVec2(-1, kCumH), ImPlotFlags_NoTitle | ImPlotFlags_NoMenus))
    return;
  ImPlot::SetupAxes(nullptr, "累计 rIC", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
  ImPlot::SetupLegend(ImPlotLocation_NorthWest);
  DayAxis ax{d.dates, 1.0, 0.0, {}, date_fmt, const_cast<std::vector<int> *>(&d.dates)};
  setup_day_axis(ax);
  ImPlot::SetNextLineStyle(rank_col(0, n_pts, 1.0f), kBestW);
  ImPlot::PlotLine("权重", cum.data(), static_cast<int>(cum.size()));
  ImPlot::EndPlot();
}

// 写进的权重的分层图 (与 Inspect 左上同一份): 滚动窗口滑条 + 分层对数累计 / 光标日截面
void layers_view(const TrialRes &k) {
  const Daily &d = k.st.res.daily;
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
  Layer::LayerDerived &l = *k.lay;
  int w = k.win > 0 ? std::min(k.win, l.days) : l.days;
  ImGui::SetNextItemWidth(160.f);
  if (ImGui::SliderInt("##win", &w, 1, l.days, w >= l.days ? "窗口 全段" : "窗口 %d 天", ImGuiSliderFlags_Logarithmic | ImGuiSliderFlags_AlwaysClamp))
    k.win = w >= l.days ? 0 : w;
  Layer::roll_layers(l, w);
  Layer::plot_layers(l, false, k.cursor, ImVec2(-1, kLayersH));
}

// 右: 参数点表 (列头点了排序, 缺省中位 rIC 降序); plot_hot = 图上悬停的点 (该行底色亮); 返回表上悬停的点 (−1 = 无)
int sweep_table(const Sweep &sw, int plot_hot, float h) {
  const size_t P = sw.slots.size(), n = sw.pts.size();
  enum Col : int { kSign,
                   kMed,
                   kCols };
  static const char *kHeads[kCols] = {"向", "中位 rIC"};
  constexpr ImGuiTableFlags kFlags = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollX |
                                     ImGuiTableFlags_ScrollY | ImGuiTableFlags_Sortable | ImGuiTableFlags_NoSavedSettings;
  if (!ImGui::BeginTable("##sweep_tbl", static_cast<int>(P) + kCols, kFlags, ImVec2(-1, h)))
    return -1;
  ImGui::TableSetupScrollFreeze(static_cast<int>(P), 1);
  for (const std::string &s : sw.slots)
    ImGui::TableSetupColumn(s.c_str());
  for (int c = 0; c < kCols; ++c)
    ImGui::TableSetupColumn(kHeads[c], c == kSign  ? ImGuiTableColumnFlags_NoSort
                                       : c == kMed ? ImGuiTableColumnFlags_DefaultSort | ImGuiTableColumnFlags_PreferSortDescending
                                                   : ImGuiTableColumnFlags_PreferSortDescending);
  ImGui::TableHeadersRow();
  const auto val = [&](const Point &p, int c) -> double {
    if (c < static_cast<int>(P))
      return p.v[static_cast<size_t>(c)];
    return c - static_cast<int>(P) == kMed ? p.score : 0.0;
  };
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
    cell(DirCn(p.sign)), cell_num(p.score, "%.4f", true);
    ImGui::PopID();
  }
  ImGui::EndTable();
  return hot;
}

void sweep_view(const Sweep &sw, int &hot) {
  if (sw.slots.empty() || sw.pts.size() < 2 || sw.dates.empty())
    return;
  heading(SweepHead(sw));
  if (!ImGui::BeginTable("##sweep", 2, ImGuiTableFlags_SizingStretchProp | ImGuiTableFlags_BordersInnerV))
    return;
  ImGui::TableSetupColumn("图", ImGuiTableColumnFlags_WidthStretch, 1.0f);
  ImGui::TableSetupColumn("表", ImGuiTableColumnFlags_WidthStretch, 1.0f);
  ImGui::TableNextRow();
  ImGui::TableNextColumn();
  const int near = sweep_plot(sw, hot, ImVec2(-1, kSweepH));
  ImGui::TableNextColumn();
  const int row = sweep_table(sw, near, kSweepH);
  ImGui::EndTable();
  hot = near >= 0 ? near : row;
}

} // namespace

void TrialRes::draw() const {
  const Result &r = st.res;
  ImGui::PushID(this);
  heading(st.cand.name + "  " + st.cand.name_cn);
  wrapped(kColExpr, st.cand.expr);
  if (!st.cand.note.empty())
    wrapped_dim("思路 (" + st.cand.hyp + "): " + st.cand.note);
  const TaskStatus::Kind kind = r.verdict == Verdict::Weights ? (GateMiss(gate, r).empty() ? TaskStatus::Kind::Ready : TaskStatus::Kind::None)
                                : r.verdict == Verdict::Null  ? TaskStatus::Kind::Warn
                                                              : TaskStatus::Kind::Error;
  wrapped(color(kind), Brief(r));
  if (const std::string g = GateLine(r, gate); !g.empty())
    wrapped(color(kind), g);
  if (r.visible) {
    if (!r.ws.empty() && grid("##ws", {"权重", "中位 rIC", "向", "每期毛多空 bp", "每期净多空 bp", "年换手"})) {
      for (const Weight &w : r.ws) {
        ImGui::TableNextRow();
        cell(Fills(r.tmpl, w.expr));
        if (!w.has) {
          cell_dim("无该持有期统计");
          for (int i = 0; i < 4; ++i)
            cell("");
          continue;
        }
        cell_num(w.score, "%.4f", true), cell(DirCn(w.sign)), cell_num(w.gross * 1e4, "%+.2f", true), cell_num(w.net * 1e4, "%+.2f", true), cell_num(w.turn, "%.0f");
      }
      ImGui::EndTable();
    }
    sweep_view(r.sweep, hot);
    if (!r.daily.dates.empty()) {
      cum_plot(r.daily, r.sweep.pts.size());
      layers_view(*this);
    }
    if (const Weight *w = r.best(); w && !w->grp.empty()) {
      if (r.daily.dates.empty())
        groups_plot((GroupsHead() + "##grp").c_str(), w->grp, 180.0f);
      if (!w->years.empty()) {
        heading(YearsHead(w->years));
        std::vector<std::string> heads;
        for (const Year &y : w->years)
          heads.push_back(std::to_string(y.year));
        if (grid("##years", heads)) {
          ImGui::TableNextRow();
          for (const Year &y : w->years)
            cell_num(y.ric, "%.4f", true);
          ImGui::EndTable();
        }
      }
    }
  }
  ImGui::PopID();
}

} // namespace GUI::Agent
