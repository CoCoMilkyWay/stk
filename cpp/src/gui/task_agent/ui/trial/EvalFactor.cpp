// eval_factor 的展示: 候选 (模板 / 思路) + 结局 + 各权重表 + 最佳权重的 20 组与分年 +
//   参数扫描 (同 Beta→Style 选池: 左 = 各参数点的累计中位 rIC 复合图, 右 = 参数点表, 缺省中位 rIC 降序; 图 ↔ 表悬停联动)
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/tools/Trial.hpp"
#include "gui/task_agent/ui/Widgets.hpp"
#include "gui/util/Chart.hpp"     // DayAxis / band_col
#include "gui/util/HoverSync.hpp" // near_line / hot_row / kHotCol

#include "implot.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <limits>
#include <vector>

namespace GUI::Agent {

using namespace ui;

namespace {

constexpr float kSweepH = 320.0f; // 复合图 / 点表的高
constexpr float kLineA = 0.55f;   // 非悬停线的不透明度
constexpr float kBestW = 2.0f;    // 中位 rIC 最高的点 (= 写进的权重) 线宽

int date_fmt(double v, char *buf, int size, void *ud) {
  const std::vector<int> &ds = *static_cast<const std::vector<int> *>(ud);
  if (ds.empty() || v < 0)
    return std::snprintf(buf, static_cast<size_t>(size), " ");
  const int y = ds[std::min(static_cast<size_t>(v), ds.size() - 1)];
  return std::snprintf(buf, static_cast<size_t>(size), "%02d/%02d/%02d", y / 10000 % 100, y / 100 % 100, y % 100);
}

// 线 p 的首个有值日 (累计中位线首窗之前 NaN)
size_t first_of(const Point &p) {
  size_t d = 0;
  while (d < p.line.size() && std::isnan(p.line[d]))
    ++d;
  return d;
}

// 名次色 (pts 已按中位 rIC 降序: 下标即名次, 0 = 最好 绿 → 末 = 红)
ImVec4 rank_col(size_t i, size_t n, float a) { return band_col(n > 1 ? static_cast<double>(i) / static_cast<double>(n - 1) : 0.0, a); }

// 左: 各点的累计中位 rIC; 悬停 (本表或上帧图) 的点白粗线画在最上; 返回图上悬停的点 (−1 = 无)
int sweep_plot(const Sweep &sw, int hot, const ImVec2 &size) {
  const size_t n = sw.pts.size(), D = sw.dates.size();
  if (!ImPlot::BeginPlot("##sweep_med", size, ImPlotFlags_NoTitle | ImPlotFlags_NoLegend | ImPlotFlags_NoMenus))
    return -1;
  ImPlot::SetupAxes(nullptr, "累计中位 rIC", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
  DayAxis ax{sw.dates, 1.0, 0.0, {}, date_fmt, const_cast<std::vector<int> *>(&sw.dates)};
  setup_day_axis(ax);
  std::vector<size_t> from(n);
  for (size_t i = 0; i < n; ++i)
    from[i] = first_of(sw.pts[i]);
  const auto draw = [&](size_t i, const ImVec4 &col, float w) {
    const Point &p = sw.pts[i];
    if (from[i] >= D)
      return;
    char id[16];
    std::snprintf(id, sizeof(id), "##p%zu", i);
    ImPlot::SetNextLineStyle(col, w);
    ImPlot::PlotLine(id, p.line.data() + from[i], static_cast<int>(D - from[i]), 1.0, static_cast<double>(from[i]));
  };
  for (size_t i = n; i-- > 0;) // 好的画在上
    if (static_cast<int>(i) != hot)
      draw(i, rank_col(i, n, i == 0 ? 1.0f : kLineA), i == 0 ? kBestW : 1.0f);
  if (hot >= 0 && static_cast<size_t>(hot) < n)
    draw(static_cast<size_t>(hot), kHotCol, kHotW);
  const int near = near_line(n, [&](size_t i, double x) {
    const long d = std::lround(x);
    return d >= static_cast<long>(from[i]) && d < static_cast<long>(D) ? static_cast<double>(sw.pts[i].line[static_cast<size_t>(d)])
                                                                       : std::numeric_limits<double>::quiet_NaN();
  });
  ImPlot::EndPlot();
  return near;
}

// 右: 参数点表 (列头点了排序, 缺省中位 rIC 降序); plot_hot = 图上悬停的点 (该行底色亮); 返回表上悬停的点 (−1 = 无)
int sweep_table(const Sweep &sw, int plot_hot, float h) {
  const size_t P = sw.slots.size(), n = sw.pts.size();
  enum Col : int { kSign,
                   kMed,
                   kMean,
                   kAnnMed,
                   kAnnMean,
                   kRic,
                   kMono,
                   kLs,
                   kCols };
  static const char *kHeads[kCols] = {"向", "中位 rIC", "平均 rIC", "年化多空 中位 %", "年化多空 平均 %", "全期 rIC", "mono", "多空 bp"};
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
    switch (c - static_cast<int>(P)) {
    case kMed:
      return p.roll.ric_med;
    case kMean:
      return p.roll.ric_mean;
    case kAnnMed:
      return p.roll.ann_med;
    case kAnnMean:
      return p.roll.ann_mean;
    case kRic:
      return p.st.ric;
    case kMono:
      return p.st.mono;
    case kLs:
      return p.st.ls;
    default:
      return 0.0;
    }
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
    cell(p.sign < 0 ? "反" : "正");
    cell_num(p.roll.ric_med, "%.4f", true), cell_num(p.roll.ric_mean, "%.4f", true);
    cell_num(p.roll.ann_med * 100.0, "%+.1f", true), cell_num(p.roll.ann_mean * 100.0, "%+.1f", true);
    cell_num(p.st.ric, "%.4f", true), cell_num(p.st.mono, "%.3f", true), cell_num(p.st.ls * 1e4, "%+.2f", true);
    ImGui::PopID();
  }
  ImGui::EndTable();
  return hot;
}

void sweep_view(const Sweep &sw, int &hot) {
  if (sw.slots.empty() || sw.pts.size() < 2 || sw.dates.empty())
    return;
  heading("参数扫描 (Search 评过 " + std::to_string(sw.pts.size()) + " 个点, 滚动窗 " + std::to_string(sw.pts.front().roll.win) +
          " 日; 线 = 截至该日全部窗 rIC 的中位, 色 = 中位 rIC 名次, 粗线 = 写进的权重)");
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
    if (!r.ws.empty() && grid("##ws", {"权重", "中位 rIC", "平均 rIC", "窗", "年化多空 中位 %", "全期 rIC", "mono", "多空 bp", "有效率", "年换手", "秩自相关", "向"})) {
      for (const Weight &w : r.ws) {
        ImGui::TableNextRow();
        cell(Fills(r.tmpl, w.expr));
        if (!w.has) {
          cell_dim("无该持有期统计");
          for (int i = 0; i < 10; ++i)
            cell("");
          continue;
        }
        cell_num(w.roll.ric_med, "%.4f", true), cell_num(w.roll.ric_mean, "%.4f", true);
        cell(std::to_string(w.roll.n) + " × " + std::to_string(w.roll.win) + " 日");
        cell_num(w.roll.ann_med * 100.0, "%+.1f", true), cell_num(w.st.ric, "%.4f", true), cell_num(w.st.mono, "%.3f", true), cell_num(w.st.ls * 1e4, "%+.2f", true);
        cell_pct(w.valid), cell_num(w.turn, "%.0f"), cell_num(w.rac, "%.3f"), cell(w.sign < 0 ? "反" : "正");
      }
      ImGui::EndTable();
    }
    if (const Weight *w = r.best(); w && !w->grp.empty()) {
      groups_plot("最佳权重 20 组每期毛收益 (减组间均值, bp)##grp", w->grp, 180.0f);
      if (!w->years.empty() && grid("##years", {"年", "行", "rIC", "mono", "多空 bp"})) {
        for (const Year &y : w->years) {
          ImGui::TableNextRow();
          cell(std::to_string(y.year)), cell(std::to_string(y.n)), cell_num(y.st.ric, "%.4f", true), cell_num(y.st.mono, "%.3f", true), cell_num(y.st.ls * 1e4, "%+.2f", true);
        }
        ImGui::EndTable();
      }
    }
    sweep_view(r.sweep, hot);
  }
  ImGui::PopID();
}

} // namespace GUI::Agent
