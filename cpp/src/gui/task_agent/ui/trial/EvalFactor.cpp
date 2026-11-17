// eval_factor 的展示: 候选 (模板 / 思路) + 结局 + 各权重表 + 最佳权重的 20 组与分年 + 参数曲面散点
#include "gui/task_agent/tools/Trial.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include "implot.h"

namespace GUI::Agent {

using namespace ui;

void TrialRes::draw() const {
  const Staged &st = k.st;
  const Result &r = st.res;
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
  if (!r.visible)
    return;
  if (!r.ws.empty() && grid("##ws", {"权重", "mono", "多空 bp", "rIC", "score", "有效率", "年换手", "秩自相关", "向"})) {
    for (const Weight &w : r.ws) {
      ImGui::TableNextRow();
      cell(Fills(r.tmpl, w.expr));
      if (!w.has) {
        cell_dim("无该持有期统计");
        for (int i = 0; i < 7; ++i)
          cell("");
        continue;
      }
      cell_num(w.st.mono, "%.3f", true), cell_num(w.st.ls * 1e4, "%+.2f", true), cell_num(w.st.ric, "%.4f", true), cell_num(w.score, "%.3g"), cell_pct(w.valid),
          cell_num(w.turn, "%.0f"), cell_num(w.rac, "%.3f"), cell(w.sign < 0 ? "反" : "正");
    }
    ImGui::EndTable();
  }
  if (r.top) {
    heading(std::string("代表点 (") + def(r.top->rep).cn + "): " + r.top->miss);
    wrapped(Fills(r.tmpl, r.top->expr) + " · " + Text(*r.top));
  }
  if (!r.rep_text.empty())
    wrapped_dim(r.rep_text);
  if (const Weight *w = r.best(); w && !w->grp.empty()) {
    groups_plot("最佳权重 20 组每期毛收益 (减组间均值, bp)##grp", w->grp, 180.0f);
    if (!w->years.empty() && grid("##years", {"年", "行", "mono", "多空 bp", "rIC"})) {
      for (const Year &y : w->years) {
        ImGui::TableNextRow();
        cell(std::to_string(y.year)), cell(std::to_string(y.n)), cell_num(y.st.mono, "%.3f", true), cell_num(y.st.ls * 1e4, "%+.2f", true), cell_num(y.st.ric, "%.4f", true);
      }
      ImGui::EndTable();
    }
  }
  const Surface &sf = r.surf;
  if (sf.slots.empty() || sf.pts.size() < 2)
    return;
  heading("参数曲面 (Search 评过 " + std::to_string(sf.pts.size()) + " 个点)");
  for (size_t j = 0; j < sf.slots.size(); ++j) {
    std::vector<double> xs, mono, ls;
    for (const Probe &p : sf.pts)
      xs.push_back(p.v[j]), mono.push_back(p.st.mono), ls.push_back(p.st.ls * 1e4);
    ImGui::PushID(static_cast<int>(j));
    if (ImPlot::BeginPlot((sf.slots[j] + " → mono / 多空##surf").c_str(), ImVec2(-1, 160), ImPlotFlags_NoMenus)) {
      ImPlot::SetupAxes(sf.slots[j].c_str(), "mono", ImPlotAxisFlags_AutoFit, ImPlotAxisFlags_AutoFit);
      ImPlot::SetupAxis(ImAxis_Y2, "多空 bp", ImPlotAxisFlags_AuxDefault | ImPlotAxisFlags_AutoFit);
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
      ImPlot::PlotScatter("mono", xs.data(), mono.data(), static_cast<int>(xs.size()));
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
      ImPlot::PlotScatter("多空 bp", xs.data(), ls.data(), static_cast<int>(xs.size()));
      ImPlot::EndPlot();
    }
    ImGui::PopID();
  }
}

} // namespace GUI::Agent
