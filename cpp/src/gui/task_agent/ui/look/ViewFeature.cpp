// view_feature 的展示: 每个式子一节 (概况 + 分布柱 + 方差来源 + 持续性 + 日内轮廓 / 逐日中位 + rIC 分时段) + 两两相关表
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <cstdio>

namespace GUI::Agent {

using namespace ui;

void FeatureRes::draw() const {
  if (!period.empty())
    wrapped(color(TaskStatus::Kind::Warn), period);
  char b[256];
  for (size_t i = 0; i < feats.size(); ++i) {
    const Feat &f = feats[i];
    ImGui::PushID(static_cast<int>(i));
    heading("E" + std::to_string(i + 1) + "  " + f.raw);
    if (!f.err.empty()) {
      wrapped(color(TaskStatus::Kind::Error), "没算成: " + f.err);
    } else if (f.ok < 100) {
      ImGui::TextDisabled("池内有效格太少 (%zu / %zu)", f.ok, f.in);
    } else {
      std::snprintf(b, sizeof(b), "池内有效 %.0f%% (%zu 格) · 均值 %.4g · 标准差 %.4g%s", 100.0 * f.ok / f.in, f.ok, f.mean, f.sd, f.zero >= 0.05 ? " · 零值多" : "");
      ImGui::TextUnformatted(b);
      if (grid("##q", {"p1", "p5", "p25", "p50", "p75", "p95", "p99"})) {
        ImGui::TableNextRow();
        for (const double q : f.q)
          cell_num(q, "%.4g");
        ImGui::EndTable();
      }
      if (f.levels)
        ImGui::TextColored(color(TaskStatus::Kind::Warn), "离散: 抽样里只有 %zu 种取值", f.levels);
      hist_plot("分布 (p1~p99)##hist", f.hist, f.h_lo, f.h_hi, 130.0f);
      if (f.var)
        bars_plot("方差来源##var", {"资产间", "日间", "日内"}, {f.v_asset, f.v_day, f.v_intra}, 80.0f);
      if (grid("##lag", {"持续性", lags.size() > 0 ? lags[0] : "", lags.size() > 1 ? lags[1] : "", lags.size() > 2 ? lags[2] : ""})) {
        ImGui::TableNextRow();
        cell("自相关");
        for (const double x : f.lag)
          cell_num(x, "%.3f");
        ImGui::EndTable();
      }
      if (seg > 1 && f.slow)
        ImGui::TextDisabled("日内方差太小 (日频慢变量), 不画日内轮廓");
      else if (seg > 1)
        line_plot("日内轮廓 (各行截面中位数)##prof", f.prof, 140.0f, "中位数", &clock);
      else
        line_plot("截面中位数逐日##prof", f.prof, 140.0f, "中位数");
      std::snprintf(b, sizeof(b), "截面 rIC (原向) %.3f · t %.1f · 正 rIC 行 %.0f%%", f.ric, f.ric_t, f.ric_up * 100.0);
      ImGui::TextUnformatted(b);
      if (seg > 1 && f.ric_part.size() == parts.size())
        bars_plot("分时段 rIC##rp", parts, f.ric_part, 30.0f + 18.0f * static_cast<float>(parts.size()));
    }
    ImGui::PopID();
  }
  if (good.size() >= 2) {
    heading("两两截面秩相关");
    std::vector<std::string> hs{""};
    for (const size_t g : good)
      hs.push_back("E" + std::to_string(g + 1));
    if (grid("##corr", hs)) {
      for (size_t i = 0; i < good.size(); ++i) {
        ImGui::TableNextRow();
        cell("E" + std::to_string(good[i] + 1));
        for (size_t j = 0; j < good.size(); ++j)
          cell_num(corr[i * good.size() + j], "%.3f", true);
      }
      ImGui::EndTable();
    }
  }
}

} // namespace GUI::Agent
