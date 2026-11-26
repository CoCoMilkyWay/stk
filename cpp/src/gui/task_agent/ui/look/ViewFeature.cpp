// view_feature 的图: 每个式子的分布柱 + 方差来源 + 日内轮廓 / 逐日中位 + 分时段 rIC (节与表见 tools/look/ViewFeature.cpp)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void FeatureRes::plot(int i) const {
  const Feat &f = feats[static_cast<size_t>(i)];
  hist_plot("分布 (p1~p99)##hist", f.hist, f.h_lo, f.h_hi, 130.0f);
  if (f.var)
    bars_plot("方差来源##var", {"资产间", "日间", "日内"}, {f.v_asset, f.v_day, f.v_intra}, 80.0f);
  if (seg > 1 && !f.slow)
    line_plot("日内轮廓 (各行截面中位数)##prof", f.prof, 140.0f, "中位数", &clock);
  else if (seg == 1)
    line_plot("截面中位数逐日##prof", f.prof, 140.0f, "中位数");
  if (seg > 1 && f.ric_part.size() == parts.size())
    bars_plot("分时段 rIC##rp", parts, f.ric_part, 30.0f + 18.0f * static_cast<float>(parts.size()));
}

} // namespace GUI::Agent
