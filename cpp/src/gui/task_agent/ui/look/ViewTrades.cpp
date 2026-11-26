// view_trades 的图: 段超额分布 (节与表见 tools/look/ViewTrades.cpp)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void TradesRes::plot(int) const { hist_plot("段超额分布 (p1~p99)##hist", hist, h_lo, h_hi, 130.0f); }

} // namespace GUI::Agent
