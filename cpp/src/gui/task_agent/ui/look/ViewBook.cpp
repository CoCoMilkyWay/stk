// view_book 的图: 逐秒成交价 / 累计主动净额 + 全簿剖面横条 (节与表见 tools/look/ViewBook.cpp)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <cstdio>

namespace GUI::Agent {

using namespace ui;

void BookRes::plot(int i) const {
  if (i == 0) {
    line_plot("逐秒成交价##price", price, 150.0f, "价");
    line_plot("累计主动买 − 主动卖 (万)##net", net, 110.0f, "万");
    return;
  }
  std::vector<std::string> names;
  std::vector<double> vals;
  char b[64];
  for (const Level &l : profile) {
    std::snprintf(b, sizeof(b), "%s%d %.2f", l.bid ? "买" : "卖", l.rank, l.price);
    names.emplace_back(b), vals.push_back((l.bid ? 1.0 : -1.0) * l.amount / 1e4);
  }
  bars_plot("挂单额 (万; 卖为负)##book", names, vals, 30.0f + 16.0f * static_cast<float>(profile.size()));
}

} // namespace GUI::Agent
