// view_stock_days 的图: 收盘价 / 当日涨跌 / 因子日均分位的日级图 (节与表见 tools/look/ViewStockDays.cpp)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void StockDaysRes::plot(int) const {
  std::vector<double> cl, ch, rk;
  std::vector<std::string> ds;
  for (const Day &d : days)
    cl.push_back(d.close), ch.push_back(d.chg * 100.0), rk.push_back(d.rank * 100.0), ds.push_back(d.date.substr(5));
  line_plot("收盘价##close", cl, 150.0f, "收", &ds);
  line_plot("当日涨跌 %##chg", ch, 110.0f, "%", &ds);
  if (has_fac)
    line_plot(("因子 " + fac + " 日均分位##rank").c_str(), rk, 110.0f, "分位", &ds);
}

} // namespace GUI::Agent
