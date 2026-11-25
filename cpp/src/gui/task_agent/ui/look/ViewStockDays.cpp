// view_stock_days 的展示: 收盘价 / 当日涨跌 / 因子日均分位的日级图 + 逐日表
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

namespace GUI::Agent {

using namespace ui;

void StockDaysRes::draw() const {
  draw_head();
  for (const std::string &e : exprs)
    wrapped_dim(e);
  std::vector<double> cl, ch, rk;
  std::vector<std::string> ds;
  for (const Day &d : days)
    cl.push_back(d.close), ch.push_back(d.chg * 100.0), rk.push_back(d.rank * 100.0), ds.push_back(d.date.substr(5));
  line_plot("收盘价##close", cl, 150.0f, "收", &ds);
  line_plot("当日涨跌 %##chg", ch, 110.0f, "%", &ds);
  if (has_fac)
    line_plot(("因子 " + fac + " 日均分位##rank").c_str(), rk, 110.0f, "分位", &ds);
  std::vector<std::string> hs{"日期", "涨跌", "振幅", "成交额 万", "在池行"};
  if (has_fac)
    hs.push_back("日均分位"), hs.push_back("收盘分位"), hs.push_back("多/空行");
  hs.push_back("收盘开仓超额 bp");
  for (size_t j = 0; j < n_feats; ++j)
    hs.push_back("E" + std::to_string(j + 1));
  if (!grid("##days", hs))
    return;
  for (const Day &d : days) {
    ImGui::TableNextRow();
    cell(d.date), cell_num(d.chg * 100, "%+.2f%%", true), cell_pct(d.amp), cell_num(d.amt / 1e4, "%.0f"), cell(std::to_string(d.in));
    if (has_fac)
      cell_num(d.rank * 100, "%.0f"), cell_num(d.rank_close * 100, "%.0f"), cell(std::to_string(d.up) + "/" + std::to_string(d.dn));
    cell_bp(d.ex);
    for (size_t j = 0; j < n_feats; ++j)
      cell_num(j < d.fv.size() ? d.fv[j] : NAN, "%.4g");
  }
  ImGui::EndTable();
}

} // namespace GUI::Agent
