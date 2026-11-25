// view_book 的展示: 当日与窗口概况 + 逐秒成交价 / 累计主动净额 + 逐时表 + 大单计数与明细 + 全簿剖面 (横条)
#include "gui/task_agent/tools/Look.hpp"
#include "gui/task_agent/ui/Widgets.hpp"

#include <cmath>
#include <cstdio>
#include <iterator>

namespace GUI::Agent {

using namespace ui;

namespace {
std::string hms(int c) {
  char b[16];
  std::snprintf(b, sizeof(b), "%02d:%02d:%02d", c / 3600, c / 60 % 60, c % 60);
  return b;
}
} // namespace

void BookRes::draw() const {
  draw_head();
  char b[320];
  if (traded)
    std::snprintf(b, sizeof(b), "逐笔 %zu 条 · 开 %.2f 高 %.2f 低 %.2f 收 %.2f · 成交额 %.0f 万 · 大单门槛 %.1f 万%s", orders, open, high, low, close, amt / 1e4, big / 1e4,
                  big_given ? " (给定)" : " (99 分位)");
  else
    std::snprintf(b, sizeof(b), "逐笔 %zu 条 · 无成交 · 大单门槛 %.1f 万", orders, big / 1e4);
  wrapped(b);
  std::snprintf(b, sizeof(b), "窗口: 主买 %.1f / 主卖 %.1f · 挂 买 %.1f / 卖 %.1f · 撤 买 %.1f / 卖 %.1f (万)", taker[0] / 1e4, taker[1] / 1e4, maker[0] / 1e4, maker[1] / 1e4,
                cancel[0] / 1e4, cancel[1] / 1e4);
  wrapped(b);
  line_plot("逐秒成交价##price", price, 150.0f, "价");
  line_plot("累计主动买 − 主动卖 (万)##net", net, 110.0f, "万");

  heading("逐时表 (每行 " + std::to_string(step) + " 秒)");
  std::vector<std::string> hs{"起", "末价", "买一", "卖一", "主买", "主卖", "挂买", "挂卖", "撤买", "撤卖", "买五档", "卖五档"};
  if (call)
    hs.push_back("参考价"), hs.push_back("匹配"), hs.push_back("未匹配");
  if (grid("##rows", hs)) {
    for (const Row &z : rows) {
      ImGui::TableNextRow();
      cell(hms(z.clock)), cell_num(z.last, "%.2f"), cell_num(z.bid > 0 ? z.bid : NAN, "%.2f"), cell_num(z.ask > 0 ? z.ask : NAN, "%.2f");
      for (const double v : {z.taker[0], z.taker[1], z.maker[0], z.maker[1], z.cancel[0], z.cancel[1], z.depth[0], z.depth[1]})
        cell_num(v / 1e4, "%.1f");
      if (call)
        cell_num(z.ref > 0 ? z.ref : NAN, "%.2f"), cell_num(z.matched / 1e4, "%.1f"), cell_num(z.imbalance / 1e4, "%+.1f", true);
    }
    ImGui::EndTable();
  }

  heading("大单事件 (共 " + std::to_string(events) + " 起)");
  if (grid("##kinds", {"事件", "买 起", "买 额 万", "卖 起", "卖 额 万"})) {
    for (size_t k = 0; k < std::size(kBookKinds); ++k) {
      ImGui::TableNextRow();
      cell(kBookKinds[k]), cell(std::to_string(count[k][0])), cell_num(amount[k][0] / 1e4, "%.1f"), cell(std::to_string(count[k][1])), cell_num(amount[k][1] / 1e4, "%.1f");
    }
    ImGui::EndTable();
  }
  if (!top.empty() && grid("##top", {"时刻", "事件", "侧", "价", "距盘口", "额 万", "寿命 秒"})) {
    for (const Event &e : top) {
      ImGui::TableNextRow();
      std::snprintf(b, sizeof(b), "%s.%02d", hms(e.clock).c_str(), e.cs);
      cell(b), cell(kBookKinds[e.kind]), cell(e.side == 0 ? "买" : "卖");
      if (e.kind == 3)
        std::snprintf(b, sizeof(b), "%.2f→%.2f", e.first, e.price), cell(b), cell("吃 " + std::to_string(e.levels) + " 价位");
      else
        cell_num(e.price, "%.2f"), cell_num(e.best > 0 ? std::round((e.side == 0 ? e.best - e.price : e.price - e.best) * 100.0) : NAN, "%+.0f");
      cell_num(e.amount / 1e4, "%.1f"), cell_num(e.life >= 0 ? e.life : NAN, "%.1f");
    }
    ImGui::EndTable();
  }

  heading("全簿剖面 @ " + hms(at) + " (全簿 买 " + std::to_string(static_cast<long>(book_bid / 1e4)) + " / 卖 " + std::to_string(static_cast<long>(book_ask / 1e4)) + " 万)");
  if (profile.empty()) {
    ImGui::TextDisabled("簿空");
    return;
  }
  std::vector<std::string> names;
  std::vector<double> vals;
  for (const Level &l : profile) {
    std::snprintf(b, sizeof(b), "%s%d %.2f", l.bid ? "买" : "卖", l.rank, l.price);
    names.emplace_back(b), vals.push_back((l.bid ? 1.0 : -1.0) * l.amount / 1e4);
  }
  bars_plot("挂单额 (万; 卖为负)##book", names, vals, 30.0f + 16.0f * static_cast<float>(profile.size()));
}

} // namespace GUI::Agent
