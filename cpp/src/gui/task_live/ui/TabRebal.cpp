#include "gui/task_live/ui/TabRebal.hpp"

#include "gui/util/Chart.hpp"
#include "gui/util/Tip.hpp"

#include "imgui.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdlib>

namespace GUI::Live {

namespace {

using live::strf;

const ImVec4 kRed = kUp, kHot = kStratCol;
inline ImVec4 grey() { return ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled); }
void text_col(const ImVec4 &c, const std::string &s) { ImGui::TextColored(c, "%s", s.c_str()); }

constexpr ImGuiTableFlags kFlags = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg;

// 能成单的卖出股数里离 x 最近的
int snap(const live::rebal::Hold &h, double unit, double x) {
  const int first = static_cast<int>(std::ceil(std::max<double>(h.lot, unit / h.px) / h.step)) * h.step;
  const int last = static_cast<int>(std::ceil(static_cast<double>(h.avail) / h.step)) * h.step - h.step;
  int best = 0;
  const auto take = [&](int c) {
    if (std::abs(c - x) <= std::abs(best - x))
      best = c;
  };
  if (h.avail * h.px >= unit)
    take(h.avail);
  if (first <= last)
    take(std::min(last, std::max(first, static_cast<int>(std::lround(x / h.step)) * h.step)));
  return best;
}

void config(LiveHub &hub, RebalState &st) {
  const live::rebal::Cfg &cfg = hub.cfg();
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 12);
  if (ImGui::BeginCombo("名单", cfg.list.empty() ? "(无)" : cfg.list.c_str())) {
    for (const std::string &n : hub.lists())
      if (ImGui::Selectable(n.c_str(), n == cfg.list))
        hub.set_list(n);
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    tip("config/local/live/lists/<名>.csv, 名为字母数字下划线; 每行首列一个 6 位代码 (可有 code 表头), 行序即排名; 新在前\n选中即重读");
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 只", hub.codes().size());
  if (!hub.list_err().empty()) {
    ImGui::SameLine();
    text_col(kRed, "✗ " + hub.list_err());
  }
  ImGui::SameLine();
  double unit = cfg.unit;
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 6);
  if (ImGui::InputDouble("最小调仓单元", &unit, 0, 0, "%.0f", ImGuiInputTextFlags_EnterReturnsTrue))
    hub.set_unit(std::max(0.0, unit));
  if (ImGui::IsItemHovered())
    tip("元, 不足即不做; 回车生效");
  ImGui::SameLine();
  int pct = static_cast<int>(std::lround(cfg.pct));
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 8);
  if (ImGui::SliderInt("行使比例", &pct, 0, 100, "%d%%"))
    hub.set_pct(pct);
  ImGui::SameLine();
  if (hub.terms())
    ImGui::Text("两融条款 ✓ 今日 %zu 只", *hub.terms());
  else
    text_col(kRed, "两融条款 今日未拉");
  ImGui::SameLine();
  ImGui::BeginDisabled(!hub.connected() || hub.busy());
  if (ImGui::Button("更新两融名单"))
    hub.pull();
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("客户端重查融资标的与担保品两张全表 (慢, 期间独占客户端界面); 当日落盘, 当日内不必重拉");
  ImGui::SameLine();
  const auto &p = hub.plan().plan;
  ImGui::BeginDisabled(!hub.connected() || !p || p->orders.empty() || hub.busy() || hub.ordering());
  if (ImGui::Button("执行调仓"))
    st.ask = true;
  ImGui::EndDisabled();
}

void confirm(LiveHub &hub, RebalState &st) {
  if (st.ask) {
    ImGui::OpenPopup("确认调仓");
    st.ask = false;
  }
  if (!ImGui::BeginPopupModal("确认调仓", nullptr, ImGuiWindowFlags_AlwaysAutoResize))
    return;
  const auto &p = hub.plan().plan;
  if (!p) {
    ImGui::TextDisabled("试算已失效");
  } else {
    int sell = 0, margin = 0, plain = 0;
    for (const live::rebal::Order &o : p->orders)
      (o.s < 0 ? sell : o.credit == live::Credit::Margin ? margin
                                                         : plain)++;
    ImGui::Text("按当前试算执行调仓: 卖 %d 单、融资买 %d 单、现金买 %d 单, 行使 %s, 成本约 %.0f 元?", sell, margin, plain, wan(p->use).c_str(), p->cost);
  }
  ImGui::BeginDisabled(!p || hub.busy() || hub.ordering());
  if (ImGui::Button("执行")) {
    hub.run();
    ImGui::CloseCurrentPopup();
  }
  ImGui::EndDisabled();
  ImGui::SameLine();
  if (ImGui::Button("取消"))
    ImGui::CloseCurrentPopup();
  ImGui::EndPopup();
}

void info(const live::rebal::Plan &p) {
  ImGui::TextWrapped("购买力 %s · 本次行使 %s (融资 %s) · 成本 %.0f 元 · 入选 %d / %d 只, 每只目标 %s · 卖后现金 %s / 可用保证金 %s",
                     wan(p.power).c_str(), wan(p.use).c_str(), wan(p.financed).c_str(), p.cost, p.picked, p.buyable, wan(p.V).c_str(), wan(p.C).c_str(),
                     wan(p.X).c_str());
}

// 账户对照: 现 / 调仓后, 调仓后与现不同的格标色
void sheet(const live::rebal::Plan *p) {
  constexpr const char *kHeads[] = {"", "总资产", "净资产", "负债", "现金", "可用保证金", "可融余量", "杠杆", "维持担保"};
  const auto cells = [](const live::rebal::Sheet &s) {
    const auto opt = [](const std::optional<double> &v, auto f) { return v ? f(*v) : std::string(); };
    return std::array<std::string, 8>{wan(s.assets), wan(s.net), wan(s.debt), wan(s.cash), opt(s.X, wan), wan(s.L), strf("%.2f", s.lev),
                                      opt(s.maint, [](double m) { return strf("%.0f%%", m * 100); })};
  };
  if (!ImGui::BeginTable("##sheet", 9, kFlags))
    return;
  for (const char *h : kHeads)
    ImGui::TableSetupColumn(h);
  ImGui::TableHeadersRow();
  const std::array<std::string, 8> now = p ? cells(p->now) : std::array<std::string, 8>{};
  const std::array<std::string, 8> exp = p ? cells(p->exp) : std::array<std::string, 8>{};
  for (int r = 0; r < 2; ++r) {
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted(r ? "调仓后" : "现");
    for (int c = 0; c < 8; ++c) {
      ImGui::TableSetColumnIndex(c + 1);
      const std::string &v = (r ? exp : now)[c];
      if (v.empty())
        ImGui::TextDisabled("—");
      else if (r && v != now[c])
        text_col(kHot, v);
      else
        ImGui::TextUnformatted(v.c_str());
    }
  }
  ImGui::EndTable();
}

// 持仓行: 融、市值、卖出滑杆与其面值 / 成本
void holds(LiveHub &hub, const live::rebal::Plan &p) {
  const double unit = hub.cfg().unit;
  ImGui::SeparatorText("持仓 (调仓先卖的拖卖出)");
  if (!ImGui::BeginTable("##holds", 7, kFlags))
    return;
  for (const char *h : {"代码", "名称", "融", "市值", "卖出", "面值", "成本"})
    ImGui::TableSetupColumn(h);
  ImGui::TableHeadersRow();
  double amount = 0, cost = 0;
  int orders = 0;
  for (const live::rebal::Hold &h : p.holds) {
    ImGui::TableNextRow();
    ImGui::PushID(h.code.c_str());
    ImGui::TableSetColumnIndex(0);
    ImGui::TextUnformatted(h.code.c_str());
    ImGui::TableSetColumnIndex(1);
    ImGui::TextUnformatted(h.name.c_str());
    ImGui::TableSetColumnIndex(2);
    if (h.financeable)
      text_col(kHot, "R");
    ImGui::TableSetColumnIndex(3);
    ImGui::TextUnformatted(wan(h.value).c_str());
    ImGui::TableSetColumnIndex(4);
    const auto i = hub.cfg().sells.find(h.code);
    int n = std::min(i == hub.cfg().sells.end() ? 0 : i->second, h.avail);
    ImGui::BeginDisabled(snap(h, unit, h.avail) == 0 || hub.busy());
    ImGui::SetNextItemWidth(ImGui::GetFontSize() * 8);
    if (ImGui::SliderInt("##sell", &n, 0, h.avail, ""))
      hub.set_sell(h.code, snap(h, unit, n));
    ImGui::EndDisabled();
    ImGui::TableSetColumnIndex(5);
    if (!n)
      ImGui::TextDisabled("不卖");
    else
      ImGui::Text("%s%s", n == h.avail ? "全 " : "", wan(n * h.px).c_str());
    ImGui::TableSetColumnIndex(6);
    if (n && n != std::min(h.sell, h.avail))
      ImGui::TextDisabled("…");
    else if (n && h.order) {
      ImGui::Text("%.0f", h.order->slip + h.order->fee);
      if (ImGui::IsItemHovered())
        tip("滑点 %.0f 元 · 费 %.0f 元", h.order->slip, h.order->fee);
      amount += h.order->amount;
      cost += h.order->slip + h.order->fee;
      ++orders;
    } else if (n)
      text_col(grey(), h.skip);
    ImGui::PopID();
  }
  if (orders) {
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("卖 %d 单", orders);
    ImGui::TableSetColumnIndex(5);
    ImGui::TextUnformatted(wan(amount).c_str());
    ImGui::TableSetColumnIndex(6);
    ImGui::Text("%.0f", cost);
  }
  ImGui::EndTable();
}

// 名单行: 排名、可买与否、条款、补多少、融资 / 现金两腿
void targets(const live::rebal::Plan &p) {
  ImGui::SeparatorText("名单");
  if (!ImGui::BeginTable("##targets", 10, kFlags))
    return;
  for (const char *h : {"排名", "代码", "名称", "", "保证金比例", "折算率", "现市值", "补", "融资买入", "现金买入"})
    ImGui::TableSetupColumn(h);
  ImGui::TableHeadersRow();
  const auto leg = [](const live::rebal::Target &t, live::Credit c) {
    const auto l = std::find_if(t.legs.begin(), t.legs.end(), [&](const live::rebal::Leg &x) { return x.credit == c; });
    if (l == t.legs.end())
      return;
    if (l->order)
      ImGui::Text("%.0f股 · %s", l->order->qty, wan(l->order->amount).c_str());
    else if (!l->skip.empty())
      text_col(grey(), strf("%s (%s)", l->skip.c_str(), wan(l->want).c_str()));
  };
  for (const live::rebal::Target &t : p.targets) {
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("%d", t.rank);
    ImGui::TableSetColumnIndex(1);
    ImGui::TextUnformatted(t.code.c_str());
    ImGui::TableSetColumnIndex(2);
    ImGui::TextUnformatted(t.name.c_str());
    ImGui::TableSetColumnIndex(3);
    if (!t.why.empty())
      text_col(kRed, t.why);
    else if (t.financeable)
      text_col(kHot, "融资标的");
    else
      ImGui::TextUnformatted("担保品");
    ImGui::TableSetColumnIndex(4);
    if (t.ratio)
      ImGui::Text("%.2f", *t.ratio);
    ImGui::TableSetColumnIndex(5);
    if (t.rate)
      ImGui::Text("%.2f", *t.rate);
    ImGui::TableSetColumnIndex(6);
    if (t.held)
      ImGui::TextUnformatted(wan(*t.held).c_str());
    ImGui::TableSetColumnIndex(7);
    if (!t.skip.empty())
      text_col(grey(), t.skip);
    else if (t.need)
      ImGui::TextUnformatted(wan(*t.need).c_str());
    ImGui::TableSetColumnIndex(8);
    leg(t, live::Credit::Margin);
    ImGui::TableSetColumnIndex(9);
    leg(t, live::Credit::Plain);
  }
  ImGui::EndTable();
}

void task(LiveHub &hub) {
  const auto &t = hub.task();
  if (!t)
    return;
  ImGui::SeparatorText("事务");
  text_col(t->st == JobSt::Err ? kRed : t->st == JobSt::Run ? kHot
                                                            : ImGui::GetStyleColorVec4(ImGuiCol_Text),
           strf("%s %s · %s%s%s %s", t->t.c_str(), t->name.c_str(), job_name(t->st), t->live.empty() ? "" : " · ", t->live.c_str(), t->msg.c_str()));
  ImGui::TextDisabled("%s", t->file.c_str());
  ImGui::BeginChild("##log", ImVec2(0, ImGui::GetTextLineHeightWithSpacing() * 12), true);
  for (const LogLine &l : t->log)
    ImGui::Text("%8.2f %s %s", l.t, l.kind.c_str(), l.text.c_str());
  if (ImGui::GetScrollY() >= ImGui::GetScrollMaxY())
    ImGui::SetScrollHereY(1.0f);
  ImGui::EndChild();
}

} // namespace

void RenderTabRebal(LiveHub &hub, RebalState &st) {
  if (!hub.connected())
    ImGui::TextDisabled("先在 Board 连接客户端");
  else
    hub.auto_pull();
  config(hub, st);
  confirm(hub, st);
  const PlanView &v = hub.plan();
  if (!v.err.empty())
    text_col(kRed, "✗ " + v.err);
  else if (!v.plan)
    ImGui::TextDisabled("%s", v.wait.empty() ? "(等试算)" : v.wait.c_str());
  else
    info(*v.plan);
  sheet(v.plan ? &*v.plan : nullptr);
  task(hub);
  if (v.plan) {
    holds(hub, *v.plan);
    targets(*v.plan);
  }
}

} // namespace GUI::Live
