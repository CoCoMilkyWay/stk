#include "gui/task_live/ui/TabBoard.hpp"

#include "gui/util/Chart.hpp"
#include "gui/util/Tip.hpp"
#include "live/Clients.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <map>
#include <set>
#include <span>
#include <vector>

namespace GUI::Live {

namespace {

using live::strf;

const ImVec4 kGreen = kDn, kRed = kUp, kHot = kStratCol, kAcc = kHead;
inline ImVec4 side_col(int s) { return s > 0 ? kGreen : kRed; } // 买绿卖红
inline ImVec4 grey() { return ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled); }

constexpr ImGuiTableFlags kFlags = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_RowBg | ImGuiTableFlags_BordersInnerV |
                                   ImGuiTableFlags_BordersOuterH | ImGuiTableFlags_NoHostExtendX;
constexpr size_t kOrderLines = 2; // 下单组的状态行: 最近几单

std::string slip_txt(const std::optional<double> &v, bool full = true) { return v ? strf("%s%.1fp", full ? "" : ">", *v) : "-"; }
std::optional<ImVec4> slip_col(const std::optional<double> &v) {
  if (!v)
    return std::nullopt;
  return *v < kSlipOk ? kGreen : *v < kSlipHi ? kHot
                                              : kRed;
}
bool quick(const live::Slip &x) { return x.full && x.pct && *x.pct < kSlipOk; }

std::string wan_pnl(double v) { return wan(v, true); }

void text_col(const ImVec4 &c, const std::string &s) { ImGui::TextColored(c, "%s", s.c_str()); }
void text_opt(const std::optional<double> &v, const char *fmt) {
  if (v)
    ImGui::Text(fmt, *v);
}
void pnl_opt(const std::optional<double> &v, const std::string &s) {
  if (v)
    text_col(tint(*v, 0), s);
}
void masked() { ImGui::TextDisabled("%s", kMask); } // 隐私下遮住的格: 不出悬停提示
std::string sec_txt(const live::Sec &s, bool priv) { return priv ? kMask : s.code + " " + s.name; }

// 占总市值的比例条; 悬停看金额与占比
void bar(const std::optional<double> &v, double mv, const ImVec4 &col) {
  if (!v || mv <= 0)
    return;
  const float f = static_cast<float>(std::clamp(*v / mv, 0.0, 1.0));
  ImGui::PushStyleColor(ImGuiCol_PlotHistogram, col);
  ImGui::ProgressBar(f, ImVec2(ImGui::GetFontSize() * 2.5f, ImGui::GetTextLineHeight()), "");
  ImGui::PopStyleColor();
  if (ImGui::IsItemHovered())
    tip("%s · %.0f%%", wan(*v).c_str(), f * 100);
}

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

// 一单的步骤条: 已过绿 / 当前橙 / 失败红 / 未到灰
void order_line(const OrderJob &j, bool priv) {
  ImGui::Text("%s", j.t.c_str());
  ImGui::SameLine();
  text_col(side_col(j.side), strf("%s%s", live::side_name(j.side), j.label.c_str()));
  ImGui::SameLine();
  ImGui::Text("%s %s ·", sec_txt(j.sec, priv).c_str(), j.detail.c_str());
  for (size_t i = 0; i < j.steps.size(); ++i) {
    const ImVec4 c = i < j.at || j.st == JobSt::Ok ? kGreen : i > j.at         ? grey()
                                                          : j.st == JobSt::Err ? kRed
                                                                               : kHot;
    ImGui::SameLine();
    text_col(c, j.steps[i] + (i + 1 < j.steps.size() ? " ›" : " ·"));
  }
  ImGui::SameLine();
  text_col(j.st == JobSt::Ok ? kGreen : j.st == JobSt::Err ? kRed
                                                           : kHot,
           strf("%s %s", job_name(j.st), j.msg.c_str()));
}

// 刷新进度两行: 各样读于几时 · 行情时刻 · 接入的客户端; 时有时无的 (接入断、刷新中、上轮失败), 都没有即「空闲」
void progress(LiveHub &hub) {
  const live::Refresh &g = hub.refresh();
  const auto &st = hub.state();
  std::string at;
  (st ? *st : live::State{}).each([&](const char *n, const std::string &t) { at += strf("%s%s %s", at.empty() ? "" : " · ", n, t.empty() ? "—" : t.c_str()); });
  ImGui::Text("%s · 读于 %s · 行情 %s · %s", hub.broker().name(), at.c_str(), hub.quote_time().empty() ? "—" : hub.quote_time().c_str(),
              st && !st->who.empty() ? st->who.c_str() : "未接入");
  bool any = false;
  const auto sep = [&] {
    if (any)
      ImGui::SameLine();
    any = true;
  };
  if (!g.down.empty()) {
    sep();
    text_col(kRed, "✗ 接入: " + g.down);
  }
  if (!g.step.empty()) {
    sep();
    text_col(kHot, "刷新中: " + g.step + "…");
  }
  if (!g.err.empty()) {
    sep();
    text_col(kRed, "✗ 上轮刷新失败");
    if (ImGui::IsItemHovered())
      tip("%s", g.err.c_str());
  }
  if (!any)
    ImGui::TextDisabled("空闲");
}

void data_ctl(LiveHub &hub, BoardState &st) {
  live::Broker &b = hub.broker();
  if (ImGui::SmallButton("刷新"))
    b.request();
  if (ImGui::IsItemHovered())
    tip("客户端依次重查当日成交、持仓、挂单、信用资产与融资负债, 各样查完即更新");
  ImGui::SameLine();
  ImGui::TextUnformatted("定时");
  ImGui::SameLine();
  int every = st.every.value_or(static_cast<int>(hub.refresh().every));
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 3);
  if (ImGui::InputInt("s##every", &every, 0, 0))
    st.every = every;
  if (ImGui::IsItemDeactivatedAfterEdit())
    b.every(std::max(0, every));
  if (!ImGui::IsItemActive())
    st.every.reset();
  if (ImGui::IsItemHovered())
    tip("定时刷新周期 [s], 0 = 不定时; 回车或点别处生效");
  ImGui::SameLine(0, ImGui::GetFontSize());
  ImGui::BeginDisabled(hub.busy());
  if (ImGui::SmallButton("更新两融名单"))
    hub.pull();
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("客户端重查融资标的与担保品两张全表 (慢, 期间独占客户端界面); 当日落盘, 当日内不必重拉\n本页打开时当日还没有即后台自动拉 (每天至多一次)");
  ImGui::SameLine();
  if (hub.terms())
    ImGui::Text("✓ 今日 %zu 只", *hub.terms());
  else
    text_col(kRed, "今日未拉");
  ImGui::SameLine(0, ImGui::GetFontSize());
  bool priv = hub.priv();
  if (ImGui::Checkbox("隐私", &priv))
    hub.set_priv(priv);
  if (ImGui::IsItemHovered())
    tip("遮住代码、名称与账户金额 (连同其悬停提示与事务日志), 比例、只数、下单与卖出各列照显");
}

void order_ctl(LiveHub &hub, BoardState &st) {
  int px = static_cast<int>(hub.cfg().px);
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 7);
  if (ImGui::Combo("委托价", &px, live::kPxModeNames, IM_ARRAYSIZE(live::kPxModeNames)))
    hub.set_px(static_cast<live::PxMode>(px));
  if (ImGui::IsItemHovered())
    tip("持仓行下单与调仓共用\n• 扫档: 对手盘累计恰好吃满该额那档\n• 对手价 / 对手价 ±1%%: 对手一档, 后者再朝成交方向偏 1%%\n• 本方价: 本方一档");
  ImGui::SameLine(0, ImGui::GetFontSize());
  ImGui::TextUnformatted("买入");
  ImGui::SameLine();
  if (ImGui::RadioButton("普通", !st.margin))
    st.margin = false;
  ImGui::SameLine();
  if (ImGui::RadioButton("融资", st.margin))
    st.margin = true;
  if (ImGui::IsItemHovered())
    tip("买入方式只管持仓行买入, 卖出一律担保品卖出");
  ImGui::SameLine(0, ImGui::GetFontSize());
  ImGui::Checkbox("解锁买卖", &st.armed);
}

void rebal_ctl(LiveHub &hub, BoardState &st) {
  const live::rebal::Cfg &cfg = hub.cfg();
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 10);
  if (ImGui::BeginCombo("名单", cfg.list.empty() ? "(无)" : cfg.list.c_str())) {
    for (const std::string &n : hub.lists())
      if (ImGui::Selectable(n.c_str(), n == cfg.list))
        hub.set_list(n);
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    tip("output/config/live/lists/<名>.csv, 名为字母数字下划线; 每行首列一个 6 位代码 (可有 code 表头), 行序即排名; 新在前\n选中即重读");
  ImGui::SameLine();
  ImGui::TextDisabled("%zu 只", hub.codes().size());
  if (!hub.list_err().empty()) {
    ImGui::SameLine();
    text_col(kRed, "✗ " + hub.list_err());
  }
  ImGui::SameLine(0, ImGui::GetFontSize());
  double unit = st.unit.value_or(cfg.unit);
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 5);
  if (ImGui::InputDouble("最小单元", &unit, 0, 0, "%.0f"))
    st.unit = unit;
  if (ImGui::IsItemDeactivatedAfterEdit())
    hub.set_unit(std::max(0.0, unit));
  if (!ImGui::IsItemActive())
    st.unit.reset();
  if (ImGui::IsItemHovered())
    tip("元, 不足此额的单不做, 新开仓至少买这么多; 回车或点别处生效");
  ImGui::SameLine(0, ImGui::GetFontSize());
  int pct = static_cast<int>(std::lround(cfg.pct));
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 7);
  if (ImGui::SliderInt("行使", &pct, 0, 100, "%d%%"))
    hub.set_pct(pct);
  if (ImGui::IsItemHovered())
    tip("本次用掉现金、可用保证金、可融余量的比例");
  ImGui::SameLine(0, ImGui::GetFontSize());
  const auto &p = hub.plan().plan;
  ImGui::BeginDisabled(!p || p->orders.empty() || hub.busy() || hub.ordering());
  if (ImGui::SmallButton("执行调仓"))
    st.run = true;
  ImGui::EndDisabled();
}

// 试算摘要一行: 出错 / 等什么 / 购买力与行使
void plan_line(LiveHub &hub) {
  const PlanView &v = hub.plan();
  if (!v.err.empty())
    return text_col(kRed, "✗ " + v.err);
  if (!v.plan)
    return ImGui::TextDisabled("%s", v.wait.empty() ? "(等试算)" : v.wait.c_str());
  const live::rebal::Plan &p = *v.plan;
  const auto w = [&](double x) { return hub.priv() ? std::string(kMask) : wan(x); };
  ImGui::Text("购买力 %s · 本次行使 %s (融资 %s) · 成本 %.0f 元 · 入选 %d / %d 只, 每只目标 %s · 卖后现金 %s / 可用保证金 %s", w(p.power).c_str(),
              w(p.use).c_str(), w(p.financed).c_str(), p.cost, p.picked, p.buyable, w(p.V).c_str(), w(p.C).c_str(), w(p.X).c_str());
}

// 事务一行: 时刻 名称 · 状态 · 实时进度 结果
void task_line(LiveHub &hub) {
  const auto &t = hub.task();
  if (!t)
    return ImGui::TextDisabled("无事务");
  text_col(t->st == JobSt::Err ? kRed : t->st == JobSt::Run ? kHot
                                                            : ImGui::GetStyleColorVec4(ImGuiCol_Text),
           strf("%s %s · %s%s%s %s", t->t.c_str(), t->name.c_str(), job_name(t->st), t->live.empty() ? "" : " · ", t->live.c_str(), t->msg.c_str()));
}

// 账户对照: 现 / 调仓后, 调仓后与现不同的格标色
void sheet(const live::rebal::Plan *p, bool priv) {
  constexpr const char *kHeads[] = {"", "总资产", "净资产", "负债", "现金", "可用保证金", "可融余量", "杠杆", "维持担保"};
  const auto cells = [](const live::rebal::Sheet &s) {
    const auto opt = [](const std::optional<double> &v, auto f) { return v ? f(*v) : std::string(); };
    return std::array<std::string, 8>{wan(s.assets), wan(s.net), wan(s.debt), wan(s.cash), opt(s.X, [](double x) { return wan(x); }), wan(s.L), strf("%.2f", s.lev),
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
      const std::string &shown = priv ? kMask : v;
      if (v.empty())
        ImGui::TextDisabled("—");
      else if (r && v != now[c])
        text_col(kHot, shown);
      else
        ImGui::TextUnformatted(shown.c_str());
    }
  }
  ImGui::EndTable();
}

// 控件分组: 一行一组, 左格组名, 右格控件行与定高的状态行
void groups(LiveHub &hub, BoardState &st) {
  if (!ImGui::BeginTable("##groups", 2, ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_BordersInnerH | ImGuiTableFlags_BordersOuterH))
    return;
  ImGui::TableSetupColumn("", ImGuiTableColumnFlags_WidthFixed);
  ImGui::TableSetupColumn("", ImGuiTableColumnFlags_WidthStretch);
  const auto row = [](const char *name) {
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::TextDisabled("%s", name);
    ImGui::TableSetColumnIndex(1);
  };
  row("数据");
  data_ctl(hub, st);
  progress(hub);
  row("下单");
  order_ctl(hub, st);
  for (size_t i = 0; i < kOrderLines; ++i)
    if (i < hub.orders().size())
      order_line(hub.orders()[i], hub.priv());
    else if (i == 0)
      ImGui::TextDisabled("还没下过单");
    else
      ImGui::TextUnformatted("");
  row("调仓");
  rebal_ctl(hub, st);
  plan_line(hub);
  task_line(hub);
  row("账户");
  const auto &p = hub.plan().plan;
  sheet(p ? &*p : nullptr, hub.priv());
  ImGui::EndTable();
}

// 持仓表上方的提示行 (定高): 帧没出的原因、无持仓、持仓表里没有的代码的挂单
void info_line(LiveHub &hub, const std::set<std::string> &held) {
  const BoardView &v = hub.board();
  if (!v.err.empty())
    return text_col(kRed, "✗ " + v.err);
  if (!v.wait.empty())
    return ImGui::TextDisabled("%s", v.wait.c_str());
  if (v.frame->rows.empty())
    return ImGui::TextDisabled("无持仓");
  const auto &orders = hub.state()->orders;
  std::string s;
  int n = 0;
  if (orders)
    for (const live::Pending &o : *orders)
      if (!held.count(o.code)) {
        ++n;
        s += strf("%s%s %s %.0f股 @%.3f %s", s.empty() ? "" : "、", live::side_name(o.side), sec_txt(o, hub.priv()).c_str(), o.qty - o.filled, o.price,
                  o.status.c_str());
      }
  if (!n)
    return ImGui::TextUnformatted("");
  ImGui::Text("表外挂单 %d 笔: %s", n, s.c_str());
  if (ImGui::IsItemHovered())
    tip("%s", s.c_str());
}

// 状态列: 今日成交 ●、挂单 ○, 按方向着色
struct Flows {
  std::map<std::string, std::vector<const live::Fill *>> fills;
  std::map<std::string, std::vector<const live::Pending *>> orders;
};
Flows flows_of(const live::State &s) {
  Flows f;
  if (s.fills)
    for (const live::Fill &x : s.fills->rows)
      f.fills[x.code].push_back(&x);
  if (s.orders)
    for (const live::Pending &x : *s.orders)
      f.orders[x.code].push_back(&x);
  return f;
}
void flow_cell(const Flows &f, const std::string &code, bool priv) {
  const auto fi = f.fills.find(code);
  const auto oi = f.orders.find(code);
  if (priv) {
    if (fi != f.fills.end() || oi != f.orders.end())
      masked();
    return;
  }
  std::string tips;
  bool any = false;
  const auto mark = [&](const char *m, auto &xs) {
    for (const int s : live::kSides)
      if (std::any_of(xs.begin(), xs.end(), [&](auto *x) { return x->side == s; })) {
        if (any)
          ImGui::SameLine(0, 0);
        text_col(side_col(s), m);
        any = true;
      }
  };
  if (fi != f.fills.end()) {
    mark("●", fi->second);
    tips += strf("成交 %zu 笔\n", fi->second.size());
    for (const live::Fill *x : fi->second)
      tips += strf("  %s %s %.0f股 @%.3f\n", live::side_name(x->side), x->time.c_str(), x->qty, x->price);
  }
  if (oi != f.orders.end()) {
    mark("○", oi->second);
    tips += strf("挂单 %zu 笔\n", oi->second.size());
    for (const live::Pending *x : oi->second)
      tips += strf("  %s %s %.0f%s股 @%.3f %s\n", live::side_name(x->side), x->time.c_str(), x->qty - x->filled,
                   x->filled ? strf("/%.0f", x->qty).c_str() : "", x->price, x->status.c_str());
  }
  if (any && ImGui::IsItemHovered())
    tip("%s", tips.c_str());
}

// 下单按钮: 一列滑点一个, 填充 = 该额的滑点色
void order_cell(LiveHub &hub, BoardState &st, const live::board::Row &r, bool can, bool &ask) {
  if (r.closed)
    return ImGui::TextDisabled("今日清仓");
  const auto side_tip = [&](int s) {
    std::string t = strf("%s 滑点", live::side_name(s));
    if (r.slips) {
      for (size_t i = 0; i < live::kSlipCols.size(); ++i)
        if (live::kSlipCols[i].s == s)
          t += " " + wan(live::kSlipCols[i].n) + " " + slip_txt((*r.slips)[i].pct, (*r.slips)[i].full);
    } else
      t += r.auction ? " 竞价中" : " 无行情";
    if (r.levels) {
      t += "\n价位";
      for (size_t i = 0; i < live::kLevelCols.size(); ++i)
        if (live::kLevelCols[i].s == s)
          t += strf(" %s%.0f%% %.3f", s > 0 ? "+" : "-", live::kLevelCols[i].n, (*r.levels)[i]);
    }
    if (r.weight)
      t += strf("\n占比 %.2f%%", *r.weight);
    return t;
  };
  ImGui::BeginDisabled(!can);
  ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(ImGui::GetFontSize() * 0.12f, ImGui::GetStyle().FramePadding.y));
  for (size_t i = 0; i < live::kSlipCols.size(); ++i) {
    const live::Col c = live::kSlipCols[i];
    if (i)
      ImGui::SameLine(0, i == live::kSlipCols.size() / 2 ? ImGui::GetFontSize() * 0.3f : 1);
    const live::Slip x = r.slips ? (*r.slips)[i] : live::Slip{};
    const std::optional<ImVec4> fill = slip_col(x.pct);
    ImGui::PushStyleColor(ImGuiCol_Text, side_col(c.s));
    if (fill)
      ImGui::PushStyleColor(ImGuiCol_Button, ImVec4(fill->x, fill->y, fill->z, 0.45f));
    const bool hit = ImGui::SmallButton(strf("%s##o%zu", wan(c.n).c_str(), i).c_str());
    ImGui::PopStyleColor(fill ? 2 : 1);
    if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
      tip("%s", side_tip(c.s).c_str());
    if (!hit)
      continue;
    const live::Credit credit = c.s > 0 && st.margin ? live::Credit::Margin : live::Credit::Plain;
    if (quick(x))
      hub.submit(r.code, c.s, c.n, credit);
    else {
      st.ask = BoardState::Ask{r.code, r.name, r.slips ? slip_txt(x.pct, x.full) : r.auction ? "竞价中"
                                                                                             : "无行情",
                               c.s, c.n, credit};
      ask = true;
    }
  }
  ImGui::PopStyleVar();
  ImGui::EndDisabled();
}

void confirm(LiveHub &hub, BoardState &st, bool open) {
  if (open)
    ImGui::OpenPopup("确认下单");
  if (!ImGui::BeginPopupModal("确认下单", nullptr, ImGuiWindowFlags_AlwaysAutoResize))
    return;
  assert(st.ask);
  const BoardState::Ask &a = *st.ask;
  ImGui::Text("%s%s %s %s, 滑点 %s, 确认下单?", live::side_name(a.side), a.side > 0 ? strf(" (%s)", live::credit_name(a.credit)).c_str() : "",
              wan(a.size).c_str(), sec_txt({a.code, a.name}, hub.priv()).c_str(), a.slip.c_str());
  const bool can = !hub.ordering() && !hub.busy();
  ImGui::BeginDisabled(!can);
  if (ImGui::Button("下单")) {
    hub.submit(a.code, a.side, a.size, a.credit);
    st.ask.reset();
    ImGui::CloseCurrentPopup();
  }
  ImGui::EndDisabled();
  ImGui::SameLine();
  if (ImGui::Button("取消")) {
    st.ask.reset();
    ImGui::CloseCurrentPopup();
  }
  ImGui::EndPopup();
}

void confirm_run(LiveHub &hub, BoardState &st) {
  if (st.run) {
    ImGui::OpenPopup("确认调仓");
    st.run = false;
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
    ImGui::Text("按当前试算执行调仓: 卖 %d 单、融资买 %d 单、现金买 %d 单, 行使 %s, 成本约 %.0f 元?", sell, margin, plain,
                hub.priv() ? kMask : wan(p->use).c_str(), p->cost);
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

enum Column {
  C_CODE,
  C_POS_PCT,
  C_NAME,
  C_FLOW,
  C_FIN,
  C_ORDER,
  C_DAY_PCT,
  C_DAY,
  C_PNL_PCT,
  C_PNL,
  C_MV,
  C_MARGIN,
  C_SELLABLE,
  C_SELL,
  C_FACE,
  C_COST,
  C_COLLAT,
  C_COUNT
};
constexpr const char *kHeads[C_COUNT] = {"代码", "仓位%", "名称", "状态", "融", "买 / 卖", "当日%", "当日", "盈亏%",
                                         "盈亏", "市值", "融资", "可卖", "卖出", "面值", "成本", "折算率"};

ImGuiTableColumnFlags col_flags(int c) {
  switch (c) {
  case C_CODE:
  case C_NAME:
    return 0;
  case C_FLOW:
  case C_FIN:
  case C_ORDER:
  case C_SELL:
  case C_FACE:
  case C_COST:
    return ImGuiTableColumnFlags_NoSort;
  default:
    return ImGuiTableColumnFlags_PreferSortDescending;
  }
}

// 数值列的排序键; 融资 / 可卖按占总市值
std::optional<double> sort_key(const live::board::Row &r, int c) {
  const auto share = [&](const std::optional<double> &v) { return v && r.market_value.value_or(0) > 0 ? std::optional<double>(*v / *r.market_value) : std::nullopt; };
  switch (c) {
  case C_POS_PCT:
    return r.position_pct;
  case C_DAY_PCT:
    return r.day_pnl_pct;
  case C_DAY:
    return r.day_pnl;
  case C_PNL_PCT:
    return r.pnl_pct;
  case C_PNL:
    return r.pnl;
  case C_MV:
    return r.market_value;
  case C_MARGIN:
    return share(r.margin_value);
  case C_SELLABLE:
    return share(r.sellable_value);
  case C_COLLAT:
    return r.collateral;
  default:
    assert(false);
    return std::nullopt;
  }
}

// 表头点选排序 (三态, 不排 = 帧序); 今日清仓恒在后, 空值恒在有值后
std::vector<const live::board::Row *> sorted(const live::board::Frame &f) {
  std::vector<const live::board::Row *> rows;
  for (const live::board::Row &r : f.rows)
    rows.push_back(&r);
  const ImGuiTableSortSpecs *ss = ImGui::TableGetSortSpecs();
  if (!ss || !ss->SpecsCount)
    return rows;
  const int c = ss->Specs[0].ColumnIndex;
  const bool desc = ss->Specs[0].SortDirection == ImGuiSortDirection_Descending;
  std::stable_sort(rows.begin(), rows.end(), [&](const live::board::Row *a, const live::board::Row *b) {
    if (a->closed != b->closed)
      return b->closed;
    if (c == C_CODE || c == C_NAME) {
      const std::string &x = c == C_CODE ? a->code : a->name, &y = c == C_CODE ? b->code : b->name;
      return desc ? y < x : x < y;
    }
    const std::optional<double> x = sort_key(*a, c), y = sort_key(*b, c);
    if (!x || !y)
      return x && !y;
    return desc ? *y < *x : *x < *y;
  });
  return rows;
}

void head_tip(int c, const live::board::Frame &f) {
  switch (c) {
  case C_NAME:
    return tip("名称: 灰 = 今日清仓, 青 = 在当前调仓名单里");
  case C_FLOW:
    return tip("● 今日成交 · ○ 挂单, 买绿卖红; 悬停看逐笔");
  case C_FIN:
    return tip("R = 融资标的且融资状态正常 (取自当日两融条款, 试算未出即空)");
  case C_ORDER: {
    std::string s;
    for (size_t i = 0; i < live::kSlipCols.size(); ++i)
      s += strf("%s%s%s %s", s.empty() ? "" : " · ", live::side_name(live::kSlipCols[i].s), wan(live::kSlipCols[i].n).c_str(), slip_txt(f.slips[i]).c_str());
    return tip("滑点 p = 万分之一, 正 = 成本; 按钮填充 绿 < %.0fp ≤ 黄 < %.0fp ≤ 红, 非绿的点了先确认\n持仓按占比加权 %s", kSlipOk, kSlipHi, s.c_str());
  }
  case C_DAY:
  case C_DAY_PCT:
    return tip("当日盈亏: 按成交价自算 (毛, 不扣费); 与持仓的今买 / 今卖对不上即空\n"
               "合计括号 = 当日盈亏 / 昨净资产 (现金 + 总市值 − 负债 − 当日盈亏, 信用账户未读即无)");
  case C_PNL:
  case C_PNL_PCT:
    return tip("持仓盈亏: 券商口径, 随现价平移");
  case C_MV:
    return tip("总市值");
  case C_MARGIN:
    return tip("融资部分市值, 占总市值; 悬停看金额");
  case C_SELLABLE:
    return tip("可卖部分市值, 占总市值; 悬停看金额");
  case C_SELL:
    return tip("调仓先卖的拖卖出\n只停在能成单的档: 够起买股数、够最小单元, 末档全卖");
  case C_FACE:
    return tip("卖出面值, 全部可卖标「全」");
  case C_COST:
    return tip("该卖单的滑点 + 手续费 [元]; 不成单即原因, 拖到的股数试算还没回算即 …");
  case C_COLLAT:
    return tip("担保折算率");
  default:
    return;
  }
}

// 卖出合计 (表尾)
struct Sells {
  double amount = 0, cost = 0;
  int orders = 0;
};
// 卖出三格: 滑杆、面值、该卖单成本
void sell_cells(LiveHub &hub, const live::rebal::Hold &h, Sells &sum) {
  const double unit = hub.cfg().unit;
  const auto i = hub.cfg().sells.find(h.code);
  int n = std::min(i == hub.cfg().sells.end() ? 0 : i->second, h.avail);
  ImGui::TableSetColumnIndex(C_SELL);
  ImGui::BeginDisabled(snap(h, unit, h.avail) == 0 || hub.busy());
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 4);
  if (ImGui::SliderInt("##sell", &n, 0, h.avail, ""))
    hub.set_sell(h.code, snap(h, unit, n));
  ImGui::EndDisabled();
  if (!n)
    return;
  ImGui::TableSetColumnIndex(C_FACE);
  ImGui::Text("%s%s", n == h.avail ? "全 " : "", wan(n * h.px).c_str());
  ImGui::TableSetColumnIndex(C_COST);
  if (n != std::min(h.sell, h.avail))
    ImGui::TextDisabled("…");
  else if (h.order) {
    ImGui::Text("%.0f", h.order->slip + h.order->fee);
    if (ImGui::IsItemHovered())
      tip("滑点 %.0f 元 · 费 %.0f 元", h.order->slip, h.order->fee);
    sum.amount += h.order->amount;
    sum.cost += h.order->slip + h.order->fee;
    ++sum.orders;
  } else
    text_col(grey(), h.skip);
}

void table(LiveHub &hub, BoardState &st, const live::board::Frame &f, bool can, bool &ask) {
  const Flows flows = flows_of(*hub.state());
  const std::set<std::string> listed(hub.codes().begin(), hub.codes().end());
  std::map<std::string, const live::rebal::Hold *> holds;
  if (hub.plan().plan)
    for (const live::rebal::Hold &h : hub.plan().plan->holds)
      holds.emplace(h.code, &h);
  const ImVec2 pad0 = ImGui::GetStyle().CellPadding;
  const float pad = st.pos_w > 0 ? std::clamp((ImGui::GetContentRegionAvail().x - st.pos_w) / (2 * C_COUNT), 1.0f, pad0.x) : pad0.x;
  ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(pad, pad0.y));
  const bool began = ImGui::BeginTable("##pos", C_COUNT, kFlags | ImGuiTableFlags_Sortable | ImGuiTableFlags_SortTristate);
  ImGui::PopStyleVar();
  if (!began)
    return;
  for (int c = 0; c < C_COUNT; ++c)
    ImGui::TableSetupColumn(kHeads[c], col_flags(c));
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  for (int c = 0; c < C_COUNT; ++c) {
    ImGui::TableSetColumnIndex(c);
    ImGui::TableHeader(kHeads[c]);
    if (ImGui::IsItemHovered())
      head_tip(c, f);
  }
  const bool priv = hub.priv();
  int held = 0, fin = 0;
  double pos_pct = 0, margin = 0, sellable = 0;
  Sells sells;
  for (const live::board::Row *p : sorted(f)) {
    const live::board::Row &r = *p;
    ImGui::TableNextRow();
    ImGui::PushID(r.code.c_str());
    held += !r.closed;
    pos_pct += r.position_pct.value_or(0);
    margin += r.margin_value.value_or(0);
    sellable += r.sellable_value.value_or(0);
    const double mv = r.market_value.value_or(0);
    const auto hi = holds.find(r.code);
    const live::rebal::Hold *h = hi == holds.end() ? nullptr : hi->second;

    ImGui::TableSetColumnIndex(C_CODE);
    if (ImGui::Selectable(priv ? kMask : r.code.c_str(), st.sel == r.code, ImGuiSelectableFlags_SpanAllColumns | ImGuiSelectableFlags_AllowOverlap))
      st.sel = r.code;
    ImGui::TableSetColumnIndex(C_POS_PCT);
    text_opt(r.position_pct, "%.2f");
    ImGui::TableSetColumnIndex(C_NAME);
    if (priv)
      masked();
    else if (r.closed)
      text_col(grey(), r.name);
    else if (listed.count(r.code))
      text_col(kAcc, r.name);
    else
      ImGui::TextUnformatted(r.name.c_str());
    ImGui::TableSetColumnIndex(C_FLOW);
    flow_cell(flows, r.code, priv);
    ImGui::TableSetColumnIndex(C_FIN);
    if (h && h->financeable) {
      text_col(kHot, "R");
      ++fin;
    }
    ImGui::TableSetColumnIndex(C_ORDER);
    order_cell(hub, st, r, can, ask);
    const auto pnl_cell = [&](int c, const std::optional<double> &v, const std::string &s) {
      ImGui::TableSetColumnIndex(c);
      if (priv && v)
        masked();
      else
        pnl_opt(v, s);
    };
    pnl_cell(C_DAY_PCT, r.day_pnl_pct, r.day_pnl_pct ? strf("%+.2f", *r.day_pnl_pct) : "");
    pnl_cell(C_DAY, r.day_pnl, r.day_pnl ? wan_pnl(*r.day_pnl) : "");
    pnl_cell(C_PNL_PCT, r.pnl_pct, r.pnl_pct ? strf("%+.2f", *r.pnl_pct) : "");
    pnl_cell(C_PNL, r.pnl, r.pnl ? wan_pnl(*r.pnl) : "");
    ImGui::TableSetColumnIndex(C_MV);
    if (r.market_value && priv)
      masked();
    else if (r.market_value)
      ImGui::TextUnformatted(wan(mv).c_str());
    ImGui::TableSetColumnIndex(C_MARGIN);
    bar(r.margin_value, mv, kHot);
    ImGui::TableSetColumnIndex(C_SELLABLE);
    bar(r.sellable_value, mv, kAcc);
    if (h)
      sell_cells(hub, *h, sells);
    ImGui::TableSetColumnIndex(C_COLLAT);
    text_opt(r.collateral, "%.2f");
    ImGui::PopID();
  }
  ImGui::TableNextRow();
  ImGui::TableSetColumnIndex(C_CODE);
  ImGui::Text("合计 %d 只", held);
  ImGui::TableSetColumnIndex(C_POS_PCT);
  ImGui::Text("%.2f", pos_pct);
  ImGui::TableSetColumnIndex(C_FLOW);
  if (priv)
    masked();
  else
    ImGui::Text("●%s ○%s", hub.state()->fills ? std::to_string(hub.state()->fills->rows.size()).c_str() : "-",
                hub.state()->orders ? std::to_string(hub.state()->orders->size()).c_str() : "-");
  ImGui::TableSetColumnIndex(C_FIN);
  if (hub.plan().plan)
    ImGui::Text("R%d", fin);
  ImGui::TableSetColumnIndex(C_DAY);
  const auto &acct = hub.state()->acct;
  if (f.synced) {
    const std::string ret = acct ? strf(" (%+.2f%%)", f.day_pnl / (acct->cash + f.market_value - acct->debt - f.day_pnl) * 100) : "";
    text_col(tint(f.day_pnl, 0), (priv ? std::string(kMask) : wan_pnl(f.day_pnl)) + ret);
  } else if (priv)
    masked();
  else {
    text_col(kRed, "不全");
    if (ImGui::IsItemHovered())
      tip("成交与持仓今买 / 今卖对不上, 当日盈亏不全 (点刷新)");
  }
  ImGui::TableSetColumnIndex(C_PNL);
  if (priv)
    masked();
  else
    text_col(tint(f.pnl, 0), wan_pnl(f.pnl));
  ImGui::TableSetColumnIndex(C_MV);
  if (priv)
    masked();
  else
    ImGui::TextUnformatted(wan(f.market_value).c_str());
  ImGui::TableSetColumnIndex(C_MARGIN);
  bar(margin, f.market_value, kHot);
  ImGui::TableSetColumnIndex(C_SELLABLE);
  bar(sellable, f.market_value, kAcc);
  if (sells.orders) {
    ImGui::TableSetColumnIndex(C_SELL);
    ImGui::Text("卖 %d 单", sells.orders);
    ImGui::TableSetColumnIndex(C_FACE);
    ImGui::TextUnformatted(wan(sells.amount).c_str());
    ImGui::TableSetColumnIndex(C_COST);
    ImGui::Text("%.0f", sells.cost);
  }
  ImGui::EndTable();
  st.pos_w = ImGui::GetItemRectSize().x - 2 * C_COUNT * pad;
}

// 名单行: 排名、可买与否、条款、补多少、融资 / 现金两腿
void targets(const live::rebal::Plan &p, bool priv) {
  if (!ImGui::BeginTable("##targets", 10, kFlags))
    return;
  for (const char *h : {"排名", "代码", "名称", "", "保证金比例", "折算率", "现市值", "补", "融资买入", "现金买入"})
    ImGui::TableSetupColumn(h);
  ImGui::TableHeadersRow();
  const auto leg = [&](const live::rebal::Target &t, live::Credit c) {
    const auto l = std::find_if(t.legs.begin(), t.legs.end(), [&](const live::rebal::Leg &x) { return x.credit == c; });
    if (l == t.legs.end() || (!l->order && l->skip.empty()))
      return;
    if (priv)
      masked();
    else if (l->order)
      ImGui::Text("%.0f股 · %s", l->order->qty, wan(l->order->amount).c_str());
    else
      text_col(grey(), strf("%s (%s)", l->skip.c_str(), wan(l->want).c_str()));
  };
  for (const live::rebal::Target &t : p.targets) {
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("%d", t.rank);
    ImGui::TableSetColumnIndex(1);
    ImGui::TextUnformatted(priv ? kMask : t.code.c_str());
    ImGui::TableSetColumnIndex(2);
    ImGui::TextUnformatted(priv ? kMask : t.name.c_str());
    ImGui::TableSetColumnIndex(3);
    if (!t.why.empty())
      text_col(kRed, t.why);
    else if (t.financeable)
      text_col(kHot, "融资标的");
    else
      ImGui::TextUnformatted("担保品");
    ImGui::TableSetColumnIndex(4);
    text_opt(t.ratio, "%.2f");
    ImGui::TableSetColumnIndex(5);
    text_opt(t.rate, "%.2f");
    ImGui::TableSetColumnIndex(6);
    if (t.held)
      ImGui::TextUnformatted(priv ? kMask : wan(*t.held).c_str());
    ImGui::TableSetColumnIndex(7);
    if (priv && (!t.skip.empty() || t.need))
      masked();
    else if (!t.skip.empty())
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

// 事务日志: 落盘文件 + 留给页面的尾巴, 跟到底
void journal(LiveHub &hub, bool priv) {
  const auto &t = hub.task();
  if (!t)
    return;
  if (!ImGui::CollapsingHeader("事务日志"))
    return;
  ImGui::TextDisabled("%s", t->file.c_str());
  ImGui::BeginChild("##log", ImVec2(0, ImGui::GetTextLineHeightWithSpacing() * 12), ImGuiChildFlags_Borders);
  for (const LogLine &l : t->log)
    if (priv)
      ImGui::TextDisabled("%8.2f %s %s", l.t, l.kind.c_str(), kMask);
    else
      ImGui::Text("%8.2f %s %s", l.t, l.kind.c_str(), l.text.c_str());
  if (ImGui::GetScrollY() >= ImGui::GetScrollMaxY())
    ImGui::SetScrollHereY(1.0f);
  ImGui::EndChild();
}

// 右栏: 选中持仓的行情图
void chart(SharedData &data, Quote::QuoteHub &quote, BoardState &st, const live::board::Frame *f, bool priv) {
  if (!data.task_state.database.asset_info_ready)
    return ImGui::TextDisabled("行情图要本地资产库就绪 (见 Database)");
  quote.start(data);
  const std::vector<std::string> &codes = data.asset_info.codes;
  if (st.ids.size() != codes.size()) {
    st.ids.clear();
    for (uint32_t i = 0; i < codes.size(); ++i)
      st.ids.emplace(codes[i], i);
  }
  uint32_t id = 0;
  bool have = false;
  if (f && !st.sel.empty())
    for (const live::board::Row &r : f->rows)
      if (r.code == st.sel) {
        const auto i = st.ids.find(r.code + live::kMktSuffix[static_cast<int>(live::market_of(r.code))]);
        if ((have = i != st.ids.end()))
          id = i->second;
      }
  Market::Sync(quote, Quote::kStock, have ? std::span<const uint32_t>(&id, 1) : std::span<const uint32_t>(), st.stock.feed);
  std::string title;
  const Market::Feed &feed = st.stock.feed;
  if (feed.data && !feed.data->keys.empty()) {
    const uint32_t k = feed.data->keys.front();
    assert(k < codes.size());
    const StockInfo *info = k < data.asset_info.stocks.size() ? data.asset_info.find(k) : nullptr;
    title = priv ? kMask : (info && !info->name.empty() ? info->name : std::string("-")) + "  " + codes[k];
  }
  Market::RenderStockPanel(quote, st.stock, title);
}

} // namespace

void RenderTabBoard(SharedData &data, LiveHub &hub, Quote::QuoteHub &quote, BoardState &st) {
  if (!hub.connected()) {
    ImGui::TextUnformatted("接券商客户端:");
    for (size_t i = 0; i < live::clients().size(); ++i) {
      const live::Client &c = live::clients()[i];
      if (ImGui::Button(strf("连接%s", c.name).c_str()))
        hub.connect(i);
      ImGui::SameLine();
      ImGui::TextWrapped("%s", c.about);
    }
    return;
  }
  hub.auto_pull();

  const BoardView &v = hub.board();
  std::set<std::string> held;
  if (v.frame)
    for (const live::board::Row &r : v.frame->rows)
      held.insert(r.code);

  const float w = ImGui::GetContentRegionAvail().x;
  bool ask = false;
  ImGui::BeginChild("##board_left", ImVec2(w * st.split, 0), ImGuiChildFlags_None, ImGuiWindowFlags_HorizontalScrollbar);
  ImGui::PushFont(nullptr, ImGui::GetStyle().FontSizeBase * kFontScale);
  const float em = ImGui::GetFontSize();
  ImGui::PushStyleVar(ImGuiStyleVar_ItemSpacing, ImVec2(em * 0.4f, em * 0.15f));
  ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(em * 0.3f, em * 0.05f));
  ImGui::PushStyleVar(ImGuiStyleVar_CellPadding, ImVec2(em * 0.3f, em * 0.08f));
  groups(hub, st);
  info_line(hub, held);
  if (v.frame)
    table(hub, st, *v.frame, st.armed && !hub.ordering() && !hub.busy(), ask);
  if (hub.plan().plan) {
    const bool open = ImGui::CollapsingHeader("调仓名单", ImGuiTreeNodeFlags_DefaultOpen);
    if (ImGui::IsItemHovered())
      tip("先融资标的、后其余, 各按排名入选, 等权补齐");
    if (open)
      targets(*hub.plan().plan, hub.priv());
  }
  journal(hub, hub.priv());
  ImGui::PopStyleVar(3);
  ImGui::PopFont();
  ImGui::EndChild();
  ImGui::SameLine();
  ImGui::BeginChild("##board_right", ImVec2(0, 0), ImGuiChildFlags_Borders);
  chart(data, quote, st, v.frame ? &*v.frame : nullptr, hub.priv());
  ImGui::EndChild();
  confirm(hub, st, ask);
  confirm_run(hub, st);
}

} // namespace GUI::Live
