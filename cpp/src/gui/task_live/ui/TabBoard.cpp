#include "gui/task_live/ui/TabBoard.hpp"

#include "gui/util/Chart.hpp"
#include "gui/util/Tip.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"

#include <algorithm>
#include <array>
#include <cassert>
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

std::string slip_txt(const std::optional<double> &v, bool full = true) { return v ? strf("%s%.1fp", full ? "" : ">", *v) : "-"; }
std::optional<ImVec4> slip_col(const std::optional<double> &v) {
  if (!v)
    return std::nullopt;
  return *v < kSlipOk ? kGreen : *v < kSlipHi ? kHot
                                              : kRed;
}
bool quick(const live::Slip &x) { return x.full && x.pct && *x.pct < kSlipOk; }

std::string wan_pnl(double v) { return strf("%+.2fW", v / 1e4); }

void text_col(const ImVec4 &c, const std::string &s) { ImGui::TextColored(c, "%s", s.c_str()); }
void text_opt(const std::optional<double> &v, const char *fmt) {
  if (v)
    ImGui::Text(fmt, *v);
}
void pnl_opt(const std::optional<double> &v, const std::string &s) {
  if (v)
    text_col(tint(*v, 0), s);
}

// 占总市值的比例条; 悬停看金额与占比
void bar(const std::optional<double> &v, double mv, const ImVec4 &col) {
  if (!v || mv <= 0)
    return;
  const float f = static_cast<float>(std::clamp(*v / mv, 0.0, 1.0));
  ImGui::PushStyleColor(ImGuiCol_PlotHistogram, col);
  ImGui::ProgressBar(f, ImVec2(ImGui::GetFontSize() * 4, ImGui::GetTextLineHeight()), "");
  ImGui::PopStyleColor();
  if (ImGui::IsItemHovered())
    tip("%s · %.0f%%", wan(*v).c_str(), f * 100);
}

// 一单的步骤条: 已过绿 / 当前橙 / 失败红 / 未到灰
void order_line(const OrderJob &j) {
  ImGui::Text("%s", j.t.c_str());
  ImGui::SameLine();
  text_col(side_col(j.side), strf("%s%s", live::side_name(j.side), j.label.c_str()));
  ImGui::SameLine();
  ImGui::Text("%s %s %s ·", j.sec.code.c_str(), j.sec.name.c_str(), j.detail.c_str());
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

void progress(LiveHub &hub) {
  const live::Refresh &g = hub.refresh();
  std::string at;
  for (const std::string &n : g.steps) {
    const auto i = g.at.find(n);
    at += strf("%s%s %s", at.empty() ? "" : " · ", n.c_str(), i == g.at.end() ? "—" : i->second.c_str());
  }
  const auto &st = hub.state();
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

void controls(LiveHub &hub, BoardState &st) {
  live::Broker &b = hub.broker();
  if (ImGui::Button("刷新"))
    b.request();
  ImGui::SameLine();
  ImGui::TextUnformatted("定时");
  ImGui::SameLine();
  int every = static_cast<int>(hub.refresh().every);
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 4);
  if (ImGui::InputInt("s##every", &every, 0, 0, ImGuiInputTextFlags_EnterReturnsTrue))
    b.every(std::max(0, every));
  if (ImGui::IsItemHovered())
    tip("定时刷新周期 [s], 0 = 不定时; 回车生效");
  ImGui::SameLine();
  int px = static_cast<int>(hub.cfg().px);
  ImGui::SetNextItemWidth(ImGui::GetFontSize() * 7);
  if (ImGui::Combo("委托价", &px, live::kPxModeNames, IM_ARRAYSIZE(live::kPxModeNames)))
    hub.set_px(static_cast<live::PxMode>(px));
  if (ImGui::IsItemHovered())
    tip("持仓行下单与调仓共用\n• 扫档: 对手盘累计恰好吃满该额那档\n• 对手价 / 对手价 ±1%%: 对手一档, 后者再朝成交方向偏 1%%\n• 本方价: 本方一档");
  ImGui::SameLine();
  ImGui::TextUnformatted("买入");
  ImGui::SameLine();
  if (ImGui::RadioButton("普通", !st.margin))
    st.margin = false;
  ImGui::SameLine();
  if (ImGui::RadioButton("融资", st.margin))
    st.margin = true;
  if (ImGui::IsItemHovered())
    tip("买入方式只管买入, 卖出一律担保品卖出");
  ImGui::SameLine();
  ImGui::Checkbox("解锁买卖", &st.armed);
}

// 持仓表上方的提示行: 帧没出的原因、无持仓、持仓表里没有的代码的挂单
void info_line(LiveHub &hub, const std::set<std::string> &held) {
  const BoardView &v = hub.board();
  if (!v.err.empty())
    return text_col(kRed, "✗ " + v.err);
  if (!v.wait.empty())
    return ImGui::TextDisabled("%s", v.wait.c_str());
  if (v.frame->rows.empty())
    return ImGui::TextDisabled("无持仓");
  const auto &orders = hub.state()->orders;
  if (!orders)
    return;
  std::string s;
  int n = 0;
  for (const live::Pending &o : *orders)
    if (!held.count(o.code)) {
      ++n;
      s += strf("%s%s %s %s %.0f股 @%.3f %s", s.empty() ? "" : "、", live::side_name(o.side), o.code.c_str(), o.name.c_str(), o.qty - o.filled, o.price,
                o.status.c_str());
    }
  if (n)
    ImGui::TextWrapped("挂单 %d 笔: %s", n, s.c_str());
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
void flow_cell(const Flows &f, const std::string &code) {
  const auto fi = f.fills.find(code);
  const auto oi = f.orders.find(code);
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
  for (size_t i = 0; i < live::kSlipCols.size(); ++i) {
    const live::Col c = live::kSlipCols[i];
    if (i)
      ImGui::SameLine(0, i == live::kSlipCols.size() / 2 ? ImGui::GetFontSize() * 0.6f : 2);
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
  ImGui::EndDisabled();
}

void confirm(LiveHub &hub, BoardState &st, bool open) {
  if (open)
    ImGui::OpenPopup("确认下单");
  if (!ImGui::BeginPopupModal("确认下单", nullptr, ImGuiWindowFlags_AlwaysAutoResize))
    return;
  assert(st.ask);
  const BoardState::Ask &a = *st.ask;
  ImGui::Text("%s%s %s %s %s, 滑点 %s, 确认下单?", live::side_name(a.side), a.side > 0 ? strf(" (%s)", live::credit_name(a.credit)).c_str() : "",
              wan(a.size).c_str(), a.code.c_str(), a.name.c_str(), a.slip.c_str());
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
  C_COLLAT,
  C_COUNT
};
constexpr const char *kHeads[C_COUNT] = {"代码", "仓位%", "名称", "状态", "融", "买 / 卖", "当日盈亏%", "当日盈亏", "盈亏%", "盈亏", "总市值", "融资", "可卖", "担保折算率"};

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
    return tip("按成交价自算 (毛, 不扣费); 与持仓的今买 / 今卖对不上即空");
  case C_MARGIN:
    return tip("融资部分市值, 占总市值; 悬停看金额");
  case C_SELLABLE:
    return tip("可卖部分市值, 占总市值; 悬停看金额");
  default:
    return;
  }
}

void table(LiveHub &hub, BoardState &st, const live::board::Frame &f, bool can, bool &ask) {
  const Flows flows = flows_of(*hub.state());
  std::set<std::string> fin, listed(hub.codes().begin(), hub.codes().end());
  if (hub.plan().plan)
    for (const live::rebal::Hold &h : hub.plan().plan->holds)
      if (h.financeable)
        fin.insert(h.code);
  constexpr ImGuiTableFlags kFlags = ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_Borders | ImGuiTableFlags_RowBg | ImGuiTableFlags_ScrollY |
                                     ImGuiTableFlags_ScrollX;
  if (!ImGui::BeginTable("##pos", C_COUNT, kFlags, ImVec2(-1, -1)))
    return;
  ImGui::TableSetupScrollFreeze(1, 1);
  for (int c = 0; c < C_COUNT; ++c)
    ImGui::TableSetupColumn(kHeads[c]);
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  for (int c = 0; c < C_COUNT; ++c) {
    ImGui::TableSetColumnIndex(c);
    ImGui::TableHeader(kHeads[c]);
    if (ImGui::IsItemHovered())
      head_tip(c, f);
  }
  int held = 0;
  double pos_pct = 0, margin = 0, sellable = 0;
  for (const live::board::Row &r : f.rows) {
    ImGui::TableNextRow();
    ImGui::PushID(r.code.c_str());
    held += !r.closed;
    pos_pct += r.position_pct.value_or(0);
    margin += r.margin_value.value_or(0);
    sellable += r.sellable_value.value_or(0);
    const double mv = r.market_value.value_or(0);

    ImGui::TableSetColumnIndex(C_CODE);
    if (ImGui::Selectable(r.code.c_str(), st.sel == r.code, ImGuiSelectableFlags_SpanAllColumns | ImGuiSelectableFlags_AllowOverlap))
      st.sel = r.code;
    ImGui::TableSetColumnIndex(C_POS_PCT);
    text_opt(r.position_pct, "%.2f");
    ImGui::TableSetColumnIndex(C_NAME);
    if (r.closed)
      text_col(grey(), r.name);
    else if (listed.count(r.code))
      text_col(kAcc, r.name);
    else
      ImGui::TextUnformatted(r.name.c_str());
    ImGui::TableSetColumnIndex(C_FLOW);
    flow_cell(flows, r.code);
    ImGui::TableSetColumnIndex(C_FIN);
    if (fin.count(r.code))
      text_col(kHot, "R");
    ImGui::TableSetColumnIndex(C_ORDER);
    order_cell(hub, st, r, can, ask);
    ImGui::TableSetColumnIndex(C_DAY_PCT);
    pnl_opt(r.day_pnl_pct, r.day_pnl_pct ? strf("%+.2f", *r.day_pnl_pct) : "");
    ImGui::TableSetColumnIndex(C_DAY);
    pnl_opt(r.day_pnl, r.day_pnl ? wan_pnl(*r.day_pnl) : "");
    ImGui::TableSetColumnIndex(C_PNL_PCT);
    pnl_opt(r.pnl_pct, r.pnl_pct ? strf("%+.2f", *r.pnl_pct) : "");
    ImGui::TableSetColumnIndex(C_PNL);
    pnl_opt(r.pnl, r.pnl ? wan_pnl(*r.pnl) : "");
    ImGui::TableSetColumnIndex(C_MV);
    if (r.market_value)
      ImGui::TextUnformatted(wan(mv).c_str());
    ImGui::TableSetColumnIndex(C_MARGIN);
    bar(r.margin_value, mv, kHot);
    ImGui::TableSetColumnIndex(C_SELLABLE);
    bar(r.sellable_value, mv, kAcc);
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
  ImGui::Text("●%s ○%s", hub.state()->fills ? std::to_string(hub.state()->fills->rows.size()).c_str() : "-",
              hub.state()->orders ? std::to_string(hub.state()->orders->size()).c_str() : "-");
  ImGui::TableSetColumnIndex(C_FIN);
  if (hub.plan().plan)
    ImGui::Text("R%zu", fin.size());
  ImGui::TableSetColumnIndex(C_DAY);
  if (f.synced)
    text_col(tint(f.day_pnl, 0), wan_pnl(f.day_pnl));
  else {
    text_col(kRed, "不全");
    if (ImGui::IsItemHovered())
      tip("成交与持仓今买 / 今卖对不上, 当日盈亏不全 (点刷新)");
  }
  ImGui::TableSetColumnIndex(C_PNL);
  text_col(tint(f.pnl, 0), wan_pnl(f.pnl));
  ImGui::TableSetColumnIndex(C_MV);
  ImGui::TextUnformatted(wan(f.market_value).c_str());
  ImGui::TableSetColumnIndex(C_MARGIN);
  bar(margin, f.market_value, kHot);
  ImGui::TableSetColumnIndex(C_SELLABLE);
  bar(sellable, f.market_value, kAcc);
  ImGui::EndTable();
}

// 右栏: 选中持仓的行情图
void chart(SharedData &data, Quote::QuoteHub &quote, BoardState &st, const live::board::Frame *f) {
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
    title = (info && !info->name.empty() ? info->name : std::string("-")) + "  " + codes[k];
  }
  Market::RenderStockPanel(quote, st.stock, title);
}

} // namespace

void RenderTabBoard(SharedData &data, LiveHub &hub, Quote::QuoteHub &quote, BoardState &st) {
  if (!hub.connected()) {
    ImGui::TextWrapped("接券商客户端: 现只有通达信 (Wine 下的 TdxW.exe, 先登录并停在持仓页). "
                       "连接后读其内存里的持仓 / 成交 / 挂单 / 信用账户, 下单与重查经注入的 shim 驱动其界面.");
    if (ImGui::Button("连接通达信"))
      hub.connect();
    return;
  }
  progress(hub);
  controls(hub, st);
  for (size_t i = 0; i < std::min<size_t>(2, hub.orders().size()); ++i)
    order_line(hub.orders()[i]);

  const BoardView &v = hub.board();
  std::set<std::string> held;
  if (v.frame)
    for (const live::board::Row &r : v.frame->rows)
      held.insert(r.code);
  info_line(hub, held);

  const float w = ImGui::GetContentRegionAvail().x;
  bool ask = false;
  ImGui::BeginChild("##board_left", ImVec2(w * st.split, 0), false);
  if (v.frame)
    table(hub, st, *v.frame, st.armed && !hub.ordering() && !hub.busy(), ask);
  ImGui::EndChild();
  ImGui::SameLine();
  ImGui::BeginChild("##board_right", ImVec2(0, 0), true);
  chart(data, quote, st, v.frame ? &*v.frame : nullptr);
  ImGui::EndChild();
  confirm(hub, st, ask);
}

} // namespace GUI::Live
