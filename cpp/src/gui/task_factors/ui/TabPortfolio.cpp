// Factors→Portfolio: 头 / 页签 / 布局 / 三级缓存 + Factors 页的组合小框 — 见头文件; 主图 / 区间条 = PortfolioPlot.cpp, 右图例 = PortfolioSheet.cpp
#include "gui/task_factors/ui/TabPortfolio.hpp"

#include "gui/Tasks.hpp" // StatusColor
#include "gui/task_factors/ui/PortfolioCommon.hpp"
#include "gui/task_factors/ui/TabFactors.hpp" // tip / hold_desc
#include "imgui.h"
#include "shared/Basket.hpp" // basket_name

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace GUI::Factors {

namespace {

void load_current(PortfolioUIState &ui) {
  ui.entries = ui.name.empty() ? std::vector<PortfolioEntry>{} : LoadPortfolio(ui.dir, ui.name);
  ui.items.clear();
  ++ui.items_gen;
  ui.sel = -1;
  ++ui.content_gen;
}
// 重列目录, 选 want (不在 → 第一个 / 无) 并载入
void relist(PortfolioUIState &ui, const std::string &want) {
  ui.names = ListPortfolios(ui.dir);
  ui.name = std::find(ui.names.begin(), ui.names.end(), want) != ui.names.end() ? want : ui.names.empty() ? std::string{}
                                                                                                          : ui.names.front();
  load_current(ui);
}
void save_current(PortfolioUIState &ui) {
  assert(!ui.name.empty());
  SavePortfolio(ui.dir, ui.name, ui.entries);
  ui.items.clear();
  ++ui.items_gen;
  ++ui.content_gen;
}

std::string entry_label(const PortfolioEntry &e) {
  return "(" + e.pool + ", " + factor::stat::hold_name(e.hold) + "): " + std::filesystem::path(e.file).stem().string();
}

// ---- 头: 组合 / 成本 / Recompute / Cancel / 状态 / Clear (选择与图回默认) + 失效 / 失败条目原因. 返回 1 = Recompute, -1 = Cancel, 0 = 无 ----
int head(PortfolioService &svc, PortfolioUIState &ui, const PortfolioDerived &d, const PortfolioResult &res, const std::string &message,
         double fixed_impact) {
  int action = 0;
  size_t n_bad = 0;
  for (const PortfolioItem &it : ui.items)
    n_bad += !it.missing.empty();
  ImGui::Text("%s  %zu 因子 (算成 %d", ui.name.c_str(), ui.entries.size(), d.n_ready);
  ImGui::SameLine(0, 0);
  if (d.n_error > 0) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), ", 失败 %d", d.n_error);
    ImGui::SameLine(0, 0);
  }
  if (n_bad > 0) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), ", 失效 %zu", n_bad);
    ImGui::SameLine(0, 0);
  }
  ImGui::Text(")");
  ImGui::SameLine();
  ImGui::TextDisabled("|");
  ImGui::SameLine();
  ImGui::TextDisabled("有冲击");
  if (ImGui::IsItemHovered())
    tip("换手成本 = 各因子自己的最小冲击段, 不可选\n"
        "• 每行开 1/h̄ 净值一批 (h̄ = 平均持有行数, 全日平均满仓), 换手按 Δw: 上一批平仓与本批建仓同一价格点时只交接差额 (留仓不成交);\n"
        "  新进成交不了不建仓、续持者封板照持也加不了仓, 平仓成交不了的顺延\n"
        "• 用了 lob.* 叶: 增仓 × 最小金额段冲击 (封顶 max_impact) + 平仓量 × 固定冲击 %.1f bp\n"
        "• 否则固定段: 增仓量 / 平仓量都按固定冲击 %.1f bp\n"
        "• 组合只算 G20 多头; 无冲击线不扣",
        fixed_impact * 1e4, fixed_impact * 1e4);
  ImGui::SameLine();
  ImGui::Checkbox("log", &ui.log_y);
  if (ImGui::IsItemHovered())
    tip("y 轴 (净值) 切对数刻度");
  ImGui::SameLine();
  const bool busy = svc.busy();
  ImGui::BeginDisabled(busy);
  if (ImGui::Button("Recompute", ImVec2(90, 0)))
    action = 1;
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("弃缓存重算全部因子 + 池指数 (特征库重算过之后用)\n"
        "• 平时不用: 组合加 / 删因子自动只算新的那个");
  ImGui::SameLine();
  ImGui::BeginDisabled(!busy);
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    action = -1;
  ImGui::EndDisabled();
  ImGui::SameLine();
  {
    const Phase ph = svc.phase.load(std::memory_order_acquire);
    const int jd = svc.jobs_done.load(std::memory_order_relaxed), jt = svc.jobs_total.load(std::memory_order_relaxed);
    const int done = static_cast<int>(svc.done.load(std::memory_order_relaxed)), total = static_cast<int>(svc.total.load(std::memory_order_relaxed));
    const ImVec4 bc = StatusColor(TaskStatus::Kind::Busy);
    switch (ph) {
    case Phase::Idle:
    case Phase::Scanning:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
      break;
    case Phase::Loading:
      ImGui::TextColored(bc, "[%d/%d] loading %d/%d days", jd + 1, jt, done, total);
      break;
    case Phase::Running:
      ImGui::TextColored(bc, "[%d/%d] running %d/%d", jd + 1, jt, done, total);
      break;
    case Phase::Done:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "done");
      break;
    case Phase::Cancelled:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelled %d/%d", jd, jt);
      break;
    }
  }
  if (!message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "| %s", message.c_str());
  }
  ImGui::SameLine();
  if (ImGui::Button("Clear", ImVec2(60, 0))) {
    ui.pick = PortfolioPick{.of = ui.name};
    ui.range = PortfolioRange{.d0 = 0, .d1 = static_cast<int>(d.dates.size())};
    ui.log_y = false;
  }
  if (ImGui::IsItemHovered())
    tip("清掉全部选择, 图回默认 (空): NAV / FLOW 选中行 / VAL 标的 / 展开的行 / 叠加的成分股 / 区间 (回全段) / log 关");
  for (const PortfolioItem &it : ui.items) {
    const auto f = res.factors.find(it.e.key());
    const std::string &why = !it.missing.empty() ? it.missing : f != res.factors.end() ? f->second->error
                                                                                       : it.missing;
    if (!why.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "  %s  %s", entry_label(it.e).c_str(), why.c_str());
  }
  return action;
}

// ---- 页签: NAV = 资金曲线 / VAL = 池估值 (同一布局; 区间 / log 共用) / FLOW = 篮子日内资金流. 返回 PortfolioPage ----
int pages() {
  int page = kPageNav;
  if (ImGui::BeginTabBar("##pf_page")) {
    for (int p = 0; p < kPages; ++p) {
      const bool open = ImGui::BeginTabItem(kPageDefs[p].name);
      if (ImGui::IsItemHovered())
        tip("%s", kPageDefs[p].tip);
      if (open) {
        page = p;
        ImGui::EndTabItem();
      }
    }
    ImGui::EndTabBar();
  }
  return page;
}

} // namespace

void SyncPortfolioDir(PortfolioUIState &ui, const std::string &dir) {
  if (ui.dir == dir)
    return;
  ui.dir = dir;
  relist(ui, ui.name);
}

void RenderPortfolioBoxes(PortfolioUIState &ui, const FactorRow *view_row, const std::string &pool, LevelId lv, int hold) {
  const ImGuiStyle &st = ImGui::GetStyle();
  const float h = 3.0f * ImGui::GetFrameHeightWithSpacing() - st.ItemSpacing.y + 2.0f * st.WindowPadding.y;
  constexpr float kW1 = 250.0f, kW2 = 380.0f;

  // ---- 小框 1: 选组合 / New / Rename / Del ----
  ImGui::BeginChild("##pf_box", ImVec2(kW1, h), ImGuiChildFlags_Borders, ImGuiWindowFlags_NoScrollbar);
  ImGui::TextColored(kHead, "Portfolio");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(-FLT_MIN);
  if (ImGui::BeginCombo("##pf_sel", ui.name.empty() ? "-" : ui.name.c_str())) {
    for (const std::string &n : ui.names)
      if (ImGui::Selectable(n.c_str(), n == ui.name) && n != ui.name) {
        ui.name = n;
        load_current(ui);
        ui.msg.clear();
        ui.confirm_delete = false;
      }
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered()) // 组合内容 = <dir>/<name>.json, 改了即存; 选哪个只在内存
    tip("当前组合 (改动即存盘; Portfolio 页画它的曲线)");
  const std::string nm = ui.name_buf;
  const bool nm_ok = ValidFactorName(nm) && std::find(ui.names.begin(), ui.names.end(), nm) == ui.names.end();
  ImGui::SetNextItemWidth(96);
  ImGui::InputTextWithHint("##pf_name", "name", ui.name_buf, sizeof(ui.name_buf));
  ImGui::SameLine();
  ImGui::BeginDisabled(!nm_ok);
  if (ImGui::SmallButton("New")) {
    SavePortfolio(ui.dir, nm, {});
    relist(ui, nm);
    ui.msg = "新建 " + nm;
    ui.name_buf[0] = '\0';
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("用左边的名字新建空组合 (仅 [A-Za-z0-9_], 不与已有重名)");
  ImGui::SameLine();
  ImGui::BeginDisabled(!nm_ok || ui.name.empty());
  if (ImGui::SmallButton("Rename")) {
    const std::string old = ui.name;
    const bool ok = RenamePortfolio(ui.dir, old, nm);
    assert(ok && "重名已在按钮处排除");
    (void)ok;
    relist(ui, nm);
    ui.msg = old + " → " + nm;
    ui.name_buf[0] = '\0';
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("当前组合改成左边的名字");
  ImGui::SameLine();
  ImGui::BeginDisabled(ui.name.empty());
  if (ImGui::SmallButton("Del"))
    ui.confirm_delete = true;
  ImGui::EndDisabled();
  if (ui.confirm_delete) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "删除 %s?", ui.name.c_str());
    ImGui::SameLine();
    if (ImGui::SmallButton("Yes")) {
      DeletePortfolio(ui.dir, ui.name);
      ui.msg = "已删除 " + ui.name;
      relist(ui, "");
      ui.confirm_delete = false;
    }
    ImGui::SameLine();
    if (ImGui::SmallButton("No"))
      ui.confirm_delete = false;
  } else {
    ImGui::TextDisabled("%s", ui.msg.c_str());
  }
  ImGui::EndChild();

  // ---- 小框 2: 当前组合的条目 ----
  ImGui::SameLine();
  ImGui::BeginChild("##pf_items", ImVec2(kW2, h), ImGuiChildFlags_Borders, ImGuiWindowFlags_NoScrollbar);
  ImGui::TextColored(kHead, "Factors (%zu)", ui.entries.size());
  ImGui::SameLine();
  const bool row_ok = view_row && view_row->error.empty() && !view_row->expr.empty();
  ImGui::BeginDisabled(ui.name.empty() || !row_ok || hold <= 0);
  if (ImGui::SmallButton("Add")) {
    const PortfolioEntry e{pool, lv, view_row->file, view_row->expr, hold, view_row->sign};
    const auto dup = std::find_if(ui.entries.begin(), ui.entries.end(), [&](const PortfolioEntry &x) { return x.key() == e.key(); });
    if (dup != ui.entries.end()) {
      ui.msg = entry_label(e) + " 已在组合里";
    } else {
      ui.entries.push_back(e);
      save_current(ui);
      ui.sel = static_cast<int>(ui.entries.size()) - 1;
      ui.msg = "加入 " + entry_label(e);
    }
  }
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled)) {
    if (ui.name.empty())
      tip("先在左框新建 / 选一个组合");
    else if (!row_ok)
      tip("先在表格里点一行");
    else
      tip("把 %s 加进组合 (持有期 = 当前 Hold %s)", view_row->key.c_str(), factor::stat::hold_name(hold).c_str());
  }
  ImGui::SameLine();
  const bool sel_ok = ui.sel >= 0 && ui.sel < static_cast<int>(ui.entries.size());
  ImGui::BeginDisabled(!sel_ok);
  if (ImGui::SmallButton("Del##pf_item")) {
    ui.msg = "移除 " + entry_label(ui.entries[static_cast<size_t>(ui.sel)]);
    ui.entries.erase(ui.entries.begin() + ui.sel);
    save_current(ui);
    ui.sel = -1;
  }
  ImGui::EndDisabled();
  ImGui::BeginChild("##pf_list", ImVec2(0, 0));
  const bool synced = ui.items.size() == ui.entries.size();
  for (size_t i = 0; i < ui.entries.size(); ++i) {
    const PortfolioEntry &e = ui.entries[i];
    const PortfolioItem *it = synced && ui.items[i].e.key() == e.key() ? &ui.items[i] : nullptr;
    const bool bad = it && !it->missing.empty();
    if (bad)
      ImGui::PushStyleColor(ImGuiCol_Text, StatusColor(TaskStatus::Kind::Error));
    const std::string label = entry_label(e) + "##pfe" + std::to_string(i);
    if (ImGui::Selectable(label.c_str(), ui.sel == static_cast<int>(i)))
      ui.sel = ui.sel == static_cast<int>(i) ? -1 : static_cast<int>(i);
    if (bad)
      ImGui::PopStyleColor();
    if (ImGui::IsItemHovered())
      tip("%s / %s  %s  %s\n"
          "• %s\n"
          "• 规范串 %s%s%s%s",
          e.pool.c_str(), factor_level(e.lv).name, e.file.c_str(), it ? it->name_cn.c_str() : "", hold_desc(e.hold, e.lv).c_str(), e.expr.c_str(),
          e.sign < 0 ? " · 取反" : "", bad ? "\n• 失效: " : "", bad ? it->missing.c_str() : "");
  }
  ImGui::EndChild();
  ImGui::EndChild();
}

int RenderTabPortfolio(PortfolioService &svc, PortfolioUIState &ui, double fixed_impact, const Database::Market *mk, const StockInfos &infos) {
  PortfolioResult res;
  uint64_t gen = 0;
  std::string message;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    res = svc.result; // shared_ptr 拷贝, 不拷序列
    gen = svc.result_gen;
    message = svc.message;
  }
  PortfolioDerived &d = ui.der.val;

  // ---- 三级缓存: 派生 → 滚动分位 / 末点分位 (键 = 篮子代) → 视图 (键 = 派生 gen + 二级两份 gen): 上级重算 → 下级自动重算 ----
  ui.der.update(PortfolioDerKey{gen, ui.items_gen, ui.basket.feed.shown, fixed_impact}, [&](PortfolioDerived &dv) {
    const std::vector<misc::Ymd> old = dv.dates;
    MakePortfolioDerived(ui.items, ui.name, res, ui.basket.feed.data, fixed_impact, dv);
    if (dv.dates != old) // 日轴变 → 区间回全段
      ui.range = PortfolioRange{.d0 = 0, .d1 = static_cast<int>(dv.dates.size())};
  });
  if (ui.pick.of != ui.name) // 换组合 → 选中清掉
    ui.pick = PortfolioPick{.of = ui.name};
  if (mk)
    SyncPortfolioCons(ui, *mk->quote);
  const std::set<std::string> &one = ui.pick.rows[kPickOne];
  const PortfolioPin pin = one.empty() ? PortfolioPin{} : d.pin_of(*one.begin()); // VAL 标的 (篮子行 / 成分股)
  ui.roll.update(PortfolioRollKey{d.basket_gen, ui.band_years, pin}, [&](PortfolioRolls &r) { MakePortfolioRoll(d, pin, ui.band_years, r); });
  ui.pct.update(PortfolioPctKey{d.basket_gen, ui.band_years, ui.range.d1, ui.cons.ids},
                [&](PortfolioPct &p) { MakePortfolioPct(d, ui.range.d1, ui.band_years, ui.cons.ids, p); });
  ui.view.update(PortfolioViewKey{ui.der.gen, ui.roll.gen, ui.pct.gen, ui.range.d0, ui.range.d1},
                 [&](PortfolioView &vw) { MakePortfolioView(d, ui.range.d0, ui.range.d1, pin, ui.roll.val, ui.pct.val, vw); });

  ImGui::TextColored(kHead, "Portfolio:");
  ImGui::SameLine();
  if (ui.name.empty()) {
    ImGui::TextDisabled("无 (在 Factors 页 1. 区右侧新建 / 选组合)");
    return 0;
  }
  const int action = head(svc, ui, d, res, message, fixed_impact);
  const int page = pages();
  const bool nav = page == kPageNav;

  // ---- 布局: 左 = 日内 (NAV 开日内, 切 1/3 高) / 主图 (NAV ↔ VAL) / 区间条 自上而下, FLOW = 只一张资金流图; 最右 = 右图例 (三页同宽) ----
  constexpr float kBarH = 90.0f, kLegendCells = 100.0f, kLegendMaxFrac = 0.5f; // 右图例定宽 (三页同宽: 字符格数, 表格超宽横滚) / 至多占整宽的比例
  const ImGuiStyle &st = ImGui::GetStyle();
  const ImVec2 av = ImGui::GetContentRegionAvail();
  const float lw = std::min(kLegendCells * ImGui::CalcTextSize("0").x + 2.0f * (st.WindowPadding.x + st.ChildBorderSize), av.x * kLegendMaxFrac);
  const float left = av.x - lw - st.ItemSpacing.x;
  const float ph = av.y - kBarH - st.ItemSpacing.y;
  const float ih = nav && ui.intraday ? std::floor((ph - st.ItemSpacing.y) / 3.0f) : 0.0f;
  // 选中篮子行 + 点中的成分股的行情叠加图 (日内栏 / FLOW 同一份选中, 篮子线色 = 主图的), 贴边占满 size
  const auto basket_child = [&](const char *id, const ImVec2 &size, bool flow) {
    ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
    ImGui::BeginChild(id, size);
    ImGui::PopStyleVar();
    if (mk) {
      std::vector<Database::OverlayPick> picks;
      for (int g = 0; g < kBasketGroups; ++g)
        for (size_t r = 0; r < d.rows[g].size(); ++r)
          if (const PortfolioRow &h = d.rows[g][r]; picked(ui, page, h))
            picks.push_back({Database::kBasket, static_cast<uint32_t>(h.basket), ImGui::ColorConvertFloat4ToU32(row_color(d, g, r, kTrade)),
                             basket_name(static_cast<size_t>(h.basket))});
      for (const uint32_t id : ui.pick.stocks)
        picks.push_back({Database::kStock, id, ImGui::ColorConvertFloat4ToU32(stock_color(id)), stock_name(infos, id)});
      if (flow)
        Database::RenderBasketFlow(*mk, ui.basket, picks);
      else
        Database::RenderBasketIntraday(*mk, ui.basket, picks);
    } else {
      ImGui::TextDisabled("行情等基本面就绪…");
    }
    ImGui::EndChild();
  };
  if (page == kPageFlow) {
    basket_child("##pf_flow", ImVec2(left, av.y), true);
    ImGui::SameLine();
    ImGui::BeginChild("##pf_flegend", ImVec2(lw, av.y), ImGuiChildFlags_Borders);
    RenderPortfolioSheet(ui, page, mk ? Database::FlowDays(*mk, ui.basket) : std::vector<misc::Ymd>{}, infos);
    ImGui::EndChild();
    return action;
  }
  ImGui::BeginGroup();
  if (ih > 0.0f)
    basket_child("##pf_intra", ImVec2(left, ih), false);
  RenderPortfolioPlot(ui, pin, nav, ImVec2(left, ih > 0.0f ? ph - ih - st.ItemSpacing.y : ph), infos); // fit 只作用于紧接的 BeginPlot: 须在日内栏之后
  RenderPortfolioRange(ui, pin, nav, ImVec2(left, kBarH));
  ImGui::EndGroup();
  ImGui::SameLine();
  ImGui::BeginChild("##pf_flegend", ImVec2(lw, av.y), ImGuiChildFlags_Borders);
  RenderPortfolioSheet(ui, page, {}, infos);
  ImGui::EndChild();
  return action;
}

} // namespace GUI::Factors
