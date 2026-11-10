// 组合小框 + 当前组合的载入 / 存盘 — 见头文件
#include "gui/engine/ui/PortfolioBoxes.hpp"
#include "gui/engine/Hold.hpp" // hold_desc

#include "gui/Tasks.hpp"      // StatusColor
#include "gui/util/Chart.hpp" // kHead
#include "gui/util/Tip.hpp"
#include "imgui.h"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <string>
#include <vector>

namespace GUI::Engine {

namespace {

void load_current(PortfolioUIState &ui) {
  ui.entries = ui.name.empty() ? std::vector<PortfolioEntry>{} : LoadPortfolio(ui.name);
  ui.items.clear();
  ++ui.items_gen;
  ui.sel = -1;
  ++ui.content_gen;
}
// 重列目录, 选 want (不在 → 第一个 / 无) 并载入
void relist(PortfolioUIState &ui, const std::string &want) {
  ui.names = ListPortfolios();
  ui.name = std::find(ui.names.begin(), ui.names.end(), want) != ui.names.end() ? want : ui.names.empty() ? std::string{}
                                                                                                          : ui.names.front();
  load_current(ui);
}
void save_current(PortfolioUIState &ui) {
  assert(!ui.name.empty());
  SavePortfolio(ui.name, ui.entries);
  ui.items.clear();
  ++ui.items_gen;
  ++ui.content_gen;
}

} // namespace

std::string EntryLabel(const PortfolioEntry &e) {
  return "(" + e.pool + ", " + factor::stat::hold_name(e.hold) + "): " + std::filesystem::path(e.file).stem().string();
}

void SyncPortfolioList(PortfolioUIState &ui) {
  if (ui.listed)
    return;
  ui.listed = true;
  relist(ui, ui.name);
}

void RenderPortfolioBoxes(PortfolioUIState &ui, const FactorRow *view_row, const std::string &pool, FactorLevelId lv, int hold) {
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
  if (ImGui::IsItemHovered()) // 组合内容 = output/portfolios/<name>.json, 改了即存; 选哪个只在内存
    tip("当前组合 (改动即存盘; Market → NAV 画它的曲线)");
  const std::string nm = ui.name_buf;
  const bool nm_ok = ValidFactorName(nm) && std::find(ui.names.begin(), ui.names.end(), nm) == ui.names.end();
  ImGui::SetNextItemWidth(96);
  ImGui::InputTextWithHint("##pf_name", "name", ui.name_buf, sizeof(ui.name_buf));
  ImGui::SameLine();
  ImGui::BeginDisabled(!nm_ok);
  if (ImGui::SmallButton("New")) {
    SavePortfolio(nm, {});
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
    const bool ok = RenamePortfolio(old, nm);
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
      DeletePortfolio(ui.name);
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
      ui.msg = EntryLabel(e) + " 已在组合里";
    } else {
      ui.entries.push_back(e);
      save_current(ui);
      ui.sel = static_cast<int>(ui.entries.size()) - 1;
      ui.msg = "加入 " + EntryLabel(e);
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
    ui.msg = "移除 " + EntryLabel(ui.entries[static_cast<size_t>(ui.sel)]);
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
    const std::string label = EntryLabel(e) + "##pfe" + std::to_string(i);
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

} // namespace GUI::Engine
