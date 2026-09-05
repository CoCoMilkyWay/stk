// 资产筛选栏控件 (DATABASE/TABLE 与 FEATURES/ORDERFLOW 共用)
//
// 两页筛选口径同源、数据源不同:
//   TABLE     = AssetInfo 全局快照 (StockInfo: 字符串行业码, 静态上市状态)
//   ORDERFLOW = 锚点日 L1 落盘列 (逐日 PIT: 数字 SW2021 行业 ID)
// 所以这里只共享控件本身与 ST/Listed/Board 三维的选项表; 行业维 (键类型不同)
// 和尾部统计由各页自绘.
#pragma once

#include "gui/task_database/models/SharedTypes.hpp" // BoardType (全 GUI 通用口径)
#include "imgui.h"

#include <set>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Filter {

// ============================================================================
// 多选下拉
// ============================================================================
// items: (取值, 显示名); selected 空集 = "全选/不过滤" (与两页的过滤判据一致,
// 而不是"什么都不选"). 预览: 空 = "All"; 少量选中 = 逐项列出; 过长 = "N selected".
// 返回本帧是否发生变更 (ORDERFLOW 靠它 bump filter_epoch).
template <typename T>
bool MultiSelectCombo(const char *label, float width,
                      const std::vector<std::pair<T, std::string>> &items,
                      std::set<T> &selected) {
  std::string preview;
  if (selected.empty()) {
    preview = "All";
  } else {
    for (const auto &[value, text] : items) {
      if (selected.count(value)) {
        if (!preview.empty())
          preview += ", ";
        preview += text;
      }
    }
    if (preview.size() > 24)
      preview = std::to_string(selected.size()) + " selected";
  }

  bool changed = false;
  ImGui::SetNextItemWidth(width);
  if (ImGui::BeginCombo(label, preview.c_str())) {
    for (const auto &[value, text] : items) {
      bool is_selected = selected.count(value) != 0;
      if (ImGui::Checkbox(text.c_str(), &is_selected)) {
        if (is_selected)
          selected.insert(value);
        else
          selected.erase(value);
        changed = true;
      }
    }
    ImGui::Separator();
    if (ImGui::SmallButton("All")) {
      selected.clear();
      changed = true;
    }
    ImGui::EndCombo();
  }
  return changed;
}

// ============================================================================
// 选项表
// ============================================================================

// ST. with_delist_period: ORDERFLOW 的 risk_warn 有 3=退市整理期, 而 TABLE 的
// GetStLevel 只产 0..2 —— 传 false 免得列出永不命中的选项.
inline const std::vector<std::pair<int, std::string>> &StItems(bool with_delist_period) {
  static const std::vector<std::pair<int, std::string>> without = {
      {0, "正常"}, {1, "ST"}, {2, "*ST"}};
  static const std::vector<std::pair<int, std::string>> with = {
      {0, "正常"}, {1, "ST"}, {2, "*ST"}, {3, "退市整理"}};
  return with_delist_period ? with : without;
}

inline const std::vector<std::pair<int, std::string>> &ListedItems() {
  static const std::vector<std::pair<int, std::string>> items = {{0, "在市"}, {1, "退市"}};
  return items;
}

// Board. T = BoardType (TABLE 直接存枚举) 或 int (ORDERFLOW 存底层值, 因为
// 状态挂在 shared/ 层的 OrderFlow::Universe, 不该依赖 gui/ 层类型).
// 不含 BoardType::All 哨兵 —— 空集已经等价于全选.
template <typename T>
const std::vector<std::pair<T, std::string>> &BoardItems() {
  static const std::vector<std::pair<T, std::string>> items = {
      {static_cast<T>(Database::BoardType::Unknown), "Unknown"},
      {static_cast<T>(Database::BoardType::SH_Main), "沪主板"},
      {static_cast<T>(Database::BoardType::SZ_Main), "深主板"},
      {static_cast<T>(Database::BoardType::STAR), "科创板"},
      {static_cast<T>(Database::BoardType::ChiNext), "创业板"},
      {static_cast<T>(Database::BoardType::BSE), "北交所"}};
  return items;
}

// ============================================================================
// ST + Listed + Board 三维
// ============================================================================
// 同一行内画三个下拉. 首个不 SameLine —— 调用方要接在前面内容之后就自己先
// ImGui::SameLine(); 行业维同理由调用方 SameLine 后自绘.
// id_suffix 隔离 ImGui ID (两页可能同帧存在).
template <typename BoardT>
bool CommonFilters(const char *id_suffix, bool st_with_delist_period,
                   std::set<int> &st, std::set<int> &listed, std::set<BoardT> &board) {
  const std::string suffix(id_suffix);
  bool changed = false;

  changed |= MultiSelectCombo(("ST##st" + suffix).c_str(), 100.0f,
                              StItems(st_with_delist_period), st);
  ImGui::SameLine();
  changed |= MultiSelectCombo(("Listed##listed" + suffix).c_str(), 100.0f,
                              ListedItems(), listed);
  ImGui::SameLine();
  changed |= MultiSelectCombo(("Board##board" + suffix).c_str(), 120.0f,
                              BoardItems<BoardT>(), board);
  return changed;
}

} // namespace GUI::Filter
