#pragma once

// 表格的列与格 (各表同一套: Lines 右表 / ASSET 资产表): 列 = 表头 + 悬停说明, 格 = 文本 (填格时量宽) + 排序键 + 名次底色.
//   说明各列同一格式: "列名 = 全称" + 算法 / 数据 / 显示 / 适用 四行 ("• " 开头)

#include "gui/util/Chart.hpp" // band_col
#include "gui/util/Tip.hpp"
#include "imgui.h"

#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <limits>
#include <string>

namespace GUI {

// 一列: 表头 / 悬停说明
struct SheetCol {
  std::string name, tip;
};
// 格文本: 填格时量宽, 画时按列宽对齐
struct NumCell {
  char s[32] = "-";
  ImVec4 col;
  float w = 0.f; // 文本宽 (填格时的字号)
};
// 一格
struct SheetCell {
  NumCell text;
  double key = std::numeric_limits<double>::quiet_NaN();  // 排序键 (不可算 = NaN, 恒末)
  double rank = std::numeric_limits<double>::quiet_NaN(); // 底色 = 截面名次 (rank_by 的 u: 0 = 好 绿 → 1 = 差 红); NaN = 不铺
};
// 名次底色的 alpha
inline constexpr float kRankA = 0.45f;

inline NumCell num_cell(const ImVec4 &col, const char *fmt, ...) {
  NumCell c;
  va_list ap;
  va_start(ap, fmt);
  std::vsnprintf(c.s, sizeof(c.s), fmt, ap);
  va_end(ap);
  c.col = col, c.w = ImGui::CalcTextSize(c.s).x;
  return c;
}
inline NumCell num_none() { return num_cell(ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled), "-"); }
// 值 y 的格: 字色 col, 按 fmt 显示 a; 不可算 → "-"; 排序键 = y
template <class... A>
SheetCell sheet_cell(double y, const ImVec4 &col, const char *fmt, A... a) {
  return {std::isfinite(y) ? num_cell(col, fmt, a...) : num_none(), y};
}

// 表头行 (同 TableHeadersRow, 另加悬停说明): n 个表列, 表列 c 的列 = col(c) (const SheetCol &; 名以 ## 起 = 不显)
template <class Col>
void sheet_head(int n, const Col &col) {
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  for (int c = 0; c < n; ++c) {
    if (!ImGui::TableSetColumnIndex(c))
      continue;
    const SheetCol &h = col(c);
    ImGui::PushID(c);
    ImGui::TableHeader(h.name.c_str());
    ImGui::PopID();
    if (ImGui::IsItemHovered())
      tip("%s", h.tip.c_str());
  }
}
// 画格 (当前表格格内): 名次底色 + 文本右对齐到宽 w (w = 文本宽 → 左对齐). 格式串不补宽: w = 该列各格 (含表头) 最宽 ⇒ 列宽恒 = 内容, 可缩
//   (不能对齐到列宽本身: 列宽是上帧的, 对齐后每格都顶满, 列只涨不缩)
inline void draw_cell(const SheetCell &c, float w) {
  if (std::isfinite(c.rank))
    ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, ImGui::ColorConvertFloat4ToU32(band_col(c.rank, kRankA)));
  ImGui::SetCursorPosX(ImGui::GetCursorPosX() + w - c.text.w);
  ImGui::TextColored(c.text.col, "%s", c.text.s);
}

} // namespace GUI
