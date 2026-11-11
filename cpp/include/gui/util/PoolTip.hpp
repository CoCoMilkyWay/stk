#pragma once

// 池的悬停 (Beta Style / Compute, Market, 各页池下拉同一份): 子池 = 首行 head, 其下逐维一行 维 | 取值 | 说明 (pool::detail; 不起作用的维灰)
//   列位与窗尺寸都按本帧内容现算 (不用表格 / 不等自适应): 切换悬停的池时窗当帧就是终尺寸, 不跳

#include "features/Panel/PoolSpec.hpp"
#include "features/Storage/FeatureLib.hpp" // pool_def / kAllPoolTip
#include "gui/util/Tip.hpp"
#include "imgui.h"

#include <algorithm>
#include <array>
#include <string>

namespace GUI {

inline constexpr float kPoolNoteEm = 28.0f; // 说明列宽 (字号倍数), 超长折行

inline void pool_tip(const pool::Spec &s, const std::string &head) {
  const std::array<pool::DimDetail, pool::kDims> ds = pool::detail(s);
  const float f = ImGui::GetFontSize(), sp = ImGui::GetStyle().ItemSpacing.y, note_w = f * kPoolNoteEm;
  const ImVec2 hs = ImGui::CalcTextSize(head.c_str());
  float w0 = 0.0f, w1 = 0.0f, w2 = 0.0f, h = hs.y; // 三列最宽 / 内容总高 (行高 = 说明折行后的高)
  for (int d = 0; d < pool::kDims; ++d) {
    const ImVec2 n = ImGui::CalcTextSize(ds[d].note.c_str(), nullptr, false, note_w);
    w0 = std::max(w0, ImGui::CalcTextSize(pool::kDimDefs[d].name_cn).x);
    w1 = std::max(w1, ImGui::CalcTextSize(ds[d].val.c_str()).x);
    w2 = std::max(w2, n.x);
    h += sp + std::max(f, n.y);
  }
  const float c1 = w0 + f, c2 = c1 + w1 + f; // 取值 / 说明列的起点 (相对内容左沿)
  const float w = std::max(hs.x, c2 + w2);
  ImGui::SetNextWindowContentSize(ImVec2(w, h));
  ImGui::BeginTooltip();
  ImGui::TextUnformatted(head.c_str());
  const ImVec2 at = ImGui::GetCursorScreenPos();
  ImGui::GetWindowDrawList()->AddLine(ImVec2(at.x, at.y - sp * 0.5f), ImVec2(at.x + w, at.y - sp * 0.5f), ImGui::GetColorU32(ImGuiCol_Separator));
  const float x0 = ImGui::GetCursorPosX();
  ImGui::PushTextWrapPos(x0 + c2 + note_w);
  for (int d = 0; d < pool::kDims; ++d) {
    if (!ds[d].on)
      ImGui::PushStyleColor(ImGuiCol_Text, ImGui::GetStyleColorVec4(ImGuiCol_TextDisabled));
    ImGui::TextUnformatted(pool::kDimDefs[d].name_cn);
    ImGui::SameLine(x0 + c1);
    ImGui::TextUnformatted(ds[d].val.c_str());
    ImGui::SameLine(x0 + c2);
    ImGui::TextUnformatted(ds[d].note.c_str());
    if (!ds[d].on)
      ImGui::PopStyleColor();
  }
  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// 池表的池 p: all = 一句话, 子池 = 显示名 + 池名 + 逐维拆解
inline void pool_tip(PoolId p) {
  const PoolDef &d = pool_def(p);
  if (p == kAllPool)
    tip("%s", kAllPoolTip);
  else
    pool_tip(d.spec, d.name_cn + "  " + d.name);
}

} // namespace GUI
