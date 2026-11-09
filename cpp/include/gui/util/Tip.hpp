#pragma once

// 悬停提示: 固定宽度处折行 (SetTooltip 不折行, 长句会被视口裁掉).
// 写法约定: 首行一句话说用途; 多个方面要讲时一方面一行, "• " 开头, 同类行对仗

#include "imgui.h"

#include <cstdarg>

namespace GUI {

inline constexpr float kTipWrapEm = 48.0f;

inline void tip(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
inline void tip(const char *fmt, ...) {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(ImGui::GetCursorPosX() + ImGui::GetFontSize() * kTipWrapEm);
  va_list args;
  va_start(args, fmt);
  ImGui::TextV(fmt, args);
  va_end(args);
  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

} // namespace GUI
