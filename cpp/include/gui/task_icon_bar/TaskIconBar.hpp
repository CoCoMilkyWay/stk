#pragma once

struct GuiState;

namespace GUI::TaskIconBar {

void InitIconBar(GuiState &gui_state);
void DrawIconBar();
void CleanupIconBar();

} // namespace GUI::TaskIconBar
