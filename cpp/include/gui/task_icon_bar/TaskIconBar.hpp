#pragma once

struct GuiState;

namespace GUI::TaskIconBar {

void InitIconBar(GuiState &gui);
void DrawIconBar();
void CleanupIconBar();

} // namespace GUI::TaskIconBar
