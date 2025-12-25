#pragma once

struct CoroManager;

namespace GUI::TaskIconBar {

void InitIconBar(CoroManager &coromgr);
void DrawIconBar();
void CleanupIconBar();

} // namespace GUI::TaskIconBar
