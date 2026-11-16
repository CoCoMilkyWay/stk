#pragma once

struct SharedData;

namespace GUI {

struct TaskTree;

// Main entry point (called by different pipeline implementations)
// Returns 0 on success, non-zero on error
int RunGUI();

// Shared business logic: Draw GUI layout (used by both OpenGL and Vulkan)
void DrawGUILayout(SharedData &data, TaskTree &tree);

// 界面字体 (两 pipeline 共用): 主字体 MapleMono 缺的字形逐字落到合并的后备字体 DroidSansFallback
// (CJK 全集 / 全角字母 / 各类符号), 任何来源的文本都不出豆腐块; dpi_scale = framebuffer / window
void LoadFonts(float dpi_scale);

} // namespace GUI
