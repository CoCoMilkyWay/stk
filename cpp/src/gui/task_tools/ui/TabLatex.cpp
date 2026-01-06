#include "gui/task_tools/ui/TabLatex.hpp"
#include "latex.h"
#include "render.h"
#include "platform/imgui/graphic_imgui.h"
#include "imgui.h"

#include <cassert>
#include <windows.h>

namespace GUI::Tools {

LatexEditorState::LatexEditorState() = default;
LatexEditorState::~LatexEditorState() = default;

static std::wstring utf8ToWide(const std::string& utf8) {
  if (utf8.empty()) return L"";
  int size = MultiByteToWideChar(CP_UTF8, 0, utf8.c_str(), -1, nullptr, 0);
  assert(size > 0);
  std::wstring result(size - 1, L'\0');
  MultiByteToWideChar(CP_UTF8, 0, utf8.c_str(), -1, result.data(), size);
  return result;
}

void InitLatexEditor(LatexEditorState& state) {
  if (state.initialized) return;
  tex::LaTeX::init("res");
  state.initialized = true;
}

void DrawLatexEditor(LatexEditorState& state) {
  // Input area
  ImGui::Text("LaTeX Input:");
  if (ImGui::InputTextMultiline("##latex_input", state.input_buffer, sizeof(state.input_buffer),
                                 ImVec2(-1, 100))) {
    state.need_reparse = true;
  }

  // Settings
  if (ImGui::SliderFloat("Text Size", &state.text_size, 10.0f, 50.0f)) {
    state.need_reparse = true;
  }
  if (ImGui::SliderInt("Width", &state.width, 200, 1920)) {
    state.need_reparse = true;
  }

  // Manual render button
  if (ImGui::Button("Render") || state.need_reparse) {
    state.need_reparse = false;
    state.error_msg.clear();

    // Release previous render
    state.render.reset();

    // Parse LaTeX
    std::wstring wlatex = utf8ToWide(state.input_buffer);
    auto* render = tex::LaTeX::parse(wlatex, state.width, state.text_size, 5.0f, tex::black);
    if (render) {
      state.render.reset(render);
    } else {
      state.error_msg = "Failed to parse LaTeX";
    }
  }

  // Error display
  if (!state.error_msg.empty()) {
    ImGui::TextColored(ImVec4(1, 0, 0, 1), "Error: %s", state.error_msg.c_str());
  }

  // Render preview
  ImGui::Separator();
  ImGui::Text("Preview:");

  if (state.render) {
    ImDrawList* draw_list = ImGui::GetWindowDrawList();
    ImVec2 cursor_pos = ImGui::GetCursorScreenPos();

    // Create ImGui graphics context
    tex::Graphics2D_imgui g2(draw_list);

    // Apply offset translation
    g2.translate(cursor_pos.x, cursor_pos.y);

    // Render LaTeX
    state.render->draw(g2, 0, 0);

    // Advance cursor
    ImGui::Dummy(ImVec2((float)state.render->getWidth(), (float)state.render->getHeight()));
  }
}

void CleanupLatexEditor(LatexEditorState& state) {
  state.render.reset();
  if (state.initialized) {
    tex::LaTeX::release();
    state.initialized = false;
  }
}

} // namespace GUI::Tools

