#include "gui/task_tools/ui/TabLatex.hpp"
#include "imgui.h"
#include "latex.h"
#include "platform/imgui/graphic_imgui.h"
#include "render.h"
#include "utfcpp/utf8.hpp"

namespace GUI::Tools {

LatexEditorState::LatexEditorState() = default;
LatexEditorState::~LatexEditorState() = default;

static std::wstring utf8ToWide(std::string_view s) {
  auto u16 = utf8::utf8to16(s);
  return {u16.begin(), u16.end()};
}

void DrawLatexEditor(LatexEditorState &state) {
  // Rebuild font atlas if new fonts were added
  tex::Font_imgui::rebuildFontAtlasIfNeeded();

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

  if (state.need_reparse) {
    state.need_reparse = false;
    state.error_msg.clear();

    // Release previous render
    state.render.reset();

    // Parse LaTeX
    std::wstring wlatex = utf8ToWide(state.input_buffer);
    auto *render = tex::LaTeX::parse(wlatex, 0, state.text_size, 5.0f, tex::yellow);
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
    ImDrawList *draw_list = ImGui::GetWindowDrawList();
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

} // namespace GUI::Tools
