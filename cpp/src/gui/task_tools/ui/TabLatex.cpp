#include "gui/task_tools/ui/TabLatex.hpp"
#include "latex.h"
#include "render.h"
#include "platform/imgui/graphic_imgui.h"
#include "imgui.h"

#include <cassert>

namespace GUI::Tools {

LatexEditorState::LatexEditorState() = default;
LatexEditorState::~LatexEditorState() = default;

static std::wstring utf8ToWide(const std::string& utf8) {
  if (utf8.empty()) return L"";
  std::wstring result;
  result.reserve(utf8.size());
  
  size_t i = 0;
  while (i < utf8.size()) {
    unsigned char c = utf8[i];
    wchar_t wc = 0;
    
    if (c < 0x80) {
      wc = c;
      i++;
    } else if ((c & 0xE0) == 0xC0) {
      assert(i + 1 < utf8.size());
      wc = ((c & 0x1F) << 6) | (utf8[i + 1] & 0x3F);
      i += 2;
    } else if ((c & 0xF0) == 0xE0) {
      assert(i + 2 < utf8.size());
      wc = ((c & 0x0F) << 12) | ((utf8[i + 1] & 0x3F) << 6) | (utf8[i + 2] & 0x3F);
      i += 3;
    } else if ((c & 0xF8) == 0xF0) {
      assert(i + 3 < utf8.size());
      uint32_t codepoint = ((c & 0x07) << 18) | ((utf8[i + 1] & 0x3F) << 12) 
                         | ((utf8[i + 2] & 0x3F) << 6) | (utf8[i + 3] & 0x3F);
      i += 4;
      if (sizeof(wchar_t) == 2 && codepoint > 0xFFFF) {
        codepoint -= 0x10000;
        result.push_back(0xD800 + (codepoint >> 10));
        result.push_back(0xDC00 + (codepoint & 0x3FF));
        continue;
      }
      wc = codepoint;
    } else {
      assert(false);
      i++;
      continue;
    }
    
    result.push_back(wc);
  }
  
  return result;
}

void DrawLatexEditor(LatexEditorState& state) {
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
    auto* render = tex::LaTeX::parse(wlatex, 0, state.text_size, 5.0f, tex::yellow);
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

} // namespace GUI::Tools

