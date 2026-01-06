#pragma once
#include <memory>
#include <string>

namespace tex {
class TeXRender;
}

namespace GUI::Tools {

struct LatexEditorState {
  char input_buffer[4096] = "\\int_{0}^{\\infty} e^{-x^2} dx = \\frac{\\sqrt{\\pi}}{2}";
  std::unique_ptr<tex::TeXRender> render;
  float text_size = 20.0f;
  int width = 720;
  bool need_reparse = true;
  std::string error_msg;
  bool initialized = false;
  
  LatexEditorState();
  ~LatexEditorState();
};

void InitLatexEditor(LatexEditorState& state);
void DrawLatexEditor(LatexEditorState& state);
void CleanupLatexEditor(LatexEditorState& state);

} // namespace GUI::Tools

