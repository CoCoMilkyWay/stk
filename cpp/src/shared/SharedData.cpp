#include "shared/SharedData.hpp"
#include "gui/GuiState.hpp"

void SharedData::Log(const std::string &msg) {
  // Only add to GUI terminal (no console output)
  if (gui_state) {
    gui_state->terminal.AddLine(msg);
  }
}

