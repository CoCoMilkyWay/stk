#include "gui/Gui.hpp"
#include <windows.h>

int main() {
  // Set console to UTF-8 mode
  SetConsoleOutputCP(CP_UTF8);
  SetConsoleCP(CP_UTF8);

  return GUI::RunGUI();
}
