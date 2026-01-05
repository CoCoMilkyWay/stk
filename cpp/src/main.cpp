#include "gui/Gui.hpp"
#ifdef _WIN32
#include <windows.h>
#endif

int main() {
#ifdef _WIN32
  // Set console to UTF-8 mode (Windows only)
  SetConsoleOutputCP(CP_UTF8);
  SetConsoleCP(CP_UTF8);
#endif

  return GUI::RunGUI();
}
