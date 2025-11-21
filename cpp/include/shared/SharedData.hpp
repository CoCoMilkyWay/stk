#pragma once
#include "./Config.hpp"

struct GuiState;

struct SharedData {
  SharedData() = default;

  Config config;
  
  // Pointer to GUI state for logging (set by GUI)
  GuiState* gui_state = nullptr;
  
  void Log(const std::string &msg);
};
