#pragma once
#include "./Config.hpp"
#include "./GuiState.hpp"

struct SharedData {
  SharedData() = default;

  Config config;
  GuiState *gui_state = nullptr;
};
