#pragma once

struct Config;
struct Asset;
struct GuiState;

#include "./Asset.hpp"
#include "./Config.hpp"
#include "./GuiState.hpp"

struct SharedData {
  SharedData() = default;

  Config config;
  Asset asset;
  GuiState *gui_state = nullptr;
};
