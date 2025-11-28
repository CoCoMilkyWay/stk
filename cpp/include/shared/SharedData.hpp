#pragma once

struct Config;
struct Asset;
struct GuiState;

#include "./Asset.hpp"
#include "./Config.hpp"
#include "./GuiState.hpp"

struct SharedData {
  Config config;
  Asset asset;
  GuiState gui;
  
  // Request full GUI reinitialization (triggered by config sync)
  bool request_reinit = false;
};
