#pragma once

struct Config;
struct Asset;
struct GuiState;
struct Feature;

#include "./Asset.hpp"
#include "./Config.hpp"
#include "./GuiState.hpp"
#include "./OrderFlow.hpp"
#include "./Feature.hpp"

struct SharedData {
  Config config;
  Asset asset;
  GuiState gui;
  OrderFlow orderflow;
  Feature feature;
  
  // Request full GUI reinitialization (triggered by config sync)
  bool request_reinit = false;
};
