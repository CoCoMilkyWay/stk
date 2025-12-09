#pragma once

struct Config;
struct Asset;
struct GuiState;
struct Feature;
struct Dist;

#include "./Asset.hpp"
#include "./Config.hpp"
#include "./GuiState.hpp"
#include "./OrderFlow.hpp"
#include "./Feature.hpp"
#include "./Dist.hpp"

struct SharedData {
  Config config;
  Asset asset;
  GuiState gui;
  OrderFlow orderflow;
  Feature feature;
  Dist dist;
  
  // Request full GUI reinitialization (triggered by config sync)
  bool request_reinit = false;
};
