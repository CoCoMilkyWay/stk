#pragma once

struct Config;
struct Asset;
struct AssetInfo;
struct GuiState;
struct Feature;
struct Dist;

#include "./Asset.hpp"
#include "./AssetInfo.hpp"
#include "./Config.hpp"
#include "./GuiState.hpp"
#include "./OrderFlow.hpp"
#include "./Feature.hpp"
#include "./Dist.hpp"

struct SharedData {
  Config config;
  Asset asset;           // L2 database metadata
  AssetInfo asset_info;  // Stock info from Baostock
  GuiState gui;
  OrderFlow orderflow;
  Feature feature;
  Dist dist;
  
  // Request full GUI reinitialization (triggered by config sync)
  bool request_reinit = false;
};
