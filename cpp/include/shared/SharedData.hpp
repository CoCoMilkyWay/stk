#pragma once

struct Config;
struct Asset;
struct AssetInfo;
struct Feature;
struct Dist;
struct TimeSeries;
struct TaskState;
struct TaskTerminal;
struct CoroManager;

#include "./Asset.hpp"
#include "./AssetInfo.hpp"
#include "./Config.hpp"
#include "./OrderFlow.hpp"
#include "./Feature.hpp"
#include "./Dist.hpp"
#include "./TimeSeries.hpp"
#include "./TaskState.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "gui/coro/CoroManager.hpp"

struct SharedData {
  Config config;
  Asset asset;           // L2 database metadata
  AssetInfo assetinfo;  // Stock info from Baostock
  TaskTerminal terminal;
  CoroManager coromgr;
  OrderFlow orderflow;
  Feature feature;
  Dist dist;
  TimeSeries timeseries;
  TaskState taskstate;  // 统一的任务状态管理
  
  // Request full GUI reinitialization (triggered by config sync)
  bool request_reinit = false;
  
  // High Performance Mode: GUI thread sleeps, all CPU for compute tasks
  bool high_performance_mode = false;
  void EnableHighPerformanceMode() { high_performance_mode = true; }
  void DisableHighPerformanceMode() { high_performance_mode = false; }
};
