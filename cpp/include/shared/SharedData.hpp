#pragma once

#include "./Asset.hpp"
#include "./AssetInfo.hpp"
#include "./Config.hpp"
#include "./Dist.hpp"
#include "./Feature.hpp"
#include "./OrderFlow.hpp"
#include "./TaskState.hpp"
#include "./TimeSeries.hpp"
#include "./Transform.hpp"
#include "gui/coro/CoroManager.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"

struct SharedData {
  Config config;
  TaskState taskstate;
  Asset asset;
  AssetInfo assetinfo;
  Feature feature;
  OrderFlow orderflow;
  Dist dist;
  TimeSeries timeseries;
  Transform transform;

  CoroManager coromgr;
  TaskTerminal terminal;

  bool request_reinit = false;
  bool high_performance_mode = false;
  void EnableHighPerformanceMode() { high_performance_mode = true; }
  void DisableHighPerformanceMode() { high_performance_mode = false; }
};
