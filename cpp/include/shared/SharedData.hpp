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
#include "features/Method/Fundamental.hpp"
#include "gui/coro/CoroManager.hpp"

struct SharedData {
  Config config;
  TaskState taskstate;
  Asset asset;
  AssetInfo assetinfo;
  fund::Pool fund_pool; // 日频 PIT 基本面数据源 (Phase 2 前 build, TS worker 只读共享)
  Feature feature;
  OrderFlow orderflow;
  Dist dist;
  TimeSeries timeseries;
  Transform transform;

  CoroManager coromgr;

  bool request_reinit = false;
  bool high_performance_mode = false;
  void EnableHighPerformanceMode() { high_performance_mode = true; }
  void DisableHighPerformanceMode() { high_performance_mode = false; }
};
