#pragma once

#include "./Asset.hpp"
#include "./AssetInfo.hpp"
#include "./Config.hpp"
#include "./Feature.hpp"
#include "./TaskState.hpp"

#include <atomic>

struct SharedData {
  Config config;
  TaskState taskstate;
  Asset asset;
  AssetInfo assetinfo;
  Feature feature;

  bool request_reinit = false;
  std::atomic<bool> high_performance_mode{false}; // 编码 / 特征计算的 worker 线程开关, 渲染循环读
  void EnableHighPerformanceMode() { high_performance_mode = true; }
  void DisableHighPerformanceMode() { high_performance_mode = false; }
};
