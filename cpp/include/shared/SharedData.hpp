#pragma once

#include "./Asset.hpp"
#include "./AssetInfo.hpp"
#include "./Config.hpp"
#include "./Feature.hpp"
#include "./TaskState.hpp"

#include <atomic>

struct SharedData {
  Config config;
  TaskState task_state;
  Asset asset;
  AssetInfo asset_info;
  Feature feature;

  bool request_reinit = false;
  std::atomic<bool> high_performance_mode{false}; // 编码 / 特征计算在飞 (各服务 run 首尾写), 渲染循环读以降帧率
};
