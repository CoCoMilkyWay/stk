#pragma once

#include "./Asset.hpp"
#include "./AssetInfo.hpp"
#include "./Config.hpp"
#include "./Corr.hpp"
#include "./Dist.hpp"
#include "./Feature.hpp"
#include "./OrderFlow.hpp"
#include "./Preview.hpp"
#include "./TaskState.hpp"
#include "./Transform.hpp"

#include <atomic>

struct SharedData {
  Config config;
  TaskState taskstate;
  Asset asset;
  AssetInfo assetinfo;
  Feature feature;
  Preview preview;
  OrderFlow orderflow;
  Dist dist;
  Transform transform;
  Corr corr;          // 特征两两相关矩阵 (Corr tab)
  CorrPair corr_pair; // 悬停对的 lead-lag 曲线
  CorrLag corr_lag;   // 全矩阵 lead-lag 峭点 (Corr tab 勾选 "Lag 图" 才算)

  bool request_reinit = false;
  std::atomic<bool> high_performance_mode{false}; // 编码 / 特征计算的 worker 线程开关, 渲染循环读
  void EnableHighPerformanceMode() { high_performance_mode = true; }
  void DisableHighPerformanceMode() { high_performance_mode = false; }
};
