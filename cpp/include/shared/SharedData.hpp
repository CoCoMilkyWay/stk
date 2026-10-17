#pragma once

#include "./Asset.hpp"
#include "./AssetInfo.hpp"
#include "./Config.hpp"
#include "./Correlation.hpp"
#include "./Dist.hpp"
#include "./Feature.hpp"
#include "./FeaturePreview.hpp"
#include "./OrderFlow.hpp"
#include "./TaskState.hpp"
#include "./Transform.hpp"
#include "./Universe.hpp"
#include "features/Method/Fundamental.hpp"
#include "gui/coro/CoroManager.hpp"

#include <atomic>

struct SharedData {
  Config config;
  TaskState taskstate;
  Asset asset;
  AssetInfo assetinfo;
  fund::Pool fund_pool;         // 日频 PIT 基本面数据源 (Phase 2 前 build, TS worker 只读共享)
  universe::Pool universe_pool; // 选池逐日成员 (Lob 库规划时由 all 库 build, TS worker 只读共享 → Meta 的 cs_valid)
  Feature feature;
  FeaturePreview preview;
  OrderFlow orderflow;
  Dist dist;
  Transform transform;
  Correlation corr;   // 特征两两相关矩阵 (Corr tab)
  CorrPair corr_pair; // 悬停对的 lead-lag 曲线
  CorrLag corr_lag;   // 全矩阵 lead-lag 峭点 (Corr tab 勾选 "Lag 图" 才算)

  CoroManager coromgr;

  bool request_reinit = false;
  std::atomic<bool> high_performance_mode{false}; // 编码 / 特征计算的 worker 线程开关, 渲染循环读
  void EnableHighPerformanceMode() { high_performance_mode = true; }
  void DisableHighPerformanceMode() { high_performance_mode = false; }
};
