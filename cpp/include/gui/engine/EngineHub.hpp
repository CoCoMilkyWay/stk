// EngineHub: 因子引擎的跨任务状态, Alpha (写因子 / 编辑组合) 与 Market (NAV / VAL / FLOW 画组合) 共用一份; CreateAllTasks 建, 两任务各持一个 shared_ptr.
//   字段表按层建一次 (Factors / Search / Inspect / 组合共享, 换层不重建); 后端选择全部引擎请求同用.
//   组合 (与层无关): Alpha→Factors 页 1. 区右侧两小框编辑 (portfolio_ui), Market 行情页画; 组合内容变 / 因子文件变后重解析, 有效条目变才发请求
//   (PortfolioService 只算新增的因子; 篮子账不依赖组合, all 标签库建好即算).
#pragma once

#include "gui/engine/Table.hpp" // FeatureTables
#include "gui/engine/services/PortfolioService.hpp"
#include "gui/engine/ui/PortfolioBoxes.hpp" // PortfolioUIState

#include <cstdint>
#include <memory>
#include <string>

struct SharedData;

namespace GUI::Engine {

struct EngineHub {
  FeatureTables tables;   // 各层字段表视图
  int backend = -1;       // 0 = CPU, 1 = GPU; -1 = 未定, 首帧按 GPU 可用性定 (有 GPU 默认 GPU)
  uint64_t files_gen = 0; // 因子文件代: Alpha 的 RunService 每进一次 Done (扫描 / Run / Add / 删除后的 Rescan) +1

  // 组合 (首次进 Factors / Inspect 页或 Market 行情页建)
  std::unique_ptr<PortfolioService> portfolio_service;
  PortfolioUIState portfolio_ui;
  uint64_t portfolio_gen = ~0ull; // 已解析的 portfolio_ui.content_gen
  uint64_t portfolio_files = 0;   // 已解析时的 files_gen
  bool portfolio_dirty = false;   // 解析过, 请求还没发 (资产轴未就绪时挂着)
  std::string portfolio_sig;      // 上次请求的内容签名

  bool gpu() const { return backend == 1; }
  void reset(); // 析构 Stop() join worker, 状态回初值
};

// 组合两端每帧都调 (Alpha 的 Factors / Inspect, Market 的行情页): 建服务 + 首次列组合
void EnsurePortfolio(EngineHub &e);
// 组合: 内容变 / 因子文件变 → 重解析; 有效条目 / 池集合变 (或 reload) 才发请求, 不打断在跑的
void SyncPortfolio(const SharedData &data, EngineHub &e, bool reload);

} // namespace GUI::Engine
