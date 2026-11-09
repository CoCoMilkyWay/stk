// 因子面: Alpha (Operators / Factors / Inspect) 与 Market (NAV / VAL / FLOW / CTA) 共用的服务与状态; CreateAllTasks 建, 两任务各持一个 shared_ptr.
//   作用域 (池 + 因子层) 由 Alpha 页顶条自持: 所有服务空闲时生效 (lv / pool); 换层 = 四个服务整体重建, 同层换池只换目录.
//   字段表按层建一次 (Factors / Search / Inspect / 组合共享, 换层不重建).
//   组合 (与层无关): Alpha→Factors 页 1. 区右侧两小框编辑, Market 各页画; 组合内容变 / 因子重扫后重解析, 有效条目变才发请求
//   (PortfolioService 只算新增的因子; 篮子账不依赖组合, all 标签库建好即算).
#pragma once

#include "gui/task_alpha/services/InspectService.hpp"
#include "gui/task_alpha/services/KlineService.hpp"
#include "gui/task_alpha/services/OperatorsService.hpp"
#include "gui/task_alpha/services/RunService.hpp"
#include "gui/task_alpha/services/SearchService.hpp"
#include "gui/task_alpha/ui/TabFactors.hpp"
#include "gui/task_alpha/ui/TabInspect.hpp"
#include "gui/task_alpha/ui/TabOperators.hpp"
#include "gui/task_market/services/PortfolioService.hpp"
#include "gui/task_market/ui/TabPortfolio.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Alpha {

struct AlphaHub {
  std::unique_ptr<OperatorsService> operators_service;
  OperatorsUIState operators_ui;
  bool operators_started = false; // 首次 Draw 载入本地 json, 没有才 Request 一次
  // 一轮结束检测 (Running → Done / Cancelled 那一帧把算子表落地 operators.json)
  Phase prev_operators_phase = Phase::Idle;

  // 页顶条的选择 (Alpha 自持)
  PoolId want_pool = kAllPool;
  LevelId want_lv = level_of(L_1min);
  // 已生效的 (池, 因子层): 所有服务空闲时才追上选择; lv 空 = 未进过 Factors / Inspect (无服务)
  std::optional<LevelId> lv;
  std::string pool;
  // 页顶层按钮后的因子文件数 (所选池各因子层目录): 池变 / 因子服务进 Done (扫描 / Run / Add / 删除后的 Rescan) 时重数
  std::string counted_key; // config.factor_dir | 所选池; 空 = 没数过. 变 (进页 / 换池) → 按因子文件数重定默认层
  PerLevel<size_t> level_files;
  std::vector<size_t> pool_files; // [PoolId] 页顶池下拉里各池因子文件数 (该池各因子层之和): 每次打开下拉重数
  Phase prev_factors_phase = Phase::Idle;
  FeatureTables tables; // 各层字段表视图 (按层建一次, Factors / Search / Inspect / 组合共享, 换层不重建)
  std::unique_ptr<RunService> run_service;
  FactorsUIState factors_ui;
  std::string factors_scanned_dir;               // 上次扫描的目录 (池切换 → 自动重扫)
  std::unique_ptr<SearchService> search_service; // Factors 页 Search (与 run_service 同建); 写了 weights 的那一轮结束 → 自动 Run
  Phase prev_search_phase = Phase::Idle;

  std::unique_ptr<InspectService> inspect_service; // 首次进 Factors / Inspect 页建 (构造即起 worker); 缓存 (标签 + 特征平面) 随它活到任务 Destroy
  std::unique_ptr<KlineService> kline_service;     // Inspect 右下选中资产的分钟 K 线流 (与 inspect_service 同生灭)
  InspectUIState inspect_ui;

  // 组合 (与层无关, 换层不重建; 首次进 Factors / Inspect 页或 Market 行情页建)
  std::unique_ptr<PortfolioService> portfolio_service;
  PortfolioUIState portfolio_ui;
  uint64_t portfolio_gen = ~0ull;        // 已解析的 portfolio_ui.content_gen
  bool portfolio_dirty = false;          // 解析过, 请求还没发 (资产轴未就绪时挂着)
  std::string portfolio_sig;             // 上次请求的内容签名
  Phase portfolio_prev_fs = Phase::Idle; // 因子服务进 Done (因子文件可能改了) → 重解析
};

// 组合两端每帧都调 (Alpha 的 Factors / Inspect, Market 的行情页): 建服务 + 目录 / 自选同步
void EnsurePortfolio(const SharedData &data, AlphaHub &s);
// 组合: 内容变 / 因子服务进 Done → 重解析; 有效条目 / 池集合变 (或 reload) 才发请求, 不打断在跑的
void SyncPortfolio(const SharedData &data, AlphaHub &s, bool reload);

} // namespace GUI::Alpha