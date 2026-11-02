// Compute Service - Manages feature computation with multi-threaded workers
// High-performance mode: Prefetch + TS workers + CS worker + IO worker (两档同一管线, 见 worker/feature_pipeline.hpp)
// 线程模型 (gui/util/Service.hpp, 随对象): 进度面 = 自身 (done/total = 已落盘天 / 档的日期轴);
//   store / 调度面 / 仪表盘全是档的管线 (tier_ops) 的局部, 一轮一建
#pragma once

#include "features/Storage/FeatureLevels.hpp" // TierId / PerTier / PerLib
#include "gui/util/Service.hpp"
#include "misc/date.hpp"
#include "shared/FeatureLib.hpp"
#include "worker/feature_workers.hpp" // StageLayout (UI 核布局表) / tier_ops

#include <algorithm>
#include <optional>
#include <vector>

// Forward declarations
struct Config;
struct SharedData;

namespace GUI::Features {

// ============================================================================
// 库规划 (库表; GUI 线程: 源日历只读数据层已换进的产物; 扫盘有成本, 只在进页 / 一轮算完 / 数据换进 / Refresh 时调)
//   规划日期轴 = 本档源日历 (tier_ops(t).source_dates, 随同步 / 编码前移) ∩ 上游库日期轴 (lib_parent; prev: 前一交易日也在其上);
//   上游未落盘齐 → 空 (blocked). 请求带走规划日期轴的快照, worker 按它跑 (lib_run)
// ============================================================================
struct LibPlan {
  FeatureLib lib;               // 现清单 (load_lib; 未建库 = 空轴)
  size_t done = 0;              // 已落盘日数 (以档内末层日文件为 commit 标记, 见 FeatureRead::has_date)
  size_t pending = 0;           // 未落盘: 规划日期轴上尚未落盘的日数 (未建库 = 整轴)
  bool resumable = false;       // 续算点对得上 (见 resumable)
  std::vector<misc::Ymd> plan;  // 规划日期轴 (空 = 不可算), 末日 > 库末日即落后
  std::optional<LibId> blocked; // 未落盘齐的上游库 (非空 ⇒ plan 空)
  bool complete() const { return lib.ready() && done == lib.dates.size(); }
  // 可续算: 落盘齐 + 续算点在 + 库日期轴是规划的真前缀 (有新日)
  bool appendable() const {
    return complete() && resumable && plan.size() > lib.dates.size() && std::equal(lib.dates.begin(), lib.dates.end(), plan.begin());
  }
};
PerLib<LibPlan> plan_libs(const SharedData &data); // 按规划序 (lib_order, 上游先) 逐库规划

// 可续算: 库清单末日的续算点在, 且对得上清单轴 + 当前状态指纹 (见 FeatureSnap.hpp); 随轴库另须清单轴 ⊆ 同池定轴库清单轴; 否则只能全量.
// 续算 ≡ 同清单全量 (逐位一致): 轴随清单冻结 (轴外见 FeatureLib::outside), 日频面板按库首日起建, 尾窗日标签此轮结清后重落盘
bool resumable(const Config &cfg, const FeatureLib &lib);

// 一轮的写端规划 (worker 线程; 请求 = 规划日期轴快照 + 全量 / 续算):
//   lib   新清单 = 本库 + 规划日期轴 (A 轴 / 轴外由池面板定, 见 shared/Pool.hpp)
//   old   旧清单 (续算; 全量 = 未建库)
//   dates 管线日期轴 = 规划日期轴从尾窗起; 尾窗 tail = 旧末 min(悬挂日环, 旧日数) 日 (标签可能仍悬挂, 此轮结清; 全量 0 = 整轴)
// 越出上游库 / 不可续算 / 旧日期轴不是规划的真前缀 → assert
struct LibRun {
  FeatureLib lib, old;
  std::vector<misc::Ymd> dates;
  size_t tail = 0;
};
LibRun lib_run(const Config &cfg, LibId id, std::vector<misc::Ymd> plan, bool resume);

// ============================================================================
// Compute Config (= 请求)
// ============================================================================

// 日环深度 / 领养额度不在这里: 管线按负载模拟定初值、运行中按实测调 (见 worker/feature_workers.hpp 时序调参)
struct ComputeConfig {
  // 按档. 预取 / 截面 / 落盘核数: 各自按日轮转, 从时序让核 (Lob 时序是瓶颈, 不让).
  PerTier<int> pf_cores{{1, 1}};
  PerTier<int> cs_cores{{1, 1}};
  PerTier<int> io_cores{{1, 1}};
  LibId lib = kAllBar;          // 算哪个库 (Compute 页每库一行)
  std::vector<misc::Ymd> dates; // 规划日期轴 (LibPlan::plan 的快照)
  bool resume = false;          // 续算 (库末日之后接着算, 须 resumable); 否则全量 (清库重算)
};

// ============================================================================
// Compute Service
// ============================================================================

class ComputeService : public Service<ComputeService, ComputeConfig>, public Progress {
  friend Service<ComputeService, ComputeConfig>;

public:
  static constexpr const char *kName = "ComputeWorker";
  explicit ComputeService(SharedData &data); // 末尾 Start
  ~ComputeService() { Stop(); }

  // 核布局由机器核数 + 本档预取 / 截面 / 落盘核数推导 (StageLayout::make), UI 只交 ComputeConfig; 先断言再转 Service::Request
  void Request(ComputeConfig config);
  bool cancelling() const { return busy() && cancel_.load(std::memory_order_relaxed); }

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return *this; }
  bool run(ComputeConfig &config);

  SharedData &data_;
};

} // namespace GUI::Features
