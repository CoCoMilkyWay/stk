#pragma once

#include "features/Storage/FeatureLevels.hpp" // PerTier / ALL_TIERS
#include "misc/date.hpp"

#include <atomic>
#include <cassert>
#include <functional>
#include <vector>

struct SharedData;
struct FeatureLib;
struct SnapHeader;
namespace fund {
class Panel;
}
namespace pool {
class Panel;
}

// ============================================================================
// 特征管线入口 (两档同一骨架 pipeline<T>, 定义见 Pipeline.hpp, 经档入口表 TierOps 调): ComputeService 备好共享前置
// (日期轴 / 写读库 / 基本面面板 / 池面板 / 清库 / 日志) 后调用, 返回 = 未取消. on_io(n) = 已落盘天数 (协调线程每 100ms 调, 进度面镜像)
// 跑完 (未取消) 即落本库续算点 (末日 = dates.back(), 见 FeatureSnap.hpp); tail > 0 = 续算, 从 dates[tail − 1] 的续算点接着算
// ============================================================================
struct PipelineArgs {
  SharedData &data;
  const PerTier<FeatureLib> &libs; // 本库 + 同池 CS 旁读档的库 (余档空), 见 StageCtx
  const fund::Panel &fund;         // 日频 PIT 基本面 (全市场, 库日期轴), TS worker 只读共享
  const pool::Panel &members;      // 本库逐日成员 (轴 = 本库 A 轴, 日期轴同 dates), TS worker 只读共享 → Meta 的 cs_valid
  const std::vector<misc::Ymd> &dates;
  size_t tail;  // 尾窗日数, 见 StageCtx
  int pf_cores; // 预取核数 (按日轮转, 见 StageLayout)
  int cs_cores; // 截面核数
  int io_cores; // 落盘核数
  const std::atomic<bool> &cancel;
  std::function<void(size_t)> on_io;
};

// ============================================================================
// 档入口表: 档 TU 各定义一份 <ns>_ops (= tier_ops_of<ns::Tier>(), 见 Pipeline.hpp), 按运行时档号取 (tier_ops);
// 调用方 (ComputeService / 库表) 不碰档类型与字段表
// ============================================================================
struct TierOps {
  const char *unit;                                           // 时序工作量单位 (TierFeed::kUnit)
  size_t pend_days;                                           // 悬挂日环 = 续算尾窗上限 (LabelHold::PEND_DAYS)
  std::vector<misc::Ymd> (*source_dates)(const SharedData &); // 源日历: 本档原始数据现在能服务的交易日 (TierFeed::source_dates)
  SnapHeader (*snap_header)(const FeatureLib &, misc::Ymd);   // 续算点头: 库截到某日可续算的全部前提 (见 FeatureSnap.hpp)
  bool (*pipeline)(const PipelineArgs &);                     // 管线, 返回 = 未取消
};
#define TIER_OPS_DECL(T, ns) extern const TierOps ns##_ops;
ALL_TIERS(TIER_OPS_DECL)
#undef TIER_OPS_DECL
inline const TierOps &tier_ops(TierId t) {
#define TIER_OPS_ONE(T, ns) \
  if (t == TierId::T)       \
    return ns##_ops;
  ALL_TIERS(TIER_OPS_ONE)
#undef TIER_OPS_ONE
  assert(false && "tier_ops: 未知档");
  __builtin_unreachable();
}
