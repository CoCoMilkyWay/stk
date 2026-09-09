#pragma once

#include "features/TimeIndex.hpp"
#include "shared/Analysis.hpp"

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

class FeatureRead;

// ============================================================================
// Distribution Analysis (KLL-based, 分批流式; 公共骨架见 shared/Analysis.hpp)
// ============================================================================
// 天是流式维度: 每批 kDaysPerBatch 个抽样天, 批内扫全部资产, 全部视图逐批收敛.
// 首帧 = 第一批的 IO + 扫描 (几十 ms), 与总区间长度无关 —— 列存按天分文件,
// 任何"某资产的完整历史"都要等全量 IO, 所以终态式发布与快速首帧不可兼得,
// 这里选收敛式: 视图从第一批起就是全市场全时段的完整图景, 只是精度逐批收紧.
//
// 发布协议 (单调收敛, UI 画已发布快照; 与 Transform 同一 flow):
//
//   worker (DistService 单线程编排; build 起一波 n_threads 常驻线程, 每批三道栅栏):
//     for 每批 kDaysPerBatch 个抽样天:
//       Phase IO:   抢单天并行载入 → 资产主序批平面 [A][批天][分钟] (DayBatchPlane, 与 Transform 共用:
//                   f16; valid 门控与真 NaN 折叠成统一哨兵, NaN 就地记账)
//       ── 栅栏 ──
//       Phase 扫描: 抢 kAssetBlock 个活跃资产一块, 全在锁外:
//                   每个资产: integrity 账目 + stride 抽样喂聚合槽私有副本 (shard)
//                   + 全量样本 → 该资产私有 sketch → 顺手导出整条 AssetLine (PDF/矩/W2) 到 staging.
//                   活跃资产一视同仁 —— 每批覆盖 universe × 批内天, 收敛维度只有天,
//                   跑完即 universe × 全区间的终态 (UI 只画其中固定随机子集的折线)
//       ── 栅栏 ──
//       Phase 归约: shard 两两并行归约 (log2 步) → shard 0 并入 Runtime 累加器 (全程锁外)
//       ── 栅栏 (completion, 单线程): 累加器导出 PdfSnap 成品 (锁外) → 短锁 swap 发布 + 进度 + epoch ──
//
//   universe: A 轴 = universe 子轴 (与特征库文件列序一致, 见 UniverseAxis) ——
//     平面 / lines / sketch 都只有子轴大小, 每个槽都是活跃资产, 不存在空列.
//     槽位 = 子轴下标; AssetLine.asset 存全局轴下标 (UI 查 items/行业用).
//
//   UI (每帧): 持 mutex 渲染; 全部视图消费 PdfSnap 快照, 零计算零重建只画
//     (增量收敛每批都作废 sketch 缓存, 拉模式会让 UI 每帧重建几百条 — 故推模式).
//   四个维度对仗: 月度漂移 (months) / 周内偏移 (by_weekday) / 日内偏移 (by_tod) / 资产截面 (lines),
//     UI 一条通用焦点滑条按选中维度切换 (焦点槽 = 高亮 + 详情).
//   重算: reset 不清展示字段 (旧图留住), 首批 publish 整体覆盖 —— 与 Transform 同一约定.
//   生命周期: 进 Features 任务且输入就绪 → 起 worker; 选中特征/层变了 → 新请求取消在跑重算;
//             切走 Tab → 只中断, 内存与 worker 保留; 切回 Tab → 自动重算; 切出 Features 任务 → clear() 整体释放.
// ============================================================================

static constexpr size_t kMinSamples = 1000; // 全局 sketch 达到此样本数才作 W2 参考
static constexpr int kW2Deciles = 19;       // W2 用的分位点: 5%, 10%, ..., 95%

// 总样本预算: 超出则日抽样 (stride 与交易周互质避免星期偏置). 分批后平面只存一批,
// 内存不再约束天数 —— 这个预算只是总扫描/IO 时长的旋钮, 5 年 × 5000 标的在预算内全量.
static constexpr size_t kMaxTotalSamples = size_t(2) << 30;

// 日内桶: 按时钟 10 分钟对齐 (09:15-09:19 单独一桶 = 集合竞价; 午休不跨桶), 共 kTodBins 个.
// 桶键 = 时钟分钟 / 10, 上下午各自连续编号 → 槽位紧凑无空洞.
static constexpr size_t kTodBinMinutes = 10;
constexpr size_t tod_bin_of(size_t l1) {
  const ClockTime c = L1_to_Clock(l1);
  const size_t key = (static_cast<size_t>(c.hour) * 60 + c.minute) / kTodBinMinutes;
  constexpr size_t key_am0 = MORNING_START_MIN / kTodBinMinutes;
  constexpr size_t n_am = (MORNING_END_MIN - 1) / kTodBinMinutes - key_am0 + 1;
  constexpr size_t key_pm0 = AFTERNOON_START_MIN / kTodBinMinutes;
  return l1 < MORNING_MINUTES ? key - key_am0 : n_am + key - key_pm0;
}
static constexpr size_t kTodBins = tod_bin_of(TRADE_MINUTES_PER_DAY - 1) + 1;
static_assert(tod_bin_of(0) == 0 && tod_bin_of(MORNING_MINUTES) == tod_bin_of(MORNING_MINUTES - 1) + 1);
// 桶首分钟 (L1 下标) → UI 标签 "HH:MM"
constexpr std::array<uint16_t, kTodBins> make_tod_bin_start() {
  std::array<uint16_t, kTodBins> s{};
  for (size_t l1 = TRADE_MINUTES_PER_DAY; l1-- > 0;)
    s[tod_bin_of(l1)] = static_cast<uint16_t>(l1);
  return s;
}
inline constexpr auto kTodBinStart = make_tod_bin_start();

// 聚合槽 (月/星期/日内/全局) 的目标样本量. KLL 的分位误差 ε≈1/k 只由容量决定, 与样本数
// 无关 —— 5 年全市场灌进去的几亿样本, 最终也只存下 k·log2(n/k) ≈ 1 万个点, 早已饱和.
// 所以按总量自适应 stride 抽到这个量级即可, 图上看不出差别. 每资产的资产槽仍吃全量.
static constexpr size_t kAggTargetSamples = size_t(32) << 20; // 32M

struct Dist : analysis::StreamState {
  using Integrity = analysis::Integrity;

  // 每资产一条线 (槽位 == 子轴下标) + 相对全局分位的 W2 偏移
  struct AssetLine : analysis::AssetLine {
    float w2 = -1.0f; // 均值校准 W2, 相对上一批末的全局分位 (逐批收敛); < 0 = 参考未就绪
  };

  // ==========================================================================
  // 发布快照 (worker 批末短锁 swap; UI 持锁只画)
  // ==========================================================================
  std::vector<analysis::AggPdf> months;     // [n_months] 月度漂移 (月键 = config 区间月份表, UI 自持)
  std::vector<analysis::AggPdf> by_weekday; // [7]
  std::vector<analysis::AggPdf> by_tod;     // [kTodBins] 日内 10 分钟桶
  analysis::AggPdf global;                  // 全区间
  std::vector<AssetLine> lines;             // [A_sub] 槽位 == 子轴下标, .asset = 全局轴下标
  Integrity integrity;                      // 全区间
  // 聚合槽抽样 stride (1 = 全量). 月/星期/日内/全局的 n 是抽样后的数, 资产线恒为全量 ——
  // UI 得把这个比例说出来, 免得两边的 n 并列看着矛盾.
  std::atomic<size_t> agg_stride{1};

  // ==========================================================================
  // Methods (worker 线程调用)
  // ==========================================================================

  Dist();
  ~Dist();

  // 重置构建参数并进入 Building. cols = [值列 (+ valid 列)]; global_ids = 子轴 → 全局轴映射
  // (UniverseAxis::ids). 展示字段不清 (旧图留住), universe 变了才整体重建.
  void reset_for_build(std::vector<size_t> cols, std::vector<std::string> month_keys,
                       std::vector<uint32_t> global_ids);

  // 全区间构建: 分批流式 (每批 IO → 扫描 → 归约 → 发布); 被取消返回 false
  bool build(FeatureRead &reader, const std::atomic<bool> &cancel);

  void clear();

private:
  struct Runtime; // worker 私有 (批平面 / sketch / staging / shard), 定义在 Dist.cpp
  std::unique_ptr<Runtime> rt_;
  // 构建参数 (UI 不看): reset 时定好, build 全程只读
  std::vector<size_t> columns_;
  std::vector<std::string> months_;
  std::vector<uint32_t> global_ids_;
};
