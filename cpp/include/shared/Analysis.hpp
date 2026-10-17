#pragma once

#include "features/Backend/FeatureLevels.hpp" // TierId
#include "features/MetaFlag.hpp"              // fmeta::ColumnSet
#include "features/TimeIndex.hpp"
#include "math/distribution/KLLcache.hpp"
#include "math/spectral/DayPSD.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "misc/profiler.hpp"
#include "shared/AssetAxis.hpp"  // UniverseAxis
#include "shared/FeatureLib.hpp" // FeatureLib

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <utility>
#include <vector>

class FeatureRead;
struct Config;
struct Feature;

// ============================================================================
// analysis — Features 页流式分析 (Dist / Transform / FeaturePreview / Correlation / CorrLag / CorrPair) 的公共骨架
// ============================================================================
// 六个目标同一个形状:
//   struct X : Published {
//     struct Spec;  Spec spec;   本次构建的输入 (reset 持锁写; UI 持锁读 = 快照身份)
//     struct Snap;  Snap snap;   发布成品 (批末持锁 swap; UI 渲染帧内持锁只画)
//     void reset(ReadScope, Spec);  bool build(FeatureRead &, cancel);  void clear();
//     (要 Bar 档旁读列的目标改为 bool build(FeatureRead &, const FeatureRead &all, cancel))
//   };
// 同一条流水线, 只是配置不同:  天序 (DayPlan) → IO + 门控 (平面, features/Backend/DayBatchPlane.hpp)
//   → 相 (phase: 一次 dispatch, 返回即栅栏) → 可合并累加器 (按写者分区: 资产独占 / 行独占 / shard + reduce_tree)
//   → 导出 staging Snap → 短锁 swap 发布
//   - A 轴 = universe 子轴 (槽位 = 子轴下标, 查 items 经 global_ids); Dist / Transform / Preview 只在 L1 跑, Corr* 跟随层
//   - 状态面 Published (shared/Progress.hpp): 进度原子 (UI 免锁读) + 发布快照锁;
//     reset 末尾 publish() (数据变了), 每批 publish(批天数), clear 末尾 idle()
//   - Service 层 (gui/task_features/services/StreamService.hpp) 统一单 worker 编排: Request = ReadScope + Spec,
//     GUI 线程解析, worker 不碰 config
// ============================================================================
namespace analysis {

// ---- 共用常量 ----
inline constexpr size_t kLevel = 1;                  // 流式分析只在 L1 跑
inline constexpr size_t kVR = TRADE_MINUTES_PER_DAY; // L1 有效行 (== level_valid_rows(kLevel), build 内断言)
inline constexpr size_t kDaysPerBatch = 8;           // 天是流式维度: 首帧 = 一批的 IO + 计算
inline constexpr size_t kAssetBlock = 64;            // 资产抢块粒度
inline constexpr size_t kDrawAssets = 512;           // PDF 折线绘制子集 (纯 UI 顶点预算, 计算恒为全资产)
inline constexpr size_t kMinAssetSamples = 100;      // 资产 PDF 成线的最少样本
inline constexpr size_t kAssetKllCapacity = 256;     // 每资产 sketch (精度换内存)
inline constexpr size_t kAssetKllResolution = 128;   // 资产 PDF 网格 (细线 128 点足够)
inline constexpr size_t kAggKllCapacity = 512;       // 聚合槽 / 全局 sketch
inline constexpr size_t kAggKllResolution = 256;     // 小面板 ~400px, 255 点 PDF 足够
using DayPSD = math::spectral::DayPSD<kVR>;

// ---- 值账目 (Dist 完整性条 / Feature 表账目列 共用) ----
struct Integrity {
  size_t n_total = 0;
  size_t n_valid = 0;
  size_t n_zero = 0;
  size_t n_nan = 0;
  size_t n_pos_inf = 0;
  size_t n_neg_inf = 0;
  // 无有效样本时恒为 ±inf: 空账目可与任意账目无条件合并 (UI 层 n_valid == 0 时显示 "--").
  // bit 模式构造: 本头会被 fast-math TU 包含, 不能碰 numeric_limits::infinity();
  // ±inf 的比较只发生在 precise-math TU
  float val_min = std::bit_cast<float>(0x7F800000u);
  float val_max = std::bit_cast<float>(0xFF800000u);

  void add(const Integrity &o) {
    n_total += o.n_total;
    n_valid += o.n_valid;
    n_zero += o.n_zero;
    n_nan += o.n_nan;
    n_pos_inf += o.n_pos_inf;
    n_neg_inf += o.n_neg_inf;
    val_min = std::min(val_min, o.val_min);
    val_max = std::max(val_max, o.val_max);
  }
  // 一个有限值入账 (NaN / ±inf 由调用方分账)
  void add_finite(float v) {
    ++n_valid;
    n_zero += (v == 0.0f);
    val_min = std::min(val_min, v);
    val_max = std::max(val_max, v);
  }
  // 平面一格入账 (哨兵口径见 features/Backend/DayBatchPlane.hpp; 纯位判, fast-math TU 包含也安全).
  // n_total 由调用方按格数记; 返回 true = 有限值 (调用方收样本)
  bool add_cell(_Float16 x) {
    const uint16_t u = std::bit_cast<uint16_t>(x);
    if ((u & 0x7C00u) != 0x7C00u) {
      add_finite(static_cast<float>(x));
      return true;
    }
    if ((u & 0x03FFu) == 0)
      ++((u >> 15) ? n_neg_inf : n_pos_inf);
    else
      n_nan += (u == fmeta::kNaNBits);
    return false;
  }

  // 门控不过的格子 (ts_valid 按 valid_type 判否): 两个账目源 (FeaturePreview / Dist) 都只把门控过的格子分到
  // valid / nan / ±inf, 其余即门控不过, 推导得出不另设计数器. 特征应稠密: 这一项比 nan 更要紧
  size_t n_invalid() const {
    assert(n_total >= n_valid + n_nan + n_pos_inf + n_neg_inf);
    return n_total - n_valid - n_nan - n_pos_inf - n_neg_inf;
  }
  float invalid_pct() const { return n_total > 0 ? 100.0f * n_invalid() / n_total : 0.0f; }
  float zero_pct() const { return n_valid > 0 ? 100.0f * n_zero / n_valid : 0.0f; }
  float nan_pct() const { return n_total > 0 ? 100.0f * n_nan / n_total : 0.0f; }
  float inf_pct() const { return n_total > 0 ? 100.0f * (n_pos_inf + n_neg_inf) / n_total : 0.0f; }

  void clear() { *this = Integrity{}; }
};

// ---- PDF 发布快照: KLL 导出成品, UI 零计算只画 ----
template <size_t R>
struct PdfSnap {
  uint64_t n = 0; // 累积样本数
  float mean = 0.0f, var = 0.0f, skew = 0.0f, kurt = 0.0f;
  uint32_t n_pts = 0; // 折线点数; 0 = 样本不足, 不画
  std::array<float, R - 1> x{}, y{};

  float sd() const { return std::sqrt(std::max(0.0f, var)); }

  // n > 0 → 矩; n ≥ min_samples → PDF 折线; 否则 n_pts = 0
  void fill(const KLLcache &kll, size_t min_samples) {
    n = kll.totalCount();
    n_pts = 0;
    if (n == 0)
      return;
    mean = static_cast<float>(kll.mean());
    var = static_cast<float>(kll.var());
    skew = static_cast<float>(kll.skew());
    kurt = static_cast<float>(kll.kurt());
    if (n < min_samples)
      return;
    const auto pdf = kll.exportPDF();
    assert(pdf.n <= x.size());
    n_pts = static_cast<uint32_t>(pdf.n);
    std::copy_n(pdf.x, pdf.n, x.data());
    std::copy_n(pdf.y, pdf.n, y.data());
  }
};
using AssetPdf = PdfSnap<kAssetKllResolution>;
using AggPdf = PdfSnap<kAggKllResolution>;

// [0, n) 固定种子洗牌 (绘制子集 / 抽样序: 无偏且跨构建稳定)
std::vector<uint32_t> shuffled_order(size_t n);

// 每资产一条线 (槽位 == 子轴下标): asset = 全局轴下标 (UI 查 items), draw = PDF 折线绘制子集
struct AssetLine : AssetPdf {
  uint32_t asset = 0;
  uint8_t draw = 0;
};
// 贴 asset (全局轴下标) + 固定种子洗牌取前 kDrawAssets 个为绘制子集 (无偏随机, 画面统计形态与全量等价)
template <class Line>
void init_lines(std::vector<Line> &lines, const std::vector<uint32_t> &global_ids) {
  assert(lines.size() == global_ids.size());
  for (size_t a = 0; a < lines.size(); ++a) {
    lines[a].asset = global_ids[a];
    lines[a].draw = 0;
  }
  const std::vector<uint32_t> order = shuffled_order(lines.size());
  for (size_t i = 0; i < std::min(kDrawAssets, order.size()); ++i)
    lines[order[i]].draw = 1;
}

// ---- 读取范围 (Request 公共头): GUI 线程从 config 解析, worker 只消费 ----
struct ReadScope {
  std::vector<misc::Ym> months; // 升序 (config 区间覆盖的月)
  FeatureLib lib;               // 主库 (档 / A 轴 / 目录, 与写端同一 feature_lib 推导)
  // Bar 档旁读 (涨跌停 / 市值 / 行业等只在 Bar 库的列, 两档分析共用一条路径): 主轴 a ↔ 全轴 lib.uni.ids[a]
  FeatureLib bar;
};
ReadScope read_scope(const Config &cfg, size_t num_assets, TierId tier);

// ---- 请求 (Service → 目标): 读取范围 + 目标自己的 Spec ----
template <class Spec>
struct Request {
  ReadScope scope;
  Spec spec;
};

// GUI 线程: 该档该层元数据 → 列描述 (vt 查元数据; 任一列需门控才追加该层 ts_valid). 不给档 = 当前档
fmeta::ColumnSet column_set(const Feature &feature, TierId tier, size_t level, std::vector<size_t> cols);
fmeta::ColumnSet column_set(const Feature &feature, size_t level, std::vector<size_t> cols);

// 库里实际存在的日期 (升序) + 每日所属月下标 (months 下标)
struct DateList {
  std::vector<misc::Ymd> dates;
  std::vector<uint16_t> month;
};
DateList enumerate_dates(const FeatureRead &reader, const std::vector<misc::Ym> &months);

// ---- 天序 (全部流式分析唯一一份) ----
//   Shuffle:    固定种子洗牌取前 limit 天 —— 无偏, 首批即覆盖全区间 (Dist / Preview / Corr*)
//   Sequential: 升序取前 limit 天 —— 因果 (Transform 的 TOD 轮廓跨天)
enum class DayOrder : uint8_t { Shuffle,
                                Sequential };
struct DayPlan {
  std::vector<misc::Ymd> dates;
  std::vector<uint16_t> month;  // months 下标
  std::vector<uint8_t> weekday; // Mon=0 .. Sun=6
  size_t size() const { return dates.size(); }
};
DayPlan plan_days(const FeatureRead &reader, const std::vector<misc::Ym> &months, DayOrder order, size_t limit); // limit 0 = 全部

// ---- 容量准备 / 线程布局 (幂等, 尺寸对得上零分配) ----
// 槽数对得上就只 clear (KLL 保留 buffer 容量), 对不上才重建
void prepare_slots(std::vector<KLLcache> &slots, size_t n, size_t capacity, size_t resolution);
void clear_slots(std::vector<KLLcache> &slots);
// 子轴 → 全局轴映射必须升序去重非空 (UniverseAxis::ids)
void check_axis(const std::vector<uint32_t> &global_ids);

inline size_t n_asset_blocks(size_t A) { return (A + kAssetBlock - 1) / kAssetBlock; }
// 资产块 blk → [a0, a1)
inline std::pair<size_t, size_t> asset_block(size_t blk, size_t A) {
  const size_t a0 = blk * kAssetBlock;
  return {a0, std::min(a0 + kAssetBlock, A)};
}
// 池线程数: 硬件并发, 按一相最多任务数封顶 (多出的线程每相都空转)
size_t threads_for(size_t max_tasks);

// ---- 批内并行 (misc/exec.hpp): build = 一串相, 每相一次 dispatch (返回即栅栏), 相间代码单线程 ----
// 执行器常驻 Runtime (misc::exec::ensure, 跨 build 存活, worker 线程自当 tid 0)
// 一相: n 个任务 fn(i, tid); 取消后剩余任务空转
template <class Fn>
void run_phase(misc::exec::ForkJoin &ex, size_t n, const std::atomic<bool> &cancel, Fn &&fn) {
  ex.tasks(static_cast<int>(n), [&](int i, int tid) {
    if (!cancel.load(std::memory_order_relaxed))
      fn(static_cast<size_t>(i), static_cast<size_t>(tid));
  });
}
// shard (每 tid 一份) 两两并行归约到 shards[0] (log2 步, 每步一次 dispatch): shards[0] = 全程累加器 (不清), 其余清零.
// Shard 提供 merge_from(const Shard &) / clear_agg()
template <class Shard>
void reduce_tree(misc::exec::ForkJoin &ex, std::vector<Shard> &shards) {
  const size_t n = shards.size();
  for (size_t step = 1; step < n; step <<= 1) {
    const size_t pairs = (n - step + 2 * step - 1) / (2 * step); // dst = 0, 2·step, 4·step, … 且 dst + step < n
    ex.tasks(static_cast<int>(pairs), [&](int k, int) {
      TraceN("Reduce");
      Shard &dst = shards[2 * step * static_cast<size_t>(k)];
      Shard &src = shards[2 * step * static_cast<size_t>(k) + step];
      dst.merge_from(src);
      src.clear_agg();
    });
  }
}

// ---- 共用累加器 ----

// 每资产累积 sketch (Dist / Transform): 资产独占 (每资产单线程写), 免归约
struct AssetSketches {
  std::vector<KLLcache> klls; // [A] 全程累积

  void prepare(size_t A) { prepare_slots(klls, A, kAssetKllCapacity, kAssetKllResolution); }
  // 资产 a 本批样本入账 → 导出整条线; 返回该资产 sketch (调用方再派生, 如 W2)
  const KLLcache &add(size_t a, const float *x, size_t n, AssetPdf &line) {
    KLLcache &kll = klls[a];
    if (n > 0)
      kll.addBatch(x, n);
    line.fill(kll, kMinAssetSamples);
    return kll;
  }
  void clear() { klls = std::vector<KLLcache>{}; }
};

// 单日谱均值 (Transform / Preview): 线性域算术平均, 每 (资产, 天) 一票
struct PsdAcc {
  std::array<double, DayPSD::N_FREQS> sum{};
  uint64_t n = 0;

  // 绝对功率 (Transform: 看变换链改了多少能量分布)
  void add(const DayPSD &psd) {
    for (size_t k = 0; k < DayPSD::N_FREQS; ++k)
      sum[k] += psd.power[k];
    ++n;
  }
  // Parseval 归一: 除以 [1, N/2] 总功率 (跳 DC) → 每 bin = 方差占比, 跨特征绝对可比 (Preview).
  // 恒值日 total = 0 谱无定义, 投 "占比全 0" 一票 (曲线下沉 = 有动的天占比)
  void add_share(const DayPSD &psd) {
    double total = 0.0;
    for (size_t k = 1; k <= DayPSD::N / 2; ++k)
      total += psd.power[k];
    if (total > 0.0) {
      const double inv = 1.0 / total;
      for (size_t k = 1; k <= DayPSD::N / 2; ++k)
        sum[k] += psd.power[k] * inv;
    }
    ++n;
  }
  double mean(size_t k) const { return n > 0 ? sum[k] / static_cast<double>(n) : 0.0; }
  void merge_from(const PsdAcc &o) {
    for (size_t k = 0; k < DayPSD::N_FREQS; ++k)
      sum[k] += o.sum[k];
    n += o.n;
  }
  void clear() { *this = PsdAcc{}; }
};

} // namespace analysis
