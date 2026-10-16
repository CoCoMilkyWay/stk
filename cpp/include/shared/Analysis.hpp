#pragma once

#include "features/TimeIndex.hpp"
#include "math/distribution/KLLcache.hpp"
#include "math/spectral/DayPSD.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "misc/profiler.hpp"
#include "shared/AssetAxis.hpp" // UniverseAxis

#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

class FeatureRead;
struct Config;

// ============================================================================
// analysis — Features 页三个流式分析 (Dist / Transform / FeaturePreview) 的公共骨架
// ============================================================================
// 三者同一套契约:
//   - 只在 L1 跑; A 轴 = universe 子轴 (槽位 = 子轴下标, 查 items 经 global_ids)
//   - 状态面 Published (shared/Progress.hpp): 进度原子 (UI 免锁读) + 发布快照锁 (worker 短锁写, UI 渲染帧内持锁读);
//     reset_for_build 末尾 publish() (数据变了), 每批 publish(批天数), clear 末尾 idle()
//   - 发布成品: PdfSnap (KLL 导出好的 PDF 折线 + 矩), UI 零计算只画
//   - Service 层 (gui/task_features/services/StreamService.hpp) 统一单 worker 编排,
//     Request 都以 ReadScope (月份表 + 子轴 + 库目录) 开头, GUI 线程解析, worker 不碰 config
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
  UniverseAxis uni;             // universe 子轴 (A 轴/文件列序/目录都由它定)
  std::string features_dir;     // 该 universe 的特征库目录 (Config::FeatureUniverseDir)
};
ReadScope read_scope(const Config &cfg, size_t num_assets);

// 库里实际存在的日期 (升序) + 每日所属月下标 (months 下标)
struct DateList {
  std::vector<misc::Ymd> dates;
  std::vector<uint16_t> month;
};
DateList enumerate_dates(const FeatureRead &reader, const std::vector<misc::Ym> &months);

// ---- 容量准备 / 线程布局 (幂等, 尺寸对得上零分配) ----
// 槽数对得上就只 clear (KLL 保留 buffer 容量), 对不上才重建
void prepare_slots(std::vector<KLLcache> &slots, size_t n, size_t capacity, size_t resolution);
void clear_slots(std::vector<KLLcache> &slots);
// 子轴 → 全局轴映射必须升序去重非空 (UniverseAxis::ids)
void check_axis(const std::vector<uint32_t> &global_ids);

inline size_t n_asset_blocks(size_t A) { return (A + kAssetBlock - 1) / kAssetBlock; }
// 执行器线程数: 硬件并发, 按 max(批天数, 扫描块数) 封顶 (多出的线程每相都空转)
size_t thread_count(size_t A);

// ---- 批内并行 (misc/exec.hpp): build = 一串相, 每相一次 ex.tasks (返回即栅栏), 相间代码单线程 ----
// 执行器常驻 Runtime (misc::exec::ensure, 跨 build 存活, worker 线程自当 tid 0)
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

} // namespace analysis
