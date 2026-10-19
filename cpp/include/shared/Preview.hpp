#pragma once

#include "shared/Analysis.hpp"
#include "shared/Progress.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

class FeatureRead;

// ============================================================================
// Preview (特征表内联预览: 每特征 平均分布 + 平均单日频谱, 轮训抽样流式;
//                 公共骨架见 shared/Analysis.hpp)
// ============================================================================
// 目的: Feature 表每行尾部两个迷你曲线 (PDF / PSD), 全表一眼对比形态.
// 与 Dist/Transform 维度反转 —— 那边单特征全数据, 这边全特征抽样数据:
//
//   每层各跑一份 (网格 = 该层, 段 = 日内层 1 天 / L_day 连续 255 天), 各层轮交错 (层 0 轮 0, 层 1 轮 0, …);
//   轮 = 一个抽样段 (plan_segs Shuffle 取前 kPvRounds 个 → 无偏覆盖);
//   轮首单线程读笼: Bar 档 L_day lim_dn / lim_up 逐日读 (当日常量, 盘前已知 → 同日对照, 不走 as-of; 按 lib.axis.ids 取全轴);
//   轮内按特征分块 (每块 kPvBlockCols 列), 一块一任务 (Runtime 常驻 ForkJoin): 各自按块读计划读块列 (+ 该层 ts_valid) →
//     私有 TsPlane 转置 (各列按自己的 valid_type 折哨兵);
//     每特征槽一轮只被一个任务写, 每轮一次 dispatch 隔开相邻轮 → 累积器无锁无归约;
//   每 (轮, 特征) 按洗牌资产序旋转抽 kPvAssetsPerRound 个资产:
//     有效值 → 该特征小 KLL (累积);  逐资产整段序列 (缺/门控 ffill, 不填 0) → Grid::PSD
//     (段内去均值 + Hann) → Parseval 归一 (除当段总功率, 每 bin = 方差占比, 跨特征绝对可比)
//     → 算术平均 (累积) → 按 k 几何分桶 (PsdAxis, 每桶 ≥ 1 个 bin) 取桶内均值 × N/2 (白噪声 = 1, 跨层同一参照)
//   块末短锁发布该块 Cell (PDF 折线 + log10 谱) → 首帧 = 一块的 IO + 扫描 (几 ms),
//   之后逐块逐轮收敛; 跑完全部轮即 Done, 不再耗后台.
//
// A 轴 = 库轴; 抽样资产 = 库轴下标 (UI 不需要资产身份).
// 重算: reset 清空 cells (预览是全表底图, 输入变了旧格子无参照意义), 逐块填回.
// 生命周期: 进 Features 任务且输入就绪 → 起 worker + 自动构建 (不依赖选中特征);
//   库 / 日期区间变了 或 Compute 落了新库 → 新请求即取消在跑重算;
//   切出 Features 任务 → clear() 整体释放.
// ============================================================================

static constexpr size_t kPvRounds = 64;          // 抽样段上限 (每层轮数): 精度逐轮收敛
static constexpr size_t kPvAssetsPerRound = 512; // 每 (轮, 特征) 抽样资产数 (轮间旋转覆盖)
static constexpr size_t kPvBlockCols = 8;        // 每次 load_day_columns 的特征列数 (+1 ts_valid); 长段层按行数折减
static constexpr size_t kPvKllCapacity = 128;    // 每特征 sketch (迷你图, 精度换内存)
static constexpr size_t kPvKllResolution = 256;  // PDF 255 点 (±3sd 裁剪 + hover 放大图要吃分辨率)
static constexpr size_t kPvMinSamples = 500;     // 样本不足不画
static constexpr size_t kPvPsdBins = 32;         // 谱桶上限 (实际 = min(32, N/2))
static constexpr float kPvPsdFloor = -20.0f;     // log10 地板 (全恒值)

// 谱桶轴 (每层一份): k ∈ [1, N/2] 按 k 几何分 n 桶 (每桶 ≥ 1 个 bin), 下标 j 周期升序 (k 降序)
struct PsdAxis {
  uint32_t n = 0;                              // 桶数
  std::array<uint16_t, kPvPsdBins> k0{}, k1{}; // 桶 j 覆盖 bin k ∈ [k0[j], k1[j]]
  std::array<float, kPvPsdBins> period{};      // 桶中心周期 (网格行, 几何均值)
};
PsdAxis psd_axis(size_t N); // N = FFT 长度

struct Preview : Published {
  // 每特征发布快照 (槽位 = (层, 该层 metadata 下标); 非预览列 (META 类) 恒空)
  struct Cell : analysis::PdfSnap<kPvKllResolution> {
    uint32_t psd_n = 0;                  // 参与谱平均的 (资产, 段) 数 (含段内恒值段: 投占比全 0 的一票)
    std::array<float, kPvPsdBins> psd{}; // log10 桶内每 bin 能量占比均值 × N/2 (Parseval 归一), 周期升序 (见 PsdAxis);
                                         // 白噪声参照 = 0 (全层同一条线); 恒值段拉低整条曲线 (高度 = 有动的段占比),
                                         // 全部恒值 → 全 kPvPsdFloor 地板
    analysis::Integrity integrity;       // 抽样格子账目 (NaN/±Inf/零/极值), 逐轮累积
    // flag 探测: 抽样有效值全为整数 (±0, 1, 2, …; 随主扫描逐格判, 见一次非整数即永久 false).
    // 只对 f16 还分得出小数的量级有意义 (|x| < 1024, 再往上格距 ≥ 1 什么都是 "整数"), 界限在 detect_cat2 判
    bool int_only = true;
    // price 笼账目 (随主扫描逐 (资产, 分钟) 判, 逐轮累积): 有效值对照当日该资产
    // [lim_dn, lim_up] (带 f16 量化 + kPxEps 容差, 见 Preview.cpp 笼快照);
    // cage_n > 0 且 cage_miss == 0 → 每个抽样日都全落笼内 = price
    uint32_t cage_n = 0;    // 受检样本数 (值有效 且 当日笼两列有值)
    uint32_t cage_miss = 0; // 笼外样本数
  };

  struct Spec {
    TierId tier = TierId::Lob;                    // 预览的档 (= 主库档; UI 按 (档, 层) 取 cells)
    std::array<ReadPlan, LEVEL_COUNT> plans;      // 每层预览列 (主档该层, metadata 下标升序, 不含 META 类); 空 = 该层不跑
    std::array<size_t, LEVEL_COUNT> n_features{}; // 每层特征总数 (cells 尺寸)
    std::vector<size_t> cage;                     // price 笼两列 {lim_dn, lim_up} (Bar 档旁读, All 的 L_day 字段表下标; 逐日笼内判定, 融合进主扫描)
  };
  // 发布快照 (进度/epoch/mutex 在 Published; done/total = 各层轮数和). 块末短锁拷贝该块 cells
  struct Snap {
    std::array<std::vector<Cell>, LEVEL_COUNT> cells; // [层][该层特征总数]
    std::array<PsdAxis, LEVEL_COUNT> axis;            // [层] 谱桶轴 (reset 定)
  };
  Spec spec;
  Snap snap;

  // (持锁) (档, 层, 列) 的预览格; 非预览档 / 越界 = nullptr
  const Cell *cell(TierId t, size_t level, size_t col) const {
    return t == spec.tier && level < LEVEL_COUNT && col < snap.cells[level].size() ? &snap.cells[level][col] : nullptr;
  }

  Preview();
  ~Preview();

  // 重置全部状态并进入 Building (预览是全表底图: cells 清空, 逐块填回)
  void reset(analysis::ReadScope scope, Spec spec);
  // 轮训构建 (轮内特征块一块一任务: IO + 扫描 + 块末发布; 每轮一次 dispatch); 被取消返回 false
  bool build(const PerLib<const FeatureRead *> &fr, const std::atomic<bool> &cancel);
  void clear();

private:
  struct Runtime; // worker 私有 (每线程 shard + 各层状态), 定义在 Preview.cpp
  struct Level;   // 一层的网格无关状态 (每特征 sketch/账目, 段序, 块读计划, 笼快照) + 虚轮
  template <class G>
  struct LevelOf; // 按网格实例化 (谱累加 + FFT 工作区)
  std::unique_ptr<Runtime> rt_;
  analysis::ReadScope scope_; // reset 时定好, build 全程只读
};
