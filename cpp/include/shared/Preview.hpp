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
//   轮 = 一个抽样日 (plan_days Shuffle 取前 kPvRounds 个 → 无偏覆盖);
//   轮首单线程读共享列: Bar 档旁读 lim_dn / lim_up (笼快照, 按 lib.axis.ids 取全轴) + 主库 ts_valid (门控行, 轮内各块共用);
//   轮内按特征分块 (每块 kPvBlockCols 列), 一块一任务 (Runtime 常驻 ForkJoin): 各自读块列 →
//     私有 TsPlane 转置 (外部门控 = 轮首那份 ts_valid, 各列按自己的 valid_type 折哨兵);
//     每特征槽一轮只被一个任务写, 每轮一次 dispatch 隔开相邻轮 → 累积器无锁无归约;
//   每 (轮, 特征) 按洗牌资产序旋转抽 kPvAssetsPerRound 个资产:
//     有效值 → 该特征小 KLL (累积);  逐资产整日序列 (缺/门控 ffill, 不填 0) → DayPSD
//     (日内去均值 + Hann) → Parseval 归一 (除当日总功率, 每 bin = 方差占比, 跨特征绝对可比)
//     → 算术平均 (累积)
//   块末短锁发布该块 Cell (PDF 折线 + log10 谱) → 首帧 = 一块的 IO + 扫描 (几 ms),
//   之后逐块逐轮收敛; 跑完全部轮即 Done, 不再耗后台.
//
// 只在 L1 跑 (L0 秒频单列单日 IO 过重; 表格 L0 行显示 "—").
// A 轴 = 库轴; 抽样资产 = 库轴下标 (UI 不需要资产身份).
// 重算: reset 清空 cells (预览是全表底图, 输入变了旧格子无参照意义), 逐块填回.
// 生命周期: 进 Features 任务且输入就绪 → 起 worker + 自动构建 (不依赖选中特征);
//   库 / 日期区间变了 或 Compute 落了新库 → 新请求即取消在跑重算;
//   切出 Features 任务 → clear() 整体释放.
// ============================================================================

static constexpr size_t kPvRounds = 64;                             // 抽样日上限 (轮数): 精度逐轮收敛
static constexpr size_t kPvAssetsPerRound = 512;                    // 每 (轮, 特征) 抽样资产数 (轮间旋转覆盖)
static constexpr size_t kPvBlockCols = 8;                           // 每次 load_day_columns 的特征列数 (+1 ts_valid)
static constexpr size_t kPvKllCapacity = 128;                       // 每特征 sketch (迷你图, 精度换内存)
static constexpr size_t kPvKllResolution = 256;                     // PDF 255 点 (±3sd 裁剪 + hover 放大图要吃分辨率)
static constexpr size_t kPvMinSamples = 500;                        // 样本不足不画
static constexpr size_t kPvPsdPts = analysis::DayPSD::N / 2;        // 进谱 bin 数: k ∈ [1, N/2] (跳 DC), 周期 2~256 min
static constexpr size_t kPvPsdK(size_t j) { return kPvPsdPts - j; } // Cell::psd[j] ↔ bin k (周期升序 = k 降序)

struct Preview : Published {
  // 每特征发布快照 (槽位 = 该层 metadata 下标; 非预览列 (META 类) 恒空)
  struct Cell : analysis::PdfSnap<kPvKllResolution> {
    uint32_t psd_n = 0;                 // 参与谱平均的 (资产, 天) 数 (含日内恒值日: 投占比全 0 的一票)
    std::array<float, kPvPsdPts> psd{}; // log10 单日谱每 bin 能量占比 (Parseval 归一) 均值, 周期升序 (psd[j] ↔ k = kPvPsdK(j));
                                        // 白噪声参照 = 每 bin 1/kPvPsdPts; 恒值日拉低整条曲线 (高度 = 有动的天占比),
                                        // 全部恒值 → 全 -20 地板
    analysis::Integrity integrity;      // 抽样格子账目 (NaN/±Inf/零/极值), 逐轮累积
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
    fmeta::ColumnSet cols;    // 预览特征列 (metadata 下标升序, 不含 META 类) (+ 门控)
    std::vector<size_t> cage; // price 笼两列 {lim_dn, lim_up} (Bar 档旁读, All 字段表下标; 逐日笼内判定, 融合进主扫描)
    size_t n_features = 0;    // 该层特征总数 (cells 尺寸)
  };
  // 发布快照 (进度/epoch/mutex 在 Published; done/total = 轮). 块末短锁拷贝该块 cells
  struct Snap {
    std::vector<Cell> cells; // [该层特征总数]
  };
  Spec spec;
  Snap snap;

  Preview();
  ~Preview();

  // 重置全部状态并进入 Building (预览是全表底图: cells 清空, 逐块填回)
  void reset(analysis::ReadScope scope, Spec spec);
  // 轮训构建 (轮内特征块一块一任务: IO + 扫描 + 块末发布; 每轮一次 dispatch); 被取消返回 false
  bool build(FeatureRead &reader, const FeatureRead &all, const std::atomic<bool> &cancel);
  void clear();

private:
  struct Runtime; // worker 私有 (每特征 sketch/账目/谱累加, 轮共享列, 每线程 shard), 定义在 Preview.cpp
  std::unique_ptr<Runtime> rt_;
  analysis::ReadScope scope_; // reset 时定好, build 全程只读
};
