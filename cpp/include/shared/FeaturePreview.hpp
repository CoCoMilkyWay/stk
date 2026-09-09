#pragma once

#include "codec/L2_DataType.hpp" // L2::ValidType
#include "shared/Analysis.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

class FeatureRead;

// ============================================================================
// FeaturePreview (特征表内联预览: 每特征 平均分布 + 平均单日频谱, 轮训抽样流式;
//                 公共骨架见 shared/Analysis.hpp)
// ============================================================================
// 目的: Feature 表每行尾部两个迷你曲线 (PDF / PSD), 全表一眼对比形态.
// 与 Dist/Transform 维度反转 —— 那边单特征全数据, 这边全特征抽样数据:
//
//   轮 = 一个抽样日 (全区间日期固定种子洗牌取前 kPvRounds 个 → 无偏覆盖);
//   轮内按特征分块: 每块 kPvBlockCols 列 + _meta 一次 load_day_columns →
//     DayBatchPlane 转置 (has_valid=false: 门控按各特征自己的 valid_type 在扫描侧判);
//   每 (轮, 特征) 按洗牌资产序旋转抽 kPvAssetsPerRound 个资产:
//     有效值 → 该特征小 KLL (累积);  逐资产整日序列 → DayPSD → 算术平均 (累积)
//   块末短锁发布该块 Cell (PDF 折线 + log10 谱) → 首帧 = 一块的 IO + 扫描 (几 ms),
//   之后逐块逐轮收敛; 跑完全部轮即 Done, 不再耗后台.
//
// 只在 L1 跑 (L0 秒频单列单日 IO 过重; 表格 L0 行显示 "—").
// universe: A 轴 = universe 子轴; 抽样资产 = 子轴下标 (UI 不需要资产身份).
// 重算: reset 清空 cells (预览是全表底图, 输入变了旧格子无参照意义), 逐块填回.
// 生命周期: 进 Features 任务且输入就绪 → 起 worker + 自动构建 (不依赖选中特征);
//   universe / 日期区间变了 或 Compute 落了新库 → 新请求即取消在跑重算;
//   切出 Features 任务 → clear() 整体释放.
// ============================================================================

static constexpr size_t kPvRounds = 64;                            // 抽样日上限 (轮数): 精度逐轮收敛
static constexpr size_t kPvAssetsPerRound = 512;                   // 每 (轮, 特征) 抽样资产数 (轮间旋转覆盖)
static constexpr size_t kPvBlockCols = 8;                          // 每次 load_day_columns 的特征列数 (+1 _meta)
static constexpr size_t kPvKllCapacity = 128;                      // 每特征 sketch (迷你图, 精度换内存)
static constexpr size_t kPvKllResolution = 64;                     // PDF 63 点 (~百像素迷你图足够)
static constexpr size_t kPvMinSamples = 500;                       // 样本不足不画
static constexpr size_t kPvPsdPts = analysis::DayPSD::N_FREQS - 1; // 跳 DC

struct FeaturePreview : analysis::StreamState {
  // 每特征发布快照 (槽位 = 该层 metadata 下标; 非预览列 (META 类) 恒空)
  struct Cell : analysis::PdfSnap<kPvKllResolution> {
    uint32_t psd_n = 0;                 // 参与谱平均的 (资产, 天) 数
    std::array<float, kPvPsdPts> psd{}; // log10 单日谱均值 (k = 1.., 跳 DC)
    analysis::Integrity integrity;      // 抽样格子账目 (NaN/±Inf/零/极值), 逐轮累积
  };

  // 发布快照 (进度/epoch/mutex 在 StreamState; done/total = 轮)
  std::vector<Cell> cells; // [该层特征总数]

  // ==========================================================================
  // Methods (worker 线程调用)
  // ==========================================================================

  FeaturePreview();
  ~FeaturePreview();

  // 重置全部状态并进入 Building. feat_cols = 预览特征列 (metadata 下标, 升序,
  // 不含 META 类), valid_types 与之平行; meta_col = "_meta" 门控列下标;
  // n_features = 该层特征总数 (cells 尺寸); n_assets = universe 子轴大小
  void reset_for_build(std::vector<size_t> feat_cols, std::vector<L2::ValidType> valid_types,
                       size_t meta_col, std::vector<std::string> month_keys,
                       size_t n_features, size_t n_assets);

  // 轮训构建 (单线程串行 IO + 扫描, 块末发布); 被取消返回 false
  bool build(FeatureRead &reader, const std::atomic<bool> &cancel);

  void clear();

private:
  struct Runtime; // worker 私有 (每特征 sketch/账目/谱累加, 批平面), 定义在 FeaturePreview.cpp
  std::unique_ptr<Runtime> rt_;
  // 构建参数 (reset 时定, build 全程只读)
  std::vector<size_t> feat_cols_;
  std::vector<L2::ValidType> valid_types_;
  size_t meta_col_ = 0;
  std::vector<std::string> months_; // "YYYYMM" 升序
  size_t A_ = 0;                    // universe 子轴大小
};
