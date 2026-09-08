#pragma once

#include "codec/L2_DataType.hpp" // L2::ValidType
#include "features/Backend/DayBatchPlane.hpp"
#include "features/TimeIndex.hpp"
#include "math/distribution/KLLcache.hpp"
#include "math/spectral/DayPSD.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

class FeatureRead;

// ============================================================================
// FeaturePreview (特征表内联预览: 每特征 平均分布 + 平均单日频谱, 轮训抽样流式)
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
// 生命周期: 进 Features 任务且输入就绪 → 起 worker + 自动构建 (不依赖选中特征);
//   universe / 日期区间变了 或 Compute 落了新库 → 新请求即取消在跑重算;
//   切出 Features 任务 → clear() 整体释放.
// ============================================================================

static constexpr size_t kPvLevel = 1;
static constexpr size_t kPvVR = TRADE_MINUTES_PER_DAY; // == level_valid_rows(kPvLevel), build 内断言
static constexpr size_t kPvRounds = 64;                // 抽样日上限 (轮数): 精度逐轮收敛
static constexpr size_t kPvAssetsPerRound = 512;       // 每 (轮, 特征) 抽样资产数 (轮间旋转覆盖)
static constexpr size_t kPvBlockCols = 8;              // 每次 load_day_columns 的特征列数 (+1 _meta)
static constexpr size_t kPvKllCapacity = 128;          // 每特征 sketch (迷你图, 精度换内存)
static constexpr size_t kPvKllResolution = 64;         // PDF 63 点 (~百像素迷你图足够)
static constexpr size_t kPvMinSamples = 500;           // 样本不足不画
using PvDayPSD = math::spectral::DayPSD<kPvVR>;
static constexpr size_t kPvPsdPts = PvDayPSD::N_FREQS - 1; // 跳 DC

struct FeaturePreview {
  // 每特征发布快照 (槽位 = 该层 metadata 下标; 非预览列 (META 类) 恒空)
  struct Cell {
    uint64_t n = 0;                                   // 累积有效样本数 (dist)
    uint32_t psd_n = 0;                               // 参与谱平均的 (资产, 天) 数
    uint32_t n_pts = 0;                               // PDF 点数; 0 = 未就绪/样本不足
    std::array<float, kPvKllResolution - 1> x{}, y{}; // PDF 折线
    std::array<float, kPvPsdPts> psd{};               // log10 单日谱均值 (k = 1.., 跳 DC)
  };

  enum class Status : uint8_t { Idle,
                                Building,
                                Done,
                                Cancelled };

  // 进度: 原子, UI 免锁读
  std::atomic<Status> status{Status::Idle};
  std::atomic<size_t> rounds_done{0};  // 已完成轮数 (= 已扫抽样日)
  std::atomic<size_t> rounds_total{0}; // 抽样日总数
  std::atomic<uint64_t> epoch{0};      // 数据每变一次 +1 (reset/clear/每块发布), 跨构建单调

  // 发布快照: mutex 保护 (worker 块末短锁发布; UI 渲染表格期间持锁)
  mutable std::mutex mutex;
  std::vector<Cell> cells; // [该层特征总数]

  // ==========================================================================
  // Methods (worker 线程调用)
  // ==========================================================================

  // 重置全部状态并进入 Building. feat_cols = 预览特征列 (metadata 下标, 升序,
  // 不含 META 类), valid_types 与之平行; meta_col = "_meta" 门控列下标;
  // n_features = 该层特征总数 (cells 尺寸); n_assets = universe 子轴大小
  void reset_for_build(std::vector<size_t> feat_cols, std::vector<L2::ValidType> valid_types,
                       size_t meta_col, const std::vector<std::string> &month_keys,
                       size_t n_features, size_t n_assets);

  // 轮训构建 (单线程串行 IO + 扫描, 块末发布); 被取消返回 false
  bool build(FeatureRead &reader, const std::atomic<bool> &cancel);

  void clear();

private:
  // 构建参数 (reset 时定, build 全程只读)
  std::vector<size_t> feat_cols_;
  std::vector<L2::ValidType> valid_types_;
  size_t meta_col_ = 0;
  std::vector<std::string> months_; // "YYYYMM" 升序
  size_t A_ = 0;                    // universe 子轴大小

  // worker 私有 (clear() 只在 worker join 之后调用, 无竞争)
  std::vector<KLLcache> klls_;                                 // [n_preview] 每特征累积 sketch
  std::vector<std::array<double, PvDayPSD::N_FREQS>> psd_sum_; // [n_preview] 单日谱累加
  std::vector<uint64_t> psd_n_;                                // [n_preview] 参与谱平均的 (资产, 天) 数
  std::vector<uint32_t> asset_order_;                          // [A] 固定种子洗牌 (轮间旋转取片)
  std::vector<float> samples_;                                 // 单 (特征, 轮) 有效样本缓冲
  std::array<float, kPvVR> day_buf_{};                         // 单 (资产, 日) 序列 (NaN = 缺)
  PvDayPSD psd_;                                               // FFT workspace
  DayBatchPlane plane_;                                        // [块列+1][A][1][VR]
};
