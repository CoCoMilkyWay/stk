#pragma once

#include "features/FeaturesDefine.hpp"
#include <atomic>
#include <cassert>
#include <cstdint>
#include <string>
#include <vector>

// ============================================================================
// Transform Analysis Data Structure
// ============================================================================
//
// 平稳化 + 归一化实时分析
// 设计目标: 参数即时可调，计算极快，实时响应
//
// 数据流:
//   原始特征 → 平稳化 → 归一化 → 展示 (ADF/KPSS/K线/PDF/FFT)
//
// 计算粒度:
//   L0 (秒级): 按天计算
//   L1 (分钟级): 按月计算
//   L2 (小时级): 按整个回测区间计算
//
// ============================================================================

struct Transform {

  // ==========================================================================
  // 平稳化方法
  // ==========================================================================

  enum class StationaryMethod : uint8_t {
    NONE = 0,      // 不做平稳化
    MA_DETREND,    // 移动平均去趋势: x_t - MA_W(x_t)
    INT_DIFF,      // 整数阶差分: (1-L)^d x_t, d ∈ Z+
    FRAC_DIFF      // 分数阶差分: (1-L)^d x_t, d ∈ R (FFD)
  };

  // ==========================================================================
  // 配置参数
  // ==========================================================================

  struct Config {
    // 平稳化配置
    StationaryMethod stationary_method = StationaryMethod::NONE;
    int ma_window = 60;           // MA去趋势窗口 [10, 1000]
    int diff_order = 1;           // 整数差分阶数 [1, 3]
    float frac_d = 0.5f;          // 分数差分阶 [0.0, 1.0]
    int frac_window = 100;        // FFD窗口大小 [10, 500]

    // 归一化配置
    NormMethod norm_method = NormMethod::NONE;
    float clip_k = 3.0f;          // CLIP/WINSOR k值 [1.0, 10.0]
    float winsor_pct = 0.05f;     // WINSOR百分位 [0.01, 0.25]
    float power_alpha = 0.5f;     // POWER指数 [0.1, 2.0]

    // 比较: 是否有变化
    bool operator==(const Config &o) const {
      return stationary_method == o.stationary_method &&
             ma_window == o.ma_window &&
             diff_order == o.diff_order &&
             std::abs(frac_d - o.frac_d) < 1e-6f &&
             frac_window == o.frac_window &&
             norm_method == o.norm_method &&
             std::abs(clip_k - o.clip_k) < 1e-6f &&
             std::abs(winsor_pct - o.winsor_pct) < 1e-6f &&
             std::abs(power_alpha - o.power_alpha) < 1e-6f;
    }
    bool operator!=(const Config &o) const { return !(*this == o); }
  };

  // ==========================================================================
  // 单资产计算结果
  // ==========================================================================

  struct AssetResult {
    std::vector<float> raw;           // 原始特征序列
    std::vector<float> stationary;    // 平稳化后
    std::vector<float> normalized;    // 归一化后

    // ADF检验结果 (平稳化后)
    float adf_stat = 0.0f;
    float adf_pval = 1.0f;
    bool adf_pass = false;            // p < 0.05

    // KPSS检验结果 (平稳化后)
    float kpss_stat = 0.0f;
    float kpss_pval = 0.0f;
    bool kpss_pass = false;           // p > 0.05

    // FFT功率谱 (归一化后)
    std::vector<float> fft_freq;      // 频率轴
    std::vector<float> fft_power;     // 功率

    size_t n_samples = 0;
    bool valid = false;

    void clear() { *this = AssetResult{}; }

    void reserve(size_t n) {
      raw.reserve(n);
      stationary.reserve(n);
      normalized.reserve(n);
    }
  };

  // ==========================================================================
  // 横截面PDF (单时间片)
  // ==========================================================================

  struct CrossSectionSlice {
    size_t time_idx = 0;              // 时间索引
    std::vector<float> values;        // 各资产的值 [n_assets]
    
    // PDF统计
    float mean = 0.0f;
    float std = 0.0f;
    float skew = 0.0f;
    float kurt = 0.0f;
    float min = 0.0f;
    float max = 0.0f;

    // 直方图
    static constexpr size_t N_BINS = 50;
    std::vector<float> hist_x;        // bin中心 [N_BINS]
    std::vector<float> hist_y;        // 频率 [N_BINS]

    bool valid = false;
    void clear() { *this = CrossSectionSlice{}; }
  };

  // ==========================================================================
  // 数据块定义 (计算单元)
  // ==========================================================================

  struct DataBlock {
    std::string label;                // "2024-01-15" 或 "2024-01" 或 "全区间"
    size_t start_idx = 0;             // 在全局时间序列中的起始索引
    size_t length = 0;                // 样本数
    bool valid = false;
  };

  // ==========================================================================
  // 计算状态
  // ==========================================================================

  struct Compute {
    enum class Status : uint8_t {
      Idle,
      Loading,    // 加载数据
      Computing,  // 计算中
      Done,
      Error,
      Cancelled
    };

    Status status = Status::Idle;
    std::string error;

    std::atomic<size_t> done{0};
    std::atomic<size_t> total{0};
    std::atomic<bool> cancel{false};

    float progress() const {
      size_t t = total.load();
      return t > 0 ? 100.0f * done.load() / t : 0.0f;
    }

    bool is_idle() const {
      return status == Status::Idle || status == Status::Done ||
             status == Status::Cancelled || status == Status::Error;
    }

    bool is_busy() const {
      return status == Status::Loading || status == Status::Computing;
    }

    void reset() {
      status = Status::Idle;
      error.clear();
      done = 0;
      total = 0;
      cancel = false;
    }
  };

  // ==========================================================================
  // 输入缓存 (检测参数变化)
  // ==========================================================================

  struct Input {
    int feature_idx = -1;
    int level = -1;
    int block_idx = -1;               // 当前选中的数据块
    Config config;

    bool has_changes(int feat, int lvl, int blk, const Config &cfg) const {
      return feature_idx != feat || level != lvl || 
             block_idx != blk || config != cfg;
    }

    void update(int feat, int lvl, int blk, const Config &cfg) {
      feature_idx = feat;
      level = lvl;
      block_idx = blk;
      config = cfg;
    }
  };

  // ==========================================================================
  // 主数据成员
  // ==========================================================================

  // 配置
  Config config;
  Input input;
  Compute compute;

  // 数据块列表 (根据level生成)
  std::vector<DataBlock> blocks;
  int selected_block = 0;

  // 计算结果
  std::vector<AssetResult> results;   // [n_assets]
  size_t n_assets = 0;

  // 当前时间片的横截面
  CrossSectionSlice cross_section;
  int time_slider = 0;                // 时间拖动条位置

  // 聚合FFT (跨资产平均)
  std::vector<float> avg_fft_freq;
  std::vector<float> avg_fft_power;

  // ==========================================================================
  // 方法
  // ==========================================================================

  void cancel() {
    compute.cancel = true;
    compute.status = Compute::Status::Cancelled;
  }

  void clear() {
    config = Config{};
    input = Input{};
    compute.reset();
    blocks.clear();
    selected_block = 0;
    results.clear();
    n_assets = 0;
    cross_section.clear();
    time_slider = 0;
    avg_fft_freq.clear();
    avg_fft_power.clear();
  }

  bool need_rebuild(int feat, int lvl, int blk, const Config &cfg) const {
    return input.has_changes(feat, lvl, blk, cfg);
  }

  // 根据level生成数据块
  void generate_blocks(int level, const std::vector<std::string> &dates);

  // 更新横截面 (给定时间索引)
  void update_cross_section(size_t time_idx);
};
