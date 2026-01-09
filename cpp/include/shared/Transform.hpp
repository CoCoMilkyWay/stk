#pragma once

#include "features/FeaturesDefine.hpp"
#include "math/distribution/KLLcache.hpp"
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

// ============================================================================
// Transform Analysis Data Structure
// ============================================================================
//
// 设计原则:
//   1. 固定大小数据用 std::array - 零分配
//   2. KLLcache 持久复用 - exportPDF 返回内部指针，零 copy
//   3. 变长数据预分配后只 clear 不 resize
//   4. UI 线程只读取，不创建任何复杂数据结构
//
// 数据流:
//   FeatureReader → DataCache → 平稳化 → 归一化 → AssetResult → UI
//
// ============================================================================

struct Transform {

  // ==========================================================================
  // 平稳化方法
  // ==========================================================================

  enum class StationaryMethod : uint8_t {
    NONE = 0,
    MA_DETREND, // x_t - MA_W(x_t)
    INT_DIFF,   // (1-L)^d x_t, d ∈ Z+
    FRAC_DIFF   // (1-L)^d x_t, d ∈ R
  };

  // ==========================================================================
  // 计算参数 (平稳化 + 归一化)
  // ==========================================================================

  struct Params {
    // 平稳化
    StationaryMethod stationary_method = StationaryMethod::NONE;
    int ma_window = 60;
    int diff_order = 1;
    float frac_d = 0.5f;
    int frac_window = 100;

    // 归一化
    NormMethod norm_method = NormMethod::NONE;
    float clip_k = 3.0f;
    float winsor_pct = 0.05f;
    float power_alpha = 0.5f;

    bool operator==(const Params &o) const {
      return stationary_method == o.stationary_method &&
             ma_window == o.ma_window && diff_order == o.diff_order &&
             std::abs(frac_d - o.frac_d) < 1e-6f &&
             frac_window == o.frac_window && norm_method == o.norm_method &&
             std::abs(clip_k - o.clip_k) < 1e-6f &&
             std::abs(winsor_pct - o.winsor_pct) < 1e-6f &&
             std::abs(power_alpha - o.power_alpha) < 1e-6f;
    }
    bool operator!=(const Params &o) const { return !(*this == o); }
  };

  // ==========================================================================
  // 数据块定义 (L0=天, L1=月, L2=全区间)
  // ==========================================================================

  struct Block {
    std::string date;    // "20240115" or "202401" or "全区间"
    std::string display; // "24/01/15" or "24/01" or "全区间"
    size_t n_samples = 0;
  };

  // ==========================================================================
  // 原始数据缓存 (加载后缓存，避免重复读取)
  // ==========================================================================

  struct DataCache {
    std::vector<std::vector<float>> raw; // [asset_idx][time]
    size_t n_assets = 0;
    size_t n_samples = 0;

    // 缓存键 (用于判断是否需要重新加载)
    int level = -1;
    int feature_idx = -1;
    int block_idx = -1;

    bool valid() const { return n_assets > 0 && n_samples > 0; }

    bool matches(int lvl, int feat, int blk) const {
      return valid() && level == lvl && feature_idx == feat && block_idx == blk;
    }

    void set_key(int lvl, int feat, int blk) {
      level = lvl;
      feature_idx = feat;
      block_idx = blk;
    }

    void clear() {
      raw.clear();
      n_assets = 0;
      n_samples = 0;
      level = -1;
      feature_idx = -1;
      block_idx = -1;
    }
  };

  // ==========================================================================
  // 单资产计算结果 (持久复用)
  // ==========================================================================

  struct AssetResult {
    // 时序数据 (预分配 n_samples，后续复用)
    std::vector<float> stationary;
    std::vector<float> normalized;

    // ADF/KPSS (标量)
    float adf_stat = 0.0f;
    float adf_pval = 1.0f;
    bool adf_pass = false;

    float kpss_stat = 0.0f;
    float kpss_pval = 0.0f;
    bool kpss_pass = false;

    // FFT (动态大小，使用整个time window)
    std::vector<float> fft_freq;
    std::vector<float> fft_power;

    // PDF (KLLcache 持久复用，exportPDF 返回内部指针)
    KLLcache KLL{512, 1024};

    bool valid = false;

    // 重置 (不分配，只清零)
    void reset() {
      std::fill(stationary.begin(), stationary.end(), 0.0f);
      std::fill(normalized.begin(), normalized.end(), 0.0f);
      adf_stat = 0.0f;
      adf_pval = 1.0f;
      adf_pass = false;
      kpss_stat = 0.0f;
      kpss_pval = 0.0f;
      kpss_pass = false;
      std::fill(fft_freq.begin(), fft_freq.end(), 0.0f);
      std::fill(fft_power.begin(), fft_power.end(), 0.0f);
      KLL.clear();
      valid = false;
    }

    // 预分配时序数据和FFT
    void reserve(size_t n_samples) {
      stationary.resize(n_samples, 0.0f);
      normalized.resize(n_samples, 0.0f);
      // FFT大小: 向下取整到最接近的2的幂
      size_t fft_n = 1;
      while (fft_n * 2 <= n_samples)
        fft_n *= 2;
      size_t fft_size = fft_n / 2 + 1;
      fft_freq.resize(fft_size, 0.0f);
      fft_power.resize(fft_size, 0.0f);
    }
  };

  // ==========================================================================
  // 计算状态机
  // ==========================================================================

  struct Compute {
    enum class Status : uint8_t {
      Idle,
      Loading,   // 加载数据中
      Computing, // 计算中
      Done,
      Error,
      Cancelled
    };

    Status status = Status::Idle;
    std::string error;

    std::atomic<size_t> done{0};
    std::atomic<size_t> total{0};
    std::atomic<uint64_t> generation{0}; // 计算版本号，用于中断检测

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
      // generation 不重置，保持递增
    }
  };

  // ==========================================================================
  // UI 显示状态 (不触发计算)
  // ==========================================================================

  struct Display {
    int selected_asset = -1; // -1 = ALL, >=0 = specific asset

    bool is_all() const { return selected_asset < 0; }

    void clamp(size_t n_assets) {
      if (n_assets > 0 && selected_asset >= (int)n_assets)
        selected_asset = (int)n_assets - 1;
    }
  };

  // ==========================================================================
  // 主数据成员
  // ==========================================================================

  // 输入参数 (UI 控制)
  Params params;

  // 数据块列表
  std::vector<Block> blocks;
  int selected_block = 0;

  // 原始数据缓存
  DataCache cache;

  // 计算结果 (n_assets 个，预分配后复用)
  std::vector<AssetResult> results;

  // 聚合 FFT (动态大小)
  std::vector<float> avg_fft_freq;
  std::vector<float> avg_fft_power;

  // 显示状态
  Display display;

  // 计算状态
  Compute compute;

  // ==========================================================================
  // 输入变化检测
  // ==========================================================================

  bool need_reload(int level, int feature_idx) const {
    return !cache.matches(level, feature_idx, selected_block);
  }

  bool need_recompute(const Params &new_params) const {
    return params != new_params;
  }

  // ==========================================================================
  // 方法
  // ==========================================================================

  void cancel() {
    // 通过递增generation来中断当前计算
    ++compute.generation;
    compute.status = Compute::Status::Cancelled;
  }

  void clear() {
    params = Params{};
    blocks.clear();
    selected_block = 0;
    cache.clear();
    results.clear();
    avg_fft_freq.clear();
    avg_fft_power.clear();
    display = Display{};
    compute.reset();
  }

  // 根据 level 生成数据块
  void generate_blocks(int level, const std::vector<std::string> &dates);

  // 预分配 results (知道 n_assets 和 n_samples 后调用)
  void preallocate(size_t n_assets, size_t n_samples) {
    results.resize(n_assets);
    for (auto &r : results) {
      r.reserve(n_samples);
    }
  }
};
