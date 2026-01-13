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
//   raw → stationary → ts_normed → cs_normed
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
    float frac_d = 0.05f;
    int frac_window = 100;

    // 归一化 (TS/CS 对仗)
    NormMethod ts_norm = NormMethod::NONE;
    NormMethod cs_norm = NormMethod::NONE;

    // 公共参数
    float clip_k = 3.0f;
    float winsor_pct = 0.05f;
    float power_alpha = 0.5f;

    bool operator==(const Params &o) const {
      return stationary_method == o.stationary_method &&
             ma_window == o.ma_window && diff_order == o.diff_order &&
             std::abs(frac_d - o.frac_d) < 1e-6f &&
             frac_window == o.frac_window &&
             ts_norm == o.ts_norm && cs_norm == o.cs_norm &&
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
    std::string label;                    // "24/01/15" / "24/01" / "全区间"
    std::vector<std::string> dates;       // 日期列表 (L0:1天, L1:~20天, L2:全部)
    size_t n_samples = 0;                 // 总样本数 (加载后填充)
  };

  // ==========================================================================
  // 原始数据缓存 (加载后缓存，避免重复读取)
  // ==========================================================================

  // 单资产稀疏数据 (value + index)
  struct SparseData {
    std::vector<float> value; // 有效值
    std::vector<size_t> index; // 有效值的原始索引

    size_t size() const { return value.size(); }
    bool empty() const { return value.empty(); }

    void clear() {
      value.clear();
      index.clear();
    }

    void reserve(size_t n) {
      value.reserve(n);
      index.reserve(n);
    }

    void push(float v, size_t i) {
      value.push_back(v);
      index.push_back(i);
    }
  };

  struct DataCache {
    std::vector<std::vector<float>> raw;    // [asset_idx][time] (dense, for compatibility)
    std::vector<SparseData> sparse;         // [asset_idx] 稀疏数据 (value + index)
    size_t n_assets = 0;
    size_t n_samples = 0;                   // 原始时间长度

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
      sparse.clear();
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
    std::vector<float> stationary;  // 平稳化后
    std::vector<float> ts_normed;   // 时序归一化后
    std::vector<float> cs_normed;   // 截面归一化后 (最终)

    // ADF/KPSS (标量)
    float adf_stat = 0.0f;
    float adf_pval = 1.0f;
    bool adf_pass = false;

    float kpss_stat = 0.0f;
    float kpss_pval = 0.0f;
    bool kpss_pass = false;

    // PDF (KLLcache 持久复用，exportPDF 返回内部指针)
    KLLcache KLL{512, 1024};

    bool valid = false;

    // 重置 (不分配，只清零)
    void reset() {
      std::fill(stationary.begin(), stationary.end(), 0.0f);
      std::fill(ts_normed.begin(), ts_normed.end(), 0.0f);
      std::fill(cs_normed.begin(), cs_normed.end(), 0.0f);
      adf_stat = 0.0f;
      adf_pval = 1.0f;
      adf_pass = false;
      kpss_stat = 0.0f;
      kpss_pval = 0.0f;
      kpss_pass = false;
      KLL.clear();
      valid = false;
    }

    // 预分配时序数据
    void reserve(size_t n_samples) {
      stationary.resize(n_samples, 0.0f);
      ts_normed.resize(n_samples, 0.0f);
      cs_normed.resize(n_samples, 0.0f);
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

    // 单一 generation：每次触发计算递增
    std::atomic<uint64_t> generation{0};

    // Phase 同步 (TS → CS)
    std::atomic<size_t> ts_done{0};
    size_t n_workers{0};

    float progress() const {
      size_t t = total.load();
      if (t == 0) return 0.0f;
      size_t d = done.load();
      float p = 100.0f * d / t;
      return p > 100.0f ? 100.0f : p;
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
      ts_done = 0;
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

  // ==========================================================================
  // PSD 缓存 (非标周期轴，128 bins)
  // ==========================================================================

  static constexpr size_t N_PSD_BINS = 128;

  struct PSDCache {
    // 每个 asset 的 PSD (128 bins)
    std::vector<std::array<float, 128>> asset_psd;

    // 聚合 PSD (能量均线)
    std::array<float, 128> avg_psd{};
    std::array<float, 128> avg_psd_db{};  // 取 log10 后

    // 绘图用 x 轴
    std::array<float, 128> plot_x{};

    // 刻度
    std::vector<double> tick_positions;
    std::vector<std::string> tick_labels;

    // 频段能量比例 (用于标注)
    float ratio_sec = 0.0f;   // 秒级 (bins 0-57)
    float ratio_min = 0.0f;   // 分钟级 (bins 58-116)
    float ratio_hour = 0.0f;  // 小时级 (bins 117-126)
    float ratio_dc = 0.0f;    // DC (bin 127)

    bool valid = false;

    void init_axis() {
      for (size_t k = 0; k < 128; ++k) {
        plot_x[k] = static_cast<float>(k);
      }
      // 刻度: 10s, 20s, ..., 50s, 10m, 20m, ..., 50m, 2h, 4h, ..., 10h
      tick_positions.clear();
      tick_labels.clear();
      for (size_t s = 10; s < 60; s += 10) {
        tick_positions.push_back(static_cast<double>(s - 2));
        tick_labels.push_back(std::to_string(s) + "s");
      }
      for (size_t m = 10; m < 60; m += 10) {
        tick_positions.push_back(static_cast<double>(58 + m - 1));
        tick_labels.push_back(std::to_string(m) + "m");
      }
      for (size_t h = 2; h <= 10; h += 2) {
        tick_positions.push_back(static_cast<double>(117 + h - 1));
        tick_labels.push_back(std::to_string(h) + "h");
      }
    }

    void clear() {
      asset_psd.clear();
      avg_psd.fill(0.0f);
      avg_psd_db.fill(0.0f);
      ratio_sec = ratio_min = ratio_hour = ratio_dc = 0.0f;
      valid = false;
    }

    void resize(size_t n_assets) {
      asset_psd.resize(n_assets);
      for (auto &p : asset_psd) {
        p.fill(0.0f);
      }
    }
  };

  PSDCache psd;

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
    // 通过递增 generation 来触发中断
    ++compute.generation;
    compute.status = Compute::Status::Cancelled;
  }

  void clear() {
    params = Params{};
    blocks.clear();
    selected_block = 0;
    cache.clear();
    results.clear();
    psd.clear();
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
