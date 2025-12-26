#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

// Forward declarations
struct Feature;
struct Asset;

// ============================================================================
// TimeSeries Analysis Data Structure
// ============================================================================
//
// 时序分析流程 (SARIMA + GARCH 框架):
//   目标: 如果存在稳定可预测的成分,剥离它,减少与其他特征的虚假相关性
//
// 分析步骤:
//   Step 0: 平稳性检验 - ADF/KPSS 确认序列可建模
//   Step 1: 频域分析   - 检测周期性成分,确认频谱宽度
//   Step 2: ARMA建模   - ACF/PACF 确定模型阶数
//   Step 3: 残差分析   - 验证模型充分性,诊断残差性质
//   Step 4: 时间衰减   - 评估截面结构的时间稳定性
//
// ============================================================================

struct TimeSeries {

  // ==========================================================================
  // Stationarity Cache (per-month per-asset results)
  // ==========================================================================

  // Per-asset per-month 检验结果
  struct StationarityCell {
    float adf_statistic = 0.0f;
    float adf_pvalue = 0.0f;
    bool adf_pass = false;   // p < 0.05

    float kpss_statistic = 0.0f;
    float kpss_pvalue = 0.0f;
    bool kpss_pass = false;  // p > 0.05

    size_t n_samples = 0;
    bool valid = false;
  };

  // MonthlyCache (对仗 Dist::MonthlyCache)
  struct MonthlyStationarity {
    std::string month;  // "YYYYMM"
    std::vector<StationarityCell> by_asset;  // [n_assets]
    size_t n_assets = 0;
    bool valid = false;

    void clear() {
      month.clear();
      by_asset.clear();
      n_assets = 0;
      valid = false;
    }

    void init(size_t n_assets_val) {
      n_assets = n_assets_val;
      by_asset.clear();
      by_asset.resize(n_assets);
    }
  };

  // Step 0: 平稳性检验结果 (聚合统计，用于 UI summary)
  struct StationarityTest {
    float adf_pvalue = 0.0f;
    float adf_statistic = 0.0f;
    bool adf_pass = false;

    float kpss_pvalue = 0.0f;
    float kpss_statistic = 0.0f;
    bool kpss_pass = false;

    std::vector<float> raw_series;
    std::vector<float> detrend_series;
    std::vector<float> deseason_series;

    bool valid = false;
    void clear() { *this = StationarityTest{}; }
  };

  // Step 1: PSD热力图缓存 (天 × 频率)
  struct PSDHeatmap {
    static constexpr size_t N_FREQS = 257;  // N_FFT/2+1, N_FFT=512

    std::vector<float> data;         // [n_days * N_FREQS] 连续存储 (跨资产平均)
    std::vector<std::string> dates;  // [n_days]
    size_t n_days = 0;

    // ===== Per-asset PSD [n_days * n_assets * N_FREQS] =====
    std::vector<float> per_asset_data;
    size_t n_assets = 0;

    // 频率参数
    float sample_rate = 1.0f;        // Hz (由层级决定)
    float freq_resolution = 0.0f;    // Hz/bin
    float nyquist_freq = 0.0f;       // Hz

    // ===== 渲染用缓存 (finalize时计算) =====
    std::vector<size_t> valid_indices;   // 有效天索引
    std::vector<float> render_data;      // 转置+log后的数据 [N_FREQS * valid_days]
    float scale_min = -1.0f;             // colormap min (固定-1)
    float scale_max = 3.0f;              // colormap max (95% percentile)

    // 轴刻度 (频率bin索引 + 周期标签)
    std::vector<double> tick_positions;   // 频率 bin 索引位置
    std::vector<std::string> tick_labels; // 对应的周期标签

    // ===== 选中日期 (用于图2) =====
    int selected_day = -1;

    bool valid = false;

    void clear() { *this = PSDHeatmap{}; }

    void init(size_t days, size_t assets, float sr) {
      n_days = days;
      n_assets = assets;
      sample_rate = sr;
      freq_resolution = sr / 512.0f;
      nyquist_freq = sr / 2.0f;
      data.assign(n_days * N_FREQS, 0.0f);
      per_asset_data.assign(n_days * n_assets * N_FREQS, 0.0f);
      dates.assign(n_days, std::string{});
      valid_indices.clear();
      render_data.clear();
      tick_positions.clear();
      tick_labels.clear();
      selected_day = -1;
      valid = false;
    }

    // 获取某天的总PSD [N_FREQS]
    float *day_psd(size_t day_idx) {
      assert(day_idx < n_days);
      return data.data() + day_idx * N_FREQS;
    }

    const float *day_psd(size_t day_idx) const {
      assert(day_idx < n_days);
      return data.data() + day_idx * N_FREQS;
    }

    // 获取某天某资产的PSD [N_FREQS]
    float *asset_day_psd(size_t day_idx, size_t asset_idx) {
      assert(day_idx < n_days && asset_idx < n_assets);
      return per_asset_data.data() + (day_idx * n_assets + asset_idx) * N_FREQS;
    }

    const float *asset_day_psd(size_t day_idx, size_t asset_idx) const {
      assert(day_idx < n_days && asset_idx < n_assets);
      return per_asset_data.data() + (day_idx * n_assets + asset_idx) * N_FREQS;
    }

    // 热力图访问
    float get(size_t day_idx, size_t freq_idx) const {
      assert(day_idx < n_days && freq_idx < N_FREQS);
      return data[day_idx * N_FREQS + freq_idx];
    }

    size_t valid_days() const { return valid_indices.size(); }
  };

  // Step 1: 频域分析统计 (从热力图聚合)
  struct FrequencyAnalysis {
    float q_factor = 0.0f;
    float peak_frequency = 0.0f;
    float peak_bandwidth = 0.0f;
    bool q_factor_pass = false;

    float low_freq_power_ratio = 0.0f;
    bool has_low_freq = false;

    size_t n_significant_peaks = 0;
    bool has_peaks = false;

    // 平均功率谱 (所有天平均)
    std::vector<float> avg_power_spectrum;  // [N_FREQS]
    float nyquist_freq = 0.0f;

    bool valid = false;
    void clear() { *this = FrequencyAnalysis{}; }
  };

  // Step 2: ARMA建模分析结果
  struct ARMAAnalysis {
    int acf_cutoff_lag = 0;
    bool acf_is_cutoff = false;

    int pacf_cutoff_lag = 0;
    bool pacf_is_cutoff = false;

    int suggested_p = 0;
    int suggested_q = 0;
    bool is_white_noise = false;

    std::vector<float> acf_values;
    std::vector<float> pacf_values;
    float confidence_bound = 0.0f;
    int max_lag = 0;

    bool valid = false;
    void clear() { *this = ARMAAnalysis{}; }
  };

  // Step 3: 残差分析结果
  struct ResidualAnalysis {
    float ljung_box_pvalue = 0.0f;
    float ljung_box_statistic = 0.0f;
    bool ljung_box_pass = false;

    float arch_lm_pvalue = 0.0f;
    float arch_lm_statistic = 0.0f;
    bool arch_lm_pass = false;

    float jarque_bera_pvalue = 0.0f;
    float jarque_bera_statistic = 0.0f;
    float skewness = 0.0f;
    float kurtosis = 0.0f;
    bool jarque_bera_pass = false;
    bool jarque_bera_warn = false;

    bool cusum_pass = false;
    bool cusumq_pass = false;

    std::vector<float> residuals;
    std::vector<float> qq_theoretical;
    std::vector<float> qq_empirical;
    std::vector<float> cusum_values;
    std::vector<float> cusum_upper;
    std::vector<float> cusum_lower;

    bool valid = false;
    void clear() { *this = ResidualAnalysis{}; }
  };

  // Step 4: 时间衰减分析结果
  struct TemporalDecay {
    float gini_stability = 0.0f;
    float hhi_stability = 0.0f;
    float grs_stability = 0.0f;

    float rank_corr_stability = 0.0f;

    std::vector<float> time_points;
    std::vector<float> gini_series;
    std::vector<float> hhi_series;
    std::vector<float> rank_corr_series;

    bool valid = false;
    void clear() { *this = TemporalDecay{}; }
  };

  // ==========================================================================
  // Compute Control (对仗 Dist::Compute)
  // ==========================================================================

  struct Compute {
    enum class Status : uint8_t {
      Idle,
      Building,  // 构建月度缓存
      Done,
      Error,
      Cancelled
    };

    Status status = Status::Idle;
    std::string error;

    std::atomic<size_t> done{0};    // 已完成的月份数
    std::atomic<size_t> total{0};   // 总月份数
    std::atomic<bool> cancel{false};

    float progress() const {
      size_t t = total.load();
      return t > 0 ? 100.0f * done.load() / t : 0.0f;
    }

    bool is_idle() const {
      return status == Status::Idle || status == Status::Done;
    }

    bool is_busy() const { return status == Status::Building; }

    void reset() {
      status = Status::Idle;
      error.clear();
      done = 0;
      total = 0;
      cancel = false;
    }
  };

  // ==========================================================================
  // Input Control (对仗 Dist::Input)
  // ==========================================================================

  struct Input {
    int feature_idx = -1;
    int level = -1;
    std::string month_range;

    bool has_changes(int feat_idx, int lvl, const std::string &range) const {
      return feature_idx != feat_idx || level != lvl || month_range != range;
    }

    void update_cache(int feat_idx, int lvl, const std::string &range) {
      feature_idx = feat_idx;
      level = lvl;
      month_range = range;
    }
  };

  // ==========================================================================
  // Main Data Members
  // ==========================================================================

  // Stationarity cache: [n_months][n_assets]
  std::vector<MonthlyStationarity> stationarity_cache;

  // PSD cache: [total_days * N_FREQS] 热力图
  PSDHeatmap psd_cache;

  // Step results (聚合统计)
  StationarityTest step0_stationarity;
  FrequencyAnalysis step1_frequency;
  ARMAAnalysis step2_arma;
  ResidualAnalysis step3_residual;
  TemporalDecay step4_temporal_decay;

  Input input;
  Compute compute;

  // ==========================================================================
  // Methods - Build Stationarity (对仗 Dist::build_month/build_all)
  // ==========================================================================

  // Build single month stationarity (thread-safe, called from worker)
  void build_stationarity_month(size_t cache_idx, const std::string &features_dir,
                                const Feature &feature, const Asset &asset);

  // Build all months (dispatch to thread pool)
  void build_stationarity(const std::vector<std::string> &months,
                          const std::string &features_dir,
                          const Feature &feature, const Asset &asset,
                          std::function<void(std::function<void()>)> submit);

  // Finalize after build completes: compute aggregate statistics
  void finalize_stationarity();

  // ==========================================================================
  // Methods - Build PSD (Step 1: 频域分析)
  // ==========================================================================

  // Build single month PSD (thread-safe, called from worker)
  // psd_day_offset: 该月第一天在全局psd_cache中的索引
  void build_psd_month(size_t psd_day_offset, const std::string &features_dir,
                       const Feature &feature, const Asset &asset,
                       const std::string &month);

  // Build all months PSD (dispatch to thread pool)
  void build_psd(const std::vector<std::string> &months,
                 const std::string &features_dir,
                 const Feature &feature, const Asset &asset,
                 std::function<void(std::function<void()>)> submit);

  // Finalize after PSD build: compute aggregate statistics
  void finalize_psd();

  // ==========================================================================
  // Methods - Control
  // ==========================================================================

  void cancel() {
    compute.cancel = true;
    compute.status = Compute::Status::Cancelled;
  }

  void clear() {
    stationarity_cache.clear();
    psd_cache.clear();
    step0_stationarity.clear();
    step1_frequency.clear();
    step2_arma.clear();
    step3_residual.clear();
    step4_temporal_decay.clear();
    input = Input{};
    compute.reset();
  }

  bool need_rebuild(int feat_idx, int lvl, const std::string &range) const {
    return input.has_changes(feat_idx, lvl, range);
  }
};

