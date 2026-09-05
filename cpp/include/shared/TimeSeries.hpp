#pragma once

#include "features/Backend/FeatureRead.hpp"
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
    bool adf_pass = false; // p < 0.05

    float kpss_statistic = 0.0f;
    float kpss_pvalue = 0.0f;
    bool kpss_pass = false; // p > 0.05

    size_t n_samples = 0;
    bool valid = false;
  };

  // MonthlyCache (对仗 Dist::MonthlyCache)
  struct MonthlyStationarity {
    std::string month;                      // "YYYYMM"
    std::vector<StationarityCell> by_asset; // [n_assets]
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

  // ==========================================================================
  // Step 1: PSD Heatmap (per-day per-asset)
  // ==========================================================================
  //
  // 尺度bin定义 (共128个):
  //   秒级:   2,3,...,59    → 58个 (idx 0~57)
  //   分钟级: 1,2,...,59    → 59个 (idx 58~116)
  //   小时级: 1,2,...,10    → 10个 (idx 117~126)
  //   DC:                   → 1个  (idx 127)
  //
  struct PSDHeatmap {
    static constexpr size_t N_SCALE_BINS = 128;

    // ===== 核心数据: per-asset per-day =====
    std::vector<float> per_asset_data; // [n_days * n_assets * N_SCALE_BINS]
    std::vector<std::string> dates;    // [n_days]
    size_t n_days = 0;
    size_t n_assets = 0;

    // ===== 渲染用缓存 (finalize时计算) =====
    int sampling_level = 0;     // 0=秒, 1=分钟, 2=小时
    size_t first_valid_day = 0; // 第一个FFT满的天索引 (用于默认X range)
    size_t default_y_start = 0; // 默认Y range起始 (L0=0, L1=58, L2=117)

    std::vector<size_t> valid_indices; // 有效天索引
    std::vector<float> render_data;    // [N_SCALE_BINS * valid_days] log变换后
    float scale_min = -1.0f;
    float scale_max = 3.0f;

    // 轴刻度
    std::vector<double> tick_positions;
    std::vector<std::string> tick_labels;
    std::vector<float> plot_x; // [N_SCALE_BINS]

    int selected_day = -1;
    bool valid = false;

    void clear() { *this = PSDHeatmap{}; }

    void init(size_t days, size_t assets, int level) {
      n_days = days;
      n_assets = assets;
      sampling_level = level;
      // 默认Y range根据level
      if (level == 0) {
        default_y_start = 0; // 秒级: 从2s开始
      } else if (level == 1) {
        default_y_start = 58; // 分钟级: 从1min开始
      } else {
        default_y_start = 117; // 小时级: 从1h开始
      }
      first_valid_day = 0;
      per_asset_data.assign(n_days * n_assets * N_SCALE_BINS, 0.0f);
      dates.assign(n_days, std::string{});
      valid_indices.clear();
      render_data.clear();
      tick_positions.clear();
      tick_labels.clear();
      plot_x.clear();
      selected_day = -1;
      valid = false;
    }

    // 获取某天某资产的PSD [N_SCALE_BINS]
    float *asset_day_psd(size_t day_idx, size_t asset_idx) {
      assert(day_idx < n_days && asset_idx < n_assets);
      return per_asset_data.data() + (day_idx * n_assets + asset_idx) * N_SCALE_BINS;
    }

    const float *asset_day_psd(size_t day_idx, size_t asset_idx) const {
      assert(day_idx < n_days && asset_idx < n_assets);
      return per_asset_data.data() + (day_idx * n_assets + asset_idx) * N_SCALE_BINS;
    }

    size_t valid_days() const { return valid_indices.size(); }
  };

  // Step 1: 频域分析统计 (从热力图聚合)
  struct FrequencyAnalysis {
    float sec_power_ratio = 0.0f;  // 秒级能量占比 (bin 0-57, 周期2-59秒)
    float min_power_ratio = 0.0f;  // 分钟级能量占比 (bin 58-116, 周期1-59分钟)
    float hour_power_ratio = 0.0f; // 小时级能量占比 (bin 117-126, 周期1-10小时)
    float dc_power_ratio = 0.0f;   // DC能量占比 (bin 127)

    std::vector<float> avg_power_spectrum; // [N_SCALE_BINS]

    bool valid = false;
    void clear() { *this = FrequencyAnalysis{}; }
  };

  // ==========================================================================
  // 共享数据: Phase1读取的所有月数据 (所有worker只读访问)
  // ==========================================================================

  struct DayRange {
    size_t month_idx;    // 在 months 数组中的索引
    size_t day_in_month; // 月内天索引
    size_t t_start;      // tensor内的样本起始
    size_t t_end;        // tensor内的样本结束
    std::string date;
  };

  struct SharedMonthData {
    std::vector<FeatureRead::MonthTensor> months; // [n_months]
    std::vector<DayRange> day_ranges;             // 连续时间索引

    size_t n_months = 0;
    size_t n_assets = 0;
    size_t n_days = 0;
    int level = 0;
    size_t F_selected = 0;
    bool has_valid_flag = false;
    L2::ValidType valid_type = L2::ValidType::ALL; // 主特征的门控语义 (_meta 位选择, 见 Meta.hpp)

    size_t total_days() const { return day_ranges.size(); }

    void clear() {
      months.clear();
      day_ranges.clear();
      n_months = 0;
      n_assets = 0;
      n_days = 0;
      level = 0;
      F_selected = 0;
      has_valid_flag = false;
      valid_type = L2::ValidType::ALL;
    }
  };

  // ==========================================================================
  // Worker分配 (每个worker的职责)
  // ==========================================================================

  struct WorkerAllocation {
    size_t worker_id = 0;
    size_t month_idx = 0;   // 负责加载的月
    size_t asset_start = 0; // 负责的asset范围起始
    size_t asset_end = 0;   // 负责的asset范围结束
    size_t day_start = 0;   // 负责的天范围起始 (用于Stage 4)
    size_t day_end = 0;     // 负责的天范围结束
  };

  // ==========================================================================
  // 同步Barriers
  // ==========================================================================

  struct Barriers {
    std::atomic<size_t> phase1_ready{0};   // Phase 1 加载完成计数
    std::atomic<bool> shared_built{false}; // SharedMonthData 构建完成

    void reset() {
      phase1_ready = 0;
      shared_built = false;
    }
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
      Loading,  // Phase 1: 加载月数据
      Building, // Phase 2: 按asset计算
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
      return status == Status::Idle || status == Status::Done;
    }

    bool is_busy() const {
      return status == Status::Loading || status == Status::Building;
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

  // 共享数据 (Phase1读取, 所有worker只读访问, finalize后释放)
  SharedMonthData shared;

  // 同步barriers
  Barriers barriers;

  // Stationarity cache: [n_months][n_assets]
  std::vector<MonthlyStationarity> stationarity_cache;

  // PSD cache: per-asset per-day (持久)
  PSDHeatmap psd_cache;

  // Step 2: ARMA cache: per-asset
  struct ARMACell {
    std::vector<float> acf;  // [max_lag+1]
    std::vector<float> pacf; // [max_lag+1]
    int cutoff_lag_acf = 0;
    int cutoff_lag_pacf = 0;
    bool valid = false;
  };
  std::vector<ARMACell> arma_cache; // [n_assets]

  // Step 3: Residual cache: per-asset
  struct ResidualCell {
    float ljung_box_stat = 0.0f;
    float ljung_box_pval = 0.0f;
    float arch_lm_stat = 0.0f;
    float arch_lm_pval = 0.0f;
    float jarque_bera_stat = 0.0f;
    float jarque_bera_pval = 0.0f;
    float skewness = 0.0f;
    float kurtosis = 0.0f;
    bool valid = false;
  };
  std::vector<ResidualCell> residual_cache; // [n_assets]

  // Step 4: Temporal Decay cache: per-day
  struct TemporalCell {
    float gini = 0.0f;
    float hhi = 0.0f;
    float rank_corr = 0.0f; // vs previous day
    bool valid = false;
  };
  std::vector<TemporalCell> temporal_cache; // [n_days]

  // Step results (聚合统计)
  StationarityTest step0_stationarity;
  FrequencyAnalysis step1_frequency;
  ARMAAnalysis step2_arma;
  ResidualAnalysis step3_residual;
  TemporalDecay step4_temporal_decay;

  Input input;
  Compute compute;

  // ==========================================================================
  // Methods - Unified Build (替代分离的 build_stationarity/build_psd)
  // ==========================================================================

  // 统一入口: Phase 1 并行加载 + Phase 2 流水线计算所有stages
  // 调用者等待 compute.done == compute.total，然后调用 finalize_all
  void build_all(const std::vector<std::string> &months,
                 const std::string &features_dir,
                 const Feature &feature, const Asset &asset,
                 std::function<void(std::function<void()>)> submit);

  // Finalize: 聚合所有stage结果，释放共享数据
  void finalize_all();

  // ==========================================================================
  // Methods - Control
  // ==========================================================================

  void cancel() {
    compute.cancel = true;
    compute.status = Compute::Status::Cancelled;
  }

  void clear() {
    shared.clear();
    barriers.reset();
    stationarity_cache.clear();
    psd_cache.clear();
    arma_cache.clear();
    residual_cache.clear();
    temporal_cache.clear();
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
