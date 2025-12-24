#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

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

  // Step 0: 平稳性检验结果
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

  // Step 1: 频域分析结果
  struct FrequencyAnalysis {
    float q_factor = 0.0f;
    float peak_frequency = 0.0f;
    float peak_bandwidth = 0.0f;
    bool q_factor_pass = false;

    float low_freq_power_ratio = 0.0f;
    bool has_low_freq = false;

    size_t n_significant_peaks = 0;
    bool has_peaks = false;

    std::vector<float> frequencies;
    std::vector<float> power_spectrum;
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

  // Compute Control

  struct Compute {
    enum class Status : uint8_t {
      Idle,
      Running,
      Done,
      Error,
      Cancelled
    };

    Status status = Status::Idle;
    std::string error;

    std::atomic<size_t> current_step{0};
    std::atomic<size_t> total_steps{5};
    std::atomic<bool> cancel{false};

    float progress() const {
      return 100.0f * current_step.load() / total_steps.load();
    }

    bool is_idle() const {
      return status == Status::Idle || status == Status::Done;
    }

    bool is_busy() const { return status == Status::Running; }

    void reset() {
      status = Status::Idle;
      error.clear();
      current_step = 0;
      total_steps = 5;
      cancel = false;
    }
  };

  // Main Data Members

  StationarityTest step0_stationarity;
  FrequencyAnalysis step1_frequency;
  ARMAAnalysis step2_arma;
  ResidualAnalysis step3_residual;
  TemporalDecay step4_temporal_decay;

  Compute compute;

  int feature_idx = -1;
  int level = -1;

  // Methods

  void cancel() {
    compute.cancel = true;
    compute.status = Compute::Status::Cancelled;
  }

  void clear() {
    step0_stationarity.clear();
    step1_frequency.clear();
    step2_arma.clear();
    step3_residual.clear();
    step4_temporal_decay.clear();
    compute.reset();
    feature_idx = -1;
    level = -1;
  }

  bool need_rebuild(int feat_idx, int lvl) const {
    return feature_idx != feat_idx || level != lvl;
  }
};

