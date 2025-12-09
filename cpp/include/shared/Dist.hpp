#pragma once

#include "features/backend/FeatureReader.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <vector>

// Forward declarations
struct Feature;
struct Config;
struct Asset;

// ============================================================================
// Distribution Analysis Data Structure
// ============================================================================
// Designed for primary feature cross-sectional distribution analysis
// Multi-level cache architecture with version control
// Supports async computation with thread pool
// ============================================================================

struct Dist {
  // ==========================================================================
  // Input Control - User configuration (reuses existing structures)
  // ==========================================================================

  struct Input {
    // Time grouping dimension for binning
    enum class TimeGrouping : uint8_t {
      NONE = 0,    // No grouping (all data together)
      HOUR = 1,    // Group by hour of day (0-23)
      WEEKDAY = 2, // Group by weekday (Mon-Sun)
      MONTH = 3,   // Group by month (1-12)
      YEAR = 4     // Group by year
    };

    TimeGrouping time_grouping = TimeGrouping::NONE;

    // PDF sensitivity (for kernel density estimation bandwidth)
    float pdf_sensitivity = 1.0f; // Range: [0.1, 10.0]

    // Asset filter (empty = all assets)
    std::vector<size_t> selected_asset_indices; // Indices into Asset.items

    // UI state
    bool show_by_asset = false;          // Show per-asset breakdown in plots
    bool show_trajectory = true;         // Show trajectory plot
    bool show_quantile_heatmap = true;   // Show quantile heatmap

    // Change detection cache
    int cached_primary_feature_idx = -1;
    int cached_level = -1;
    std::string cached_date_range;
    TimeGrouping cached_grouping = TimeGrouping::NONE;

    // Check if input configuration has changed (const version)
    bool has_changes(const Feature &feature, const Config &config) const;
    
    // Detect and update cache (non-const version)
    void update_cache(const Feature &feature, const Config &config);
  };

  // ==========================================================================
  // RawData - Level 1 Cache: Raw feature data from FeatureReader
  // ==========================================================================

  // ==========================================================================
  // PerDateBuffer - Internal: Per-worker buffer (zero-contention)
  // ==========================================================================
  
  struct PerDateBuffer {
    std::vector<float> values;
    std::vector<uint32_t> day_idx;
    std::vector<uint32_t> time_idx;
    std::vector<uint16_t> asset_idx;
    std::vector<uint8_t> hour;
    std::vector<uint8_t> weekday;
    std::vector<uint8_t> month;
    std::vector<uint16_t> year;
    
    // Counts (for parallel aggregation)
    size_t n_samples = 0;      // Finite values only
    size_t n_invalid = 0;      // Filtered by valid flag
    size_t n_zero = 0;         // Zero values (kept)
    size_t n_nan = 0;          // NaN values (filtered)
    size_t n_pos_inf = 0;      // +Inf values (filtered)
    size_t n_neg_inf = 0;      // -Inf values (filtered)
    
    void reserve(size_t n) {
      values.reserve(n);
      day_idx.reserve(n);
      time_idx.reserve(n);
      asset_idx.reserve(n);
      hour.reserve(n);
      weekday.reserve(n);
      month.reserve(n);
      year.reserve(n);
    }
    
    void clear() {
      values.clear();
      day_idx.clear();
      time_idx.clear();
      asset_idx.clear();
      hour.clear();
      weekday.clear();
      month.clear();
      year.clear();
      n_samples = 0;
      n_invalid = 0;
      n_zero = 0;
      n_nan = 0;
      n_pos_inf = 0;
      n_neg_inf = 0;
    }
  };

  struct RawData {
    // Flattened feature values from [T][F][A] tensor
    std::vector<float> values;          // Feature values [n_samples]
    std::vector<uint32_t> day_idx;      // Day index [n_samples]
    std::vector<uint32_t> time_idx;     // Time index within day [n_samples]
    std::vector<uint16_t> asset_idx;    // Asset index [n_samples]

    // Time metadata for grouping
    std::vector<uint8_t> hour;    // Hour (0-23) [n_samples]
    std::vector<uint8_t> weekday; // Weekday (0-6, Mon=0) [n_samples]
    std::vector<uint8_t> month;   // Month (1-12) [n_samples]
    std::vector<uint16_t> year;   // Year (e.g., 2025) [n_samples]

    size_t n_samples = 0;      // Finite values (after all filtering)
    size_t n_invalid = 0;      // Invalid samples (filtered by valid flag)
    size_t n_zero = 0;         // Zero values (kept in data)
    size_t n_nan = 0;          // NaN values (filtered out)
    size_t n_pos_inf = 0;      // +Inf values (filtered out)
    size_t n_neg_inf = 0;      // -Inf values (filtered out)
    size_t n_assets = 0;

    // Version control
    size_t version = 0;
    bool loaded = false;

    // Reserve space (call before loading)
    void reserve(size_t estimated_samples);

    // Push single sample (during loading)
    void push(float value, uint32_t day, uint32_t time, uint16_t asset,
              uint8_t h, uint8_t wd, uint8_t m, uint16_t y);
    
    // Merge per-date buffers into RawData (called after parallel loading)
    void merge_buffers(const std::vector<PerDateBuffer> &buffers, size_t n_assets);

    // Invalidate and clear
    void clear();
  };

  // ==========================================================================
  // GroupedData - Level 2 Cache: Time-grouped data
  // ==========================================================================

  struct GroupedData {
    struct Bin {
      std::string key;                     // e.g., "hour_09", "weekday_1", "all"
      std::vector<float> values;           // Feature values in this bin
      std::vector<uint16_t> asset_indices; // Corresponding asset indices
      size_t n_samples = 0;

      void reserve(size_t n);
      void clear();
    };

    std::vector<Bin> bins;
    Input::TimeGrouping grouping_type = Input::TimeGrouping::NONE;

    // Version control (bound to RawData version)
    size_t raw_version = 0;
    bool valid = false;

    // Query
    size_t n_bins() const { return bins.size(); }
    const Bin &get_bin(size_t idx) const;

    // Check cache validity
    bool is_valid(size_t current_raw_version) const {
      return valid && raw_version == current_raw_version;
    }

    void clear();
  };

  // ==========================================================================
  // Statistics - Level 3 Cache: Statistical results
  // ==========================================================================

  struct Statistics {
    // 4.1 Data Integrity
    struct Integrity {
      size_t total_count = 0;      // Total samples after valid filtering
      size_t valid_count = 0;      // Samples with valid flag = 1
      size_t invalid_count = 0;    // Samples with valid flag = 0 (filtered out)
      size_t zero_count = 0;
      size_t nan_count = 0;
      size_t pos_inf_count = 0;
      size_t neg_inf_count = 0;
      float valid_pct = 0.0f;      // valid / (valid + invalid)
      float zero_pct = 0.0f;       // zero / total_count
      float nan_pct = 0.0f;        // nan / total_count
      float pos_inf_pct = 0.0f;    // +inf / total_count
      float neg_inf_pct = 0.0f;    // -inf / total_count
    };
    Integrity integrity;

    // 4.2 Moments (per bin, per asset)
    struct MomentsPerAsset {
      float mean = 0.0f;     // 1st raw moment
      float variance = 0.0f; // 2nd central moment
      float skewness = 0.0f; // 3rd standardized moment (colored)
      float kurtosis = 0.0f; // 4th standardized moment (colored)
    };

    struct MomentsPerBin {
      std::vector<MomentsPerAsset> per_asset; // [n_assets]
    };
    std::vector<MomentsPerBin> moments; // [n_bins]

    // 4.3 Quantiles (per bin, cross-sectional)
    struct QuantilesPerBin {
      float q01 = 0.0f, q05 = 0.0f, q25 = 0.0f, q50 = 0.0f;
      float q75 = 0.0f, q95 = 0.0f, q99 = 0.0f;
    };
    std::vector<QuantilesPerBin> quantiles; // [n_bins]

    // 4.4 PDF (per bin, per asset) - for distribution density plot
    struct PDFPerAsset {
      std::vector<float> bins;    // X-axis bins
      std::vector<float> density; // Y-axis density
      float bandwidth = 0.0f;     // KDE bandwidth
    };

    struct PDFPerBin {
      std::vector<PDFPerAsset> per_asset; // [n_assets]
    };
    std::vector<PDFPerBin> pdf; // [n_bins]

    // 4.5 Trajectory (per bin, per asset) - for trajectory scatter plot
    struct TrajectoryPerAsset {
      float tail_thickness = 0.0f;  // Y-axis
      float robust_skewness = 0.0f; // X-axis
      float peakedness_ccr = 0.0f;  // Color (central concentration ratio)
      float variance = 0.0f;        // Size
    };

    struct TrajectoryPerBin {
      std::vector<TrajectoryPerAsset> per_asset; // [n_assets]
    };
    std::vector<TrajectoryPerBin> trajectory; // [n_bins]

    // 4.6 Heterogeneity (per bin, cross-sectional)
    struct HeterogeneityPerBin {
      float gini = 0.0f; // Gini coefficient
      float hhi = 0.0f;  // Herfindahl-Hirschman Index
    };
    std::vector<HeterogeneityPerBin> heterogeneity; // [n_bins]

    // 4.7 Scale Robustness (global, rank correlation)
    struct ScaleRobustness {
      float rank_corr_raw_zscore = 0.0f; // Raw vs Z-score
      float rank_corr_raw_minmax = 0.0f; // Raw vs MinMax
      float rank_corr_raw_robust = 0.0f; // Raw vs Robust
    };
    ScaleRobustness scale_robustness;

    // Version control (bound to GroupedData version)
    size_t grouped_version = 0;
    bool valid = false;

    // Check cache validity
    bool is_valid(size_t current_grouped_version) const {
      return valid && grouped_version == current_grouped_version;
    }

    void clear();
  };

  // ==========================================================================
  // VisualizationCache - Level 4 Cache: Pre-computed rendering data
  // ==========================================================================

  struct VisualizationCache {
    // 5.1 PDF Plot Data (pre-computed for ImPlot)
    struct PDFPlot {
      std::vector<double> x;       // Combined X-axis for all assets
      std::vector<double> y;       // Combined Y-axis for all assets
      std::vector<size_t> offsets; // Start index for each asset [n_assets+1]
      float y_min = 0.0f, y_max = 0.0f;
    };
    std::vector<PDFPlot> pdf_plots; // [n_bins]

    // 5.2 Trajectory Scatter Plot Data
    struct TrajectoryPlot {
      std::vector<double> x;        // Robust skewness [n_assets]
      std::vector<double> y;        // Tail thickness [n_assets]
      std::vector<uint32_t> colors; // Point color (from peakedness) [n_assets]
      std::vector<float> sizes;     // Point size (from variance) [n_assets]
      float x_min = 0.0f, x_max = 0.0f;
      float y_min = 0.0f, y_max = 0.0f;
    };
    std::vector<TrajectoryPlot> trajectory_plots; // [n_bins]

    // 5.3 Quantile Heatmap Data (time × quantile matrix)
    struct QuantileHeatmap {
      std::vector<float> matrix;           // [n_bins × 7 quantiles], row-major
      size_t n_rows = 7;                   // q01, q05, q25, q50, q75, q95, q99
      size_t n_cols = 0;                   // n_bins
      std::vector<std::string> col_labels; // Bin keys
      float min_val = 0.0f, max_val = 0.0f;
    };
    QuantileHeatmap quantile_heatmap;

    // 5.4 Moments Display Data (pre-formatted strings)
    struct MomentsDisplay {
      std::string mean_text;   // e.g., "0.0023"
      std::string var_text;    // e.g., "1.452"
      std::string skew_text;   // e.g., "-0.34"
      std::string kurt_text;   // e.g., "3.12"
      uint32_t skew_color = 0; // Color for skewness
      uint32_t kurt_color = 0; // Color for kurtosis
    };
    std::vector<std::vector<MomentsDisplay>> moments_display; // [n_bins][n_assets]

    // Version control (bound to Statistics version)
    size_t stats_version = 0;
    float cached_pdf_sensitivity = -1.0f;
    bool valid = false;

    // Check cache validity
    bool is_valid(size_t current_stats_version, float pdf_sens) const {
      return valid && stats_version == current_stats_version &&
             cached_pdf_sensitivity == pdf_sens;
    }

    void clear();
  };

  // ==========================================================================
  // Compute - Async computation control
  // ==========================================================================

  struct Compute {
    enum class Status : uint8_t {
      Idle = 0,
      LoadingData = 1,
      Grouping = 2,
      Computing = 3,
      BuildingCache = 4,
      Completed = 5,
      Error = 6,
      Cancelled = 7
    };

    Status status = Status::Idle;
    std::string error_message;

    // Thread pool (shared, managed by GUI)
    // Type-erased thread pool for async computation
    std::shared_ptr<void> thread_pool;

    // Progress tracking
    std::atomic<size_t> progress_current{0};
    std::atomic<size_t> progress_total{0};

    // Cancel control
    std::atomic<bool> cancel_flag{false};

    // Futures for async tasks
    std::vector<std::future<void>> futures;

    // Async loading state (managed by DistService)
    size_t load_tasks_total = 0;
    size_t load_tasks_completed = 0;
    std::shared_ptr<void> load_buffers; // Type-erased per-date buffers
    std::shared_ptr<std::atomic<size_t>> load_completed_counter;
    std::shared_ptr<std::atomic<size_t>> load_failed_counter;

    // Timing
    std::chrono::steady_clock::time_point start_time;

    // Query
    bool is_idle() const {
      return status == Status::Idle || status == Status::Completed;
    }
    bool is_running() const {
      return status >= Status::LoadingData && status <= Status::BuildingCache;
    }
    float get_progress_pct() const {
      size_t total = progress_total.load();
      return total > 0 ? 100.0f * progress_current.load() / total : 0.0f;
    }

    void reset() {
      cancel_flag = false;
      progress_current = 0;
      progress_total = 0;
      futures.clear();
    }
  };

  // ==========================================================================
  // Main Data Members
  // ==========================================================================

  Input input;
  RawData raw;
  GroupedData grouped;
  Statistics stats;
  VisualizationCache vis_cache;
  Compute compute;

  // Version control (increment on data change)
  size_t version = 0;

  // ==========================================================================
  // Methods - Data Loading
  // ==========================================================================

  // Load raw feature data from FeatureReader cache (async)
  void load_data_async(const FeatureReader::MultiDayCache &cache,
                       const Feature &feature, const Config &config,
                       const Asset &asset);
  
  // Load data with parallel workflow (dispatch to thread pool)
  // Returns tuple: (buffers, completed_counter, failed_counter) for async polling
  // After all tasks complete, call raw.merge_buffers() to integrate
  std::tuple<std::shared_ptr<std::vector<PerDateBuffer>>,
             std::shared_ptr<std::atomic<size_t>>,
             std::shared_ptr<std::atomic<size_t>>>
  dispatch_parallel_loading(const std::vector<std::string> &available_dates,
                            const std::string &features_dir,
                            const Feature &feature,
                            const Config &config,
                            const Asset &asset,
                            std::function<void(std::function<void()>)> submit_task);

  bool is_data_loaded() const { return raw.loaded; }

  // ==========================================================================
  // Methods - Time Grouping
  // ==========================================================================

  // Apply time grouping (reorganize raw data into bins) (async)
  void apply_time_grouping_async();

  bool is_grouped() const { return grouped.valid; }

  // ==========================================================================
  // Methods - Statistics Computation
  // ==========================================================================

  // Compute all statistics (parallel on thread pool)
  void compute_all_statistics_async();

  // Individual compute functions (can be called separately)
  void compute_integrity();
  void compute_moments_parallel();        // Parallel over bins
  void compute_quantiles_parallel();      // Parallel over bins
  void compute_pdf_parallel();            // Parallel over bins×assets
  void compute_trajectory_parallel();     // Parallel over bins×assets
  void compute_heterogeneity_parallel();  // Parallel over bins
  void compute_scale_robustness();

  bool is_stats_computed() const { return stats.valid; }

  // ==========================================================================
  // Methods - Visualization Cache
  // ==========================================================================

  // Build visualization cache (call from main/GUI thread after stats computed)
  // This is fast and synchronous
  void build_visualization_cache();

  bool is_vis_cache_valid() const {
    return vis_cache.is_valid(stats.grouped_version, input.pdf_sensitivity);
  }

  // ==========================================================================
  // Methods - Control
  // ==========================================================================

  // Full computation pipeline (async)
  void start_full_compute(const FeatureReader::MultiDayCache &cache,
                          const Feature &feature, const Config &config,
                          const Asset &asset);

  // Cancel computation
  void cancel_compute();

  // Check if cache is valid (for re-rendering)
  bool need_recompute(const Feature &feature, const Config &config) const {
    return input.has_changes(feature, config);
  }

  // Invalidate all caches and increment version
  void invalidate_all() {
    ++version;
    raw.clear();
    grouped.clear();
    stats.clear();
    vis_cache.clear();
  }

  // Full clear
  void clear();
};

