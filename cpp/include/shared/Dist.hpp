#pragma once

#include "math/distribution/KLLcache.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <string>
#include <vector>

// Forward declarations
struct Feature;
struct Config;
struct Asset;

// ============================================================================
// Distribution Analysis Data Structure (KLL-based)
// ============================================================================
// Per-month KLL caches with Welford moments
// Thread pool: one thread per month
// ============================================================================

static constexpr size_t kMinSamples = 1000; // sample不够的不纳入统计
static constexpr size_t KLL_CAPACITY = 512;
static constexpr size_t KLL_RESOLUTION = 1024;

struct Dist {

  // ==========================================================================
  // Foundation Types
  // ==========================================================================

  // KLL with lazy moments (computed from weighted samples on demand)
  struct KLLWithMoments {
    KLLcache kll;

    explicit KLLWithMoments(size_t k = KLL_CAPACITY, size_t n_recon = KLL_RESOLUTION) 
        : kll(k, n_recon) {}

    // Move only (KLLcache is move-only)
    KLLWithMoments(KLLWithMoments &&) noexcept = default;
    KLLWithMoments &operator=(KLLWithMoments &&) noexcept = default;
    KLLWithMoments(const KLLWithMoments &) = delete;
    KLLWithMoments &operator=(const KLLWithMoments &) = delete;

    void addBatch(const std::vector<float> &samples) {
      kll.addBatch(samples);
    }

    void merge(const KLLWithMoments &o) {
      kll.mergeWith(o.kll);
    }

    void clear() {
      kll.clear();
    }

    bool empty() const { return kll.empty(); }
    uint64_t count() const { return kll.totalCount(); }

    // Moments (lazy, O(stored_size) ≈ O(k * log(n/k)))
    double mean() const { return kll.empty() ? 0.0 : kll.mean(); }
    double var() const { return kll.empty() ? 0.0 : kll.var(); }
    double skew() const { return kll.empty() ? 0.0 : kll.skew(); }
    double kurt() const { return kll.empty() ? 0.0 : kll.kurt(); }

    // Quantile query: simple linear interpolation on ICDF grid
    double quantile(double q) const {
      if (kll.empty()) return 0.0;
      auto icdf = kll.exportICDF();
      return interpolate(q, icdf.x, icdf.y, icdf.n);
    }

    // CDF query: simple linear interpolation on CDF grid
    double queryCDF(double x) const {
      if (kll.empty()) return 0.0;
      auto cdf = kll.exportCDF();
      return interpolate(x, cdf.x, cdf.y, cdf.n);
    }

    // Export PDF grid (zero-copy)
    void exportPDF(const float *&x, const float *&f, size_t &n) const {
      if (kll.empty()) {
        x = nullptr;
        f = nullptr;
        n = 0;
        return;
      }
      auto pdf = kll.exportPDF();
      x = pdf.x;
      f = pdf.y;
      n = pdf.n;
    }

    // Export CDF grid (zero-copy)
    void exportCDF(const float *&x, const float *&F, size_t &n) const {
      if (kll.empty()) {
        x = nullptr;
        F = nullptr;
        n = 0;
        return;
      }
      auto cdf = kll.exportCDF();
      x = cdf.x;
      F = cdf.y;
      n = cdf.n;
    }

    // Export ICDF grid (zero-copy)
    void exportQuantile(const float *&u, const float *&Q, size_t &n) const {
      if (kll.empty()) {
        u = nullptr;
        Q = nullptr;
        n = 0;
        return;
      }
      auto icdf = kll.exportICDF();
      u = icdf.x;
      Q = icdf.y;
      n = icdf.n;
    }

  private:
    // Simple linear interpolation on precomputed grid
    static double interpolate(double query, const float* x, const float* y, size_t n) {
      if (n == 0) return 0.0;
      if (n == 1) return y[0];
      if (query <= x[0]) return y[0];
      if (query >= x[n-1]) return y[n-1];
      
      size_t lo = 0, hi = n - 1;
      while (hi - lo > 1) {
        size_t mid = (lo + hi) / 2;
        if (x[mid] <= query) lo = mid;
        else hi = mid;
      }
      
      double t = (query - x[lo]) / (x[hi] - x[lo]);
      return y[lo] + t * (y[hi] - y[lo]);
    }
  };

  // Data integrity counters
  struct Integrity {
    size_t n_total = 0;
    size_t n_valid = 0;
    size_t n_zero = 0;
    size_t n_nan = 0;
    size_t n_pos_inf = 0;
    size_t n_neg_inf = 0;
    float val_min = 0.0f;
    float val_max = 0.0f;

    void add(const Integrity &o) {
      n_total += o.n_total;
      n_valid += o.n_valid;
      n_zero += o.n_zero;
      n_nan += o.n_nan;
      n_pos_inf += o.n_pos_inf;
      n_neg_inf += o.n_neg_inf;
      if (o.n_valid > 0) {
        if (n_valid == o.n_valid) {
          // First valid data
          val_min = o.val_min;
          val_max = o.val_max;
        } else {
          val_min = std::min(val_min, o.val_min);
          val_max = std::max(val_max, o.val_max);
        }
      }
    }

    float valid_pct() const {
      return n_total > 0 ? 100.0f * n_valid / n_total : 0.0f;
    }

    float zero_pct() const {
      return n_valid > 0 ? 100.0f * n_zero / n_valid : 0.0f;
    }

    float nan_pct() const {
      return n_total > 0 ? 100.0f * n_nan / n_total : 0.0f;
    }

    float inf_pct() const {
      return n_total > 0 ? 100.0f * (n_pos_inf + n_neg_inf) / n_total : 0.0f;
    }

    void clear() { *this = Integrity{}; }

    void update_minmax(float val) {
      if (n_valid == 1) {
        val_min = val_max = val;
      } else {
        if (val < val_min) val_min = val;
        if (val > val_max) val_max = val;
      }
    }
  };

  // ==========================================================================
  // Monthly Cache
  // ==========================================================================

  struct MonthlyCache {
    std::string month; // "YYYYMM"

    Integrity integrity;

    std::vector<KLLWithMoments> by_asset;   // [n_assets]
    std::vector<KLLWithMoments> by_hour;    // [24] dynamic
    std::vector<KLLWithMoments> by_weekday; // [7] dynamic
    KLLWithMoments total;

    size_t n_assets = 0;
    bool valid = false;

    void clear() {
      month.clear();
      integrity.clear();
      by_asset.clear();
      by_hour.clear();
      by_weekday.clear();
      total.clear();
      n_assets = 0;
      valid = false;
    }

    void init(size_t n_assets_val, size_t kll_k = KLL_CAPACITY) {
      n_assets = n_assets_val;
      by_asset.clear();
      by_asset.reserve(n_assets);
      for (size_t i = 0; i < n_assets; ++i) {
        by_asset.emplace_back(kll_k);
      }
      by_hour.clear();
      by_hour.reserve(24);
      for (size_t i = 0; i < 24; ++i) {
        by_hour.emplace_back(kll_k);
      }
      by_weekday.clear();
      by_weekday.reserve(7);
      for (size_t i = 0; i < 7; ++i) {
        by_weekday.emplace_back(kll_k);
      }
      total = KLLWithMoments(kll_k);
    }

  };

  // ==========================================================================
  // Query Result
  // ==========================================================================

  struct QueryResult {
    struct BinStats {
      std::string key;
      size_t n_samples = 0;

      // Moments
      float mean = 0.0f;
      float variance = 0.0f;
      float skewness = 0.0f;
      float kurtosis = 0.0f;

      // Quantiles: q01, q05, q25, q50, q75, q95, q99
      std::array<float, 7> quantiles = {};

      // PDF (zero-copy pointers to KLL internal cache)
      const float *pdf_x = nullptr;
      const float *pdf_y = nullptr;
      size_t pdf_n = 0;

      void extract_from(const KLLWithMoments &src) {
        n_samples = src.count();
        mean = static_cast<float>(src.mean());
        variance = static_cast<float>(src.var());
        skewness = static_cast<float>(src.skew());
        kurtosis = static_cast<float>(src.kurt());

        if (n_samples > 0) {
          quantiles[0] = static_cast<float>(src.quantile(0.01));
          quantiles[1] = static_cast<float>(src.quantile(0.05));
          quantiles[2] = static_cast<float>(src.quantile(0.25));
          quantiles[3] = static_cast<float>(src.quantile(0.50));
          quantiles[4] = static_cast<float>(src.quantile(0.75));
          quantiles[5] = static_cast<float>(src.quantile(0.95));
          quantiles[6] = static_cast<float>(src.quantile(0.99));

          // Zero-copy: get pointers to KLL internal cache
          src.exportPDF(pdf_x, pdf_y, pdf_n);
        }
      }
    };

    // Storage for merged KLLs (keeps pointers valid)
    std::vector<KLLWithMoments> kll_storage;
    std::vector<BinStats> bins;
    Integrity integrity;
    bool valid = false;

    void clear() {
      kll_storage.clear();
      bins.clear();
      integrity.clear();
      valid = false;
    }
  };


  // ==========================================================================
  // Compute Control
  // ==========================================================================

  struct Compute {
    enum class Status : uint8_t {
      Idle,
      Building,
      Querying,
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
      return status == Status::Building || status == Status::Querying;
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
  // Input Control
  // ==========================================================================

  struct Input {
    enum class GroupBy : uint8_t { MONTH, WEEKDAY, HOUR, ASSETS };

    GroupBy group_by = GroupBy::MONTH;
    int focus_month_idx = -1; // for trajectory highlight

    // Cache keys for change detection
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

  Input input;
  std::vector<MonthlyCache> cache; // [n_months], sorted by month
  QueryResult result;
  Compute compute;

  // Global aggregations (computed once, reused for PDF visualization)
  std::vector<KLLWithMoments> global_by_hour;    // [24]
  std::vector<KLLWithMoments> global_by_weekday; // [7]
  std::vector<KLLWithMoments> global_by_asset;   // [n_assets]
  KLLWithMoments global_total;                   // merged from all assets

  // ==========================================================================
  // Stability Visualization (cross-sectional distribution stability)
  // ==========================================================================

  struct StabilityViz {
    // Only includes assets with count >= 1000
    std::vector<size_t> asset_idx; // original asset indices [n_valid]
    std::vector<float> x_norm;     // normalized x position [0,1] [n_valid]
    std::vector<float> color_t;    // color parameter [0,1] [n_valid]
    float score_min = 0.0f;        // min signed-square score (for label)
    float score_max = 0.0f;        // max signed-square score (for label)

    bool valid = false;

    void clear() {
      asset_idx.clear();
      x_norm.clear();
      color_t.clear();
      score_min = score_max = 0.0f;
      valid = false;
    }
  };
  StabilityViz stability;

  // ==========================================================================
  // Feature Cache (for OrderFlow plot overlay)
  // ==========================================================================
  // Stores resampled feature data at minute granularity (L1 index 0-254)
  // All levels (L0/L1/L2) are converted to minute-level OHLC bars

  struct FeatureCache {
    // Single minute bar (OHLC aggregation)
    struct MinuteBar {
      float open = 0.0f;
      float high = std::numeric_limits<float>::lowest();
      float low = std::numeric_limits<float>::max();
      float close = 0.0f;
      bool valid = false;

      void update(float val) {
        if (std::isnan(val) || std::isinf(val))
          return;
        if (!valid) {
          open = val;
          high = val;
          low = val;
          valid = true;
        }
        if (val > high)
          high = val;
        if (val < low)
          low = val;
        close = val;
      }

      void clear() {
        open = 0.0f;
        high = std::numeric_limits<float>::lowest();
        low = std::numeric_limits<float>::max();
        close = 0.0f;
        valid = false;
      }
    };

    // Single day cache (per asset)
    struct DayCache {
      std::string date;                                    // "YYYYMMDD"
      std::vector<std::vector<MinuteBar>> asset_bars;      // [asset_idx][minute_idx]

      void init(size_t n_assets) {
        asset_bars.resize(n_assets);
        for (auto &bars : asset_bars) {
          bars.resize(255);
          for (auto &bar : bars)
            bar.clear();
        }
      }

      void clear() {
        date.clear();
        asset_bars.clear();
      }
    };

    std::vector<DayCache> days;              // sorted by date (after finalize)
    std::map<std::string, size_t> date_to_idx;
    size_t n_assets = 0;                     // number of assets
    int level = -1;
    int feature_idx = -1;
    bool valid = false;

    // Mutex for thread-safe day insertion during parallel build
    mutable std::mutex mutex;

    void clear() {
      std::lock_guard<std::mutex> lock(mutex);
      days.clear();
      date_to_idx.clear();
      n_assets = 0;
      level = -1;
      feature_idx = -1;
      valid = false;
    }

    // Thread-safe: add days from a month (called from build_month)
    void add_days(std::vector<DayCache> &&month_days) {
      std::lock_guard<std::mutex> lock(mutex);
      for (auto &day : month_days) {
        days.push_back(std::move(day));
      }
    }

    // Build date index after all months are processed (called from finalize)
    void build_index() {
      // Sort by date
      std::sort(days.begin(), days.end(),
                [](const DayCache &a, const DayCache &b) { return a.date < b.date; });
      // Build index
      date_to_idx.clear();
      for (size_t i = 0; i < days.size(); ++i) {
        date_to_idx[days[i].date] = i;
      }
      valid = true;
    }

    // Check if cache matches current selection
    bool matches(int lvl, int feat_idx) const {
      return valid && level == lvl && feature_idx == feat_idx;
    }
  };

  FeatureCache feature_cache;

  // ==========================================================================
  // Methods - Build
  // ==========================================================================

  // Build single month cache (thread-safe, called from worker)
  void build_month(size_t cache_idx, const std::string &features_dir,
                   const Feature &feature, const Asset &asset);

  // Build all months (dispatch to thread pool)
  void build_all(const std::vector<std::string> &months,
                 const std::string &features_dir, const Feature &feature,
                 const Asset &asset,
                 std::function<void(std::function<void()>)> submit);

  // ==========================================================================
  // Methods - Finalize & Query
  // ==========================================================================

  // Finalize after build_all completes: build globals + assets + query
  void finalize();

  // Re-query with different grouping (for UI group_by switching)
  void query(Input::GroupBy group_by);

  // ==========================================================================
  // Methods - Control
  // ==========================================================================

  void cancel() {
    compute.cancel = true;
    compute.status = Compute::Status::Cancelled;
  }

  void clear() {
    input = Input{};
    cache.clear();
    result.clear();
    compute.reset();
    global_by_hour.clear();
    global_by_weekday.clear();
    global_by_asset.clear();
    global_total.clear();
    stability.clear();
    feature_cache.clear();
  }

  bool need_rebuild(int feat_idx, int lvl, const std::string &range) const {
    return input.has_changes(feat_idx, lvl, range);
  }
};
