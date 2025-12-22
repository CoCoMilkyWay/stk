#pragma once

#include "math/distribution/KLLcache.hpp"
#include <array>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <functional>
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

  // Welford online moments accumulator (mergeable)
  struct MomentsAccum {
    size_t n = 0;
    double mean = 0.0;
    double M2 = 0.0; // for variance
    double M3 = 0.0; // for skewness
    double M4 = 0.0; // for kurtosis

    void add(double x) {
      size_t n1 = n;
      n++;
      double delta = x - mean;
      double delta_n = delta / n;
      double delta_n2 = delta_n * delta_n;
      double term1 = delta * delta_n * n1;

      mean += delta_n;

      // Update M4 before M3 before M2 (order matters)
      M4 += term1 * delta_n2 * (n * n - 3 * n + 3) + 6 * delta_n2 * M2 -
            4 * delta_n * M3;
      M3 += term1 * delta_n * (n - 2) - 3 * delta_n * M2;
      M2 += term1;
    }

    void merge(const MomentsAccum &o) {
      if (o.n == 0)
        return;
      if (n == 0) {
        *this = o;
        return;
      }

      size_t n_ab = n + o.n;
      double delta = o.mean - mean;
      double delta2 = delta * delta;
      double delta3 = delta2 * delta;
      double delta4 = delta2 * delta2;

      double n_a = static_cast<double>(n);
      double n_b = static_cast<double>(o.n);
      double n_ab_d = static_cast<double>(n_ab);

      double new_mean = (n_a * mean + n_b * o.mean) / n_ab_d;

      double new_M2 = M2 + o.M2 + delta2 * n_a * n_b / n_ab_d;

      double new_M3 = M3 + o.M3 + delta3 * n_a * n_b * (n_a - n_b) / (n_ab_d * n_ab_d) +
                      3.0 * delta * (n_a * o.M2 - n_b * M2) / n_ab_d;

      double new_M4 =
          M4 + o.M4 +
          delta4 * n_a * n_b * (n_a * n_a - n_a * n_b + n_b * n_b) /
              (n_ab_d * n_ab_d * n_ab_d) +
          6.0 * delta2 * (n_a * n_a * o.M2 + n_b * n_b * M2) / (n_ab_d * n_ab_d) +
          4.0 * delta * (n_a * o.M3 - n_b * M3) / n_ab_d;

      n = n_ab;
      mean = new_mean;
      M2 = new_M2;
      M3 = new_M3;
      M4 = new_M4;
    }

    double var() const { return n > 1 ? M2 / n : 0.0; }

    double skew() const {
      if (n < 3 || M2 < 1e-14)
        return 0.0;
      double s = std::sqrt(M2 / n);
      return (M3 / n) / (s * s * s);
    }

    double kurt() const {
      if (n < 4 || M2 < 1e-14)
        return 0.0;
      return (n * M4) / (M2 * M2) - 3.0;
    }

    void clear() {
      n = 0;
      mean = M2 = M3 = M4 = 0.0;
    }
  };

  // KLL + Moments bundle
  struct KLLWithMoments {
    KLLcache kll;
    MomentsAccum mom;

    explicit KLLWithMoments(size_t k = KLL_CAPACITY, size_t n_recon = KLL_RESOLUTION) 
        : kll(k, n_recon) {}

    // Move only (KLLcache is move-only)
    KLLWithMoments(KLLWithMoments &&) noexcept = default;
    KLLWithMoments &operator=(KLLWithMoments &&) noexcept = default;
    KLLWithMoments(const KLLWithMoments &) = delete;
    KLLWithMoments &operator=(const KLLWithMoments &) = delete;

    void addBatch(const std::vector<float> &samples) {
      kll.addBatch(samples);
      for (float x : samples) {
        mom.add(x);
      }
    }

    void merge(const KLLWithMoments &o) {
      kll.mergeWith(o.kll);
      mom.merge(o.mom);
    }

    void clear() {
      kll.clear();
      mom.clear();
    }

    bool empty() const { return mom.n == 0; }
    size_t count() const { return mom.n; }

    // Accessors
    double mean() const { return mom.mean; }
    double var() const { return mom.var(); }
    double skew() const { return mom.skew(); }
    double kurt() const { return mom.kurt(); }

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

  public:
  };

  // Data integrity counters
  struct Integrity {
    size_t n_total = 0;
    size_t n_valid = 0;
    size_t n_zero = 0;
    size_t n_nan = 0;
    size_t n_pos_inf = 0;
    size_t n_neg_inf = 0;

    void add(const Integrity &o) {
      n_total += o.n_total;
      n_valid += o.n_valid;
      n_zero += o.n_zero;
      n_nan += o.n_nan;
      n_pos_inf += o.n_pos_inf;
      n_neg_inf += o.n_neg_inf;
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
  }

  bool need_rebuild(int feat_idx, int lvl, const std::string &range) const {
    return input.has_changes(feat_idx, lvl, range);
  }
};
