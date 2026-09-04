#pragma once

#include "features/Backend/FeatureRead.hpp"
#include "math/distribution/KLLcache.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

// ============================================================================
// Distribution Analysis (KLL-based, 资产优先流式)
// ============================================================================
// 层次: 资产优先 —— 先拿到一个资产的全时段 (抽样) 数据, 再流下一个资产.
// 这样月度漂移/时刻/星期视图在前几百个资产完成时就是覆盖全时段的图景,
// 而不是等所有月份跑完才拼齐漂移.
//
// 发布协议 (与 FeatureStore 的门控同一姿态: 单调发布, UI 画已完成前缀):
//
//   worker (单线程, DistService):
//     Phase IO:  日期枚举 → 自适应日抽样 (块 ≤ kMaxBlockBytes; 区间小则 stride=1 全量;
//                stride 与交易周互质避免星期偏置) → 逐日载入常驻块, days_loaded++
//     Phase 流:  for a in 0..A:                  ← 资产优先, 每资产走完全时段
//                  无锁收集样本 → 加锁发布:
//                    assets[a] (发布即终态) / months[*] / by_hour / by_weekday / total
//                  assets_done = a+1             ← UI 下一帧就画到 [0, assets_done)
//     Stability: W2 偏移 + W1 主成分投影排序, 全部完成后一次 (分块持锁, 不冻 UI)
//
//   UI (每帧): 持 mutex 渲染全部视图; 进度 (status/days_loaded/assets_done) 原子免锁.
//   生命周期: 改参数 → 新请求即取消在跑重算; 切走 Tab → 立刻中断 + clear() 释放;
//             切回 Tab → 自动重算 (构建只需秒级, 不留常驻).
// ============================================================================

static constexpr size_t kMinSamples = 1000;     // sample 不够的不纳入统计
static constexpr size_t kMinAssetSamples = 100; // 资产纳入截面视图的最小样本数
static constexpr size_t KLL_CAPACITY = 512;     // 月/小时/星期/全局 sketch
static constexpr size_t KLL_RESOLUTION = 1024;
static constexpr size_t KLL_ASSET_CAPACITY = 256;         // 每资产 sketch (5000+ 个, 精度换内存)
static constexpr size_t KLL_ASSET_RESOLUTION = 128;       // 资产 PDF 网格 (画细线, 128 点足够)
static constexpr size_t kMaxBlockBytes = size_t(2) << 30; // 常驻块上限, 超出则日抽样

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
      if (kll.empty())
        return 0.0;
      auto icdf = kll.exportICDF();
      return interpolate(q, icdf.x, icdf.y, icdf.n);
    }

    // CDF query: simple linear interpolation on CDF grid
    double queryCDF(double x) const {
      if (kll.empty())
        return 0.0;
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
    static double interpolate(double query, const float *x, const float *y, size_t n) {
      if (n == 0)
        return 0.0;
      if (n == 1)
        return y[0];
      if (query <= x[0])
        return y[0];
      if (query >= x[n - 1])
        return y[n - 1];

      size_t lo = 0, hi = n - 1;
      while (hi - lo > 1) {
        size_t mid = (lo + hi) / 2;
        if (x[mid] <= query)
          lo = mid;
        else
          hi = mid;
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
        if (val < val_min)
          val_min = val;
        if (val > val_max)
          val_max = val;
      }
    }
  };

  // ==========================================================================
  // Slots (worker 写, UI 持锁读)
  // ==========================================================================

  // 每资产终态槽: 全区间累积 (不按月×资产存, 内存 ÷ 月数)
  struct AssetSlot {
    KLLWithMoments kll{KLL_ASSET_CAPACITY, KLL_ASSET_RESOLUTION};
    Integrity integrity;

    void clear() {
      kll.clear();
      integrity.clear();
    }
  };

  // 每月聚合 (月度漂移视图 + 滑条标签)
  struct MonthAgg {
    std::string month; // "YYYYMM"
    KLLWithMoments total;
    Integrity integrity;
  };

  // ==========================================================================
  // Build Params (GUI 线程解析好的快照, worker 只读)
  // ==========================================================================

  struct Params {
    int feature_idx = -1;
    int level = -1;
    std::vector<size_t> columns; // [特征列 (+ valid 列)], 由 Feature 元数据解析
  };

  // ==========================================================================
  // Stability Visualization (cross-sectional distribution stability)
  // ==========================================================================

  struct StabilityViz {
    // Only includes assets with count >= kMinAssetSamples
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

  // ==========================================================================
  // State
  // ==========================================================================

  enum class Status : uint8_t { Idle,
                                Building,
                                Done,
                                Cancelled };

  // 进度: 原子, UI 免锁读
  std::atomic<Status> status{Status::Idle};
  std::atomic<size_t> days_loaded{0}; // Phase IO 进度
  std::atomic<size_t> days_total{0};  // 抽样后入块天数
  std::atomic<size_t> assets_done{0}; // Phase 流 进度 (全程单调, 槽发布即终态)

  // 聚合状态: mutex 保护 (worker 逐资产短锁发布; UI 渲染帧内持锁)
  mutable std::mutex mutex;

  Params params;                          // 本次构建参数
  std::vector<MonthAgg> months;           // [n_months]
  std::vector<AssetSlot> assets;          // [A] 全区间累积
  std::vector<KLLWithMoments> by_hour;    // [24] 全区间
  std::vector<KLLWithMoments> by_weekday; // [7]  全区间
  KLLWithMoments total;                   // 全区间
  Integrity integrity;                    // 全区间
  StabilityViz stability;

  // ==========================================================================
  // Methods (worker 线程调用, 内部按需加锁)
  // ==========================================================================

  // 重置全部状态并进入 Building (sketch 容量复用, 稳态零分配)
  void reset_for_build(Params p, const std::vector<std::string> &month_keys, size_t n_assets);

  // 全区间构建: Phase IO (逐日入块) + Phase 资产流 (逐资产发布); 被取消返回 false
  // block 为 worker 私有的复用缓冲 (preallocate 在内部按抽样天数做)
  bool build(FeatureRead &reader, FeatureRead::MonthTensor &block,
             const std::atomic<bool> &cancel);

  // 全部资产完成后: W2 偏移 + 主成分投影排序 (分块持锁, 不冻 UI); 被取消返回 false
  bool build_stability(const std::atomic<bool> &cancel);

  void clear();

private:
  // worker 私有采样缓冲 (跨资产复用, 稳态零分配)
  std::vector<float> scratch_all_;
  std::array<std::vector<float>, 24> scratch_hour_;
  std::array<std::vector<float>, 7> scratch_weekday_;
  std::vector<std::vector<float>> scratch_month_; // [n_months]
};
