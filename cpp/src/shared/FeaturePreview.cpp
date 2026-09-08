#include "shared/FeaturePreview.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "features/Operator/TS/Meta/Meta.hpp" // fmeta::valid
#include "misc/profiler.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <random>

// ============================================================================
// Reset
// ============================================================================

void FeaturePreview::reset_for_build(std::vector<size_t> feat_cols,
                                     std::vector<L2::ValidType> valid_types,
                                     size_t meta_col, const std::vector<std::string> &month_keys,
                                     size_t n_features, size_t n_assets) {
  TraceN("PreviewReset");
  std::lock_guard<std::mutex> lock(mutex);

  feat_cols_ = std::move(feat_cols);
  valid_types_ = std::move(valid_types);
  assert(!feat_cols_.empty() && feat_cols_.size() == valid_types_.size());
  assert(std::is_sorted(feat_cols_.begin(), feat_cols_.end()));
  assert(meta_col < n_features && n_assets > 0 && !month_keys.empty());
  meta_col_ = meta_col;
  months_ = month_keys;
  A_ = n_assets;

  const size_t n_pv = feat_cols_.size();
  assert(feat_cols_.back() < n_features);

  // cells 尺寸对得上就清零复用 (Cell 是 POD 数组, assign 即清)
  if (cells.size() != n_features)
    cells.assign(n_features, Cell{});
  else
    std::fill(cells.begin(), cells.end(), Cell{});

  // 每特征 sketch: 数量对得上只 clear (KLL 保留 buffer 容量)
  if (klls_.size() != n_pv) {
    klls_.clear();
    klls_.reserve(n_pv);
    for (size_t i = 0; i < n_pv; ++i)
      klls_.emplace_back(kPvKllCapacity, kPvKllResolution);
  } else {
    for (auto &kll : klls_)
      kll.clear();
  }
  psd_sum_.assign(n_pv, {});
  psd_n_.assign(n_pv, 0);

  // 抽样资产序: 固定种子洗牌 → 无偏, 轮间旋转取片覆盖不同资产
  asset_order_.resize(A_);
  for (size_t a = 0; a < A_; ++a)
    asset_order_[a] = static_cast<uint32_t>(a);
  std::shuffle(asset_order_.begin(), asset_order_.end(), std::mt19937{0x5eed});

  rounds_done.store(0, std::memory_order_relaxed);
  rounds_total.store(0, std::memory_order_relaxed);
  epoch.fetch_add(1, std::memory_order_release); // 清空态也是一次数据变化
  status.store(Status::Building, std::memory_order_release);
}

// ============================================================================
// Build (轮训: 轮 = 抽样日, 轮内按特征分块, 块末发布; 首帧 = 一块的 IO + 扫描)
// ============================================================================

bool FeaturePreview::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("PreviewBuild");
  assert(A_ > 0 && !feat_cols_.empty() && "reset_for_build 未调");
  assert(level_valid_rows(kPvLevel) == kPvVR);

  // 日期枚举 → 固定种子洗牌取前 kPvRounds 个 (无偏覆盖全区间, 轮序即收敛序)
  std::vector<std::string> dates;
  {
    TraceN("EnumDates");
    for (const auto &key : months_) {
      auto ds = reader.list_dates(key.substr(0, 4), key.substr(4, 2));
      for (auto &d : ds)
        dates.push_back(std::move(d));
    }
  }
  if (dates.empty())
    return true; // 库为空: 直接 Done, 表格全 "—"
  std::shuffle(dates.begin(), dates.end(), std::mt19937{0x5eed});
  const size_t n_rounds = std::min(kPvRounds, dates.size());
  dates.resize(n_rounds);
  rounds_total.store(n_rounds, std::memory_order_release);

  const size_t n_pv = feat_cols_.size();
  const size_t n_draw = std::min(kPvAssetsPerRound, A_);
  constexpr float qnan = std::numeric_limits<float>::quiet_NaN();
  std::vector<size_t> cols;
  cols.reserve(kPvBlockCols + 1);

  for (size_t r = 0; r < n_rounds; ++r) {
    const size_t a_off = (r * n_draw) % A_; // 洗牌序旋转取片

    for (size_t f0 = 0; f0 < n_pv; f0 += kPvBlockCols) {
      if (cancel.load(std::memory_order_relaxed))
        return false;
      const size_t f1 = std::min(f0 + kPvBlockCols, n_pv);
      const size_t nb = f1 - f0;

      // 块 IO: nb 值列 + _meta 作为末值列 (has_valid=false → 门控在扫描侧按
      // 各特征 valid_type 判; plane 只把真 NaN 折成哨兵, _meta 值原样保留)
      {
        TraceN("PreviewIO");
        cols.assign(feat_cols_.begin() + f0, feat_cols_.begin() + f1);
        cols.push_back(meta_col_);
        plane_.prepare(A_, kPvLevel, 1, nb + 1, 1, kPvBlockCols + 1);
        plane_.load_day(reader, dates[r], cols, false, L2::ValidType::ALL, 0, 0);
      }

      // 块扫描: 每特征抽 n_draw 个资产
      TraceN("PreviewScan");
      for (size_t i = 0; i < nb; ++i) {
        const size_t slot = f0 + i;
        const L2::ValidType vt = valid_types_[slot];
        samples_.clear();

        for (size_t k = 0; k < n_draw; ++k) {
          const size_t a = asset_order_[(a_off + k) % A_];
          const feature_storage_t *v = plane_.series(i, a, 0);
          const feature_storage_t *mv = plane_.series(nb, a, 0);
          for (size_t t = 0; t < kPvVR; ++t) {
            const float x = static_cast<float>(v[t]);
            // 哨兵 NaN (真 NaN 已折叠) / 门控不过 / ±inf 一律不进统计
            const bool ok = x == x && fmeta::valid(static_cast<float>(mv[t]), vt) && !std::isinf(x);
            day_buf_[t] = ok ? x : qnan;
            if (ok)
              samples_.push_back(x);
          }
          if (psd_.compute(day_buf_.data())) {
            auto &sum = psd_sum_[slot];
            for (size_t q = 0; q < PvDayPSD::N_FREQS; ++q)
              sum[q] += static_cast<double>(psd_.power[q]);
            ++psd_n_[slot];
          }
        }
        if (!samples_.empty())
          klls_[slot].addBatch(samples_.data(), samples_.size());
      }

      // 块末发布: 锁外导出成品, 短锁拷贝进 cells
      {
        TraceN("PreviewPublish");
        for (size_t i = 0; i < nb; ++i) {
          const size_t slot = f0 + i;
          KLLcache &kll = klls_[slot];
          Cell c;
          c.n = kll.totalCount();
          if (c.n >= kPvMinSamples) {
            const auto pdf = kll.exportPDF();
            assert(pdf.n <= c.x.size());
            c.n_pts = static_cast<uint32_t>(pdf.n);
            std::copy_n(pdf.x, pdf.n, c.x.data());
            std::copy_n(pdf.y, pdf.n, c.y.data());
          }
          c.psd_n = static_cast<uint32_t>(psd_n_[slot]);
          if (c.psd_n > 0) {
            const double inv = 1.0 / static_cast<double>(psd_n_[slot]);
            for (size_t q = 1; q < PvDayPSD::N_FREQS; ++q) {
              const double p = psd_sum_[slot][q] * inv;
              c.psd[q - 1] = p > 1e-20 ? static_cast<float>(std::log10(p)) : -20.0f;
            }
          }
          std::lock_guard<std::mutex> lock(mutex);
          cells[feat_cols_[slot]] = c;
        }
      }
      epoch.fetch_add(1, std::memory_order_release);
    }

    rounds_done.store(r + 1, std::memory_order_release);
  }

  return !cancel.load(std::memory_order_relaxed);
}

// ============================================================================
// Clear
// ============================================================================

void FeaturePreview::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  feat_cols_.clear();
  valid_types_.clear();
  months_.clear();
  A_ = 0;
  // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存
  cells = std::vector<Cell>{};
  klls_ = std::vector<KLLcache>{};
  psd_sum_ = std::vector<std::array<double, PvDayPSD::N_FREQS>>{};
  psd_n_ = std::vector<uint64_t>{};
  asset_order_ = std::vector<uint32_t>{};
  samples_ = std::vector<float>{};
  plane_.clear();
  rounds_done.store(0, std::memory_order_relaxed);
  rounds_total.store(0, std::memory_order_relaxed);
  epoch.fetch_add(1, std::memory_order_release); // 单调, 不归零
  status.store(Status::Idle, std::memory_order_release);
}
