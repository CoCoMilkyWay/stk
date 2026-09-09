#include "shared/FeaturePreview.hpp"
#include "features/Backend/DayBatchPlane.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "features/Operator/TS/Meta/Meta.hpp" // fmeta::valid
#include "misc/profiler.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <random>

using namespace analysis;

// ============================================================================
// Runtime: worker 私有状态 (UI 不看; 单线程, 无 shard)
// ============================================================================

struct FeaturePreview::Runtime {
  std::vector<KLLcache> klls;                               // [n_preview] 每特征累积 sketch
  std::vector<Integrity> integ;                             // [n_preview] 每特征累积账目
  std::vector<std::array<double, DayPSD::N_FREQS>> psd_sum; // [n_preview] 单日谱累加
  std::vector<uint64_t> psd_n;                              // [n_preview] 参与谱平均的 (资产, 天) 数
  std::vector<uint32_t> asset_order;                        // [A] 固定种子洗牌 (轮间旋转取片)
  std::vector<float> samples;                               // 单 (特征, 轮) 有效样本缓冲
  std::array<float, kVR> day_buf{};                         // 单 (资产, 日) 序列 (NaN = 缺)
  DayPSD psd;                                               // FFT workspace
  DayBatchPlane plane;                                      // [块列+1][A][1][VR]

  // 容量准备 (幂等, 尺寸对得上零分配)
  void prepare(size_t n_pv, size_t A) {
    TraceN("PreviewPrepare");
    prepare_slots(klls, n_pv, kPvKllCapacity, kPvKllResolution);
    integ.assign(n_pv, Integrity{});
    psd_sum.assign(n_pv, {});
    psd_n.assign(n_pv, 0);
    asset_order = shuffled_order(A); // 无偏, 轮间旋转取片覆盖不同资产
    samples.reserve(kPvAssetsPerRound * kVR);
    // plane 按块 prepare (块列数随尾块变)
  }
};

FeaturePreview::FeaturePreview() = default;
FeaturePreview::~FeaturePreview() = default;

// ============================================================================
// Reset
// ============================================================================

void FeaturePreview::reset_for_build(std::vector<size_t> feat_cols,
                                     std::vector<L2::ValidType> valid_types,
                                     size_t meta_col, std::vector<std::string> month_keys,
                                     size_t n_features, size_t n_assets) {
  TraceN("PreviewReset");
  assert(!feat_cols.empty() && feat_cols.size() == valid_types.size());
  assert(std::is_sorted(feat_cols.begin(), feat_cols.end()) && feat_cols.back() < n_features);
  assert(meta_col < n_features && n_assets > 0 && !month_keys.empty());
  std::lock_guard<std::mutex> lock(mutex);

  feat_cols_ = std::move(feat_cols);
  valid_types_ = std::move(valid_types);
  meta_col_ = meta_col;
  months_ = std::move(month_keys);
  A_ = n_assets;

  // 预览是全表底图: 输入变了旧格子无参照意义, 清零 (尺寸对得上就地 fill, 零分配)
  if (cells.size() != n_features)
    cells.assign(n_features, Cell{});
  else
    std::fill(cells.begin(), cells.end(), Cell{});
  if (!rt_)
    rt_ = std::make_unique<Runtime>();

  begin_build();
}

// ============================================================================
// Build (轮训: 轮 = 抽样日, 轮内按特征分块, 块末发布; 首帧 = 一块的 IO + 扫描)
// ============================================================================

bool FeaturePreview::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("PreviewBuild");
  assert(rt_ && A_ > 0 && !feat_cols_.empty() && "reset_for_build 先于 build");
  Runtime &rt = *rt_;
  const size_t VR = kVR;
  assert(level_valid_rows(kLevel) == VR);

  // 日期枚举 → 固定种子洗牌取前 kPvRounds 个 (无偏覆盖全区间, 轮序即收敛序)
  std::vector<std::string> dates = enumerate_dates(reader, months_).dates;
  if (dates.empty())
    return true; // 库为空: 直接 Done, 表格全 "—"
  std::shuffle(dates.begin(), dates.end(), std::mt19937{0x5eed});
  const size_t n_rounds = std::min(kPvRounds, dates.size());
  dates.resize(n_rounds);
  total.store(n_rounds, std::memory_order_release);

  const size_t n_pv = feat_cols_.size();
  const size_t n_draw = std::min(kPvAssetsPerRound, A_);
  rt.prepare(n_pv, A_);
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
        rt.plane.prepare(A_, kLevel, 1, nb + 1, 1, kPvBlockCols + 1);
        rt.plane.load_day(reader, dates[r], cols, false, L2::ValidType::ALL, 0, 0);
      }

      // 块扫描: 每特征抽 n_draw 个资产
      TraceN("PreviewScan");
      for (size_t i = 0; i < nb; ++i) {
        const size_t slot = f0 + i;
        const L2::ValidType vt = valid_types_[slot];
        Integrity &it = rt.integ[slot];
        rt.samples.clear();

        for (size_t k = 0; k < n_draw; ++k) {
          const size_t a = rt.asset_order[(a_off + k) % A_];
          const feature_storage_t *v = rt.plane.series(i, a, 0);
          const feature_storage_t *mv = rt.plane.series(nb, a, 0);
          it.n_total += VR;
          for (size_t t = 0; t < VR; ++t) {
            const float x = static_cast<float>(v[t]);
            // 账目与 Dist 同口径: 只记门控过的格子 (NaN = 哨兵, 真 NaN 已折叠; ±inf 单列);
            // 只有有限值进 sketch / PSD
            bool ok = false;
            if (fmeta::valid(static_cast<float>(mv[t]), vt)) {
              if (x != x) {
                ++it.n_nan;
              } else if (std::isinf(x)) {
                ++(x > 0.0f ? it.n_pos_inf : it.n_neg_inf);
              } else {
                ok = true;
                it.add_finite(x);
              }
            }
            rt.day_buf[t] = ok ? x : qnan;
            if (ok)
              rt.samples.push_back(x);
          }
          if (rt.psd.compute(rt.day_buf.data())) {
            auto &sum = rt.psd_sum[slot];
            for (size_t q = 0; q < DayPSD::N_FREQS; ++q)
              sum[q] += static_cast<double>(rt.psd.power[q]);
            ++rt.psd_n[slot];
          }
        }
        if (!rt.samples.empty())
          rt.klls[slot].addBatch(rt.samples.data(), rt.samples.size());
      }

      // 块末发布: 锁外导出成品, 短锁拷贝进 cells
      {
        TraceN("PreviewPublish");
        for (size_t i = 0; i < nb; ++i) {
          const size_t slot = f0 + i;
          Cell c;
          c.fill(rt.klls[slot], kPvMinSamples);
          c.integrity = rt.integ[slot];
          c.psd_n = static_cast<uint32_t>(rt.psd_n[slot]);
          if (c.psd_n > 0) {
            const double inv = 1.0 / static_cast<double>(rt.psd_n[slot]);
            for (size_t q = 1; q < DayPSD::N_FREQS; ++q) {
              const double p = rt.psd_sum[slot][q] * inv;
              c.psd[q - 1] = p > 1e-20 ? static_cast<float>(std::log10(p)) : -20.0f;
            }
          }
          std::lock_guard<std::mutex> lock(mutex);
          cells[feat_cols_[slot]] = c;
        }
      }
      epoch.fetch_add(1, std::memory_order_release);
    }

    publish_progress(1);
  }

  return !cancel.load(std::memory_order_relaxed);
}

// ============================================================================
// Clear
// ============================================================================

void FeaturePreview::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存
  cells = std::vector<Cell>{};
  rt_.reset(); // worker 已 join, 整体释放
  feat_cols_.clear();
  valid_types_.clear();
  months_.clear();
  A_ = 0;
  reset_idle();
}
