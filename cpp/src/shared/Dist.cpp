#include "shared/Dist.hpp"
#include "features/TimeIndex.hpp"
#include "misc/profiler.hpp"

#include <cstring>
#include <tuple>

// ============================================================================
// Helper: Date parsing
// ============================================================================

namespace {

// Parse "YYYYMMDD" -> (year, month, day)
std::tuple<uint16_t, uint8_t, uint8_t> parse_date(const std::string &date) {
  assert(date.size() == 8);
  uint16_t year = std::stoi(date.substr(0, 4));
  uint8_t month = std::stoi(date.substr(4, 2));
  uint8_t day = std::stoi(date.substr(6, 2));
  return {year, month, day};
}

// Zeller's congruence: weekday (Mon=0, Sun=6)
uint8_t calc_weekday(uint16_t y, uint8_t m, uint8_t d) {
  if (m < 3) {
    m += 12;
    y -= 1;
  }
  int q = d, M = m, K = y % 100, J = y / 100;
  int h = (q + (13 * (M + 1)) / 5 + K + K / 4 + J / 4 - 2 * J) % 7;
  return static_cast<uint8_t>((h + 5) % 7);
}

} // namespace

// ============================================================================
// Reset
// ============================================================================

void Dist::reset_for_build(Params p, const std::vector<std::string> &month_keys, size_t n_assets) {
  std::lock_guard<std::mutex> lock(mutex);

  params = std::move(p);
  assert(params.level == 1 && "Dist 只在 L1 上跑");

  // 月聚合: 数量随区间变, sketch 容量复用不到就重建
  months.clear();
  months.resize(month_keys.size());
  for (size_t i = 0; i < month_keys.size(); ++i) {
    months[i].month = month_keys[i];
  }

  // 资产槽: 尺寸不变时只 clear (KLL 保留容量, 稳态零分配)
  if (assets.size() != n_assets) {
    assets.clear();
    assets.resize(n_assets);
  } else {
    for (auto &slot : assets)
      slot.clear();
  }

  if (by_hour.size() != 24) {
    by_hour.clear();
    by_hour.resize(24);
  } else {
    for (auto &kll : by_hour)
      kll.clear();
  }
  if (by_weekday.size() != 7) {
    by_weekday.clear();
    by_weekday.resize(7);
  } else {
    for (auto &kll : by_weekday)
      kll.clear();
  }

  total.clear();
  integrity.clear();
  stability.clear();

  days_loaded.store(0, std::memory_order_relaxed);
  days_total.store(0, std::memory_order_relaxed);
  assets_done.store(0, std::memory_order_relaxed);
  status.store(Status::Building, std::memory_order_release);
}

// ============================================================================
// Build (Phase IO: 抽样入块 → Phase 流: 资产优先发布)
// ============================================================================

bool Dist::build(FeatureRead &reader, FeatureRead::MonthTensor &block,
                 const std::atomic<bool> &cancel) {
  TraceN("DistBuild");

  const size_t A = assets.size();
  const size_t n_cols = params.columns.size();
  const bool has_valid = (n_cols > 1);
  const size_t rows = LEVELS[params.level].rows;
  const size_t valid_rows = level_valid_rows(static_cast<size_t>(params.level));
  const size_t n_months = months.size();

  // ==========================================================================
  // 日期枚举 + 自适应日抽样
  // ==========================================================================
  std::vector<std::string> all_dates;
  std::vector<size_t> all_month; // 日 → 月下标
  for (size_t m = 0; m < n_months; ++m) {
    const std::string &key = months[m].month; // reset 后不变, 免锁读
    auto ds = reader.list_dates(key.substr(0, 4), key.substr(4, 2));
    for (auto &d : ds) {
      all_dates.push_back(std::move(d));
      all_month.push_back(m);
    }
  }
  if (all_dates.empty())
    return true;

  const size_t bytes_per_day = rows * n_cols * A * sizeof(feature_storage_t);
  size_t stride = (all_dates.size() * bytes_per_day + kMaxBlockBytes - 1) / kMaxBlockBytes;
  if (stride == 0)
    stride = 1;
  if (stride % 5 == 0)
    ++stride; // 与交易周互质, 避免星期偏置

  std::vector<size_t> sel;
  for (size_t i = 0; i < all_dates.size(); i += stride)
    sel.push_back(i);
  const size_t n_sel = sel.size();

  days_total.store(n_sel, std::memory_order_release);

  // ==========================================================================
  // Phase IO: 逐日载入常驻块 (worker 私有, 无锁; 逐列文件只碰特征列 + valid 列)
  // ==========================================================================
  block.preallocate(A, n_sel, n_cols, static_cast<size_t>(params.level));
  block.reset();
  block.feature_indices = params.columns;

  for (size_t i = 0; i < n_sel; ++i) {
    if (cancel.load(std::memory_order_relaxed))
      return false;
    TraceN("LoadDay");
    block.dates.push_back(all_dates[sel[i]]);
    reader.load_date_columns_into(block.dates.back(), params.columns, block, i);
    days_loaded.store(i + 1, std::memory_order_release);
  }

  // 预计算: 每日星期/月下标, 分钟 → 小时 (L1)
  std::vector<uint8_t> weekdays(n_sel);
  std::vector<size_t> day_month(n_sel);
  for (size_t i = 0; i < n_sel; ++i) {
    auto [y, m, dd] = parse_date(block.dates[i]);
    weekdays[i] = calc_weekday(y, m, dd);
    day_month[i] = all_month[sel[i]];
  }
  std::array<uint8_t, 255> hour_lut;
  for (size_t lt = 0; lt < valid_rows; ++lt) {
    hour_lut[lt] = L1_to_Clock(lt).hour;
  }

  // ==========================================================================
  // Phase 流: 资产优先, 逐资产 收集全时段(无锁) → 发布(短锁) → 水位+1
  // ==========================================================================
  scratch_month_.resize(n_months);
  std::vector<Integrity> inte_month(n_months);

  for (size_t a = 0; a < A; ++a) {
    if (cancel.load(std::memory_order_relaxed))
      return false;

    scratch_all_.clear();
    for (auto &v : scratch_hour_)
      v.clear();
    for (auto &v : scratch_weekday_)
      v.clear();
    for (auto &v : scratch_month_)
      v.clear();
    for (auto &inte : inte_month)
      inte.clear();

    for (size_t i = 0; i < n_sel; ++i) {
      const size_t t0 = block.day_start(i);
      const uint8_t wd = weekdays[i];
      const size_t m = day_month[i];
      Integrity &inte = inte_month[m];

      for (size_t lt = 0; lt < valid_rows; ++lt) {
        // 布局 [T][n_cols][A]: 值列 i=0, valid 列 i=1
        const size_t base = ((t0 + lt) * n_cols) * A + a;
        inte.n_total++;

        if (has_valid && static_cast<float>(block.data[base + A]) <= 0.5f)
          continue;

        const float val = static_cast<float>(block.data[base]);
        if (val != val) {
          inte.n_nan++;
          continue;
        }
        if (val > 1e38f) {
          inte.n_pos_inf++;
          continue;
        }
        if (val < -1e38f) {
          inte.n_neg_inf++;
          continue;
        }
        if (val == 0.0f)
          inte.n_zero++;

        inte.n_valid++;
        inte.update_minmax(val);
        scratch_all_.push_back(val);
        scratch_hour_[hour_lut[lt]].push_back(val);
        scratch_weekday_[wd].push_back(val);
        scratch_month_[m].push_back(val);
      }
    }

    // 发布 (短锁: KLL 批量摄入 + 计数); 该资产全时段贡献一次到位, 槽即终态
    {
      std::lock_guard<std::mutex> lock(mutex);
      auto &slot = assets[a];
      if (!scratch_all_.empty()) {
        slot.kll.addBatch(scratch_all_);
        total.addBatch(scratch_all_);
        for (size_t h = 0; h < 24; ++h) {
          if (!scratch_hour_[h].empty())
            by_hour[h].addBatch(scratch_hour_[h]);
        }
        for (size_t w = 0; w < 7; ++w) {
          if (!scratch_weekday_[w].empty())
            by_weekday[w].addBatch(scratch_weekday_[w]);
        }
        for (size_t m = 0; m < n_months; ++m) {
          if (!scratch_month_[m].empty())
            months[m].total.addBatch(scratch_month_[m]);
        }
      }
      for (size_t m = 0; m < n_months; ++m) {
        if (inte_month[m].n_total > 0) {
          slot.integrity.add(inte_month[m]);
          months[m].integrity.add(inte_month[m]);
          integrity.add(inte_month[m]);
        }
      }
    }
    assets_done.store(a + 1, std::memory_order_release);
  }

  return true;
}

// ============================================================================
// Stability (全部资产完成后一次: W2 偏移 + 主成分投影排序)
// ============================================================================

bool Dist::build_stability(const std::atomic<bool> &cancel) {
  TraceN("DistStability");

  const size_t n_assets = assets.size();
  constexpr int N_PERCENTILES = 19;
  std::array<double, N_PERCENTILES> percentiles;
  for (int i = 0; i < N_PERCENTILES; ++i) {
    percentiles[i] = 0.05 * (i + 1); // 5%, 10%, ..., 95%
  }

  // ==========================================================================
  // 抽取 (分块持锁: quantile 会写 KLL 懒缓存, 与 UI 读互斥; 分块避免冻 UI)
  // ==========================================================================
  std::array<float, N_PERCENTILES> q_global;
  float global_mean = 0.0f, global_median = 0.0f;
  uint64_t global_count = 0;
  {
    std::lock_guard<std::mutex> lock(mutex);
    global_count = total.count();
    global_mean = static_cast<float>(total.mean());
    global_median = static_cast<float>(total.quantile(0.5));
    for (int d = 0; d < N_PERCENTILES; ++d)
      q_global[d] = static_cast<float>(total.quantile(percentiles[d]));
  }

  std::vector<size_t> valid_idx;
  std::vector<std::array<float, N_PERCENTILES>> q_asset; // [n_valid]
  std::vector<float> mean_asset, median_asset;

  constexpr size_t CHUNK = 256;
  for (size_t a0 = 0; a0 < n_assets; a0 += CHUNK) {
    if (cancel.load(std::memory_order_relaxed))
      return false;
    std::lock_guard<std::mutex> lock(mutex);
    const size_t a1 = std::min(a0 + CHUNK, n_assets);
    for (size_t a = a0; a < a1; ++a) {
      const auto &kll = assets[a].kll;
      if (kll.count() < kMinAssetSamples)
        continue;
      valid_idx.push_back(a);
      mean_asset.push_back(static_cast<float>(kll.mean()));
      median_asset.push_back(static_cast<float>(kll.quantile(0.5)));
      auto &qs = q_asset.emplace_back();
      for (int d = 0; d < N_PERCENTILES; ++d)
        qs[d] = static_cast<float>(kll.quantile(percentiles[d]));
    }
  }

  const size_t n_valid = valid_idx.size();
  if (global_count <= kMinSamples || n_valid <= 1)
    return true;

  // ==========================================================================
  // 计算 (纯本地数据, 无锁)
  // ==========================================================================

  // X 轴: W2 距离 (ICDF + 均值对齐)
  std::vector<float> scores_w2(n_valid);
  for (size_t i = 0; i < n_valid; ++i) {
    const float mean_shift = mean_asset[i] - global_mean;
    float sum_sq = 0.0f;
    for (int d = 0; d < N_PERCENTILES; ++d) {
      const float diff = (q_asset[i][d] - mean_shift) - q_global[d];
      sum_sq += diff * diff;
    }
    scores_w2[i] = std::sqrt(sum_sq / N_PERCENTILES);
  }

  float M = *std::max_element(scores_w2.begin(), scores_w2.end());
  if (M < 1e-9f)
    M = 1.0f;

  // 偏移色: W1 偏移向量 (ICDF + 中位数对齐) → 主成分投影排序
  // (Ward 聚类是 O(n³), 5000+ 资产不可行; 主模投影 O(n·D²) 毫秒级, 同样"形状相近颜色相近")
  std::vector<std::array<float, N_PERCENTILES>> offsets(n_valid);
  std::array<double, N_PERCENTILES> mean_off{};
  for (size_t i = 0; i < n_valid; ++i) {
    const float median_shift = median_asset[i] - global_median;
    for (int d = 0; d < N_PERCENTILES; ++d) {
      offsets[i][d] = (q_asset[i][d] - median_shift) - q_global[d];
      mean_off[d] += offsets[i][d];
    }
  }
  for (int d = 0; d < N_PERCENTILES; ++d)
    mean_off[d] /= static_cast<double>(n_valid);

  // 协方差 (D×D) → 幂迭代主特征向量
  std::array<std::array<double, N_PERCENTILES>, N_PERCENTILES> cov{};
  for (size_t i = 0; i < n_valid; ++i) {
    for (int d1 = 0; d1 < N_PERCENTILES; ++d1) {
      const double x1 = offsets[i][d1] - mean_off[d1];
      for (int d2 = d1; d2 < N_PERCENTILES; ++d2) {
        cov[d1][d2] += x1 * (offsets[i][d2] - mean_off[d2]);
      }
    }
  }
  for (int d1 = 0; d1 < N_PERCENTILES; ++d1)
    for (int d2 = 0; d2 < d1; ++d2)
      cov[d1][d2] = cov[d2][d1];

  std::array<double, N_PERCENTILES> v;
  v.fill(1.0);
  for (int iter = 0; iter < 50; ++iter) {
    std::array<double, N_PERCENTILES> w{};
    for (int d1 = 0; d1 < N_PERCENTILES; ++d1)
      for (int d2 = 0; d2 < N_PERCENTILES; ++d2)
        w[d1] += cov[d1][d2] * v[d2];
    double norm = 0.0;
    for (int d = 0; d < N_PERCENTILES; ++d)
      norm += w[d] * w[d];
    norm = std::sqrt(norm);
    if (norm < 1e-30)
      break;
    for (int d = 0; d < N_PERCENTILES; ++d)
      v[d] = w[d] / norm;
  }

  // 投影 → 排序 → 色标
  std::vector<float> proj(n_valid);
  for (size_t i = 0; i < n_valid; ++i) {
    double p = 0.0;
    for (int d = 0; d < N_PERCENTILES; ++d)
      p += v[d] * (offsets[i][d] - mean_off[d]);
    proj[i] = static_cast<float>(p);
  }
  std::vector<size_t> order(n_valid);
  for (size_t i = 0; i < n_valid; ++i)
    order[i] = i;
  std::sort(order.begin(), order.end(),
            [&](size_t a, size_t b) { return proj[a] < proj[b]; });

  // ==========================================================================
  // 发布
  // ==========================================================================
  StabilityViz viz;
  viz.asset_idx = std::move(valid_idx);
  viz.score_min = 0.0f; // W2 距离非负
  viz.score_max = M;
  viz.x_norm.resize(n_valid);
  for (size_t i = 0; i < n_valid; ++i)
    viz.x_norm[i] = scores_w2[i] / M;
  viz.color_t.resize(n_valid);
  for (size_t pos = 0; pos < n_valid; ++pos) {
    viz.color_t[order[pos]] = static_cast<float>(pos) / (n_valid - 1);
  }
  viz.valid = true;

  {
    std::lock_guard<std::mutex> lock(mutex);
    stability = std::move(viz);
  }
  return true;
}

// ============================================================================
// Clear
// ============================================================================

void Dist::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  params = Params{};
  months.clear();
  assets.clear();
  by_hour.clear();
  by_weekday.clear();
  total.clear();
  integrity.clear();
  stability.clear();
  days_loaded.store(0, std::memory_order_relaxed);
  days_total.store(0, std::memory_order_relaxed);
  assets_done.store(0, std::memory_order_relaxed);
  status.store(Status::Idle, std::memory_order_release);
}
