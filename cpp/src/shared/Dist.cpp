#include "shared/Dist.hpp"
#include "features/TimeIndex.hpp"
#include "misc/profiler.hpp"

#include <cstring>
#include <numeric>
#include <random>
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

  // 随机资产序: 已发布集合恒为全市场无偏抽样 (固定种子, 可复现)
  order.resize(n_assets);
  std::iota(order.begin(), order.end(), size_t{0});
  std::shuffle(order.begin(), order.end(), std::mt19937{0x5eed});

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
  // Phase 流: 随机序资产优先, 逐资产 收集全时段(无锁) → 发布(短锁) → 进度+1
  // ==========================================================================
  scratch_month_.resize(n_months);
  std::vector<Integrity> inte_month(n_months);

  for (size_t k = 0; k < A; ++k) {
    const size_t a = order[k];
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
    assets_done.store(k + 1, std::memory_order_release);
  }

  return true;
}

// ============================================================================
// Clear
// ============================================================================

void Dist::clear() {
  std::lock_guard<std::mutex> lock(mutex);
  params = Params{};
  order.clear();
  months.clear();
  assets.clear();
  by_hour.clear();
  by_weekday.clear();
  total.clear();
  integrity.clear();
  days_loaded.store(0, std::memory_order_relaxed);
  days_total.store(0, std::memory_order_relaxed);
  assets_done.store(0, std::memory_order_relaxed);
  status.store(Status::Idle, std::memory_order_release);
}
