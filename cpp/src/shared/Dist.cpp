#include "shared/Dist.hpp"
#include "features/TimeIndex.hpp"
#include "misc/profiler.hpp"

#include <bit>
#include <cmath>
#include <cstdio>
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

// "YYYY-MM-DD" / "YYYYMMDD" -> "YYYYMM"
std::string parse_month(const std::string &date) {
  std::string d;
  for (char c : date)
    if (c != '-')
      d += c;
  assert(d.size() >= 6);
  return d.substr(0, 6);
}

} // namespace

std::vector<std::string> dist_enumerate_months(const std::string &start_date,
                                               const std::string &end_date) {
  std::vector<std::string> months;
  const std::string start_month = parse_month(start_date);
  const std::string end_month = parse_month(end_date);

  int y = std::stoi(start_month.substr(0, 4));
  int m = std::stoi(start_month.substr(4, 2));
  while (true) {
    char buf[8];
    snprintf(buf, sizeof(buf), "%04d%02d", y, m);
    std::string month_key = buf;
    if (month_key > end_month)
      break;
    months.push_back(std::move(month_key));
    if (++m > 12) {
      m = 1;
      ++y;
    }
  }
  return months;
}

// ============================================================================
// Reset
// ============================================================================

void Dist::reset_for_build(std::vector<size_t> cols, const std::vector<std::string> &month_keys,
                           size_t n_assets) {
  std::lock_guard<std::mutex> lock(mutex);

  columns = std::move(cols);
  assert(!columns.empty() && "至少要有值列");

  // 月聚合: 数量随区间变, sketch 容量复用不到就重建
  months.clear();
  months.resize(month_keys.size());
  for (size_t i = 0; i < month_keys.size(); ++i) {
    months[i].month = month_keys[i];
  }

  // 资产槽: 尺寸不变时只 clear (KLL 保留容量, 稳态零分配)
  if (assets.size() != n_assets) {
    assets.clear();
    assets.reserve(n_assets);
    for (size_t a = 0; a < n_assets; ++a)
      assets.emplace_back(KLL_ASSET_CAPACITY, KLL_ASSET_RESOLUTION);
  } else {
    for (auto &kll : assets)
      kll.clear();
  }

  // 随机资产序: 已发布集合恒为全市场无偏抽样 (固定种子, 可复现)
  order.resize(n_assets);
  std::iota(order.begin(), order.end(), size_t{0});
  std::shuffle(order.begin(), order.end(), std::mt19937{0x5eed});

  if (by_hour.empty())
    for (size_t h = 0; h < 24; ++h)
      by_hour.emplace_back(KLL_CAPACITY, KLL_RESOLUTION);
  else
    for (auto &kll : by_hour)
      kll.clear();
  if (by_weekday.empty())
    for (size_t w = 0; w < 7; ++w)
      by_weekday.emplace_back(KLL_CAPACITY, KLL_RESOLUTION);
  else
    for (auto &kll : by_weekday)
      kll.clear();

  total.clear();
  integrity.clear();

  days_loaded.store(0, std::memory_order_relaxed);
  days_total.store(0, std::memory_order_relaxed);
  assets_done.store(0, std::memory_order_relaxed);
  status.store(Status::Building, std::memory_order_release);
}

// ============================================================================
// Build (Phase IO: 抽样转置入平面 → Phase 流: 资产优先发布)
// ============================================================================

bool Dist::build(FeatureRead &reader, const std::atomic<bool> &cancel) {
  TraceN("DistBuild");

  const size_t A = assets.size();
  const size_t n_cols = columns.size();
  const bool has_valid = (n_cols > 1);
  const size_t VR = level_valid_rows(kDistLevel); // 分钟/日
  const size_t n_months = months.size();

  // ==========================================================================
  // 日期枚举 + 自适应日抽样: 抽样天表 = 日期 → (星期, 月下标)
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

  const size_t bytes_per_day = VR * A * sizeof(float);
  size_t stride = (all_dates.size() * bytes_per_day + kMaxBlockBytes - 1) / kMaxBlockBytes;
  if (stride % 5 == 0)
    ++stride; // 与交易周互质, 避免星期偏置

  std::vector<std::string> dates;
  std::vector<uint8_t> weekdays;
  std::vector<uint16_t> day_month;
  for (size_t i = 0; i < all_dates.size(); i += stride) {
    auto [y, m, dd] = parse_date(all_dates[i]);
    dates.push_back(std::move(all_dates[i]));
    weekdays.push_back(calc_weekday(y, m, dd));
    day_month.push_back(static_cast<uint16_t>(all_month[i]));
  }
  const size_t n_sel = dates.size();
  days_total.store(n_sel, std::memory_order_release);

  // ==========================================================================
  // Phase IO: 逐日载入 [T][列][A] 暂存 → 分块转置进资产主序平面 (worker 私有, 无锁)
  // 内存与旧布局等价 (2×f16 列 = 1×f32 平面), 换来 Phase 流的纯顺序扫描
  // ==========================================================================
  const float invalid = std::bit_cast<float>(kInvalidBits);
  const size_t asset_stride = n_sel * VR;
  plane_.resize(A * asset_stride);

  FeatureRead::DayColumns staging;
  staging.preallocate(A, kDistLevel, n_cols);

  for (size_t i = 0; i < n_sel; ++i) {
    if (cancel.load(std::memory_order_relaxed))
      return false;
    {
      TraceN("LoadDay");
      reader.load_day_columns(dates[i], columns, staging);
    }
    {
      TraceN("TransposeDay");
      // [T][列][A] → plane[a][i][t]; 64 资产一块, 块内写驻留在 64 条 cache line 上
      float *day_base = plane_.data() + i * VR;
      for (size_t a0 = 0; a0 < A; a0 += 64) {
        const size_t a1 = std::min(a0 + 64, A);
        for (size_t t = 0; t < VR; ++t) {
          const feature_storage_t *val = staging.data.data() + (t * n_cols) * A; // valid 列紧随其后
          for (size_t a = a0; a < a1; ++a) {
            const bool ok = !has_valid || static_cast<float>(val[A + a]) > 0.5f;
            day_base[a * asset_stride + t] = ok ? static_cast<float>(val[a]) : invalid;
          }
        }
      }
    }
    days_loaded.store(i + 1, std::memory_order_release);
  }

  // 预计算: 分钟 → 小时 (L1)
  std::vector<uint8_t> hour_lut(VR);
  for (size_t t = 0; t < VR; ++t)
    hour_lut[t] = L1_to_Clock(t).hour;

  // ==========================================================================
  // Phase 流: 随机序资产优先, 逐资产 顺序扫描(无锁) → 切片发布(短锁) → 进度+1
  // ==========================================================================
  for (size_t k = 0; k < A; ++k) {
    const size_t a = order[k];
    if (cancel.load(std::memory_order_relaxed))
      return false;

    samples_.clear();
    day_groups_.clear();
    hour_runs_.clear();
    Integrity inte;

    const float *pa = plane_.data() + a * asset_stride;
    int cur_hour = -1; // 小时 run 跨天延续 (同小时连续样本即一段)
    uint32_t run_begin = 0;

    for (size_t i = 0; i < n_sel; ++i) {
      const float *p = pa + i * VR;
      const uint32_t day_begin = static_cast<uint32_t>(samples_.size());

      for (size_t t = 0; t < VR; ++t) {
        const float v = p[t];
        ++inte.n_total;
        if (v != v) { // 真 NaN 或 invalid 哨兵
          if (std::bit_cast<uint32_t>(v) != kInvalidBits)
            ++inte.n_nan;
          continue;
        }
        if (std::isinf(v)) {
          if (v > 0.0f)
            ++inte.n_pos_inf;
          else
            ++inte.n_neg_inf;
          continue;
        }
        if (v == 0.0f)
          ++inte.n_zero;
        ++inte.n_valid;
        inte.val_min = std::min(inte.val_min, v);
        inte.val_max = std::max(inte.val_max, v);

        const int h = hour_lut[t];
        if (h != cur_hour) {
          if (cur_hour >= 0)
            hour_runs_.push_back({run_begin, static_cast<uint32_t>(samples_.size()),
                                  static_cast<uint8_t>(cur_hour)});
          cur_hour = h;
          run_begin = static_cast<uint32_t>(samples_.size());
        }
        samples_.push_back(v);
      }

      if (samples_.size() > day_begin)
        day_groups_.push_back({day_begin, static_cast<uint32_t>(samples_.size()),
                               day_month[i], weekdays[i]});
    }
    if (cur_hour >= 0)
      hour_runs_.push_back({run_begin, static_cast<uint32_t>(samples_.size()),
                            static_cast<uint8_t>(cur_hour)});

    // 发布 (短锁: 切片直喂 KLL, 零拷贝); 该资产全时段贡献一次到位, 槽即终态
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (!samples_.empty()) {
        const float *s = samples_.data();
        assets[a].addBatch(s, samples_.size());
        total.addBatch(s, samples_.size());
        for (const DayGroup &g : day_groups_) {
          months[g.month].kll.addBatch(s + g.begin, g.end - g.begin);
          by_weekday[g.weekday].addBatch(s + g.begin, g.end - g.begin);
        }
        for (const HourRun &r : hour_runs_)
          by_hour[r.hour].addBatch(s + r.begin, r.end - r.begin);
      }
      integrity.add(inte);
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
  columns.clear();
  order.clear();
  months.clear();
  assets.clear();
  by_hour.clear();
  by_weekday.clear();
  total.clear();
  integrity.clear();
  // worker 私有缓冲: clear() 只在 worker join 后调用, 直接释放 (平面 GB 级)
  plane_ = {};
  samples_ = {};
  day_groups_ = {};
  hour_runs_ = {};
  days_loaded.store(0, std::memory_order_relaxed);
  days_total.store(0, std::memory_order_relaxed);
  assets_done.store(0, std::memory_order_relaxed);
  status.store(Status::Idle, std::memory_order_release);
}
