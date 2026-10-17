#include "shared/Analysis.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "misc/date.hpp"
#include "misc/profiler.hpp"
#include "shared/Config.hpp"
#include "shared/Feature.hpp"

#include <numeric>
#include <random>
#include <thread>

namespace analysis {

ReadScope read_scope(const Config &cfg, std::string_view lib) {
  ReadScope s;
  s.lib = load_lib(cfg, lib);
  s.bar = load_lib(cfg, Config::kAllTierDir);
  if (s.lib.ready() && s.bar.ready()) // 未建库 = 空范围 (请求方见 months 为空即放弃)
    s.months = misc::months_between(cfg.start_date, cfg.end_date);
  return s;
}

fmeta::ColumnSet column_set(const Feature &feature, size_t level, std::vector<size_t> cols) {
  return column_set(feature, feature.selection.tier, level, std::move(cols));
}

fmeta::ColumnSet column_set(const Feature &feature, TierId tier, size_t level, std::vector<size_t> cols) {
  const auto &meta = feature.meta(tier);
  const auto &list = meta.features[level];
  assert(!cols.empty());
  fmeta::ColumnSet cs;
  cs.vts.reserve(cols.size());
  bool gated = false;
  for (const size_t c : cols) {
    assert(c < list.size());
    cs.vts.push_back(list[c].valid_type);
    gated |= list[c].valid_type != L2::ValidType::ALL;
  }
  cs.io = std::move(cols);
  if (gated)
    cs.io.push_back(meta.col_of(level, "ts_valid"));
  return cs;
}

DateList enumerate_dates(const FeatureRead &reader, const std::vector<misc::Ym> &months) {
  TraceN("EnumDates"); // 首帧账目: 每月一次目录迭代 + 每天一次 stat
  DateList out;
  for (size_t m = 0; m < months.size(); ++m) {
    for (const misc::Ymd d : reader.list_dates(months[m])) {
      out.dates.push_back(d);
      out.month.push_back(static_cast<uint16_t>(m));
    }
  }
  return out;
}

DayPlan plan_days(const FeatureRead &reader, const std::vector<misc::Ym> &months, DayOrder order, size_t limit) {
  const DateList all = enumerate_dates(reader, months);
  std::vector<uint32_t> idx(all.dates.size());
  std::iota(idx.begin(), idx.end(), 0u);
  if (order == DayOrder::Shuffle)
    std::shuffle(idx.begin(), idx.end(), std::mt19937{0x5eed}); // 与 shuffled_order 同种子: 跨构建稳定
  if (limit > 0 && idx.size() > limit)
    idx.resize(limit);
  DayPlan p;
  p.dates.reserve(idx.size());
  p.month.reserve(idx.size());
  p.weekday.reserve(idx.size());
  for (const uint32_t i : idx) {
    p.dates.push_back(all.dates[i]);
    p.month.push_back(all.month[i]);
    p.weekday.push_back(static_cast<uint8_t>(misc::weekday_of(all.dates[i])));
  }
  return p;
}

void prepare_slots(std::vector<KLLcache> &slots, size_t n, size_t capacity, size_t resolution) {
  if (slots.size() == n) {
    clear_slots(slots);
    return;
  }
  slots.clear();
  slots.reserve(n);
  for (size_t i = 0; i < n; ++i)
    slots.emplace_back(capacity, resolution);
}

void clear_slots(std::vector<KLLcache> &slots) {
  for (auto &kll : slots)
    kll.clear();
}

std::vector<uint32_t> shuffled_order(size_t n) {
  std::vector<uint32_t> order(n);
  std::iota(order.begin(), order.end(), 0u);
  std::shuffle(order.begin(), order.end(), std::mt19937{0x5eed});
  return order;
}

void check_axis(const std::vector<uint32_t> &global_ids) {
  assert(!global_ids.empty() && "universe 为空");
  assert(std::is_sorted(global_ids.begin(), global_ids.end()) &&
         std::adjacent_find(global_ids.begin(), global_ids.end()) == global_ids.end() &&
         "global_ids 必须升序去重 (UniverseAxis::ids)");
  (void)global_ids;
}

size_t threads_for(size_t max_tasks) {
  const size_t n_hw = std::max<size_t>(1, std::thread::hardware_concurrency());
  return std::max<size_t>(1, std::min(n_hw, max_tasks));
}

} // namespace analysis
