#include "shared/Analysis.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "misc/date.hpp"
#include "misc/profiler.hpp"
#include "shared/Config.hpp"
#include "shared/Feature.hpp"

#include <map>
#include <numeric>
#include <random>

namespace analysis {

ReadScope read_scope(const Config &cfg, const Feature &feature, TierId main) {
  ReadScope s;
  s.main = main;
  bool ready = true;
  for (const TierId t : TIER_IDS)
    if (s.has(t)) {
      s.libs[t] = load_lib(cfg, feature.selection.lib_of(t));
      assert(s.libs[t].tier == t);
      ready &= s.libs[t].ready();
    }
  if (!ready) // 未建库 = 空范围 (请求方见 months 为空即放弃)
    return s;
  s.months = misc::months_between(cfg.start_date, cfg.end_date);
  const LibAxis &axis = s.lib().axis;
  for (const TierId t : TIER_IDS)
    if (t != main && s.has(t)) {
      const LibAxis &side = s.libs[t].axis;
      s.to[t].resize(axis.size());
      for (size_t a = 0; a < axis.size(); ++a) {
        s.to[t][a] = static_cast<uint32_t>(side.sub_of(axis.global(a)));
        assert(s.to[t][a] < side.size() && "主档资产不在旁读档 A 轴内");
      }
    }
  return s;
}

PerTier<size_t> ReadScope::sizes() const {
  PerTier<size_t> out;
  for (const TierId t : TIER_IDS)
    out[t] = libs[t].axis.size();
  return out;
}

Readers ReadScope::readers(const PerTier<const FeatureRead *> &fr) const {
  Readers rd{.main = main};
  for (const TierId t : TIER_IDS)
    if (has(t)) {
      assert(fr[t] && "库集档缺 reader");
      rd.rd[t] = fr[t];
      rd.ids[t] = t == main ? nullptr : to[t].data();
    }
  return rd;
}

bool read_request(const Config &cfg, const Feature &feature, size_t level, std::vector<ColRef> cols, ReadScope &scope, ReadPlan &plan) {
  const TierId main = Feature::main_tier(cols);
  scope = read_scope(cfg, feature, main);
  if (scope.months.empty())
    return false;
  plan = read_plan(feature, main, level, std::move(cols));
  return true;
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

ReadPlan read_plan(const Feature &feature, TierId main, size_t level, std::vector<ColRef> cols) {
  assert(!cols.empty() && level < LEVEL_COUNT);
  ReadPlan p;
  p.main = main;
  p.level = level;
  p.cols = std::move(cols);
  std::map<std::pair<TierId, size_t>, std::vector<uint32_t>> groups; // (档, 层) → 平面列 (请求序)
  for (size_t c = 0; c < p.cols.size(); ++c) {
    const ColRef &r = p.cols[c];
    assert((r.tier == main || tier_side(main, r.tier)) && "别档列只许旁读档");
    groups[{r.tier, r.level}].push_back(static_cast<uint32_t>(c));
  }
  const size_t rpd = level_valid_rows(level);
  for (auto &[key, slot] : groups) {
    Source s;
    s.tier = key.first;
    s.level = key.second;
    std::vector<size_t> io;
    io.reserve(slot.size());
    for (const uint32_t c : slot)
      io.push_back(p.cols[c].col);
    s.cs = column_set(feature, s.tier, s.level, std::move(io));
    s.slot = std::move(slot);
    s.row.resize(rpd);
    for (size_t t = 0; t < rpd; ++t) {
      s.row[t] = static_cast<int32_t>(level_asof(s.level, level, t));
      s.carry |= s.row[t] < 0;
    }
    p.srcs.push_back(std::move(s));
  }
  return p;
}

SegPlan plan_segs(const FeatureRead &reader, const std::vector<misc::Ym> &months, size_t level, DayOrder order,
                  size_t limit) {
  const DateList all = enumerate_dates(reader, months);
  const size_t days = seg_days(level), n = all.dates.size();
  std::vector<uint32_t> idx((n + days - 1) / days);
  std::iota(idx.begin(), idx.end(), 0u);
  if (order == DayOrder::Shuffle)
    std::shuffle(idx.begin(), idx.end(), std::mt19937{0x5eed}); // 与 shuffled_order 同种子: 跨构建稳定
  if (limit > 0 && idx.size() > limit)
    idx.resize(limit);
  SegPlan p;
  p.days = days;
  p.dates.reserve(idx.size() * days);
  p.prev.reserve(idx.size() * days);
  p.month.reserve(idx.size() * days);
  p.weekday.reserve(idx.size() * days);
  for (const uint32_t seg : idx) {
    for (size_t d = 0; d < days; ++d) {
      const size_t i = seg * days + d;
      const bool in = i < n;
      p.dates.push_back(in ? all.dates[i] : 0);
      p.prev.push_back(in && i > 0 ? all.dates[i - 1] : 0);
      p.month.push_back(in ? all.month[i] : 0);
      p.weekday.push_back(in ? static_cast<uint8_t>(misc::weekday_of(all.dates[i])) : 0);
    }
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
  assert(!global_ids.empty() && "库轴为空");
  assert(std::is_sorted(global_ids.begin(), global_ids.end()) &&
         std::adjacent_find(global_ids.begin(), global_ids.end()) == global_ids.end() &&
         "global_ids 必须升序去重 (LibAxis::ids)");
  (void)global_ids;
}

size_t threads_for(size_t max_tasks) { return static_cast<size_t>(misc::exec::fit(max_tasks)); }

} // namespace analysis
