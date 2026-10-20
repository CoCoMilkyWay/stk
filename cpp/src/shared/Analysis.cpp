#include "shared/Analysis.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "misc/date.hpp"
#include "misc/profiler.hpp"
#include "shared/Config.hpp"
#include "shared/Feature.hpp"

#include <algorithm>
#include <map>
#include <numeric>
#include <random>

namespace analysis {

namespace {
// A 轴 + 各池映射 (assets 空 = 主池轴, 主池同轴不映射)
void map_axis(ReadScope &s, std::vector<uint32_t> assets) {
  const bool own = assets.empty();
  s.axis = own ? s.lib().axis : PoolAxis{.ids = std::move(assets)};
  check_axis(s.axis.ids);
  for (const PoolId l : pool_ids()) {
    s.to[l].clear();
    if (!s.has(l) || (own && l == s.main))
      continue;
    const PoolAxis &side = s.libs[l].axis;
    s.to[l].resize(s.axis.size());
    for (size_t a = 0; a < s.axis.size(); ++a) {
      const size_t x = side.sub_of(s.axis.global(a));
      s.to[l][a] = x < side.size() ? static_cast<uint32_t>(x) : kAbsent;
    }
  }
}
// 池 l 落了第 d 天 (按库清单日期轴 + 末层日文件, 与写端 commit 标记同一判据)
bool landed(const ReadScope &s, PoolId l, misc::Ymd d) {
  const FeatureLib &lib = s.libs[l];
  return std::binary_search(lib.dates.begin(), lib.dates.end(), d) && FeatureRead::has_date(lib.tier, lib.dir, d);
}
// 第 d 天池集里落了盘的池
PoolMask landed_mask(const ReadScope &s, misc::Ymd d) {
  PoolMask m = 0;
  for (const PoolId l : pool_ids())
    if (s.has(l) && landed(s, l, d))
      m |= pool_bit(l);
  return m;
}
} // namespace

ReadScope read_scope(const Config &cfg, PoolId main, const std::vector<ColRef> &cols, std::vector<uint32_t> assets,
                     DayAxis days) {
  ReadScope s;
  bool ready = true;
  const auto add = [&](PoolId l) {
    if (s.has(l))
      return;
    s.libs[l] = load_lib(cfg, pool_def(l).name);
    assert(s.libs[l].tier == pool_tier(l));
    ready &= s.libs[l].ready();
  };
  if (main != kAutoMain)
    add(main);
  for (const ColRef &c : cols)
    add(c.pool);
  if (!ready) // 未建库 = 空范围 (请求方见 months 为空即放弃)
    return s;
  if (main == kAutoMain) {
    assert(!cols.empty() && "自动主池要列");
    main = cols.front().pool;
    for (const PoolId l : pool_ids())
      if (s.has(l) && s.libs[l].axis.size() < s.libs[main].axis.size())
        main = l;
  }
  s.main = main;
  if (days == DayAxis::Main) {
    s.days = pool_bit(main);
    s.months = misc::months_between(cfg.start_date, cfg.end_date);
  } else {
    misc::Ymd lo = 0, hi = 0;
    for (const PoolId l : pool_ids())
      if (s.has(l)) {
        const std::vector<misc::Ymd> &d = s.libs[l].dates;
        assert(!d.empty() && "已建库日期轴为空");
        s.days |= pool_bit(l);
        lo = lo == 0 ? d.front() : std::min(lo, d.front());
        hi = std::max(hi, d.back());
      }
    s.months = misc::months_between(lo, hi);
  }
  map_axis(s, std::move(assets));
  return s;
}

PerPool<size_t> ReadScope::sizes() const {
  PerPool<size_t> out;
  for (const PoolId l : pool_ids())
    out[l] = libs[l].axis.size();
  return out;
}

Readers ReadScope::readers(const PerPool<const FeatureRead *> &fr) const {
  Readers rd;
  for (const PoolId l : pool_ids())
    if (has(l)) {
      assert(fr[l] && "池集池缺 reader");
      rd.rd[l] = fr[l];
      rd.ids[l] = to[l].empty() ? nullptr : to[l].data();
    }
  return rd;
}

bool read_request(const Config &cfg, const Feature &feature, ReadSpec rs, ReadScope &scope, ReadPlan &plan) {
  scope = read_scope(cfg, rs.main, rs.cols, std::move(rs.assets), rs.days);
  if (scope.months.empty())
    return false;
  plan = read_plan(feature, rs.level, rs.rows, rs.carry, std::move(rs.cols));
  return true;
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

DateList enumerate_dates(const ReadScope &s) {
  TraceN("EnumDates"); // 首帧账目: 日轴池每 (池, 日) 一次 stat
  assert(!s.months.empty());
  const misc::Ymd lo = misc::first_day(s.months.front()), hi = misc::last_day(s.months.back());
  std::vector<std::pair<misc::Ymd, PoolMask>> axis; // (日, 落了盘的日轴池)
  for (const PoolId l : pool_ids()) {
    if (!(s.days & pool_bit(l)))
      continue;
    const FeatureLib &lib = s.libs[l];
    for (auto it = std::lower_bound(lib.dates.begin(), lib.dates.end(), lo); it != lib.dates.end() && *it <= hi; ++it)
      if (FeatureRead::has_date(lib.tier, lib.dir, *it))
        axis.emplace_back(*it, pool_bit(l));
  }
  std::sort(axis.begin(), axis.end());
  DateList out;
  for (const auto &[d, bit] : axis) {
    if (!out.dates.empty() && out.dates.back() == d) {
      out.has.back() |= bit;
      continue;
    }
    out.dates.push_back(d);
    out.has.push_back(bit);
    const auto m = std::lower_bound(s.months.begin(), s.months.end(), misc::ym_of(d));
    assert(m != s.months.end() && *m == misc::ym_of(d));
    out.month.push_back(static_cast<uint16_t>(m - s.months.begin()));
  }
  return out;
}

// 同 (池, 层) 的列并成一个源 (槽 = 请求序), 源行由 rows 定
ReadPlan read_plan(const Feature &feature, size_t level, Rows rows, bool carry, std::vector<ColRef> cols) {
  assert(!cols.empty() && level < LEVEL_COUNT);
  assert((rows == Rows::Tail || carry) && "网格行恒承接 (as-of 投影到 -1 即前日末行)");
  const size_t rpd = level_valid_rows(level);
  ReadPlan p;
  p.level = level;
  p.cols = std::move(cols);
  std::map<std::pair<PoolId, size_t>, std::vector<uint32_t>> groups; // (池, 层) → 平面列 (请求序)
  for (size_t c = 0; c < p.cols.size(); ++c)
    groups[{p.cols[c].pool, p.cols[c].level}].push_back(static_cast<uint32_t>(c));
  for (auto &[key, slot] : groups) {
    Source s;
    s.pool = key.first;
    s.level = key.second;
    std::vector<size_t> io;
    io.reserve(slot.size());
    for (const uint32_t c : slot)
      io.push_back(p.cols[c].col);
    s.cs = column_set(feature, pool_tier(s.pool), s.level, std::move(io));
    s.slot = std::move(slot);
    if (rows == Rows::AsOf)
      project_rows(s, level, rpd);
    else
      tail_rows(s, level, carry);
    p.srcs.push_back(std::move(s));
  }
  return p;
}

SegPlan plan_segs(const ReadScope &scope, size_t level, DayOrder order, size_t limit) {
  const DateList all = enumerate_dates(scope);
  const size_t days = seg_days(level), n = all.dates.size();
  std::vector<uint32_t> idx((n + days - 1) / days);
  std::iota(idx.begin(), idx.end(), 0u);
  if (order == DayOrder::Shuffle)
    std::shuffle(idx.begin(), idx.end(), std::mt19937{0x5eed}); // 与 shuffled_order 同种子: 跨构建稳定
  if (limit > 0 && idx.size() > limit)
    idx.resize(limit);
  // 第 i 天池集里落了盘的池 = 日轴池位 + 余池 (只判入选的日; 记住上一次: 相邻日的承接日 = 上一日, 不重判)
  size_t memo_i = SIZE_MAX;
  PoolMask memo = 0;
  const auto has = [&](size_t i) {
    if (i == memo_i)
      return memo;
    PoolMask m = all.has[i];
    for (const PoolId l : pool_ids())
      if (scope.has(l) && !(scope.days & pool_bit(l)) && landed(scope, l, all.dates[i]))
        m |= pool_bit(l);
    memo_i = i;
    memo = m;
    return m;
  };
  SegPlan p;
  p.days = days;
  p.dates.reserve(idx.size() * days);
  p.prev.reserve(idx.size() * days);
  p.has.reserve(idx.size() * days);
  p.has_prev.reserve(idx.size() * days);
  p.month.reserve(idx.size() * days);
  p.weekday.reserve(idx.size() * days);
  for (const uint32_t seg : idx) {
    for (size_t d = 0; d < days; ++d) {
      const size_t i = seg * days + d;
      const bool in = i < n;
      const PoolMask hp = in && i > 0 ? has(i - 1) : 0; // 先承接日后当日: 顺序相邻时承接日命中上一次
      const PoolMask hc = in ? has(i) : 0;
      p.dates.push_back(in ? all.dates[i] : 0);
      p.prev.push_back(in && i > 0 ? all.dates[i - 1] : 0);
      p.has.push_back(hc);
      p.has_prev.push_back(hp);
      p.month.push_back(in ? all.month[i] : 0);
      p.weekday.push_back(in ? static_cast<uint8_t>(misc::weekday_of(all.dates[i])) : 0);
    }
  }
  return p;
}

SegPlan plan_day(const ReadScope &scope, misc::Ymd date) {
  assert(date != 0 && !scope.months.empty());
  // 日轴上前一日 = 各日轴池 < date 的末个落盘日之最大者 (不早于区间首日; 0 = 轴首)
  const misc::Ymd lo = misc::first_day(scope.months.front());
  misc::Ymd prev = 0;
  for (const PoolId l : pool_ids()) {
    if (!(scope.days & pool_bit(l)))
      continue;
    const FeatureLib &lib = scope.libs[l];
    for (auto it = std::lower_bound(lib.dates.begin(), lib.dates.end(), date); it != lib.dates.begin() && *(it - 1) > prev && *(it - 1) >= lo; --it)
      if (FeatureRead::has_date(lib.tier, lib.dir, *(it - 1))) {
        prev = *(it - 1);
        break;
      }
  }
  SegPlan p;
  p.days = 1;
  p.dates = {date};
  p.prev = {prev};
  p.has = {landed_mask(scope, date)};
  p.has_prev = {prev != 0 ? landed_mask(scope, prev) : PoolMask{0}};
  p.month = {0};
  p.weekday = {static_cast<uint8_t>(misc::weekday_of(date))};
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
  assert(!global_ids.empty() && "池轴为空");
  assert(std::is_sorted(global_ids.begin(), global_ids.end()) &&
         std::adjacent_find(global_ids.begin(), global_ids.end()) == global_ids.end() &&
         "global_ids 必须升序去重 (PoolAxis::ids)");
  (void)global_ids;
}

size_t threads_for(size_t max_tasks) { return static_cast<size_t>(misc::exec::fit(max_tasks)); }

} // namespace analysis
