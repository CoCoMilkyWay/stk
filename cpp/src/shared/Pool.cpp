#include "shared/Pool.hpp"
#include "features/Storage/DayBatchPlane.hpp" // 上游读: Source / load_source / source_row (与读端同一份)
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <cassert>

namespace pool {

Panel Panel::build(const FeatureLib &bar, size_t col, std::span<const misc::Ymd> dates, size_t grid) {
  assert(bar.tier == TierId::Bar && bar.ready() && "池源 = Bar 池 (先算 Bar 池)");
  assert(!dates.empty() && std::is_sorted(dates.begin(), dates.end()));
  // 第 D 日成员 = grid 层 as-of 的 L_1day 列 (与读端同一投影): L_sec / 日内层 → -1 = D−1 收盘行; L_1day → 0 = D 收盘行
  const PoolId pool = pool_id(bar.name);
  Source s{.pool = pool, .level = L_1day};
  s.cs.io = {col};
  s.cs.vts = {L2::ValidType::ALL};
  s.slot = {0};
  project_rows(s, grid, level_valid_rows(grid));
  assert(s.cur != s.carry && std::ranges::all_of(s.row, [&](int32_t r) { return r == s.row[0]; }) && "日层 as-of 须整日常量");

  FeatureRead reader(bar);
  Readers rd;
  rd.rd[pool] = &reader;
  IoSlot::Buf b;
  const size_t As = bar.axis.size();
  (s.cur ? b.cur : b.prev).preallocate(As, s.level, 1);

  // 全轴逐日成员位 [date_i][a_bar], 顺带记并集
  std::vector<uint8_t> full(dates.size() * As, 0);
  std::vector<uint8_t> seen(As, 0);
  for (size_t i = 0; i < dates.size(); ++i) {
    const auto it = std::lower_bound(bar.dates.begin(), bar.dates.end(), dates[i]);
    assert(it != bar.dates.end() && *it == dates[i] && "回测日不在 Bar 池日期轴: 重算 Bar 池覆盖到 end_date");
    assert((s.cur || it != bar.dates.begin()) && "回测日是 Bar 池首日, 无 D−1 池源");
    const misc::Ymd prev = it != bar.dates.begin() ? *(it - 1) : 0;
    assert((s.cur ? reader.has_date(dates[i]) : reader.has_date(prev)) && "Bar 池缺池源日: 先把 Bar 池算完");
    load_source(rd, s, Day{dates[i], prev, s.cur ? pool_bit(pool) : 0u, s.carry ? pool_bit(pool) : 0u}, s.cs.io, b);
    assert(!reader.stale() && "Bar 库判废 (头不符): 重算 Bar 池");
    size_t r = 0;
    const FeatureRead::DayColumns *dc = source_row(s, b, 0, r);
    assert(dc && "盘前 as-of 无源");
    const feature_storage_t *row = dc->col(0) + r * As;
    uint8_t *dst = full.data() + i * As;
    for (size_t a = 0; a < As; ++a) {
      dst[a] = static_cast<float>(row[a]) > 0.5f; // NaN (母集外) → 不在池 (本 TU precise-math)
      seen[a] |= dst[a];
    }
  }

  std::vector<uint32_t> ids;
  std::vector<size_t> pick; // 池轴 sub → Bar 池下标
  for (size_t a = 0; a < As; ++a)
    if (seen[a]) {
      ids.push_back(bar.axis.global(a));
      pick.push_back(a);
    }
  assert(!ids.empty() && "池在回测区间内并集为空");

  Panel p;
  p.dates_.assign(dates.begin(), dates.end());
  p.axis_ = pool_axis(std::move(ids));
  const size_t A = pick.size();
  p.member_.resize(dates.size() * A);
  for (size_t i = 0; i < dates.size(); ++i)
    for (size_t s = 0; s < A; ++s)
      p.member_[i * A + s] = full[i * As + pick[s]];
  return p;
}

bool Panel::in_pool(size_t sub, misc::Ymd d) const {
  const auto dit = std::lower_bound(dates_.begin(), dates_.end(), d);
  assert(dit != dates_.end() && *dit == d && "pool: 该日不在本池日期轴");
  assert(sub < axis_.size());
  const size_t di = static_cast<size_t>(dit - dates_.begin());
  return member_[di * axis_.size() + sub] != 0;
}

} // namespace pool
