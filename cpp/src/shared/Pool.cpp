#include "shared/Pool.hpp"
#include "features/Storage/DayBatchPlane.hpp" // 上游读: Source / load_source / source_row (与读端同一份)
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <cassert>

namespace pool {

Panel Panel::build(const FeatureLib &bar, size_t col, std::span<const misc::Ymd> dates, const PoolAxis *frozen) {
  assert(bar.tier == TierId::Bar && bar.ready() && "池源 = Bar 池 (先算 Bar 池)");
  assert(!dates.empty() && std::is_sorted(dates.begin(), dates.end()));
  // 第 D 日成员 = 盘中 as-of 的池列 (与读端同一投影): 盘后列对任何网格都是 -1 = D−1 收盘行, 故各层同一份
  const PoolId pool = pool_id(bar.name);
  Source s{.pool = pool, .level = L_1day, .settle = level_info(TierId::Bar, L_1day).fields[col].settle};
  assert(s.settle && "池列须是盘后列 (池键 onSettle 的同层截面)");
  s.cs.io = {col};
  s.cs.vts = {L2::ValidType::ALL};
  s.slot = {0};
  project_rows(s, L_1day, level_valid_rows(L_1day));
  assert(!s.cur && s.carry && "盘后列的盘中 as-of 须是 D−1 收盘行");

  FeatureRead reader(bar);
  Readers rd;
  rd.rd[pool] = &reader;
  IoSlot::Buf b;
  const size_t As = bar.axis.size();
  b.prev.preallocate(As, s.level, 1);

  // 全轴逐日成员位 [date_i][a_bar], 顺带记并集
  std::vector<uint8_t> full(dates.size() * As, 0);
  std::vector<uint8_t> seen(As, 0);
  for (size_t i = 0; i < dates.size(); ++i) {
    const auto it = std::lower_bound(bar.dates.begin(), bar.dates.end(), dates[i]);
    assert(it != bar.dates.end() && *it == dates[i] && "回测日不在 Bar 池日期轴 (Lob 日期轴须截到 Bar 池末日)");
    assert(it != bar.dates.begin() && "回测日是 Bar 池首日, 无 D−1 池源");
    const misc::Ymd prev = *(it - 1);
    assert(reader.has_date(prev) && "Bar 池缺池源日: 先把 Bar 池算完");
    load_source(rd, s, Day{dates[i], prev, PoolMask{0}, pool_bit(pool)}, s.cs.io, b);
    assert(!reader.stale() && "Bar 库判废 (头不符): 重算 Bar 池");
    size_t r = 0;
    const FeatureRead::DayColumns *dc = source_row(s, b, 0, r);
    assert(dc && "盘中 as-of 无源");
    const feature_storage_t *row = dc->col(0) + r * As;
    uint8_t *dst = full.data() + i * As;
    for (size_t a = 0; a < As; ++a) {
      dst[a] = static_cast<float>(row[a]) > 0.5f; // NaN (母集外) → 不在池 (本 TU precise-math)
      seen[a] |= dst[a];
    }
  }

  // 池轴: 并集 / 冻结轴 (区间内进过池而不在轴上的记入 outside_)
  Panel p;
  std::vector<uint32_t> ids;
  std::vector<size_t> pick; // 池轴 sub → Bar 池下标
  if (frozen) {
    ids = frozen->ids;
    for (const uint32_t g : ids) {
      pick.push_back(bar.axis.sub_of(g));
      assert(pick.back() < As && "冻结池轴有资产不在 Bar 池轴上");
    }
    for (size_t a = 0; a < As; ++a)
      if (seen[a] && frozen->sub_of(bar.axis.global(a)) == frozen->size())
        p.outside_.push_back(bar.axis.global(a));
  } else {
    for (size_t a = 0; a < As; ++a)
      if (seen[a]) {
        ids.push_back(bar.axis.global(a));
        pick.push_back(a);
      }
  }
  assert(!ids.empty() && "池在回测区间内并集为空");

  p.dates_.assign(dates.begin(), dates.end());
  p.axis_ = pool_axis(std::move(ids));
  assert((!frozen || p.axis_.hash == frozen->hash) && "冻结池轴重建出的指纹不符 (asset_axis 被改过?)");
  const size_t A = pick.size();
  p.member_.resize(dates.size() * A);
  for (size_t i = 0; i < dates.size(); ++i)
    for (size_t s = 0; s < A; ++s)
      p.member_[i * A + s] = full[i * As + pick[s]];
  return p;
}

const uint8_t *Panel::row(misc::Ymd d) const {
  const auto dit = std::lower_bound(dates_.begin(), dates_.end(), d);
  assert(dit != dates_.end() && *dit == d && "pool: 该日不在本池日期轴");
  const size_t di = static_cast<size_t>(dit - dates_.begin());
  return member_.data() + di * axis_.size();
}

bool Panel::in_pool(size_t sub, misc::Ymd d) const {
  assert(sub < axis_.size());
  return row(d)[sub] != 0;
}

} // namespace pool
