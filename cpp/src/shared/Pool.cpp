#include "shared/Pool.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <cassert>

namespace pool {

Panel Panel::build(const FeatureLib &bar, size_t col, std::span<const misc::Ymd> dates) {
  assert(bar.tier == TierId::Bar && bar.ready() && "票池源 = Bar 库 (先算 Bar 库)");
  assert(!dates.empty() && std::is_sorted(dates.begin(), dates.end()));
  constexpr size_t kLevel = 1;
  constexpr size_t kCloseRow = level_valid_rows(kLevel) - 1; // L1 254 (收盘)

  FeatureRead reader(bar);
  FeatureRead::DayColumns dc;
  const size_t As = bar.axis.size();
  dc.preallocate(As, kLevel, 1);
  const std::vector<size_t> cols{col};

  // 全轴逐日成员位 [date_i][a_bar], 顺带记并集
  std::vector<uint8_t> full(dates.size() * As, 0);
  std::vector<uint8_t> seen(As, 0);
  for (size_t i = 0; i < dates.size(); ++i) {
    const auto it = std::lower_bound(bar.dates.begin(), bar.dates.end(), dates[i]);
    assert(it != bar.dates.end() && *it == dates[i] && it != bar.dates.begin() && "回测日不在 Bar 库日期轴 (或是其首日): 重算 Bar 库覆盖到 end_date");
    const misc::Ymd prev = *(it - 1);
    assert(reader.has_date(prev) && "Bar 库缺票池源日 (D−1): 先把 Bar 库算完");
    reader.load_day_columns(prev, cols, dc);
    assert(!reader.stale() && "Bar 库判废 (头不符): 重算 Bar 库");
    const feature_storage_t *row = dc.col(0) + kCloseRow * As;
    uint8_t *dst = full.data() + i * As;
    for (size_t a = 0; a < As; ++a) {
      dst[a] = static_cast<float>(row[a]) > 0.5f; // NaN (母集外) → 不在池 (本 TU precise-math)
      seen[a] |= dst[a];
    }
  }

  std::vector<uint32_t> ids;
  std::vector<size_t> pick; // 库轴 sub → Bar 库下标
  for (size_t a = 0; a < As; ++a)
    if (seen[a]) {
      ids.push_back(bar.axis.global(a));
      pick.push_back(a);
    }
  assert(!ids.empty() && "票池在回测区间内并集为空");

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
  assert(dit != dates_.end() && *dit == d && "pool: 该日不在本库日期轴");
  assert(sub < axis_.size());
  const size_t di = static_cast<size_t>(dit - dates_.begin());
  return member_[di * axis_.size() + sub] != 0;
}

} // namespace pool
