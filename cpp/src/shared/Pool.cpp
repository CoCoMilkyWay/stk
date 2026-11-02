#include "shared/Pool.hpp"
#include "features/Method/Fundamental.hpp"
#include "features/Storage/DayBatchPlane.hpp" // 上游读: Source / load_source / source_row (与读端同一份)
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <cassert>

namespace pool {

namespace {

// all: 全轴逐日成员位 [date_i][global] = 当日活跃
std::vector<uint8_t> live_members(const fund::Panel &fund, std::span<const misc::Ymd> dates, size_t n) {
  std::vector<uint8_t> full(dates.size() * n);
  for (size_t i = 0; i < dates.size(); ++i)
    fund.live(dates[i], std::span(full).subspan(i * n, n));
  return full;
}

// 子池: 全轴逐日成员位 [date_i][global] = all Bar 库 D−1 收盘行的池列 col (all Bar 库轴外 = 不在池)
std::vector<uint8_t> pool_members(const FeatureLib &bar, size_t col, std::span<const misc::Ymd> dates, size_t n) {
  assert(bar.id == kAllBar && bar.ready() && "池源 = all Bar 库 (先算 all Bar 库)");
  // 第 D 日成员 = 盘中 as-of 的池列 (与读端同一投影): 盘后列对任何网格都是 -1 = D−1 收盘行, 故各层同一份
  Source s{.lib = kAllBar, .level = L_1day, .settle = level_info(TierId::Bar, L_1day).fields[col].settle};
  assert(s.settle && "池列须是盘后列 (池键 onSettle 的同层截面)");
  s.cs.io = {col};
  s.cs.vts = {L2::ValidType::ALL};
  s.slot = {0};
  project_rows(s, L_1day, level_valid_rows(L_1day));
  assert(!s.cur && s.carry && "盘后列的盘中 as-of 须是 D−1 收盘行");

  FeatureRead reader(bar);
  Readers rd;
  rd.rd[kAllBar] = &reader;
  IoSlot::Buf b;
  const size_t As = bar.axis.size();
  b.prev.preallocate(As, s.level, 1);

  std::vector<uint8_t> full(dates.size() * n, 0);
  for (size_t i = 0; i < dates.size(); ++i) {
    const auto it = std::lower_bound(bar.dates.begin(), bar.dates.end(), dates[i]);
    assert(it != bar.dates.end() && *it == dates[i] && "库日不在 all Bar 库日期轴 (子池日期轴须截到 all Bar 库末日)");
    assert(it != bar.dates.begin() && "库日是 all Bar 库首日, 无 D−1 池源");
    const misc::Ymd prev = *(it - 1);
    assert(reader.has_date(prev) && "all Bar 库缺池源日: 先把 all Bar 库算完");
    load_source(rd, s, Day{dates[i], prev, LibMask{0}, lib_bit(kAllBar)}, s.cs.io, b);
    assert(!reader.stale() && "all Bar 库判废 (头不符): 重算 all Bar 库");
    size_t r = 0;
    const FeatureRead::DayColumns *dc = source_row(s, b, 0, r);
    assert(dc && "盘中 as-of 无源");
    const feature_storage_t *row = dc->col(0) + r * As;
    uint8_t *dst = full.data() + i * n;
    for (size_t a = 0; a < As; ++a)
      dst[bar.axis.global(a)] = static_cast<float>(row[a]) > 0.5f; // NaN (母集外) → 不在池 (本 TU precise-math)
  }
  return full;
}

} // namespace

Panel Panel::build(PoolId pool, const Config &cfg, const fund::Panel &fund, std::span<const misc::Ymd> dates, const PoolAxis *frozen) {
  assert(!dates.empty() && std::is_sorted(dates.begin(), dates.end()));
  const size_t n = asset_axis().size();
  const std::vector<uint8_t> full = pool == kAllPool ? live_members(fund, dates, n) : pool_members(load_lib(cfg, kAllBar), pool_def(pool).pool_col, dates, n);

  // 自然轴: all = 全部标的; 子池 = dates 内进过池的
  std::vector<uint32_t> natural;
  for (size_t g = 0; g < n; ++g) {
    bool in = pool == kAllPool;
    for (size_t i = 0; i < dates.size() && !in; ++i)
      in = full[i * n + g] != 0;
    if (in)
      natural.push_back(static_cast<uint32_t>(g));
  }
  assert(!natural.empty() && "池在区间内为空");

  Panel p;
  p.dates_.assign(dates.begin(), dates.end());
  p.axis_ = frozen ? *frozen : pool_axis(natural);
  for (const uint32_t g : natural)
    if (p.axis_.sub_of(g) == p.axis_.size())
      p.outside_.push_back(g);
  const size_t A = p.axis_.size();
  p.member_.resize(dates.size() * A);
  for (size_t i = 0; i < dates.size(); ++i)
    for (size_t s = 0; s < A; ++s)
      p.member_[i * A + s] = full[i * n + p.axis_.global(s)];
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
