#include "features/Panel/Pool.hpp"
#include "features/Panel/Fund.hpp"
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeaturePlane.hpp" // 上游读: Source / load_source / source_row (与读端同一份)

#include <algorithm>
#include <cassert>
#include <iterator>
#include <optional>
#include <string_view>

namespace pool {

namespace {

// all: 全轴逐日成员位 [date_i][global] = 当日活跃
std::vector<uint8_t> live_members(const fund::Panel &fund, std::span<const misc::Ymd> dates, size_t n) {
  std::vector<uint8_t> full(dates.size() * n);
  for (size_t i = 0; i < dates.size(); ++i)
    fund.live(dates[i], std::span(full).subspan(i * n, n));
  return full;
}

// 档 tier 的 lvl 层里 code 列的下标 (运行时层表)
size_t col_of(TierId tier, size_t lvl, std::string_view code) {
  const LevelInfo &L = level_info(tier, lvl);
  size_t i = 0;
  while (i < L.field_count && code != L.fields[i].code)
    ++i;
  assert(i < L.field_count && "列不在层表");
  return i;
}

// 上游库 up 的 L_1day 列 col 在本库日 D 的盘中 as-of (盘后列 → D−1 收盘行, 盘中列 → D 行) > 0.5 → [date_i][上游轴下标]
// NaN (范围外) → 0 (本 TU precise-math)
std::vector<uint8_t> parent_bits(const FeatureLib &up, size_t col, std::span<const misc::Ymd> dates) {
  assert(up.ready() && "上游库未建: 先算上游库");
  Source s{.lib = up.id, .level = L_1day, .settle = level_info(up.tier(), L_1day).fields[col].settle};
  s.cs.io = {col};
  s.cs.vts = {L2::ValidType::ALL};
  s.slot = {0};
  project_rows(s, L_1day, level_valid_rows(L_1day));
  assert(s.cur != s.carry && "L_1day 单行: as-of 落当日或前一交易日其一");

  FeatureRead reader(up);
  Readers rd;
  rd.rd[up.id] = &reader;
  IoSlot::Buf b;
  const size_t As = up.axis.size();
  (s.carry ? b.prev : b.cur).preallocate(As, s.level, 1);
  const LibMask bit = lib_bit(up.id);

  std::vector<uint8_t> bits(dates.size() * As);
  for (size_t i = 0; i < dates.size(); ++i) {
    assert(parent_covers(up.dates, s.carry, dates[i]) && "库日不在上游库日期轴 (或是其首日, 无 D−1)");
    const misc::Ymd src = s.carry ? *(std::lower_bound(up.dates.begin(), up.dates.end(), dates[i]) - 1) : dates[i];
    assert(reader.has_date(src) && "上游库缺这天: 先把上游库算完");
    load_source(rd, s, Day{dates[i], s.carry ? src : 0, s.cur ? bit : 0, s.carry ? bit : 0}, s.cs.io, b);
    assert(!reader.stale() && "上游库判废 (头不符): 重算上游库");
    size_t r = 0;
    const FeatureRead::DayColumns *dc = source_row(s, b, 0, r);
    assert(dc && "上游库无源");
    const feature_storage_t *row = dc->col(0) + r * As;
    for (size_t a = 0; a < As; ++a)
      bits[i * As + a] = static_cast<float>(row[a]) > 0.5f;
  }
  return bits;
}

} // namespace

Panel Panel::build(LibId lib, const std::string &feature_dir, const fund::Panel &fund, std::span<const misc::Ymd> dates, const FeatureLib &old) {
  assert(!dates.empty() && std::is_sorted(dates.begin(), dates.end()));
  assert((!old.ready() || old.id == lib) && "旧清单不是本库");
  Panel p;
  p.dates_.assign(dates.begin(), dates.end());

  // 全轴逐日成员 [date_i][global]: 根 (all 定轴库) = 当日活跃; 其余 = 上游列 (子池定轴库: all 库池列; 随轴库: 同池定轴库 cs_valid), 上游轴外 = 不在池
  const std::optional<LibParent> parent = lib_parent(lib);
  const size_t n = asset_axis().size();
  std::vector<uint8_t> full;
  FeatureLib up;
  if (!parent)
    full = live_members(fund, dates, n);
  else {
    up = load_lib(feature_dir, parent->lib);
    const size_t col = axis_lib(lib) != lib ? col_of(up.tier(), L_1day, "cs_valid") : pool_def(lib_pool(lib)).pool_col;
    const std::vector<uint8_t> bits = parent_bits(up, col, dates);
    const size_t As = up.axis.size();
    full.assign(dates.size() * n, 0);
    for (size_t i = 0; i < dates.size(); ++i)
      for (size_t a = 0; a < As; ++a)
        full[i * n + up.axis.global(a)] = bits[i * As + a];
  }

  // 自然轴: 根 = 全部标的; 其余 = dates 内进过池的
  std::vector<uint32_t> natural;
  for (size_t g = 0; g < n; ++g) {
    bool in = !parent;
    for (size_t i = 0; i < dates.size() && !in; ++i)
      in = full[i * n + g] != 0;
    if (in)
      natural.push_back(static_cast<uint32_t>(g));
  }
  assert(!natural.empty() && "池在区间内为空");

  p.axis_ = old.ready() ? old.axis : pool_axis(natural);
  assert((axis_lib(lib) == lib || axis_within(p.axis_, up.axis)) && "随轴库轴须 ⊆ 同池定轴库清单轴 (续算: 定轴库换过轴就全量重算本库)");
  std::vector<uint32_t> out;
  for (const uint32_t g : natural)
    if (p.axis_.sub_of(g) == p.axis_.size())
      out.push_back(g);
  std::ranges::set_union(old.outside, out, std::back_inserter(p.outside_));
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
