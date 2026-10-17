#include "features/Bar1mDay.hpp"

#include "fundamental/Store.hpp"
#include "misc/parquet.hpp"
#include "shared/AssetAxis.hpp"

#include <cassert>
#include <cstdint>
#include <limits>
#include <string>

namespace {

constexpr int kAuctionStamp = 9 * 60 + 25; // 09:25 撮合 bar

// bar 结束时间戳 (墙钟分钟) → L1 行
size_t stamp_to_l1(int stamp) {
  const int m = stamp == kAuctionStamp ? stamp : stamp - 1;
  if (m >= MORNING_START_MIN && m < MORNING_END_MIN)
    return static_cast<size_t>(m - MORNING_START_MIN);
  assert(m >= AFTERNOON_START_MIN && m < AFTERNOON_START_MIN + static_cast<int>(TRADE_MINUTES_PER_DAY - MORNING_MINUTES) &&
         "bar1m: 时间戳不在交易时段");
  return MORNING_MINUTES + static_cast<size_t>(m - AFTERNOON_START_MIN);
}

static_assert(MORNING_START_MIN + 10 == kAuctionStamp);

} // namespace

void Bar1mDay::load(const fundamental::Catalog &cat, misc::Ymd d, size_t num_assets) {
  assert(num_assets > 0);
  date = d;
  A = num_assets;
  rows.assign(A * TRADE_MINUTES_PER_DAY, Bar1m{});
  has.assign(A, 0);
  n_assets = 0;
  n_skipped_rows = 0;

  const fundamental::Shard *sh = cat.find_day("bar1m", d);
  present = sh != nullptr;
  if (!present)
    return;

  const misc::pq::TableView t(fundamental::read(*sh, {"date", "instrument", "open", "high", "low", "close", "volume", "amount"}, {"instrument"}));
  const misc::pq::Col c_date = t.col("date"), c_open = t.col("open"), c_high = t.col("high"), c_low = t.col("low"),
                      c_close = t.col("close"), c_vol = t.col("volume"), c_amt = t.col("amount");
  const misc::pq::DictCol c_inst = t.dict_col("instrument");

  const AssetAxis &axis = asset_axis();
  std::vector<size_t> id_of(static_cast<size_t>(c_inst.dict_size()));
  for (std::int32_t k = 0; k < c_inst.dict_size(); ++k) {
    const size_t g = axis.find(std::string(c_inst.dict_value(k)));
    id_of[static_cast<size_t>(k)] = g < A ? g : A;
  }

  const std::int32_t *idx = c_inst.indices();
  const std::int64_t n = t.rows();
  for (std::int64_t i = 0; i < n; ++i) {
    const size_t a = id_of[static_cast<size_t>(idx[i])];
    if (a == A) {
      ++n_skipped_rows;
      continue;
    }
    assert(c_date.yyyymmdd(i) == d && "bar1m: 分片内日期与文件名不符");
    const float vol = c_vol.f32(i);
    assert(vol >= 0.0f && vol <= static_cast<float>(std::numeric_limits<uint32_t>::max()) && "bar1m: volume 越界");
    Bar1m &b = rows[a * TRADE_MINUTES_PER_DAY + stamp_to_l1(c_date.minute_of_day(i))];
    b.open = c_open.f32(i);
    b.high = c_high.f32(i);
    b.low = c_low.f32(i);
    b.close = c_close.f32(i);
    b.amount = c_amt.f32(i);
    b.volume = static_cast<uint32_t>(vol);
    n_assets += has[a] == 0;
    has[a] = 1;
  }
}
