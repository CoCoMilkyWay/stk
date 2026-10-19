#include "features/Bar1mDay.hpp"

#include "fundamental/Scan.hpp"
#include "misc/parquet.hpp"

#include <cassert>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace {

constexpr int kAuctionStamp = 9 * 60 + 25; // 09:25 撮合 bar

// bar 结束时间戳 (墙钟分钟) → L_min 行
size_t stamp_to_min(int stamp) {
  const int m = stamp == kAuctionStamp ? stamp : stamp - 1;
  if (m >= MORNING_START_MIN && m < MORNING_END_MIN)
    return static_cast<size_t>(m - MORNING_START_MIN);
  assert(m >= AFTERNOON_START_MIN && m < AFTERNOON_START_MIN + static_cast<int>(TRADE_MINUTES_PER_DAY - MORNING_MINUTES) &&
         "bar1m: 时间戳不在交易时段");
  return MORNING_MINUTES + static_cast<size_t>(m - AFTERNOON_START_MIN);
}

static_assert(MORNING_START_MIN + 10 == kAuctionStamp);

} // namespace

void Bar1mDay::load(const fundamental::Catalog &cat, const fundamental::Ids &ids, fundamental::Scratch &x, misc::Ymd d) {
  static const fundamental::Spec &kSpec = fundamental::spec("bar1m");
  static const std::vector<std::string> kCols = {"open", "high", "low", "close", "volume", "amount"};
  date = d;
  A = ids.size();
  assert(A > 0);
  rows.assign(A * TRADE_MINUTES_PER_DAY, Bar1m{});
  has.assign(A, 0);
  n_assets = 0;
  n_skipped_rows = 0;

  present = fundamental::daily(cat, ids, kSpec, d, kCols, x, [&](const fundamental::Rows &r) {
    const misc::pq::Col c_date = r.col("date"), c_open = r.col("open"), c_high = r.col("high"), c_low = r.col("low"),
                        c_close = r.col("close"), c_vol = r.col("volume"), c_amt = r.col("amount");
    std::int64_t hit = 0;
    r.each([&](std::int64_t i, std::uint32_t id, misc::Ymd day) {
      assert(day == d && "bar1m: 分片内日期与文件名不符");
      const size_t a = id;
      const float vol = c_vol.f32(i);
      assert(vol >= 0.0f && vol <= static_cast<float>(std::numeric_limits<uint32_t>::max()) && "bar1m: volume 越界");
      Bar1m &b = rows[a * TRADE_MINUTES_PER_DAY + stamp_to_min(c_date.minute_of_day(i))];
      b.open = c_open.f32(i);
      b.high = c_high.f32(i);
      b.low = c_low.f32(i);
      b.close = c_close.f32(i);
      b.amount = c_amt.f32(i);
      b.volume = static_cast<uint32_t>(vol);
      n_assets += has[a] == 0;
      has[a] = 1;
      ++hit;
    });
    n_skipped_rows = static_cast<size_t>(r.size() - hit);
  });
}
