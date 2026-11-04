#include "gui/task_database/services/StockHistory.hpp"

#include "fundamental/Scan.hpp"

#include <algorithm>
#include <cassert>

namespace GUI::Database {

bool StockHistory::run(StockRequest &req) {
  static const fundamental::Spec &kDaily = fundamental::spec("cn_stock_real_bar1d");
  static const fundamental::Spec &kMinute = fundamental::spec("bar1m");
  static const std::vector<std::string> kCols = {"open", "high", "low", "close", "volume", "amount"};
  constexpr int kAuction = 9 * 60 + 25; // bar1m 单列的 09:25 撮合 bar → 并入首根

  const fundamental::Catalog cat;
  fundamental::Ids ids(fundamental::Ids::Cover::Part);
  ids.add(req.code);
  fundamental::Scratch x;
  const std::vector<fundamental::Shard> &months = cat.shards(kDaily.name), &days = cat.shards(kMinute.name);
  assert(!months.empty() && !days.empty() && "基本面未同步日线 / 分钟");

  const auto cols = [](const fundamental::Rows &r, misc::pq::Col *c) {
    for (size_t k = 0; k < kCols.size(); ++k)
      c[k] = r.col(kCols[k]);
  };
  const auto cell = [](const misc::pq::Col *c, std::int64_t i) {
    return Cell{c[0].f32(i), c[1].f32(i), c[2].f32(i), c[3].f32(i), c[4].f32(i), c[5].f32(i)};
  };

  auto out = std::make_shared<Kline>();
  misc::Ymd mark = 0; // 日线水位: 末个非空月分片 (0 行月也落文件)
  for (size_t k = months.size(); k-- > 0 && mark == 0;)
    mark = fundamental::mark(kDaily, months[k]);
  out->last = std::min(mark, days.back().day);

  for (size_t k = months.size(); k-- > 0 && out->daily.size() < kDailyDays;) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    const fundamental::Rows r = x.open(months[k], kDaily, kCols, ids);
    misc::pq::Col c[6];
    cols(r, c);
    r.each([&](std::int64_t i, std::uint32_t, misc::Ymd d) {
      const Cell b = cell(c, i);
      if (b.volume > 0 && b.close > 0)
        out->daily.push_back({b, d, 0});
    });
  }
  std::sort(out->daily.begin(), out->daily.end(), [](const Candle &a, const Candle &b) { return a.date < b.date; });
  if (out->daily.size() > kDailyDays)
    out->daily.erase(out->daily.begin(), out->daily.end() - kDailyDays);

  for (size_t k = days.size() - std::min(days.size(), kMinuteDays); k < days.size(); ++k) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    Bars b;
    b.date = days[k].day;
    Cell auction{};
    fundamental::daily(cat, ids, kMinute, b.date, kCols, x, [&](const fundamental::Rows &r) {
      misc::pq::Col c[6];
      cols(r, c);
      const misc::pq::Col c_date = r.col("date");
      r.each([&](std::int64_t i, std::uint32_t, misc::Ymd) {
        const int m = c_date.minute_of_day(i);
        const Cell v = cell(c, i);
        if (!(v.volume > 0 && v.close > 0))
          return;
        if (m == kAuction) {
          auction = v;
          return;
        }
        const int s = label_slot(m);
        fundamental::expect(s >= 0, kMinute, {.d = b.date, .code = req.code}, "时间戳不在交易时段");
        b.add(s, v);
      });
    });
    if (auction.volume > 0) { // 撮合 bar 早于首根: 先并它
      const Cell first = b.cell[0];
      const bool had = b.has[0];
      b.has.reset(0);
      b.add(0, auction);
      if (had)
        b.add(0, first);
    }
    if (b.has.any())
      out->minutes.push_back(b);
  }

  std::lock_guard<std::mutex> lock(x_.mutex);
  x_.gen = req.gen;
  x_.data = std::move(out);
  x_.publish();
  return true;
}

} // namespace GUI::Database
