#include "gui/task_database/services/StockHistory.hpp"

#include "features/Bar1mDay.hpp"
#include "fundamental/Scan.hpp"

#include <algorithm>
#include <cassert>

namespace GUI::Database {

bool StockHistory::run(StockRequest &req) {
  static const fundamental::Spec &kDaily = fundamental::spec("cn_stock_real_bar1d");
  static const fundamental::Spec &kMinute = fundamental::spec("bar1m");
  static const std::vector<std::string> kCols = {"open", "high", "low", "close", "volume", "amount", "pre_close"};

  const fundamental::Catalog cat;
  fundamental::Ids ids(fundamental::Ids::Cover::Part);
  const uint32_t id = ids.add(req.code);
  fundamental::Scratch x;
  const std::vector<fundamental::Shard> &months = cat.shards(kDaily.name), &days = cat.shards(kMinute.name);
  assert(!months.empty() && !days.empty() && "基本面未同步日线 / 分钟");

  auto out = std::make_shared<Kline>();
  misc::Ymd mark = 0; // 日线水位: 末个非空月分片 (0 行月也落文件)
  for (size_t k = months.size(); k-- > 0 && mark == 0;)
    mark = fundamental::mark(kDaily, months[k]);
  out->last = std::min(mark, days.back().day);

  for (size_t k = months.size(); k-- > 0 && out->daily.size() < kDailyDays;) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    const fundamental::Rows r = x.open(months[k], kDaily, kCols, ids);
    misc::pq::Col c[7];
    for (size_t j = 0; j < kCols.size(); ++j)
      c[j] = r.col(kCols[j]);
    r.each([&](std::int64_t i, std::uint32_t, misc::Ymd d) {
      const float px = c[3].f32(i), vol = c[4].f32(i);
      if (vol > 0 && px > 0)
        out->daily.push_back({cell(c[0].f32(i), c[1].f32(i), c[2].f32(i), px, vol, c[5].f32(i)), d, 0, c[6].f32(i)});
    });
  }
  std::sort(out->daily.begin(), out->daily.end(), [](const Candle &a, const Candle &b) { return a.date < b.date; });
  if (out->daily.size() > kDailyDays)
    out->daily.erase(out->daily.begin(), out->daily.end() - kDailyDays);

  Bar1mDay day;
  for (size_t k = days.size() - std::min(days.size(), kMinuteDays); k < days.size(); ++k) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    day.load(cat, ids, x, days[k].day);
    const Bar1m *m = day.asset(id);
    if (!m)
      continue;
    Bars b;
    b.date = day.date;
    for (size_t t = 0; t < TRADE_MINUTES_PER_DAY; ++t)
      if (m[t].volume > 0 && m[t].close > 0)
        b.add(row_slot(t), cell(m[t].open, m[t].high, m[t].low, m[t].close, m[t].volume, m[t].amount));
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
