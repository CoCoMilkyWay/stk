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
  const std::vector<fundamental::Shard> &months = cat.shards(kDaily.name), &days = cat.shards(kMinute.name);
  assert(!months.empty() && !days.empty() && "基本面未同步日线 / 分钟");

  auto out = std::make_shared<Kline>();
  misc::Ymd mark = 0; // 日线水位: 末个非空月分片 (0 行月也落文件)
  for (size_t k = months.size(); k-- > 0 && mark == 0;)
    mark = fundamental::mark(kDaily, months[k]);
  out->last = std::min(mark, days.back().day);

  // 一个任务 = 一个分片 (月 / 分钟日), 各写各的格; 暂存按线程
  const size_t nd = std::min(days.size(), kMinuteDays), T = static_cast<size_t>(pool_.threads());
  std::vector<std::vector<Candle>> month(months.size());
  std::vector<Bars> minute(nd);
  std::vector<fundamental::Scratch> xs(T);
  std::vector<Bar1mDay> bd(T);
  const auto read_month = [&](size_t k, size_t tid) {
    const fundamental::Rows r = xs[tid].open(months[k], kDaily, kCols, ids);
    misc::pq::Col c[7];
    for (size_t j = 0; j < kCols.size(); ++j)
      c[j] = r.col(kCols[j]);
    r.each([&](std::int64_t i, std::uint32_t, misc::Ymd d) {
      const float px = c[3].f32(i), vol = c[4].f32(i);
      if (vol > 0 && px > 0)
        month[k].push_back({cell(c[0].f32(i), c[1].f32(i), c[2].f32(i), px, vol, c[5].f32(i)), d, 0, c[6].f32(i)});
    });
  };
  const auto read_minute = [&](size_t j, size_t tid) {
    Bar1mDay &day = bd[tid];
    day.load(cat, ids, xs[tid], days[days.size() - nd + j].day);
    Bars &b = minute[j];
    b.date = day.date;
    if (const Bar1m *m = day.asset(id))
      for (size_t t = 0; t < TRADE_MINUTES_PER_DAY; ++t)
        if (m[t].volume > 0 && m[t].close > 0)
          b.add(row_slot(t), cell(m[t].open, m[t].high, m[t].low, m[t].close, m[t].volume, m[t].amount));
  };

  // 月分片自末往前一批 2T 片并行读, 本只日根够 kDailyDays 即停; 分钟日并进首批
  size_t lo = months.size(), got = 0; // [lo, end) 已读
  for (size_t extra = nd; lo > 0 && got < kDailyDays; extra = 0) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    const size_t hi = lo, n = std::min(lo, 2 * T);
    lo -= n;
    pool_.tasks(static_cast<int>(n + extra), [&](int i, int tid) {
      const size_t u = static_cast<size_t>(i);
      u < n ? read_month(hi - 1 - u, static_cast<size_t>(tid)) : read_minute(u - n, static_cast<size_t>(tid));
    });
    for (size_t k = lo; k < hi; ++k)
      got += month[k].size();
  }

  out->daily.reserve(got);
  for (size_t k = lo; k < months.size(); ++k)
    out->daily.insert(out->daily.end(), month[k].begin(), month[k].end());
  std::sort(out->daily.begin(), out->daily.end(), [](const Candle &a, const Candle &b) { return a.date < b.date; });
  if (out->daily.size() > kDailyDays)
    out->daily.erase(out->daily.begin(), out->daily.end() - kDailyDays);
  for (const Bars &b : minute)
    if (b.has.any())
      out->minutes.push_back(b);

  std::lock_guard<std::mutex> lock(x_.mutex);
  x_.gen = req.gen;
  x_.data = std::move(out);
  x_.publish();
  return true;
}

} // namespace GUI::Database
