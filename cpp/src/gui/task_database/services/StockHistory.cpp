#include "gui/task_database/services/StockHistory.hpp"

#include "features/Bar1mDay.hpp"
#include "fundamental/Scan.hpp"
#include "shared/AssetAxis.hpp"

#include <algorithm>
#include <cassert>
#include <memory>
#include <vector>

namespace GUI::Database {

bool StockHistory::run(HistoryRequest &req) {
  assert(req.keys.size() <= kKeep);

  const fundamental::Catalog cat;
  const misc::Ymd last = fund_last(cat);
  const std::vector<fundamental::Shard> &months = cat.shards(kBar1d), &days = cat.shards(kBar1m);

  const auto cached = [&](uint32_t id) {
    return std::find_if(keep_.begin(), keep_.end(), [&](const auto &e) { return e.first == id && e.second.last == last; });
  };
  fundamental::Ids ids(fundamental::Ids::Cover::Part);
  std::vector<uint32_t> miss; // [ids 下标] 全局 id
  for (const uint32_t id : req.keys)
    if (cached(id) == keep_.end() && std::find(miss.begin(), miss.end(), id) == miss.end()) {
      [[maybe_unused]] const uint32_t j = ids.add(asset_axis().code(id));
      assert(j == miss.size());
      miss.push_back(id);
    }

  // 一个任务 = 一个分片 (月 / 分钟日), 各写各的格; 暂存按线程
  const size_t nk = miss.size(), nd = std::min(days.size(), kMinuteDays), T = static_cast<size_t>(pool_.threads());
  std::vector<std::vector<Candle>> month(months.size() * nk); // [月][键]
  std::vector<Bars> minute(nd * nk);                          // [日][键]
  std::vector<fundamental::Scratch> xs(T);
  std::vector<Bar1mDay> bd(T);
  const auto read_month = [&](size_t k, size_t tid) {
    each_daily(xs[tid], months[k], ids, [&](uint32_t j, const Candle &c) {
      if (traded(c))
        month[k * nk + j].push_back(c);
    });
  };
  const auto read_minute = [&](size_t q, size_t tid) {
    Bar1mDay &day = bd[tid];
    day.load(cat, ids, xs[tid], days[days.size() - nd + q].day);
    for (size_t j = 0; j < nk; ++j)
      bars_of(day.asset(j), day.date, minute[q * nk + j]);
  };

  // 月分片自末往前一批 2T 片并行读, 每只日根都够 kDailyDays 即停; 分钟日并进首批
  std::vector<size_t> got(nk, 0);
  size_t lo = months.size(); // [lo, end) 已读
  for (size_t extra = nd; nk > 0 && lo > 0 && *std::min_element(got.begin(), got.end()) < kDailyDays; extra = 0) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    const size_t hi = lo, n = std::min(lo, 2 * T);
    lo -= n;
    pool_.tasks(static_cast<int>(n + extra), [&](int i, int tid) {
      const size_t u = static_cast<size_t>(i);
      u < n ? read_month(hi - 1 - u, static_cast<size_t>(tid)) : read_minute(u - n, static_cast<size_t>(tid));
    });
    for (size_t k = lo; k < hi; ++k)
      for (size_t j = 0; j < nk; ++j)
        got[j] += month[k * nk + j].size();
  }

  for (size_t j = 0; j < nk; ++j) {
    Kline kl;
    kl.last = last;
    kl.daily.reserve(got[j]);
    for (size_t k = lo; k < months.size(); ++k)
      kl.daily.insert(kl.daily.end(), month[k * nk + j].begin(), month[k * nk + j].end());
    std::sort(kl.daily.begin(), kl.daily.end(), [](const Candle &a, const Candle &b) { return a.date < b.date; });
    if (kl.daily.size() > kDailyDays)
      kl.daily.erase(kl.daily.begin(), kl.daily.end() - kDailyDays);
    for (size_t q = 0; q < nd; ++q)
      if (minute[q * nk + j].has.any())
        kl.minutes.push_back(minute[q * nk + j]);
    std::erase_if(keep_, [&](const auto &e) { return e.first == miss[j]; });
    keep_.emplace_back(miss[j], std::move(kl));
  }

  auto out = std::make_shared<Tape>();
  out->keys = req.keys;
  for (const uint32_t id : req.keys) {
    const auto it = cached(id);
    assert(it != keep_.end());
    out->list.push_back(it->second);
    std::rotate(it, it + 1, keep_.end());
  }
  if (keep_.size() > kKeep)
    keep_.erase(keep_.begin(), keep_.end() - kKeep);

  std::lock_guard<std::mutex> lock(x_.mutex);
  x_.gen = req.gen;
  x_.data = std::move(out);
  x_.publish();
  return true;
}

} // namespace GUI::Database
