#include "gui/quote/services/BasketHistory.hpp"

#include "disk/Disk.hpp"
#include "features/Bar1mDay.hpp"
#include "features/Panel/PoolSelect.hpp"
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "fundamental/AssetAxis.hpp"
#include "fundamental/Scan.hpp"
#include "gui/quote/BasketRow.hpp"
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_table

#include <algorithm>
#include <cassert>
#include <memory>
#include <utility>

namespace GUI::Quote {

namespace {

constexpr uint64_t kMagic = 0x314B534142544B53ULL; // "SKTBASK1" 小端

using misc::snap::items;

// Book 的一条改前先独占 (发布还持有着就复制一份)
Kline &own(std::shared_ptr<Kline> &p) {
  if (p.use_count() > 1)
    p = std::make_shared<Kline>(*p);
  return *p;
}

// 一日合成的各篮子接到各自的根上 (当日无根的篮子不动): kl(b) = 篮子 b 的 Kline; level [篮子] 推到当日收盘
template <class Kl>
void append(const std::vector<Day> &day, bool minutes, std::vector<double> &level, Kl &&kl) {
  for (size_t b = 0; b < day.size(); ++b) {
    const Day &y = day[b];
    if (!y.candle.date)
      continue;
    Kline &k = kl(b);
    k.daily.push_back(y.candle);
    if (minutes)
      k.minutes.push_back(y.bars);
    level[b] = y.candle.close;
  }
}

} // namespace

template <class Ar>
void BasketHistory::Book::snap(Ar &ar) {
  items(ar, dates);
  items(ar, level);
  uint64_t n = list.size();
  ar(n);
  if constexpr (Ar::kLoad)
    list.resize(n);
  for (std::shared_ptr<Kline> &p : list) {
    if constexpr (Ar::kLoad)
      p = std::make_shared<Kline>();
    Kline &k = *p;
    ar(k.last);
    items(ar, k.daily);
    items(ar, k.minutes);
  }
}

bool BasketHistory::run(HistoryRequest &req) {
  const pool::AllBar ab = pool::all_bar("指数 / 行业由 all.bar 特征库合成");
  if (!ab.lack.empty())
    return publish(req, ab.lack);
  const FeatureLib &lib = ab.lib;
  FeatureRead rd(lib, &cancel_);
  const std::shared_ptr<const pool::Picks> pk = pool::picks(lib, &cancel_);
  if (!pk)
    return false;
  const std::vector<misc::Ymd> &dates = pk->tape->dates;
  const BasketTable &bt = basket_table();
  const Book::Head head{kMagic, lib.axis.hash, bt.hash(), static_cast<uint32_t>(lib.axis.size()), static_cast<uint32_t>(bt.size()), sizeof(Candle), sizeof(Bars)};
  const auto fits = [&](const Book &b) { return b.head == head && pool::extends(b.dates, dates); };
  if (!fits(book_)) {
    book_ = {};
    if (disk::exists(disk::LibBaskets, lib_key(lib.id))) {
      const std::string blob = disk::read(disk::LibBaskets, lib_key(lib.id));
      misc::snap::Load ar(std::as_bytes(std::span(blob)));
      ar(book_.head);
      if (book_.head == head) {
        book_.snap(ar);
        assert(ar.done() && "baskets.bin: 文件尾有多余字节");
      }
    }
    if (!fits(book_)) {
      book_ = {head, {dates.front()}, std::vector<double>(head.baskets, kBase), {}};
      for (uint32_t b = 0; b < head.baskets; ++b)
        book_.list.push_back(std::make_shared<Kline>());
    }
  }

  const size_t done = book_.dates.size();
  Members mem;
  if (!extend(lib, rd, *pk, mem)) {
    if (rd.stale())
      book_ = {};
    return false;
  }
  if (!fill(lib, mem))
    return false;
  if (book_.dates.size() > done) {
    std::vector<std::byte> blob;
    misc::snap::Save ar(blob);
    ar(book_.head);
    book_.snap(ar);
    disk::write(disk::LibBaskets, lib_key(lib.id), {reinterpret_cast<const char *>(blob.data()), blob.size()});
  }

  const PoolAxis &market = live_.axis();
  std::vector<uint32_t> row(lib.axis.size(), UINT32_MAX); // [库轴] mem 的行
  for (size_t k = 0; k < mem.size(); ++k)
    row[mem.id[k]] = static_cast<uint32_t>(k);
  Members live; // 资产 = 全市场
  for (size_t q = 0; q < market.size(); ++q) {
    if (const size_t a = lib.axis.sub_of(market.global(q)); a < lib.axis.size() && row[a] != UINT32_MAX)
      for (const Members::Weight &w : mem.of(row[a]))
        live.add(w.key, w.w);
    live.next(static_cast<uint32_t>(q));
  }
  live_.bind(std::move(live), tail_.level);
  return publish(req, {}, &lib.axis);
}

bool BasketHistory::fill(const FeatureLib &lib, Members &mem) {
  const fundamental::Catalog cat;
  const misc::Ymd from = book_.dates.back(), to = fund_last(cat);
  const std::span<const misc::Ymd> trading = live_.trading();
  const auto lo = std::upper_bound(trading.begin(), trading.end(), from);
  const std::vector<misc::Ymd> dates(lo, std::max(lo, std::upper_bound(trading.begin(), trading.end(), to)));
  if (tail_.from == from && tail_.dates == dates && tail_.base == book_.level) {
    mem = tail_.mem;
    return true;
  }

  const size_t A = lib.axis.size(), NB = book_.level.size(), N = dates.size();
  tail_ = {from, book_.level, dates, book_.level, std::vector<Kline>(NB), {}};
  fundamental::Ids ids(fundamental::Ids::Cover::Part);
  for (size_t a = 0; a < A; ++a)
    ids.add(asset_axis().code(lib.axis.global(a)));
  fundamental::Scratch xs;
  std::vector<Candle> px(N * A); // [补日][库轴]; 无行 → 收 0
  for (const fundamental::Shard &sh : cat.shards(kBar1d)) {
    if (N == 0 || sh.ym < misc::ym_of(dates.front()) || sh.ym > misc::ym_of(dates.back()))
      continue;
    each_daily(xs, sh, ids, [&](uint32_t a, const Candle &c) {
      const auto it = std::lower_bound(dates.begin(), dates.end(), c.date);
      if (it != dates.end() && *it == c.date)
        px[static_cast<size_t>(it - dates.begin()) * A + a] = c;
    });
  }

  Lane L(NB);
  L.mem = std::move(mem);
  Bar1mDay bd;
  std::vector<float> rel(A); // [库轴] 当日 收 / 前收 (无成交 1)
  for (size_t k = 0; k < N; ++k) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    const misc::Ymd d = dates[k];
    const std::span<const Candle> row(px.data() + k * A, A);
    bd.load(cat, ids, xs, d);
    L.run(FundDay{d, row, bd}, k ? dates[k - 1] : from, tail_.level);
    append(L.day, true, tail_.level, [&](size_t b) -> Kline & { return tail_.list[b]; });
    for (size_t a = 0; a < A; ++a)
      rel[a] = row[a].ref > 0 && row[a].close > 0 ? static_cast<float>(row[a].close / row[a].ref) : 1.f;
    const auto next = std::upper_bound(trading.begin(), trading.end(), d);
    assert(next != trading.end() && "交易日历缺补日的次交易日");
    drift(L.mem, d, *next, rel);
  }
  mem = std::move(L.mem);
  tail_.mem = mem;
  return true;
}

bool BasketHistory::extend(const FeatureLib &lib, const FeatureRead &rd, const pool::Picks &pk, Members &mem) {
  const std::vector<misc::Ymd> &dates = pk.tape->dates;
  const size_t A = lib.axis.size(), N = dates.size(), i_lo = book_.dates.size();
  const size_t T = static_cast<size_t>(pool_.threads()), batch = 2 * T;
  std::vector<FeatureRead::DayColumns> dc(T); // 按线程
  for (FeatureRead::DayColumns &c : dc)
    row_alloc(c, A);
  FeatureRead::DayColumns mc;
  minute_alloc(mc, A);
  Lane L(book_.level.size());

  std::vector<Row> rows(batch + 1); // [0] = 上一批末日, [1 + k] = 本批第 k 日
  const auto read = [&](size_t i, Row &r, size_t tid) {
    read_row(rd, dates[i], dc[tid], r);
    x_.publish(1);
  };
  x_.begin(Phase::Loading, N - i_lo + 1);
  read(i_lo - 1, rows[0], 0);
  for (size_t i0 = i_lo; i0 < N; i0 += batch) {
    if (cancel_.load(std::memory_order_relaxed) || rd.stale())
      return false;
    const size_t n = std::min(batch, N - i0);
    pool_.tasks(static_cast<int>(n), [&](int k, int tid) { read(i0 + static_cast<size_t>(k), rows[1 + static_cast<size_t>(k)], static_cast<size_t>(tid)); });
    if (rd.stale())
      return false;
    for (size_t k = 0; k < n; ++k) {
      const size_t i = i0 + k;
      const misc::Ymd d = dates[i];
      const bool minutes = i + kMinuteDays >= N;
      if (minutes) {
        read_minutes(rd, d, mc);
        if (rd.stale())
          return false;
      }
      members(pk, lib.axis, d, L.mem);
      L.run(LibDay{d, rows[k], rows[1 + k], minutes ? &mc : nullptr}, dates[i - 1], book_.level);
      append(L.day, minutes, book_.level, [&](size_t b) -> Kline & { return own(book_.list[b]); });
      book_.dates.push_back(d);
    }
    std::swap(rows[0], rows[n]);
  }

  const misc::Ymd d_lo = dates[N > kDailyDays ? N - kDailyDays : 0], m_lo = dates[N > kMinuteDays ? N - kMinuteDays : 0];
  if (i_lo < N) // 无新日 → 窗与末日不变, 不动 (免得复制发布还持有着的)
    for (std::shared_ptr<Kline> &p : book_.list) {
      Kline &kl = own(p);
      std::erase_if(kl.daily, [&](const Candle &c) { return c.date < d_lo; });
      std::erase_if(kl.minutes, [&](const Bars &b) { return b.date < m_lo; });
      kl.last = dates.back();
    }
  const std::span<const misc::Ymd> trading = live_.trading();
  const auto next = std::upper_bound(trading.begin(), trading.end(), dates.back());
  assert(next != trading.end() && "交易日历缺库末日的次交易日");
  members(pk, lib.axis, *next, mem);
  return true;
}

bool BasketHistory::publish(const HistoryRequest &req, const std::string &lack, const PoolAxis *axis) {
  auto out = std::make_shared<Tape>();
  out->keys = req.keys;
  out->lack = lack;
  if (lack.empty()) {
    assert(axis && (tail_.mem.id.empty() || tail_.mem.id.back() < axis->size()));
    // 窗按 Book 落盘日 + 补日截: 自末往前第 n 日
    const size_t nb = book_.dates.size(), nt = tail_.dates.size();
    const auto nth_last = [&](size_t n) { return n <= nt ? tail_.dates[nt - n] : book_.dates[nb - std::min(nb, n - nt)]; };
    const misc::Ymd d_lo = nth_last(kDailyDays), m_lo = nth_last(kMinuteDays);
    out->basis = book_.dates.back();
    census(tail_.mem, *axis, req.keys, *out);
    for (const uint32_t b : req.keys) {
      assert(b < book_.list.size() && b < tail_.list.size());
      if (nt == 0) { // Book 已按同一个窗截过
        out->list.push_back(book_.list[b]);
        continue;
      }
      auto kl = std::make_shared<Kline>(*book_.list[b]);
      const Kline &t = tail_.list[b];
      kl->daily.insert(kl->daily.end(), t.daily.begin(), t.daily.end());
      kl->minutes.insert(kl->minutes.end(), t.minutes.begin(), t.minutes.end());
      std::erase_if(kl->daily, [&](const Candle &c) { return c.date < d_lo; });
      std::erase_if(kl->minutes, [&](const Bars &m) { return m.date < m_lo; });
      kl->last = tail_.dates.back();
      out->list.push_back(std::move(kl));
    }
  }
  std::lock_guard<std::mutex> lock(x_.mutex);
  x_.gen = req.gen;
  x_.data = std::move(out);
  x_.publish();
  return true;
}

} // namespace GUI::Quote