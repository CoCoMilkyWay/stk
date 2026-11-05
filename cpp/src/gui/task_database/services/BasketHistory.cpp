#include "gui/task_database/services/BasketHistory.hpp"

#include "features/Storage/FeatureRead.hpp"
#include "misc/fs.hpp"
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_line / basket_count / cut_of
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <string_view>

namespace GUI::Database {

namespace {

constexpr uint64_t kMagic = 0x314B534142544B53ULL; // "SKTBASK1" 小端
constexpr const char *kFile = "/baskets.bin";

// all Bar 字段表里 code 的列下标 (落盘变换须是 tf)
size_t col_of(size_t lvl, std::string_view code, ts::TfId tf) {
  const LevelInfo &li = level_info(TierId::Bar, lvl);
  for (size_t i = 0; i < li.field_count; ++i)
    if (code == li.fields[i].code) {
      assert(li.fields[i].ts_tf == tf && "all.bar: 列的落盘变换与读法不符");
      return i;
    }
  assert(false && "all.bar: 字段表里没有这一列");
  return 0;
}

float val(const FeatureRead::DayColumns &dc, size_t t, size_t i, size_t a) {
  const float x = static_cast<float>(dc.get(t, i, a));
  return std::isfinite(x) ? x : 0.f;
}

// 一日的 L_1day 收盘行 (A 轴); 缺 / 非有限 → 0
enum DayCol : size_t { kCs,
                       kTs,
                       kOpen,
                       kHigh,
                       kLow,
                       kClose,
                       kAmount,
                       kPre,
                       kShares,
                       kInd,
                       kPools }; // 子池 p ∈ [1, NP) 的池列 = kPools + p − 1
struct Row {
  std::vector<float> px[4], amount, pre, shares; // 开 高 低 收
  std::vector<uint8_t> live, trade, ind;
  std::vector<uint8_t> pool; // [(p − 1) · A + a]
};

void read_row(const FeatureRead::DayColumns &dc, size_t A, size_t NP, Row &r) {
  for (std::vector<float> &v : r.px)
    v.resize(A);
  r.amount.resize(A), r.pre.resize(A), r.shares.resize(A);
  r.live.resize(A), r.trade.resize(A), r.ind.resize(A);
  r.pool.resize((NP - 1) * A);
  for (size_t a = 0; a < A; ++a) {
    r.live[a] = val(dc, 0, kCs, a) > 0.5f;
    r.trade[a] = val(dc, 0, kTs, a) > 0.5f;
    for (size_t j = 0; j < 4; ++j)
      r.px[j][a] = val(dc, 0, kOpen + j, a);
    r.amount[a] = std::expm1(val(dc, 0, kAmount, a));
    r.pre[a] = val(dc, 0, kPre, a);
    r.shares[a] = val(dc, 0, kShares, a);
    const float k = val(dc, 0, kInd, a);
    r.ind[a] = k >= 1.f && k < static_cast<float>(kAllCuts) ? static_cast<uint8_t>(k + 0.5f) : 0;
    for (size_t p = 1; p < NP; ++p)
      r.pool[(p - 1) * A + a] = val(dc, 0, kPools + p - 1, a) > 0.5f;
  }
}

// 收盘行 r 定次日成员表 (下标 = 库轴): 所属切片各进等权 (1) / 加权 (收盘市值; 缺股本不进)
void members(const Row &r, size_t A, size_t NP, Members &m) {
  m.clear();
  for (size_t a = 0; a < A; ++a) {
    const float c = r.px[3][a], cap = c * r.shares[a];
    const auto put = [&](size_t cut) {
      m.add(static_cast<uint32_t>(basket_line(cut, kWeightEq)), 1.f);
      if (cap > 0)
        m.add(static_cast<uint32_t>(basket_line(cut, kWeightCap)), cap);
    };
    if (c > 0 && r.live[a]) {
      put(0);
      if (r.ind[a])
        put(r.ind[a]);
    }
    for (PoolId p = 1; c > 0 && p < NP; ++p)
      if (r.pool[(p - 1) * A + a])
        put(cut_of(p));
    m.next();
  }
}

// 一只一日的 L_1min → 分钟根 (列序同 mcols); 无成交的行不收
void read_bars(const FeatureRead::DayColumns &mc, size_t a, misc::Ymd d, Bars &b) {
  b.date = d;
  b.has.reset();
  for (size_t t = 0; t < level_valid_rows(L_1min); ++t) {
    const float x = val(mc, t, 4, a);
    if (val(mc, t, 0, a) > 0.5f && x > 0)
      b.add(row_slot(t), cell(val(mc, t, 1, a), val(mc, t, 2, a), val(mc, t, 3, a), x, 0, std::expm1(val(mc, t, 5, a))));
  }
}

// 定长元素的序列: 长度 + 逐个
template <class Ar, class T>
void items(Ar &ar, std::vector<T> &v) {
  uint64_t n = v.size();
  ar(n);
  if constexpr (Ar::kLoad)
    v.resize(n);
  for (T &x : v)
    ar(x);
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
  for (Kline &k : list) {
    ar(k.last);
    items(ar, k.daily);
    items(ar, k.minutes);
  }
}

bool BasketHistory::run(HistoryRequest &req) {
  const FeatureLib lib = load_lib(cfg_, kAllBar);
  if (!lib.ready())
    return publish(req, "指数 / 行业由 all.bar 特征库合成: 先在 Features → Compute 算好 all 池的 Bar 库.");
  FeatureRead rd(lib, &cancel_);
  std::vector<misc::Ymd> dates; // 已落盘日 (清单日期轴可能还在算)
  for (const misc::Ymd d : lib.dates)
    if (rd.has_date(d))
      dates.push_back(d);
  if (dates.size() < 2) // 首日只给成员 / 基准, 次日起才有根
    return publish(req, "all.bar 特征库落盘不足 2 日.");

  const Book::Head head{kMagic, lib.axis.hash, static_cast<uint32_t>(lib.axis.size()), static_cast<uint32_t>(basket_count()),
                        sizeof(Candle), sizeof(Bars)};
  const auto fits = [&](const Book &b) {
    return b.head == head && !b.dates.empty() && b.dates.size() <= dates.size() && std::equal(b.dates.begin(), b.dates.end(), dates.begin());
  };
  if (!fits(book_)) {
    book_ = {};
    if (std::ifstream f(lib.dir + kFile, std::ios::binary); f) {
      const std::vector<char> blob((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
      misc::snap::Load ar(std::as_bytes(std::span(blob)));
      ar(book_.head);
      if (book_.head == head) {
        book_.snap(ar);
        assert(ar.done() && "baskets.bin: 文件尾有多余字节");
      }
    }
    if (!fits(book_))
      book_ = {head, {dates.front()}, std::vector<double>(head.baskets, kBase), std::vector<Kline>(head.baskets)};
  }

  const size_t done = book_.dates.size();
  Members mem;
  if (!extend(lib, rd, dates, mem)) {
    if (rd.stale())
      book_ = {};
    return false;
  }
  if (book_.dates.size() > done) {
    std::vector<std::byte> blob;
    misc::snap::Save ar(blob);
    ar(book_.head);
    book_.snap(ar);
    misc::atomic_write(lib.dir + kFile, reinterpret_cast<const char *>(blob.data()), blob.size());
  }

  const PoolAxis &market = live_.axis();
  Members live; // 下标 = 全市场
  for (size_t q = 0; q < market.size(); ++q) {
    if (const size_t a = lib.axis.sub_of(market.global(q)); a < lib.axis.size())
      for (const Members::Weight &w : mem.of(a))
        live.add(w.basket, w.w);
    live.next();
  }
  live_.bind(std::move(live), book_.level);
  return publish(req, nullptr);
}

bool BasketHistory::extend(const FeatureLib &lib, const FeatureRead &rd, const std::vector<misc::Ymd> &dates, Members &mem) {
  const size_t A = lib.axis.size(), NP = pool_count(), N = dates.size(), NB = basket_count(), i_lo = book_.dates.size();
  std::vector<size_t> dcols = {col_of(L_1day, "cs_valid", ts::TfId::None), col_of(L_1day, "ts_valid", ts::TfId::None),
                               col_of(L_1day, "open_day", ts::TfId::None), col_of(L_1day, "high_day", ts::TfId::None),
                               col_of(L_1day, "low_day", ts::TfId::None), col_of(L_1day, "close_day", ts::TfId::None),
                               col_of(L_1day, "amount_day", ts::TfId::Log), col_of(L_1day, "pre_close", ts::TfId::None),
                               col_of(L_1day, "total_shares", ts::TfId::None), col_of(L_1day, "ind_l1", ts::TfId::None)};
  assert(dcols.size() == kPools);
  for (PoolId p = 1; p < NP; ++p)
    dcols.push_back(pool_def(p).pool_col);
  const std::vector<size_t> mcols = {col_of(L_1min, "ts_valid", ts::TfId::None), col_of(L_1min, "open", ts::TfId::None),
                                     col_of(L_1min, "high", ts::TfId::None), col_of(L_1min, "low", ts::TfId::None),
                                     col_of(L_1min, "close", ts::TfId::None), col_of(L_1min, "amount", ts::TfId::Log)};
  const size_t T = static_cast<size_t>(pool_.threads()), batch = 2 * T;
  std::vector<FeatureRead::DayColumns> dc(T); // 按线程
  for (FeatureRead::DayColumns &c : dc)
    c.preallocate(A, L_1day, dcols.size());
  FeatureRead::DayColumns mc;
  mc.preallocate(A, L_1min, mcols.size());

  std::vector<Row> rows(batch + 1); // [0] = 上一批末日, [1 + k] = 本批第 k 日
  const auto read = [&](size_t i, Row &r, size_t tid) {
    rd.load_day_columns(dates[i], dcols, dc[tid]);
    read_row(dc[tid], A, NP, r);
    x_.publish(1);
  };
  std::vector<Day> day(NB);
  Fold fold(NB);
  Bars bars; // 一只一日的分钟根
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
      const Row &prev = rows[k], &cur = rows[1 + k];
      members(prev, A, NP, mem);
      const bool minutes = i + kMinuteDays >= N;
      if (minutes) {
        rd.load_day_columns(d, mcols, mc);
        if (rd.stale())
          return false;
      }
      fold.open(minutes ? kSlots : 0);
      for (size_t a = 0; a < A; ++a) {
        const std::span<const Members::Weight> ws = mem.of(a);
        if (ws.empty())
          continue;
        const float x = cur.px[3][a];
        const Cell c = cell(cur.px[0][a], cur.px[1][a], cur.px[2][a], x, 0, cur.amount[a]);
        if (minutes)
          read_bars(mc, a, d, bars);
        fold.add({prev.pre[a], cur.trade[a] && x > 0 ? &c : nullptr, minutes ? &bars : nullptr}, ws);
      }
      fold.finish(d, dates[i - 1], book_.level, day);
      for (size_t b = 0; b < NB; ++b) {
        if (!day[b].candle.date)
          continue;
        Kline &kl = book_.list[b];
        kl.daily.push_back(day[b].candle);
        if (minutes)
          kl.minutes.push_back(day[b].bars);
        book_.level[b] = day[b].candle.close;
      }
      book_.dates.push_back(d);
    }
    std::swap(rows[0], rows[n]);
  }

  const misc::Ymd d_lo = dates[N > kDailyDays ? N - kDailyDays : 0], m_lo = dates[N > kMinuteDays ? N - kMinuteDays : 0];
  for (Kline &kl : book_.list) {
    std::erase_if(kl.daily, [&](const Candle &c) { return c.date < d_lo; });
    std::erase_if(kl.minutes, [&](const Bars &b) { return b.date < m_lo; });
    kl.last = dates.back();
  }
  members(rows[0], A, NP, mem);
  return true;
}

bool BasketHistory::publish(const HistoryRequest &req, const char *lack) {
  auto out = std::make_shared<Tape>();
  out->keys = req.keys;
  out->lack = lack;
  if (!lack)
    for (const uint32_t b : req.keys) {
      assert(b < book_.list.size());
      out->list.push_back(book_.list[b]);
    }
  std::lock_guard<std::mutex> lock(x_.mutex);
  x_.gen = req.gen;
  x_.data = std::move(out);
  x_.publish();
  return true;
}

} // namespace GUI::Database
