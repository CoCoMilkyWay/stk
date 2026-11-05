#include "gui/task_database/services/BasketHistory.hpp"

#include "features/Bar1mDay.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "fundamental/Scan.hpp"
#include "misc/fs.hpp"
#include "misc/snap.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/Basket.hpp" // basket_line / basket_count / cut_of / concept_cut / weight_of
#include "shared/FeatureLib.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <string_view>
#include <utility>

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

// 收盘行 r 定次日成员表 (下标 = 库轴): 所属切片各进等权 (1) / 加权 (收盘市值; 缺股本不进); 概念随全A
void members(const Row &r, const PoolAxis &axis, size_t NP, Members &m) {
  const ConceptTable &ct = concepts();
  const size_t A = axis.size();
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
      for (const uint16_t k : ct.concepts_of(axis.global(a)))
        put(concept_cut(k));
    }
    for (PoolId p = 1; c > 0 && p < NP; ++p)
      if (r.pool[(p - 1) * A + a])
        put(cut_of(p));
    m.next();
  }
}

// 一只一日的 L_1min (列序同 mcols) → 分钟根: 无成交的行不收 (与 bar1m 的 bars_of 同一并根)
void bars_of(const FeatureRead::DayColumns &mc, size_t a, misc::Ymd d, Bars &b) {
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

// Book 的一条改前先独占 (发布还持有着就复制一份)
Kline &own(std::shared_ptr<Kline> &p) {
  if (p.use_count() > 1)
    p = std::make_shared<Kline>(*p);
  return *p;
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

  const Book::Head head{kMagic, lib.axis.hash, concepts().hash, static_cast<uint32_t>(lib.axis.size()), static_cast<uint32_t>(basket_count()),
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
    if (!fits(book_)) {
      book_ = {head, {dates.front()}, std::vector<double>(head.baskets, kBase), {}};
      for (uint32_t b = 0; b < head.baskets; ++b)
        book_.list.push_back(std::make_shared<Kline>());
    }
  }

  const size_t done = book_.dates.size();
  Members mem;
  if (!extend(lib, rd, dates, mem)) {
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
  live_.bind(std::move(live), tail_.level);
  return publish(req, nullptr, &lib.axis);
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

  const size_t A = lib.axis.size(), NB = basket_count(), N = dates.size();
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

  Fold fold(NB);
  std::vector<Day> day(NB);
  Bar1mDay bd;
  Bars bars; // 一只一日的分钟根
  for (size_t k = 0; k < N; ++k) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    const misc::Ymd d = dates[k];
    bd.load(cat, ids, xs, d);
    fold.open(kSlots);
    for (size_t a = 0; a < A; ++a) {
      const std::span<const Members::Weight> ws = mem.of(a);
      if (ws.empty())
        continue;
      const Candle &c = px[k * A + a];
      bars_of(bd.asset(a), d, bars);
      fold.add({c.ref, traded(c) ? &c : nullptr, &bars}, ws);
    }
    fold.finish(d, k ? dates[k - 1] : from, tail_.level, day);
    for (size_t b = 0; b < NB; ++b) {
      if (!day[b].candle.date)
        continue;
      tail_.list[b].daily.push_back(day[b].candle);
      tail_.list[b].minutes.push_back(day[b].bars);
      tail_.level[b] = day[b].candle.close;
    }
    for (size_t a = 0; a < A; ++a) {
      const Candle &c = px[k * A + a];
      if (!(c.ref > 0 && c.close > 0))
        continue;
      for (Members::Weight &w : mem.of(a))
        if (weight_of(w.basket) == kWeightCap)
          w.w *= c.close / c.ref;
    }
  }
  tail_.mem = mem;
  return true;
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
      members(prev, lib.axis, NP, mem);
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
          bars_of(mc, a, d, bars);
        fold.add({prev.pre[a], cur.trade[a] && x > 0 ? &c : nullptr, minutes ? &bars : nullptr}, ws);
      }
      fold.finish(d, dates[i - 1], book_.level, day);
      for (size_t b = 0; b < NB; ++b) {
        if (!day[b].candle.date)
          continue;
        Kline &kl = own(book_.list[b]);
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
  if (i_lo < N) // 无新日 → 窗与末日不变, 不动 (免得复制发布还持有着的)
    for (std::shared_ptr<Kline> &p : book_.list) {
      Kline &kl = own(p);
      std::erase_if(kl.daily, [&](const Candle &c) { return c.date < d_lo; });
      std::erase_if(kl.minutes, [&](const Bars &b) { return b.date < m_lo; });
      kl.last = dates.back();
    }
  members(rows[0], lib.axis, NP, mem);
  return true;
}

bool BasketHistory::publish(const HistoryRequest &req, const char *lack, const PoolAxis *axis) {
  auto out = std::make_shared<Tape>();
  out->keys = req.keys;
  out->lack = lack;
  if (!lack) {
    assert(axis && axis->size() == tail_.mem.assets());
    // 窗按 Book 落盘日 + 补日截: 自末往前第 n 日
    const size_t nb = book_.dates.size(), nt = tail_.dates.size();
    const auto nth_last = [&](size_t n) { return n <= nt ? tail_.dates[nt - n] : book_.dates[nb - std::min(nb, n - nt)]; };
    const misc::Ymd d_lo = nth_last(kDailyDays), m_lo = nth_last(kMinuteDays);
    out->basis = book_.dates.back();
    std::vector<uint32_t> n(basket_count());
    std::vector<double> w(basket_count());
    for (const Members::Weight &x : tail_.mem.ws)
      ++n[x.basket], w[x.basket] += x.w;
    const size_t K = req.keys.size();
    std::vector<size_t> at(basket_count(), K); // 篮子线 → keys 下标 (不在 = K)
    for (size_t k = 0; k < K; ++k)
      at[req.keys[k]] = k;
    std::vector<double> dot(K * K);
    std::vector<std::pair<size_t, double>> on;      // 一只所属的在请求里的篮子 (keys 下标, 权)
    std::vector<std::pair<size_t, Constituent>> cs; // (keys 下标, 成分)
    for (size_t a = 0; a < tail_.mem.assets(); ++a) {
      on.clear();
      for (const Members::Weight &x : tail_.mem.of(a))
        if (at[x.basket] < K) {
          on.emplace_back(at[x.basket], x.w);
          cs.push_back({at[x.basket], {axis->global(a), x.w}});
        }
      for (const auto &[i, wi] : on)
        for (const auto &[j, wj] : on)
          dot[i * K + j] += wi * wj;
    }
    std::stable_sort(cs.begin(), cs.end(), [](const auto &p, const auto &q) { return p.second.w > q.second.w; });
    out->cons = bucket(K, cs, [](const auto &p) { return p.first; }, [](const auto &p) { return p.second; });
    out->overlap.resize(K * K);
    for (size_t i = 0; i < K; ++i)
      for (size_t j = 0; j < K; ++j) {
        const double nn = dot[i * K + i] * dot[j * K + j];
        out->overlap[i * K + j] = nn > 0 ? static_cast<float>(dot[i * K + j] / std::sqrt(nn)) : 0.f;
      }
    for (const uint32_t b : req.keys) {
      out->members.push_back(n[b]), out->weight.push_back(w[b]);
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

} // namespace GUI::Database
