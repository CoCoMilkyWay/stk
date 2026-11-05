#include "gui/task_database/services/BasketHistory.hpp"

#include "features/Method/Fundamental.hpp" // fund::SW2021_L1_NAMES
#include "features/Storage/FeatureRead.hpp"
#include "shared/AssetAxis.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <string_view>

namespace GUI::Database {

namespace {

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
    r.ind[a] = k >= 1.f && k < static_cast<float>(fund::SW2021_L1_COUNT) ? static_cast<uint8_t>(k + 0.5f) : 0;
    for (size_t p = 1; p < NP; ++p)
      r.pool[(p - 1) * A + a] = val(dc, 0, kPools + p - 1, a) > 0.5f;
  }
}

// 收盘行 r 定次日成员表 (下标 = 池轴; 篮子序见 pool_basket / industry_basket)
void members(const Row &r, size_t A, size_t NP, Members &m) {
  m.clear();
  for (size_t a = 0; a < A; ++a) {
    const float c = r.px[3][a], cap = c * r.shares[a];
    if (c > 0 && r.live[a]) {
      m.add(static_cast<uint32_t>(pool_basket(kAllPool, false)), 1.f);
      if (cap > 0) {
        m.add(static_cast<uint32_t>(pool_basket(kAllPool, true)), cap);
        if (r.ind[a])
          m.add(static_cast<uint32_t>(industry_basket(r.ind[a])), cap);
      }
    }
    for (PoolId p = 1; c > 0 && p < NP; ++p)
      if (r.pool[(p - 1) * A + a])
        m.add(static_cast<uint32_t>(pool_basket(p, false)), 1.f);
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

} // namespace

bool BasketHistory::run(BasketRequest &req) {
  const FeatureLib &lib = req.lib;
  assert(lib.ready() && lib.id == kAllBar);
  FeatureRead rd(lib, &cancel_);
  std::vector<misc::Ymd> dates;
  for (auto it = lib.dates.rbegin(); it != lib.dates.rend() && dates.size() < kDailyDays + 1; ++it)
    if (rd.has_date(*it))
      dates.push_back(*it);
  std::reverse(dates.begin(), dates.end());
  auto out = std::make_shared<Baskets>();
  const auto publish = [&] {
    std::lock_guard<std::mutex> lock(x_.mutex);
    x_.gen = req.gen;
    x_.data = std::move(out);
    x_.publish();
    return true;
  };
  if (dates.size() < 2) // 首日只给成员 / 基准, 次日起才有根: 落盘 (清单日期轴可能还在算) 不足 2 日 → 空发布
    return publish();

  const size_t A = lib.axis.size(), NP = pool_count(), N = dates.size(), NB = NP + fund::SW2021_L1_COUNT;
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
  FeatureRead::DayColumns dc, mc;
  dc.preallocate(A, L_1day, dcols.size());
  mc.preallocate(A, L_1min, mcols.size());

  out->list.resize(NB);
  out->list[pool_basket(kAllPool, false)].name = "全A等权";
  out->list[pool_basket(kAllPool, true)].name = "全A加权";
  for (PoolId p = 1; p < NP; ++p)
    out->list[pool_basket(p, false)].name = pool_def(p).name_cn;
  for (size_t k = 1; k < fund::SW2021_L1_COUNT; ++k) {
    Basket &b = out->list[industry_basket(k)];
    b.name = fund::SW2021_L1_NAMES[k];
    b.industry = true;
  }
  for (Basket &b : out->list)
    b.kline.last = dates.back();

  Row rows[2]; // 第 i 日 = rows[i % 2]
  Members mem;
  std::vector<double> level(NB, kBase);
  std::vector<Day> day(NB);
  Fold fold(NB);
  Bars bars; // 一只一日的分钟根
  x_.begin(Phase::Loading, N);
  for (size_t i = 0; i < N; ++i) {
    if (cancel_.load(std::memory_order_relaxed))
      return false;
    const misc::Ymd d = dates[i];
    rd.load_day_columns(d, dcols, dc);
    if (rd.stale())
      return false;
    Row &cur = rows[i % 2];
    read_row(dc, A, NP, cur);
    x_.publish(1);
    if (i < 1)
      continue;
    const Row &prev = rows[(i - 1) % 2];
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
      const Cell k = cell(cur.px[0][a], cur.px[1][a], cur.px[2][a], x, 0, cur.amount[a]);
      if (minutes)
        read_bars(mc, a, d, bars);
      fold.add({prev.pre[a], cur.trade[a] && x > 0 ? &k : nullptr, minutes ? &bars : nullptr}, ws);
    }
    fold.finish(d, dates[i - 1], level, day);
    for (size_t b = 0; b < NB; ++b) {
      if (!day[b].date)
        continue;
      Kline &kl = out->list[b].kline;
      kl.daily.push_back(day[b].candle);
      if (minutes)
        kl.minutes.push_back(day[b].bars);
      level[b] = day[b].candle.close;
    }
  }

  std::vector<uint32_t> at(live_.size(), ~0u); // 全市场 → 池轴
  for (size_t a = 0; a < A; ++a)
    if (const uint32_t q = live_.find(asset_axis().code(lib.axis.global(a))); q != ~0u) {
      assert(at[q] == ~0u && "池轴两只对到新浪同一只");
      at[q] = static_cast<uint32_t>(a);
    }
  members(rows[(N - 1) % 2], A, NP, mem);
  Members live;
  for (const uint32_t a : at) {
    if (a != ~0u)
      for (const Members::Weight &w : mem.of(a)) {
        live.add(w.basket, w.w);
        ++out->list[w.basket].size;
      }
    live.next();
  }
  live_.bind(std::move(live), std::move(level));
  return publish();
}

} // namespace GUI::Database
