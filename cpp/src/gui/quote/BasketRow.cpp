#include "gui/quote/BasketRow.hpp"

#include "features/Bar1mDay.hpp"
#include "fundamental/Scan.hpp"
#include "shared/Basket.hpp" // basket_table

#include <algorithm>
#include <cassert>
#include <cmath>
#include <span>
#include <utility>
#include <vector>

namespace GUI::Quote {

namespace {

float val(const FeatureRead::DayColumns &dc, size_t t, size_t i, size_t a) {
  const float x = static_cast<float>(dc.get(t, i, a));
  return std::isfinite(x) ? x : 0.f;
}

// L_1min 读的列 (bars_of 的列序): ts_valid / 开 / 高 / 低 / 收 / 额
enum MinCol : size_t { kMinTs,
                       kMinOpen,
                       kMinHigh,
                       kMinLow,
                       kMinClose,
                       kMinAmount,
                       kMinCols };
const std::vector<size_t> &minute_cols() {
  static const std::vector<size_t> c = {pool::bar_col(L_1min, "ts_valid"), pool::bar_col(L_1min, "open"), pool::bar_col(L_1min, "high"),
                                        pool::bar_col(L_1min, "low"), pool::bar_col(L_1min, "close"), pool::bar_col(L_1min, "amount", ts::TfId::Log)};
  return c;
}
const std::vector<size_t> &row_cols() {
  static const std::vector<size_t> c = pool::day_cols(pool::kQuoteCols);
  return c;
}

} // namespace

void row_alloc(FeatureRead::DayColumns &dc, size_t A) { dc.preallocate(A, L_1day, pool::kQuoteCols); }

void read_row(const FeatureRead &rd, misc::Ymd d, FeatureRead::DayColumns &dc, Row &r) {
  rd.load_day_columns(d, row_cols(), dc);
  const size_t A = dc.A;
  for (std::vector<float> &v : r.px)
    v.resize(A);
  r.amount.resize(A), r.pre.resize(A), r.trade.resize(A);
  for (size_t a = 0; a < A; ++a) {
    r.trade[a] = val(dc, 0, pool::kColTs, a) > 0.5f;
    for (size_t j = 0; j < 4; ++j)
      r.px[j][a] = val(dc, 0, pool::kColOpen + j, a);
    r.amount[a] = std::expm1(val(dc, 0, pool::kColAmount, a));
    r.pre[a] = val(dc, 0, pool::kColPre, a);
  }
}

void minute_alloc(FeatureRead::DayColumns &mc, size_t A) { mc.preallocate(A, L_1min, kMinCols); }

void read_minutes(const FeatureRead &rd, misc::Ymd d, FeatureRead::DayColumns &mc) { rd.load_day_columns(d, minute_cols(), mc); }

void members(const pool::Picks &pk, const PoolAxis &axis, misc::Ymd d, Members &m) {
  const pool::Tape &t = *pk.tape;
  const BasketTable &bt = basket_table();
  const ConceptTable &ct = concepts();
  const size_t A = axis.size(), NP = pool_count(), q = pool::src_of(pool::kRebalDay, t.dates, d);
  assert(t.axis == axis.hash && t.A == A && "选股表与库轴不符");
  std::vector<size_t> src(NP, SIZE_MAX); // [池] 源行
  std::vector<PoolMask> in(A, 0);        // [库轴] 当日所在的池
  for (PoolId p = 1; p < NP && q != SIZE_MAX; ++p) {
    src[p] = pk.src(p, d);
    if (src[p] != SIZE_MAX)
      for (const pool::Aid a : pk.row(p, src[p]))
        in[a] |= pool_bit(p);
  }
  m.clear();
  float om[pool::kWeights];
  for (size_t a = 0; a < A; ++a) {
    const bool base = in[a] & pool_bit(kBasketBase);
    const Fact f{in[a], base ? t.ind[q * A + a] : uint8_t{0}};
    assert((!base || f.ind > 0) && "母集成员的行业已知");
    PoolId at = kAllPool; // om 所依的池
    bt.each(f, ct.concepts_of(axis.global(a)), [&](size_t b) {
      if (const PoolId p = bt.base(b); p != at)
        t.omega(src[p], q, a, om), at = p;
      if (const float w = om[bt.weight(b)]; w > 0)
        m.add(static_cast<uint32_t>(b), w);
    });
    m.next(axis.global(a), q != SIZE_MAX ? t.mcap[q * A + a] : 0.f);
  }
}

void drift(Members &m, misc::Ymd prev, misc::Ymd d, std::span<const float> rel) {
  assert(m.id.empty() || m.id.back() < rel.size());
  const BasketTable &bt = basket_table();
  bool reb[pool::kRebals];
  for (int k = 0; k < pool::kRebals; ++k)
    reb[k] = pool::rebal_at(static_cast<pool::Rebal>(k), prev, d);
  m.scale([&](uint32_t a) { return rel[a]; });
  for (size_t k = 0; k < m.size(); ++k)
    for (Members::Weight &w : m.of(k))
      if (reb[bt.rebal(w.key)])
        w.w = static_cast<float>(pool::omega(bt.weight(w.key), m.cap[k], 1.0));
}

void bars_of(const FeatureRead::DayColumns &mc, size_t a, Bars &b) {
  b.has.reset();
  for (size_t t = 0; t < level_valid_rows(L_1min); ++t)
    if (val(mc, t, kMinTs, a) > 0.5f)
      if (const Cell c = cell(val(mc, t, kMinOpen, a), val(mc, t, kMinHigh, a), val(mc, t, kMinLow, a), val(mc, t, kMinClose, a), std::expm1(val(mc, t, kMinAmount, a)));
          traded(c))
        b.add(row_slot(t), c);
}

void each_daily(fundamental::Scratch &xs, const fundamental::Shard &sh, const fundamental::Ids &ids,
                const std::function<void(uint32_t, const Candle &)> &f) {
  static const fundamental::Spec &kDaily = fundamental::spec(kBar1d);
  static const std::vector<std::string> kCols = {"open", "high", "low", "close", "amount", "pre_close"};
  const fundamental::Rows r = xs.open(sh, kDaily, kCols, ids);
  misc::pq::Col c[6];
  for (size_t j = 0; j < kCols.size(); ++j)
    c[j] = r.col(kCols[j]);
  r.each([&](std::int64_t i, std::uint32_t id, misc::Ymd d) {
    f(id, {cell(c[0].f32(i), c[1].f32(i), c[2].f32(i), c[3].f32(i), c[4].f32(i)), d, c[5].f32(i)});
  });
}

void bars_of(const Bar1m *m, Bars &b) {
  b.has.reset();
  if (!m)
    return;
  for (size_t t = 0; t < TRADE_MINUTES_PER_DAY; ++t)
    if (const Cell c = cell(m[t].open, m[t].high, m[t].low, m[t].close, m[t].amount); traded(c))
      b.add(row_slot(t), c);
}

Leg LibDay::leg(size_t j, Cell &c, Bars &b) const {
  const float x = cur.px[3][j];
  c = cell(cur.px[0][j], cur.px[1][j], cur.px[2][j], x, cur.amount[j]);
  bars_of(mc, j, b);
  return {prev.pre[j], cur.trade[j] && x > 0 ? &c : nullptr, &b};
}

Leg FundDay::leg(size_t j, Cell &, Bars &b) const {
  const Candle &c = px[j];
  bars_of(bd.asset(j), b);
  return {c.ref, traded(c) ? &c : nullptr, &b};
}

Leg SinaDay::leg(size_t j, Cell &, Bars &) const {
  const Candle &c = px[j];
  return {c.ref, traded(c) ? &c : nullptr, &bars[j]};
}

Census census(const Members &m, size_t K) {
  Census t;
  t.members.assign(K, 0), t.weight.assign(K, 0.0), t.cap.assign(K, 0.0);
  std::vector<double> dot(K * K);
  std::vector<std::pair<size_t, Constituent>> cs; // (篮子, 成分)
  for (size_t r = 0; r < m.size(); ++r) {
    const std::span<const Members::Weight> ws = m.of(r);
    for (const Members::Weight &x : ws) {
      assert(x.key < K);
      ++t.members[x.key], t.weight[x.key] += x.w, t.cap[x.key] += m.cap[r];
      cs.push_back({x.key, {m.id[r], x.w}});
      for (const Members::Weight &y : ws)
        dot[x.key * K + y.key] += static_cast<double>(x.w) * y.w;
    }
  }
  std::stable_sort(cs.begin(), cs.end(), [](const auto &p, const auto &q) { return p.second.w > q.second.w; });
  t.cons = bucket(K, cs, [](const auto &p) { return p.first; }, [](const auto &p) { return p.second; });
  t.overlap.resize(K * K);
  for (size_t i = 0; i < K; ++i)
    for (size_t j = 0; j < K; ++j) {
      const double nn = dot[i * K + i] * dot[j * K + j];
      t.overlap[i * K + j] = nn > 0 ? static_cast<float>(dot[i * K + j] / std::sqrt(nn)) : 0.f;
    }
  return t;
}

} // namespace GUI::Quote
