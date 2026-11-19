#include "gui/quote/BasketRow.hpp"

#include "features/Bar1mDay.hpp"
#include "features/Panel/PoolSelect.hpp"
#include "fundamental/Scan.hpp"

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

void prefetch_day(const FeatureRead &rd, misc::Ymd d, FeatureDay::Scratch &s) {
  rd.prefetch_day_columns(d, L_1day, row_cols(), s);
  rd.prefetch_day_columns(d, L_1min, minute_cols(), s);
}

const Bars &MinuteBlock::of(const FeatureRead::DayColumns &mc, size_t a) {
  assert(a < mc.A);
  if (mc.date != date_ || a < a0_ || a >= a0_ + kWidth) {
    assert(mc.fields == minute_cols());
    date_ = mc.date, a0_ = a / kWidth * kWidth;
    const size_t n = std::min(kWidth, mc.A - a0_);
    for (size_t j = 0; j < n; ++j)
      bars_[j].has.reset();
    const feature_storage_t *col[kMinCols];
    for (size_t i = 0; i < kMinCols; ++i)
      col[i] = mc.col(i);
    const auto v = [&](size_t i, size_t o) {
      const float x = static_cast<float>(col[i][o]);
      return std::isfinite(x) ? x : 0.f;
    };
    for (size_t t = 0; t < level_valid_rows(L_1min); ++t) {
      const int s = row_slot(t);
      for (size_t j = 0, o = t * mc.A + a0_; j < n; ++j, ++o)
        if (v(kMinTs, o) > 0.5f)
          if (const Cell c = cell(v(kMinOpen, o), v(kMinHigh, o), v(kMinLow, o), v(kMinClose, o), std::expm1(v(kMinAmount, o))); traded(c))
            bars_[j].add(s, c);
    }
  }
  return bars_[a - a0_];
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

Leg LibDay::leg(size_t j, Cell &c, Bars &) const {
  const float x = cur.px[3][j];
  c = cell(cur.px[0][j], cur.px[1][j], cur.px[2][j], x, cur.amount[j]);
  return {prev.pre[j], traded_row(cur, j) ? &c : nullptr, &mb.of(mc, j)};
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
    for (size_t u = 0; u < ws.size(); ++u) {
      const Members::Weight &x = ws[u];
      assert(x.key < K);
      ++t.members[x.key], t.weight[x.key] += x.w, t.cap[x.key] += m.cap[r];
      cs.push_back({x.key, {m.id[r], x.w}});
      dot[x.key * K + x.key] += static_cast<double>(x.w) * x.w;
      for (size_t v = u + 1; v < ws.size(); ++v) {
        const Members::Weight &y = ws[v];
        const double p = static_cast<double>(x.w) * y.w;
        dot[x.key * K + y.key] += p, dot[y.key * K + x.key] += p;
      }
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
