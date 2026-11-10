#include "gui/quote/BasketRow.hpp"

#include "shared/Basket.hpp" // basket_line / cut_of / concept_cut / cut_rebal

#include <cassert>
#include <cmath>
#include <span>
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
  const ConceptTable &ct = concepts();
  const size_t A = axis.size(), NP = pool_count(), q = pool::src_of(pool::kRebalDay, t.dates, d);
  assert(t.axis == axis.hash && t.A == A && "选股表与库轴不符");
  struct In {
    std::span<const uint16_t> ids; // 未扫到的成员 (升序, 随资产下标推进)
    size_t r;                      // 源行
  };
  std::vector<In> in(NP); // [子池]
  for (PoolId p = 1; p < NP && q != SIZE_MAX; ++p)
    if (const size_t r = pk.src(p, d); r != SIZE_MAX)
      in[p] = {pk.row(p, r), r};
  m.clear();
  float om[kWeights];
  const auto put = [&](size_t cut) {
    for (int w = 0; w < kWeights; ++w)
      if (om[w] > 0)
        m.add(static_cast<uint32_t>(basket_line(cut, static_cast<Weight>(w))), om[w]);
  };
  for (size_t a = 0; a < A; ++a) {
    if (q != SIZE_MAX && t.live[q * A + a]) {
      t.omega(q, q, a, om);
      put(0);
      if (const uint8_t k = t.ind[q * A + a])
        put(k);
      for (const uint16_t k : ct.concepts_of(axis.global(a)))
        put(concept_cut(k));
    }
    for (PoolId p = 1; p < NP; ++p)
      if (!in[p].ids.empty() && in[p].ids.front() == a) {
        in[p].ids = in[p].ids.subspan(1);
        t.omega(in[p].r, q, a, om);
        put(cut_of(p));
      }
    m.next();
  }
}

void drift(Members &m, misc::Ymd prev, misc::Ymd d, std::span<const float> rel) {
  assert(rel.size() == m.assets());
  bool reb[pool::kRebals];
  for (int k = 0; k < pool::kRebals; ++k)
    reb[k] = pool::rebal_at(static_cast<pool::Rebal>(k), prev, d);
  for (size_t a = 0; a < m.assets(); ++a)
    for (Members::Weight &w : m.of(a))
      w.w = weight_of(w.basket) == kWeightEq && reb[cut_rebal(cut_of_line(w.basket))] ? 1.f : w.w * rel[a];
}

void bars_of(const FeatureRead::DayColumns &mc, size_t a, misc::Ymd d, Bars &b) {
  b.date = d;
  b.has.reset();
  for (size_t t = 0; t < level_valid_rows(L_1min); ++t) {
    const float x = val(mc, t, kMinClose, a);
    if (val(mc, t, kMinTs, a) > 0.5f && x > 0)
      b.add(row_slot(t), cell(val(mc, t, kMinOpen, a), val(mc, t, kMinHigh, a), val(mc, t, kMinLow, a), x, 0, std::expm1(val(mc, t, kMinAmount, a))));
  }
}

Lane::Lane(size_t A, size_t lines, std::vector<uint32_t> slot) : fold(lines), day(lines), slot_(std::move(slot)) {
  row_alloc(dc, A);
  minute_alloc(mc, A);
}

} // namespace GUI::Quote
