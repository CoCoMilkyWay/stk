#include "gui/quote/BasketRow.hpp"

#include "shared/Basket.hpp" // basket_line / cut_of / concept_cut

#include <cassert>
#include <cmath>
#include <span>
#include <string_view>
#include <vector>

namespace GUI::Quote {

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

// row_cols 的列序
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
                       kDayCols };

} // namespace

std::vector<size_t> row_cols() {
  std::vector<size_t> c = {col_of(L_1day, "cs_valid", ts::TfId::None), col_of(L_1day, "ts_valid", ts::TfId::None),
                           col_of(L_1day, "open_day", ts::TfId::None), col_of(L_1day, "high_day", ts::TfId::None),
                           col_of(L_1day, "low_day", ts::TfId::None), col_of(L_1day, "close_day", ts::TfId::None),
                           col_of(L_1day, "amount_day", ts::TfId::Log), col_of(L_1day, "pre_close", ts::TfId::None),
                           col_of(L_1day, "total_shares", ts::TfId::None), col_of(L_1day, "ind_l1", ts::TfId::None)};
  assert(c.size() == kDayCols);
  return c;
}

std::vector<size_t> minute_cols() {
  return {col_of(L_1min, "ts_valid", ts::TfId::None), col_of(L_1min, "open", ts::TfId::None), col_of(L_1min, "high", ts::TfId::None),
          col_of(L_1min, "low", ts::TfId::None), col_of(L_1min, "close", ts::TfId::None), col_of(L_1min, "amount", ts::TfId::Log)};
}

void read_row(const FeatureRead::DayColumns &dc, size_t A, Row &r) {
  for (std::vector<float> &v : r.px)
    v.resize(A);
  r.amount.resize(A), r.pre.resize(A), r.shares.resize(A);
  r.live.resize(A), r.trade.resize(A), r.ind.resize(A);
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
  }
}

void members(const Row &r, const PoolAxis &axis, const pool::Picks &pk, misc::Ymd d, Members &m) {
  const ConceptTable &ct = concepts();
  const size_t A = axis.size(), NP = pool_count();
  assert(pk.axis == axis.hash && "选股表与库轴不符");
  std::vector<std::span<const uint16_t>> in(NP); // [子池] 未扫到的成员 (升序, 随资产下标推进)
  for (PoolId p = 1; p < NP; ++p)
    in[p] = pk.of(p, d);
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
    for (PoolId p = 1; p < NP; ++p)
      if (!in[p].empty() && in[p].front() == a) {
        in[p] = in[p].subspan(1);
        if (c > 0)
          put(cut_of(p));
      }
    m.next();
  }
}

void bars_of(const FeatureRead::DayColumns &mc, size_t a, misc::Ymd d, Bars &b) {
  b.date = d;
  b.has.reset();
  for (size_t t = 0; t < level_valid_rows(L_1min); ++t) {
    const float x = val(mc, t, 4, a);
    if (val(mc, t, 0, a) > 0.5f && x > 0)
      b.add(row_slot(t), cell(val(mc, t, 1, a), val(mc, t, 2, a), val(mc, t, 3, a), x, 0, std::expm1(val(mc, t, 5, a))));
  }
}

} // namespace GUI::Quote