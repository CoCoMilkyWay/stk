#include "gui/account/AccountSide.hpp"

#include "fundamental/AssetAxis.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <span>
#include <unordered_map>

namespace GUI::Account {

namespace {

using Quote::kSlots;

// 逐槽收 out[s] (槽 [0, out.size())): 有根取收, 无根沿用上一根, 首根前 = ref
void closes(const Quote::Bars &b, float ref, std::span<float> out) {
  float c = ref;
  for (size_t s = 0; s < out.size(); ++s)
    out[s] = c = b.has[s] ? b.cell[s].close : c;
}

// 一条逐槽点位 v (前收 ref) 成一日 (见文件头)
Quote::Day path_day(misc::Ymd d, float ref, std::span<const double> v) {
  assert(v.size() <= static_cast<size_t>(kSlots));
  Quote::Day y;
  float prev = ref, hi = ref, lo = ref;
  for (size_t s = 0; s < v.size(); ++s) {
    const float x = static_cast<float>(v[s]);
    y.bars.add(static_cast<int>(s), Quote::cell(prev, std::max(prev, x), std::min(prev, x), x, 0));
    hi = std::max(hi, x), lo = std::min(lo, x), prev = x;
  }
  y.candle = {Quote::cell(ref, hi, lo, prev, 0), d, ref, {}};
  return y;
}

} // namespace

Quote::Holding holding(const std::string &code, double qty, double px) {
  const AssetAxis &ax = asset_axis();
  const size_t id = ax.find(account::asset_code(code));
  return {code, id < ax.size() ? static_cast<uint32_t>(id) : Quote::Ref::kNone, qty, px};
}

Quote::Day day_of(const account::Day &d, double nav0, double idx0, size_t n, const DayOf &of) {
  assert(n >= 1 && n <= static_cast<size_t>(kSlots));
  std::array<float, kSlots> p;
  std::array<double, kSlots> idx;
  const std::span<float> ps = std::span(p).first(n);
  const auto path = [&](const std::string &code) -> std::span<const float> {
    const Quote::Day *s = of(code);
    if (!s)
      return {};
    closes(s->bars, s->candle.ref, ps);
    return ps;
  };
  const std::span<double> is = std::span(idx).first(n);
  account::intraday(d, nav0, idx0, path, is);
  return path_day(d.date, static_cast<float>(idx0), is);
}

Replay replay(const account::Ledger &l, const std::vector<misc::Ymd> &dates, const account::Spot &spot, const Minutes &minutes) {
  Replay o;
  const size_t D = dates.size();
  const size_t i0 = l.rows.empty() ? D : static_cast<size_t>(std::ranges::lower_bound(dates, l.rows.front().date) - dates.begin());
  if (i0 == D)
    return o;
  // 各证券自 i0 起的连分钟全史 (库里无根的不收 → 回放按成交价 / 券商现价插值)
  const AssetAxis &ax = asset_axis();
  std::unordered_map<std::string, Quote::Series> held;
  account::Closes px;
  for (const std::string &s : account::securities(l.rows))
    if (const size_t id = ax.find(account::asset_code(s)); id < ax.size())
      if (Quote::Series se; minutes(static_cast<uint32_t>(id), i0, se)) {
        for (const Quote::Candle &c : se.daily)
          px[s][c.date] = c.close;
        held.emplace(s, std::move(se));
      }
  const std::vector<account::Day> ds = account::replay(l.rows, l.rate, std::vector<misc::Ymd>(dates.begin() + static_cast<ptrdiff_t>(i0), dates.end()), px, spot);
  assert(ds.size() == D - i0 && "账户回放: 日数与日脊不符");
  auto block = std::make_shared<std::vector<Quote::Packed>>(ds.size());
  auto se = std::make_shared<Quote::Series>();
  se->minute = true;
  se->daily.reserve(ds.size()), se->adj.reserve(ds.size()), se->mins.reserve(ds.size());
  Quote::Day at;                   // of 的格
  std::array<double, kSlots> flat; // 首日: 平于首日指数
  flat.fill(ds.front().index);
  for (size_t k = 0; k < ds.size(); ++k) {
    const account::Day &d = ds[k];
    const auto of = [&](const std::string &code) -> const Quote::Day * {
      const auto it = held.find(code);
      if (it == held.end())
        return nullptr;
      const Quote::Series &h = it->second;
      const size_t j = h.find(d.date);
      if (j == h.daily.size() || !h.bars(j, at.bars))
        return nullptr;
      at.candle = h.daily[j];
      return &at;
    };
    const Quote::Day y = k > 0 ? day_of(d, ds[k - 1].nav, ds[k - 1].index, static_cast<size_t>(kSlots), of) : path_day(d.date, 1.f, flat);
    Quote::Packed &m = (*block)[k];
    se->daily.push_back(y.candle), se->adj.push_back(1.0);
    se->mins.push_back(y.bars.has.any() ? Quote::Series::Mins{&m, Quote::pack(y.bars, y.bars.net, false, m), 1.0} : Quote::Series::Mins{});
  }
  se->hold = {block};
  o.se = std::move(se);
  o.eq = {ds.back().nav, account::flow_after(l.rows, ds.back().date), 0, {}};
  for (const account::Day &d : ds)
    o.eq.in += d.flow;
  for (const account::Hold &h : ds.back().hold)
    o.eq.hold.push_back(holding(h.code, h.qty, h.px));
  return o;
}

} // namespace GUI::Account
