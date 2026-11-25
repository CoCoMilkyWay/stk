#include "gui/account/AccountSide.hpp"

#include "fundamental/AssetAxis.hpp"
#include "live/Rebal.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <map>
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

// 逐日扫流水出各只的持仓段 (成本线 / 仓位色带): 买入按额加权摊成本 (卖出不动), 股数归零即清仓分段;
//   线锚 = 买入额 ≥ 10 万的日 (历史成交), 记该日首笔买入槽; 仓位 = 股数 × 收盘 / 净资产 (清仓段 = 清仓日开盘股数, 仍持仓 = 末日);
//   ratio(日) = 成本 × adj(成本定日) ÷ (当日收盘 × adj(当日)) —— 成本在连复权空间里是恒定水平线, 跨除权日连续
std::shared_ptr<Quote::Positions> positions_of(const live::flows::Cache &l, const std::vector<account::Day> &ds,
                                               const std::unordered_map<std::string, Quote::Series> &held, const AssetAxis &ax) {
  using live::flows::holds;
  using live::flows::kShare;
  // 代码 → 日 → {首笔买入槽, 当日买入额} (历史成交)
  std::map<std::string, std::map<misc::Ymd, std::pair<int, double>>> buys;
  for (const live::Deal &x : l.deals) {
    if (x.side != live::kBuy)
      continue;
    auto &[slot, amt] = buys[x.code][x.date];
    const int s = std::clamp(Quote::sec_slot(x.sec), 0, kSlots - 1);
    slot = amt == 0.0 ? s : std::min(slot, s);
    amt += x.amount;
  }
  struct Raw {
    misc::Ymd open = 0, close = 0;
    double wqty = 0;                                 // 仓位计的股数
    std::vector<std::pair<misc::Ymd, double>> steps; // 成本变了的日 → 新成本 [原价]
  };
  struct Pos {
    misc::Ymd last = 0; // 末个有流水的日
    double qty = 0, cost = 0, start = 0;
    bool open = false;
    Raw *cur = nullptr;
  };
  std::map<std::string, std::vector<Raw>> raw;
  std::map<std::string, Pos> ps;
  for (const live::CashFlow &r : l.rows) {
    if (!holds(r) || r.qty == 0)
      continue;
    Pos &p = ps[r.code];
    if (p.last != r.date) {
      p.last = r.date;
      p.start = p.qty;
    }
    const double q = static_cast<double>(r.qty);
    if (r.qty > 0) {
      if (!p.open) {
        p.open = true;
        raw[r.code].push_back({.open = r.date});
        p.cur = &raw[r.code].back();
      }
      if (r.price > 0) // 划入等无价行: 只加股数, 成本不动
        p.cost = (p.cost * p.qty + r.price * q) / (p.qty + q);
      p.qty += q;
      if (!p.cur->steps.empty() && p.cur->steps.back().first == r.date)
        p.cur->steps.back().second = p.cost;
      else if (p.cost > 0)
        p.cur->steps.push_back({r.date, p.cost});
    } else {
      p.qty += q;
      if (p.open && p.qty < kShare) {
        p.open = false;
        p.cur->close = r.date;
        p.cur->wqty = p.start;
        p.qty = p.cost = 0, p.cur = nullptr;
      }
    }
  }
  for (auto &[c, p] : ps)
    if (p.open)
      p.cur->wqty = p.qty;
  // 日 → 净资产 (回放)
  std::map<misc::Ymd, double> nav;
  for (const account::Day &x : ds)
    nav[x.date] = x.nav;
  auto out = std::make_shared<Quote::Positions>();
  for (const auto &[code, segs] : raw) {
    const auto hi = held.find(code);
    const size_t id = ax.find(live::asset_sym(live::market_of(code), code));
    if (hi == held.end() || id >= ax.size())
      continue;
    const Quote::Series &h = hi->second;
    // 最后日序 ≤ day 的日根下标; 无 (在库段之前) → daily.size()
    const auto at_or_before = [&](misc::Ymd day) -> size_t {
      const auto it = std::ranges::lower_bound(h.daily, day, {}, [](const Quote::Candle &c) { return c.date; });
      if (it != h.daily.end() && it->date == day)
        return static_cast<size_t>(it - h.daily.begin());
      if (it == h.daily.begin())
        return h.daily.size();
      return static_cast<size_t>(it - h.daily.begin() - 1);
    };
    std::vector<Quote::PosSeg> &dst = (*out)[static_cast<uint32_t>(id)];
    for (const Raw &r : segs) {
      const misc::Ymd end = r.close ? r.close : ds.back().date;
      Quote::PosSeg s{.open = r.open, .close = r.close};
      if (const size_t wi = at_or_before(end); wi < h.daily.size())
        if (const auto ni = nav.find(end); ni != nav.end() && ni->second > 0)
          s.weight = r.wqty * h.daily[wi].close / ni->second;
      size_t sp = 0; // 成本步 (steps 升序)
      const size_t c0 = static_cast<size_t>(std::ranges::lower_bound(h.daily, r.open, {}, [](const Quote::Candle &c) { return c.date; }) - h.daily.begin());
      for (size_t ci = c0; ci < h.daily.size() && h.daily[ci].date <= end; ++ci) {
        while (sp + 1 < r.steps.size() && r.steps[sp + 1].first <= h.daily[ci].date)
          ++sp;
        if (sp >= r.steps.size() || r.steps[sp].first > h.daily[ci].date)
          continue;
        const size_t si = at_or_before(r.steps[sp].first);
        if (si < h.daily.size() && h.daily[ci].close > 0 && h.adj[ci] > 0)
          s.ratio.push_back({h.daily[ci].date, r.steps[sp].second * h.adj[si] / (h.daily[ci].close * h.adj[ci])});
      }
      if (s.ratio.empty())
        continue;
      if (const auto bi = buys.find(code); bi != buys.end())
        for (const auto &[bd, sa] : bi->second)
          if (bd >= r.open && bd <= end && sa.second >= 1e5)
            s.lines.push_back({bd, sa.first});
      dst.push_back(std::move(s));
    }
  }
  return out;
}

} // namespace

account::Trade trade_of(const live::Fill &x, int last) {
  return {std::clamp(Quote::sec_slot(x.sec), 0, last), x.code, x.side * static_cast<double>(x.qty), x.price, -x.side * x.amount - live::rebal::fee(x.amount, x.side)};
}

Quote::TradeMark mark_of(misc::Ymd d, const live::Fill &x) {
  Quote::TradeMark m{.date = d, .slot = std::clamp(Quote::sec_slot(x.sec), 0, kSlots - 1)};
  m.amount[x.side] = x.amount;
  m.secs.push_back({x.code, x.name});
  m.secs.back().amount[x.side] = x.amount, m.secs.back().qty[x.side] = static_cast<double>(x.qty);
  return m;
}

Quote::Holding holding(const std::string &code, double qty, double px) {
  const AssetAxis &ax = asset_axis();
  const size_t id = ax.find(live::asset_sym(live::market_of(code), code));
  return {code, id < ax.size() ? static_cast<uint32_t>(id) : Quote::Ref::kNone, qty, px};
}

Quote::Day day_of(const account::Day &d, double nav0, double idx0, std::span<const account::Trade> trades, size_t n, const DayOf &of) {
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
  account::intraday(d, nav0, idx0, trades, path, is);
  return path_day(d.date, static_cast<float>(idx0), is);
}

Replay replay(const live::flows::Cache &l, const std::vector<misc::Ymd> &dates, const account::Spot &spot, const Minutes &minutes) {
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
    if (const size_t id = ax.find(live::asset_sym(live::market_of(s), s)); id < ax.size())
      if (Quote::Series se; minutes(static_cast<uint32_t>(id), i0, se)) {
        for (const Quote::Candle &c : se.daily)
          px[s][c.date] = c.close;
        held.emplace(s, std::move(se));
      }
  const std::vector<account::Day> ds = account::replay(l.rows, l.rate, std::vector<misc::Ymd>(dates.begin() + static_cast<ptrdiff_t>(i0), dates.end()), px, spot);
  assert(ds.size() == D - i0 && "账户回放: 日数与日脊不符");
  std::map<misc::Ymd, std::vector<account::Trade>> trades; // 日 → 该日的历史成交 (回推用)
  Quote::Trades marks;
  for (const live::Deal &x : l.deals)
    if (x.date >= ds.front().date && x.date <= ds.back().date) {
      trades[x.date].push_back(trade_of(x, kSlots - 1));
      marks.marks.push_back(mark_of(x.date, x));
    }
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
    const auto t = trades.find(d.date);
    const std::span<const account::Trade> ts = t != trades.end() ? std::span<const account::Trade>(t->second) : std::span<const account::Trade>{};
    const Quote::Day y = k > 0 ? day_of(d, ds[k - 1].nav, ds[k - 1].index, ts, static_cast<size_t>(kSlots), of) : path_day(d.date, 1.f, flat);
    Quote::Packed &m = (*block)[k];
    se->daily.push_back(y.candle), se->adj.push_back(1.0);
    se->mins.push_back(y.bars.has.any() ? Quote::Series::Mins{&m, Quote::pack(y.bars, y.bars.net, false, m), 1.0} : Quote::Series::Mins{});
  }
  se->hold = {block};
  o.se = std::move(se);
  auto days = std::make_shared<std::vector<Quote::EquityDay>>();
  days->reserve(ds.size());
  double pnl = 0;
  for (size_t k = 0; k < ds.size(); ++k) {
    double base = 0;
    if (k > 0) {
      pnl += ds[k].nav - ds[k - 1].nav - ds[k].flow;
      base = std::max(ds[k - 1].nav + std::max(ds[k].flow, 0.0), 0.0);
    }
    days->push_back({ds[k].date, base, pnl});
  }
  o.eq = {ds.back().nav, account::flow_after(l.rows, ds.back().date), 0, {}, std::move(days), std::make_shared<const Quote::Trades>(Quote::rebin(marks, 1)), positions_of(l, ds, held, ax)};
  for (const account::Day &d : ds)
    o.eq.in += d.flow;
  for (const account::Hold &h : ds.back().hold)
    o.eq.hold.push_back(holding(h.code, h.qty, h.px));
  return o;
}

} // namespace GUI::Account
