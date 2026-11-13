#include "gui/quote/Quote.hpp"

#include "api/sina/client.hpp"

#include <cassert>

namespace GUI::Quote {

misc::Ymd Calendar::next(misc::Ymd d) const {
  const auto it = std::ranges::upper_bound(days, d);
  assert(it != days.end() && "交易日历缺次交易日");
  return *it;
}

misc::Ymd Calendar::prev(misc::Ymd d) const {
  const auto it = std::ranges::lower_bound(days, d);
  assert(it != days.begin() && "交易日历缺前一交易日");
  return *std::prev(it);
}

size_t Series::find(misc::Ymd d) const {
  const auto it = std::ranges::lower_bound(daily, d, {}, &Candle::date);
  return it != daily.end() && it->date == d ? static_cast<size_t>(it - daily.begin()) : daily.size();
}

bool Series::bars(size_t i, Bars &b) const {
  const float *p = slots(i);
  b.has.reset(), b.hole.reset();
  b.net.fill(0);
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s) {
    const float *q = p + s * kFields;
    if (!(q[kClose] > 0))
      continue;
    b.cell[s] = {q[kOpen], q[kHigh], q[kLow], q[kClose], q[kAmount]};
    b.net[s] = q[kNet];
    b.has.set(s);
  }
  return b.has.any();
}

const Candle *Source::last() const {
  return joined ? &today->candle : !daily.empty() ? &daily.back()
                                                  : nullptr;
}

const Candle *Source::candle(misc::Ymd d) const {
  if (joined && today->candle.date == d)
    return &today->candle;
  const auto it = std::ranges::lower_bound(daily, d, {}, &Candle::date);
  return it != daily.end() && it->date == d ? &*it : nullptr;
}

Flow Source::flow_of(misc::Ymd d, bool ends) const {
  const Candle *c = candle(d);
  return c ? c->flow.of(ends) : Flow{};
}

bool Source::bars(misc::Ymd d, Bars &b) const {
  if (joined && today->bars.has.any() && today->candle.date == d) {
    b = today->bars;
    return true;
  }
  if (!se || !se->minute)
    return false;
  const size_t i = se->find(d);
  return i < se->daily.size() && se->bars(i, b);
}

size_t Tails::find(uint32_t id) const {
  const auto it = std::ranges::lower_bound(ids, id);
  return it != ids.end() && *it == id ? static_cast<size_t>(it - ids.begin()) : ids.size();
}

const Series *Tape::line(uint32_t key) const {
  const size_t k = find(key);
  return k < keys.size() ? list[k].get() : nullptr;
}

const Series *PriceView::series(Ref r) const {
  assert(r.ok());
  return lack.empty() ? tape[r.side]->line(r.key) : nullptr;
}

bool PriceView::covers(Ref r, bool minute) const {
  assert(r.ok());
  if (!lack.empty())
    return true;
  const Tape &t = *tape[r.side];
  const size_t k = t.find(r.key);
  return k < t.keys.size() && (!t.list[k] || !minute || t.list[k]->minute);
}

const Day *Live::at(Ref r) const {
  assert(r.ok());
  const Days *x = days[r.side].get();
  return x && r.key < x->size() ? (*x)[r.key].get() : nullptr;
}

const sina::Quote *Live::quote(uint32_t id) const { return quotes && id < quotes->size() ? (*quotes)[id].get() : nullptr; }

const sina::Quote *Live::watched(std::string_view sym) const {
  if (!watch)
    return nullptr;
  const auto it = std::ranges::lower_bound(*watch, sym, {}, [](const sina::Quote &q) { return std::string_view(q.sym); });
  return it != watch->end() && it->sym == sym ? &*it : nullptr;
}

const Tape *Joint::tape() const { return view && view->lack.empty() ? view->tape[side].get() : nullptr; }

misc::Ymd Joint::today() const {
  const Days *x = live ? live->days[side].get() : nullptr;
  const bool has = x && (side == kStock || std::ranges::any_of(*x, [](const auto &y) { return y != nullptr; }));
  return has && view && view->joins(live->prev) ? live->date : 0;
}

Source Joint::of(uint32_t key) const {
  Source o;
  o.today = live ? live->at({side, key}) : nullptr;
  if (const Tape *t = tape()) {
    if ((o.se = t->line(key))) {
      o.daily = o.se->daily;
    } else if (t->tails) {
      if (const size_t k = t->tails->find(key); k < t->tails->ids.size())
        o.daily = t->tails->daily[k];
    }
  }
  o.joined = o.today && (o.daily.empty() || view->joins(live->prev));
  return o;
}

} // namespace GUI::Quote
