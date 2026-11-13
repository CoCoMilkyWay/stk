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

float Series::close(size_t i, int s) const {
  assert(has(i));
  const Mins &m = mins[i];
  const Slot &x = m.p->s[static_cast<size_t>(s)];
  return x.c == kNoBar ? 0.f : unpack(x, m.k, m.a).close;
}

bool Series::bar(size_t i, int s, Cell &c) const {
  assert(has(i));
  const Mins &m = mins[i];
  const Slot &x = m.p->s[static_cast<size_t>(s)];
  if (x.c == kNoBar)
    return false;
  c = unpack(x, m.k, m.a);
  return true;
}

bool Series::bars(size_t i, Bars &b) const {
  b.has.reset(), b.hole.reset();
  b.net.fill(0);
  if (!has(i))
    return false;
  const Mins &m = mins[i];
  for (size_t s = 0; s < static_cast<size_t>(kSlots); ++s)
    if (const Slot &x = m.p->s[s]; x.c != kNoBar) {
      b.cell[s] = unpack(x, m.k, m.a);
      b.net[s] = unpack_net(x);
      b.has.set(s);
    }
  return b.has.any();
}

size_t Source::find(misc::Ymd d) const {
  const auto it = std::ranges::lower_bound(daily, d, {}, &Candle::date);
  if (it != daily.end() && it->date == d)
    return static_cast<size_t>(it - daily.begin());
  return joined && today->candle.date == d ? daily.size() : size();
}

bool Source::bars(size_t i, Bars &b) const {
  if (i >= size())
    return false;
  if (i == daily.size()) {
    b = today->bars;
    return b.has.any();
  }
  return se && se->minute && se->bars(i, b);
}

double Source::adj(size_t i) const {
  assert(i < size());
  if (se && i < daily.size())
    return se->adj[i];
  size_t k = se ? daily.size() - 1 : 0;
  double f = se ? se->adj[k] : 1.0;
  for (; k < i; ++k)
    if (const Candle &c = candle(k + 1); c.ref > 0)
      f *= candle(k).close / c.ref;
  return f;
}

Flow Source::flow_of(misc::Ymd d, bool ends) const {
  const size_t i = find(d);
  return i < size() ? candle(i).flow.of(ends) : Flow{};
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
    } else if (const Tails *tl = side == kStock ? view->tails.get() : nullptr) {
      if (const size_t k = tl->find(key); k < tl->ids.size())
        o.daily = tl->daily[k];
    }
  }
  o.joined = o.today && (o.daily.empty() || view->joins(live->prev));
  return o;
}

} // namespace GUI::Quote
