#include "shared/Basket.hpp"

#include <cassert>
#include <cstdint>
#include <vector>

Weigh::Weigh(const pool::Picks &pk, misc::Ymd d) : s_(pk.strip.get()) {
  for (int rb = 0; rb < pool::kRebals; ++rb)
    src_[static_cast<size_t>(rb)] = pool::src_of(static_cast<pool::Rebal>(rb), s_->dates, d);
  q_ = src_[pool::kRebalDay];
}

Weigh::Cell Weigh::of(size_t a) const {
  assert(q_ < s_->days());
  Cell c;
  const size_t A = s_->A();
  const float mc = s_->mcap[q_ * A + a];
  c.v[pool::kRebalDay][pool::kWeightEq] = 1.0f;
  c.v[pool::kRebalDay][pool::kWeightCap] = mc;
  if (const size_t r = src_[pool::kRebalWeek]; r == q_) {
    c.v[pool::kRebalWeek][pool::kWeightEq] = 1.0f;
    c.v[pool::kRebalWeek][pool::kWeightCap] = mc;
  } else if (r != SIZE_MAX) {
    const double path = static_cast<double>(s_->grow[q_ * A + a]) / s_->grow[r * A + a];
    c.v[pool::kRebalWeek][pool::kWeightEq] = static_cast<float>(path);
    c.v[pool::kRebalWeek][pool::kWeightCap] = static_cast<float>(s_->mcap[r * A + a] * path);
  }
  return c;
}

void members(const pool::Picks &pk, misc::Ymd d, Members &m, bool global, std::span<const uint8_t> only) {
  m.clear();
  const Weigh w(pk, d);
  if (w.empty())
    return;
  const pool::Strip &s = *pk.strip;
  const BasketTable &bt = basket_table();
  const ConceptTable &ct = concepts();
  const size_t A = s.A(), q = w.row();
  thread_local std::vector<PoolMask> in;
  in.assign(A, 0);
  pk.pools(d, in);
  for (size_t a = 0; a < A; ++a) {
    const uint32_t g = s.axis.global(a);
    const Weigh::Cell wc = w.of(a);
    bt.each(in[a], s.ind[q * A + a], ct.concepts_of(g), [&](size_t b) {
      if (only.empty() || only[b])
        if (const float x = wc(bt, b); x > 0.0f)
          m.add(static_cast<uint32_t>(b), x);
    });
    m.next(global ? g : static_cast<uint32_t>(a), s.mcap[q * A + a]);
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
