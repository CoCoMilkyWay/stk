#include "shared/Basket.hpp"

#include <cassert>
#include <cstdint>
#include <vector>

Weigh::Weigh(const pool::Picks &pk, misc::Ymd d, size_t q) : s_(pk.strip.get()), bt_(&basket_table()), q_(q), src_(pool_count(), SIZE_MAX) {
  assert(q < s_->days());
  for (PoolId p = 1; p < pool_count(); ++p)
    src_[p] = pk.src(p, d);
}

float Weigh::operator()(size_t b, size_t a) const {
  const size_t r = src_[bt_->base(b)];
  assert(r != SIZE_MAX && "所在的子池无源行");
  return s_->omega(r, q_, a, bt_->weight(b));
}

void members(const pool::Picks &pk, const PoolAxis &axis, misc::Ymd d, size_t q, Members &m, std::span<const uint8_t> only) {
  const pool::Strip &s = *pk.strip;
  const BasketTable &bt = basket_table();
  const ConceptTable &ct = concepts();
  const size_t A = axis.size();
  assert(s.axis == axis.hash && s.A == A && "选股表与轴不符");
  std::vector<PoolMask> in(A, 0);
  pk.pools(d, in);
  const Weigh w(pk, d, q);
  m.clear();
  for (size_t a = 0; a < A; ++a) {
    bt.each({in[a], s.ind[q * A + a]}, ct.concepts_of(axis.global(a)), [&](size_t b) {
      if (only.empty() || only[b])
        if (const float x = w(b, a); x > 0)
          m.add(static_cast<uint32_t>(b), x);
    });
    m.next(static_cast<uint32_t>(a), s.mcap[q * A + a]);
  }
}
