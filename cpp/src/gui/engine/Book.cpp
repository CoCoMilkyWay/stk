// Book — 见头文件. include 因子契约 / Stat 行核头 (h2f), 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/Book.hpp"

#include "define/Members.hpp"
#include "factor/Stat/Host.hpp" // h2f
#include "shared/Basket.hpp"    // basket_table

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <span>
#include <utility>

namespace GUI::Engine {

BookLines fold_book(misc::exec::ForkJoin &ex, int seg, const RowLines &r, int stride) {
  const factor::Geo g = factor::geo_of(seg);
  const int n = r.n, T = r.T;
  assert(n >= 1 && stride >= 1 && T % stride == 0 && T % g.S == 0);
  assert(r.hold.size() == static_cast<size_t>(n) && r.at_exit.size() == r.hold.size() && r.x.size() == static_cast<size_t>(n) * T);
  BookLines out;
  out.reset(n, T / stride);
  ex.rows(n, [&](int i0, int i1, int) {
    std::vector<double> inc(static_cast<size_t>(T));
    for (int i = i0; i < i1; ++i) {
      const int hd = r.hold[static_cast<size_t>(i)];
      const bool at_exit = r.at_exit[static_cast<size_t>(i)];
      const double h = factor::stat::hold_span(hd, g);
      const float *x = r.x.data() + static_cast<size_t>(i) * T;
      std::fill(inc.begin(), inc.end(), 0.0);
      for (int t = 0; t < T; ++t)
        if (const int u = at_exit ? factor::stat::hold_exit_row(t, hd, g) : t; u < T)
          inc[static_cast<size_t>(u)] += x[t] / h;
      float *dst = out.at(i);
      for (int p = 0; p < out.P; ++p) {
        double s = 0.0;
        for (int u = p * stride; u < (p + 1) * stride; ++u)
          s += inc[static_cast<size_t>(u)];
        dst[p] = static_cast<float>(s);
      }
    }
  });
  return out;
}

namespace {

struct Omega {
  Omega(const BPlanes &B, const BasketTable &bt) : B(B), bt(bt) {}
  double operator()(size_t i, size_t l) const {
    const size_t A = static_cast<size_t>(B.sh.A), q = i / A, j = B.from[bt.rebal(l)][q] * A + i % A;
    const bool drift = j != i && B.close.m[j] && B.close.m[i];
    return pool::omega(bt.weight(l), B.mcap[drift ? j : i], drift ? static_cast<double>(B.close.v[i]) / B.close.v[j] : 1.0);
  }
  const BPlanes &B;
  const BasketTable &bt;
};

std::vector<int32_t> prev_rows(misc::exec::ForkJoin &ex, const factor::check::Plane &close, int T, int A) {
  const size_t nT = static_cast<size_t>(T), nA = static_cast<size_t>(A);
  std::vector<int32_t> prev(nT * nA, -1);
  ex.rows(A, [&](int a0, int a1, int) {
    std::vector<int32_t> lc(static_cast<size_t>(a1 - a0), -1);
    for (size_t t = 0; t < nT; ++t)
      for (int a = a0; a < a1; ++a) {
        const size_t i = t * nA + static_cast<size_t>(a), j = static_cast<size_t>(a - a0);
        if (close.m[i] && close.v[i] > 0.f)
          prev[i] = lc[j], lc[j] = static_cast<int32_t>(t);
      }
  });
  return prev;
}

template <class Each>
BookLines index_rows(misc::exec::ForkJoin &ex, const Shape &sh, int r0, int r1, const Holds &hd, const LegPlane *const *labels, const uint8_t *eo, size_t nl,
                     Each &&each) {
  assert(hd.n >= 1 && 0 <= r0 && r0 < r1 && r1 <= sh.T);
  const factor::stat::Span sp{sh.T, sh.A, factor::geo_of(sh.seg)};
  const size_t nT = static_cast<size_t>(r1 - r0), nA = static_cast<size_t>(sh.A), H = static_cast<size_t>(hd.n);
  RowLines x;
  x.reset(static_cast<int>(H * nl), r1 - r0);
  for (size_t i = 0; i < H * nl; ++i)
    x.hold[i] = hd.k[i / nl];
  ex.rows(r1 - r0, [&](int u0, int u1, int) {
    std::vector<double> s(2 * H * nl);
    for (size_t t = static_cast<size_t>(r0 + u0); t < static_cast<size_t>(r0 + u1); ++t) {
      std::fill(s.begin(), s.end(), 0.0);
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = t * nA + a;
        for (size_t hi = 0; hi < H; ++hi) {
          const LegPlane &lab = *labels[hi];
          if (!lab.m[i])
            continue;
          const factor::stat::Chain c = factor::stat::chain_of(static_cast<int>(t), sp, hd.k[hi]);
          const float y[factor::stat::kLegs] = {factor::cpu::stat::h2f(lab.v[factor::stat::kLong][i]), factor::cpu::stat::h2f(lab.v[factor::stat::kShort][i])};
          each(hi, i, [&]<class Code, class In>(size_t l, const Code *code, const In &in, double om) {
            const factor::stat::Cell<Code> cl{c, code, lab.m.data() + i, eo + i, y};
            if (!cl.held(in))
              return;
            double *q = s.data() + 2 * (hi * nl + l);
            q[0] += om * cl.ret(cl.continues(in)), q[1] += om;
          });
        }
      }
      for (size_t l = 0; l < H * nl; ++l)
        if (s[2 * l + 1] > 0.0)
          x.x[l * nT + (t - static_cast<size_t>(r0))] = static_cast<float>(s[2 * l] / s[2 * l + 1]);
    }
  });
  return fold_book(ex, sh.seg, x, 1);
}

void ledger(misc::exec::ForkJoin &ex, const BPlanes &B, const Omega &om, const std::vector<int32_t> &prev, Baskets &o) {
  const factor::check::Plane &close = B.close;
  const float *sh = B.shares.data();
  std::vector<float> pxv = B.px;
  float *px = pxv.data();
  const BasketTable &bt = om.bt;
  const size_t nT = static_cast<size_t>(B.sh.T), nA = static_cast<size_t>(B.sh.A), S = static_cast<size_t>(B.sh.seg), nl = bt.size();
  ex.rows(B.sh.A, [&](int a0, int a1, int) {
    std::vector<float> lp(static_cast<size_t>(a1 - a0), 0.f);
    for (size_t t = 0; t < nT; ++t)
      for (int a = a0; a < a1; ++a) {
        const size_t i = t * nA + static_cast<size_t>(a), j = static_cast<size_t>(a - a0);
        if (px[i] > 0.f)
          lp[j] = px[i];
        else
          px[i] = lp[j];
      }
  });
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  SideBook &bk = o[Quote::kBasket], &sk = o[Quote::kStock];
  bk.buy.reset(static_cast<int>(nl), B.sh.T);
  sk.ret.reset(B.sh.A, B.sh.T);
  for (int m = 0; m < kPoolVals; ++m) {
    bk.val[m].reset(static_cast<int>(nl), B.sh.T, kNaN);
    sk.val[m].reset(B.sh.A, B.sh.T, kNaN);
  }
  const auto cell = [&](size_t t, size_t a) {
    const int32_t q = prev[t * nA + a];
    return q < 0 ? SIZE_MAX : static_cast<size_t>(q) * nA + a;
  };
  const auto members = [&](size_t t, Members &m) {
    m.clear();
    for (size_t a = 0; a < nA; ++a)
      if (const size_t p = cell(t, a); p != SIZE_MAX) {
        bt.each(B.fact(p), B.concepts_of(a), [&](size_t l) {
          if (const double w = om(p, l); w > 0.0)
            m.add(static_cast<uint32_t>(l), static_cast<float>(w));
        });
        m.next(static_cast<uint32_t>(a));
      }
  };
  const auto drift = [&](size_t t, Members &m) { m.scale([&](uint32_t a) { return close.v[t * nA + a] / close.v[cell(t, a)]; }); };
  ex.rows(B.sh.T, [&](int t0, int t1, int) {
    std::vector<double> buy(nl), v(nl * kPoolVals * 2);
    const auto vq = [&](size_t l, int m) { return v.data() + 2 * (l * kPoolVals + static_cast<size_t>(m)); };
    Members cur, last;
    if (t0 > 0) {
      members(static_cast<size_t>(t0) - 1, last);
      drift(static_cast<size_t>(t0) - 1, last);
    }
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      members(t, cur);
      std::fill(buy.begin(), buy.end(), 0.0);
      cur.turnover(last, buy);
      for (size_t l = 0; l < nl; ++l)
        bk.buy.v[l * nT + t] = static_cast<float>(buy[l]);
      drift(t, cur);
      std::swap(cur, last);
      std::fill(v.begin(), v.end(), 0.0);
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = t * nA + a;
        if (const size_t p = cell(t, a); p != SIZE_MAX)
          sk.ret.v[a * nT + t] = static_cast<float>(close.v[i] / close.v[p] - 1.0);
        const double mc = static_cast<double>(px[i]) * sh[i];
        if (!(mc > 0.0))
          continue;
        double num[kPoolVals];
        for (int m = 0; m < kPoolVals; ++m)
          num[m] = B.val[m][i];
        const size_t d0 = t - t % S;
        num[kValDy] *= d0 > 0 ? static_cast<double>(px[(d0 - 1) * nA + a]) * sh[(d0 - 1) * nA + a] : static_cast<double>(kNaN);
        for (int m = 0; m < kPoolVals; ++m)
          sk.val[m].v[a * nT + t] = static_cast<float>(num[m] / mc);
        if (t == 0)
          continue;
        const double vw[pool::kWeights] = {1.0, mc};
        bt.each(B.fact(i - nA), B.concepts_of(a), [&](size_t l) {
          const double w = vw[bt.weight(l)];
          for (int m = 0; m < kPoolVals; ++m)
            if (std::isfinite(num[m])) {
              double *e = vq(l, m);
              e[0] += w * (num[m] / mc), e[1] += w;
            }
        });
      }
      for (size_t l = 0; l < nl; ++l)
        for (int m = 0; m < kPoolVals; ++m)
          if (const double *e = vq(l, m); e[1] > 0.0)
            bk.val[m].v[l * nT + t] = static_cast<float>(e[0] / e[1]);
    }
  });
}

} // namespace

Baskets baskets(misc::exec::ForkJoin &ex, const BPlanes &B) {
  const size_t A = static_cast<size_t>(B.sh.A);
  assert(B.in.size() == B.sh.n() && B.eo.size() == B.sh.n() && B.lab.m.size() == B.sh.n() && B.close.v.size() == B.sh.n() && "B 未装 (load_b)");
  Baskets o;
  const PoolMask *in = B.in.data();
  const Holds hd = keys_of<Holds>(std::vector<int>{factor::stat::hold_close(1)});
  const LegPlane *const lab = &B.lab;
  const BasketTable &bt = basket_table();
  const Omega om(B, bt);
  o[Quote::kBasket].ret = index_rows(ex, B.sh, 0, B.sh.T, hd, &lab, B.eo.data(), bt.size(), [&](size_t, size_t i, auto &&put) {
    const std::span<const uint16_t> cs = B.concepts_of(i % A);
    bt.each(B.fact(i), cs, [&](size_t l) {
      put(l, in + i, [&, l](const PoolMask &v) { return bt.holds(l, B.fact(static_cast<size_t>(&v - in)), cs); }, om(i, l));
    });
  });
  ledger(ex, B, om, prev_rows(ex, B.close, B.sh.T, B.sh.A), o);
  return o;
}

BookLines pool_line(misc::exec::ForkJoin &ex, const Block &c) {
  const YPlanes &y = c.y;
  assert(y.eo && "Y 未钉");
  const factor::stat::Span s = c.span();
  return index_rows(ex, y.sh, s.t_lo, s.t_hi(), y.hd, y.labels, y.eo, 1,
                    [&](size_t hi, size_t i, auto &&put) { put(size_t{0}, y.labels[hi]->m.data() + i, [](uint8_t v) { return v != 0; }, 1.0); });
}

} // namespace GUI::Engine
