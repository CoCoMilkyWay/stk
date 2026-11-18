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

// 篮子 buy / val + 个股 ret / val (行 t 与可成交指数同行对齐: 行 t−1 收盘建的仓在行 t 实现):
//   篮子账 Sleeve: 行 t (≥ 1) 换到交易日 t−1 的成员表 (roll → buy[t]), 漂到 t 收盘 (pass) → val[t] = 持仓加权的 分子 / 市值
//   个股: ret[t] = 原料带 rel(t−1, t) − 1; val[t] = 分子 / 行 t 收盘总市值 (DY 的分子 = 股息率 × 前一行市值)
void ledger(misc::exec::ForkJoin &ex, const BPlanes &B, Baskets &o) {
  const pool::Strip &s = B.strip();
  const size_t nT = static_cast<size_t>(B.sh.T), nA = static_cast<size_t>(B.sh.A), nl = basket_table().size(), r0 = B.r0;
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  SideBook &bk = o[Quote::kBasket], &sk = o[Quote::kStock];
  bk.buy.reset(static_cast<int>(nl), B.sh.T);
  sk.ret.reset(B.sh.A, B.sh.T);
  for (int m = 0; m < kPoolVals; ++m) {
    bk.val[m].reset(static_cast<int>(nl), B.sh.T, kNaN);
    sk.val[m].reset(B.sh.A, B.sh.T, kNaN);
  }
  const auto mcap = [&](size_t r, size_t a) { return static_cast<double>(s.mcap[r * nA + a]); };
  ex.rows(B.sh.T, [&](int t0, int t1, int) {
    Sleeve lg;
    Members to;
    std::vector<double> buy(nl), v(nl * kPoolVals * 2);
    const auto vq = [&](size_t l, int m) { return v.data() + 2 * (l * kPoolVals + static_cast<size_t>(m)); };
    const auto step = [&](size_t t) {
      members(*B.pk, B.axis, B.date(t - 1), r0 + t - 1, to);
      std::fill(buy.begin(), buy.end(), 0.0);
      lg.roll(to, buy);
      lg.pass([&](uint32_t a) { return s.rel(r0 + t - 1, r0 + t, a); }, {});
    };
    if (t0 > 1)
      step(static_cast<size_t>(t0) - 1);
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      const size_t r = r0 + t;
      if (t > 0) {
        step(t);
        for (size_t l = 0; l < nl; ++l)
          bk.buy.v[l * nT + t] = static_cast<float>(buy[l]);
      }
      for (size_t a = 0; a < nA; ++a) {
        if (t > 0)
          sk.ret.v[a * nT + t] = s.rel(r - 1, r, a) - 1.f;
        const double mc = mcap(r, a);
        if (!(mc > 0.0))
          continue;
        const size_t i = t * nA + a;
        const double dy = r > 0 && mcap(r - 1, a) > 0.0 ? mcap(r - 1, a) : static_cast<double>(kNaN);
        for (int m = 0; m < kPoolVals; ++m)
          sk.val[m].v[a * nT + t] = static_cast<float>(B.val[m][i] * (m == kValDy ? dy : 1.0) / mc);
      }
      if (t == 0)
        continue;
      std::fill(v.begin(), v.end(), 0.0);
      const Members &h = lg.hold;
      for (size_t k = 0; k < h.size(); ++k)
        for (int m = 0; m < kPoolVals; ++m)
          if (const float x = sk.val[m].v[h.id[k] * nT + t]; std::isfinite(x))
            for (const Members::Weight &e : h.of(k)) {
              double *q = vq(e.key, m);
              q[0] += e.w * static_cast<double>(x), q[1] += e.w;
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
  const size_t A = static_cast<size_t>(B.sh.A), T = static_cast<size_t>(B.sh.T);
  assert(B.pk && B.in.size() == B.sh.n() && B.eo.size() == B.sh.n() && B.lab.m.size() == B.sh.n() && "B 未装 (load_b)");
  Baskets o;
  const PoolMask *in = B.in.data();
  const Holds hd = keys_of<Holds>(std::vector<int>{factor::stat::hold_close(1)});
  const LegPlane *const lab = &B.lab;
  const BasketTable &bt = basket_table();
  const ConceptTable &ct = concepts();
  std::vector<Weigh> w; // [行] 行 t 收盘建仓的权
  w.reserve(T);
  for (size_t t = 0; t < T; ++t)
    w.emplace_back(*B.pk, B.date(t), B.r0 + t);
  o[Quote::kBasket].ret = index_rows(ex, B.sh, 0, B.sh.T, hd, &lab, B.eo.data(), bt.size(), [&](size_t, size_t i, auto &&put) {
    const size_t a = i % A;
    const std::span<const uint16_t> cs = ct.concepts_of(B.axis.global(a));
    const Weigh &wt = w[i / A];
    bt.each(B.fact(i), cs, [&](size_t l) {
      put(l, in + i, [&, l](const PoolMask &v) { return bt.holds(l, B.fact(static_cast<size_t>(&v - in)), cs); }, static_cast<double>(wt(l, a)));
    });
  });
  ledger(ex, B, o);
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
