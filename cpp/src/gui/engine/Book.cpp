// Book — 见头文件. include 因子契约 / Stat 行核头 (h2f), 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/Book.hpp"

#include "define/Members.hpp"
#include "factor/Stat/Host.hpp" // h2f
#include "shared/Basket.hpp"    // basket_table / Weigh / members

#include <algorithm>
#include <cassert>
#include <limits>
#include <span>
#include <utility>

namespace GUI::Engine {

namespace {
constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
} // namespace

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

double BasketPass::fixed_bytes(const Domain &dm) {
  const double nl = static_cast<double>(basket_table().size()), A = static_cast<double>(dm.rs.axis.size());
  return (nl * (4 + kPoolVals + kMarginVals) + A * (2 + kPoolVals + kMarginVals)) * static_cast<double>(dm.stat_days()) * sizeof(float);
}

BasketPass::BasketPass(const Domain &dm, std::shared_ptr<const pool::Picks> pk, std::span<const LeafId, kPoolVals> val)
    : pk_(std::move(pk)), warm_(dm.warm), ds_(dm.stat_days()) {
  assert(pk_ && dm.pool == kAllPool && pool_count() <= static_cast<size_t>(kPoolBits));
  std::copy(val.begin(), val.end(), val_);
  const pool::Strip &s = *pk_->strip;
  assert(s.axis.hash == dm.rs.axis.hash && s.A() == dm.rs.axis.size() && "选股表与评估域 A 轴不符");
  r0_ = s.row_of(dm.dates.front());
  assert(r0_ + dm.dates.size() <= s.days() && std::equal(dm.dates.begin(), dm.dates.end(), s.dates.begin() + static_cast<std::ptrdiff_t>(r0_)) &&
         "评估日 = 原料带的连续行");
  const int nl = static_cast<int>(basket_table().size()), A = static_cast<int>(s.A()), P = static_cast<int>(ds_);
  rx_.reset(nl, P);
  std::fill(rx_.hold.begin(), rx_.hold.end(), factor::stat::hold_close(1));
  Book &bk = out_[Quote::kBasket], &sk = out_[Quote::kStock];
  bk.col[kBookBuy].reset(nl, P);
  sk.col[kBookRet].reset(A, P);
  bk.cap.reset(nl, P, kNaN);
  sk.cap.reset(A, P, kNaN);
  for (int j = 0; j < kMarginVals; ++j) {
    bk.bal[j].reset(nl, P, kNaN);
    sk.bal[j].reset(A, P, kNaN);
  }
  for (int m = 0; m < kPoolVals; ++m) {
    bk.val[m].reset(nl, P, kNaN);
    sk.val[m].reset(A, P, kNaN);
  }
}

void BasketPass::chunk(misc::exec::ForkJoin &ex, const Block &c) {
  const pool::Strip &s = *pk_->strip;
  const BasketTable &bt = basket_table();
  const ConceptTable &ct = concepts();
  const YPlanes &y = c.y;
  assert(y.sh.seg == 1 && y.hd.n == 1 && y.hd.k[0] == factor::stat::hold_close(1) && y.eo && y.labels[0]);
  const LegPlane &lab = *y.labels[0];
  const factor::stat::Span sp = c.span();
  const size_t T = static_cast<size_t>(sp.T), A = static_cast<size_t>(sp.A), nl = bt.size();
  const size_t rc = r0_ + c.ck.d0, r_stat = r0_ + warm_;
  const int t_lo = sp.t_lo, t_hi = sp.t_hi(), n_stat = t_hi - t_lo;

  // 1. 块内各建仓行 t 对应次一交易日持仓的子池位集 in [T][A] + 统计行的持仓权 Weigh [n_stat]
  std::vector<PoolMask> in(T * A, 0);
  ex.tasks(sp.T, [&](int t, int) {
    const size_t k = static_cast<size_t>(t);
    std::span<PoolMask> row(in.data() + k * A, A);
    pk_->pools(s.holding_after(rc + k), row);
    const uint8_t *g = y.gate + k * A;
    for (size_t a = 0; a < A; ++a)
      if (!g[a])
        row[a] = 0;
  });
  std::vector<Weigh> wt;
  wt.reserve(static_cast<size_t>(n_stat));
  for (int t = t_lo; t < t_hi; ++t)
    wt.emplace_back(*pk_, s.holding_after(rc + static_cast<size_t>(t)));

  // 2. 可成交篮子批收益 → rx_ (子池按位集; 母集算一次直接广播给该股全部概念, 零二分)
  const uint8_t *ind_base = s.ind.data() + rc * A;
  const PoolMask base_bit = pool_bit(kBasketBase);
  ex.rows(n_stat, [&](int u0, int u1, int) {
    std::vector<double> acc(2 * nl);
    for (int t = t_lo + u0; t < t_lo + u1; ++t) {
      std::fill(acc.begin(), acc.end(), 0.0);
      const factor::stat::Chain ch = factor::stat::chain_of(t, sp, y.hd.k[0]);
      const Weigh &w = wt[static_cast<size_t>(t - t_lo)];
      const size_t row = static_cast<size_t>(t) * A;
      for (size_t a = 0; a < A; ++a) {
        const size_t i = row + a;
        const PoolMask pm = in[i];
        if (!pm || !lab.m[i])
          continue;
        const float lv[factor::stat::kLegs] = {factor::cpu::stat::h2f(lab.v[factor::stat::kLong][i]), factor::cpu::stat::h2f(lab.v[factor::stat::kShort][i])};
        const factor::stat::Cell<PoolMask> cl{ch, in.data() + i, lab.m.data() + i, y.eo + i, lv};
        const Weigh::Cell wc = w.of(a);
        const auto add = [&](size_t l, double om, float r) { acc[2 * l] += om * r, acc[2 * l + 1] += om; };
        bool base_held = false;
        float base_ret = 0.0f;
        for (PoolMask m = pm; m; m &= m - 1) {
          const PoolId p = pool_first(m);
          const PoolMask bit = pool_bit(p);
          const auto pred = [bit](PoolMask v) { return (v & bit) != 0; };
          if (!cl.held(pred))
            continue;
          const float r = cl.ret(cl.continues(pred));
          if (p == kBasketBase)
            base_held = true, base_ret = r;
          const size_t l = bt.at_pool(p);
          add(l, wc(bt, l), r);
        }
        if (!(pm & base_bit))
          continue;
        const double om = wc(pool::kRebalDay, pool::kWeightCap);
        if (base_held)
          for (const uint16_t k : ct.concepts_of(s.axis.global(a)))
            add(bt.at_concept(k), om, base_ret);
        if (const uint8_t ind = ind_base[i]; ind > 0) {
          const auto pred = [&, ind](const PoolMask &v) { return (v & base_bit) != 0 && ind_base[static_cast<size_t>(&v - in.data())] == ind; };
          if (cl.held(pred))
            add(bt.at_ind(ind), om, cl.ret(cl.continues(pred)));
        }
      }
      const size_t u = static_cast<size_t>(c.land0() + t);
      for (size_t l = 0; l < nl; ++l)
        if (acc[2 * l + 1] > 0.0)
          rx_.x[l * ds_ + u] = static_cast<float>(acc[2 * l] / acc[2 * l + 1]);
    }
  });

  // 3. 个股 ret / val / cap + 篮子账 Sleeve (buy / val / cap, 与可成交指数在实现行 u 同行对齐)
  const Leaf *lf[kPoolVals] = {};
  for (int m = 0; m < kPoolVals; ++m) {
    lf[m] = c.x.find(val_[m]);
    assert(lf[m] && "估值叶未钉在块上");
  }
  Book &bk = out_[Quote::kBasket], &sk = out_[Quote::kStock];
  const auto mcap = [&](size_t r, size_t a) { return static_cast<double>(s.mcap[r * A + a]); };
  ex.rows(n_stat, [&](int u0, int u1, int) {
    Sleeve lg;
    Members to;
    std::vector<double> buy(nl), vs(nl), vn(nl), cs(nl), ws(nl), ms(kMarginVals * nl), mb(kMarginVals * nl), mn(kMarginVals * nl);
    const auto step = [&](size_t u) {
      const size_t r = r_stat + u;
      members(*pk_, s.dates[r], to);
      std::fill(buy.begin(), buy.end(), 0.0);
      lg.roll(to, buy);
      if (u > 1)
        for (size_t l = 0; l < nl; ++l)
          if (!pool::rebal_at(bt.rebal(l), s.dates[r - 1], s.dates[r]))
            buy[l] = 0.0;
      lg.pass([&](uint32_t a) { return s.rel(r - 1, r, a); }, {});
    };
    const size_t u_first = static_cast<size_t>(c.land0() + t_lo + u0);
    if (u_first > 1)
      step(u_first - 1);
    for (int t = t_lo + u0; t < t_lo + u1; ++t) {
      const size_t u = static_cast<size_t>(c.land0() + t), r = r_stat + u, row = static_cast<size_t>(t) * A;
      for (size_t a = 0; a < A; ++a) {
        if (u > 0 && r > 0)
          sk.col[kBookRet].v[a * ds_ + u] = s.rel(r - 1, r, a) - 1.f;
        const double mc = mcap(r, a);
        if (!(mc > 0.0))
          continue;
        sk.cap.v[a * ds_ + u] = static_cast<float>(mc);
        for (size_t j = 0; j < kMarginVals; ++j)
          if (const Leaf &f = *lf[kValRz + j]; f.m[row + a])
            sk.bal[j].v[a * ds_ + u] = f.v[row + a];
        const double dy = r > 0 && mcap(r - 1, a) > 0.0 ? mcap(r - 1, a) : static_cast<double>(kNaN);
        for (int m = 0; m < kPoolVals; ++m)
          if (lf[m]->m[row + a])
            sk.val[m].v[a * ds_ + u] = static_cast<float>(lf[m]->v[row + a] * (m == kValDy ? dy : 1.0) / mc);
      }
      if (u == 0 || r == 0)
        continue;
      step(u);
      for (size_t l = 0; l < nl; ++l)
        bk.col[kBookBuy].v[l * ds_ + u] = static_cast<float>(buy[l]);
      for (int m = 0; m < kValRz; ++m) {
        std::fill(vs.begin(), vs.end(), 0.0), std::fill(vn.begin(), vn.end(), 0.0);
        lg.hold.scatter([&](uint32_t a, double &v) { return std::isfinite(v = sk.val[m].v[a * ds_ + u]); }, vs, vn);
        for (size_t l = 0; l < nl; ++l)
          if (vn[l] > 0.0)
            bk.val[m].v[l * ds_ + u] = static_cast<float>(vs[l] / vn[l]);
      }
      // 总市值 / 两融余额 (成员不加权求和) + 两融口 (同估值口按持仓权平均个股占比: 等权 = 均值, 加权 = Σ余额 / Σ市值);
      // 有市值的成员都计入, 非两融标的余额 / 占比计 0; 成员全无余额 → NaN
      std::fill(cs.begin(), cs.end(), 0.0), std::fill(ws.begin(), ws.end(), 0.0);
      std::fill(ms.begin(), ms.end(), 0.0), std::fill(mb.begin(), mb.end(), 0.0), std::fill(mn.begin(), mn.end(), 0.0);
      for (size_t k = 0; k < lg.hold.size(); ++k) {
        const uint32_t a = lg.hold.id[k];
        const double mc = mcap(r, a);
        if (!(mc > 0.0))
          continue;
        double bal[kMarginVals];
        bool has[kMarginVals];
        for (size_t j = 0; j < kMarginVals; ++j) {
          const float x = sk.bal[j].v[a * ds_ + u];
          has[j] = std::isfinite(x);
          bal[j] = has[j] ? static_cast<double>(x) : 0.0;
        }
        for (const Members::Weight &e : lg.hold.of(k)) {
          cs[e.key] += mc, ws[e.key] += e.w;
          for (size_t j = 0; j < kMarginVals; ++j) {
            const size_t i = j * nl + e.key;
            ms[i] += e.w * bal[j] / mc, mb[i] += bal[j], mn[i] += has[j] ? 1.0 : 0.0;
          }
        }
      }
      for (size_t l = 0; l < nl; ++l) {
        if (!(cs[l] > 0.0))
          continue;
        bk.cap.v[l * ds_ + u] = static_cast<float>(cs[l]);
        for (size_t j = 0; j < kMarginVals; ++j)
          if (const size_t i = j * nl + l; mn[i] > 0.0) {
            bk.val[kValRz + j].v[l * ds_ + u] = static_cast<float>(ms[i] / ws[l]);
            bk.bal[j].v[l * ds_ + u] = static_cast<float>(mb[i]);
          }
      }
    }
  });
}

Baskets BasketPass::finish(misc::exec::ForkJoin &ex) {
  out_[Quote::kBasket].col[kBookRet] = fold_book(ex, 1, rx_, 1);
  rx_ = RowLines{};
  return std::move(out_);
}

BookLines pool_line(misc::exec::ForkJoin &ex, const Block &c) {
  const YPlanes &y = c.y;
  assert(y.hd.n >= 1 && y.eo && "Y 未钉");
  const factor::stat::Span sp = c.span();
  const int r0 = sp.t_lo, r1 = sp.t_hi();
  const size_t nT = static_cast<size_t>(r1 - r0), nA = static_cast<size_t>(sp.A), H = static_cast<size_t>(y.hd.n);
  RowLines x;
  x.reset(y.hd.n, r1 - r0);
  std::copy_n(y.hd.k, H, x.hold.begin());
  ex.rows(r1 - r0, [&](int u0, int u1, int) {
    std::vector<double> acc(2 * H);
    for (int t = r0 + u0; t < r0 + u1; ++t) {
      std::fill(acc.begin(), acc.end(), 0.0);
      const size_t row = static_cast<size_t>(t) * nA;
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = row + a;
        for (size_t hi = 0; hi < H; ++hi) {
          const LegPlane &lab = *y.labels[hi];
          if (!lab.m[i])
            continue;
          const factor::stat::Chain ch = factor::stat::chain_of(t, sp, y.hd.k[hi]);
          const float lv[factor::stat::kLegs] = {factor::cpu::stat::h2f(lab.v[factor::stat::kLong][i]), factor::cpu::stat::h2f(lab.v[factor::stat::kShort][i])};
          const factor::stat::Cell<uint8_t> cl{ch, lab.m.data() + i, lab.m.data() + i, y.eo + i, lv};
          const auto pred = [](uint8_t v) { return v != 0; };
          if (cl.held(pred))
            acc[2 * hi] += cl.ret(cl.continues(pred)), acc[2 * hi + 1] += 1.0;
        }
      }
      for (size_t hi = 0; hi < H; ++hi)
        if (acc[2 * hi + 1] > 0.0)
          x.x[hi * nT + static_cast<size_t>(t - r0)] = static_cast<float>(acc[2 * hi] / acc[2 * hi + 1]);
    }
  });
  return fold_book(ex, y.sh.seg, x, 1);
}

} // namespace GUI::Engine
