// Ledger — 见头文件. include 因子 Stat 账集头 (book_rows), 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/Ledger.hpp"

#include "define/Members.hpp"
#include "factor/Stat/Book.hpp"            // book_rows / MaskBook
#include "features/Storage/FeatureLib.hpp" // pool_id
#include "shared/Basket.hpp"               // basket_table / Weigh / members

#include <algorithm>
#include <cassert>
#include <limits>
#include <span>
#include <utility>

namespace GUI::Engine {

namespace st = factor::stat;

std::array<LeafId, kPoolVals> pool_val_leaves(const FeatureTable &ft) {
  const std::string pre = std::string(tier_name(TierId::Bar)) + "." + kLevelToken[L_1day] + ".";
  std::array<LeafId, kPoolVals> v{};
  for (int m = 0; m < kPoolVals; ++m) {
    const auto it = ft.x.index.find(pre + kPoolValCols[m]);
    assert(it != ft.x.index.end() && "估值列不在 bar.1d 叶表里");
    v[static_cast<size_t>(m)] = it->second;
  }
  return v;
}

namespace {

constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();

// 篮子 + 个股账集 (Stat/Book.hpp): 本 = 篮子号 b (BasketTable::each 枚举, 持仓权 Weigh) | 个股 nl + a (自身, 权 1); 一律按真持有权和归一
//   判据: 子池 = 位集含该池; 行业 = 含母集且源行行业同; 概念 = 含母集 (概念成分恒定)
struct BasketBooks {
  const BasketTable &bt;
  const ConceptTable &ct;
  const pool::Strip &s;
  const PoolMask *in; // 块首 [T][A] 各建仓行对应持仓日的子池位集 (门控外 0)
  const uint8_t *ind; // 块首 [T][A] 源行行业
  const uint8_t *m;   // 块首 [T][A] 标签有效位
  const std::vector<Weigh> &wt;
  int t_w = 0, A = 0;
  size_t size() const { return bt.size() + static_cast<size_t>(A); }
  double cap(size_t) const { return 0.0; }
  template <class Put>
  void each(int t, uint32_t a, Put &&put) const {
    const size_t i = static_cast<size_t>(t) * A + a;
    const Weigh::Cell wc = wt[static_cast<size_t>(t - t_w)].of(a);
    const PoolMask base = pool_bit(kBasketBase);
    bt.each(in[i], ind[i], ct.concepts_of(s.axis.global(a)), [&](size_t b) {
      const float w = wc(bt, b);
      if (!(w > 0.f))
        return;
      const uint32_t k = static_cast<uint32_t>(b);
      switch (bt.group(b)) {
      case kGroupPool:
        put(k, w, st::InBit<PoolMask>{in + i, pool_bit(bt.pool_of(b))});
        break;
      case kGroupInd:
        put(k, w, st::InInd<PoolMask>{in + i, base, ind + i, ind[i]});
        break;
      case kGroupConcept:
        put(k, w, st::InBit<PoolMask>{in + i, base});
        break;
      }
    });
    if (m[i])
      put(static_cast<uint32_t>(bt.size() + a), 1.f, st::InMask{m + i});
  }
};

} // namespace

Need basket_need(const FeatureTable &ft) {
  assert(ft.seg == 1 && "篮子账按日");
  const std::array<LeafId, kPoolVals> val = pool_val_leaves(ft);
  // Y 只装门控 (标签取原料带 Strip, 与 Beta 选池账同一份); 前后带按 T+1 的交接链
  return {{val.begin(), val.end()}, Holds{}, Segs::None, false, bands_of(0, Holds::of(hold_bit(kHoldT1)), ft.seg), Budget{.work = static_cast<double>(sizeof(PoolMask))}, BasketPass::bytes()};
}

Bytes BasketPass::bytes() {
  const double nl = static_cast<double>(basket_table().size());
  constexpr double f = sizeof(float);
  return {.cell = (kLedgerCols + 2 + kPoolVals + kMarginVals) * f, .day = nl * (kLedgerCols + 2 + 2 * kPoolVals + kMarginVals) * f};
}

BasketPass::BasketPass(FactorFlow &F, std::shared_ptr<const pool::Picks> pk, std::span<const LeafId, kPoolVals> val) : F_(F), pk_(std::move(pk)) {
  assert(pk_ && pool_count() <= static_cast<size_t>(kPoolBits));
  std::copy(val.begin(), val.end(), val_);
}

void BasketPass::init(const Job &j) {
  const Domain &dm = j.dm;
  assert(dm.pool == kAllPool);
  warm_ = dm.warm, ds_ = dm.stat_days(), n_ = j.P.chunks.size();
  const pool::Strip &s = *pk_->strip;
  assert(s.axis.hash == dm.rs.axis.hash && s.A() == dm.rs.axis.size() && "选股表与评估域 A 轴不符");
  r0_ = s.row_of(dm.dates.front());
  assert(r0_ + dm.dates.size() <= s.days() && std::equal(dm.dates.begin(), dm.dates.end(), s.dates.begin() + static_cast<std::ptrdiff_t>(r0_)) &&
         "评估日 = 原料带的连续行");
  const int nl = static_cast<int>(basket_table().size()), A = static_cast<int>(s.A()), P = static_cast<int>(ds_), B = nl + A;
  rz_.reset(kLedgerCols * B, P, 1, 1); // 线 = 列 · B + 本 (本 = 篮子 | 个股)
  for (int i = 0; i < rz_.out.n; ++i)
    rz_.at_exit[static_cast<size_t>(i)] = i < B; // 收益列平仓行实现, 其余建仓行即付
  out = Baskets{};
  Ledger &bk = out[Quote::kBasket], &sk = out[Quote::kStock];
  bk.pf.cap.reset(nl, P, kNaN);
  sk.pf.cap.reset(A, P, kNaN);
  for (int j = 0; j < kMarginVals; ++j) {
    bk.pf.bal[j].reset(nl, P, kNaN);
    sk.pf.bal[j].reset(A, P, kNaN);
  }
  for (int m = 0; m < kPoolVals; ++m) {
    bk.pf.val[m].reset(nl, P, kNaN);
    bk.pf.alt[m].reset(nl, P, kNaN);
    sk.pf.val[m].reset(A, P, kNaN);
  }
}

Rc BasketPass::block(const Block &c, size_t ci, bool, double) {
  misc::exec::ForkJoin &ex = F_.S.ex;
  F_.prog.begin(Phase::Running, n_);
  F_.prog.publish(ci);
  const pool::Strip &s = *pk_->strip;
  const BasketTable &bt = basket_table();
  const ConceptTable &ct = concepts();
  const YPlanes &y = c.y;
  assert(y.sh.seg == 1 && y.hd.n == 0 && y.gate);
  const st::Span sp = c.span();
  const Hold hd = hold_on(kHoldT1, sp.g);
  const size_t T = static_cast<size_t>(sp.T), A = static_cast<size_t>(sp.A), nl = bt.size(), B = nl + A;
  const size_t rc = r0_ + c.ck.d0, r_stat = r0_ + warm_;
  const size_t o = rc * A; // 块首在原料带上的格
  const st::Label lb{s.lv.data() + o, s.nv.data() + o, s.m.data() + o, s.eo.data() + o};
  const int t_lo = sp.t_lo, t_hi = sp.t_hi(), n_stat = t_hi - t_lo;

  // 1. 块内各建仓行 t 对应次一交易日持仓的子池位集 in [T][A] + 持仓权 Weigh (统计行与其前一行: 交接的前一批)
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
  const int t_w = std::max(0, t_lo - hd.h);
  std::vector<Weigh> wt;
  wt.reserve(static_cast<size_t>(t_hi - t_w));
  for (int t = t_w; t < t_hi; ++t)
    wt.emplace_back(*pk_, s.holding_after(rc + static_cast<size_t>(t)));

  // 2. 篮子账 + 个股账 (同一账集): 收益 (平仓行) / 四列流水 (建仓行); 只付固定冲击. 本块逐行暂存 → Realize
  const BasketBooks books{bt, ct, s, in.data(), s.ind.data() + o, lb.m, wt, t_w, static_cast<int>(A)};
  const size_t nb = static_cast<size_t>(n_stat);
  blk_.assign(kLedgerCols * B * nb, 0.f);
  st::book_rows(ex, sp, hd, lb, books, st::Cost{}, [&](int, int t, const double *ret, const st::Flow *flow) {
    const size_t r = static_cast<size_t>(t - t_lo);
    for (size_t b = 0; b < B; ++b) {
      blk_[(kLedgerRet * B + b) * nb + r] = static_cast<float>(ret[b]);
      for (int k = kLedgerImp; k < kLedgerCols; ++k)
        blk_[(static_cast<size_t>(k) * B + b) * nb + r] = ledger_cell(flow[b], kAmtFixed, k);
    }
  });
  ex.rows(rz_.out.n, [&](int i0, int i1, int) {
    for (int i = i0; i < i1; ++i)
      rz_.fold(i, c.land0() + t_lo, blk_.data() + static_cast<size_t>(i) * nb, n_stat);
  });

  // 3. 画像: 个股七口 / 市值 / 余额 + 篮子按持仓权 (成员表 members ⇄ 随价漂移到当日收盘) 平均的七口
  const Leaf *lf[kPoolVals] = {};
  for (int m = 0; m < kPoolVals; ++m) {
    lf[m] = c.x.find(val_[m]);
    assert(lf[m] && "估值叶未钉在块上");
  }
  Ledger &bk = out[Quote::kBasket], &sk = out[Quote::kStock];
  const auto mcap = [&](size_t r, size_t a) { return static_cast<double>(s.mcap[r * A + a]); };
  ex.rows(n_stat, [&](int u0, int u1, int) {
    Members hold;
    std::vector<double> cs_(nl), os(kPoolVals * nl), ow(kPoolVals * nl), xs(kPoolVals * nl), xw(kPoolVals * nl), mb(kMarginVals * nl), mn(kMarginVals * nl);
    for (int t = t_lo + u0; t < t_lo + u1; ++t) {
      const size_t u = static_cast<size_t>(c.land0() + t), r = r_stat + u, row = static_cast<size_t>(t) * A;
      for (size_t a = 0; a < A; ++a) {
        const double mc = mcap(r, a);
        if (!(mc > 0.0))
          continue;
        sk.pf.cap.v[a * ds_ + u] = static_cast<float>(mc);
        for (size_t j = 0; j < kMarginVals; ++j)
          if (const Leaf &f = *lf[kValRz + j]; f.m[row + a])
            sk.pf.bal[j].v[a * ds_ + u] = f.v[row + a];
        const double dy = r > 0 && mcap(r - 1, a) > 0.0 ? mcap(r - 1, a) : static_cast<double>(kNaN);
        for (int m = 0; m < kPoolVals; ++m)
          if (lf[m]->m[row + a])
            sk.pf.val[m].v[a * ds_ + u] = static_cast<float>(lf[m]->v[row + a] * (m == kValDy ? dy : 1.0) / mc);
      }
      if (u == 0 || r == 0)
        continue;
      members(*pk_, s.dates[r], hold);
      hold.scale([&](uint32_t a) { return s.rel(r - 1, r, a); });
      // 七口 = 个股七口的加权平均: val = 持仓权 (篮子自己的 Weight, 随价漂移), alt = 另一种权重 (篮子加权 → 等权 = 个股均值; 等权 → 加权 = 按市值);
      // 估值口个股不可算不计入; 两融口有市值的成员都计入, 非两融标的占比计 0, 成员全无余额 → NaN. 总市值 / 两融余额 = 成员不加权求和
      std::fill(cs_.begin(), cs_.end(), 0.0), std::fill(mb.begin(), mb.end(), 0.0), std::fill(mn.begin(), mn.end(), 0.0);
      std::fill(os.begin(), os.end(), 0.0), std::fill(ow.begin(), ow.end(), 0.0), std::fill(xs.begin(), xs.end(), 0.0), std::fill(xw.begin(), xw.end(), 0.0);
      for (size_t k = 0; k < hold.size(); ++k) {
        const uint32_t a = hold.id[k];
        const double mc = mcap(r, a);
        if (!(mc > 0.0))
          continue;
        double x[kPoolVals], bal[kMarginVals];
        bool ok[kPoolVals], has[kMarginVals];
        for (size_t m = 0; m < kPoolVals; ++m) {
          const float v = sk.pf.val[m].v[a * ds_ + u];
          ok[m] = std::isfinite(v) || margin_val(static_cast<int>(m));
          x[m] = std::isfinite(v) ? static_cast<double>(v) : 0.0;
        }
        for (size_t j = 0; j < kMarginVals; ++j) {
          const float v = sk.pf.bal[j].v[a * ds_ + u];
          has[j] = std::isfinite(v);
          bal[j] = has[j] ? static_cast<double>(v) : 0.0;
        }
        for (const Members::Weight &e : hold.of(k)) {
          const double wx = bt.weight(e.key) == pool::kWeightCap ? 1.0 : mc;
          cs_[e.key] += mc;
          for (size_t m = 0; m < kPoolVals; ++m)
            if (ok[m]) {
              const size_t i = m * nl + e.key;
              os[i] += e.w * x[m], ow[i] += e.w, xs[i] += wx * x[m], xw[i] += wx;
            }
          for (size_t j = 0; j < kMarginVals; ++j) {
            const size_t i = j * nl + e.key;
            mb[i] += bal[j], mn[i] += has[j] ? 1.0 : 0.0;
          }
        }
      }
      for (size_t l = 0; l < nl; ++l) {
        if (!(cs_[l] > 0.0))
          continue;
        bk.pf.cap.v[l * ds_ + u] = static_cast<float>(cs_[l]);
        for (size_t j = 0; j < kMarginVals; ++j)
          if (const size_t i = j * nl + l; mn[i] > 0.0)
            bk.pf.bal[j].v[l * ds_ + u] = static_cast<float>(mb[i]);
        for (size_t m = 0; m < kPoolVals; ++m)
          if (const size_t i = m * nl + l; ow[i] > 0.0 && (!margin_val(static_cast<int>(m)) || mn[(m - kValRz) * nl + l] > 0.0)) {
            bk.pf.val[m].v[l * ds_ + u] = static_cast<float>(os[i] / ow[i]);
            bk.pf.alt[m].v[l * ds_ + u] = static_cast<float>(xs[i] / xw[i]);
          }
      }
    }
  });
  F_.prog.publish(1);
  return Rc::Ok;
}

void BasketPass::finish(const Job &) {
  const LedgerLines &all = rz_.out;
  const int nl = static_cast<int>(basket_table().size()), B = all.n / kLedgerCols;
  for (int k = 0; k < kLedgerCols; ++k) {
    out[Quote::kBasket].col[k] = all.slice(k * B, nl);
    out[Quote::kStock].col[k] = all.slice(k * B + nl, B - nl);
  }
  rz_ = Realize{};
  std::vector<float>().swap(blk_);
}

Rc eval_baskets(FactorFlow &F, const FactorScope &sc, Job &j, Baskets &out) {
  assert(sc.ft && sc.ft->seg == 1 && pool_id(sc.pool) == kAllPool && "池篮子在 all 池日层评估域");
  assert(sc.ft->y.has(kHoldT1) && "篮子须有 T+1 标签 (日层: 池指数 C(行) → C(D+1); 标签取原料带)");
  if (const Rc rc = open_job(F, sc, basket_need(*sc.ft), j); rc != Rc::Ok)
    return rc;
  assert(j.dm.rs.has(kAllBar));
  std::shared_ptr<const pool::Picks> pk = pool::picks(j.dm.rs.libs[kAllBar], &F.cancel);
  if (!pk) {
    const Rc rc = rc_of(false, F.S.lr);
    if (rc == Rc::Stale)
      F.S.clear();
    return rc;
  }
  BasketPass p(F, std::move(pk), pool_val_leaves(*sc.ft));
  if (const Rc rc = run_job(F, j, p); rc != Rc::Ok)
    return rc;
  out = std::move(p.out);
  return Rc::Ok;
}

LedgerLines pool_line(misc::exec::ForkJoin &ex, const Block &c) {
  const YPlanes &y = c.y;
  assert(y.hd.n >= 1 && y.eo && "Y 未钉");
  const st::Span sp = c.span();
  const int r0 = sp.t_lo, nT = sp.t_hi() - r0;
  Realize rz;
  rz.reset(y.hd.n, nT, y.sh.seg, 1);
  std::copy_n(y.hd.k, y.hd.n, rz.hold.begin());
  std::vector<float> x(static_cast<size_t>(nT));
  for (int hi = 0; hi < y.hd.n; ++hi) {
    const LabPlane &lab = *y.labels[hi];
    std::fill(x.begin(), x.end(), 0.f);
    st::book_rows(ex, sp, hold_on(y.hd.k[hi], sp.g), label_of(lab, y.eo, nullptr), st::MaskBook{lab.m.data(), sp.A}, st::Cost{},
                  [&](int, int t, const double *ret, const st::Flow *) { x[static_cast<size_t>(t - r0)] = static_cast<float>(ret[0]); });
    rz.fold(hi, 0, x.data(), nT);
  }
  return std::move(rz.out);
}

} // namespace GUI::Engine
