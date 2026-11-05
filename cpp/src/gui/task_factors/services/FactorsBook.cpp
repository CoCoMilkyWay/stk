// FactorsBook — 见头文件. include 因子契约 / Stat 行核头 (h2f), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsBook.hpp"

#include "factor/Stat/Host.hpp" // h2f
#include "shared/Basket.hpp"    // kWeights / cut_count / cut_of / concept_cut / basket_line

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <span>

namespace GUI::Factors {

BookLines fold_book(misc::exec::ForkJoin &ex, int seg, const RowLines &r, int stride) {
  const factor::Geo g = factor::geo_of(seg);
  const int n = r.n, T = r.T;
  assert(n >= 1 && stride >= 1 && T % stride == 0 && T % g.S == 0);
  assert(r.hold.size() == static_cast<size_t>(n) && r.at_exit.size() == r.hold.size() && r.x.size() == static_cast<size_t>(n) * T);
  BookLines out;
  out.n = n, out.P = T / stride;
  out.v.assign(static_cast<size_t>(n) * static_cast<size_t>(out.P), 0.f);
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
      float *dst = out.v.data() + static_cast<size_t>(i) * static_cast<size_t>(out.P);
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

// 篮子 (Baskets::index / buy / val / stock*; pool_line = 单池篮子 0 等权): 成员位行 = 当日池 (D−1 收盘定, 盘中 as-of, 整日常量), 行 t 的收益按上一有效 close 行的成员.
//   B 的日频盘后列 (复权因子 / 股本 / 财务 / 行业) 经 as-of 已是当日适用值, 与本行 close 配对.
//   index_rows 出指数 (fold_book, 标签; 顺延在标签里, 建仓 / 续持按 Stat 契约【仓位感知】); basket_rows 出估值 + 各篮子两权重的逐行收益 / 权和 (BasketRows,
//   只给增仓漂移; 停牌行不更新 ⇒ 权带到复牌); basket_buy 用它算增仓. 收益成分两者同一份 prev_rows + each_basket (成分格 = 上一有效 close 行合判据).
//   全部篮子同一遍 A 轴: 每格按成员位逐个进它所属的篮子 (each_basket)

// 各篮子逐行 [篮子线 · T]: 成分收益 R = Σ权·r / Σ权 (无成分 0) / 权和 Σ权 (等权 = 成分数; 0 = 非成分行)
struct BasketRows {
  std::vector<double> r, w;
};

constexpr PoolMask kAllBit = PoolMask{1} << kAllPool;

// 成员位 m + 行业 ind + 所属概念 cs → 所属篮子逐个 fn(b, bit, k): 池整体 = 成员位含 bit (k = 0); all 内行业 k = 含 kAllBit ∧ ind = k (行业未知的只进篮子 0);
//   all 内概念 = 含 kAllBit (k = 0: 概念成分全期不变, 同篮子即同在 all)
template <class Fn>
void each_basket(PoolMask m, uint8_t ind, std::span<const uint16_t> cs, Fn &&fn) {
  if (m & kAllBit) {
    fn(size_t{0}, kAllBit, uint8_t{0});
    if (ind > 0)
      fn(static_cast<size_t>(ind), kAllBit, ind);
    for (const uint16_t k : cs)
      fn(concept_cut(k), kAllBit, uint8_t{0});
  }
  for (PoolMask r = m & ~kAllBit; r; r &= r - 1) {
    const PoolId p = pool_first(r);
    fn(cut_of(p), pool_bit(p), uint8_t{0});
  }
}

// 每格的上一有效 close 行 [T][A] (有效 = close.m ∧ close > 0; −1 = 无 / 本格无效): 资产条带并行沿 t 推 (跨日, 停牌带着)
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

// 篮子指数 (Baskets::index / pool_line) = fold_book, 全部持有期一遍: 行 t 一批 = 篮子 b 的成分 (该持有期标签有效 ∧ 篮子判据) 里做多侧真持有的格
//   (Stat 契约【仓位感知】: 沿交接链看同一篮子), 收益按续持取名义值, 按权平均 (等权 1 / 加权 mcap; Σ权 ≤ 0 → 0)
//   → 线 nl · hi + nw · b + w 逐行收益 (stride 1; nl = nb · nw). nw = 权重数 (1 = 只等权; mcap 可空).
//   each(hi, i, put): 标签有效格 i 所属的篮子逐个 put(b, code, in): code = 成员码平面 (已偏到本格), in(code) = 是否同一篮子
template <class Each>
BookLines index_rows(misc::exec::ForkJoin &ex, const Shape &sh, const Holds &hd, const LegPlane *const *labels, const uint8_t *eo, size_t nb, int nw,
                     const float *mcap, Each &&each) {
  static_assert(kWeightEq == 0, "只等权 (nw = 1) 的篮子线 = b");
  assert(hd.n >= 1 && (nw == 1 || mcap));
  const factor::stat::Span sp{sh.T, sh.A, factor::geo_of(sh.seg)};
  const size_t nT = static_cast<size_t>(sh.T), nA = static_cast<size_t>(sh.A), nl = nb * static_cast<size_t>(nw), H = static_cast<size_t>(hd.n);
  RowLines x; // [hold][篮子线][T] 行 t 这一批的篮子收益
  x.reset(static_cast<int>(H * nl), sh.T);
  for (size_t i = 0; i < H * nl; ++i)
    x.hold[i] = hd.k[i / nl];
  ex.rows(sh.T, [&](int t0, int t1, int) {
    std::vector<double> s(2 * H * nl); // [hold][篮子线][Σ权·r, Σ权]
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      std::fill(s.begin(), s.end(), 0.0);
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = t * nA + a;
        const double om[kWeights] = {1.0, nw > 1 ? mcap[i] : 0.0};
        for (size_t hi = 0; hi < H; ++hi) {
          const LegPlane &lab = *labels[hi];
          if (!lab.m[i])
            continue;
          const factor::stat::Chain c = factor::stat::chain_of(static_cast<int>(t), sp, hd.k[hi]);
          const float y[factor::stat::kLegs] = {factor::cpu::stat::h2f(lab.v[factor::stat::kLong][i]), factor::cpu::stat::h2f(lab.v[factor::stat::kShort][i])};
          each(hi, i, [&]<class Code, class In>(size_t b, const Code *code, const In &in) {
            const factor::stat::Cell<Code> cl{c, code, lab.m.data() + i, eo + i, y};
            if (!cl.held(in))
              return;
            const double r = cl.ret(cl.continues(in));
            for (int w = 0; w < nw; ++w) {
              double *q = s.data() + 2 * (hi * nl + b * static_cast<size_t>(nw) + static_cast<size_t>(w));
              q[0] += om[w] * r, q[1] += om[w];
            }
          });
        }
      }
      for (size_t l = 0; l < H * nl; ++l)
        if (s[2 * l + 1] > 0.0)
          x.x[l * nT + t] = static_cast<float>(s[2 * l] / s[2 * l + 1]);
    }
  });
  return fold_book(ex, sh.seg, x, 1);
}

// 篮子估值 (Baskets::val, 两权重) + 各篮子逐行收益 / 权和 (rows, 增仓用) + 个股的收益 / 收益率 (Baskets::stock / stock_val = 每格的个股项本身), 两遍:
//   1) 资产条带并行, 沿 t 把 B.px 的副本补成上一有效收盘价 (停牌带着);
//   2) 行并行, 每行一遍 A 轴:
//      收益成分 = 上一有效 close 行 p (prev) 的成员位 (行业按行 p), 收益 = 后复权 close / 上一有效后复权 close − 1, 权 = 1 / 行 p 总市值 mcap;
//      估值成分 = 上一行的成员位 (行业按本行), 市值 = px × 本行总股本, 估值输入 val [PoolVal] 取本行 (盘后列 as-of = 当日适用; DY 为股息率, 分子在此还原:
//      Fund 分母 = 算它那日 (前一交易日末行) 的收盘市值 = 该行收盘 × 该行股本)
void basket_rows(misc::exec::ForkJoin &ex, const BPlanes &B, const std::vector<int32_t> &prev, Baskets &o, BasketRows &rows) {
  const factor::check::Plane &close = B.close;
  const PoolMask *in = B.in.data();
  const float *mcap = B.mcap.data(), *sh = B.shares.data();
  const uint8_t *ind = B.ind.data();
  std::vector<float> pxv = B.px;
  float *px = pxv.data();
  const size_t nT = static_cast<size_t>(B.sh.T), nA = static_cast<size_t>(B.sh.A), S = static_cast<size_t>(B.sh.seg), nb = cut_count();
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
  for (int m = 0; m < kPoolVals; ++m)
    o.val[m].assign(basket_count() * nT, kNaN), o.stock_val[m].assign(nA * nT, kNaN);
  o.stock.n = B.sh.A, o.stock.P = B.sh.T;
  o.stock.v.assign(nA * nT, 0.f);
  rows.r.assign(basket_count() * nT, 0.0), rows.w.assign(basket_count() * nT, 0.0);
  ex.rows(B.sh.T, [&](int t0, int t1, int) {
    // 收益 [篮子][Weight][Σ权·r, Σ权]; 估值 [篮子][Weight][PoolVal][Σ权·收益率, Σ权] (收益率 = 分子 / 市值; 权 = 等权 1 / 加权 市值 ⇒ 整体法 Σ分子 / Σ市值)
    std::vector<double> x(nb * kWeights * 2), v(nb * kWeights * kPoolVals * 2);
    const auto xq = [&](size_t b, int w) { return x.data() + 2 * (b * kWeights + static_cast<size_t>(w)); };
    const auto vq = [&](size_t b, int w, int m) { return v.data() + 2 * ((b * kWeights + static_cast<size_t>(w)) * kPoolVals + static_cast<size_t>(m)); };
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      std::fill(x.begin(), x.end(), 0.0);
      std::fill(v.begin(), v.end(), 0.0);
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = t * nA + a;
        if (const int32_t q = prev[i]; q >= 0) {
          const size_t p = static_cast<size_t>(q) * nA + a;
          const double r = close.v[i] / close.v[p] - 1.0;
          o.stock.v[a * nT + t] = static_cast<float>(r);
          if (in[p]) {
            const double om[kWeights] = {1.0, mcap[p]};
            each_basket(in[p], ind[p], B.concepts_of(a), [&](size_t b, PoolMask, uint8_t) {
              for (int w = 0; w < kWeights; ++w) {
                double *e = xq(b, w);
                e[0] += om[w] * r, e[1] += om[w];
              }
            });
          }
        }
        const double mc = static_cast<double>(px[i]) * sh[i];
        if (!(mc > 0.0)) // 无价 / 缺股本 (NaN) / 非正
          continue;
        double num[kPoolVals];
        for (int m = 0; m < kPoolVals; ++m)
          num[m] = B.val[m][i];
        const size_t d0 = t - t % S; // 当日首行
        num[kValDy] *= d0 > 0 ? static_cast<double>(px[(d0 - 1) * nA + a]) * sh[(d0 - 1) * nA + a] : static_cast<double>(kNaN);
        for (int m = 0; m < kPoolVals; ++m)
          o.stock_val[m][a * nT + t] = static_cast<float>(num[m] / mc);
        if (t == 0 || !in[i - nA])
          continue;
        const double om[kWeights] = {1.0, mc};
        each_basket(in[i - nA], ind[i], B.concepts_of(a), [&](size_t b, PoolMask, uint8_t) {
          for (int m = 0; m < kPoolVals; ++m)
            if (std::isfinite(num[m]))
              for (int w = 0; w < kWeights; ++w) {
                double *e = vq(b, w, m);
                e[0] += om[w] * (num[m] / mc), e[1] += om[w];
              }
        });
      }
      for (size_t b = 0; b < nb; ++b)
        for (int w = 0; w < kWeights; ++w) {
          const size_t l = basket_line(b, static_cast<Weight>(w));
          if (const double *e = xq(b, w); e[1] > 0.0)
            rows.r[l * nT + t] = e[0] / e[1], rows.w[l * nT + t] = e[1];
          for (int m = 0; m < kPoolVals; ++m)
            if (const double *e = vq(b, w, m); e[1] > 0.0)
              o.val[m][l * nT + t] = static_cast<float>(e[0] / e[1]);
        }
    }
  });
}

// 各篮子的逐行增仓 (Baskets::buy): 行并行, 每行一遍 A 轴, 成分 / 权与 basket_rows 同一份. 线 l 的上一成分行 pm 几乎恒 = t − 1:
//   先按本资产在 t − 1 行的成员位记下它在各线的漂移权 (按资产戳), pm 更早的线 (篮子空过) 现查
void basket_buy(misc::exec::ForkJoin &ex, const BPlanes &B, const std::vector<int32_t> &prev, const BasketRows &rows, BookLines &buy) {
  const factor::check::Plane &close = B.close;
  const PoolMask *in = B.in.data();
  const float *mcap = B.mcap.data();
  const uint8_t *ind = B.ind.data();
  const size_t nT = static_cast<size_t>(B.sh.T), nA = static_cast<size_t>(B.sh.A), nl = basket_count();
  std::vector<int32_t> pm(nl * nT, -1); // [篮子线 · T] 上一成分行 (−1 = 无)
  for (size_t l = 0; l < nl; ++l)
    for (size_t t = 1; t < nT; ++t) {
      const size_t at = l * nT + t;
      pm[at] = rows.w[at - 1] > 0.0 ? static_cast<int32_t>(t - 1) : pm[at - 1];
    }
  buy.n = static_cast<int>(nl), buy.P = B.sh.T;
  buy.v.assign(nl * nT, 0.f);
  // 资产 a 在行 t 的成分格 (上一有效 close 格, 成员位非空); 不是成分 → SIZE_MAX
  const auto cell = [&](size_t t, size_t a) {
    const int32_t q = prev[t * nA + a];
    const size_t c = q < 0 ? SIZE_MAX : static_cast<size_t>(q) * nA + a;
    return c != SIZE_MAX && in[c] ? c : SIZE_MAX;
  };
  // 资产 a 在线 l 的成分行 t (成分格 c, 权 ω) 之后漂移的权 ω / Σω · (1 + r) / (1 + R)
  const auto drift = [&](size_t t, size_t a, size_t c, size_t l, double om) {
    const size_t at = l * nT + t;
    return om / rows.w[at] * (close.v[t * nA + a] / close.v[c]) / (1.0 + rows.r[at]);
  };
  const auto omega = [&](size_t c, int w) { return w == kWeightEq ? 1.0 : static_cast<double>(mcap[c]); };
  ex.rows(B.sh.T, [&](int t0, int t1, int) {
    std::vector<uint32_t> mark(nl, 0); // [篮子线] 记下 dw 的资产戳
    std::vector<double> dw(nl);        // [篮子线] 本资产在 t − 1 行之后漂移的权
    uint32_t stamp = 0;
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t)
      for (size_t a = 0; a < nA; ++a) {
        const size_t c = cell(t, a);
        if (c == SIZE_MAX)
          continue;
        ++stamp;
        if (const size_t c1 = t > 0 ? cell(t - 1, a) : SIZE_MAX; c1 != SIZE_MAX)
          each_basket(in[c1], ind[c1], B.concepts_of(a), [&](size_t b, PoolMask, uint8_t) {
            for (int w = 0; w < kWeights; ++w)
              if (const size_t l = basket_line(b, static_cast<Weight>(w)); rows.w[l * nT + t - 1] > 0.0)
                mark[l] = stamp, dw[l] = drift(t - 1, a, c1, l, omega(c1, w));
          });
        // 更早的成分行 p: 本资产在 p 是线 l (切片 b) 的成分 → 漂移权, 否则 0
        const auto held = [&](size_t p, size_t b, size_t l, int w) {
          const size_t cp = cell(p, a);
          bool hit = false;
          if (cp != SIZE_MAX)
            each_basket(in[cp], ind[cp], B.concepts_of(a), [&](size_t x, PoolMask, uint8_t) { hit |= x == b; });
          return hit ? drift(p, a, cp, l, omega(cp, w)) : 0.0;
        };
        each_basket(in[c], ind[c], B.concepts_of(a), [&](size_t b, PoolMask, uint8_t) {
          for (int w = 0; w < kWeights; ++w) {
            const size_t l = basket_line(b, static_cast<Weight>(w)), at = l * nT + t;
            if (!(rows.w[at] > 0.0))
              continue;
            const int32_t p = pm[at];
            const double d = p < 0                             ? 0.0
                             : static_cast<size_t>(p) == t - 1 ? (mark[l] == stamp ? dw[l] : 0.0)
                                                               : held(static_cast<size_t>(p), b, l, w);
            buy.v[at] += static_cast<float>(std::max(0.0, omega(c, w) / rows.w[at] - d));
          }
        });
      }
  });
}

} // namespace

Baskets baskets(misc::exec::ForkJoin &ex, const BPlanes &B) {
  const size_t n = B.sh.n(), A = static_cast<size_t>(B.sh.A);
  assert(B.in.size() == n && B.eo.size() == n && B.lab.m.size() == n && B.close.v.size() == n && "B 未装 (load_b)");
  Baskets o;
  // 指数的成员码: 池整体 / 概念 = 成员位本身; 行业 = all 内行业码 ind0 (含 kAllBit 才是 ind_l1, 否则 0)
  std::vector<uint8_t> ind0(n);
  ex.rows(B.sh.T, [&](int t0, int t1, int) {
    for (size_t i = static_cast<size_t>(t0) * A; i < static_cast<size_t>(t1) * A; ++i)
      ind0[i] = (B.in[i] & kAllBit) ? B.ind[i] : uint8_t{0};
  });
  const PoolMask *in = B.in.data();
  const Holds hd = keys_of<Holds>(std::vector<int>{factor::stat::hold_close(1)});
  const LegPlane *const lab = &B.lab;
  o.index = index_rows(ex, B.sh, hd, &lab, B.eo.data(), cut_count(), kWeights, B.mcap.data(),
                       [&](size_t, size_t i, auto &&put) {
                         each_basket(in[i], ind0[i], B.concepts_of(i % A), [&](size_t b, PoolMask bit, uint8_t k) {
                           if (k > 0)
                             put(b, ind0.data() + i, [k](uint8_t v) { return v == k; });
                           else
                             put(b, in + i, [bit](PoolMask v) { return (v & bit) != 0; });
                         });
                       });
  const std::vector<int32_t> prev = prev_rows(ex, B.close, B.sh.T, B.sh.A);
  BasketRows rows;
  basket_rows(ex, B, prev, o, rows);
  basket_buy(ex, B, prev, rows, o.buy);
  return o;
}

BookLines pool_line(misc::exec::ForkJoin &ex, const YPlanes &y) {
  assert(y.eo && "Y 未钉");
  return index_rows(ex, y.sh, y.hd, y.labels, y.eo, 1, 1, nullptr,
                    [&](size_t hi, size_t i, auto &&put) { put(size_t{0}, y.labels[hi]->m.data() + i, [](uint8_t v) { return v != 0; }); });
}

} // namespace GUI::Factors
