// FactorsBook — 见头文件. include 因子契约 / Stat 行核头 (h2f), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsBook.hpp"

#include "factor/Stat/Host.hpp" // h2f

#include <algorithm>
#include <bit>
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

// 篮子 (Baskets::index / val / turn; pool_line = 单池篮子 0 等权): 成员位行 = 当日池 (D−1 收盘定, 盘中 as-of, 整日常量), 行 t 的收益按上一有效 close 行的成员.
//   B 的日频盘后列 (复权因子 / 股本 / 财务 / 行业) 经 as-of 已是当日适用值, 与本行 close 配对.
//   index_rows 出指数 (fold_book, 标签; 顺延在标签里, 建仓 / 续持按 Stat 契约【仓位感知】); pool_baskets 出估值 + 各池整体两权重的逐行收益 / 权和 (BasketRows,
//   只给换手漂移; 停牌行不更新 ⇒ 权带到复牌); pool_turn 用它算换手. 收益成分两者同一份 prev_rows (成分格 = 上一有效 close 行合判据).
//   全部篮子同一遍 A 轴: 每格按成员位逐个进它所属的篮子 (each_basket)

// 各池整体逐行 [Weight][PoolId · T]: 成分收益 r = Σ权·r / Σ权 (无成分 0) / 权和 Σ权 (等权 = 成分数)
struct BasketRows {
  std::vector<double> r[kWeights], w[kWeights];
};

constexpr PoolMask kAllBit = PoolMask{1} << kAllPool;

// 成员位 m + 行业 ind → 所属篮子逐个 fn(b, bit, k): 池整体 = 成员位含 bit (k = 0); all 内行业 k = 含 kAllBit ∧ ind = k (行业未知的只进篮子 0)
template <class Fn>
void each_basket(PoolMask m, uint8_t ind, Fn &&fn) {
  if (m & kAllBit) {
    fn(size_t{0}, kAllBit, uint8_t{0});
    if (ind > 0)
      fn(static_cast<size_t>(ind), kAllBit, ind);
  }
  for (PoolMask r = m & ~kAllBit; r; r &= r - 1) {
    const int p = std::countr_zero(r);
    fn(Baskets::of_pool(static_cast<PoolId>(p)), PoolMask{1} << p, uint8_t{0});
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
BookLines index_rows(misc::exec::ForkJoin &ex, const Shape &sh, const Holds &hd, std::span<const LegPlane> labels, const uint8_t *eo, size_t nb, int nw,
                     const float *mcap, Each &&each) {
  static_assert(kWeightEq == 0, "只等权 (nw = 1) 的篮子线 = b");
  assert(hd.n >= 1 && labels.size() == static_cast<size_t>(hd.n) && (nw == 1 || mcap));
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
          const LegPlane &lab = labels[hi];
          if (!lab.m[i])
            continue;
          const factor::stat::Chain c = factor::stat::chain_of(static_cast<int>(t), sp, hd.k[hi]);
          const float y[factor::stat::kLegs] = {factor::cpu::stat::h2f(lab.v[factor::stat::kLong][i]), factor::cpu::stat::h2f(lab.v[factor::stat::kShort][i])};
          each(hi, i, [&]<class Code, class In>(size_t b, const Code *code, const In &in) {
            const factor::stat::Cell<Code> cl{c, code, lab.m.data() + i, eo + i, y};
            if (!cl.held(factor::stat::kLong, in))
              return;
            const double r = cl.ret(factor::stat::kLong, cl.continues(in));
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

// 篮子估值 (Baskets::val, 两权重) + 收盘价指数 (Baskets::index_px) + 各池整体逐行收益 / 权和 (rows, 换手用), 两遍:
//   1) 资产条带并行, 沿 t 把 B.px 的副本补成上一有效收盘价 (停牌带着);
//   2) 行并行, 每行一遍 A 轴:
//      收益成分 = 上一有效 close 行 p (prev) 的成员位 (行业按行 p), 收益 = 后复权 close / 上一有效后复权 close − 1, 权 = 1 / 行 p 总市值 mcap;
//      估值成分 = 上一行的成员位 (行业按本行), 市值 = px × 本行总股本, 估值输入 val [PoolVal] 取本行 (盘后列 as-of = 当日适用; DY 为股息率, 分子在此还原:
//      Fund 分母 = 算它那日 (前一交易日末行) 收盘 × 本行股本)
void pool_baskets(misc::exec::ForkJoin &ex, const BPlanes &B, const std::vector<int32_t> &prev, Baskets &o, BasketRows &rows) {
  const factor::check::Plane &close = B.close;
  const PoolMask *in = B.in.data();
  const float *mcap = B.mcap.data(), *sh = B.shares.data();
  const uint8_t *ind = B.ind.data();
  std::vector<float> pxv = B.px;
  float *px = pxv.data();
  const size_t nT = static_cast<size_t>(B.sh.T), nA = static_cast<size_t>(B.sh.A), S = static_cast<size_t>(B.sh.seg), nb = Baskets::count(), NP = pool_count();
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
  for (int w = 0; w < kWeights; ++w)
    for (int m = 0; m < kPoolVals; ++m)
      o.val[w][m].assign(nb * nT, kNaN);
  for (int w = 0; w < kWeights; ++w)
    rows.r[w].assign(NP * nT, 0.0), rows.w[w].assign(NP * nT, 0.0);
  o.index_px.n = static_cast<int>(nb) * kWeights, o.index_px.P = B.sh.T;
  o.index_px.v.assign(static_cast<size_t>(o.index_px.n) * nT, 0.f);
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
          if (in[p]) {
            const double r = close.v[i] / close.v[p] - 1.0, om[kWeights] = {1.0, mcap[p]};
            each_basket(in[p], ind[p], [&](size_t b, PoolMask, uint8_t) {
              for (int w = 0; w < kWeights; ++w) {
                double *e = xq(b, w);
                e[0] += om[w] * r, e[1] += om[w];
              }
            });
          }
        }
        if (t == 0 || !in[i - nA])
          continue;
        const double mc = static_cast<double>(px[i]) * sh[i];
        if (!(mc > 0.0)) // 无价 / 缺股本 (NaN) / 非正
          continue;
        double num[kPoolVals];
        for (int m = 0; m < kPoolVals; ++m)
          num[m] = B.val[m][i];
        const size_t d0 = t - t % S; // 当日首行
        num[kValDy] *= d0 > 0 ? static_cast<double>(px[(d0 - 1) * nA + a]) * sh[i] : static_cast<double>(kNaN);
        const double om[kWeights] = {1.0, mc};
        each_basket(in[i - nA], ind[i], [&](size_t b, PoolMask, uint8_t) {
          for (int m = 0; m < kPoolVals; ++m)
            if (std::isfinite(num[m]))
              for (int w = 0; w < kWeights; ++w) {
                double *e = vq(b, w, m);
                e[0] += om[w] * (num[m] / mc), e[1] += om[w];
              }
        });
      }
      for (size_t p = 0; p < NP; ++p)
        for (int w = 0; w < kWeights; ++w)
          if (const double *e = xq(Baskets::of_pool(static_cast<PoolId>(p)), w); e[1] > 0.0)
            rows.r[w][p * nT + t] = e[0] / e[1], rows.w[w][p * nT + t] = e[1];
      for (size_t b = 0; b < nb; ++b)
        for (int w = 0; w < kWeights; ++w)
          if (const double *e = xq(b, w); e[1] > 0.0)
            o.index_px.v[static_cast<size_t>(Baskets::index_line(b, w)) * nT + t] = static_cast<float>(e[0] / e[1]);
      for (size_t b = 0; b < nb; ++b)
        for (int w = 0; w < kWeights; ++w)
          for (int m = 0; m < kPoolVals; ++m)
            if (const double *e = vq(b, w, m); e[1] > 0.0)
              o.val[w][m][b * nT + t] = static_cast<float>(e[0] / e[1]);
    }
  });
}

// 各池整体的换手 (Baskets::turn): 成分行 t 的权 w_t = ω / Σω (等权 ω = 1, 加权 ω = mcap) 对上一成分行漂移后的权 w̃ = w (1 + r) / (1 + R) 的
//   单边 ½ Σ|w_t − w̃|, 停牌 = 移出 / 复牌 = 移回 (与 pool_baskets 同成分: prev 行成员位含该池位); 首个成分行 (建仓) 不计; 沿成分行均值.
//   Σ w̃ = 1 ⇒ ½ Σ|·| = ½ (1 + Σ_成分 (|w_t − w̃| − w̃)): 资产条带并行只走成分格, 每线程逐池逐行攒, 按 tid 序归并. rows = pool_baskets 出的
void pool_turn(misc::exec::ForkJoin &ex, const BPlanes &B, const std::vector<int32_t> &prev, const BasketRows &rows, std::vector<double> (&turn)[kWeights]) {
  const factor::check::Plane &close = B.close;
  const PoolMask *in = B.in.data();
  const float *mcap = B.mcap.data();
  const size_t nT = static_cast<size_t>(B.sh.T), nA = static_cast<size_t>(B.sh.A), nt = static_cast<size_t>(ex.threads()), NP = pool_count();
  std::vector<int> pm[kWeights];     // [Weight][PoolId · T] 上一成分行 (−1 = 无)
  std::vector<double> dev[kWeights]; // [Weight][tid · PoolId · T]
  for (int w = 0; w < kWeights; ++w) {
    pm[w].assign(NP * nT, -1);
    for (size_t p = 0; p < NP; ++p)
      for (size_t t = 1; t < nT; ++t) {
        const size_t at = p * nT + t;
        pm[w][at] = rows.w[w][at - 1] > 0.0 ? static_cast<int>(t - 1) : pm[w][at - 1];
      }
    dev[w].assign(nt * NP * nT, 0.0);
  }
  ex.rows(B.sh.A, [&](int a0, int a1, int tid) {
    const size_t na = static_cast<size_t>(a1 - a0), off = static_cast<size_t>(tid) * NP * nT;
    std::vector<double> dw[kWeights]; // [PoolId · na] 本资产在 tw 行之后漂移的权
    std::vector<int> tw[kWeights];    // [PoolId · na] 本资产上次是成分的行
    for (int w = 0; w < kWeights; ++w)
      dw[w].assign(NP * na, 0.0), tw[w].assign(NP * na, -1);
    for (size_t t = 0; t < nT; ++t)
      for (int a = a0; a < a1; ++a) {
        const size_t i = t * nA + static_cast<size_t>(a);
        const int32_t q = prev[i];
        if (q < 0)
          continue;
        const size_t ip = static_cast<size_t>(q) * nA + static_cast<size_t>(a);
        if (!in[ip])
          continue;
        const double r = close.v[i] / close.v[ip] - 1.0, om[kWeights] = {1.0, mcap[ip]};
        for (PoolMask rest = in[ip]; rest; rest &= rest - 1) {
          const size_t p = static_cast<size_t>(std::countr_zero(rest)), j = p * na + static_cast<size_t>(a - a0), at = p * nT + t;
          for (int w = 0; w < kWeights; ++w) {
            const double W = rows.w[w][at];
            if (!(W > 0.0))
              continue;
            const double wt = om[w] / W, d = tw[w][j] == pm[w][at] ? dw[w][j] : 0.0;
            dev[w][off + at] += std::abs(wt - d) - d;
            dw[w][j] = wt * (1.0 + r) / (1.0 + rows.r[w][at]), tw[w][j] = static_cast<int>(t);
          }
        }
      }
  });
  for (int w = 0; w < kWeights; ++w) {
    turn[w].assign(NP, 0.0);
    for (size_t p = 0; p < NP; ++p) {
      double s = 0.0;
      int n = 0;
      for (size_t t = 0; t < nT; ++t) {
        const size_t at = p * nT + t;
        if (!(rows.w[w][at] > 0.0) || pm[w][at] < 0)
          continue;
        double x = 0.0;
        for (size_t j = 0; j < nt; ++j)
          x += dev[w][j * NP * nT + at];
        s += 0.5 * (1.0 + x), ++n;
      }
      turn[w][p] = n > 0 ? s / n : 0.0;
    }
  }
}

} // namespace

Baskets baskets(misc::exec::ForkJoin &ex, const BPlanes &B) {
  const size_t n = B.sh.n(), A = static_cast<size_t>(B.sh.A);
  assert(B.in.size() == n && B.eo.size() == n && B.lab.m.size() == n && B.close.v.size() == n && "B 未装 (load_b)");
  Baskets o;
  // 指数的成员码: 池整体 = 成员位本身; 行业 = all 内行业码 ind0 (含 kAllBit 才是 ind_l1, 否则 0)
  std::vector<uint8_t> ind0(n);
  ex.rows(B.sh.T, [&](int t0, int t1, int) {
    for (size_t i = static_cast<size_t>(t0) * A; i < static_cast<size_t>(t1) * A; ++i)
      ind0[i] = (B.in[i] & kAllBit) ? B.ind[i] : uint8_t{0};
  });
  const PoolMask *in = B.in.data();
  const Holds hd = keys_of<Holds>(std::vector<int>{factor::stat::hold_close(1)});
  o.index = index_rows(ex, B.sh, hd, std::span<const LegPlane>(&B.lab, 1), B.eo.data(), Baskets::count(), kWeights, B.mcap.data(),
                       [&](size_t, size_t i, auto &&put) {
                         each_basket(in[i], ind0[i], [&](size_t b, PoolMask bit, uint8_t k) {
                           if (k > 0)
                             put(b, ind0.data() + i, [k](uint8_t v) { return v == k; });
                           else
                             put(b, in + i, [bit](PoolMask v) { return (v & bit) != 0; });
                         });
                       });
  const std::vector<int32_t> prev = prev_rows(ex, B.close, B.sh.T, B.sh.A);
  BasketRows rows;
  pool_baskets(ex, B, prev, o, rows);
  pool_turn(ex, B, prev, rows, o.turn);
  return o;
}

BookLines pool_line(misc::exec::ForkJoin &ex, const YPlanes &y) {
  assert(y.eo.size() == y.sh.n());
  return index_rows(ex, y.sh, y.hd, y.labels, y.eo.data(), 1, 1, nullptr,
                    [&](size_t hi, size_t i, auto &&put) { put(size_t{0}, y.labels[hi].m.data() + i, [](uint8_t v) { return v != 0; }); });
}

} // namespace GUI::Factors
