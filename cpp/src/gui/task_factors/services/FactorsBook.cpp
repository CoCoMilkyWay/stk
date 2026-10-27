// FactorsBook — 见头文件. include 因子契约 / Stat 行核头 (h2f), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsBook.hpp"

#include "factor/Stat/Host.hpp" // h2f

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>

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

// 篮子 (Baskets::index / val / turn; pool_line 只取篮子 0 等权): 日层门控行 D = D−1 收盘定的池 (盘中 as-of), D 收盘换仓, 故 D 的收益按行 D−1 的成员.
//   B 的盘后列 (复权因子 / 股本 / 财务 / 行业) 经同一 as-of 已是行 D 当日适用值, 与 close_day 同行配对.
//   pool_index 出指数 (fold_book, 标签; 顺延在标签里, 建仓 / 续持按 Stat 契约【仓位感知】); pool_baskets 出估值 + 篮子 0 两权重的逐行收益 / 权和 (BasketRows, 只给换手漂移;
//   停牌行不更新 ⇒ 权带到复牌); pool_turn 用它算换手. 收益成分两者同一份 prev_rows (成分格 = 上一有效 close 行在池).

// 篮子 0 逐行 [Weight][T]: 成分收益 r = Σ权·r / Σ权 (无成分 0) / 权和 Σ权 (等权 = 成分数)
struct BasketRows {
  std::vector<double> r[kWeights], w[kWeights];
};

// 行业 ID → 篮子: 篮子 0 (池整体) 恒进, 行业 k ≥ 1 再进篮子 k
template <class Fn>
void each_basket(uint8_t ind, Fn &&fn) {
  fn(size_t{0});
  if (ind > 0)
    fn(static_cast<size_t>(ind));
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

// 篮子指数 (Baskets::index / pool_line) = fold_book, Y 的全部持有期一遍: 行 t 一批 = 成分 (该持有期标签有效, 已含在池; ∧ 该行 ind_l1 = k)
//   里做多侧真持有的格 (Stat 契约【仓位感知】: 篮子沿交接链 = 池整体看在池, 行业篮子看同一行业), 收益按续持取名义值, 按权平均
//   (等权 1 / 加权 mcap; Σ权 ≤ 0 → 0) → 线 nb · hi + index_line(k, w) 逐行收益 (stride 1; nb = nk · nw). nk = 篮子数
//   (1 = 只池整体; ind 可空), nw = 权重数 (1 = 只等权; mcap 可空)
BookLines pool_index(misc::exec::ForkJoin &ex, const YPlanes &Y, const float *mcap, const uint8_t *ind, int nk, int nw) {
  static_assert(kWeightEq == 0, "只等权 (nw = 1) 的篮子线 = k");
  assert(Y.hd.n >= 1 && (nk == 1 || ind) && (nw == 1 || mcap) && Y.eo.size() == Y.sh.n());
  const factor::Geo g = factor::geo_of(Y.sh.seg);
  const size_t nT = static_cast<size_t>(Y.sh.T), nA = static_cast<size_t>(Y.sh.A), nb = static_cast<size_t>(nk * nw), H = static_cast<size_t>(Y.hd.n);
  RowLines x; // [hold][篮子线][T] 行 t 这一批的篮子收益
  x.reset(static_cast<int>(H * nb), Y.sh.T);
  for (size_t i = 0; i < H * nb; ++i)
    x.hold[i] = Y.hd.k[i / nb];
  ex.rows(Y.sh.T, [&](int t0, int t1, int) {
    std::vector<double> s(2 * H * nb); // [hold][篮子线][Σ权·r, Σ权]
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      std::fill(s.begin(), s.end(), 0.0);
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = t * nA + a;
        const double om[kWeights] = {1.0, nw > 1 ? mcap[i] : 0.0};
        const uint8_t k_ind = nk > 1 ? ind[i] : uint8_t{0};
        for (size_t hi = 0; hi < H; ++hi) {
          const LegPlane &lab = Y.labels[hi];
          if (!lab.m[i])
            continue;
          const factor::stat::Chain c = factor::stat::chain_of(static_cast<int>(t), Y.sh.T, Y.sh.A, Y.hd.k[hi], g, 0);
          const float y[factor::stat::kLegs] = {factor::cpu::stat::h2f(lab.v[factor::stat::kLong][i]), factor::cpu::stat::h2f(lab.v[factor::stat::kShort][i])};
          // 篮子 k 的成员码 code (已偏到本格) / 判据 in
          const auto book = [&](size_t k, const uint8_t *code, const auto &in) {
            const factor::stat::Cell<uint8_t> cl{c, code, lab.m.data() + i, Y.eo.data() + i, y};
            if (!cl.held(factor::stat::kLong, in))
              return;
            const double r = cl.ret(factor::stat::kLong, cl.continues(in));
            for (int w = 0; w < nw; ++w) {
              double *q = s.data() + 2 * (hi * nb + k * static_cast<size_t>(nw) + static_cast<size_t>(w));
              q[0] += om[w] * r, q[1] += om[w];
            }
          };
          each_basket(k_ind, [&](size_t k) {
            if (k == 0)
              book(k, lab.m.data() + i, [](uint8_t v) { return v != 0; });
            else
              book(k, ind + i, [k_ind](uint8_t v) { return v == k_ind; });
          });
        }
      }
      for (size_t b = 0; b < H * nb; ++b)
        if (s[2 * b + 1] > 0.0)
          x.x[b * nT + t] = static_cast<float>(s[2 * b] / s[2 * b + 1]);
    }
  });
  return fold_book(ex, Y.sh.seg, x, 1);
}

// 篮子估值 (Baskets::val, 两权重) + 收盘价指数 (Baskets::index_px) + 篮子 0 逐行收益 / 权和 (rows, 换手用), 两遍:
//   1) 资产条带并行, 沿 t 把 B.px 的副本补成上一有效收盘价 (停牌带着);
//   2) 行并行, 每行一遍 A 轴:
//      收益成分 (篮子 0 + 行业) = 上一有效 close 行 p (prev) 在池 (行业按行 p), 收益 = 后复权 close / 上一有效后复权 close − 1, 权 = 1 / 行 p 总市值 mcap;
//      估值成分 (篮子 0 + 行业) = 上一行在池, 市值 = px × 本行总股本, 估值输入 val [PoolVal] / 行业取本行 (盘后列 as-of = 当日适用; DY 为股息率, 分子在此还原)
void pool_baskets(misc::exec::ForkJoin &ex, const BPlanes &B, const uint8_t *in, const std::vector<int32_t> &prev, Baskets &o, BasketRows &rows) {
  constexpr size_t K = Baskets::kBaskets;
  const factor::check::Plane &close = B.close;
  const float *mcap = B.mcap.data(), *sh = B.shares.data();
  const uint8_t *ind = B.ind.data();
  std::vector<float> pxv = B.px;
  float *px = pxv.data();
  const size_t nT = static_cast<size_t>(B.sh.T), nA = static_cast<size_t>(B.sh.A);
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
      o.val[w][m].assign(K * nT, kNaN);
  for (int w = 0; w < kWeights; ++w)
    rows.r[w].assign(nT, 0.0), rows.w[w].assign(nT, 0.0);
  o.index_px.n = static_cast<int>(K) * kWeights, o.index_px.P = B.sh.T;
  o.index_px.v.assign(static_cast<size_t>(o.index_px.n) * nT, 0.f);
  ex.rows(B.sh.T, [&](int t0, int t1, int) {
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      // 收益 [篮子][Weight][Σ权·r, Σ权]; 估值 [篮子][Weight][PoolVal][Σ权·收益率, Σ权] (收益率 = 分子 / 市值; 权 = 等权 1 / 加权 市值 ⇒ 整体法 Σ分子 / Σ市值)
      double x[K][kWeights][2] = {}, v[K][kWeights][kPoolVals][2] = {};
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = t * nA + a;
        if (const int32_t q = prev[i]; q >= 0) {
          const size_t p = static_cast<size_t>(q) * nA + a;
          if (in[p]) {
            const double r = close.v[i] / close.v[p] - 1.0, om[kWeights] = {1.0, mcap[p]};
            each_basket(ind[p], [&](size_t k) {
              for (int w = 0; w < kWeights; ++w)
                x[k][w][0] += om[w] * r, x[k][w][1] += om[w];
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
        num[kValDy] *= static_cast<double>(px[i - nA]) * sh[i]; // Fund 分母 = 算它那日 (上一行) 收盘 × 其次日 (本行) 股本
        const double om[kWeights] = {1.0, mc};
        each_basket(ind[i], [&](size_t k) {
          for (int m = 0; m < kPoolVals; ++m)
            if (std::isfinite(num[m]))
              for (int w = 0; w < kWeights; ++w)
                v[k][w][m][0] += om[w] * (num[m] / mc), v[k][w][m][1] += om[w];
        });
      }
      for (int w = 0; w < kWeights; ++w)
        if (x[0][w][1] > 0.0)
          rows.r[w][t] = x[0][w][0] / x[0][w][1], rows.w[w][t] = x[0][w][1];
      for (size_t k = 0; k < K; ++k)
        for (int w = 0; w < kWeights; ++w)
          if (x[k][w][1] > 0.0)
            o.index_px.v[static_cast<size_t>(Baskets::index_line(k, w)) * nT + t] = static_cast<float>(x[k][w][0] / x[k][w][1]);
      for (size_t k = 0; k < K; ++k)
        for (int w = 0; w < kWeights; ++w)
          for (int m = 0; m < kPoolVals; ++m)
            if (v[k][w][m][1] > 0.0)
              o.val[w][m][k * nT + t] = static_cast<float>(v[k][w][m][0] / v[k][w][m][1]);
    }
  });
}

// 篮子 0 的换手 (Baskets::turn): 成分行 t 的权 w_t = ω / Σω (等权 ω = 1, 加权 ω = mcap) 对上一成分行漂移后的权 w̃ = w (1 + r) / (1 + R) 的
//   单边 ½ Σ|w_t − w̃|, 停牌 = 移出 / 复牌 = 移回 (与 pool_baskets 同成分: prev 在池); 首个成分行 (建仓) 不计; 沿成分行均值.
//   Σ w̃ = 1 ⇒ ½ Σ|·| = ½ (1 + Σ_成分 (|w_t − w̃| − w̃)): 资产条带并行只走成分格, 每线程逐行攒, 按 tid 序归并. rows = pool_baskets 出的
void pool_turn(misc::exec::ForkJoin &ex, const factor::check::Plane &close, const float *mcap, const uint8_t *in, const std::vector<int32_t> &prev, int T,
               int A, const BasketRows &rows, double (&turn)[kWeights]) {
  const size_t nT = static_cast<size_t>(T), nA = static_cast<size_t>(A), nt = static_cast<size_t>(ex.threads());
  std::vector<int> pm[kWeights]; // [Weight][T] 上一成分行 (−1 = 无)
  std::vector<double> dev[kWeights];
  for (int w = 0; w < kWeights; ++w) {
    pm[w].assign(nT, -1);
    for (size_t t = 1; t < nT; ++t)
      pm[w][t] = rows.w[w][t - 1] > 0.0 ? static_cast<int>(t - 1) : pm[w][t - 1];
    dev[w].assign(nt * nT, 0.0);
  }
  ex.rows(A, [&](int a0, int a1, int tid) {
    const size_t off = static_cast<size_t>(tid) * nT;
    std::vector<double> dw[kWeights]; // 本资产在 tw 行之后漂移的权
    std::vector<int> tw[kWeights];    // 本资产上次是成分的行
    for (int w = 0; w < kWeights; ++w)
      dw[w].assign(static_cast<size_t>(a1 - a0), 0.0), tw[w].assign(static_cast<size_t>(a1 - a0), -1);
    for (size_t t = 0; t < nT; ++t)
      for (int a = a0; a < a1; ++a) {
        const size_t i = t * nA + static_cast<size_t>(a), j = static_cast<size_t>(a - a0);
        const int32_t q = prev[i];
        if (q < 0)
          continue;
        const size_t p = static_cast<size_t>(q) * nA + static_cast<size_t>(a);
        if (!in[p])
          continue;
        const double r = close.v[i] / close.v[p] - 1.0, om[kWeights] = {1.0, mcap[p]};
        for (int w = 0; w < kWeights; ++w) {
          const double W = rows.w[w][t];
          if (!(W > 0.0))
            continue;
          const double wt = om[w] / W, d = tw[w][j] == pm[w][t] ? dw[w][j] : 0.0;
          dev[w][off + t] += std::abs(wt - d) - d;
          dw[w][j] = wt * (1.0 + r) / (1.0 + rows.r[w][t]), tw[w][j] = static_cast<int>(t);
        }
      }
  });
  for (int w = 0; w < kWeights; ++w) {
    double s = 0.0;
    int n = 0;
    for (size_t t = 0; t < nT; ++t) {
      if (!(rows.w[w][t] > 0.0) || pm[w][t] < 0)
        continue;
      double x = 0.0;
      for (size_t j = 0; j < nt; ++j)
        x += dev[w][j * nT + t];
      s += 0.5 * (1.0 + x), ++n;
    }
    turn[w] = n > 0 ? s / n : 0.0;
  }
}

} // namespace

Baskets baskets(misc::exec::ForkJoin &ex, const YPlanes &Y, const BPlanes &B) {
  assert(B.sh == Y.sh && "篮子原料与 Y 同块");
  assert(Y.hd.n == 1 && Y.hd.k[0] == factor::stat::hold_close(1) && "篮子的持有期只 T+1C (C(D) → C(D+1))");
  Baskets o;
  o.index = pool_index(ex, Y, B.mcap.data(), B.ind.data(), static_cast<int>(Baskets::kBaskets), kWeights);
  const std::vector<int32_t> prev = prev_rows(ex, B.close, B.sh.T, B.sh.A);
  BasketRows rows;
  pool_baskets(ex, B, Y.gate.data(), prev, o, rows);
  pool_turn(ex, B.close, B.mcap.data(), Y.gate.data(), prev, Y.sh.T, Y.sh.A, rows, o.turn);
  return o;
}

BookLines pool_line(misc::exec::ForkJoin &ex, const YPlanes &y) { return pool_index(ex, y, nullptr, nullptr, 1, 1); }

} // namespace GUI::Factors
