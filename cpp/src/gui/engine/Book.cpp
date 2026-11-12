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

// 篮子 (Baskets::index / buy / val / stock*; pool_line = 单池篮子 0 等权): 格的事实 (BPlanes::fact) = 成员位 + 行业, 同取成员源行 (D−1 收盘定, 盘中 as-of,
//   整日常量), 概念全期同一份; 成分只经 BasketTable::each (枚举) / holds (仓位链上的测). B 的日频盘后列 (复权因子 / 股本 / 财务) 经 as-of 已是当日适用值,
//   与本行 close 配对. index_rows 出可成交指数 (fold_book, 标签; 顺延在标签里, 建仓 / 续持按 Stat 契约【仓位感知】);
//   ledger 出增仓 + 估值 + 个股项: 增仓 = 逐行成员表 (成分格 = 上一有效 close 格) 的换仓 Members::turnover, 与 pool::run_book 的账同一式.
//   权两者同一份 Omega. 全部篮子同一遍 A 轴

// 格的持仓权: 格 i (行 q, 资产 a) 在篮子 l 的权 = pool::omega (调仓期首行 r = B.from[l 的周期][q] 的目标权, 后复权 close_q / close_r)
//   (r = q 或两头缺有效 close → 行 q 的目标权) ⇒ 日频篮子恒 = 目标权, 周频池一周内随价漂移 (与选股表 Tape::omega 同式, 晚一行建仓)
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

// 篮子指数 (Baskets::index / pool_line) = fold_book, 全部持有期一遍: 行 t 一批 = 篮子线 l 的成分 (该持有期标签有效 ∧ 篮子判据) 里做多侧真持有的格
//   (Stat 契约【仓位感知】: 沿交接链看同一篮子), 收益按续持取名义值, 按权平均 (Σ权 ≤ 0 → 0) → 线 nl · hi + l 逐行收益 (stride 1).
//   each(hi, i, put): 标签有效格 i 所属的篮子线逐个 put(l, code, in, om): code = 成员码平面 (已偏到本格), in(code) = 是否同一篮子, om = 本格在线 l 的权
template <class Each>
BookLines index_rows(misc::exec::ForkJoin &ex, const Shape &sh, const Holds &hd, const LegPlane *const *labels, const uint8_t *eo, size_t nl, Each &&each) {
  assert(hd.n >= 1);
  const factor::stat::Span sp{sh.T, sh.A, factor::geo_of(sh.seg)};
  const size_t nT = static_cast<size_t>(sh.T), nA = static_cast<size_t>(sh.A), H = static_cast<size_t>(hd.n);
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
          x.x[l * nT + t] = static_cast<float>(s[2 * l] / s[2 * l + 1]);
    }
  });
  return fold_book(ex, sh.seg, x, 1);
}

// 增仓 (Baskets::buy) + 估值 (Baskets::val) + 个股的收益 / 收益率 (Baskets::stock / stock_val = 每格的个股项本身), 两遍:
//   1) 资产条带并行, 沿 t 把 B.px 的副本补成上一有效收盘价 (停牌带着);
//   2) 行段并行 (段首先建上一行的成员表), 每行一遍 A 轴:
//      成员表 = 各只成分格 p (上一有效 close 格) 的事实过判据, 权 = 格 p 的 Omega; 增仓 = 本行成员表对上一行成员表随价 (close / 成分格 close)
//      漂移后的换仓 (上一行篮子空 = 建仓 1); 个股收益 = 后复权 close / 成分格 close − 1;
//      估值成分 = 上一行的事实, 市值 = px × 本行总股本, 估值输入 val [PoolVal] 取本行 (盘后列 as-of = 当日适用; DY 为股息率, 分子在此还原:
//      Fund 分母 = 算它那日 (前一交易日末行) 的收盘市值 = 该行收盘 × 该行股本)
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
  for (int m = 0; m < kPoolVals; ++m)
    o.val[m].assign(nl * nT, kNaN), o.stock_val[m].assign(nA * nT, kNaN);
  o.stock.n = B.sh.A, o.stock.P = B.sh.T;
  o.stock.v.assign(nA * nT, 0.f);
  o.buy.n = static_cast<int>(nl), o.buy.P = B.sh.T;
  o.buy.v.assign(nl * nT, 0.f);
  // 资产 a 在行 t 的成分格 (上一有效 close 格); 无 → SIZE_MAX
  const auto cell = [&](size_t t, size_t a) {
    const int32_t q = prev[t * nA + a];
    return q < 0 ? SIZE_MAX : static_cast<size_t>(q) * nA + a;
  };
  // 行 t 的成员表 ⇄ 随价漂移到行 t 收盘
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
    // 增仓 [篮子线]; 估值 [篮子线][PoolVal][Σ权·收益率, Σ权] (收益率 = 分子 / 市值; 权 = 等权 1 / 加权 市值 ⇒ 整体法 Σ分子 / Σ市值)
    std::vector<double> buy(nl), v(nl * kPoolVals * 2);
    const auto vq = [&](size_t l, int m) { return v.data() + 2 * (l * kPoolVals + static_cast<size_t>(m)); };
    Members cur, last; // 本行 / 上一行 (已漂移到上一行收盘) 的成员表
    if (t0 > 0) {
      members(static_cast<size_t>(t0) - 1, last);
      drift(static_cast<size_t>(t0) - 1, last);
    }
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      members(t, cur);
      std::fill(buy.begin(), buy.end(), 0.0);
      cur.turnover(last, buy);
      for (size_t l = 0; l < nl; ++l)
        o.buy.v[l * nT + t] = static_cast<float>(buy[l]);
      drift(t, cur);
      std::swap(cur, last);
      std::fill(v.begin(), v.end(), 0.0);
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = t * nA + a;
        if (const size_t p = cell(t, a); p != SIZE_MAX)
          o.stock.v[a * nT + t] = static_cast<float>(close.v[i] / close.v[p] - 1.0);
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
            o.val[m][l * nT + t] = static_cast<float>(e[0] / e[1]);
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
  // 成员码 = 成员位平面; 仓位链上格 j 是否同一篮子 = holds(l, 格 j 的事实): 谓词收元素引用, 按址还原格号 j
  o.index = index_rows(ex, B.sh, hd, &lab, B.eo.data(), bt.size(), [&](size_t, size_t i, auto &&put) {
    const std::span<const uint16_t> cs = B.concepts_of(i % A);
    bt.each(B.fact(i), cs, [&](size_t l) {
      put(l, in + i, [&, l](const PoolMask &v) { return bt.holds(l, B.fact(static_cast<size_t>(&v - in)), cs); }, om(i, l));
    });
  });
  ledger(ex, B, om, prev_rows(ex, B.close, B.sh.T, B.sh.A), o);
  return o;
}

BookLines pool_line(misc::exec::ForkJoin &ex, const YPlanes &y) {
  assert(y.eo && "Y 未钉");
  return index_rows(ex, y.sh, y.hd, y.labels, y.eo, 1,
                    [&](size_t hi, size_t i, auto &&put) { put(size_t{0}, y.labels[hi]->m.data() + i, [](uint8_t v) { return v != 0; }, 1.0); });
}

} // namespace GUI::Engine