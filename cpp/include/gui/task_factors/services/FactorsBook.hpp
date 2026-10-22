// FactorsBook — 分批持仓净值引擎: 一切指数 / 净值评估同一份 (Portfolio 因子线 / 池篮子指数 / Inspect 分层累计). 宿主 CPU, 资产条带并行.
//
// 模型 (假设可加杠杆, 无资金约束 → 开仓不受已占用资金反馈, 不震荡):
//   每行 t 开一批 (cohort), 资金 = 当前净值的 1/h (h = 持有期行数), 批内按篮子权 w(t, a) 分到意向成员 (Σw ≤ 1, 余下是现金).
//   批 c 持到计划平仓点, 收益 = ret(c, a) (成交价沿用原模型: 标签 / 价格比); 只多一层"能不能成交"的判断, 能就做, 不能下一个时点再判:
//     建仓 (行 t): 上一批该篮子已持有 (交接) 或该篮子有卡住的仓 (交接) → 不成交; 否则 enter(t, a) 才买得进, 买不进 → 那份钱空着.
//     平仓 (批 c 的平仓点): 批 c + h 仍是该篮子意向成员 → 交接 (留仓, 不成交); 否则平仓点要可卖 (空腿: 可买), 卖不出 → 卡住:
//       沿价格链 (后复权收盘 / Open 型另有开盘, 无成交带上一有效价) 继续估值, 每个平仓点再判, 能卖就卖; 期间又成该篮子意向成员 → 交接实现.
//     成本只在真成交时记: 买 = cost_in(t, a) (冲击比例) + unit_in 份固定冲击; 卖 = 1 份固定冲击 (固定冲击只出份数 fix, 调用方乘 fixed_impact).
//   平仓点型 BookKind (持有期键 → 型): Row = 每行 (分钟型: 行 t 的批在行 t + h 平; 池指数 h = 1); Close = 当日收盘 (行 c 的批在 c 所在日收盘平);
//     Open = T+N 开盘 (c 所在日 + N 日开盘平). 计划平仓点越过区间末 / Row 型段末尾部 (Stat tail_masked) 的批不开 (与 Stat 同口径).
//     交接沿用原换手口径: 比 c 与 c + h 两行的意向成员 (Close / Open 型的 c + h 行在平仓点之后, 与原模型同).
//   出参 (占净值, 已含 1/h) 逐篮子逐行, 按 BookSpec::fields 只出要的: pnl = 本行实现的收益 (实现所在行: Row = 本行, Close = 收盘行,
//     Open = 开盘所在行, 交接 / 卡住实现 = 本行), bench = 实现批的基准 Σ side·w·bench(c) (超额 = pnl − bench), imp / fix = 成交冲击 / 固定冲击份数,
//     expo = 行末仓位 (含卡住的, 1 = 满仓). 净值 = 逐行 (1 + 增量) 连乘, 调用方做. 区间末仍卡住的按末行价实现 (不计卖出成本).
//   空腿 (side −1): 收益取负, 建仓同 enter, 平仓看可买. 无可成交平面 (Lob 档) → 恒可成交, 不会卡住, 价格链不用.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>
#include <vector>

namespace GUI::Factors {

// 可成交位 (YPlanes::trade 每格一字节, 与 YCols::trade 同序): 行末可买 / 可卖, 当日开盘可买 / 可卖
enum : uint8_t { kTrBuy = 1,
                 kTrSell = 2,
                 kTrBuyOpen = 4,
                 kTrSellOpen = 8 };
inline constexpr uint8_t kTrBoth = kTrBuy | kTrSell; // Stat 标签有效还要求建仓行两向皆可成交

enum class BookKind : uint8_t { Row,
                                Close,
                                Open };
inline BookKind book_kind(int hold) {
  return factor::stat::hold_intraday(hold) ? BookKind::Row : hold == factor::stat::kHoldClose ? BookKind::Close
                                                                                              : BookKind::Open;
}

inline constexpr int kBookMax = 64; // 篮子数上限 (成员位图 uint64)

// 出参字段 (BookSpec::fields 位)
enum BookField : int { kBkPnl,
                       kBkBench,
                       kBkImp,
                       kBkFix,
                       kBkExpo,
                       kBkFields };
inline constexpr unsigned bk_bit(BookField f) { return 1u << f; }

struct BookSpec {
  int T = 0, A = 0, S = 0; // 行 / 资产 / 每日行数
  int h = 0;               // 批持有行数 (hold_rows; 池指数 1)
  int hold = 0;            // 持有期键: Row 型据此判段末尾部; 0 = 不判 (池指数)
  BookKind kind = BookKind::Row;
  int nb = 0;                     // 篮子数
  int8_t side[kBookMax] = {};     // ±1: 多 / 空
  unsigned fields = 0;            // 要的出参 (bk_bit 位或; pnl 恒出)
  const uint8_t *trade = nullptr; // [T][A] kTr* 位; nullptr = 恒可成交 (px / open 可空)
  const float *px = nullptr;      // [T][A] 后复权收盘 (≤ 0 = 本行无成交)
  const float *open = nullptr;    // [days][A] 后复权开盘 (≤ 0 = 无; Open 型用)
  double unit_in = 0.0;           // 每买入一份记的固定冲击份数 (Bar 1: 建仓也付 fixed; Lob 0: 建仓只付 cost_in)
};
struct BookOut {
  int nb = 0, T = 0;
  std::vector<double> f[kBkFields]; // [字段][nb · T], 篮子 b 行 t 在 [b · T + t]; 没要的字段空
  const double *at(BookField k, int b) const { return f[k].data() + static_cast<size_t>(b) * T; }
};

// src 的接口 (调用方的篮子定义; 全部 const, 多线程并发调):
//   uint64_t mask(int t, size_t a)        行 t 资产 a 的意向篮子位 (与能否成交无关)
//   double weight(int t, size_t a, int b) 篮子 b 内的权 (Σ_a ≤ 1)
//   bool enter(int t, size_t a)           行 t 买得进 (两腿同判)
//   double ret(int c, size_t a)           批 c 到计划平仓点的做多收益
//   double bench(int c)                   批 c 的基准 (超额用; 无 = 0)
//   double cost_in(int t, size_t a)       建仓冲击 (比例; 无 = 0)
template <class Src>
void run_book(misc::exec::ForkJoin &ex, const BookSpec &s, const Src &src, BookOut &o) {
  assert(s.T >= 1 && s.A >= 1 && s.S >= 1 && s.T % s.S == 0 && s.h >= 1 && s.nb >= 1 && s.nb <= kBookMax);
  assert((!s.trade || s.px) && (!s.trade || s.kind != BookKind::Open || s.open) && "有可成交平面就要估值价");
  assert((s.kind != BookKind::Close || s.h == s.S) && (s.kind != BookKind::Open || s.h % s.S == 0));
  const int T = s.T, S = s.S, h = s.h, nb = s.nb, days = T / s.S, N = s.kind == BookKind::Open ? s.h / s.S : 0;
  const size_t nA = static_cast<size_t>(s.A), H = static_cast<size_t>(h), NB = static_cast<size_t>(nb), nT = static_cast<size_t>(T);
  const factor::Geo g = factor::geo_of(S);
  const unsigned fields = s.fields | bk_bit(kBkPnl);
  int slot_of[kBkFields], nf = 0; // 字段 → 累加槽 (−1 = 不要)
  for (int f = 0; f < kBkFields; ++f)
    slot_of[f] = fields >> f & 1 ? nf++ : -1;
  o = BookOut{};
  o.nb = nb, o.T = T;
  for (int f = 0; f < kBkFields; ++f)
    if (slot_of[f] >= 0)
      o.f[f].assign(NB * nT, 0.0);
  const auto opens = [&](int t) {
    switch (s.kind) {
    case BookKind::Row:
      return t + h < T && !(s.hold > 0 && factor::stat::tail_masked(t, s.hold, g));
    case BookKind::Close:
      return true;
    case BookKind::Open:
      return t / S + N < days;
    }
    return false;
  };
  const auto can_exit = [&](int t, size_t a, bool at_open, int sd) {
    if (!s.trade)
      return true;
    const uint8_t f = s.trade[static_cast<size_t>(t) * nA + a];
    return (f & (at_open ? (sd > 0 ? kTrSellOpen : kTrBuyOpen) : (sd > 0 ? kTrSell : kTrBuy))) != 0;
  };

  // 资产状态 (跨块): 批环 [a][slot = c mod h] 已建仓位 / 交接位 (批 c 平仓时定, 批 c + h 建仓时用); 卡住的仓 [a][b]; 上一有效估值价
  struct Stuck {
    double w = 0.0, u = 0.0, v = 0.0; // 权 / 计划平仓点的值 w (1 + ret) (已实现到这) / 现值
  };
  std::vector<uint64_t> ring_in(nA * H, 0), ring_keep(nA * H, 0), stk_m(nA, 0);
  std::vector<Stuck> stk(nA * NB);
  std::vector<double> plast(nA, 0.0);

  // 行块: 每线程 [槽][篮子][块内行] 累加, 块末按 tid 序归并 (静态条带 → 逐位确定), 归并时清零
  constexpr int kBlock = 512;
  const size_t per = static_cast<size_t>(nf) * NB * kBlock;
  const int nt = ex.threads();
  std::vector<double> acc(static_cast<size_t>(nt) * per, 0.0);
  for (int r0 = 0; r0 < T; r0 += kBlock) {
    const int r1 = std::min(T, r0 + kBlock);
    ex.rows(s.A, [&](int a0, int a1, int tid) {
      double *ac = acc.data() + static_cast<size_t>(tid) * per;
      int row = 0; // 当前行的块内下标
      const auto add = [&](BookField f, int b, double x) {
        if (slot_of[f] >= 0)
          ac[(static_cast<size_t>(slot_of[f]) * NB + static_cast<size_t>(b)) * kBlock + static_cast<size_t>(row)] += x;
      };
      const auto revalue = [&](size_t a, float p) {
        if (!(p > 0.f))
          return;
        double &pl = plast[a];
        if (pl > 0.0 && stk_m[a]) {
          const double f = p / pl;
          for (uint64_t m = stk_m[a]; m; m &= m - 1)
            stk[a * NB + static_cast<size_t>(std::countr_zero(m))].v *= f;
        }
        pl = p;
      };
      const auto realize = [&](size_t a, int b, bool sold) {
        Stuck &k = stk[a * NB + static_cast<size_t>(b)];
        add(kBkPnl, b, s.side[b] * (k.v - k.u));
        if (sold)
          add(kBkFix, b, k.w);
        add(kBkExpo, b, -k.w);
        k = Stuck{};
        stk_m[a] &= ~(uint64_t{1} << b);
      };
      const auto stuck_check = [&](size_t a, int t, bool at_open) {
        for (uint64_t m = stk_m[a]; m; m &= m - 1) {
          const int b = std::countr_zero(m);
          if (can_exit(t, a, at_open, s.side[b]))
            realize(a, b, true);
        }
      };
      // 批 c 在本行 (平仓点 t) 平仓
      const auto settle = [&](int c, size_t a, int t, bool at_open) {
        const size_t slot = a * H + static_cast<size_t>(c % h);
        const uint64_t in = ring_in[slot];
        ring_in[slot] = 0;
        const int tn = c + h;
        const uint64_t keep = in && tn < T && opens(tn) ? src.mask(tn, a) & in : 0;
        ring_keep[slot] = keep;
        if (!in)
          return;
        const double r = src.ret(c, a), bm = src.bench(c);
        for (uint64_t m = in; m; m &= m - 1) {
          const int b = std::countr_zero(m);
          const int sd = s.side[b];
          const double w = src.weight(c, a, b) / h;
          add(kBkPnl, b, sd * w * r);
          add(kBkBench, b, sd * w * bm);
          if (keep >> b & 1) {
            add(kBkExpo, b, -w);
          } else if (can_exit(t, a, at_open, sd)) {
            add(kBkFix, b, w);
            add(kBkExpo, b, -w);
          } else {
            Stuck &k = stk[a * NB + static_cast<size_t>(b)];
            k.w += w, k.u += w * (1.0 + r), k.v += w * (1.0 + r);
            stk_m[a] |= uint64_t{1} << b;
          }
        }
      };
      // 批 t 建仓
      const auto entry = [&](int t, size_t a) {
        const size_t slot = a * H + static_cast<size_t>(t % h);
        const uint64_t keep = ring_keep[slot];
        ring_keep[slot] = 0;
        assert(ring_in[slot] == 0 && "批环槽未平仓就被新批占用");
        uint64_t in = 0;
        int can = -1; // enter 懒求
        for (uint64_t m = src.mask(t, a); m; m &= m - 1) {
          const int b = std::countr_zero(m);
          const double w = src.weight(t, a, b) / h;
          const bool kept = keep >> b & 1, stuck = stk_m[a] >> b & 1;
          if (stuck)
            realize(a, b, false);
          if (!kept && !stuck) {
            if (can < 0)
              can = src.enter(t, a);
            if (!can)
              continue;
            add(kBkImp, b, w * src.cost_in(t, a));
            add(kBkFix, b, w * s.unit_in);
          }
          in |= uint64_t{1} << b;
          add(kBkExpo, b, w);
        }
        ring_in[slot] = in;
      };
      for (int t = r0; t < r1; ++t) {
        row = t - r0;
        const int D = t / S, r = t % S;
        for (size_t a = static_cast<size_t>(a0); a < static_cast<size_t>(a1); ++a) {
          if (s.kind == BookKind::Open && r == 0) {
            if (s.trade) {
              revalue(a, s.open[static_cast<size_t>(D) * nA + a]);
              stuck_check(a, t, true);
            }
            if (D >= N)
              for (int c = (D - N) * S; c < (D - N + 1) * S; ++c)
                settle(c, a, t, true);
          }
          if (s.trade)
            revalue(a, s.px[static_cast<size_t>(t) * nA + a]);
          if (s.kind == BookKind::Row) {
            if (s.trade)
              stuck_check(a, t, false);
            if (t >= h)
              settle(t - h, a, t, false);
          }
          if (opens(t))
            entry(t, a);
          if (s.kind == BookKind::Close && r == S - 1) {
            if (s.trade)
              stuck_check(a, t, false);
            for (int c = D * S; c < (D + 1) * S; ++c)
              settle(c, a, t, false);
          }
          if (t == T - 1)
            for (uint64_t m = stk_m[a]; m; m &= m - 1)
              realize(a, std::countr_zero(m), false);
        }
      }
    });
    ex.rows(nf * nb, [&](int i0, int i1, int) { // (槽, 篮子) 并行归并
      for (int i = i0; i < i1; ++i) {
        const int k = i / nb, b = i % nb;
        int f = 0;
        while (slot_of[f] != k)
          ++f;
        double *dst = o.f[f].data() + static_cast<size_t>(b) * nT;
        for (int tid = 0; tid < nt; ++tid) {
          double *p = acc.data() + static_cast<size_t>(tid) * per + static_cast<size_t>(i) * kBlock;
          for (int t = r0; t < r1; ++t) {
            dst[t] += p[t - r0];
            p[t - r0] = 0.0;
          }
        }
      }
    });
  }
  if (slot_of[kBkExpo] >= 0)
    for (size_t b = 0; b < NB; ++b) {
      double *e = o.f[kBkExpo].data() + b * nT;
      for (size_t t = 1; t < nT; ++t)
        e[t] += e[t - 1];
    }
}

} // namespace GUI::Factors
