// FactorsBook — 分批持仓净值引擎: 一切指数 / 净值评估同一份 (Portfolio 因子线 / 池篮子指数 / Inspect 分层累计). 宿主 CPU, 资产条带并行.
//
// 四个正交件 → run_book → 线表 → fold_lines → BookLines (期收益) → compound (净值, 调用方):
//   时钟 BookClock  (时间轴 T) 换仓点 / 平仓点 / 批环槽; 持有期键 → book_clock
//   意向 Intent     谁在篮子里 + 权 + 腿向: RankIntent (秩平面, 篮子 = r16 区间) / 池篮子 (FactorsLoad.cpp)
//   成交 Fill       能否成交 + 收益 + 成本 (持市场 BookMarket = 资产轴 A + 可成交位 + 估值价): LabelFill (标签) / PxFill (价格链)
//   线表 Line       出参的线性组合; run_book 的出参字段 = 线表所用之并 (fields_of)
//
// 模型 (假设可加杠杆, 无资金约束 → 开仓不受已占用资金反馈, 不震荡):
//   每个换仓点 c 开一批 (cohort), 资金 = 当前净值的 1/h (h = 同时在场的批数), 批内按篮子权 w(c, a) 分到意向成员 (Σw ≤ 1, 余下是现金;
//   信号 = 换仓行本行). 批 c 的平仓点 = 后继批的换仓点 (同一时刻), 收益 = ret(c, a); 只多一层"能不能成交"的判断, 能就做, 不能下一个时点再判:
//     平仓: 后继批仍是该篮子意向成员 → 交接 (留仓, 不成交); 否则要可卖 (空腿: 可买), 卖不出 → 卡住: 沿价格链继续估值,
//       之后每个可成交时点再判, 能卖就卖.
//     建仓: 交接 → 不成交; 该篮子有卡住的仓 → 转交 min(卡住权, 新权) (不成交, 卡住的余量照卡), 不足的部分补买;
//       要新买的 (含补买) 须买得进, 买不进 → 本批不建该篮子 (那份钱空着, 卡住的照卡).
//     成本只在真成交时记: 买 = 冲击比例 + 固定冲击份数; 卖 = 1 份固定冲击 (固定冲击只出份数 fix, 调用方乘 fixed_impact).
//   时钟的型 BookKind (持有期键 → 型; 尾部 = Stat tail_masked, 与换手成本同口径):
//     Row   分钟型 (池指数 h = 1): 每行 t 一批 (非尾部, 且 t + h 在区间内), 行 t + h 平 = 批 t + h 的换仓点
//     Close 收盘型 (h = S): 每行 t 一批 (非尾部), 当日收盘全平 (T0); 收盘不是换仓点 → 无交接
//     Day   T+N 型 (h = N): 每日收盘一批 (越过区间末的不开); 只在每日末行动
//   成交: LabelFill (Row / Close) 收益 / 买得进 / 冲击 = 标签; PxFill (Day / 池指数) 价 = 价格链, 可成交 = 有价 ∧ 行末可成交位,
//     收盘竞价单一价 → 无冲击, 买卖各 1 份固定冲击. 平仓看行末可成交位, 卡住的仓按估值价重估;
//     LabelFill 无可成交平面 (Lob 档) → 恒可平, 不会卡住.
//   出参 (占净值, 已含 1/h) 逐篮子逐行, 按 fields 只出要的: pnl = 本行实现的收益 (平仓点 / 卡住的卖出或转交行),
//     bench = 实现批的基准 Σ side·w·bench(c) (超额 = pnl − bench; bench(c) = 批 c 意向非空 ∧ 买得进 (多向) 的资产收益等权均值,
//     Row / Close 即 Stat Row::mkt[kLong]), imp / fix = 成交冲击 / 固定冲击份数, expo = 行末仓位 (含卡住的, 1 = 满仓).
//     线 Line → fold_lines 折成期收益 BookLines; 净值 = 期收益连乘 (compound, 调用方做). 区间末仍卡住的按末次估值实现 (不计卖出成本).
//   空腿 (side −1): 收益取负, 建仓看可卖 (LabelFill 同标签有效位), 平仓看可买.
//
//   【成交价】Stat 标签 / 换手成本、本引擎、池指数的买卖价一张表 (D = 交易日, m = 行末分钟, n = 分钟型分钟数, N = T+N 日数):
//     E(m)  建仓价: Lob 档 = 分钟 m 末 + LABEL_DELAY_SECONDS 的可成交盘口中间价; Bar 档 = m 之后首个有成交连续竞价分钟的 VWAP;
//           之后再无可成交 (14:57 起) → C(D)
//     C(D)  收盘价 = 当日末笔成交 (收盘竞价撮合价) = 末行 bar close
//     O(D)  开盘价 = 09:25 撮合价, 无撮合则连续竞价首笔成交价; 无开盘 (停牌) 顺延, 最多再等 N 日 (仍无 → 最近收盘价)
//     场景        用在哪                信号行     买入                  卖出
//     分钟型 n    Stat / 引擎 Row       行 m       E(m)                  E(m+n)
//     收盘型      Stat / 引擎 Close     行 m       E(m)                  C(D)
//     T+N 标签    Stat                  行 m       E(m) (日层 = C(D))    O(D+N)
//     T+N 换仓    引擎 Day              D 日末行   C(D)                  C(D+N)
//     池指数      引擎 Row (h = 1)      行 m       m 行末收盘            m+1 行末收盘 (日层 = C(D) → C(D+1))
//     m + n 越过 kLastAnchorRow (分钟型) / m 越过它 (收盘型) 的行是尾部 (tail_masked), 不开仓.
//     Row / Close 的价就在标签里 (LabelFill); Day / 池指数用后复权收盘 px, 无成交带上一有效价 (PxFill).
#pragma once

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Host.hpp" // h2f (LabelFill)
#include "misc/exec.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>
#include <span>
#include <vector>

namespace GUI::Factors {

// 可成交位 (YPlanes::trade 每格一字节, 与 YCols::trade 同序): 行末可买 / 可卖
enum : uint8_t { kTrBuy = 1,
                 kTrSell = 2 };
inline constexpr uint8_t kTrBoth = kTrBuy | kTrSell; // Stat 标签有效还要求建仓行两向皆可成交

enum class BookKind : uint8_t { Row,
                                Close,
                                Day };
inline BookKind book_kind(int hold) {
  return factor::stat::hold_intraday(hold) ? BookKind::Row : hold == factor::stat::kHoldClose ? BookKind::Close
                                                                                              : BookKind::Day;
}

inline constexpr int kBookMax = 64; // 篮子数上限 (成员位图 uint64)

// 出参字段 (run_book fields 位)
enum BookField : int { kBkPnl,
                       kBkBench,
                       kBkImp,
                       kBkFix,
                       kBkExpo,
                       kBkFields };
inline constexpr unsigned bk_bit(BookField f) { return 1u << f; }

// ---- 时钟 (时间轴 T): 行 t → 点 pt(t) (动的行的序号; Day 型 = 日, 其余 = 行) ----
struct BookClock {
  int T = 0, S = 0; // 行 / 每日行数
  factor::Geo g{};  // = geo_of(S)
  BookKind kind = BookKind::Row;
  int h = 0;                                                                 // 同时在场的批数: Row = hold_rows (池指数 1) / Close = S / Day = N
  int hold = 0;                                                              // 持有期键: Row / Close 据此判段末尾部; 0 = 不判 (池指数)
  bool acts(int t) const { return kind != BookKind::Day || t % S == S - 1; } // 动的行 (Day 型只在每日末行)
  int pt(int t) const { return kind == BookKind::Day ? t / S : t; }          // 行 → 点
  int row(int p) const { return kind == BookKind::Day ? p * S + S - 1 : p; } // 点 → 行
  int pts() const { return kind == BookKind::Day ? T / S : T; }
  int span() const { return kind == BookKind::Day ? h * S : h; }      // Row / Day: 批 c 的平仓行 = c + span (= 点 + h)
  size_t slot(int c) const { return static_cast<size_t>(pt(c) % h); } // 批 c 的环槽 (后继批同槽)
  bool opens(int t) const {                                           // 换仓点 (开一批)
    switch (kind) {
    case BookKind::Row:
      return t + h < T && !(hold > 0 && factor::stat::tail_masked(t, hold, g));
    case BookKind::Close:
      return !factor::stat::tail_masked(t, hold, g);
    case BookKind::Day:
      return acts(t) && pt(t) + h < pts();
    }
    return false;
  }
};
// 持有期键 → 时钟; hold 0 = 池指数 (Row, h = 1, 不判尾部)
inline BookClock book_clock(int hold, int S, int T) {
  assert(T >= 1 && S >= 1 && T % S == 0 && hold >= 0);
  BookClock k;
  k.T = T, k.S = S, k.g = factor::geo_of(S), k.hold = hold;
  k.kind = hold == 0 ? BookKind::Row : book_kind(hold);
  k.h = hold == 0                   ? 1
        : k.kind == BookKind::Row   ? factor::stat::hold_rows(hold, k.g)
        : k.kind == BookKind::Close ? S
                                    : hold - factor::stat::kHoldDayBase;
  assert(k.h >= 1);
  return k;
}

// ---- 市场 (资产轴 A): 可成交位 + 估值价 (YPlanes 给: book_market) ----
struct BookMarket {
  int A = 0;
  const uint8_t *trade = nullptr; // [T][A] kTr* 位; nullptr = 恒可成交
  const float *px = nullptr;      // [T][A] 后复权收盘 (≤ 0 = 本行无成交)
  size_t at(int t, size_t a) const { return static_cast<size_t>(t) * static_cast<size_t>(A) + a; }
  bool priced(int t, size_t a) const { return px[at(t, a)] > 0.f; }
  // 行 t 资产 a 腿向 sd 的 建仓 (open) / 平仓 能否成交: 行末可成交位 (无平面恒可); 多腿建仓 = 买, 空腿建仓 = 卖
  bool can(int t, size_t a, bool open, int sd) const { return !trade || (trade[at(t, a)] & (open == (sd > 0) ? kTrBuy : kTrSell)) != 0; }
};

struct BookOut {
  int nb = 0, T = 0;
  std::vector<double> f[kBkFields]; // [字段][nb · T], 篮子 b 行 t 在 [b · T + t]; 没要的字段空
  const double *at(BookField k, int b) const { return f[k].data() + static_cast<size_t>(b) * T; }
};

// 意向权 [T][nb] (【持仓口径】; 只看意向, 与持有期 / 能否成交无关): 行 t 篮子 b = 1 / book_den(行 t 篮子 b 的意向只数, cap_of(b)).
// rx = r16 平面 [T][A], bits(r16) = 意向篮子位 (同 RankIntent)
template <class Bits, class Cap>
std::vector<float> intent_weights(misc::exec::ForkJoin &ex, const uint16_t *rx, int T, int A, int nb, const Bits &bits, const Cap &cap_of) {
  assert(rx && T >= 1 && A >= 1 && nb >= 1 && nb <= kBookMax);
  const size_t nA = static_cast<size_t>(A), NB = static_cast<size_t>(nb);
  std::vector<float> wt(static_cast<size_t>(T) * NB);
  ex.rows(T, [&](int t0, int t1, int) {
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      int c[kBookMax] = {};
      for (size_t a = 0; a < nA; ++a)
        for (uint64_t m = bits(rx[t * nA + a]); m; m &= m - 1)
          ++c[std::countr_zero(m)];
      for (int b = 0; b < nb; ++b)
        wt[t * NB + static_cast<size_t>(b)] = static_cast<float>(1.0 / factor::stat::book_den(c[b], cap_of(b)));
    }
  });
  return wt;
}

// ---- 意向 Intent 的接口 (调用方的篮子定义; 全部 const, 多线程并发调) ----
//   int nb; size_t A                      篮子数 (≤ kBookMax) / 资产数 (= 成交的 m.A)
//   int side(int b)                       ±1: 多 / 空 (空腿收益取负, 建仓看可卖, 平仓看可买)
//   uint64_t mask(int t, size_t a)        信号行 t 资产 a 的意向篮子位 (与能否成交无关)
//   double weight(int t, size_t a, int b) 信号行 t 篮子 b 内的权 (Σ_a ≤ 1)

// 秩平面上的篮子 (组 / 尾部 / 腿都是 r16 区间): r16 ∈ [lo, hi], 腿向 side, 满仓 = cap · n_full
struct RankBasket {
  uint16_t lo = 0, hi = 0;
  int8_t side = 1;
  double cap = 1.0;
};
// 组 g 的篮子 (与 grp_of 同边界: g = ⌊r16 · kGroups / (kRankMax + 1)⌋)
inline constexpr RankBasket group_basket(int g, int side, double cap = 1.0) {
  constexpr int span = factor::stat::kRankMax + 1, G = factor::stat::kGroups;
  return {static_cast<uint16_t>((g * span + G - 1) / G), static_cast<uint16_t>(g == G - 1 ? factor::stat::kRankMax : ((g + 1) * span + G - 1) / G - 1),
          static_cast<int8_t>(side), cap};
}
// r16 → 意向篮子位: 各篮子区间的端点把 r16 轴切成段, 段内位恒定 (kRankNone 越过 kRankMax, 落在无篮子的末段 = 0)
struct RankBits {
  int n = 0;
  uint32_t cut[2 * kBookMax + 1] = {};  // 段起点 (升序, cut[0] = 0)
  uint64_t bits[2 * kBookMax + 1] = {}; // 段内篮子位
  explicit RankBits(std::span<const RankBasket> bk) {
    assert(!bk.empty() && bk.size() <= static_cast<size_t>(kBookMax));
    cut[n++] = 0;
    for (const RankBasket &b : bk) {
      assert(b.lo <= b.hi && b.hi <= factor::stat::kRankMax);
      cut[n++] = b.lo, cut[n++] = b.hi + 1u;
    }
    std::sort(cut, cut + n);
    n = static_cast<int>(std::unique(cut, cut + n) - cut);
    for (int s = 0; s < n; ++s)
      for (size_t b = 0; b < bk.size(); ++b)
        bits[s] |= static_cast<uint64_t>(bk[b].lo <= cut[s] && cut[s] <= bk[b].hi) << b;
  }
  uint64_t operator()(uint16_t r) const { return bits[std::upper_bound(cut, cut + n, uint32_t{r}) - cut - 1]; }
};
// 秩平面上的意向 (Inspect 分层 / Portfolio G20 同一份): 意向 = bits(r16), 权 = intent_weights
struct RankIntent {
  const uint16_t *rx;   // [T][A] r16
  const RankBits *bits; // r16 → 意向篮子位
  const float *wt;      // [T][nb] intent_weights
  const RankBasket *bk; // [nb]
  int nb;
  size_t A;
  int side(int b) const { return bk[b].side; }
  uint64_t mask(int t, size_t a) const { return (*bits)(rx[static_cast<size_t>(t) * A + a]); }
  double weight(int t, size_t, int b) const { return wt[static_cast<size_t>(t) * static_cast<size_t>(nb) + static_cast<size_t>(b)]; }
};

// ---- 成交 Fill 的接口 (持市场 m; 全部 const, 多线程并发调) ----
//   BookMarket m                          资产轴 A + 可成交位 + 估值价
//   bool enter(int t, size_t a, int sd)   行 t 腿向 sd 建仓能否成交 (要新买的部分)
//   bool exit(int t, size_t a, int sd)    行 t 腿向 sd 平仓能否成交 (否则卡住)
//   double ret(int c, size_t a)           批 c 到平仓点的做多收益
//   double cost_in(int t, size_t a)       建仓冲击 (比例; 无 = 0)
//   double unit_in                        每买入一份记的固定冲击份数
//   bool marks()                          会卡住: 每个动的行先重估卡住的仓, 再判能否卖出
//   float value(int t, size_t a)          卡住的仓的估值价 (≤ 0 = 尚无价, 不重估)

// 标签成交 (Row / Close): 收益 = 做多标签, 买得进 = 标签有效位 (Stat 同口径: 建仓行两向可成交), 建仓冲击 = 买入冲击;
//   平仓 = 行末可成交位, 卡住按 px 估值; 无可成交平面 (Lob 档) → 恒可平, 不会卡住
struct LabelFill {
  BookMarket m;
  const uint16_t *lv;  // [T][A] 做多标签 fp16 位
  const uint8_t *lm;   // [T][A] 标签有效位
  const uint16_t *imp; // [T][A] 买入冲击 fp16 位; nullptr = 不计
  double unit_in;      // Bar 1 (建仓也付 fixed) / Lob 0 (建仓只付 cost_in)
  bool enter(int t, size_t a, int) const { return lm[m.at(t, a)]; }
  bool exit(int t, size_t a, int sd) const { return m.can(t, a, false, sd); }
  double ret(int c, size_t a) const { return factor::cpu::stat::h2f(lv[m.at(c, a)]); }
  double cost_in(int t, size_t a) const { return imp ? factor::cpu::stat::h2f(imp[m.at(t, a)]) : 0.0; }
  bool marks() const { return m.trade != nullptr; }
  float value(int t, size_t a) const { return m.px[m.at(t, a)]; }
};
// 价格链成交 (Day 型 T+N 收盘换仓 / 池指数): 价 = 动的行的 px, 无成交带上一有效价; 可成交 = 本行有价 ∧ 行末可成交位;
//   收盘竞价单一价 → 无冲击, 买卖各 1 份固定冲击
struct PxFill {
  BookMarket m;
  BookClock k;
  std::vector<float> P; // [k.pts()][A] 价格链 (0 = 尚无)
  static constexpr double unit_in = 1.0;
  PxFill(misc::exec::ForkJoin &ex, const BookClock &k_, const BookMarket &m_)
      : m(m_), k(k_), P(static_cast<size_t>(k_.pts()) * static_cast<size_t>(m_.A), 0.f) {
    assert(m.px && k.kind != BookKind::Close && "价格链: 要 px, 收盘型无价格链");
    const size_t nA = static_cast<size_t>(m.A);
    ex.rows(m.A, [&](int a0, int a1, int) {
      for (int p = 0; p < k.pts(); ++p) {
        const float *src = m.px + m.at(k.row(p), 0);
        float *dst = P.data() + static_cast<size_t>(p) * nA;
        for (size_t a = static_cast<size_t>(a0); a < static_cast<size_t>(a1); ++a)
          dst[a] = src[a] > 0.f ? src[a] : p ? dst[a - nA]
                                             : 0.f;
      }
    });
  }
  bool enter(int t, size_t a, int sd) const { return m.priced(t, a) && m.can(t, a, true, sd); }
  bool exit(int t, size_t a, int sd) const { return m.priced(t, a) && m.can(t, a, false, sd); }
  double ret(int c, size_t a) const {
    const size_t i = static_cast<size_t>(k.pt(c)) * static_cast<size_t>(m.A) + a;
    assert(P[i] > 0.f && "批的建仓点须有价");
    return static_cast<double>(P[i + static_cast<size_t>(k.h) * static_cast<size_t>(m.A)]) / P[i] - 1.0;
  }
  double cost_in(int, size_t) const { return 0.0; }
  bool marks() const { return true; }
  float value(int t, size_t a) const { return P[static_cast<size_t>(k.pt(t)) * static_cast<size_t>(m.A) + a]; }
};

// 时钟 K × 意向 I × 成交 F → 出参 o (fields = 要的出参 bk_bit 位或, pnl 恒出; 一般由线表定: fields_of)
template <class Intent, class Fill>
void run_book(misc::exec::ForkJoin &ex, const BookClock &K, const Intent &I, const Fill &F, unsigned fields, BookOut &o) {
  assert(K.T >= 1 && K.S >= 1 && K.T % K.S == 0 && K.h >= 1 && I.nb >= 1 && I.nb <= kBookMax);
  assert(F.m.A >= 1 && I.A == static_cast<size_t>(F.m.A) && "意向 / 成交同一资产轴");
  assert(K.kind != BookKind::Close || (K.h == K.S && K.hold == factor::stat::kHoldClose));
  const int T = K.T, S = K.S, h = K.h, nb = I.nb, span = K.span();
  const size_t nA = I.A, nH = static_cast<size_t>(h), NB = static_cast<size_t>(nb), nT = static_cast<size_t>(T);
  fields |= bk_bit(kBkPnl);
  int slot_of[kBkFields], nf = 0; // 字段 → 累加槽 (−1 = 不要)
  for (int f = 0; f < kBkFields; ++f)
    slot_of[f] = fields >> f & 1 ? nf++ : -1;
  o = BookOut{};
  o.nb = nb, o.T = T;
  for (int f = 0; f < kBkFields; ++f)
    if (slot_of[f] >= 0)
      o.f[f].assign(NB * nT, 0.0);
  // 基准 [T] (要 bench 才算): 批 c = 意向非空 ∧ 买得进 (多向) 的资产收益等权均值
  std::vector<double> bench;
  if (slot_of[kBkBench] >= 0) {
    bench.assign(nT, 0.0);
    ex.rows(T, [&](int t0, int t1, int) {
      for (int c = t0; c < t1; ++c) {
        if (!K.opens(c))
          continue;
        double sm = 0.0;
        int n = 0;
        for (size_t a = 0; a < nA; ++a)
          if (I.mask(c, a) && F.enter(c, a, 1))
            sm += F.ret(c, a), ++n;
        bench[static_cast<size_t>(c)] = n ? sm / n : 0.0;
      }
    });
  }

  // 资产状态 (跨块): 批环 [a][K.slot] 已建仓位 / 交接位 (批 c 平仓时定, 后继批建仓时用); 卡住的仓 [a][b]; 上一有效估值价
  struct Stuck {
    double w = 0.0, u = 0.0, v = 0.0; // 权 / 平仓点的值 w (1 + ret) (已实现到这) / 现值
  };
  std::vector<uint64_t> ring_in(nA * nH, 0), ring_keep(nA * nH, 0), stk_m(nA, 0);
  std::vector<Stuck> stk(nA * NB);
  std::vector<double> plast(nA, 0.0);

  // 行块: 每线程 [槽][篮子][块内行] 累加, 块末按 tid 序归并 (静态条带 → 逐位确定), 归并时清零
  constexpr int kBlock = 512;
  const size_t per = static_cast<size_t>(nf) * NB * kBlock;
  const int nt = ex.threads();
  std::vector<double> acc(static_cast<size_t>(nt) * per, 0.0);
  for (int r0 = 0; r0 < T; r0 += kBlock) {
    const int r1 = std::min(T, r0 + kBlock);
    ex.rows(F.m.A, [&](int a0, int a1, int tid) {
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
        add(kBkPnl, b, I.side(b) * (k.v - k.u));
        if (sold)
          add(kBkFix, b, k.w);
        add(kBkExpo, b, -k.w);
        k = Stuck{};
        stk_m[a] &= ~(uint64_t{1} << b);
      };
      const auto stuck_check = [&](size_t a, int t) {
        for (uint64_t m = stk_m[a]; m; m &= m - 1) {
          const int b = std::countr_zero(m);
          if (F.exit(t, a, I.side(b)))
            realize(a, b, true);
        }
      };
      // 卡住的仓转交 x (≤ 卡住权) 给后继批: 转交部分按现值实现 (不成交), 余量照卡
      const auto transfer = [&](size_t a, int b, double x) {
        Stuck &k = stk[a * NB + static_cast<size_t>(b)];
        if (x >= k.w) {
          realize(a, b, false);
        } else {
          const double f = x / k.w;
          add(kBkPnl, b, I.side(b) * (k.v - k.u) * f);
          add(kBkExpo, b, -x);
          k.w -= x, k.u -= k.u * f, k.v -= k.v * f;
        }
      };
      // 批 c 在本行 (平仓点 t = 后继批的换仓点) 平仓; mt = 后继批在本行的意向位 (不是换仓点 / 收盘型 = 0 → 无交接)
      const auto settle = [&](int c, size_t a, int t, uint64_t mt) {
        const size_t slot = a * nH + K.slot(c);
        const uint64_t in = ring_in[slot];
        ring_in[slot] = 0;
        const uint64_t keep = mt & in;
        ring_keep[slot] = keep;
        if (!in)
          return;
        const double r = F.ret(c, a), bm = bench.empty() ? 0.0 : bench[static_cast<size_t>(c)];
        for (uint64_t m = in; m; m &= m - 1) {
          const int b = std::countr_zero(m);
          const int sd = I.side(b);
          const double w = I.weight(c, a, b) / h;
          add(kBkPnl, b, sd * w * r);
          add(kBkBench, b, sd * w * bm);
          if (keep >> b & 1) {
            add(kBkExpo, b, -w);
          } else if (F.exit(t, a, sd)) {
            add(kBkFix, b, w);
            add(kBkExpo, b, -w);
          } else {
            Stuck &k = stk[a * NB + static_cast<size_t>(b)];
            k.w += w, k.u += w * (1.0 + r), k.v += w * (1.0 + r);
            stk_m[a] |= uint64_t{1} << b;
          }
        }
      };
      // 批 t 建仓; mt = 本行意向位
      const auto entry = [&](int t, size_t a, uint64_t mt) {
        const size_t slot = a * nH + K.slot(t);
        const uint64_t keep = ring_keep[slot];
        ring_keep[slot] = 0;
        assert(ring_in[slot] == 0 && "批环槽未平仓就被新批占用");
        uint64_t in = 0;
        int can[2] = {-1, -1}; // 买得进 [空向, 多向] 懒求
        for (uint64_t m = mt; m; m &= m - 1) {
          const int b = std::countr_zero(m), sd = I.side(b);
          const double w = I.weight(t, a, b) / h;
          if (!(keep >> b & 1)) {
            const double held = stk_m[a] >> b & 1 ? stk[a * NB + static_cast<size_t>(b)].w : 0.0, buy = w - held;
            if (buy > 0.0) {
              int &cv = can[sd > 0];
              if (cv < 0)
                cv = F.enter(t, a, sd);
              if (!cv)
                continue;
              add(kBkImp, b, buy * F.cost_in(t, a));
              add(kBkFix, b, buy * F.unit_in);
            }
            if (held > 0.0)
              transfer(a, b, std::min(held, w));
          }
          in |= uint64_t{1} << b;
          add(kBkExpo, b, w);
        }
        ring_in[slot] = in;
      };
      for (int t = r0; t < r1; ++t) {
        if (!K.acts(t)) // 末行恒动 (Day 型 T − 1 = 末日末行)
          continue;
        row = t - r0;
        const bool op = K.opens(t), close = K.kind == BookKind::Close && t % S == S - 1, cross = K.kind != BookKind::Close && t >= span;
        for (size_t a = static_cast<size_t>(a0); a < static_cast<size_t>(a1); ++a) {
          if (F.marks()) {
            revalue(a, F.value(t, a));
            stuck_check(a, t);
          }
          const uint64_t mt = op ? I.mask(t, a) : 0; // 本行意向位: 交接与建仓同一份
          if (cross)
            settle(t - span, a, t, mt);
          if (op)
            entry(t, a, mt);
          if (close)
            for (int c = t - S + 1; c < t; ++c)
              settle(c, a, t, 0);
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

// 线 = 出参的线性组合 Σ k · f(b) (Inspect 分层 / LS 线, Portfolio G20 出参线, 池篮子指数 同一份)
inline constexpr int kLineTerms = 4;
struct LineTerm {
  int b = 0;            // 篮子
  BookField f = kBkPnl; // 字段
  double k = 1.0;
};
struct Line {
  int n = 0;
  LineTerm t[kLineTerms] = {};
};
// 线表所用的出参字段 (bk_bit 位或) = run_book 的 fields
inline unsigned fields_of(std::span<const Line> ls) {
  unsigned f = 0;
  for (const Line &l : ls)
    for (int j = 0; j < l.n; ++j)
      f |= bk_bit(l.t[j].f);
  return f;
}
// 篮子 b 的 pnl 原样一条线 (线 b = 篮子 b; 池篮子指数)
inline std::vector<Line> pnl_lines(int nb) {
  std::vector<Line> ls(static_cast<size_t>(nb));
  for (int b = 0; b < nb; ++b)
    ls[static_cast<size_t>(b)].t[ls[static_cast<size_t>(b)].n++] = {b, kBkPnl, 1.0};
  return ls;
}

// 线的期收益 (fold_lines 成品; Inspect 分层 / Portfolio 因子线 / 池篮子指数 同一形): [n][P], 线 i 点 p = 行 [p · stride, (p + 1) · stride) 的 Π(1 + x_t) − 1
struct BookLines {
  int n = 0, P = 0; // 线数 / 每线点数 (= T / stride)
  std::vector<float> v;
  const float *at(int i) const { return v.data() + static_cast<size_t>(i) * static_cast<size_t>(P); }
};
// 折叠: 行值 x_t = Σ k · f(b)[t], 每 stride 行一点 (stride 1 = 逐行原值; S = 日收益)
inline void fold_lines(misc::exec::ForkJoin &ex, const BookOut &o, std::span<const Line> ls, int stride, BookLines &out) {
  assert(!ls.empty() && stride >= 1 && o.T % stride == 0);
  const size_t P = static_cast<size_t>(o.T / stride), S = static_cast<size_t>(stride);
  out.n = static_cast<int>(ls.size()), out.P = static_cast<int>(P);
  out.v.assign(ls.size() * P, 0.f);
  ex.rows(out.n, [&](int i0, int i1, int) {
    for (int i = i0; i < i1; ++i) {
      const Line &l = ls[static_cast<size_t>(i)];
      assert(l.n >= 1 && l.n <= kLineTerms);
      const double *f[kLineTerms] = {};
      for (int j = 0; j < l.n; ++j) {
        assert(l.t[j].b < o.nb && !o.f[l.t[j].f].empty() && "线用了没要的字段");
        f[j] = o.at(l.t[j].f, l.t[j].b);
      }
      float *dst = out.v.data() + static_cast<size_t>(i) * P;
      for (size_t p = 0; p < P; ++p) {
        double v = 1.0;
        for (size_t t = p * S; t < (p + 1) * S; ++t) {
          double x = 0.0;
          for (int j = 0; j < l.n; ++j)
            x += l.t[j].k * f[j][t];
          v *= 1.0 + x;
        }
        dst[p] = static_cast<float>(v - 1.0);
      }
    }
  });
}

// 期收益 → 净值 (起点 1 不含): put(p, Π_{q ≤ p} (1 + r_q)). 一切净值 / 指数的复利只此一处 (UI: Inspect 分层 / Portfolio 组合 / 池指数)
template <class R, class Put>
void compound(const R *r, size_t n, Put &&put) {
  double l = 1.0;
  for (size_t p = 0; p < n; ++p) {
    l *= 1.0 + static_cast<double>(r[p]);
    put(p, l);
  }
}

} // namespace GUI::Factors
