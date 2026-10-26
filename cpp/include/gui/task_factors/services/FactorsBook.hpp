// FactorsBook — 分批持仓净值引擎: 一切指数 / 净值评估同一份 (Portfolio 因子线 / 池篮子指数 / Inspect 分层累计). 宿主 CPU, 资产条带并行.
//
// 模型 (假设可加杠杆, 无资金约束 → 开仓不受已占用资金反馈, 不震荡):
//   每个换仓点 c 开一批 (cohort), 资金 = 当前净值的 1/h (h = 同时在场的批数), 批内按篮子权 w(c, a) 分到意向成员 (Σw ≤ 1, 余下是现金;
//   信号 = 换仓行本行). 批 c 的平仓点 = 后继批的换仓点 (同一时刻), 收益 = ret(c, a); 只多一层"能不能成交"的判断, 能就做, 不能下一个时点再判:
//     平仓: 后继批仍是该篮子意向成员 → 交接 (留仓, 不成交); 否则要可卖 (空腿: 可买), 卖不出 → 卡住: 沿价格链继续估值,
//       之后每个可成交时点再判, 能卖就卖.
//     建仓: 交接 → 不成交; 该篮子有卡住的仓 → 转交 min(卡住权, 新权) (不成交, 卡住的余量照卡), 不足的部分补买;
//       要新买的 (含补买) 须买得进, 买不进 → 本批不建该篮子 (那份钱空着, 卡住的照卡).
//     成本只在真成交时记: 买 = 冲击比例 + 固定冲击份数; 卖 = 1 份固定冲击 (固定冲击只出份数 fix, 调用方乘 fixed_impact).
//   型 BookKind (持有期键 → 型; 尾部 = Stat tail_masked, 与换手成本同口径):
//     Row   分钟型 (池指数 h = 1): 每行 t 一批 (非尾部, 且 t + h 在区间内), 行 t + h 平 = 批 t + h 的换仓点. 收益 / 买得进 / 冲击 = src (标签)
//     Close 收盘型 (h = S): 每行 t 一批 (非尾部), 当日收盘全平 (T0); 收盘不是换仓点 → 无交接. 收益 / 买得进 / 冲击 = src (标签)
//     Day   T+N 型 (h = N): 每日收盘一批 (越过区间末的不开). 成交全在引擎内定: 价 = px 链, 可成交 = 末行有价 ∧ 行末可成交位;
//           收盘竞价单一价 → 无冲击, 买卖各 1 份固定冲击 (不调 src 的 enter / ret / cost_in)
//   平仓看行末可成交位, Row / Close 卡住的仓按 px 估值; 无可成交平面 (Lob 档) → Row / Close 恒可平, 不会卡住.
//   出参 (占净值, 已含 1/h) 逐篮子逐行, 按 BookSpec::fields 只出要的: pnl = 本行实现的收益 (平仓点 / 卡住的卖出或转交行),
//     bench = 实现批的基准 Σ side·w·bench(c) (超额 = pnl − bench; bench(c) = 批 c 意向非空 ∧ 买得进 (多向) 的资产收益等权均值,
//     Row / Close 即 Stat Row::mkt[kLong]), imp / fix = 成交冲击 / 固定冲击份数, expo = 行末仓位 (含卡住的, 1 = 满仓).
//     净值 = 逐行 (1 + 增量) 连乘, 调用方做. 区间末仍卡住的按末次估值实现 (不计卖出成本).
//   空腿 (side −1): 收益取负, 建仓看可卖 (Row / Close 同 src.enter), 平仓看可买.
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
//     池指数      引擎 Row (日层 h = 1) D 日末行   C(D)                  C(D+1)
//     m + n 越过 kLastAnchorRow (分钟型) / m 越过它 (收盘型) 的行是尾部 (tail_masked), 不开仓.
//     Row / Close 的价就在标签里 (src.ret); Day / 池指数用后复权收盘 px, 无成交带上一有效价.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Host.hpp" // h2f (RankSrc)
#include "misc/exec.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>
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
  int h = 0;               // 同时在场的批数: Row = hold_rows (池指数 1) / Close = S / Day = N
  int hold = 0;            // 持有期键: Row / Close 据此判段末尾部; 0 = 不判 (池指数)
  BookKind kind = BookKind::Row;
  int nb = 0;                     // 篮子数
  int8_t side[kBookMax] = {};     // ±1: 多 / 空
  unsigned fields = 0;            // 要的出参 (bk_bit 位或; pnl 恒出)
  const uint8_t *trade = nullptr; // [T][A] kTr* 位; nullptr = 恒可成交
  const float *px = nullptr;      // [T][A] 后复权收盘 (≤ 0 = 本行无成交): 卡住估值 (Row / Close, 有 trade 才要) / Day 换仓价
  double unit_in = 0.0;           // Row / Close 每买入一份记的固定冲击份数 (Bar 1: 建仓也付 fixed; Lob 0: 建仓只付 cost_in); Day 恒 1
};
struct BookOut {
  int nb = 0, T = 0;
  std::vector<double> f[kBkFields]; // [字段][nb · T], 篮子 b 行 t 在 [b · T + t]; 没要的字段空
  const double *at(BookField k, int b) const { return f[k].data() + static_cast<size_t>(b) * T; }
};

// 意向权 [T][nb] (【持仓口径】; 只看意向, 与持有期 / 能否成交无关): 行 t 篮子 b = 1 / book_den(行 t 篮子 b 的意向只数, cap_of(b)).
// rx = r16 平面 [T][A], bits(r16) = 意向篮子位 (同 RankSrc)
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

// src 的接口 (调用方的篮子定义; 全部 const, 多线程并发调):
//   uint64_t mask(int t, size_t a)        信号行 t 资产 a 的意向篮子位 (与能否成交无关)
//   double weight(int t, size_t a, int b) 信号行 t 篮子 b 内的权 (Σ_a ≤ 1)
//   bool enter(int t, size_t a)           Row / Close: 行 t 买得进 (两腿同判)
//   double ret(int c, size_t a)           Row / Close: 批 c 到平仓点的做多收益
//   double cost_in(int t, size_t a)       Row / Close: 建仓冲击 (比例; 无 = 0)
// 秩平面上的 src (Inspect 分层 / Portfolio G20 同一份): 意向 = bits(r16), 权 = intent_weights; 收益 = 做多标签 (空腿 side 取负),
//   买得进 = 标签有效位 (Stat 同口径: 建仓行两向可成交); 建仓冲击 = 买入冲击
template <class Bits>
struct RankSrc {
  const uint16_t *rx;  // [T][A] r16
  Bits bits;           // r16 → 意向篮子位
  const float *wt;     // [T][nb] intent_weights
  int nb;              // 篮子数
  const uint16_t *lv;  // [T][A] 做多标签 fp16 位
  const uint8_t *lm;   // [T][A] 标签有效位
  const uint16_t *imp; // [T][A] 买入冲击 fp16 位; nullptr = 不计
  size_t A;
  uint64_t mask(int t, size_t a) const { return bits(rx[static_cast<size_t>(t) * A + a]); }
  double weight(int t, size_t, int b) const { return wt[static_cast<size_t>(t) * static_cast<size_t>(nb) + static_cast<size_t>(b)]; }
  bool enter(int t, size_t a) const { return lm[static_cast<size_t>(t) * A + a]; }
  double ret(int c, size_t a) const { return factor::cpu::stat::h2f(lv[static_cast<size_t>(c) * A + a]); }
  double cost_in(int t, size_t a) const { return imp ? factor::cpu::stat::h2f(imp[static_cast<size_t>(t) * A + a]) : 0.0; }
};

template <class Src>
void run_book(misc::exec::ForkJoin &ex, const BookSpec &s, const Src &src, BookOut &o) {
  const bool day = s.kind == BookKind::Day;
  assert(s.T >= 1 && s.A >= 1 && s.S >= 1 && s.T % s.S == 0 && s.h >= 1 && s.nb >= 1 && s.nb <= kBookMax);
  assert((!s.trade || s.px) && "卡住的仓要估值价");
  assert((!day || s.px) && "Day 型要换仓价");
  assert(s.kind != BookKind::Close || (s.h == s.S && s.hold == factor::stat::kHoldClose));
  const int T = s.T, S = s.S, h = s.h, nb = s.nb, days = T / s.S, span = day ? h * S : h; // span: 批 c 的平仓行 = c + span (Row / Day)
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
  // 换仓点 (开一批; 见文件头型)
  const auto opens = [&](int t) {
    const int D = t / S, r = t % S;
    switch (s.kind) {
    case BookKind::Row:
      return t + h < T && !(s.hold > 0 && factor::stat::tail_masked(t, s.hold, g));
    case BookKind::Close:
      return !factor::stat::tail_masked(t, s.hold, g);
    case BookKind::Day:
      return r == S - 1 && D + h < days;
    }
    return false;
  };
  const auto slot_c = [&](int c) { return static_cast<size_t>(day ? c / S % h : c % h); }; // 批 c 的环槽 (后继批同槽)
  const auto pt_px = [&](int D, size_t a) {                                                // Day 型 D 日换仓价 = 末行收盘 (≤ 0 = 无成交)
    return s.px[(static_cast<size_t>(D) * S + static_cast<size_t>(S - 1)) * nA + a];
  };
  // Day 型价格链 [days][A]: 换仓价, 无成交带上一有效价 (0 = 尚无)
  std::vector<float> P;
  if (day) {
    P.assign(static_cast<size_t>(days) * nA, 0.f);
    ex.rows(s.A, [&](int a0, int a1, int) {
      for (size_t a = static_cast<size_t>(a0); a < static_cast<size_t>(a1); ++a) {
        float l = 0.f;
        for (int D = 0; D < days; ++D) {
          const float p = pt_px(D, a);
          l = p > 0.f ? p : l;
          P[static_cast<size_t>(D) * nA + a] = l;
        }
      }
    });
  }
  const auto ret = [&](int c, size_t a) -> double {
    if (!day)
      return src.ret(c, a);
    const size_t i = static_cast<size_t>(c / S) * nA + a;
    assert(P[i] > 0.f && "Day 型批的建仓日须有价");
    return static_cast<double>(P[i + H * nA]) / P[i] - 1.0;
  };
  // 行 t 资产 a 能否成交 (open_side = 建仓 / 平仓, sd = 腿向): 行末可成交位 (无平面恒可); Day 另要本行有价
  const auto can_trade = [&](int t, size_t a, bool open_side, int sd) {
    if (day && !(pt_px(t / S, a) > 0.f))
      return false;
    return !s.trade || (s.trade[static_cast<size_t>(t) * nA + a] & (open_side == (sd > 0) ? kTrBuy : kTrSell)) != 0;
  };
  const auto can_enter = [&](int t, size_t a, int sd) { return day ? can_trade(t, a, true, sd) : src.enter(t, a); };
  // 基准 [T] (要 bench 才算): 批 c = 意向非空 ∧ 买得进 (多向) 的资产收益等权均值
  std::vector<double> bench;
  if (slot_of[kBkBench] >= 0) {
    bench.assign(nT, 0.0);
    ex.rows(T, [&](int t0, int t1, int) {
      for (int c = t0; c < t1; ++c) {
        if (!opens(c))
          continue;
        double sm = 0.0;
        int n = 0;
        for (size_t a = 0; a < nA; ++a)
          if (src.mask(c, a) && can_enter(c, a, 1))
            sm += ret(c, a), ++n;
        bench[static_cast<size_t>(c)] = n ? sm / n : 0.0;
      }
    });
  }

  // 资产状态 (跨块): 批环 [a][slot_c] 已建仓位 / 交接位 (批 c 平仓时定, 后继批建仓时用); 卡住的仓 [a][b]; 上一有效估值价
  struct Stuck {
    double w = 0.0, u = 0.0, v = 0.0; // 权 / 平仓点的值 w (1 + ret) (已实现到这) / 现值
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
      const auto stuck_check = [&](size_t a, int t) {
        for (uint64_t m = stk_m[a]; m; m &= m - 1) {
          const int b = std::countr_zero(m);
          if (can_trade(t, a, false, s.side[b]))
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
          add(kBkPnl, b, s.side[b] * (k.v - k.u) * f);
          add(kBkExpo, b, -x);
          k.w -= x, k.u -= k.u * f, k.v -= k.v * f;
        }
      };
      // 批 c 在本行 (平仓点 t = 后继批的换仓点) 平仓
      const auto settle = [&](int c, size_t a, int t) {
        const size_t slot = a * H + slot_c(c);
        const uint64_t in = ring_in[slot];
        ring_in[slot] = 0;
        const uint64_t keep = in && s.kind != BookKind::Close && opens(t) ? src.mask(t, a) & in : 0;
        ring_keep[slot] = keep;
        if (!in)
          return;
        const double r = ret(c, a), bm = bench.empty() ? 0.0 : bench[static_cast<size_t>(c)];
        for (uint64_t m = in; m; m &= m - 1) {
          const int b = std::countr_zero(m);
          const int sd = s.side[b];
          const double w = src.weight(c, a, b) / h;
          add(kBkPnl, b, sd * w * r);
          add(kBkBench, b, sd * w * bm);
          if (keep >> b & 1) {
            add(kBkExpo, b, -w);
          } else if (can_trade(t, a, false, sd)) {
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
        const size_t slot = a * H + slot_c(t);
        const uint64_t keep = ring_keep[slot];
        ring_keep[slot] = 0;
        assert(ring_in[slot] == 0 && "批环槽未平仓就被新批占用");
        uint64_t in = 0;
        int can[2] = {-1, -1}; // 买得进 [空向, 多向] 懒求
        for (uint64_t m = src.mask(t, a); m; m &= m - 1) {
          const int b = std::countr_zero(m), sd = s.side[b];
          const double w = src.weight(t, a, b) / h;
          if (!(keep >> b & 1)) {
            const double held = stk_m[a] >> b & 1 ? stk[a * NB + static_cast<size_t>(b)].w : 0.0, buy = w - held;
            if (buy > 0.0) {
              int &cv = can[sd > 0];
              if (cv < 0)
                cv = can_enter(t, a, sd);
              if (!cv)
                continue;
              add(kBkImp, b, day ? 0.0 : buy * src.cost_in(t, a));
              add(kBkFix, b, buy * (day ? 1.0 : s.unit_in));
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
        row = t - r0;
        const int D = t / S, r = t % S;
        const bool pt = !day || r == S - 1; // Day 型只在换仓点 (末行) 动
        for (size_t a = static_cast<size_t>(a0); a < static_cast<size_t>(a1); ++a) {
          if (pt) {
            if (day)
              revalue(a, P[static_cast<size_t>(D) * nA + a]);
            else if (s.trade)
              revalue(a, s.px[static_cast<size_t>(t) * nA + a]);
            if (day || s.trade)
              stuck_check(a, t);
            if (s.kind != BookKind::Close && t >= span)
              settle(t - span, a, t);
            if (opens(t))
              entry(t, a);
            if (s.kind == BookKind::Close && r == S - 1)
              for (int c = D * S; c < t; ++c)
                settle(c, a, t);
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
