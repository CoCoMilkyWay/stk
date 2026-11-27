#pragma once

// =============================================================================
// Book — 账集唯一核 (因子两端 × 层级 / 因子组合 / 池篮子 / 个股 / Beta 选池同一份; 契约 Contract.hpp, 仓位 Pos.hpp) + 池基准 pool_mean
// =============================================================================
//   一本做多账 = 成员判据 (Pos.hpp) × 目标权 × 满仓; 一行一持有期的若干本 = 账集 (使用方唯一要写的, 鸭子类型):
//     size()           本数 B
//     cap(t, b)        块内行 t 本 b 的满仓权 (0 = 按真持有权和归一)
//     each(t, a, put)  块内行 t 格 a 所属的本 (本号升序): put(b0, w, in) — 目标权原值 w, in 已偏到本格; 层级 l ≤ held 落本 b0 + l
//   格位 Slot = 资产 a 在本 b 里的一格: 本批 cur (r = 本批收益, 续持口径) ⇄ 前一批 pre (只交接时; r = 到交接点的名义收益)
//   目标权 w̄ = w / max(Σ_b w, cap_b); 前一批漂移到交接点 w̃ = w̄' (1 + r') / V', V' = 1 + Σ w̄' r'; Δ = w̄ − w̃ 逐格:
//     Δ⁺ 买 (本格不可买 → 加不了仓, 记 0) ⇄ Δ⁻ 卖; 本批平仓不与下一批交接 (settle) → 建仓时一并记整批平仓 w̄ (1 + r) / V (随价漂移)
//   本批收益 ret[b] = Σ w̄ r (实现于 Hold::exit 行; 因子两端与 Row 的组 / 尾部同式, 对拍互证). 流水 TurnFlow (define/Fee.hpp 字段表) 两侧:
//     成交量 + 卖不出的量 (各口径段同一份) + 各盘口段的真实冲击 (量 × 该价点该侧冲击, 封顶 top). 交接点在本行价点; 整批平仓在平仓价点
//     (define/Label.hpp 协议表 2): E (Hold::exit_e) 取平仓行的卖出冲击 (卖不出记 stuck), 撮合价冲击 0
//   费率在 Fee::parts 现乘 (define/Fee.hpp 唯一一式). 派生量 (三级 HoldStat × 已实现流水): 每期成本 / 净多空 / 年换手, 费率与口径段现选.
//   宿主专用 (换手不上设备). 出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "define/Annual.hpp"
#include "define/Fee.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Fold.hpp"
#include "factor/Stat/Host.hpp"
#include "factor/Stat/Pos.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <vector>

namespace factor::stat {

// 块内的盘口段冲击: imp[盘口段][侧] = [T][A] 平面 (原值 fp16 位, 池内盘口缺 = +inf); 未装 / 无平面 (日层) 的段为空 = 冲击 0. 封顶 top
struct Cost {
  const uint16_t *imp[kLobAmts][kSides] = {};
  float top = 0.f;
  Cost at(size_t i) const {
    Cost c = *this;
    for (auto &side : c.imp)
      for (const uint16_t *&p : side)
        p = p ? p + i : nullptr;
    return c;
  }
  double of(int k, int s, uint32_t a) const { return std::min(static_cast<double>(cpu::stat::h2f(imp[k][s][a])), static_cast<double>(top)); }
};

// 多空两端各一本 (下标 = End)
struct Ends {
  TurnFlow e[kEnds];
  void merge(const Ends &o) {
    for (int k = 0; k < kEnds; ++k)
      e[k].merge(o.e[k]);
  }
};

// 一格在一本账里: 资产 a / 本号 b / 目标权原值 w / 收益 r
struct Slot {
  uint32_t a = 0, b = 0;
  float w = 0.f, r = 0.f;
};
// 每线程的行工作区 (跨行复用)
struct Work {
  std::vector<double> W, PW, V, PV, cap; // [B] 本批 / 前一批的目标权和与资金 (1 + Σ w̄ r) / 本行满仓
  std::vector<Slot> cur, pre;            // 一行的两列格位
};

// 行 c.t 的两列格位 (各按 (a, b) 升序): lb / eo = 块首的标签视图 / 建仓可成交码
template <class Bk>
inline void book_slots(const Bk &bk, const Chain &c, const Label &lb, const uint8_t *eo, Work &w) {
  w.cur.clear(), w.pre.clear();
  const bool net = c.nets(c.t);
  const Chain pc = c.back();
  const size_t row = static_cast<size_t>(c.t) * c.A, prow = net ? static_cast<size_t>(pc.t) * c.A : row;
  const Label lr = lb.at(row), lp = lb.at(prow);
  const uint8_t *er = eo + row, *ep = eo + prow;
  for (uint32_t a = 0; a < static_cast<uint32_t>(c.A); ++a) {
    bk.each(c.t, a, [&](uint32_t b0, float wt, const auto &in) {
      float y[2];
      const Pos p = cell(c, lr, er, a, in, cpu::stat::H2F{}, y);
      for (int l = 0; l <= p.held; ++l)
        w.cur.push_back({a, b0 + static_cast<uint32_t>(l), wt, ret_at(p, l, y)});
    });
    if (net)
      bk.each(pc.t, a, [&](uint32_t b0, float wt, const auto &in) {
        float y[2];
        const Pos p = cell(pc, lp, ep, a, in, cpu::stat::H2F{}, y);
        for (int l = 0; l <= p.held; ++l)
          w.pre.push_back({a, b0 + static_cast<uint32_t>(l), wt, hold_ret(true, er[a], y)});
      });
  }
  const auto key = [](const Slot &s) { return static_cast<uint64_t>(s.a) << 32 | s.b; };
  assert(std::ranges::is_sorted(w.cur, {}, key) && std::ranges::is_sorted(w.pre, {}, key) && "账集 each 须按本号升序给");
  (void)key;
}

// 价点 (eo / c 已偏到其行首) 上卖出 v: 可卖 → 各盘口段冲击; 卖不出 → stuck
inline void sell_at(TurnFlow &o, double v, const uint8_t *eo, const Cost &c, uint32_t a) {
  if (!(eo[a] & kEoSell)) {
    o[kFlowStuck] += v;
    return;
  }
  for (int k = 0; k < kLobAmts; ++k)
    if (c.imp[k][kSell])
      o[flow_imp(static_cast<LobAmt>(k), kSell)] += v * c.of(k, kSell, a);
}

// 一行: 两列格位 → ret[b] += 本批收益, out[b] += 两侧流水. eo / c = 本行价点 (交接 / 建仓); xe / xc = 整批平仓的 E 价点 (平仓行; 不平仓或撮合价 → xe 空)
inline void turn_row(Work &w, const uint8_t *eo, const Cost &c, const uint8_t *xe, const Cost &xc, bool settle, double *ret, TurnFlow *out) {
  const size_t B = w.cap.size();
  w.W.assign(B, 0.0), w.PW.assign(B, 0.0), w.V.assign(B, 1.0), w.PV.assign(B, 1.0);
  for (const Slot &s : w.cur)
    w.W[s.b] += s.w;
  for (const Slot &s : w.pre)
    w.PW[s.b] += s.w;
  const auto wt = [&](const Slot &s, const std::vector<double> &W) { return s.w / book_den(W[s.b], w.cap[s.b]); };
  for (const Slot &s : w.cur) {
    const double x = wt(s, w.W) * s.r;
    w.V[s.b] += x, ret[s.b] += x;
  }
  for (const Slot &s : w.pre)
    w.PV[s.b] += wt(s, w.PW) * s.r;
  const auto key = [](const Slot &s) { return static_cast<uint64_t>(s.a) << 32 | s.b; };
  for (size_t i = 0, j = 0; i < w.cur.size() || j < w.pre.size();) {
    const bool has_c = i < w.cur.size() && (j == w.pre.size() || key(w.cur[i]) <= key(w.pre[j]));
    const bool has_p = j < w.pre.size() && (i == w.cur.size() || key(w.pre[j]) <= key(w.cur[i]));
    const Slot &s = has_c ? w.cur[i] : w.pre[j];
    const double x = has_c ? wt(s, w.W) : 0.0, px = has_p ? wt(w.pre[j], w.PW) * (1.0 + w.pre[j].r) / w.PV[s.b] : 0.0;
    const double d = x - px;
    TurnFlow &o = out[s.b];
    if (d > 0.0 && (eo[s.a] & kEoBuy)) {
      o[kFlowBuy] += d;
      for (int k = 0; k < kLobAmts; ++k)
        if (c.imp[k][kBuy])
          o[flow_imp(static_cast<LobAmt>(k), kBuy)] += d * c.of(k, kBuy, s.a);
    } else if (d < 0.0) {
      o[kFlowSell] -= d;
      sell_at(o, -d, eo, c, s.a);
    }
    if (settle && has_c) {
      const double v = x * (1.0 + s.r) / w.V[s.b];
      o[kFlowSell] += v;
      if (xe)
        sell_at(o, v, xe, xc, s.a);
    }
    i += has_c, j += has_p;
  }
}

// 账集一趟: 统计行 [t_lo, t_hi) 的开仓行 (Hold::opens) 逐行 book_slots + turn_row → sink(tid, t, ret[B], flow[B]).
//   按日切线程 (同日的行同一线程, 行序; 期线可经 Realize 直落); lb / eo = 块首的标签视图 / 建仓可成交码 (一个持有期 hd);
//   c = 块首的盘口段 (只付固定冲击 = Cost{}); 整批平仓的 E 价点在后带内 (平仓行 < T)
template <class Exec, class Bk, class Sink>
inline void book_rows(Exec &ex, const Span &s, const Hold &hd, const Label &lb, const uint8_t *eo, const Bk &bk, const Cost &c, Sink &&sink) {
  const size_t B = bk.size();
  const int S = s.g.S;
  ex.rows(s.days(), [&](int d0, int d1, int tid) {
    Work w;
    w.cap.resize(B);
    std::vector<double> ret(B);
    std::vector<TurnFlow> flow(B);
    for (int t = s.t_lo + d0 * S; t < s.t_lo + d1 * S; ++t) {
      if (!hd.opens(t))
        continue;
      const size_t row = static_cast<size_t>(t) * s.A;
      book_slots(bk, chain_of(t, s, hd), lb, eo, w);
      for (size_t b = 0; b < B; ++b)
        w.cap[b] = bk.cap(t, b);
      std::fill(ret.begin(), ret.end(), 0.0);
      std::fill(flow.begin(), flow.end(), TurnFlow{});
      const bool settle = !hd.nets(t + hd.h), at_e = settle && hd.exit_e(t);
      const size_t xrow = at_e ? static_cast<size_t>(hd.exit(t)) * s.A : row;
      assert((!at_e || hd.exit(t) < s.T) && "整批平仓的 E 价点须在块内 (后带 ≥ hold_reach)");
      turn_row(w, eo + row, c.at(row), at_e ? eo + xrow : nullptr, c.at(xrow), settle, ret.data(), flow.data());
      sink(tid, t, ret.data(), flow.data());
    }
  });
}

// ---- 常用账集 ----
// 因子两端 × 层级 (本号 b = e · levels + l): rx = 块首的已定向 r16, cap = 块首的逐行满仓 (Label::cap), 层级满仓 = cap · level_frac(l);
//   只要前 ends 端 (kTop 起) × 前 levels 级的本 (组合账 = 顶端整组一本: ends = levels = 1), 不算用不到的本
inline constexpr int kFactorBooks = kEnds * kLevels;
struct FactorBooks {
  const uint16_t *rx = nullptr;
  int A = 0;
  const float *cap_ = nullptr;
  int ends = kEnds, levels = kLevels;
  size_t size() const { return static_cast<size_t>(ends * levels); }
  double cap(int t, size_t b) const { return static_cast<double>(cap_[t]) * level_frac(static_cast<int>(b % static_cast<size_t>(levels))); }
  template <class Put>
  void each(int t, uint32_t a, Put &&put) const {
    const size_t i = static_cast<size_t>(t) * A + a;
    if (rx[i] == kRankNone)
      return;
    const int g = grp_of(rx[i]);
    for (int e = 0; e < ends; ++e)
      if (g == end_group(e))
        put(static_cast<uint32_t>(e * levels), 1.f, InCap<InRank>{InRank{rx + i, g}, levels - 1});
  }
};

// ---- 池基准 (契约 Label::mk / cap; 与因子无关, 装载 / 原料带 / 造数同一份) ----
// 平面 p 的行 [t0, t1): 池内全部有标签的格各自做多 (可买才建仓 / 续持取名义值; 判据 InMask) 的等权收益 mk = 池均线的批收益,
//   满仓 cap = 有效格数 / kGroups. 交接链在平面内走 (平面外断链; 统计行由前后带盖住)
template <class Exec>
inline void pool_mean(Exec &ex, LabelPlanes &p, const uint8_t *eo, const Hold &hd, int t0, int t1) {
  const int T = static_cast<int>(p.mk.size()), A = static_cast<int>(p.A());
  assert(0 <= t0 && t0 <= t1 && t1 <= T);
  const Label lb = label_of(p);
  ex.rows(t1 - t0, [&](int r0, int r1, int) {
    for (int t = t0 + r0; t < t0 + r1; ++t) {
      const size_t o = static_cast<size_t>(t) * A;
      const Label lr = lb.at(o);
      const Chain c{hd, t, T, A};
      double s = 0.0;
      int n = 0, held = 0;
      for (int a = 0; a < A; ++a) {
        if (!lr.m[a])
          continue;
        ++n;
        float y[2];
        const Pos q = cell(c, lr, eo + o, a, InMask{lr.m + a}, cpu::stat::H2F{}, y);
        if (q.held >= 0)
          s += ret_at(q, 0, y), ++held;
      }
      p.mk[static_cast<size_t>(t)] = held ? static_cast<float>(s / held) : 0.f;
      p.cap[static_cast<size_t>(t)] = static_cast<float>(n) / kGroups;
    }
  });
}

// ---- 派生量 (三级 HoldStat × 账集流水; 流水已实现: 逐日 ÷ h̄ 记建仓日, 与 Realize 同约; 口径段与费率现选) ----
// 已实现的量 x (全期之和) 折成每期 (一批) 均值, 与 ls_period 同分母 n_all
inline double per_period(const HoldStat &s, double x) { return s.n_all > 0 ? x * s.hd().span() / static_cast<double>(s.n_all) : 0.0; }
// 一本的每期成本
inline double cost_period(const HoldStat &s, const TurnFlow &f, AmtId a, const Fee &fee) { return per_period(s, fee.cost(f, a)); }
// 每期净多空 = ls_period − 两端每期成本的平均; 年化 = × span_per_year
inline double net_ls_period(const HoldStat &s, const Ends &c, AmtId a, const Fee &f) {
  return ls_period(s) - 0.5 * (cost_period(s, c.e[kTop], a, f) + cost_period(s, c.e[kBot], a, f));
}
inline double net_ls(const HoldStat &s, const Ends &c, AmtId a, const Fee &f) { return net_ls_period(s, c, a, f) * span_per_year(s); }
// 年化换手 (倍/年, 单边, 每本资金 1) = 已实现买量的日均 × 每年交易日 (行情线 / 选池账同一式 per_year); 两端 = 两端均
inline double turn_annual(const HoldStat &s, const TurnFlow &f) { return s.n_all > 0 ? per_year(f.vol(kBuy), static_cast<double>(s.n_all) / s.seg) : 0.0; }
inline double turn_annual(const HoldStat &s, const Ends &c) { return 0.5 * (turn_annual(s, c.e[kTop]) + turn_annual(s, c.e[kBot])); }

} // namespace factor::stat
