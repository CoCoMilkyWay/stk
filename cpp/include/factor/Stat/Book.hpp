#pragma once

// =============================================================================
// Book — 账集唯一核 (因子两端 × 层级 / 因子组合 / 池篮子 / 个股 / 池均线 / Beta 选池同一份; 契约 Contract.hpp, 仓位 Pos.hpp)
// =============================================================================
//   一本做多账 = 成员判据 (Pos.hpp) × 目标权 × 满仓; 一行一持有期的若干本 = 账集 (使用方唯一要写的, 鸭子类型):
//     size()           本数 B
//     cap(b)           本 b 的满仓权 (0 = 按真持有权和归一)
//     each(t, a, put)  块内行 t 格 a 所属的本 (本号升序): put(b0, w, in) — 目标权原值 w, in 已偏到本格; 层级 l ≤ held 落本 b0 + l
//   格位 Slot = 资产 a 在本 b 里的一格: 本批 cur (r = 本批收益, 续持口径) ⇄ 前一批 pre (只交接时; r = 到交接点的名义收益)
//   目标权 w̄ = w / max(Σ_b w, cap_b); 前一批漂移到交接点 w̃ = w̄' (1 + r') / V', V' = 1 + Σ w̄' r'; Δ = w̄ − w̃ 逐格:
//     Δ⁺ 买 (本格不可买 → 加不了仓, 记 0) ⇄ Δ⁻ 卖; 本批平仓不与下一批交接 (settle) → 建仓时一并记整批平仓 w̄ (1 + r) / V (随价漂移)
//   本批收益 ret[b] = Σ w̄ r (实现于 Hold::exit 行). 流水 Flow 两侧: 成交量 vol + 付固定冲击的不可成交量 stuck (各金额段同一份)
//     + 各盘口段的真实冲击 imp (量 × 本格该侧冲击, 封顶 top). 交接点价点不可卖 (封跌停) 的卖出量记 stuck;
//     整批平仓恒在撮合价点 (define/Label.hpp 协议表 2): 盘口冲击 0
//   费率在 Flow::cost 现乘 (define/Fee.hpp 唯一一式): 固定段 = 全量付固定冲击, 盘口段 a = imp[a] + stuck 付固定冲击; 两侧另付税佣
//   派生量 (三级 HoldStat × 流水): 每期成本 / 净多空 / 年换手, 费率与金额段现选.
//   宿主专用 (换手不上设备). 出错策略: 只 assert. 【precise-math】编进 -fno-fast-math TU.
// =============================================================================

#include "define/Fee.hpp"
#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Fold.hpp"
#include "factor/Stat/Host.hpp"
#include "factor/Stat/Pos.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <span>
#include <vector>

namespace factor::stat {

inline constexpr int kLobAmts = kAmtCount - 1; // 盘口段数 (AmtId a ≥ 1 ↔ 下标 a − 1)

// 块内的盘口段冲击: imp[a − 1][侧] = [T][A] 平面 (原值 fp16 位, 池内盘口缺 = +inf); 未装的段为空. 封顶 top
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
};

// 一本账两侧的流水和 (沿行求和; 可合并)
struct Flow {
  double vol[kSides] = {}, stuck[kSides] = {}, imp[kLobAmts][kSides] = {};
  void merge(const Flow &o) {
    for (int s = 0; s < kSides; ++s) {
      vol[s] += o.vol[s], stuck[s] += o.stuck[s];
      for (int k = 0; k < kLobAmts; ++k)
        imp[k][s] += o.imp[k][s];
    }
  }
  // 金额段 a 一侧的真实冲击和 / 付固定冲击的量
  double imp_of(AmtId a, int s) const { return a == kAmtFixed ? 0.0 : imp[a - 1][s]; }
  double fix_of(AmtId a, int s) const { return a == kAmtFixed ? vol[s] : stuck[s]; }
  // 金额段 a 一侧的成本 (含税佣) / 两侧合计 / 一侧平均费率 (显示)
  double paid(AmtId a, int s, const Fee &f) const { return f.cost(imp_of(a, s), fix_of(a, s), s == kBuy ? vol[s] : 0.0, s == kSell ? vol[s] : 0.0); }
  double cost(AmtId a, const Fee &f) const { return paid(a, kBuy, f) + paid(a, kSell, f); }
  double rate(AmtId a, int s, const Fee &f) const { return vol[s] > 0.0 ? paid(a, s, f) / vol[s] : 0.0; }
};
// 多空两端各一本 (下标 = End)
struct Ends {
  Flow e[kEnds];
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
  std::vector<double> W, PW, V, PV; // [B] 本批 / 前一批的目标权和与资金 (1 + Σ w̄ r)
  std::vector<Slot> cur, pre;       // 一行的两列格位
};

// 行 c.t 的两列格位 (各按 (a, b) 升序): lb = 块首的标签视图
template <class Bk>
inline void book_slots(const Bk &bk, const Chain &c, const Label &lb, Work &w) {
  w.cur.clear(), w.pre.clear();
  const bool net = c.nets(c.t);
  const Chain pc = c.back();
  const Label lr = lb.at(static_cast<size_t>(c.t) * c.A), lp = net ? lb.at(static_cast<size_t>(pc.t) * c.A) : lr;
  for (uint32_t a = 0; a < static_cast<uint32_t>(c.A); ++a) {
    bk.each(c.t, a, [&](uint32_t b0, float wt, const auto &in) {
      float y[2];
      const Pos p = cell(c, lr, a, in, cpu::stat::H2F{}, y);
      for (int l = 0; l <= p.held; ++l)
        w.cur.push_back({a, b0 + static_cast<uint32_t>(l), wt, ret_at(p, l, y)});
    });
    if (net)
      bk.each(pc.t, a, [&](uint32_t b0, float wt, const auto &in) {
        float y[2];
        const Pos p = cell(pc, lp, a, in, cpu::stat::H2F{}, y);
        for (int l = 0; l <= p.held; ++l)
          w.pre.push_back({a, b0 + static_cast<uint32_t>(l), wt, hold_ret(true, lr.eo[a], y)});
      });
  }
  const auto key = [](const Slot &s) { return static_cast<uint64_t>(s.a) << 32 | s.b; };
  assert(std::ranges::is_sorted(w.cur, {}, key) && std::ranges::is_sorted(w.pre, {}, key) && "账集 each 须按本号升序给");
  (void)key;
}

// 一行: 两列格位 → ret[b] += 本批收益, out[b] += 两侧流水. settle = 本批平仓不与下一批交接; eo / c 已偏到行首; cap[b] = 本 b 满仓权
inline void turn_row(Work &w, bool settle, const uint8_t *eo, const Cost &c, std::span<const double> cap, double *ret, Flow *out) {
  const size_t B = cap.size();
  w.W.assign(B, 0.0), w.PW.assign(B, 0.0), w.V.assign(B, 1.0), w.PV.assign(B, 1.0);
  for (const Slot &s : w.cur)
    w.W[s.b] += s.w;
  for (const Slot &s : w.pre)
    w.PW[s.b] += s.w;
  const auto wt = [&](const Slot &s, const std::vector<double> &W) { return s.w / book_den(W[s.b], cap[s.b]); };
  for (const Slot &s : w.cur) {
    const double x = wt(s, w.W) * s.r;
    w.V[s.b] += x, ret[s.b] += x;
  }
  for (const Slot &s : w.pre)
    w.PV[s.b] += wt(s, w.PW) * s.r;
  const auto key = [](const Slot &s) { return static_cast<uint64_t>(s.a) << 32 | s.b; };
  const auto imp = [&](int k, int sd, uint32_t a) { return static_cast<double>(std::min(cpu::stat::h2f(c.imp[k][sd][a]), c.top)); };
  for (size_t i = 0, j = 0; i < w.cur.size() || j < w.pre.size();) {
    const bool has_c = i < w.cur.size() && (j == w.pre.size() || key(w.cur[i]) <= key(w.pre[j]));
    const bool has_p = j < w.pre.size() && (i == w.cur.size() || key(w.pre[j]) <= key(w.cur[i]));
    const Slot &s = has_c ? w.cur[i] : w.pre[j];
    const double x = has_c ? wt(s, w.W) : 0.0, px = has_p ? wt(w.pre[j], w.PW) * (1.0 + w.pre[j].r) / w.PV[s.b] : 0.0;
    const double d = x - px;
    Flow &o = out[s.b];
    if (d > 0.0 && (eo[s.a] & kEoBuy)) {
      o.vol[kBuy] += d;
      for (int k = 0; k < kLobAmts; ++k)
        if (c.imp[k][kBuy])
          o.imp[k][kBuy] += d * imp(k, kBuy, s.a);
    } else if (d < 0.0) {
      o.vol[kSell] -= d;
      if (!(eo[s.a] & kEoSell))
        o.stuck[kSell] -= d;
      else
        for (int k = 0; k < kLobAmts; ++k)
          if (c.imp[k][kSell])
            o.imp[k][kSell] -= d * imp(k, kSell, s.a);
    }
    if (settle && has_c)
      o.vol[kSell] += x * (1.0 + s.r) / w.V[s.b];
    i += has_c, j += has_p;
  }
}

// 账集一趟: 统计行 [t_lo, t_hi) 的开仓行 (Hold::opens) 逐行 book_slots + turn_row → sink(tid, t, ret[B], flow[B]) (线程内, 行不相交).
//   lb = 块首的标签视图 (一个持有期 hd); c = 块首的盘口段 (只付固定冲击 = Cost{})
template <class Exec, class Bk, class Sink>
inline void book_rows(Exec &ex, const Span &s, const Hold &hd, const Label &lb, const Bk &bk, const Cost &c, Sink &&sink) {
  const size_t B = bk.size();
  std::vector<double> cap(B);
  for (size_t b = 0; b < B; ++b)
    cap[b] = bk.cap(b);
  ex.rows(s.t_hi() - s.t_lo, [&](int r0, int r1, int tid) {
    Work w;
    std::vector<double> ret(B);
    std::vector<Flow> flow(B);
    for (int t = s.t_lo + r0; t < s.t_lo + r1; ++t) {
      const int ta = s.t0_abs + t;
      if (!hd.opens(ta))
        continue;
      const size_t row = static_cast<size_t>(t) * s.A;
      book_slots(bk, chain_of(t, s, hd), lb, w);
      std::fill(ret.begin(), ret.end(), 0.0);
      std::fill(flow.begin(), flow.end(), Flow{});
      turn_row(w, !hd.nets(ta + hd.h), lb.eo + row, c.at(row), cap, ret.data(), flow.data());
      sink(tid, t, ret.data(), flow.data());
    }
  });
}

// ---- 常用账集 ----
// 因子两端 × 层级 (本号 b = e · kLevels + l): rx = 块首的已定向 r16, 满仓 = n_full · level_frac(l)
inline constexpr int kFactorBooks = kEnds * kLevels;
struct FactorBooks {
  const uint16_t *rx = nullptr;
  int A = 0;
  double n_full = 0.0;
  size_t size() const { return kFactorBooks; }
  double cap(size_t b) const { return n_full * level_frac(static_cast<int>(b % kLevels)); }
  template <class Put>
  void each(int t, uint32_t a, Put &&put) const {
    const size_t i = static_cast<size_t>(t) * A + a;
    if (rx[i] == kRankNone)
      return;
    const int g = grp_of(rx[i]);
    for (int e = 0; e < kEnds; ++e)
      if (g == end_group(e))
        put(static_cast<uint32_t>(e * kLevels), 1.f, InRank{rx + i, g});
  }
};
// 成员位平面等权一本 (池均线: m = 标签有效位)
struct MaskBook {
  const uint8_t *m = nullptr;
  int A = 0;
  size_t size() const { return 1; }
  double cap(size_t) const { return 0.0; }
  template <class Put>
  void each(int t, uint32_t a, Put &&put) const {
    const size_t i = static_cast<size_t>(t) * A + a;
    if (m[i])
      put(0u, 1.f, InMask{m + i});
  }
};

// ---- 派生量 (三级 HoldStat × 账集流水; 金额段与费率现选) ----
// 一本的每期成本 (与 ls_period 同分母 n_all)
inline double cost_period(const HoldStat &s, const Flow &f, AmtId a, const Fee &fee) { return s.n_all > 0 ? f.cost(a, fee) / static_cast<double>(s.n_all) : 0.0; }
// 每期净多空 = ls_period − 两端每期成本的平均; 年化 = × span_per_year
inline double net_ls_period(const HoldStat &s, const Ends &c, AmtId a, const Fee &f) {
  return ls_period(s) - 0.5 * (cost_period(s, c.e[kTop], a, f) + cost_period(s, c.e[kBot], a, f));
}
inline double net_ls(const HoldStat &s, const Ends &c, AmtId a, const Fee &f) { return net_ls_period(s, c, a, f) * span_per_year(s); }
// 年化换手 (倍/年, 单边, 每本资金 1) = Σ Δ⁺ / n_all × span_per_year; 两端 = 两端均
inline double turn_annual(const HoldStat &s, const Flow &f) { return s.n_all > 0 ? f.vol[kBuy] / static_cast<double>(s.n_all) * span_per_year(s) : 0.0; }
inline double turn_annual(const HoldStat &s, const Ends &c) { return 0.5 * (turn_annual(s, c.e[kTop]) + turn_annual(s, c.e[kBot])); }

} // namespace factor::stat
