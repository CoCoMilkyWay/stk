#pragma once

// =============================================================================
// Pos — 仓位感知唯一核 (Row 组 / 尾部 / 换手账 / 池篮子 / 单只 / Beta 选池 / Agent 观察同一份; 契约 factor/Stat/Contract.hpp)
// =============================================================================
//   一个组合 (成员判据 In 由调用方给) 每行一批 (做多), 格 a 在行 t 的批里:
//     真持有 held: 行 t 价点可买 (eo & kEoBuy), 或 与前一批交接 (Hold::nets) 且前一批同组合 (m ∧ 成员) 并真持有 —— 续持者不必成交, 封板也照持.
//       递归沿链回走到可成交为止, 前一批至多在 t 前 kChainDays 个交易日 (= 平仓结清窗; 更早视为断链), 所以每格只看
//       [t − kChainDays 日 − h, t + h] 的行: 分块带够前后带即与整张量一致; 块外视为断链.
//     收益: 下一批 (t + h) 交接且同组合 (续持) → 不真平仓, 按名义价点估值: 名义价点可卖 → lv 即名义值, 否则 = nv;
//       不续持 → lv (含顺延). 非交接持有期 (收盘型 / 开盘型) 无链, 自然退化为"建仓须可买, 平仓顺延".
//   【成员层级】判据给层级而非真假: In::level(o) = 偏 o 格 (o = 行偏移 · A) 的层级, −1 = 非成员, 0..L 嵌套 (层级 j 的成员 ⊂ j − 1 的).
//     层级 j 真持有 ⟺ 链上 (本批起至首个可买价点) 各批层级 ≥ j ⟺ walk 出的 held ≥ j; 续持同理 (cont ≥ j). 嵌套集合一次回走同出各级.
//   判据族 (全仓只此几种): InRank 因子组 (端组再按尾深分层) / InMask 成员位平面 (单只 / 池均 / Beta 选池) / 篮子判据 (gui/engine/Book.cpp)
// =============================================================================

#include "factor/Stat/Contract.hpp"

#include <cstddef>
#include <cstdint>

namespace factor::stat {

// 行 t 的批沿交接链的几何: t = 块内行号, 交接按绝对行 t0 + u 判; 前一批越过 t 前 kChainDays 日 / 块外视为断链
struct Chain {
  Hold hd;
  int t = 0, T = 0, A = 0, t0 = 0;
  FACTOR_HD bool nets(int u) const { // 行 u 的批与行 u − h 的批交接
    return u >= hd.h && u < T && t - (u - hd.h) <= kChainDays * hd.g.S && hd.nets(t0 + u);
  }
  FACTOR_HD Chain back() const { // 前一批 (行 t − h) 的链
    Chain c = *this;
    c.t -= hd.h;
    return c;
  }
};
FACTOR_HD inline Chain chain_of(int t, const Span &s, const Hold &hd) { return {hd, t, s.T, s.A, s.t0_abs}; }

// 一格在行 t 的仓位: held = 真持有的最深层级 (−1 未持有), cont = 下一批续持的最深层级 (−1 不续持), eo_next = 下一批价点码 (续持时)
struct Pos {
  int held = -1, cont = -1;
  uint8_t eo_next = 0;
};
// m / eo 已偏到行 t 的本格 (行 u 的格 = p[(u − t)·A]); in 同偏
template <class In>
FACTOR_HD inline Pos walk(const Chain &c, const uint8_t *m, const uint8_t *eo, const In &in) {
  Pos p;
  int lv = in.level(0);
  if (lv < 0)
    return p;
  const ptrdiff_t step = static_cast<ptrdiff_t>(c.hd.h) * c.A;
  for (int u = c.t;; u -= c.hd.h) {
    const ptrdiff_t o = static_cast<ptrdiff_t>(u - c.t) * c.A;
    if (eo[o] & kEoBuy)
      break;
    if (!c.nets(u) || !m[o - step])
      return p;
    const int l = in.level(o - step);
    if (l < 0)
      return p;
    lv = l < lv ? l : lv;
  }
  p.held = lv;
  if (c.nets(c.t + c.hd.h) && m[step]) {
    p.cont = in.level(step);
    p.eo_next = eo[step];
  }
  return p;
}
// 做多一格的收益: 续持 → 名义价点估值 (下一批价点可卖 → lv 就是名义值; 否则 nv); 不续持 → lv (含平仓顺延). y = {lv, nv}
FACTOR_HD inline float hold_ret(bool cont, uint8_t eo_next, const float *y) { return !cont || (eo_next & kEoSell) ? y[0] : y[1]; }
// 层级 j 上的收益
FACTOR_HD inline float ret_at(const Pos &p, int j, const float *y) { return hold_ret(p.cont >= j, p.eo_next, y); }

// ---- 判据族 ----
// 因子组: 本格的组 g (rx 已定向); 同组 → 0, 端组再按尾部深度 1..kTails (Contract【尾部】嵌套); 他组 / 无秩 → −1. rx 已偏到本格
struct InRank {
  const uint16_t *rx;
  int g;
  FACTOR_HD int level(ptrdiff_t o) const {
    const unsigned x = rx[o];
    if (x == kRankNone || grp_of(x) != g)
      return -1;
    const int e = g == end_group(kTop) ? kTop : g == end_group(kBot) ? kBot
                                                                      : -1;
    int l = 0;
    if (e >= 0)
      for (int j = 0; j < kTails; ++j)
        l += in_tail(e, x, j);
    return l;
  }
};
// 成员位平面: 非 0 = 成员 (层级 0). p 已偏到本格
struct InMask {
  const uint8_t *p;
  FACTOR_HD int level(ptrdiff_t o) const { return p[o] ? 0 : -1; }
};

} // namespace factor::stat
