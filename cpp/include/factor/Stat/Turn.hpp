#pragma once

// =============================================================================
// Turn — 换手唯一核的契约 (因子两端 × 层级 / 因子组合 / 池篮子 / 单只 / 池均 / Beta 选池同一份; 行核 turn_row 在 Stat/Host.hpp)
// =============================================================================
//   一本做多账 = 成员判据 (Pos.hpp) × 层级 × 目标权 × 满仓; 一行一持有期的若干本 = 账集 (本号 b, 满仓权 cap[b]):
//     因子     端 e × 层级 l ∈ {整组, 尾 0..2} (b = e · kLevels + l), 目标权原值 1, cap = n_full · level_frac(l)
//     组合     (顶端, 整组) 那一本
//     篮子     篮子号 b, 目标权原值 = 持仓权 (shared/Basket.hpp Weigh), cap 0 (按真持有权和归一)
//     单只/池均 Self 判据, 目标权原值 1, cap 0
//   格位 Slot = 资产 a 在本 b 里的一格 (调用方按 (a, b) 升序给): 本批 cur (r = 本批收益, 续持口径) ⇄ 前一批 pre (只交接时; r = 到交接点的名义收益)
//   目标权 w̄ = w / max(Σ_b w, cap_b); 前一批漂移到交接点 w̃ = w̄' (1 + r') / V', V' = 1 + Σ w̄' r'; Δ = w̄ − w̃ 逐格:
//     Δ⁺ 买 (本格不可买 → 加不了仓, 记 0) ⇄ Δ⁻ 卖; 本批平仓不与下一批交接 (settle) → 建仓时一并记整批平仓 w̄ (1 + r) / V (随价漂移)
//   本批收益 ret[b] = Σ w̄ r (实现于 Hold::exit 行); 两侧量 × 本格该侧冲击 (盘口段, 封顶 top) 或记进固定量 (固定段 / 本格不可卖的卖出量); 税佣在 Fee::paid 现乘
// =============================================================================

#include "factor/Stat/Contract.hpp"

#include <cstdint>

namespace factor::stat {

// 一个金额段: 盘口段 = 两侧冲击 [T][A] 平面的指针 (原值 fp16 位, 池内盘口缺 = +inf) + 封顶 top; 固定段 = 空
struct Cost {
  const uint16_t *imp[kSides] = {};
  float top = 0.f;
  bool flat() const { return !imp[kBuy]; }
  Cost at(size_t i) const { return flat() ? Cost{} : Cost{{imp[kBuy] + i, imp[kSell] + i}, top}; }
};

// 一本账两侧的流水和 (沿行求和): 成交量 / Σ 量·真实冲击 (盘口段) / 付固定冲击的量 (固定段)
struct Turn {
  double vol[kSides] = {}, imp[kSides] = {}, fix[kSides] = {};
  void merge(const Turn &o) {
    for (int s = 0; s < kSides; ++s)
      vol[s] += o.vol[s], imp[s] += o.imp[s], fix[s] += o.fix[s];
  }
  double paid(int s, const Fee &f) const { return f.paid(static_cast<Side>(s), vol[s], imp[s], fix[s]); }
};

// 一层级 (整组 / 尾部 j) 两端各一本的换手和; 可合并
struct CostAcc {
  Turn book[kEnds];
  void merge(const CostAcc &o) {
    for (int e = 0; e < kEnds; ++e)
      book[e].merge(o.book[e]);
  }
  // 每期真实成本 (端 e): 两侧 Σ 量 · 费率 / n_all (与 ls_period 同分母)
  double cost(int e, const Fee &f, int n_all) const { return n_all > 0 ? (book[e].paid(kBuy, f) + book[e].paid(kSell, f)) / static_cast<double>(n_all) : 0.0; }
  // 一侧的平均费率 (显示用): Σ 量 · 费率 / Σ 量
  double rate(int e, int s, const Fee &f) const { return book[e].vol[s] > 0.0 ? book[e].paid(s, f) / book[e].vol[s] : 0.0; }
};

// 一格在一本账里: 资产 a / 本号 b / 目标权原值 w / 收益 r
struct Slot {
  uint32_t a = 0, b = 0;
  float w = 0.f, r = 0.f;
};

} // namespace factor::stat
