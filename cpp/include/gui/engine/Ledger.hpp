// Ledger — 净值与账核 (Inspect 分层 / Portfolio 因子组合 / 池篮子 / 个股 / 池均线同一份): Load 之上.
//   逐行批量 → 期线 Lines (factor/Stat/Fold.hpp, 经 Realize 直落: 每批资金 1/h̄, 收益在平仓行 Hold::exit 实现 / 成本在建仓行即付; 按 stride 并期).
//   账 Ledger = 账线 BookLines (收益线 ret + 流水 TurnFlow 各字段线 flow; 净 = 毛 − Fee::cost) + 画像 Profile (只篮子账 / 个股账有).
//   收益与换手同一核 (factor/Stat/Book.hpp 账集 book_rows): 因子组合 = 顶端整组 (FactorBooks); 篮子 = 持仓权 Weigh (shared/Basket.hpp); 个股 = 自身.
//   篮子作业 eval_baskets = basket_need → open_job → run_job(BasketPass) (块消费者, 与森林作业的 ForestPass 对仗) → Baskets.
#pragma once

#include "factor/Stat/Fold.hpp"          // Realize
#include "features/Panel/PoolSelect.hpp" // pool::Picks
#include "gui/engine/Load.hpp"           // Block / Domain
#include "gui/quote/Quote.hpp"           // PerMarket

#include <cstddef>
#include <memory>
#include <span>

namespace GUI::Engine {

// 期线矩阵 / 期线写端 / 一组账线 (factor/Stat/Fold.hpp; Beta 选池账同一份)
using factor::stat::BookLines;
using factor::stat::LedgerLines;
using factor::stat::Lines;

// 篮子 / 个股的画像线 (只篮子账与个股账有): 估值七口 (主权重 val / 另一种权重 alt) + 总市值 cap + 两融余额 bal
//   篮子: val = 持仓按权平均的七口 (EP / BP / SP / CP / DY / RZ / RQ); alt = 同成员按另一种权重平均 (篮子加权 → 等权; 等权 → 加权);
//         cap = 持仓成员 Σ总市值, bal[两融口] = 持仓成员 Σ余额 [亿元] (不看权重)
//   个股: val = 七口; cap = 本只总市值; bal = 本只余额 (非两融标的 NaN)
struct Profile {
  LedgerLines val[kPoolVals];
  LedgerLines alt[kPoolVals];
  LedgerLines cap;
  LedgerLines bal[kMarginVals];
  size_t bytes() const {
    size_t n = cap.v.size() * sizeof(float);
    for (const LedgerLines *g : {val, alt})
      for (int m = 0; m < kPoolVals; ++m)
        n += g[m].v.size() * sizeof(float);
    for (const LedgerLines &c : bal)
      n += c.v.size() * sizeof(float);
    return n;
  }
};
// 账核 (因子 / 篮子 kBasket / 个股 kStock 完全同形) = 账线 BookLines (收益 + 流水, 净 = 毛 − Fee::cost) + 画像 (只篮子 / 个股有)
struct Ledger : BookLines {
  Profile pf;
  size_t bytes() const { return pf.bytes() + BookLines::bytes(); }
};
using Baskets = Quote::PerMarket<Ledger>;

// 估值七口的日频输入叶 (bar.1d.<kPoolValCols[m]>, 日层叶表里)
std::array<LeafId, kPoolVals> pool_val_leaves(const FeatureTable &ft);

// 篮子作业: all 池日层的估值七叶 + T+1 标签 (无因子根: 不装标签秩 / 盘口段)
Need basket_need(const FeatureTable &ft);

// 池篮子与个股账的块消费者 (与 ForestPass 对仗: init ⇄ finish × want / block; run_job 驱动) → out
class BasketPass {
public:
  BasketPass(FactorFlow &F, std::shared_ptr<const pool::Picks> pk, std::span<const LeafId, kPoolVals> val);
  static Bytes bytes(); // 趟级常驻: 篮子 + 个股的五列账线与画像 (日层)
  bool want(const Chunk &) const { return true; }
  void init(const Job &j);
  Rc block(const Block &c, size_t ci, bool last, double load_s);
  void finish(const Job &j);
  Baskets out;

private:
  FactorFlow &F_;
  std::shared_ptr<const pool::Picks> pk_;
  LeafId val_[kPoolVals] = {};
  size_t r0_ = 0, warm_ = 0, ds_ = 0, n_ = 0;
  Lines lines_; // [字段 · B + 本] (本 = 篮子 | 个股; 字段 = 收益 + 两侧量 + 卖不出的量, 只付固定冲击)
};

// 篮子作业 (all 池日层, 与森林作业 eval_forest 对仗): open_job(basket_need) → 选股表 → run_job(BasketPass) → out
Rc eval_baskets(FactorFlow &F, const FactorScope &sc, Job &j, Baskets &out);

// 池均线: 块上标签的逐行池基准 (Label::mk = 池内全部有标签的格各自做多的等权批收益), 每持有期一条 → Realize
LedgerLines pool_line(const Block &c);

} // namespace GUI::Engine
