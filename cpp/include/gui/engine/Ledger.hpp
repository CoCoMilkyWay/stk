// Ledger — 净值与账核 (Inspect 分层 / Portfolio 因子组合 / 池篮子 / 个股 / 池均线同一份): Load 之上.
//   逐行批量 → Lines (经 factor::stat::Realize 直落期线: 每批资金 1/h̄, 收益在平仓行 Hold::exit 实现 / 成本在建仓行即付; 按 stride 并期).
//   账 Ledger = 收益线 ret + 流水 TurnFlow 各字段线 flow (成本按 Fee::parts 现乘) + 画像 Profile (只篮子账 / 个股账有).
//   收益与换手同一核 (factor/Stat/Book.hpp 账集 book_rows): 因子组合 = 顶端整组 (FactorBooks); 篮子 = 持仓权 Weigh (shared/Basket.hpp); 个股 = 自身.
//   篮子作业 eval_baskets = basket_need → open_job → run_job(BasketPass) (块消费者, 与森林作业的 ForestPass 对仗) → Baskets.
#pragma once

#include "factor/Stat/Fold.hpp"          // Realize
#include "features/Panel/PoolSelect.hpp" // pool::Picks
#include "gui/engine/Load.hpp"           // Block / Domain
#include "gui/quote/Quote.hpp"           // PerMarket
#include "misc/exec.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace GUI::Engine {

// 期收益 / 序列矩阵 [n][P], 线 i 点 p
struct LedgerLines {
  int n = 0, P = 0;
  std::vector<float> v;
  void reset(int n_, int P_, float fill = 0.f) {
    n = n_, P = P_;
    v.assign(static_cast<size_t>(n) * static_cast<size_t>(P), fill);
  }
  float *at(int i) { return v.data() + static_cast<size_t>(i) * static_cast<size_t>(P); }
  const float *at(int i) const { return v.data() + static_cast<size_t>(i) * static_cast<size_t>(P); }
  LedgerLines slice(int i0, int m) const {
    assert(i0 >= 0 && m >= 0 && i0 + m <= n);
    LedgerLines o;
    o.n = m, o.P = P;
    o.v.assign(at(i0), at(i0) + static_cast<size_t>(m) * static_cast<size_t>(P));
    return o;
  }
};

// 期线 (净值探针 / 因子账集流水 / 篮子账同一份): 线 i 的记日 = 持有期 rz[r] (factor/Stat/Fold.hpp Realize, 协议表 2【净值】【记日】唯一实现) ×
//   记在平仓行 (收益) ⇄ 建仓行 (流水 / 成本, 即付); add(i, u, x) 把评估期行 u 这一批的量直落期线 (一期只收一日开的批: 按日切线程无竞争)
struct Lines {
  struct Line {
    uint8_t r = 0;     // rz 下标
    bool exit = false; // 记在平仓行
  };
  std::vector<factor::stat::Realize> rz; // [持有期]
  std::vector<Line> ln;                  // [线]
  LedgerLines out;                       // [线][期]
  // n 条线, 线 i 的持有期 hold_of(i) / 记在平仓行 exit_of(i); seg = 层每日行数, stride = 每几行并一期, T = 评估期行数
  template <class HoldOf, class ExitOf>
  void reset(int n, int seg, int stride, int T, HoldOf &&hold_of, ExitOf &&exit_of) {
    assert(n >= 1);
    rz.clear(), ln.assign(static_cast<size_t>(n), {});
    for (int i = 0; i < n; ++i) {
      const HoldId h = hold_of(i);
      size_t r = 0;
      while (r < rz.size() && rz[r].hd.id != h)
        ++r;
      if (r == rz.size())
        rz.emplace_back(h, seg, stride, T);
      ln[static_cast<size_t>(i)] = {static_cast<uint8_t>(r), exit_of(i)};
    }
    out.reset(n, rz[0].periods());
  }
  void add(int i, int u, double x) {
    const Line &l = ln[static_cast<size_t>(i)];
    rz[l.r].add(u, x, l.exit, out.at(i));
  }
};

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
// 账核 (因子 / 篮子 kBasket / 个股 kStock 完全同形): 收益线 ret (平仓行实现) | 流水 TurnFlow 各字段线 flow (define/Fee.hpp 字段表, 建仓行即付;
//   没装的字段空 n = 0); 成本按 Fee::parts 现乘. 画像只篮子 / 个股有
struct Ledger {
  LedgerLines ret;
  LedgerLines flow[kFlowFields];
  Profile pf;
  size_t bytes() const {
    size_t n = pf.bytes() + ret.v.size() * sizeof(float);
    for (const LedgerLines &c : flow)
      n += c.v.size() * sizeof(float);
    return n;
  }
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
