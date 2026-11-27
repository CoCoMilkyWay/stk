// Ledger — 净值与账核 (Inspect 分层 / Portfolio 因子组合 / 池篮子 / 个股 / 池均线同一份): Load 之上.
//   逐行批量 → RowLines (块内暂存逐块经 factor::stat::Realize 折进期线: 每批资金 1/h̄, 收益在平仓行 Hold::exit 实现 / 成本在建仓行即付; 按 stride 并期).
//   账 Ledger = 账线 col[kLedgerCols] (收益 | 流水 TurnFlow 各字段; 成本按 Fee::parts 现乘) + 画像 Profile (只篮子账 / 个股账有).
//   收益与换手同一核 (factor/Stat/Book.hpp 账集 book_rows): 因子组合 = 顶端整组 (FactorBooks); 篮子 = 持仓权 Weigh (shared/Basket.hpp); 个股 = 自身; 池均 = MaskBook.
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

// 块内逐行暂存 → 期线 (净值探针 LinesP / 篮子账 / 池均线同一份): 线 i 的记日 rz[i] (factor/Stat/Fold.hpp Realize, 协议表 2【净值】【记日】唯一实现);
//   open(块统计行, 评估期行) → at(线, 行) 逐行写 → close 本块逐线折进 out (线间并行, 同线串行)
struct RowLines {
  std::vector<factor::stat::Realize> rz; // [线]
  LedgerLines out;                       // [线][期]
  int t_lo = 0, nb = 0, u0 = 0;
  std::vector<float> blk; // [线][本块统计行]
  void reset(std::vector<factor::stat::Realize> r) {
    assert(!r.empty());
    rz = std::move(r);
    for (const factor::stat::Realize &x : rz)
      assert(x.periods() == rz[0].periods() && "各线同期数");
    out.reset(static_cast<int>(rz.size()), rz[0].periods());
  }
  // 块统计行 [t_lo, t_hi) ↔ 评估期行 [u0, u0 + t_hi − t_lo)
  void open(int t_lo_, int t_hi, int u0_) {
    t_lo = t_lo_, nb = t_hi - t_lo_, u0 = u0_;
    blk.assign(rz.size() * static_cast<size_t>(nb), 0.f);
  }
  float &at(int i, int t) { return blk[static_cast<size_t>(i) * static_cast<size_t>(nb) + static_cast<size_t>(t - t_lo)]; }
  void close(misc::exec::ForkJoin &ex) {
    ex.rows(out.n, [&](int i0, int i1, int) {
      for (int i = i0; i < i1; ++i)
        rz[static_cast<size_t>(i)].fold(u0, blk.data() + static_cast<size_t>(i) * static_cast<size_t>(nb), nb, out.at(i));
    });
  }
  void drop() { std::vector<float>().swap(blk); }
};

// 账列: 收益 (平仓行实现) | 流水 TurnFlow 的字段 (建仓行即付): 两侧量 / 卖不出的量 / 各盘口段两侧冲击 (没装的段空); 成本按 Fee::parts 现乘
enum LedgerCol : int { kLedgerRet,
                       kLedgerBuy,
                       kLedgerSell,
                       kLedgerStuck,
                       kLedgerImp }; // kLedgerImp + 2 (a − 1) + 侧
inline constexpr int kLedgerCols = kLedgerImp + 2 * kLobAmts;
inline constexpr int ledger_imp(AmtId a, Side s) { return kLedgerImp + 2 * (a - 1) + s; }
// 一行一本的流水 → 账列 k (k ≥ kLedgerBuy)
inline float ledger_cell(const TurnFlow &u, int k) {
  if (k >= kLedgerImp)
    return static_cast<float>(u.imp[(k - kLedgerImp) / 2][(k - kLedgerImp) % 2]);
  return static_cast<float>(k == kLedgerBuy ? u.vol[kBuy] : k == kLedgerSell ? u.vol[kSell]
                                                                             : u.stuck);
}
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
// 账核 (因子 / 篮子 kBasket / 个股 kStock 完全同形): 五列恒齐; 画像只篮子 / 个股有
struct Ledger {
  LedgerLines col[kLedgerCols]; // 没装的冲击列空 (n = 0)
  Profile pf;
  size_t bytes() const {
    size_t n = pf.bytes();
    for (const LedgerLines &c : col)
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
  RowLines lines_; // [列 · B + 本] (本 = 篮子 | 个股; 列 = 收益 + 两侧量 + 卖不出的量, 只付固定冲击)
};

// 篮子作业 (all 池日层, 与森林作业 eval_forest 对仗): open_job(basket_need) → 选股表 → run_job(BasketPass) → out
Rc eval_baskets(FactorFlow &F, const FactorScope &sc, Job &j, Baskets &out);

// 池均线: 池内全部有标签的格各自做多 (Self, 可买才建仓 / 续持取名义值) 等权, 每持有期一条 → Realize
LedgerLines pool_line(misc::exec::ForkJoin &ex, const Block &c);

} // namespace GUI::Engine
