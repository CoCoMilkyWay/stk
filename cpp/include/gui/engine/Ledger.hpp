// Ledger — 净值与账核 (Inspect 分层 / Portfolio 因子组合 / 池篮子 / 个股 / 池均线同一份): Load 之上.
//   逐行批收益 → Realize (协议表 2【净值】【记日】的唯一实现: 每批资金 1/h̄, 收益在平仓行 Hold::exit 实现 / 成本在建仓行即付; 按 stride 并期;
//   行逐块交, 常驻只有期收益) → LedgerLines → compound 净值.
//   账 Ledger = 五列账线 col[kLedgerCols] (一切账都齐: 收益 / 冲击 / 固定冲击量 / 买量 / 卖量) + 画像 Profile (只篮子账 / 个股账有).
//   收益与换手同一核 (factor/Stat/Book.hpp 账集 book_rows): 因子组合 = 顶端整组 (FactorBooks); 篮子 = 持仓权 Weigh (shared/Basket.hpp); 个股 = 自身; 池均 = MaskBook.
//   篮子作业 eval_baskets = basket_need → open_job → run_job(BasketPass) (块消费者, 与森林作业的 ForestPass 对仗) → Baskets.
#pragma once

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

// 净值的实现核: 线 i 评估期行 u 这一批的量 x → 期 (实现行 / stride) 累加 x / h̄; 实现行 = 平仓行 Hold::exit (收益列) | 建仓行 (成本列, 即付);
//   实现行越过评估期末的不计. 行可分块交 (fold), 线间可并行, 同线串行
struct Realize {
  int seg = 1, stride = 1, T = 0; // 层 / 每期行数 / 评估期行数 (整除 stride)
  std::vector<HoldId> hold;       // [线] 持有期
  std::vector<uint8_t> at_exit;   // [线] 1 = 平仓行实现 / 0 = 建仓行即付
  LedgerLines out;                // [线][期]
  void reset(int n, int T_, int seg_, int stride_) {
    assert(n >= 1 && stride_ >= 1 && T_ % stride_ == 0 && T_ % seg_ == 0);
    seg = seg_, stride = stride_, T = T_;
    hold.assign(static_cast<size_t>(n), kHoldT1);
    at_exit.assign(static_cast<size_t>(n), 1);
    out.reset(n, T / stride);
  }
  // 线 i 自评估期行 u0 起的连续 m 行批量 x[0, m)
  void fold(int i, int u0, const float *x, int m) {
    const Hold hd = hold_on(hold[static_cast<size_t>(i)], geo_of(seg));
    const double h = hd.span();
    const bool ex = at_exit[static_cast<size_t>(i)];
    float *dst = out.at(i);
    for (int k = 0; k < m; ++k)
      if (const int u = ex ? hd.exit(u0 + k) : u0 + k; u < T)
        dst[u / stride] += static_cast<float>(x[k] / h);
  }
};

template <class R, class Put>
void compound(const R *r, size_t n, Put &&put) {
  double l = 1.0;
  for (size_t p = 0; p < n; ++p) {
    l *= 1.0 + static_cast<double>(r[p]);
    put(p, l);
  }
}

// 账列: 收益 (平仓行实现) | 真实冲击 (两侧 Σ 量·冲击) / 付固定冲击的量 (两侧) / 买量 / 卖量 (建仓行即付; 费率按 Fee::cost 现乘)
enum LedgerCol : int { kLedgerRet,
                       kLedgerImp,
                       kLedgerFix,
                       kLedgerBuy,
                       kLedgerSell };
inline constexpr int kLedgerCols = 5;
// 一行一本的流水在金额段 a 下 → 账列 (Stat/Book.hpp Flow 两侧合计; 与 Flow::cost 同一式)
inline float ledger_cell(const factor::stat::Flow &u, AmtId a, int k) {
  switch (k) {
  case kLedgerImp:
    return static_cast<float>(u.imp_of(a, kBuy) + u.imp_of(a, kSell));
  case kLedgerFix:
    return static_cast<float>(u.fix_of(a, kBuy) + u.fix_of(a, kSell));
  case kLedgerBuy:
    return static_cast<float>(u.vol[kBuy]);
  default:
    assert(k == kLedgerSell);
    return static_cast<float>(u.vol[kSell]);
  }
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
  LedgerLines col[kLedgerCols];
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
  Realize rz_;             // [列 · B + 本] 期收益 (本 = 篮子 | 个股)
  std::vector<float> blk_; // [列 · B + 本][块统计行] 本块逐行暂存
};

// 篮子作业 (all 池日层, 与森林作业 eval_forest 对仗): open_job(basket_need) → 选股表 → run_job(BasketPass) → out
Rc eval_baskets(FactorFlow &F, const FactorScope &sc, Job &j, Baskets &out);

// 池均线: 池内全部有标签的格各自做多 (Self, 可买才建仓 / 续持取名义值) 等权, 每持有期一条 → Realize
LedgerLines pool_line(misc::exec::ForkJoin &ex, const Block &c);

} // namespace GUI::Engine
