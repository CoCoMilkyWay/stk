// Ledger — 净值与账核 (Inspect 分层 / Portfolio 因子组合 / 池篮子 / 个股 / 池均线同一份): Load 之上.
//   逐行批收益 RowLines → fold_ledger (每批资金 1/h̄, 收益在平仓行 Hold::exit 实现 / 成本在建仓行即付; 按 stride 求和) → 期收益 LedgerLines → compound 净值.
//   账 Ledger = 五列账线 col[kLedgerCols] (一切账都齐: 收益 / 冲击 / 固定冲击量 / 买量 / 卖量) + 画像 Profile (只篮子账 / 个股账有).
//   收益与换手同一核 (factor/Stat/Pos.hpp walk + Stat/Host.hpp turn_row): 因子组合 = 顶端整组; 篮子 = 持仓权 Weigh (shared/Basket.hpp); 个股 / 池均 = Self.
//   BasketPass 沿 Store::pin 的分块流式归约出 Baskets.
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

// 逐行批收益 (fold_ledger 入): x [n][T] = 线 i 行 t 这一批的量; hold[i] = 持有期; at_exit[i]: 1 = 平仓行实现 / 0 = 建仓行即付
struct RowLines {
  int n = 0, T = 0;
  std::vector<HoldId> hold;
  std::vector<uint8_t> at_exit;
  std::vector<float> x;
  void reset(int n_, int T_) {
    n = n_, T = T_;
    hold.assign(static_cast<size_t>(n), kHoldT1);
    at_exit.assign(static_cast<size_t>(n), 1);
    x.assign(static_cast<size_t>(n) * static_cast<size_t>(T), 0.f);
  }
  float *at(int i) { return x.data() + static_cast<size_t>(i) * static_cast<size_t>(T); }
};

LedgerLines fold_ledger(misc::exec::ForkJoin &ex, int seg, const RowLines &r, int stride);

template <class R, class Put>
void compound(const R *r, size_t n, Put &&put) {
  double l = 1.0;
  for (size_t p = 0; p < n; ++p) {
    l *= 1.0 + static_cast<double>(r[p]);
    put(p, l);
  }
}

// 账列: 收益 (平仓行实现) | 真实冲击 (两侧 Σ 量·冲击) / 固定冲击量 (两侧) / 买量 / 卖量 (建仓行即付; 税佣按两侧量现乘, Fee::cost)
enum LedgerCol : int { kLedgerRet,
                       kLedgerImp,
                       kLedgerFix,
                       kLedgerBuy,
                       kLedgerSell };
inline constexpr int kLedgerCols = 5;
// 一行的流水 → 账列 (Stat/Turn.hpp Turn 两侧合计)
inline float ledger_cell(const factor::stat::Turn &u, int k) {
  switch (k) {
  case kLedgerImp:
    return static_cast<float>(u.imp[kBuy] + u.imp[kSell]);
  case kLedgerFix:
    return static_cast<float>(u.fix[kBuy] + u.fix[kSell]);
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

// 池篮子与个股账的分块归约器 (与 Probe 的 init → chunk → finish 对仗; 消费 Store::pin 的 Block)
class BasketPass {
public:
  BasketPass(const Domain &dm, std::shared_ptr<const pool::Picks> pk, std::span<const LeafId, kPoolVals> val);
  static double fixed_bytes(const Domain &dm);
  void chunk(misc::exec::ForkJoin &ex, const Block &c);
  Baskets finish(misc::exec::ForkJoin &ex);

private:
  std::shared_ptr<const pool::Picks> pk_;
  LeafId val_[kPoolVals] = {};
  size_t r0_ = 0, warm_ = 0, ds_ = 0;
  RowLines rx_; // [列][篮子 | 个股] 逐行
  Baskets out_;
};

// 池均线: 池内全部有标签的格各自做多 (Self, 可买才建仓 / 续持取名义值) 等权, 每持有期一条 → fold_ledger
LedgerLines pool_line(misc::exec::ForkJoin &ex, const Block &c);

} // namespace GUI::Engine
