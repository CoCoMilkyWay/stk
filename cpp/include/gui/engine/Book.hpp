// Book — 净值与篮子引擎 (Stat 契约【净值】; Inspect 分层 / Portfolio 因子线 / 池篮子 / 池整体线同一份): Load 之上.
//   逐行批收益 RowLines → fold_book (每批资金 1/h̄, 收益在平仓行实现 / 成本在建仓行即付; 按 stride 求和) → 期收益 BookLines → compound 净值.
//   统一账核 Book (col[kBookCols] + val[kPoolVals] + cap + bal[kMarginVals]): 因子账 / 篮子账 / 个股账完全同形; BasketPass 沿 Store::pin 的分块流式归约出 Baskets.
//   池篮子的成员 / 持仓权 = shared/Basket.hpp members / Weigh (按持仓日对齐, 与行情篮子 / Style 同一份), 换手 / 估值的持仓 = 篮子账 Sleeve (define/Members.hpp)
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
struct BookLines {
  int n = 0, P = 0;
  std::vector<float> v;
  void reset(int n_, int P_, float fill = 0.f) {
    n = n_, P = P_;
    v.assign(static_cast<size_t>(n) * static_cast<size_t>(P), fill);
  }
  float *at(int i) { return v.data() + static_cast<size_t>(i) * static_cast<size_t>(P); }
  const float *at(int i) const { return v.data() + static_cast<size_t>(i) * static_cast<size_t>(P); }
  BookLines slice(int i0, int m) const {
    assert(i0 >= 0 && m >= 0 && i0 + m <= n);
    BookLines o;
    o.n = m, o.P = P;
    o.v.assign(at(i0), at(i0) + static_cast<size_t>(m) * static_cast<size_t>(P));
    return o;
  }
};

// 逐行批收益 (fold_book 入): x [n][T] = 线 i 行 t 这一批的收益; hold[i] = 持有期键; at_exit[i]: 1 = 平仓行实现 / 0 = 建仓行即付
struct RowLines {
  int n = 0, T = 0;
  std::vector<int> hold;
  std::vector<uint8_t> at_exit;
  std::vector<float> x;
  void reset(int n_, int T_) {
    n = n_, T = T_;
    hold.assign(static_cast<size_t>(n), 0);
    at_exit.assign(static_cast<size_t>(n), 1);
    x.assign(static_cast<size_t>(n) * static_cast<size_t>(T), 0.f);
  }
  float *at(int i) { return x.data() + static_cast<size_t>(i) * static_cast<size_t>(T); }
};

BookLines fold_book(misc::exec::ForkJoin &ex, int seg, const RowLines &r, int stride);

template <class R, class Put>
void compound(const R *r, size_t n, Put &&put) {
  double l = 1.0;
  for (size_t p = 0; p < n; ++p) {
    l *= 1.0 + static_cast<double>(r[p]);
    put(p, l);
  }
}

// 统一账列: 收益 / 买入冲击 / 固定冲击份数 / 增仓量
enum BookCol : int { kBookRet,
                     kBookImp,
                     kBookFix,
                     kBookBuy };
inline constexpr int kBookCols = 4;

// 账核 (因子 / 篮子 kBasket / 个股 kStock 完全同形; 无该列的 BookLines n = 0):
//   因子: col[Ret / Imp / Fix / Buy] 齐, val / cap / bal 空
//   篮子: col[Ret] = 可成交指数 fold_book; col[Buy] = Sleeve::roll 增仓量; val = 持仓按权平均的七口 (EP / BP / SP / CP / DY / RZ / RQ);
//         cap = 持仓成员 Σ总市值, bal[两融口] = 持仓成员 Σ余额 [亿元] (不看权重)
//   个股: col[Ret] = 原料带 grow 比 − 1 (后复权); val = 七口; cap = 本只总市值; bal = 本只余额 (非两融标的 NaN)
struct Book {
  BookLines col[kBookCols];
  BookLines val[kPoolVals];
  BookLines cap;
  BookLines bal[kMarginVals];
  size_t bytes() const {
    size_t n = cap.v.size() * sizeof(float);
    for (const BookLines &c : col)
      n += c.v.size() * sizeof(float);
    for (const BookLines &c : val)
      n += c.v.size() * sizeof(float);
    for (const BookLines &c : bal)
      n += c.v.size() * sizeof(float);
    return n;
  }
};
using Baskets = Quote::PerMarket<Book>;

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
  RowLines rx_;
  Baskets out_;
};

BookLines pool_line(misc::exec::ForkJoin &ex, const Block &c);

} // namespace GUI::Engine
