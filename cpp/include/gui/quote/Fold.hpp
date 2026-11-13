// 篮子合成核 Fold (历史逐日 / 当日增量同一份): 成员表 (define/Members.hpp) 带日内路径的合 (收盘一格即 Members::scatter 的 收 / 基准)
//   比 = Σ w · 价 / 基准 / Σ w (前日 = 1); 开 / 高 / 低 / 收各自如此
//   (高 / 低 = 成员高低的加权, 是真实路径的外包); 额 = 成员之和.
//   点位 = 比接到前日点位上 (rebase); 日间接续只此一步 ⇒ 各日的合成互不相干.
//   净流入 (只合成分钟的日) = 成员各自分钟涨跌法之和 (Bars.hpp; 不加权); 逐根记 (Bars::net), 日摘要随日根 (Candle::flow).
//   基准 = 成员当日前收 (历史 = D−1 行 pre_close, 当日 = 新浪昨收; 同一个交易所前收), 无基准的成员不计;
//   当日无成交的成员比 1; 分钟缺根沿用其上根收盘, 首根前 = 基准; 计入的成员有一只在某根是缺口 (Bars::hole) → 篮子该根也是缺口.
//   逐资产一遍 (资产主序): 一只的分钟根只读一次, 比 × 权散进所属各篮子.
//   整表并行: 篮子按权数切段, 各线程扫全部成员、只累加本段的篮子 ⇒ 各篮子的累加序同逐只 (篮子段的累加格常驻本核缓存).
// 当日增量 (逐成员冻结线): 各量对资产线性可加 ⇒ 每只各有冻结线 Carry::at, 其前的槽已并入冻结累加 (跨轮保留);
//   每轮各只只合日根与 [at, slots) 的槽: [at, freeze) 并入冻结累加 (本只冻结线推进到 freeze), [freeze, slots) 与日根进活跃累加 (每轮清零).
//   冻结线由调用方逐只给 (该只之后不会再改写的槽), 不随别只的待补而退 ⇒ 每轮的活只是各只末几槽.
#pragma once

#include "define/Members.hpp"
#include "gui/quote/Bars.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <span>
#include <vector>

namespace GUI::Quote {

// 合成的一日 (前日 = 1, candle.date ≠ 0) 接到前日点位 level 上: 各根价 × level, ref = level
void rebase(Day &y, double level);

// 一只一日 (合成的输入), 各源各自填
struct Leg {
  float base = 0;             // 基准 (当日前收); 0 = 不计
  const Cell *day = nullptr;  // 日根; nullptr = 当日无成交
  const Bars *bars = nullptr; // 分钟根; 合成分钟的日必给
};

class Fold {
public:
  // 一只的冻结段: 槽 [0, at) 已并入冻结累加; 其后接着合所需的接续
  struct Carry {
    float base = 0;       // 冻结段所用的基准 (0 = 该只不计)
    int at = 0;           // 冻结线
    int dir = 0;          // 冻结段末个有成交根的涨跌方向
    double c = 1, px = 0; // 冻结段末根的比 (之前无根 = 1) / 末根收盘 (之前无根 = 基准)
  };

  explicit Fold(size_t baskets);

  // 新一轮: slots = 合成前几槽的分钟 (0 = 只合日根); 活跃累加清零, 冻结累加保留
  void open(int slots);
  // 冻结累加清零 (各只的 Carry 须一并回 {})
  void reset();
  // 一只: ws = 成员表一行; 全进活跃累加 (历史一日)
  void add(const Leg &g, std::span<const Members::Weight> ws) {
    Carry k;
    [[maybe_unused]] const bool ok = add(g, ws, 0, k);
    assert(ok);
  }
  // 一只: freeze = 本只本轮的冻结线 (k.at ≤ freeze ≤ slots); k = 本只的冻结段 (入出).
  //   false = 冻结段所用的基准与本轮的不符 (本只未计; 调用方须 reset 后重合)
  bool add(const Leg &g, std::span<const Members::Weight> ws, int freeze, Carry &k);
  // 整张成员表 m 一趟 (同逐行 add; 整表并行见文件头): leg(k) / freeze(k) = 第 k 行的 Leg / 冻结线,
  //   was / now [m 行] = 各行冻结段 入 / 出 (now 由该行首键所在的段写). false = 同 add
  template <class LegOf, class FreezeOf>
  bool add(misc::exec::ForkJoin &ex, const Members &m, LegOf &&leg, FreezeOf &&freeze, std::span<const Carry> was, std::span<Carry> now);
  // 交易日 d 的各篮子: 无成员 / 无成交的 out[b].candle.date = 0; bars.net = 各根净流入 (只合日根 = 0)
  void finish(misc::Ymd d, std::span<Day> out);

private:
  // 一格 = 开 高 低 收 (比 × 权) / 额 / 净流入 / 缺口成员数; 格 0 = 日根, 格 1 + s = 槽 s
  static constexpr size_t kQ = 7, kCells = 1 + kSlots;
  using Row = std::array<double, kCells * kQ>; // 一只的各格 (比未乘权)
  // 一只 (基准 base, 0 = 不计) 的格 r: 日根 + 槽 [k.at, slots); k 推进到 freeze; end / fend 取有根末槽 (全部 / 冻结累加) 之极.
  //   返回 −1 = 冻结段所用的基准不符, 0 = 不计 (r 未填), 1 = r 已填
  int row(const Leg &g, float base, int freeze, Carry &k, Row &r, int &end, int &fend) const;
  // r 按 ws 各键的权累加: 槽 [from, freeze) 进冻结, 日根与 [freeze, slots) 进活跃
  void put(const Row &r, int from, int freeze, std::span<const Members::Weight> ws);
  int slots_ = 0;
  int end_ = -1, fend_ = -1;  // 有根的末槽: 全部 / 冻结累加
  std::vector<double> sw_;    // [篮子] Σ w (本轮)
  std::vector<double> froze_; // [篮子][kCells][kQ] 冻结累加
  std::vector<double> live_;  // [篮子][kCells][kQ] 活跃累加
};

template <class LegOf, class FreezeOf>
bool Fold::add(misc::exec::ForkJoin &ex, const Members &m, LegOf &&leg, FreezeOf &&freeze, std::span<const Carry> was, std::span<Carry> now) {
  assert(was.size() == m.size() && now.size() == m.size());
  const size_t T = static_cast<size_t>(ex.threads()), B = sw_.size();
  std::vector<size_t> cum(B + 1, 0); // [键] 键更小的权数
  for (const Members::Weight &w : m.ws)
    ++cum[w.key + 1];
  for (size_t b = 0; b < B; ++b)
    cum[b + 1] += cum[b];
  std::vector<uint32_t> cut(T + 1, static_cast<uint32_t>(B)); // 段 t = 键 [cut[t], cut[t + 1])
  cut[0] = 0;
  for (size_t t = 1; t < T; ++t)
    cut[t] = static_cast<uint32_t>(std::lower_bound(cum.begin(), cum.end(), cum[B] * t / T) - cum.begin());
  std::vector<int> end(T, end_), fend(T, fend_);
  std::atomic<bool> ok{true};
  ex.tasks(static_cast<int>(T), [&](int ti, int) {
    const size_t t = static_cast<size_t>(ti);
    Row r;
    for (size_t k = 0; k < m.size() && ok.load(std::memory_order_relaxed); ++k) {
      const std::span<const Members::Weight> ws = m.of(k);
      const auto lo = std::ranges::lower_bound(ws, cut[t], {}, &Members::Weight::key);
      const auto hi = std::ranges::lower_bound(lo, ws.end(), cut[t + 1], {}, &Members::Weight::key);
      if (lo == hi)
        continue;
      const Leg g = leg(k);
      const int fz = freeze(k);
      Carry c = was[k];
      const int x = row(g, g.base > 0 ? g.base : 0.f, fz, c, r, end[t], fend[t]);
      if (x < 0) {
        ok.store(false, std::memory_order_relaxed);
        return;
      }
      if (x > 0)
        put(r, was[k].at, fz, {lo, hi});
      if (lo == ws.begin())
        now[k] = c;
    }
  });
  end_ = *std::ranges::max_element(end), fend_ = *std::ranges::max_element(fend);
  return ok.load(std::memory_order_relaxed);
}

} // namespace GUI::Quote
