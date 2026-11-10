// 篮子合成的 all.bar 源 (BasketHistory 价格指数 / Market → CTA 的库同一份): 选股表 → 交易日 D 的成员表, L_1day 日行 → 基准 / 日根, L_1min → 分钟根.
//   成员 / 权一律出自选股表与其原料带 (features/Panel/PoolSelect.hpp, 与 Style 页篮子账同一份): 全A / 行业 / 概念 = 前一行 (D−1) 的全A 成员,
//   子池 = 选股表 D 的成员; 权 = Tape::omega (源行的目标权随价漂移; 日频 = D−1 收盘的目标权). 基准 = D−1 行 pre_close; 合成核 = Quote.hpp Fold (Lane 一日一份).
#pragma once

#include "features/Panel/PoolSelect.hpp" // pool::Picks / day_cols
#include "features/Storage/FeatureRead.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/quote/Quote.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace GUI::Quote {

// 一日的 L_1day 日根行 (A 轴, 日行前 kQuoteCols 列); 缺 / 非有限 → 0
struct Row {
  std::vector<float> px[4], amount, pre; // 开 高 低 收 / 成交额 [元] / 前收 (该行 = 次日除权后前收)
  std::vector<uint8_t> trade;            // ts_valid
};

// 日根行 / 分钟的读: 预分配 (A 轴) / 读交易日 d
void row_alloc(FeatureRead::DayColumns &dc, size_t A);
void read_row(const FeatureRead &rd, misc::Ymd d, FeatureRead::DayColumns &dc, Row &r);
void minute_alloc(FeatureRead::DayColumns &mc, size_t A);
void read_minutes(const FeatureRead &rd, misc::Ymd d, FeatureRead::DayColumns &mc);

// 交易日 d 的成员表 (下标 = 库轴, 篮子 = basket_line): 所属切片各进等权 / 加权线, 权 = Tape::omega (≤ 0 不进); 概念随全A (pk 与 axis 同为 all.bar 轴)
void members(const pool::Picks &pk, const PoolAxis &axis, misc::Ymd d, Members &m);
// 成员表 m 过交易日 prev → d (成员冻结, 库末日之后的补日): 各线按其切片的周期换源 (pool::rebal_at) → 等权回 1, 否则随价 × rel [库轴] (收 / 前收);
//   加权恒 × rel (股本冻结 ⇒ 目标权即漂移后的权)
void drift(Members &m, misc::Ymd prev, misc::Ymd d, std::span<const float> rel);
// 一只一日的 L_1min (mc = read_minutes 的读) → 分钟根: 无成交的行不收 (与 bar1m 的 bars_of 同一并根)
void bars_of(const FeatureRead::DayColumns &mc, size_t a, misc::Ymd d, Bars &b);

// 一日合成件 (BasketHistory 历史 / CtaLib 价格库同一份; 逐日复用): 成员表 mem × 前一行 prev (基准 = 前收) × 当日行 cur (日根) × 分钟读 mc → Fold → day [合成线]
//   slot = basket_line → 合成线 (空 = 原线; ≥ 线数 = 不合)
struct Lane {
  Lane(size_t A, size_t lines, std::vector<uint32_t> slot = {});
  FeatureRead::DayColumns dc, mc; // 日根行 / 分钟的读
  Members mem;                    // 交易日 d 的成员表
  Fold fold;
  std::vector<Day> day; // [合成线]
  Bars bars;            // 一只一日的分钟根
  // 合成交易日 d (前一交易日 prev_d): minutes → mc 已读 d; level [合成线] = 前日点位;
  //   each(a, 日根 (无成交 = nullptr), 分钟根 (只在 minutes 时有)) 逐只
  template <class Each>
  void run(const Row &prev, const Row &cur, misc::Ymd d, misc::Ymd prev_d, bool minutes, std::span<const double> level, Each &&each);

private:
  std::vector<uint32_t> slot_;
  std::vector<Members::Weight> ws_; // 一只滤到合成线的所属线
};

template <class Each>
void Lane::run(const Row &prev, const Row &cur, misc::Ymd d, misc::Ymd prev_d, bool minutes, std::span<const double> level, Each &&each) {
  fold.open(minutes ? kSlots : 0);
  bars.has.reset();
  for (size_t a = 0; a < mem.assets(); ++a) {
    const float x = cur.px[3][a];
    const Cell c = cell(cur.px[0][a], cur.px[1][a], cur.px[2][a], x, 0, cur.amount[a]);
    const Cell *on = cur.trade[a] && x > 0 ? &c : nullptr;
    if (minutes)
      bars_of(mc, a, d, bars);
    each(a, on, bars);
    std::span<const Members::Weight> ws = mem.of(a);
    if (!slot_.empty()) {
      ws_.clear();
      for (const Members::Weight &w : ws)
        if (const uint32_t j = slot_[w.basket]; j < day.size())
          ws_.push_back({j, w.w});
      ws = ws_;
    }
    if (!ws.empty())
      fold.add({prev.pre[a], on, minutes ? &bars : nullptr}, ws);
  }
  fold.finish(d, prev_d, level, day);
}

} // namespace GUI::Quote
