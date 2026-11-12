// 篮子合成件 (BasketHistory 价格指数 / Market → CTA 的库同一份): 成员 × 源 → Lane (Fold 一日一份) → 各篮子一日; 普查 census 出发布的成员统计.
//   成员 (谁 × 权) 两步对仗: members = 选股表 → 交易日 D 的成员表 (判据 = shared/Basket.hpp BasketTable::each, 权 = Tape::omega; 与 Style 页篮子账同一份)
//                             drift   = 成员冻结, 过一个交易日 (库末日之后的补日: 换源回目标权, 否则随价漂移)
//   源 (每只一日的 Leg) 两种对仗: LibDay  = all.bar L_1day 日行 (基准 = D−1 行 pre_close) + L_1min 分钟根
//                                 FundDay = 基本面 bar1d 行 (基准 = 当行 pre_close) + bar1m 分钟根
//   两源同一个形 (date / minutes / leg), Lane::run 不分源; 当日 (新浪快照) 的增量合成在 QuoteService (Fold 增量).
#pragma once

#include "features/Panel/PoolSelect.hpp" // pool::Picks / day_cols
#include "features/Storage/FeatureRead.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/quote/Quote.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

struct Bar1mDay;

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

// 交易日 d 的成员表 (下标 = 库轴): 当日所在的池 (子池 = 选股表 d 的成员) 过 BasketTable::each, 权 = Tape::omega 取该篮的权重 (≤ 0 不进);
//   行业 / 概念随母集 (pk 与 axis 同为 all.bar 轴); Members::cap = D−1 收盘总市值
void members(const pool::Picks &pk, const PoolAxis &axis, misc::Ymd d, Members &m);
// 成员表 m 过交易日 prev → d (成员冻结): 各篮子按其周期换源 (pool::rebal_at) → 等权回 1, 否则随价 × rel [库轴] (收 / 前收);
//   加权恒 × rel (股本冻结 ⇒ 目标权即漂移后的权); Members::cap 恒 × rel
void drift(Members &m, misc::Ymd prev, misc::Ymd d, std::span<const float> rel);
// 一只一日的 L_1min (mc = read_minutes 的读) → 分钟根: 无成交的行不收 (与 bar1m 的 bars_of 同一并根)
void bars_of(const FeatureRead::DayColumns &mc, size_t a, misc::Ymd d, Bars &b);

// 源 (两种同形): date = 交易日; minutes() = 合成分钟; leg(a, c, b) = 一只的 Leg (c / b = 调用方给的格, Leg 可指向它们)
// all.bar 库源: 基准 = 前一行 prev 的 pre_close, 日根 = 当日行 cur, 分钟根 = mc 的 L_1min (nullptr = 只合日根)
struct LibDay {
  misc::Ymd date;
  const Row &prev, &cur;
  const FeatureRead::DayColumns *mc;
  bool minutes() const { return mc != nullptr; }
  Leg leg(size_t a, Cell &c, Bars &b) const;
};
// 基本面源: 基准 / 日根 = bar1d 行 px [库轴] (ref = pre_close; 无行 → 收 0), 分钟根 = bar1m (bd 已载 date)
struct FundDay {
  misc::Ymd date;
  std::span<const Candle> px;
  const Bar1mDay &bd;
  bool minutes() const { return true; }
  Leg leg(size_t a, Cell &c, Bars &b) const;
};

// 一日合成件 (逐日复用): 成员表 mem × 源 → Fold → day [篮子]
struct Lane {
  explicit Lane(size_t baskets) : fold(baskets), day(baskets) {}
  Members mem; // 交易日 src.date 的成员表
  Fold fold;
  std::vector<Day> day; // [篮子]
  // 合成源 src 的一日 (前一交易日 prev; level [篮子] = 前日点位); each(a, Leg) 逐只 (含非成员)
  template <class Src, class Each>
  void run(const Src &src, misc::Ymd prev, std::span<const double> level, Each &&each);
  template <class Src>
  void run(const Src &src, misc::Ymd prev, std::span<const double> level) {
    run(src, prev, level, [](size_t, const Leg &) {});
  }

private:
  Cell cell_;
  Bars bars_;
};

template <class Src, class Each>
void Lane::run(const Src &src, misc::Ymd prev, std::span<const double> level, Each &&each) {
  fold.open(src.minutes() ? kSlots : 0);
  for (size_t a = 0; a < mem.assets(); ++a) {
    const Leg g = src.leg(a, cell_, bars_);
    each(a, g);
    if (const std::span<const Members::Weight> ws = mem.of(a); !ws.empty())
      fold.add(g, ws);
  }
  fold.finish(src.date, prev, level, day);
}

// 普查: 成员表 m (库轴 axis) 在键集 keys (篮子) 上 → t 的 members / weight / cap / cons / overlap (见 Tape)
void census(const Members &m, const PoolAxis &axis, std::span<const uint32_t> keys, Tape &t);

} // namespace GUI::Quote
