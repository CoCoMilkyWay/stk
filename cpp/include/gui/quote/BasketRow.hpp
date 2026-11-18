// 价格库的合成件 (PriceLib): 个股源 → 个股的根; 成员 (shared/Basket.hpp members ⇄ drift) × 个股的根 → Lane (Fold 一日一份) → 各篮子一日; 普查 census 出发布的成员统计.
//   个股源 (每只一日的 Leg) 三种对仗: LibDay  = all.bar L_1day 日行 (基准 = D−1 行 pre_close) + L_1min 分钟根
//                                     FundDay = 基本面 bar1d 行 (基准 = 当行 pre_close) + bar1m 分钟根
//                                     SinaDay = 新浪归档的末快照 (基准 = 昨收) + 分钟根
//   篮子的源 = 价格库个股侧的一日 (落盘的段 / 库段同趟时内存里的件; 同一个形: date / size / id / leg; leg 另带槽段 [lo, hi),
//     只解这段的分钟根) ⇒ 篮子不碰个股源, 个股侧一份根两侧共用;
//   当日 (新浪快照) 的增量合成在 QuoteService (Fold 增量).
//   资产一律全局 id (成员表 / 源 / 漂移比同一下标): 库源逐库轴行给其全局 id, 补段源稠密在资产轴上.
#pragma once

#include "features/Panel/PoolSelect.hpp" // pool::Picks / day_cols
#include "features/Storage/FeatureRead.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/quote/Fold.hpp"
#include "gui/quote/Quote.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <vector>

struct Bar1m;
struct Bar1mDay;
namespace fundamental {
class Ids;
class Scratch;
struct Shard;
} // namespace fundamental

namespace GUI::Quote {

// 一日的 L_1day 日根行 (A 轴, 日行前 kQuoteCols 列); 缺 / 非有限 → 0
struct Row {
  std::vector<float> px[4], amount, pre; // 开 高 低 收 / 成交额 [元] / 前收 (该行 = 次日除权后前收)
  std::vector<uint8_t> trade;            // ts_valid
};
// 库轴行 a 当日有日根 (LibDay 收它; 库段整块的行表同此预定)
inline bool traded_row(const Row &r, size_t a) { return r.trade[a] && r.px[3][a] > 0; }

// 日根行 / 分钟的读: 预分配 (A 轴) / 读交易日 d
void row_alloc(FeatureRead::DayColumns &dc, size_t A);
void read_row(const FeatureRead &rd, misc::Ymd d, FeatureRead::DayColumns &dc, Row &r);
void minute_alloc(FeatureRead::DayColumns &mc, size_t A);
void read_minutes(const FeatureRead &rd, misc::Ymd d, FeatureRead::DayColumns &mc);
// 一只一日的 L_1min (mc = read_minutes 的读; a = 库轴行) → 分钟根: 无成交的行不收 (与 bar1m 的 bars_of 同一并根)
void bars_of(const FeatureRead::DayColumns &mc, size_t a, Bars &b);

// 基本面源 (补段两侧同一读法): 日根 = bar1d 行 (ref = pre_close), 分钟根 = bar1m 行按 row_slot 并根; 基本面段末日 = AssetInfo::quote_end
inline constexpr const char *kBar1d = "cn_stock_real_bar1d";
// bar1d 一张月分片逐行 f(id, 日根): ref = pre_close; 无成交的行照给 (traded 判)
void each_daily(fundamental::Scratch &xs, const fundamental::Shard &sh, const fundamental::Ids &ids,
                const std::function<void(uint32_t, const Candle &)> &f);
// bar1m 一只一日 (Bar1mDay::asset; nullptr = 无 bar) → 分钟根: 无成交的行不收
void bars_of(const Bar1m *m, Bars &b);

// 源 (同形): date = 交易日; size() 行, 行 j 的全局 id = id(j) (升序);
//   leg(j, c, b) = 一行的 Leg (c / b = 调用方给的格, Leg 可指向它们)
// all.bar 库源 (行 = 库轴): 基准 = 前一行 prev 的 pre_close, 日根 = 当日行 cur, 分钟根 = mc 的 L_1min
struct LibDay {
  misc::Ymd date;
  const PoolAxis &axis;
  const Row &prev, &cur;
  const FeatureRead::DayColumns &mc;
  size_t size() const { return cur.trade.size(); }
  uint32_t id(size_t j) const { return axis.global(j); }
  Leg leg(size_t j, Cell &c, Bars &b) const;
};
// 基本面源 (行 = 全局 id): 基准 / 日根 = bar1d 行 px (ref = pre_close; 无行 → 收 0), 分钟根 = bar1m (bd 已载 date, 资产同序)
struct FundDay {
  misc::Ymd date;
  std::span<const Candle> px;
  const Bar1mDay &bd;
  size_t size() const { return px.size(); }
  uint32_t id(size_t j) const { return static_cast<uint32_t>(j); }
  Leg leg(size_t j, Cell &c, Bars &b) const;
};
// 新浪归档源 (archived_day 的读; 行 = 全局 id): 基准 / 日根 = 末快照 px (ref = 昨收; 归档无该只 → 收 0), 分钟根 = bars
struct SinaDay {
  misc::Ymd date;
  std::span<const Candle> px;
  std::span<const Bars> bars;
  size_t size() const { return px.size(); }
  uint32_t id(size_t j) const { return static_cast<uint32_t>(j); }
  Leg leg(size_t j, Cell &c, Bars &b) const;
};

// 一日合成件 (逐日复用): 成员表 mem × 源 → Fold → day [篮子]
struct Lane {
  explicit Lane(size_t baskets) : fold(baskets), day(baskets) {}
  Members mem; // 交易日 src.date 的成员表
  Fold fold;
  std::vector<Day> day; // [篮子] 前日 = 1 (rebase 接到点位上)
  // 合成源 src 的一日 (Fold::day): 成员与源的行按全局 id 归并, 源里没有的成员 = 当日无根 (比 1)
  template <class Src>
  void run(const Src &src);

private:
  Cell cell_;
  Bars bars_;
  std::vector<size_t> row_; // [成员表行] 源的行 (无 = 源长)
};

template <class Src>
void Lane::run(const Src &src) {
  static const Bars kNone{};
  const Leg idle{1.f, nullptr, &kNone};
  row_.resize(mem.size());
  size_t j = 0;
  for (size_t k = 0; k < mem.size(); ++k) {
    while (j < src.size() && src.id(j) < mem.id[k])
      ++j;
    row_[k] = j < src.size() && src.id(j) == mem.id[k] ? j : src.size();
  }
  fold.day(mem, [&](size_t k, int lo, int hi) { return row_[k] < src.size() ? src.leg(row_[k], cell_, bars_, lo, hi) : idle; }, src.date, day);
}

// 普查: 成员表 m 在篮子全表 [0, K) 上 (见 Census)
Census census(const Members &m, size_t K);

} // namespace GUI::Quote
