// Assets 行情公共件: 个股 / 篮子 × 历史 (Kline) / 当日 (Day) 四路同一套类型 + 槽代数 + 篮子合成核
//
//            历史 (落盘, 后台读一次) → Kline                         当日 (新浪, 快照线程每轮) → Day
//   个股     StockHistory: bar1d 行 + bar1m 行 (Bar1mDay)            QuoteService::today(i): 本只的根
//   篮子     BasketHistory: all.bar L_1day 行 + L_1min 行 → Fold     QuoteService::today(篮子): bind 的成员表 → Fold
//   个股 / 篮子只差 Fold 一步, 历史 / 当日只差源.
//
// 分钟槽: 一交易日 kSlots = 240 根, 槽 s 的标号 (bar 结束分钟) = 09:31 … 11:30, 13:01 … 15:00 (新浪 / bar1m / L_1min 同一标号);
//   开盘集合竞价并入首根. 时间坐标 → 槽只经槽代数 (label_slot / sec_slot / row_slot); 各源按时间序 add ⇒ 竞价先于首根.
// 前收: 日根自带 ref (个股 = 交易所前收, 篮子 = 前日点位), 分钟根取同日日根的; 涨跌一律对 ref.
// 窗口: 两份历史同为最近 kDailyDays 个交易日的日线 + 最近 kMinuteDays 个交易日的分钟.
// 接续: 当日接在历史之后 ⇔ 当日的前一交易日 = 历史末日 (joins); 否则 (历史落后) 不接.
//
// 篮子合成 (Fold, 历史逐日 / 当日同一份): 点位 = 前日点位 × Σ w · 价 / 基准 / Σ w; 开 / 高 / 低 / 收各自如此
//   (高 / 低 = 成员高低的加权, 是真实路径的外包); 额 = 成员之和, 量不合成.
//   基准 = 成员当日前收 (历史 = D−1 行 pre_close, 当日 = 新浪昨收; 同一个交易所前收), 无基准的成员不计;
//   当日无成交的成员比 1; 分钟缺根沿用其上根收盘, 首根前 = 基准.
//   逐资产一遍 (资产主序): 一只的分钟根只读一次, 比 × 权散进所属各篮子.
#pragma once

#include "features/TimeIndex.hpp"
#include "misc/date.hpp"
#include "shared/Progress.hpp"

#include <algorithm>
#include <array>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace GUI::Database {

inline constexpr int kSlots = 240;
inline constexpr size_t kDailyDays = 270, kMinuteDays = 4;

// ---- 槽代数: 三种时间坐标 → 槽 ----
// 槽 → 标号 (hh · 60 + mm)
constexpr int slot_label(int s) { return s < 120 ? 9 * 60 + 31 + s : 13 * 60 + 1 + (s - 120); }
// 标号 → 槽; 不是交易分钟 → -1
constexpr int label_slot(int m) {
  return m >= 9 * 60 + 31 && m <= 11 * 60 + 30 ? m - (9 * 60 + 31) : m >= 13 * 60 + 1 && m <= 15 * 60 ? 120 + m - (13 * 60 + 1)
                                                                                                      : -1;
}
// 快照时刻 (当日秒) → 所在根: 竞价并入首根, 午休并入 11:30, 收盘后并入 15:00
constexpr int sec_slot(int sec) {
  const int m = sec / 60;
  return m < 9 * 60 + 30 ? 0 : m < 11 * 60 + 30 ? m - (9 * 60 + 30)
                           : m < 13 * 60        ? 119
                           : m < 15 * 60        ? 120 + m - 13 * 60
                                                : kSlots - 1;
}
// L_1min 行 (= Bar1mDay 行) → 槽: 行 [0, 15) 竞价段与行 15 (09:30 那分钟) 同归首根
constexpr int row_slot(size_t t) { return t <= OPEN_BIN_MINUTES ? 0 : static_cast<int>(t - OPEN_BIN_MINUTES); }
static_assert(TRADE_MINUTES_PER_DAY - OPEN_BIN_MINUTES == static_cast<size_t>(kSlots));

// 一根的值
struct Cell {
  float open = 0, high = 0, low = 0, close = 0;
  double volume = 0, amount = 0; // [股] [元]
};

// 一根的规整: 开 / 高 / 低缺 (≤ 0 / NaN) → 收
constexpr Cell cell(float o, float h, float l, float c, double v, double a) {
  return {o > 0 ? o : c, h > 0 ? h : c, l > 0 ? l : c, c, v, a};
}

// 一根: 日线 (slot 0) / 分钟 (slot = 槽); ref = 所在日前收 (0 = 无)
struct Candle : Cell {
  misc::Ymd date = 0;
  int slot = 0;
  float ref = 0;
};

// 一个标的一日的分钟根: kSlots 格稠密, has = 有成交的根
struct Bars {
  misc::Ymd date = 0;
  std::bitset<kSlots> has;
  std::array<Cell, kSlots> cell;

  // 并入槽 s: 开取先, 高 / 低取极, 收取后, 量 / 额相加
  void add(int s, const Cell &c) {
    Cell &b = cell[static_cast<size_t>(s)];
    if (!has[static_cast<size_t>(s)]) {
      b = c;
      has.set(static_cast<size_t>(s));
      return;
    }
    b.high = std::max(b.high, c.high);
    b.low = std::min(b.low, c.low);
    b.close = c.close;
    b.volume += c.volume;
    b.amount += c.amount;
  }
};

// 一个标的的历史
struct Kline {
  misc::Ymd last = 0;        // 历史末日 (数据水位, 不看本标的当日有无成交)
  std::vector<Candle> daily; // 日升序
  std::vector<Bars> minutes; // 日升序, 无成交的日不收
};

// 一个标的一个交易日 (当日 / 合成的一日); date 0 = 无
struct Day {
  misc::Ymd date = 0;
  misc::Ymd prev = 0; // 前一交易日
  Candle candle;      // 日根 (ref = 前收)
  Bars bars;          // 分钟根
};

inline bool joins(const Kline &k, const Day &d) { return d.date != 0 && d.prev == k.last; }

// 历史一路的发布 (两个历史服务同形)
template <class T>
struct HistoryPub : Published {
  uint32_t gen = 0;
  std::shared_ptr<const T> data;
};

// 篮子成员表, 资产主序 (CSR): 资产 a 的所属篮子 = of(a); 下标 a = 源的轴 (all.bar 库轴 / 新浪全市场)
struct Members {
  struct Weight {
    uint32_t basket;
    float w;
  };
  std::vector<uint32_t> at{0}; // [资产数 + 1]
  std::vector<Weight> ws;

  void clear() { at.assign(1, 0), ws.clear(); }
  void add(uint32_t basket, float w) { ws.push_back({basket, w}); }
  void next() { at.push_back(static_cast<uint32_t>(ws.size())); } // 收一只
  size_t assets() const { return at.size() - 1; }
  std::span<const Weight> of(size_t a) const { return {ws.data() + at[a], ws.data() + at[a + 1]}; }
};

// 一只一日 (合成的输入), 两源各自填
struct Leg {
  float base = 0;             // 基准 (当日前收); 0 = 不计
  const Cell *day = nullptr;  // 日根; nullptr = 当日无成交
  const Bars *bars = nullptr; // 分钟根; 合成分钟的日必给
};

// 篮子合成核: open (一日开始) → 逐资产 add → finish (× 前日点位 / Σ w 落到各篮子的 Day)
class Fold {
public:
  explicit Fold(size_t baskets);

  // slots = 合成前几槽的分钟 (0 = 只合日根; 当日 = 此刻所在根 + 1)
  void open(int slots);
  void add(const Leg &g, std::span<const Members::Weight> ws);
  // level[b] = 前日点位; 无成员 / 无成交 / 无点位的篮子 out[b].date = 0
  void finish(misc::Ymd d, misc::Ymd prev, std::span<const double> level, std::span<Day> out);

private:
  int slots_ = 0;
  int end_ = -1;                           // 有根的末槽
  std::vector<double> sw_;                 // [篮子] Σ w
  std::vector<std::array<double, 5>> day_; // [篮子] 开 高 低 收 (比 × 权) / 额
  std::vector<double> bars_;               // [篮子][kSlots][5] 同上
};

} // namespace GUI::Database
