// 行情的根: 槽代数 + 一根 (Cell / Candle) + 一日分钟根 (Bars) 及其紧凑形 (Packed) + 分钟涨跌法的资金流 (各处同一份)
//
// 分钟槽: 一交易日 kSlots = 240 根, 槽 s 的标号 (bar 结束分钟) = 09:31 … 11:30, 13:01 … 15:00 (新浪 / bar1m / L_1min 同一标号);
//   开盘集合竞价并入首根. 时间坐标 → 槽只经槽代数 (label_slot / sec_slot / row_slot); 各源按时间序 add ⇒ 竞价先于首根.
// 一根 = 开 高 低 收 额 (不带量: 有无成交只看额); 历史 / 当日 / 篮子 / 账户同一形.
// 前收: 日根自带 ref (个股 = 交易所前收, 篮子 = 前日点位), 分钟根取同日日根的; 涨跌一律对 ref.
// 资金流 (分钟涨跌法): 一只逐个有成交的分钟, 收 > 上一分钟收 (首个对前收) → 该分钟额记入, < → 记出, 平 → 随上一分钟的方向
//   (之前无方向 = 不计; tick_dir). 篮子 = 成员之和 (不加权, 见 Fold.hpp). 首 / 末根 (开收盘集合竞价) 单列, 计不计由用处定 (DayFlow::of);
//   日摘要 DayFlow 随日根 (Candle::flow) 走, 历史 / 当日同一字段.
// 紧凑形 (价格库落盘 / 全史 Series 的分钟同一格式, 读时解码): 一槽 = 开 高 低 收 (u16: 当日价基 lo + q × 步长 step) + log1p(额) × 2048 (u16)
//   + 净流入 / 额 × 32767 (i16); 收 = kNoBar 即无根. 步长: f16 口径 (个股) = 价基的 f16 步长, 容不下当日振幅则倍之 ⇒ f16 价逐位还原
//   (库段与特征 / 标签一致); 否则 = 振幅 / 65534 (篮子 / 账户)
#pragma once

#include "features/TimeIndex.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <array>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <span>

namespace GUI::Quote {

inline constexpr int kSlots = 240;

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
  double amount = 0; // [元]
};

// 一根的规整: 开 / 高 / 低缺 (≤ 0 / NaN) → 收
constexpr Cell cell(float o, float h, float l, float c, double a) { return {o > 0 ? o : c, h > 0 ? h : c, l > 0 ? l : c, c, a}; }
// 有成交: 额与收皆正
constexpr bool traded(const Cell &c) { return c.amount > 0 && c.close > 0; }
// 价 × a (额不动)
constexpr Cell scaled(Cell c, double a) {
  c.open = static_cast<float>(c.open * a), c.high = static_cast<float>(c.high * a);
  c.low = static_cast<float>(c.low * a), c.close = static_cast<float>(c.close * a);
  return c;
}

// 一段的资金流 [元]: 净流入 / 成交额; amount 0 = 无根
struct Flow {
  double net = 0, amount = 0;
};
// 一日的资金流摘要 (大范围按日读, 不碰分钟根): 全日 + 其中首 / 末两根 (开收盘集合竞价); amount 0 = 当日无分钟根
struct DayFlow {
  float net = 0, amount = 0, edge_net = 0, edge_amount = 0;
  // ends = 计入首 / 末两根
  Flow of(bool ends) const { return ends ? Flow{net, amount} : Flow{static_cast<double>(net) - edge_net, static_cast<double>(amount) - edge_amount}; }
};

// 一根带日: 日根 / 绘图的分钟根; ref = 所在日前收 (0 = 无); flow = 日根的当日资金流摘要 (分钟根不用)
struct Candle : Cell {
  misc::Ymd date = 0;
  float ref = 0;
  DayFlow flow;
};

// 一个标的一日的分钟根: kSlots 格稠密, has = 有成交的根, hole = 缺口 (当日: 快照没盯住、分钟 K 尚未补到; 有无成交未知, 不画; 恒无 has)
struct Bars {
  std::bitset<kSlots> has, hole;
  std::array<Cell, kSlots> cell;
  std::array<double, kSlots> net{}; // 各根净流入 [元]: 篮子 = 成员之和 (Fold), 个股 = 本只 (net_flow)

  // 并入槽 s: 开取先, 高 / 低取极, 收取后, 额相加
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
    b.amount += c.amount;
  }
};

// 一个标的一个交易日 (当日 / 合成的一日 / 账户的一日); candle.date 0 = 无
struct Day {
  Candle candle; // 日根 (ref = 前收; flow = bars 的摘要)
  Bars bars;     // 分钟根
};

// ---- 紧凑形 (见文件头) ----
struct Slot {
  uint16_t o, h, l, c, a;
  int16_t n;
};
inline constexpr uint16_t kNoBar = 0xFFFF;
// 一日的紧凑分钟根
struct Packed {
  Slot s[kSlots];
};
// 一日的价基 / 步长
struct Scale {
  float lo = 0, step = 0;
};
// b (各根净流入 net) → m; half = f16 口径 (个股). 返回价基 / 步长
Scale pack(const Bars &b, std::span<const double, kSlots> net, bool half, Packed &m);
// 一槽 (须有根) → 一根, 价 × a / 净流入 [元]
Cell unpack(const Slot &x, Scale k, double a);
double unpack_net(const Slot &x);

// 分钟涨跌法的方向: 收 > 上一价 → +1, < → −1, 平 → 随上一方向 dir (之前无方向 = 0, 不计)
constexpr int tick_dir(double close, double px, int dir) {
  return close > px ? 1 : close < px ? -1
                                     : dir;
}
// 个股一日的净流入: 逐个有成交的根 net[s] = 方向 × 该根额 (首根对基准 base = 当日前收; 无根 = 0); base ≤ 0 → 全 0
void net_flow(const Bars &b, float base, std::span<double, kSlots> net);
inline void net_flow(Bars &b, float base) { net_flow(b, base, b.net); }
// 一日分钟根 b (各根净流入 net) 的摘要
DayFlow day_flow(const Bars &b, std::span<const double, kSlots> net);
inline DayFlow day_flow(const Bars &b) { return day_flow(b, b.net); }

} // namespace GUI::Quote
