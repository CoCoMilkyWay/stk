// 行情公共件: 个股 / 篮子 (Side) × 历史 (Kline) / 当日 (Day) 四路同一套类型 + 槽代数 + 篮子合成核
//
//            历史 (后台读, 带缓存) → Kline                           当日 (新浪, 快照线程每轮) → Day
//   个股     StockHistory: bar1d 行 + bar1m 行 (Bar1mDay)            QuoteService::today(kStock): 本只的根
//   篮子     BasketHistory: all.bar L_1day 行 + L_1min 行 → Fold     QuoteService::today(kBasket): bind 的成员表 → Fold
//            (库末日之后: 冻结的成员表 + bar1d / bar1m → Fold)
//   个股 / 篮子只差 Fold 一步, 历史 / 当日只差源.
// 键: 个股 = 全局 id (AssetAxis 下标), 篮子 = basket_line (shared/Basket.hpp). 两路历史同一个请求 (键集) / 发布 (Tape),
//   当日同一个 today (键集 → Day), 版本各一个 (QuoteStatus::ver).
//
// 分钟槽: 一交易日 kSlots = 240 根, 槽 s 的标号 (bar 结束分钟) = 09:31 … 11:30, 13:01 … 15:00 (新浪 / bar1m / L_1min 同一标号);
//   开盘集合竞价并入首根. 时间坐标 → 槽只经槽代数 (label_slot / sec_slot / row_slot); 各源按时间序 add ⇒ 竞价先于首根.
// 前收: 日根自带 ref (个股 = 交易所前收, 篮子 = 前日点位), 分钟根取同日日根的; 涨跌一律对 ref.
// 窗口: 两份历史同为最近 kDailyDays 个交易日的日线 + 最近 kMinuteDays 个交易日的分钟.
// 接续: 当日接在历史之后 ⇔ 当日的前一交易日 = 历史末日 (joins); 否则 (历史落后) 不接.
//
// 篮子合成 (Fold, 历史逐日 / 当日同一份): 点位 = 前日点位 × Σ w · 价 / 基准 / Σ w; 开 / 高 / 低 / 收各自如此
//   (高 / 低 = 成员高低的加权, 是真实路径的外包); 额 = 成员之和, 量不合成.
//   净流入 (只合成分钟的日) = 成员各自分钟涨跌法之和 (不加权): 一只逐个有成交的分钟, 收 > 上一分钟收 (首个对基准) → 该分钟额记入,
//   < → 记出, 平 → 随上一分钟的方向 (之前无方向 = 不计; 方向判据 tick_dir); 逐根记 (Bars::net), 全日之和 = Day::flow.
//   个股的净流入 = 本只的分钟涨跌法 (net_flow, 同一判据), 历史 / 当日都带.
//   基准 = 成员当日前收 (历史 = D−1 行 pre_close, 当日 = 新浪昨收; 同一个交易所前收), 无基准的成员不计;
//   当日无成交的成员比 1; 分钟缺根沿用其上根收盘, 首根前 = 基准.
//   逐资产一遍 (资产主序): 一只的分钟根只读一次, 比 × 权散进所属各篮子.
//   当日增量: 各量对资产线性可加 ⇒ 冻结线之前的槽 (已定稿) 只合一次, 累加跨轮保留, 各资产在冻结线处的接续 (Fold::Carry) 由调用方逐只持有;
//   每轮只合日根与冻结线之后的槽, 与每轮从头合逐位一致.
// 基本面源 (个股历史 / 篮子补日同一读法): 日根 = bar1d 行 (ref = pre_close), 分钟根 = bar1m 行按 row_slot 并根; 历史末日 = 两表水位之小.
#pragma once

#include "define/Csr.hpp"
#include "features/TimeIndex.hpp"
#include "misc/date.hpp"
#include "shared/Progress.hpp"

#include <algorithm>
#include <array>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <vector>

struct Bar1m;
namespace fundamental {
class Catalog;
class Ids;
class Scratch;
struct Shard;
} // namespace fundamental

namespace GUI::Database {

inline constexpr int kSlots = 240;
inline constexpr size_t kDailyDays = 2500, kMinuteDays = 5; // 日线 10 年

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
// 有成交: 量与收皆正
constexpr bool traded(const Cell &c) { return c.volume > 0 && c.close > 0; }

// 一根带日: 日根 / 绘图的分钟根; ref = 所在日前收 (0 = 无)
struct Candle : Cell {
  misc::Ymd date = 0;
  float ref = 0;
};

// 一个标的一日的分钟根: kSlots 格稠密, has = 有成交的根
struct Bars {
  misc::Ymd date = 0;
  std::bitset<kSlots> has;
  std::array<Cell, kSlots> cell;
  std::array<double, kSlots> net{}; // 各根净流入 [元] (分钟涨跌法): 篮子 = 成员之和 (Fold), 个股 = 本只 (net_flow)

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

// 分钟涨跌法的方向: 收 > 上一价 → +1, < → −1, 平 → 随上一方向 dir (之前无方向 = 0, 不计)
constexpr int tick_dir(double close, double px, int dir) {
  return close > px ? 1 : close < px ? -1
                                     : dir;
}
// 个股一日的净流入: 逐个有成交的根 b.net = 方向 × 该根额 (首根对基准 base = 当日前收), 返回全日之和; base ≤ 0 → 全 0
double net_flow(Bars &b, float base);

// 一个标的的历史
struct Kline {
  misc::Ymd last = 0;        // 历史末日 (数据水位, 不看本标的当日有无成交)
  std::vector<Candle> daily; // 日升序
  std::vector<Bars> minutes; // 日升序, 无成交的日不收
};

// 一个标的一个交易日 (当日 / 合成的一日); candle.date 0 = 无
struct Day {
  misc::Ymd prev = 0; // 前一交易日
  Candle candle;      // 日根 (ref = 前收)
  Bars bars;          // 分钟根
  double flow = 0;    // 净流入 [元] (分钟涨跌法) = Σ bars.net
};

inline bool joins(const Kline &k, const Day &d) { return d.candle.date != 0 && d.prev == k.last; }

// ---- 基本面源 ----
inline constexpr const char *kBar1d = "cn_stock_real_bar1d", *kBar1m = "bar1m";
// 历史末日 = bar1d 水位 (末个非空月分片; 0 行月也落文件) ∧ bar1m 末日; 未同步 → assert
misc::Ymd fund_last(const fundamental::Catalog &cat);
// bar1d 一张月分片逐行 f(id, 日根): ref = pre_close; 无成交的行照给 (traded 判)
void each_daily(fundamental::Scratch &xs, const fundamental::Shard &sh, const fundamental::Ids &ids,
                const std::function<void(uint32_t, const Candle &)> &f);
// bar1m 一只一日 (Bar1mDay::asset; nullptr = 无 bar) → 分钟根: 无成交的行不收
void bars_of(const Bar1m *m, misc::Ymd d, Bars &b);

// 标的两种
enum Side : int { kStock,
                  kBasket };
inline constexpr int kSides = 2;
template <class X>
using PerSide = std::array<X, kSides>;

// 一路历史的请求 / 发布 (两种同形): 键集 → 各键的历史, 同序
struct HistoryRequest {
  uint32_t gen = 0;
  std::vector<uint32_t> keys;
};
// 篮子的一个成分: 全局 id + 权 (等权 = 1, 加权 = 末日收盘总市值 [亿元])
struct Constituent {
  uint32_t id;
  float w;
};
struct Tape {
  std::vector<uint32_t> keys;
  std::vector<std::shared_ptr<const Kline>> list; // [keys] 只读, 与服务侧及前后几次发布共享 (不拷); 空 = 源未就绪
  const char *lack = nullptr;                     // 源未就绪的原因 (只篮子有: all.bar 未建 / 落盘不足 2 日)
  misc::Ymd basis = 0;                            // 篮子: 成员 / 池 / 行业 / 股本所取的库末日 (其后至 Kline::last 为基本面补日); 个股 0
  // 篮子 [keys]: 历史末日收盘定的成员表 (= 当日 bind 给 Quote 的) 的成员只数 / Σ 权 (加权线 = 末日收盘总市值 [亿元]: 股本 total_shares 以亿股计); 个股空
  std::vector<uint32_t> members;
  std::vector<double> weight;
  // 篮子 [keys]: 同一成员表各篮子的成分 (权降序); 个股空
  Csr<Constituent> cons;
  // 篮子 [keys × keys]: 同一成员表两篮子权重向量的余弦 (成员重合; 无成员 = 0); 个股空
  std::vector<float> overlap;
};
struct HistoryPub : Published {
  uint32_t gen = 0;
  std::shared_ptr<const Tape> data;
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
  std::span<Weight> of(size_t a) { return {ws.data() + at[a], ws.data() + at[a + 1]}; }
};

// 一只一日 (合成的输入), 两源各自填
struct Leg {
  float base = 0;             // 基准 (当日前收); 0 = 不计
  const Cell *day = nullptr;  // 日根; nullptr = 当日无成交
  const Bars *bars = nullptr; // 分钟根; 合成分钟的日必给
};

// 篮子合成核: open (一日开始) → 逐资产 add → finish (× 前日点位 / Σ w 落到各篮子的 Day)
//   增量 (当日跨轮): open(from, freeze, slots), 槽 [0, from) 是上一轮冻结的 (累加保留), 本轮只合日根与槽 [from, slots), 冻结线推进到 freeze
class Fold {
public:
  // 一只在冻结线处的接续
  struct Carry {
    float base = 0;                // 冻结段所用的基准 (0 = 该只不计)
    int dir = 0;                   // 末个有成交根的涨跌方向
    double c = 1, px = 0, net = 0; // 末根的比 (之前无根 = 1) / 末根收盘 (之前无根 = 基准) / 冻结段净流入
  };

  explicit Fold(size_t baskets);

  // 从头: slots = 合成前几槽的分钟 (0 = 只合日根; 当日 = 此刻所在根 + 1)
  void open(int slots) { open(0, 0, slots); }
  // 增量: from = 0 (从头) 或上一轮的 freeze; from ≤ freeze ≤ slots
  void open(int from, int freeze, int slots);
  void add(const Leg &g, std::span<const Members::Weight> ws) {
    Carry k;
    add(g, ws, k);
  }
  // k = 本只在冻结线处的接续: 入 = 上一轮 freeze 处 (from = 0 不看), 出 = 本轮 freeze 处.
  // false = 冻结段所用的基准与本轮的不符 (本只未计; 调用方须 from = 0 重合)
  bool add(const Leg &g, std::span<const Members::Weight> ws, Carry &k);
  // level[b] = 前日点位; 无成员 / 无成交 / 无点位的篮子 out[b].candle.date = 0; out[b].flow / bars.net = 全日 / 各根净流入 (只合日根 = 0)
  void finish(misc::Ymd d, misc::Ymd prev, std::span<const double> level, std::span<Day> out);

private:
  // 一格 = 开 高 低 收 (比 × 权) / 额 / 净流入; 格 0 = 日根, 格 1 + s = 槽 s
  static constexpr size_t kQ = 6, kCells = 1 + kSlots;
  size_t cells() const { return 1 + static_cast<size_t>(slots_); }
  int from_ = 0, freeze_ = 0, slots_ = 0;
  int end_ = -1, fend_ = -1; // 有根的末槽: 全部 / 冻结段 [0, freeze)
  std::vector<double> sw_;   // [篮子] Σ w
  std::vector<double> acc_;  // [篮子][kCells][kQ]
};

} // namespace GUI::Database
