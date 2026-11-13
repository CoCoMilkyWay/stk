// 行情公共件: 个股 / 篮子 (Side) × 历史 (Kline) / 当日 (Day) 四路同一套类型 + 槽代数 + 篮子合成核
//
//   三段 × 两侧, 同一个形 (日根 + 240 槽分钟根):
//            库段 (all.bar)            补段: 基本面 → 新浪归档                                  当日 (新浪, 快照线程每轮)
//   个股     L_1day 行 + L_1min 行     bar1d 行 + bar1m 行 (Bar1mDay) / 归档的末快照 + 分钟根       本只的根
//   篮子     选股表成员 → Fold         冻结的成员表 → Fold                                        bind 的成员表 → Fold (增量)
//   库段 + 补段 = 日脊, 一趟合成落盘在 PriceLib (两侧同一条日脊), 其 worker 每趟发布一份 PriceView (日脊 + 两侧各一 Tape); 当日在 QuoteService, 每轮发布一份 Live.
//   个股 / 篮子只差 Fold 一步, 各段只差源 (BasketRow: LibDay / FundDay / SinaDay).
// 键: 个股 = 全局 id (AssetAxis 下标), 篮子 = 篮子下标 (shared/Basket.hpp); 两侧同一种 Tape (历史) / Days (当日).
// 一侧 = Joint (PriceView + Live), 一个标的 = Source (历史 + 日脊末段 + 当日 + 接不接): 行情各处只经这两个读, 接续只在 Joint 判 (of 逐只 / today 整侧).
//
// 分钟槽: 一交易日 kSlots = 240 根, 槽 s 的标号 (bar 结束分钟) = 09:31 … 11:30, 13:01 … 15:00 (新浪 / bar1m / L_1min 同一标号);
//   开盘集合竞价并入首根. 时间坐标 → 槽只经槽代数 (label_slot / sec_slot / row_slot); 各源按时间序 add ⇒ 竞价先于首根.
// 前收: 日根自带 ref (个股 = 交易所前收, 篮子 = 前日点位), 分钟根取同日日根的; 涨跌一律对 ref.
// 窗口: Tape 的历史同为日脊末 kDailyDays 日的日线 + 末 kMinuteDays 日的分钟 (按日期截, 不按根数);
//   个股另带库轴全部个股的日脊末段 (Tails: 各自末 kTailDays 个有成交日的日根; 近况 / 资金流分母).
// 接续: 当日接在历史之后 ⇔ 当日的前一交易日 = 日脊末日 (PriceView::joins; 两侧同一判据); 否则 (历史落后) 不接.
//
// 篮子合成 (Fold, 历史逐日 / 当日同一份) = 成员表 (define/Members.hpp) 带日内路径的合 (收盘一格即 Members::scatter 的 收 / 基准):
//   比 = Σ w · 价 / 基准 / Σ w (前日 = 1); 开 / 高 / 低 / 收各自如此
//   (高 / 低 = 成员高低的加权, 是真实路径的外包); 额 = 成员之和, 量不合成.
//   点位 = 比接到前日点位上 (rebase); 日间接续只此一步 ⇒ 各日的合成互不相干.
//   净流入 (只合成分钟的日) = 成员各自分钟涨跌法之和 (不加权): 一只逐个有成交的分钟, 收 > 上一分钟收 (首个对基准) → 该分钟额记入,
//   < → 记出, 平 → 随上一分钟的方向 (之前无方向 = 不计; 方向判据 tick_dir); 逐根记 (Bars::net), 全日之和 = Day::flow.
//   个股的净流入 = 本只的分钟涨跌法 (net_flow, 同一判据), 历史 / 当日都带.
//   基准 = 成员当日前收 (历史 = D−1 行 pre_close, 当日 = 新浪昨收; 同一个交易所前收), 无基准的成员不计;
//   当日无成交的成员比 1; 分钟缺根沿用其上根收盘, 首根前 = 基准; 计入的成员有一只在某根是缺口 (Bars::hole) → 篮子该根也是缺口.
//   逐资产一遍 (资产主序): 一只的分钟根只读一次, 比 × 权散进所属各篮子.
//   整表并行: 篮子按权数切段, 各线程扫全部成员、只累加本段的篮子 ⇒ 各篮子的累加序同逐只, 逐位一致 (篮子段的累加格常驻本核缓存).
//   当日增量: 各量对资产线性可加 ⇒ 冻结线之前的槽 (已定稿) 只合一次, 累加跨轮保留, 各资产在冻结线处的接续 (Fold::Carry) 由调用方逐只持有;
//   每轮只合日根与冻结线之后的槽, 与每轮从头合逐位一致.
// 基本面源 (补段两侧同一读法): 日根 = bar1d 行 (ref = pre_close), 分钟根 = bar1m 行按 row_slot 并根; 基本面段末日 = AssetInfo::quote_end.
#pragma once

#include "define/Csr.hpp"
#include "define/Members.hpp" // 篮子成员表: 资产 = 源的轴 (all.bar 库轴 / 新浪全市场), 键 = 篮子, cap [亿元]
#include "features/TimeIndex.hpp"
#include "fundamental/AssetAxis.hpp" // PoolAxis (Live 的个股轴)
#include "misc/date.hpp"
#include "misc/exec.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <vector>

struct Bar1m;
namespace sina {
struct Quote;
}
namespace fundamental {
class Ids;
class Scratch;
struct Shard;
} // namespace fundamental

namespace GUI::Quote {

inline constexpr int kSlots = 240;
inline constexpr size_t kDailyDays = 2500, kMinuteDays = 5, kTailDays = 5; // 日线 10 年

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

// 一个标的一日的分钟根: kSlots 格稠密, has = 有成交的根, hole = 缺口 (当日: 快照没盯住、分钟 K 尚未补到; 有无成交未知, 不画; 恒无 has)
struct Bars {
  misc::Ymd date = 0;
  std::bitset<kSlots> has, hole;
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

// 一个标的的历史 (日脊上的; 日脊见 PriceView)
struct Kline {
  std::vector<Candle> daily; // 日升序, 无成交的日不收
  std::vector<Bars> minutes; // 日升序, 无根的日不收
};

// 一个标的一个交易日 (当日 / 合成的一日); candle.date 0 = 无
struct Day {
  misc::Ymd prev = 0; // 前一交易日
  Candle candle;      // 日根 (ref = 前收)
  Bars bars;          // 分钟根
  double flow = 0;    // 净流入 [元] (分钟涨跌法) = Σ bars.net
};
// 键集的当日 [keys]: 各只不可变, 与服务侧共享 (只拷指针); nullptr = 无 (个股不在全市场 / 当日无成交, 篮子未合成 / 成员无成交)
using Days = std::vector<std::shared_ptr<const Day>>;

// 合成的一日 (Fold: 前日 = 1, candle.date ≠ 0) 接到前日点位 level 上: 各根价 × level, ref = level
void rebase(Day &y, double level);

// ---- 基本面源 ----
inline constexpr const char *kBar1d = "cn_stock_real_bar1d";
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

// 一个标的 (各处同一个身份): 篮子 = 篮子下标, 个股 = 全局 id; key = kNone 即无
struct Ref {
  static constexpr uint32_t kNone = std::numeric_limits<uint32_t>::max();
  Side side = kBasket;
  uint32_t key = kNone;
  bool ok() const { return key != kNone; }
  bool is(Side s) const { return ok() && side == s; }
  bool operator==(const Ref &) const = default;
};

// 篮子的一个成分: 全局 id + 权 (等权 = 1, 加权 = 末日收盘总市值 [亿元])
struct Constituent {
  uint32_t id;
  float w;
};
// 个股的日脊末段: 库轴全部个股各自末 kTailDays 个有成交日的日根 (键集外的个股也有)
struct Tails {
  std::vector<uint32_t> ids; // 库轴 (全局 id 升序)
  Csr<Candle> daily;         // [ids] 日升序
  std::span<const Candle> of(uint32_t id) const;
};
// 一侧的历史 (PriceView 给; 不可变): 键集 (升序) → 各键的历史, 同序; 日脊在 PriceView (两侧同一条)
struct Tape {
  std::vector<uint32_t> keys;                     // 篮子 = 全表; 个股 = 价格库留着的
  std::vector<std::shared_ptr<const Kline>> list; // [keys] 只读, 与价格库及前后几次发布共享 (不拷); nullptr = 无历史 (个股在库轴外)
  std::shared_ptr<const Tails> tails;             // 个股; 篮子空
  // 键 → 下标; 不在 → keys.size()
  size_t find(uint32_t key) const {
    const auto it = std::ranges::lower_bound(keys, key);
    return it != keys.end() && *it == key ? static_cast<size_t>(it - keys.begin()) : keys.size();
  }
  const Kline *line(uint32_t key) const; // 键的历史; 不在 / 无 → nullptr
  // 篮子 [keys]: 历史末日收盘定的成员表 (= 当日 bind 给 Quote 的) 的成员只数 / Σ 权 / 成员总市值 [亿元] (末日收盘, 股本 total_shares 以亿股计); 个股空
  std::vector<uint32_t> members;
  std::vector<double> weight, cap;
  // 篮子 [keys]: 同一成员表各篮子的成分 (权降序); 个股空
  Csr<Constituent> cons;
  // 篮子 [keys × keys]: 同一成员表两篮子权重向量的余弦 (成员重合; 无成员 = 0); 个股空
  std::vector<float> overlap;
};
// 价格库的戳 = 日脊的摘要: 库轴指纹 + 末日 / 日数 / 库段日数 / 基本面段日数 (对得上 = 读出的序列可沿用)
struct PriceStamp {
  uint64_t axis = 0;
  misc::Ymd last = 0;
  size_t days = 0, lib = 0, fund = 0;
  bool operator==(const PriceStamp &) const = default;
};
// 一个标的逐个有分钟根的日: 存价 (个股 = 原价, 篮子 = 日内比) × 当日因子 = 有效价; 同日之比可直接用存价
struct Minutes {
  // 一槽的格 (落盘的分钟根同序打头)
  enum Field : size_t { kHigh,
                        kLow,
                        kClose,
                        kFields };
  std::vector<misc::Ymd> dates; // 升序
  std::vector<double> f;        // [dates] 因子
  std::vector<float> px;        // [dates][kSlots][kFields] 存价 (无根 = NaN)
  // 日 d 的 [kSlots][kFields]; 无 → nullptr
  const float *on(misc::Ymd d) const;
};
// 价格库的一份发布 (PriceLib 每趟发布; 不可变, 只拷指针): lack 非空 = 用不了 (以用处开头; 其余空)
struct PriceView {
  std::string lack;
  PriceStamp stamp;
  std::shared_ptr<const std::vector<misc::Ymd>> dates; // 日脊 (两侧同一条; 一戳一份): 库段 [0, stamp.lib) + 基本面段 + 新浪段
  PerSide<std::shared_ptr<const Tape>> tape;           // 个股 = 留着的全历史 + 库轴的日脊末段; 篮子 = 全表 + 交接的普查
  std::vector<Ref> refs;                               // 留着的分钟价 (本戳)
  std::vector<std::shared_ptr<const Minutes>> minutes; // [refs]; nullptr = 不在库轴 (库建成之后上市 / 进池)
  misc::Ymd last() const { return stamp.last; }
  misc::Ymd lib_last() const { return (*dates)[stamp.lib - 1]; }               // 库末日 (篮子的成员 / 池 / 行业 / 股本所取)
  misc::Ymd fund_last() const { return (*dates)[stamp.lib + stamp.fund - 1]; } // 基本面段末日 (无该段 = 库末日)
  // 前一交易日为 prev 的当日接在日脊之后
  bool joins(misc::Ymd prev) const { return lack.empty() && prev == stamp.last; }
  std::shared_ptr<const Minutes> minute(Ref r) const; // 无 → nullptr
  bool covers(Ref r) const;                           // 有结论 (有分钟价或确定没有)
};
// 当日的一份 (QuoteService 每轮发布; 不可变, 只拷指针): 全市场个股 + 全部篮子的当日, 各只与前后几份共享
struct Live {
  misc::Ymd date = 0, prev = 0;                           // 新浪行情日 / 其前一交易日
  int sec = 0;                                            // 行情时刻 (秒)
  uint64_t ver = 0;                                       // 第几份 (每份 +1)
  std::shared_ptr<const PoolAxis> axis;                   // 新浪全市场
  Days stocks;                                            // [axis sub]
  Days baskets;                                           // [篮子]; 空 = 成员表未到
  std::shared_ptr<const std::vector<sina::Quote>> quotes; // [axis sub] 末快照 (盘口)
  const Day *at(Ref r) const;                             // 无 (不在全市场 / 无成交 / 未合成) → nullptr
  const sina::Quote *quote(uint32_t id) const;            // 个股的末快照; 不在全市场 / 未到 → nullptr
};

// 一个标的的源 (历史 + 日脊末段 + 当日; Joint 给): 当日只在 joined 时接着画, 各处不再自判
struct Source {
  const Kline *kl = nullptr;     // 全历史 (价格库留着的); nullptr = 无
  std::span<const Candle> daily; // 日线: 有全历史 = kl->daily, 否则个股的日脊末段 (Tails)
  const Day *today = nullptr;    // 当日 (有成交才有; 当日列不看接续)
  bool joined = false;           // 当日接得上: 有历史 = 接在日脊之后, 无历史 = 有当日 (只画当日)
  bool any() const { return kl || joined; }
  const Candle *last() const;              // 最新一根: 接上取当日, 否则日线末根; 无 → nullptr
  const Candle *candle(misc::Ymd d) const; // d 日的日根 (当日 / 日线); 无 → nullptr
  const Bars *bars(misc::Ymd d) const;     // d 日的分钟根 (当日有根 / 全历史); 无 → nullptr
};
// 一侧行情的一份 (都不可变, 只拷指针): 历史 + 当日
struct Joint {
  Side side = kStock;
  std::shared_ptr<const PriceView> view; // 空 = 价格库未发布
  std::shared_ptr<const Live> live;      // 空 = 新浪未到
  const Tape *tape() const;              // 本侧的历史 (随 view 活); 未就绪 → nullptr
  // 本侧当日接在日脊之后 → 行情日, 否则 0 (篮子另须成员表已到)
  misc::Ymd today() const;
  Source of(uint32_t key) const; // 键集外 = 只日脊末段 (个股) + 当日
};

// 一只一日 (合成的输入), 两源各自填
struct Leg {
  float base = 0;             // 基准 (当日前收); 0 = 不计
  const Cell *day = nullptr;  // 日根; nullptr = 当日无成交
  const Bars *bars = nullptr; // 分钟根; 合成分钟的日必给
};

// 篮子合成核: open (一日开始) → 逐成员 add (ws = 成员表一行) → finish (/ Σ w 落到各篮子的 Day, 前日 = 1)
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
  // 整张成员表 m 一趟 (同逐行 add, 逐位一致; 整表并行见文件头): leg(k) = 第 k 行的 Leg;
  //   was / now [m 行] = 各行冻结线处的接续 入 / 出 (now 由该行首键所在的段写). false = 同 add
  template <class LegOf>
  bool add(misc::exec::ForkJoin &ex, const Members &m, LegOf &&leg, std::span<const Carry> was, std::span<Carry> now);
  // 无成员 / 无成交的篮子 out[b].candle.date = 0; out[b].flow / bars.net = 全日 / 各根净流入 (只合日根 = 0)
  void finish(misc::Ymd d, misc::Ymd prev, std::span<Day> out);

private:
  // 一格 = 开 高 低 收 (比 × 权) / 额 / 净流入 / 缺口成员数; 格 0 = 日根, 格 1 + s = 槽 s
  static constexpr size_t kQ = 7, kCells = 1 + kSlots;
  using Row = std::array<double, kCells * kQ>; // 一只的各格 (比未乘权)
  size_t cells() const { return 1 + static_cast<size_t>(slots_); }
  // 一只 (基准 base, 0 = 不计) 的格 r: 日根 + 槽 [from, slots); k 同 add; end / fend 取有根末槽 (全部 / 冻结段) 之极.
  //   返回 −1 = 冻结段所用的基准不符, 0 = 不计 (r 未填), 1 = r 已填
  int row(const Leg &g, float base, Carry &k, Row &r, int &end, int &fend) const;
  // r 按 ws 各键的权累加
  void put(const Row &r, std::span<const Members::Weight> ws);
  int from_ = 0, freeze_ = 0, slots_ = 0;
  int end_ = -1, fend_ = -1; // 有根的末槽: 全部 / 冻结段 [0, freeze)
  std::vector<double> sw_;   // [篮子] Σ w
  std::vector<double> acc_;  // [篮子][kCells][kQ]
};

template <class LegOf>
bool Fold::add(misc::exec::ForkJoin &ex, const Members &m, LegOf &&leg, std::span<const Carry> was, std::span<Carry> now) {
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
      Carry c = was[k];
      const int x = row(g, g.base > 0 ? g.base : 0.f, c, r, end[t], fend[t]);
      if (x < 0) {
        ok.store(false, std::memory_order_relaxed);
        return;
      }
      if (lo == ws.begin())
        now[k] = c;
      if (x > 0)
        put(r, {lo, hi});
    }
  });
  end_ = *std::ranges::max_element(end), fend_ = *std::ranges::max_element(fend);
  return ok.load(std::memory_order_relaxed);
}

} // namespace GUI::Quote