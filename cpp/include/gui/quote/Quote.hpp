// 行情公共件: 个股 / 篮子 (Side) × 历史 (Series) / 当日 (Day) 四路同一套类型 (根见 Bars.hpp, 合成核见 Fold.hpp)
//
//   三段 × 两侧, 同一个形 (日根 + 240 槽分钟根):
//            库段 (all.bar)            补段: 基本面 → 新浪归档                                  当日 (新浪, 快照线程每轮)
//   个股     L_1day 行 + L_1min 行     bar1d 行 + bar1m 行 (Bar1mDay) / 归档的末快照 + 分钟根       本只的根
//   篮子     选股表成员 → Fold         冻结的成员表 → Fold                                        bind 的成员表 → Fold (增量)
//   库段 + 补段 = 日脊, 一趟合成落盘在 PriceLib (两侧同一条日脊), 其 worker 每趟发布一份 PriceView (日脊 + 两侧各一 Tape + 账户); 当日在 QuoteService, 每轮发布一份 Live.
//   个股 / 篮子只差 Fold 一步, 各段只差源 (BasketRow: LibDay / FundDay / SinaDay).
// 键: 个股 = 全局 id (AssetAxis 下标), 篮子 = 篮子下标 (shared/Basket.hpp); 两侧同一种 Tape (历史) / Days (当日).
// 一侧 = Joint (PriceView + Live), 一个标的 = Source (历史 + 日脊末段 + 当日 + 接不接): 行情各处只经这两个读, 接续只在 Joint 判 (of 逐只 / today 整侧).
// 历史一律全史 (Series): 日根 + 逐日资金流摘要恒在 (大范围按日算); 分钟全史只读要的 (画图 / 选中的线; 展示一律分钟频).
//   个股另带库轴全部个股的日脊末段 (Tails: 各自末 kTailDays 个有成交日的日根 + 资金流摘要; 近况 / 资金流分母).
// 接续: 当日接在历史之后 ⇔ 当日的前一交易日 = 日脊末日 (PriceView::joins; 两侧同一判据); 否则 (历史落后) 不接.
#pragma once

#include "define/Csr.hpp"
#include "fundamental/AssetAxis.hpp" // PoolAxis (Live 的个股轴)
#include "gui/quote/Bars.hpp"
#include "gui/quote/Fold.hpp"
#include "misc/date.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace sina {
struct Quote;
}

namespace GUI::Quote {

inline constexpr size_t kTailDays = 5;

// 交易日历 (AssetInfo 的交易日; 各服务共用一份)
struct Calendar {
  std::vector<misc::Ymd> days;       // 升序
  misc::Ymd next(misc::Ymd d) const; // d 之后首个交易日 (须有)
  misc::Ymd prev(misc::Ymd d) const; // d 之前末个交易日 (须有)
};

// 键集的当日 [keys]: 各只不可变, 与服务侧共享 (只拷指针); nullptr = 无 (个股不在全市场 / 当日无成交, 篮子未合成 / 成员无成交)
using Days = std::vector<std::shared_ptr<const Day>>;

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

// 一个标的在日脊上的全史 (价格库读出; 不可变, 与前后几份发布共享). 价 = 个股原价 / 篮子点位; 价 × adj = 复权价 (跨日连续)
struct Series {
  // 分钟一槽的格 (无根 = NaN)
  enum Field : size_t { kHigh,
                        kLow,
                        kClose,
                        kAmount, // [元]
                        kNet,    // 净流入 [元]
                        kFields };
  std::vector<Candle> daily;      // 日升序, 无成交的日不收
  std::vector<DayFlow> flow;      // [daily]
  std::vector<double> adj;        // [daily] 复权倍数 (个股 = 后复权因子, 篮子 = 1)
  bool minute = false;            // 带分钟全史
  std::vector<float> px;          // [daily][kSlots][kFields] (minute; 无分钟根的日全 NaN)
  size_t find(misc::Ymd d) const; // d 在 daily 的下标; 无 → daily.size()
  const float *slots(size_t i) const {
    assert(minute && i < daily.size());
    return px.data() + i * kSlots * kFields;
  }
  // 第 i 日 → 分钟根 (开 = 上一根收, 首根 = 前收); 无根 → false
  bool bars(size_t i, Bars &b) const;
};

// 篮子的一个成分: 全局 id + 权 (等权 = 1, 加权 = 末日收盘总市值 [亿元])
struct Constituent {
  uint32_t id;
  float w;
};
// 个股的日脊末段: 库轴全部个股各自末 kTailDays 个有成交日的日根 + 资金流摘要 (键集外的个股也有)
struct Tails {
  std::vector<uint32_t> ids;      // 库轴 (全局 id 升序)
  Csr<Candle> daily;              // [ids] 日升序
  Csr<DayFlow> flow;              // [ids] 同 daily
  size_t find(uint32_t id) const; // 不在 → ids.size()
};
// 一侧的历史 (PriceView 给; 不可变): 键集 (升序) → 各键的全史, 同序; 日脊在 PriceView (两侧同一条)
struct Tape {
  std::vector<uint32_t> keys;                      // 篮子 = 全表; 个股 = 价格库留着的
  std::vector<std::shared_ptr<const Series>> list; // [keys] 只读, 与价格库及前后几次发布共享 (不拷); nullptr = 无历史 (个股在库轴外)
  std::shared_ptr<const Tails> tails;              // 个股; 篮子空
  // 键 → 下标; 不在 → keys.size()
  size_t find(uint32_t key) const {
    const auto it = std::ranges::lower_bound(keys, key);
    return it != keys.end() && *it == key ? static_cast<size_t>(it - keys.begin()) : keys.size();
  }
  const Series *line(uint32_t key) const; // 键的全史; 不在 / 无 → nullptr
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
// 账户的历史 (价格库按资金流水账本 × 库轴个股的根在日脊上回放, account/Ledger.hpp): 时间加权指数, 前收 = 前一日指数, 全史恒带分钟
//   (分钟 = 当日收盘后的持仓按各只分钟收盘计价; 库轴外的持仓 (ETF / LOF) 日内不动, 逐日按成交价 / 券商现价插值)
struct Book {
  uint64_t ledger = 0;              // 所依的账本代
  std::shared_ptr<const Series> se; // 空 = 回放为空
  double nav = 0, after = 0;        // 末日净值 / 末日之后账本里的外部进出 (当日 = 末日指数 × growth(nav, 净资产, after))
  double in = 0;                    // 回放各日外部进出之和 (累计净转入)
};
// 价格库的一份发布 (PriceLib 每趟发布; 不可变, 只拷指针): lack 非空 = 用不了 (以用处开头; 其余空)
struct PriceView {
  std::string lack;
  PriceStamp stamp;
  std::shared_ptr<const std::vector<misc::Ymd>> dates; // 日脊 (两侧同一条; 一戳一份): 库段 [0, stamp.lib) + 基本面段 + 新浪段
  PerSide<std::shared_ptr<const Tape>> tape;           // 个股 = 留着的全史 + 库轴的日脊末段; 篮子 = 全表 + 交接的普查
  std::shared_ptr<const Book> book;                    // 账户; 空 = 无账本
  misc::Ymd last() const { return stamp.last; }
  misc::Ymd lib_last() const { return (*dates)[stamp.lib - 1]; }               // 库末日 (篮子的成员 / 池 / 行业 / 股本所取)
  misc::Ymd fund_last() const { return (*dates)[stamp.lib + stamp.fund - 1]; } // 基本面段末日 (无该段 = 库末日)
  // 前一交易日为 prev 的当日接在日脊之后
  bool joins(misc::Ymd prev) const { return lack.empty() && prev == stamp.last; }
  const Series *series(Ref r) const; // 无 → nullptr
  // r 的全史有结论 (minute = 连分钟): 读到了, 或确定没有 (个股在库轴外)
  bool covers(Ref r, bool minute) const;
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
  std::shared_ptr<const std::vector<sina::Quote>> watch;  // 订阅码 (QuoteService::watch; 全市场内外皆可) 的末快照, 新浪码升序; 未到的不收
  const Day *at(Ref r) const;                             // 无 (不在全市场 / 无成交 / 未合成) → nullptr
  const sina::Quote *quote(uint32_t id) const;            // 个股的末快照; 不在全市场 / 未到 → nullptr
  const sina::Quote *watched(std::string_view sym) const; // 订阅码的末快照; 未订阅 / 未到 → nullptr
};

// 一个标的的源 (全史 + 日脊末段 + 当日; Joint 给): 当日只在 joined 时接着画, 各处不再自判
struct Source {
  const Series *se = nullptr;    // 全史 (价格库留着的); nullptr = 无
  std::span<const Candle> daily; // 日线: 有全史 = se->daily, 否则个股的日脊末段 (Tails)
  std::span<const DayFlow> flow; // [daily]
  const Day *today = nullptr;    // 当日 (有成交才有; 当日列不看接续)
  bool joined = false;           // 当日接得上: 有历史 = 接在日脊之后, 无历史 = 有当日 (只画当日)
  bool any() const { return se || joined; }
  const Candle *last() const;                 // 最新一根: 接上取当日, 否则日线末根; 无 → nullptr
  const Candle *candle(misc::Ymd d) const;    // d 日的日根 (当日 / 日线); 无 → nullptr
  Flow flow_of(misc::Ymd d, bool ends) const; // d 日的资金流 (当日 / 日线); 无 → amount 0
  // d 日的分钟根 (当日有根 / 全史带分钟) 填进 b; 无 → false
  bool bars(misc::Ymd d, Bars &b) const;
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

} // namespace GUI::Quote
