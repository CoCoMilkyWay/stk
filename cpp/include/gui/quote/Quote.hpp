// 行情公共件: 个股 / 篮子 / 账户 (Side) × 历史 (Series) / 当日 (Day) 同一套类型 (根见 Bars.hpp, 合成核见 Fold.hpp)
//
//   三段 × 三侧, 同一个形 (日根 + 240 槽分钟根):
//            库段 (all.bar)            补段: 基本面 → 新浪归档                                  当日 (新浪, 快照线程每轮)
//   个股     L_1day 行 + L_1min 行     bar1d 行 + bar1m 行 (Bar1mDay) / 归档的末快照 + 分钟根       本只的根
//   篮子     选股表成员 → Fold         冻结的成员表 → Fold                                        bind 的成员表 → Fold (增量)
//   账户     资金流水账本 × 个股的根回放 (gui/account/AccountSide.hpp; 库段 / 补段同一条)              持仓 (Holdings) × 分钟收 (同一核)
//   库段 + 补段 = 日脊, 一趟合成落盘在 PriceLib (各侧同一条日脊), 其 worker 每趟发布一份 PriceView (日脊 + 各侧一 Tape); 当日在 QuoteService, 每轮发布一份 Live.
//   篮子 = 成员 × 个股的根过 Fold (历史取个股侧落盘的根), 个股各段只差源 (BasketRow: LibDay / FundDay / SinaDay); 账户不落盘 (账本一变整条重放).
// 键: 个股 = 全局 id (AssetAxis 下标; 历史 / 当日 / 快照同一下标), 篮子 = 篮子下标 (shared/Basket.hpp), 账户 = 0; 各侧同一种 Tape (历史) / Days (当日).
// 一侧 = Joint (PriceView + Live), 一个标的 = Source (日序列 = 日线 + 接得上的当日; 下标 / 分钟根 / 复权同一口径):
//   行情各处只经这两个读, 接续只在 Joint 判 (of 逐只 / today 整侧).
// 历史一律全史 (Series): 日根 (带资金流摘要) 恒在 (大范围按日算); 分钟全史只读要的 (画图 / 选中的线; 展示一律分钟频), 是紧凑根的视图 (不解码不拷).
//   个股另带全部个股的日脊末段 (Tails: 各自末 kTailDays 个有成交日的日根; 近况 / 资金流分母).
// 接续: 当日接在历史之后 ⇔ 当日的前一交易日 = 日脊末日 (PriceView::joins; 各侧同一判据); 否则 (历史落后) 不接.
#pragma once

#include "define/Csr.hpp"
#include "gui/quote/Bars.hpp"
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

// 一侧的当日 [键]: 各只不可变, 与服务侧共享 (只拷指针); nullptr = 无 (个股不在全市场 / 当日无成交, 篮子未合成 / 成员无成交)
using Days = std::vector<std::shared_ptr<const Day>>;

// 标的三侧; 前 kMarkets 侧是行情 (落盘 / 有成交额 / 进组合账), 账户由账本回放
enum Side : int { kStock,
                  kBasket,
                  kAccount };
inline constexpr int kSides = 3, kMarkets = 2;
template <class X>
using PerSide = std::array<X, kSides>;
template <class X>
using PerMarket = std::array<X, kMarkets>;

// 一个标的 (各处同一个身份): 篮子 = 篮子下标, 个股 = 全局 id, 账户 = 0; key = kNone 即无
struct Ref {
  static constexpr uint32_t kNone = std::numeric_limits<uint32_t>::max();
  Side side = kBasket;
  uint32_t key = kNone;
  bool ok() const { return key != kNone; }
  bool is(Side s) const { return ok() && side == s; }
  bool operator==(const Ref &) const = default;
};
// 账户的一只持仓: 流水代码 / 全局 id (Ref::kNone = 资产轴外, 如 ETF / LOF) / 股 / 计价 (券商现价 / 回放末日收盘)
struct Holding {
  std::string code;
  uint32_t id = Ref::kNone;
  double qty = 0, px = 0;
  bool operator==(const Holding &) const = default;
};

// 一个标的在日脊上的全史 (价格库读出; 不可变, 与前后几份发布共享). 价 = 个股原价 / 篮子点位 / 账户指数; 价 × adj = 复权价 (跨日连续)
//   分钟 = 各日紧凑根 (Packed) 的视图, 读时解码: 个股 / 篮子指进价格库段的映射, 账户指进自持的块; 所指的存储由 hold 保活
struct Series {
  // 一日的分钟根: p = nullptr 即该日无分钟根; 价 = 紧凑价 × a (存价 → 价: 篮子 = 因子, 其余 1)
  struct Mins {
    const Packed *p = nullptr;
    Scale k;
    double a = 1;
  };
  std::vector<Candle> daily;                     // 日升序, 无成交的日不收
  std::vector<double> adj;                       // [daily] 复权倍数 (个股 = 后复权因子, 篮子 / 账户 = 1)
  bool minute = false;                           // 带分钟全史
  std::vector<Mins> mins;                        // [daily] (minute)
  std::vector<std::shared_ptr<const void>> hold; // mins 所指的存储
  size_t find(misc::Ymd d) const;                // d 在 daily 的下标; 无 → daily.size()
  // 第 i 日有分钟根
  bool has(size_t i) const {
    assert(minute && i < daily.size());
    return mins[i].p != nullptr;
  }
  // 第 i 日 (须 has) 槽 s 的收 (无根 = 0) / 根 (无根 → false)
  float close(size_t i, int s) const;
  bool bar(size_t i, int s, Cell &c) const;
  // 第 i 日 → 分钟根 (含各根净流入); 无根 → false
  bool bars(size_t i, Bars &b) const;
};

// 篮子的一个成分: 全局 id + 权 (等权 = 1, 加权 = 末日收盘总市值 [亿元])
struct Constituent {
  uint32_t id;
  float w;
};
// 个股的日脊末段: 日脊上有成交的全部个股各自末 kTailDays 个有成交日的日根 (键集外的个股也有)
struct Tails {
  std::vector<uint32_t> ids;      // 全局 id 升序
  Csr<Candle> daily;              // [ids] 日升序
  size_t find(uint32_t id) const; // 不在 → ids.size()
};
// 一侧的历史 (PriceView 给; 不可变; 三侧同形): 键集 (升序) → 各键的全史, 同序; 日脊在 PriceView (各侧同一条)
struct Tape {
  std::vector<uint32_t> keys;                      // 篮子 = 全表; 个股 = 价格库留着的; 账户 = {0}
  std::vector<std::shared_ptr<const Series>> list; // [keys] 只读, 与价格库及前后几次发布共享 (不拷); nullptr = 无历史 (个股日脊上无成交 / 无账本)
  // 键 → 下标; 不在 → keys.size()
  size_t find(uint32_t key) const {
    const auto it = std::ranges::lower_bound(keys, key);
    return it != keys.end() && *it == key ? static_cast<size_t>(it - keys.begin()) : keys.size();
  }
  const Series *line(uint32_t key) const; // 键的全史; 不在 / 无 → nullptr
};
// 篮子的普查 (PriceView 给; 不可变, 一戳一份): 历史末日收盘定的成员表 (= 当日 bind 给 Quote 的) 在篮子全表上 [篮子]
struct Census {
  std::vector<uint32_t> members;   // 成员只数
  std::vector<double> weight, cap; // Σ 权 / 成员总市值 [亿元] (末日收盘, 股本 total_shares 以亿股计)
  Csr<Constituent> cons;           // 成分 (权降序)
  std::vector<float> overlap;      // [篮子 × 篮子] 两篮子权重向量的余弦 (成员重合; 无成员 = 0)
};
// 账户回放的摘要 (PriceView 给; 随账户侧的 Tape 一并换): 回放末日净值 / 其后账本里的外部进出 / 回放各日外部进出之和 (累计净转入) / 末日收后持仓;
//   账户全史 = 时间加权指数 (前收 = 前一日指数, 恒带分钟); 当日 = 末日指数 × growth(nav, 净资产_s, 进出) (account::intraday)
struct Equity {
  double nav = 0, after = 0, in = 0;
  std::vector<Holding> hold; // 计价 = 末日收盘
};
// 价格库的戳 = 日脊的摘要: 库轴指纹 + 篮子表指纹 + 末日 / 日数 / 库段日数 / 基本面段日数 (对得上 = 读出的序列可沿用)
struct PriceStamp {
  uint64_t axis = 0, table = 0;
  misc::Ymd last = 0;
  size_t days = 0, lib = 0, fund = 0;
  bool operator==(const PriceStamp &) const = default;
};
// 价格库的一份发布 (PriceLib 每趟发布; 不可变, 只拷指针): lack 非空 = 用不了 (以用处开头; 其余空)
struct PriceView {
  std::string lack;
  PriceStamp stamp;
  std::shared_ptr<const std::vector<misc::Ymd>> dates; // 日脊 (各侧同一条; 一戳一份): 库段 [0, stamp.lib) + 基本面段 + 新浪段
  PerSide<std::shared_ptr<const Tape>> tape;           // 个股 = 留着的全史; 篮子 = 全表; 账户 = 账本的回放
  std::shared_ptr<const Tails> tails;                  // 个股: 日脊末段 (全部个股; 一戳一份)
  std::shared_ptr<const Census> census;                // 篮子: 交接成员表的普查 (一戳一份)
  Equity equity;                                       // 账户: 回放的摘要 (无账本 = 全 0)
  misc::Ymd last() const { return stamp.last; }
  misc::Ymd lib_last() const { return (*dates)[stamp.lib - 1]; }               // 库末日 (篮子的成员 / 池 / 行业 / 股本所取)
  misc::Ymd fund_last() const { return (*dates)[stamp.lib + stamp.fund - 1]; } // 基本面段末日 (无该段 = 库末日)
  // 前一交易日为 prev 的当日接在日脊之后
  bool joins(misc::Ymd prev) const { return lack.empty() && prev == stamp.last; }
  const Series *series(Ref r) const; // 无 → nullptr
  // r 的全史有结论 (minute = 连分钟): 读到了, 或确定没有
  bool covers(Ref r, bool minute) const;
};
// 各只的末快照 [全局 id] (nullptr = 不在全市场 / 未到); 一轮只换变了的, 其余与前几份共享
using Quotes = std::vector<std::shared_ptr<const sina::Quote>>;
// 当日的一份 (QuoteService 每轮发布, QuoteHub 并上账户; 不可变, 只拷指针): 各侧的当日, 各只与前后几份共享
struct Live {
  misc::Ymd date = 0, prev = 0;                           // 新浪行情日 / 其前一交易日
  int sec = 0;                                            // 行情时刻 (秒)
  uint64_t ver = 0;                                       // 新浪第几份 (每份 +1; 并账户不变)
  PerSide<std::shared_ptr<const Days>> days;              // [侧][键]; 篮子空 = 成员表未到, 账户空 = 无
  std::shared_ptr<const Quotes> quotes;                   // 盘口
  std::shared_ptr<const std::vector<sina::Quote>> watch;  // 订阅码 (QuoteService::watch; 全市场内外皆可) 的末快照, 新浪码升序; 未到的不收
  const Day *at(Ref r) const;                             // 无 (不在全市场 / 无成交 / 未合成) → nullptr
  const sina::Quote *quote(uint32_t id) const;            // 个股的末快照; 不在全市场 / 未到 → nullptr
  const sina::Quote *watched(std::string_view sym) const; // 订阅码的末快照; 未订阅 / 未到 → nullptr
};
// 账户的持仓 (AccountHub 给; 不可变, 持仓 / 成交一变才换): 券商实时持仓 (信用账户读到) 优先, 否则回放末日收后持仓 (Equity::hold);
//   净资产 net 计于各只的计价 px; 此刻 = net + 资产轴上有当日的各只 股 × (当日收 − px) (行情的账户当日 / Market 账户行同一份)
struct Holdings {
  bool broker = false;
  double net = 0;                                                      // > 0
  std::vector<Holding> list;                                           // 市值 (股 × 计价) 降序
  double amount = 0, buy = 0;                                          // 今日成交额 / 其中买入 [元] (券商成交; 回放 = 0)
  double weight(const Holding &h) const { return h.qty * h.px / net; } // 仓位 = 市值 / 净资产 (融资可合计超 1)
  double now(const Live *y) const;                                     // 此刻净资产; y 空 = net
  bool operator==(const Holdings &) const = default;
};

// 一个标的的源 (Joint 给): 日序列 = 日线 + 接得上的当日 (末项); 各处只经它读 (下标 / 分钟根 / 复权同一口径), 接续不再自判
struct Source {
  const Series *se = nullptr;    // 全史 (价格库留着的); nullptr = 无
  std::span<const Candle> daily; // 日线: 有全史 = se->daily, 否则个股的日脊末段 (Tails)
  const Day *today = nullptr;    // 当日 (有成交才有; 当日列不看接续)
  bool joined = false;           // 当日接得上 (= 日序列末项): 有日线 = 接在日脊之后, 无日线 = 有当日 (只画当日)
  bool any() const { return se || joined; }
  // 日序列 [0, size()): 日线各根 + (joined) 当日
  size_t size() const { return daily.size() + joined; }
  const Candle &candle(size_t i) const { return i < daily.size() ? daily[i] : today->candle; }
  const Candle *last() const { return size() ? &candle(size() - 1) : nullptr; } // 最新一根; 无 → nullptr
  size_t find(misc::Ymd d) const;                                               // d 在日序列的下标; 无 → size()
  // 第 i 项 (≥ size() = 无) 的分钟根 (当日有根 / 全史带分钟) 填进 b; 无 → false
  bool bars(size_t i, Bars &b) const;
  // 第 i 项的复权倍数 (价 × adj 跨日连续; 前复权到末项 = adj(i) / adj(size() − 1)): 全史 = Series::adj, 日脊末段自首项 1 起;
  //   其后逐项 × 上一项收 / 本项前收 (前收缺 → 不变; 接上的当日同此)
  double adj(size_t i) const;
  Flow flow_of(misc::Ymd d, bool ends) const; // d 日的资金流 (= 该项日根的 flow); 无 → amount 0
};
// 一侧行情的一份 (都不可变, 只拷指针): 历史 + 当日
struct Joint {
  Side side = kStock;
  std::shared_ptr<const PriceView> view; // 空 = 价格库未发布
  std::shared_ptr<const Live> live;      // 空 = 新浪未到
  const Tape *tape() const;              // 本侧的历史 (随 view 活); 未就绪 → nullptr
  // 本侧当日接在日脊之后 → 行情日, 否则 0 (篮子 / 账户另须本侧有当日)
  misc::Ymd today() const;
  Source of(uint32_t key) const; // 键集外 = 只日脊末段 (个股) + 当日
};

} // namespace GUI::Quote
