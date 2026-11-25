// 通达信适配: 各表 (请求层直发的应答) 并排声明: 列 / 索引 (含义见 Cols.hpp) + 行级校验 + 引导页; 末了三样组合查询 (读五样 / 两融条款 / 账本).
// 应答的列序 = 客户端功能定义里该功能号的出参序 (req::ask 按出参的字段号逐个取); 各列的字段名对协议字典核 (shim test fields).
// 「证券名称」对 ETF 是短码名 (如 5GETF), 各表名称一律取其后的「长简称」.
// 引导页 kOpen: 界面上查一次即让插件发出该功能 (直发照搬插件的入参, 每个客户端进程须先见过一次); 页签或功能树页 + 认页的列 (ui::open).
// 持仓两种布局: 功能 1228 (23 列) / 1114 (18 列, 重登后常见), 后者没有融资数量与担保折算率; 插件发哪个即读哪个.
// 标的证券 / 担保品单页至多七百余行: 按定位字串翻页到底 (req::ask).
#pragma once

#include "live/Broker.hpp"
#include "live/tdx/Cols.hpp"
#include "live/tdx/Req.hpp"
#include "live/tdx/Ui.hpp"

#include <algorithm>
#include <array>
#include <map>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace live::tdx::tables {

namespace as = cols::as;
using cols::col;
using cols::kNone;

struct Positions {
  using Row = Position;
  static constexpr const char *kName = "持仓";
  static constexpr ui::Open kOpen{"持仓", "可卖数量"};
  static constexpr std::array<int, 2> kFuncs{1228, 1114};
  static constexpr auto kCols = std::tuple{
      col<&Row::code, as::code>("证券代码", 0, 0),
      col<&Row::name, as::name>("长简称", 2, 2),
      col<&Row::balance, as::shares>("证券数量", 3, 3),
      col<&Row::available, as::shares>("可卖数量", 4, 4),
      col<&Row::traded, as::sides_i>("今买数量", 5, 5),
      col<&Row::financed, as::shares_or>("融资数量", 7, kNone),
      col<&Row::cost, as::num>("参考成本价", 8, 7),
      col<&Row::price, as::num>("当前价", 9, 8),
      col<&Row::market_value, as::num>("最新市值", 10, 9),
      col<&Row::pnl, as::num>("参考浮动盈亏", 11, 10),
      col<&Row::position_pct, as::num_or>("仓位(%)", 13, 12),
      col<&Row::mkt, as::flag>("账号类别", 14, 13),
      col<&Row::collateral, as::num_or>("担保折算率", 16, kNone),
  };
  using Index = cols::Unique<&Row::code>;
  static void check(const Row &r); // 市值 ≈ 现价 × 余额
};

struct Fills {
  using Row = Fill;
  static constexpr const char *kName = "成交";
  static constexpr ui::Open kOpen{"成交", "成交编号", "成交日期"};
  static constexpr std::array<int, 1> kFuncs{1108};
  static constexpr auto kCols = std::tuple{
      col<&Row::sec, as::clock>("成交时间", 0),
      col<&Row::code, as::code>("证券代码", 1),
      col<&Row::name, as::text>("长简称", 3),
      col<&Row::side, as::side>("买卖方向", 5), // 列 4 是买卖标志
      col<nullptr, as::is<"成交">>("状态说明", 6),
      col<&Row::price, as::num>("成交价格", 7),
      col<&Row::qty, as::shares>("成交数量", 8),
      col<&Row::amount, as::num>("成交金额", 9),
      col<&Row::fill_id, as::text>("成交编号", 10),
      col<&Row::order_id, as::text>("委托编号", 11),
      col<&Row::mkt, as::flag>("账号类别", 13), // 与列 12 股东代码同市场
  };
  using Index = cols::Tally;
  static void check(const Row &r); // 价 / 量为正、额 ≈ 价 × 量
};

struct Pendings {
  using Row = Pending;
  static constexpr const char *kName = "挂单";
  // 功能树「当日委托」页的表头 = 撤单页的 + 废单原因 (人点开过即一直留着, 只认「委托价格」即两页分不开、刷新失败)
  static constexpr ui::Open kOpen{"撤单", "委托价格", "废单原因"};
  static constexpr std::array<int, 1> kFuncs{1100};
  static constexpr auto kCols = std::tuple{
      col<&Row::sec, as::clock>("委托时间", 0),
      col<&Row::code, as::code>("证券代码", 1),
      col<&Row::name, as::text>("长简称", 3),
      col<&Row::side, as::side>("买卖方向", 5),   // 列 4 是买卖标志
      col<&Row::status, as::text>("状态说明", 7), // 已报 / 部成 / …
      col<&Row::price, as::num>("委托价格", 8),
      col<&Row::qty, as::shares>("委托数量", 9), // [股]
      col<&Row::order_id, as::text>("委托编号", 10),
      col<&Row::filled, as::shares>("成交数量", 12), // 已成交 [股]
      col<&Row::mkt, as::flag>("账号类别", 16),
  };
  using Index = cols::Unique<&Row::order_id>;
  static void check(const Row &r); // 状态 / 编号非空、0 ≤ 已成 < 委托量
};

struct Debts {
  using Row = Debt;
  static constexpr const char *kName = "融资负债";
  static constexpr ui::Open kOpen{"融资负债汇总", "当前负债金额"};
  static constexpr std::array<int, 1> kFuncs{3264};
  static constexpr auto kCols = std::tuple{
      col<&Row::mkt, as::flag>("账号类别", 0),
      col<&Row::code, as::code>("证券代码", 2),
      col<&Row::name, as::text>("长简称", 4),
      col<&Row::qty, as::shares>("当前负债数量", 7),
      col<&Row::amount, as::num>("当前负债金额", 9),
  };
  using Index = cols::Unique<&Row::code>;
  static void check(const Row &r); // 负债非负
};

struct Asset {
  std::array<std::pair<std::string, std::string>, 3> kv; // 三组 (名称, 数值原文)
};
struct Assets {
  using Row = Asset;
  static constexpr const char *kName = "信用资产";
  static constexpr ui::Open kOpen{"信用资产", "信用资金额度"};
  static constexpr std::array<int, 1> kFuncs{3200};
  static constexpr auto kCols = std::tuple{
      col<&Row::kv, as::groups>("信用资金额度", 0),
  };
  using Index = cols::Pairs;       // 「无」「300.00%」等非数原样
  static void check(const Row &r); // 首组名称非空
};

struct Margin : Sec {
  double ratio = 0;           // 融资保证金比例
  std::string remark, status; // 融资标的,融券标的 / 融资状态
};
struct Margins {
  using Row = Margin;
  static constexpr const char *kName = "标的证券";
  static constexpr ui::Open kOpen{"标的证券查询", "融资保证金比例"};
  static constexpr std::array<int, 1> kFuncs{3216};
  static constexpr auto kCols = std::tuple{
      col<&Row::code, as::code>("证券代码", 0),
      col<&Row::name, as::text>("长简称", 2),
      col<&Row::mkt, as::flag>("账号类别", 3),
      col<&Row::ratio, as::num>("融资保证金比例", 5),
      col<&Row::remark, as::text>("备注", 7),
      col<&Row::status, as::text>("融资状态", 10),
  };
  using Index = cols::Unique<&cols::sym>;
  static void check(const Row &r); // 保证金比例为正
};

struct Collat : Sec {
  double rate = 0;            // 担保折算率
  std::string status, remark; // 担保状态
};
struct Collats {
  using Row = Collat;
  static constexpr const char *kName = "担保品";
  static constexpr ui::Open kOpen{"担保品查询", "担保状态"};
  static constexpr std::array<int, 1> kFuncs{3220};
  static constexpr auto kCols = std::tuple{
      col<&Row::mkt, as::flag>("账号类别", 0),
      col<&Row::code, as::code>("证券代码", 2),
      col<&Row::name, as::text>("长简称", 4),
      col<&Row::rate, as::num>("担保折算率", 5),
      col<&Row::status, as::text>("担保状态", 6),
      col<&Row::remark, as::text>("备注", 12),
  };
  using Index = cols::Unique<&cols::sym>;
  static void check(const Row &r); // 折算率在 [0, 1]
};

struct CashFlows {
  using Row = CashFlow;
  static constexpr const char *kName = "资金流水";
  static constexpr ui::Open kOpen{"资金流水", "剩余金额"};
  static constexpr std::array<int, 1> kFuncs{1118};
  static constexpr auto kCols = std::tuple{
      col<&Row::date, as::ymd>("成交日期", 0),
      col<&Row::code, as::text>("证券代码", 1),
      col<&Row::name, as::text>("长简称", 3),
      col<&Row::price, as::num>("成交价格", 4),
      col<&Row::qty, as::num>("成交数量", 5),
      col<&Row::kind, as::name>("业务名称", 6),
      col<&Row::amount, as::num>("发生金额", 7),
      col<&Row::balance, as::num>("剩余金额", 8),
  };
  using Index = cols::Plain;
  static void check(const Row &r); // 带股数的行有代码
};

// 页上的表头没有买卖标志、账号类别两列 (应答里有); 认页: 资金流水 / 交割单也有成交日期, 以发生金额分开
struct Deals {
  using Row = Deal;
  static constexpr const char *kName = "历史成交";
  static constexpr ui::Open kOpen{"历史成交", "成交日期", "发生金额"};
  static constexpr std::array<int, 1> kFuncs{1110};
  static constexpr auto kCols = std::tuple{
      col<&Row::date, as::ymd>("成交日期", 0),
      col<&Row::sec, as::clock>("成交时间", 1),
      col<&Row::code, as::code>("证券代码", 2),
      col<&Row::name, as::text>("长简称", 4),
      col<&Row::side, as::side>("买卖方向", 6), // 列 5 是买卖标志 (0 / 1, 只分买卖)
      col<&Row::price, as::num>("成交价格", 7),
      col<&Row::qty, as::shares>("成交数量", 8),
      col<&Row::amount, as::num>("成交金额", 9),
      col<&Row::fill_id, as::text>("成交编号", 10),
      col<&Row::order_id, as::text>("委托编号", 11),
      col<&Row::mkt, as::flag>("账号类别", 13),
  };
  using Index = cols::Plain;
  // 同成交, 但卖出的成交数量为负 (组合查询里取绝对值)、早些的行成交金额为 0 (组合查询里补成价 × 量);
  //   不是成交的行不核, 组合查询里剔掉: 买卖标志不分业务, 只能按量价与时刻认 — 费用一类的记账量 0, 申购配号价 0,
  //   担保品划入 / 基金申购确认 / 新股申购与入账 / 指定交易记在盘后或半夜 (15:00:02 / 16:00:00 / 18:… / 00:00:0x),
  //   成交在 09:15 ~ 15:00:01 (收盘集合竞价的有记作 15:00:01 的)
  static void check(const Row &r);
  static bool dealt(const Row &r) { return r.qty != 0 && r.price > 0 && kOpenSec <= r.sec && r.sec <= kCloseSec; }
  static constexpr int kOpenSec = 9 * 3600 + 15 * 60, kCloseSec = 15 * 3600 + 1;
};

// 各表按 Driver 的用处分三组: 读五样 (序同 Snapshot) / 拉两融条款 / 拉账本 (序同 Ledger)
inline constexpr std::tuple<Fills, Positions, Pendings, Debts, Assets> kReads{};
inline constexpr std::tuple<Margins, Collats> kTerms{};
inline constexpr std::tuple<CashFlows, Deals> kLedger{};
inline constexpr auto kAll = std::tuple_cat(kReads, kTerms, kLedger);

// S 的各功能号里插件发过的那个 (直发有入参可照搬); 都没发过 → 0
template <class S>
int seen_func() {
  const auto it = std::find_if(S::kFuncs.begin(), S::kFuncs.end(), [](int f) { return req::seen(f); });
  return it == S::kFuncs.end() ? 0 : *it;
}
// 直发 S 用的功能号; 插件都没发过 → Fail
template <class S>
int func_of() {
  const int f = seen_func<S>();
  need(f != 0, "%s: 插件还没发过 (先经界面查一次), 入参无从照搬", S::kName);
  return f;
}

// 直发读表 S 的各行: 功能号取插件发过的那个; over 覆盖入参 (字段号 → 值)
template <class S>
std::vector<typename S::Row> rows(const std::map<int, std::string> &over = {}) {
  const int f = func_of<S>();
  const req::Answer a = std::move(req::ask({req::Ask{f, over}})[0]);
  return cols::parse<S>(f, a.cols, a.rows);
}

// 直发读表 S → 其索引 (S::Index)
template <class S>
auto read(const std::map<int, std::string> &over = {}) {
  return S::Index::build(S::kName, rows<S>(over));
}

// 直发读一组表 S… (一并发出、同时在途, 分页的各自翻到底) → 各自的索引, 序同 S…
template <class... S>
auto read(std::tuple<S...>) {
  const std::array<int, sizeof...(S)> f{func_of<S>()...};
  std::vector<req::Ask> asks;
  for (const int x : f)
    asks.push_back({x, {}});
  const std::vector<req::Answer> a = req::ask(std::move(asks));
  return [&]<size_t... I>(std::index_sequence<I...>) {
    return std::tuple{S::Index::build(S::kName, cols::parse<S>(f[I], a[I].cols, a[I].rows))...};
  }(std::index_sequence_for<S...>{});
}

// 组合查询 (各表须插件已发过, Driver 先经界面引导):
//   读五样 = 成交 / 持仓 / 挂单 / 融资负债 / 信用资产五表一趟直发; 信用账户取自信用资产的各项「名称 → 数值」
//   两融条款 = 标的证券 × 担保品两表一并各翻页到底 (担保品近万只, 约十余页), 以 (市场, 代码) 为键, 合成时按代码归一 (跨市场同号的只留 market_of 那只)
//   账本 = 资金流水与历史成交 from 日起 (含) 至昨日 (客户端只给到上一日), 同一对起止日入参按窗 (客户端一次至多查 30 天) 直发, 各窗应答按日接起;
//     from = 0 即资金流水从昨日往前逐窗查到连续几窗无流水 (开户前), 历史成交自其首日起
Snapshot reads();
Terms terms();
Ledger ledger(misc::Ymd from);

} // namespace live::tdx::tables
