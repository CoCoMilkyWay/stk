// 通达信适配: 各表 (请求层直发的应答) 并排声明: 列 / 索引 (含义见 Mem.hpp) + 行级校验 + 引导页.
// 应答的列序 = 客户端功能定义里该功能号的出参序; 布局列数 = 出参数 + 1; 各列的字段名对协议字典核 (shim test fields).
// 「证券名称」对 ETF 是短码名 (如 5GETF), 各表名称一律取其后的「长简称」.
// 引导页 kOpen: 界面上查一次即让插件发出该功能 (直发照搬插件的入参, 每个客户端进程须先见过一次); col 空 = 交易栏页签, 否则功能树页 + 认页的列.
// 持仓两种布局: 23 列 (功能 1228) / 18 列 (功能 1114, 重登后常见), 后者没有融资数量与担保折算率; 插件发哪个即读哪个.
// 标的证券 / 担保品单页至多七百余行: 按定位字串翻页到底 (req::ask_all).
#pragma once

#include "live/Broker.hpp"
#include "live/tdx/Mem.hpp"
#include "live/tdx/Req.hpp"

#include <algorithm>
#include <array>
#include <map>
#include <string>
#include <tuple>
#include <utility>

namespace live::tdx::tables {

namespace as = mem::as;
using mem::col;
using mem::kNone;
using mem::Shape;

struct Open {
  const char *page, *col; // col 空 = 交易栏页签
};

struct Positions {
  using Row = Position;
  static constexpr Shape kShape{"持仓"};
  static constexpr Open kOpen{"持仓", nullptr};
  static constexpr std::array<int, 2> kViews{23, 18};
  static constexpr std::array<int, 2> kFuncs{1228, 1114};
  static constexpr auto kCols = std::tuple{
      col<&Row::code, as::code>("证券代码", 0, 0),
      col<&Row::name, as::name>("长简称", 2, 2),
      col<&Row::balance, as::num>("证券数量", 3, 3),
      col<&Row::available, as::num>("可卖数量", 4, 4),
      col<&Row::traded, as::sides>("今买数量", 5, 5),
      col<&Row::financed, as::num_or>("融资数量", 7, kNone),
      col<&Row::cost, as::num>("参考成本价", 8, 7),
      col<&Row::price, as::num>("当前价", 9, 8),
      col<&Row::market_value, as::num>("最新市值", 10, 9),
      col<&Row::pnl, as::num>("参考浮动盈亏", 11, 10),
      col<&Row::position_pct, as::num_or>("仓位(%)", 13, 12),
      col<&Row::mkt, as::flag>("账号类别", 14, 13),
      col<&Row::collateral, as::num_or>("担保折算率", 16, kNone),
  };
  using Index = mem::Unique<&Row::code>;
  static void check(const Shape &t, const Row &r); // 市值 ≈ 现价 × 余额
};

struct Fills {
  using Row = Fill;
  static constexpr Shape kShape{"成交"};
  static constexpr Open kOpen{"成交", nullptr};
  static constexpr std::array<int, 1> kViews{17};
  static constexpr std::array<int, 1> kFuncs{1108};
  static constexpr auto kCols = std::tuple{
      col<&Row::time, as::clock>("成交时间", 0),
      col<&Row::code, as::code>("证券代码", 1),
      col<&Row::name, as::text>("长简称", 3),
      col<&Row::side, as::side>("买卖方向", 5), // 列 4 是买卖标志
      col<nullptr, as::is<"成交">>("状态说明", 6),
      col<&Row::price, as::num>("成交价格", 7),
      col<&Row::qty, as::num>("成交数量", 8),
      col<&Row::amount, as::num>("成交金额", 9),
      col<&Row::fill_id, as::text>("成交编号", 10),
      col<&Row::order_id, as::text>("委托编号", 11),
      col<&Row::mkt, as::flag>("账号类别", 13), // 与列 12 股东代码同市场
  };
  using Index = mem::Tally;
  static void check(const Shape &t, const Row &r); // 价 / 量为正、额 ≈ 价 × 量
};

struct Pendings {
  using Row = Pending;
  static constexpr Shape kShape{"挂单"};
  static constexpr Open kOpen{"撤单", nullptr};
  static constexpr std::array<int, 1> kViews{23};
  static constexpr std::array<int, 1> kFuncs{1100};
  static constexpr auto kCols = std::tuple{
      col<&Row::time, as::clock>("委托时间", 0),
      col<&Row::code, as::code>("证券代码", 1),
      col<&Row::name, as::text>("长简称", 3),
      col<&Row::side, as::side>("买卖方向", 5),   // 列 4 是买卖标志
      col<&Row::kind, as::text>("委托类别", 6),   // 信用交易 / …
      col<&Row::status, as::text>("状态说明", 7), // 已报 / 部成 / …
      col<&Row::price, as::num>("委托价格", 8),
      col<&Row::qty, as::num>("委托数量", 9), // [股]
      col<&Row::order_id, as::text>("委托编号", 10),
      col<&Row::filled, as::num>("成交数量", 12), // 已成交 [股]
      col<&Row::mkt, as::flag>("账号类别", 16),
  };
  using Index = mem::Unique<&Row::order_id>;
  static void check(const Shape &t, const Row &r); // 状态 / 编号非空、0 ≤ 已成 < 委托量
};

struct Debt : Sec {
  double balance = 0, qty = 0, amount = 0; // 持仓量 / 当前负债数量 [股] / 当前负债金额 [元]
  bool operator==(const Debt &) const = default;
};
struct Debts {
  using Row = Debt;
  static constexpr Shape kShape{"融资负债"};
  static constexpr Open kOpen{"融资负债汇总", "当前负债金额"};
  static constexpr std::array<int, 1> kViews{15};
  static constexpr std::array<int, 1> kFuncs{3264};
  static constexpr auto kCols = std::tuple{
      col<&Row::mkt, as::flag>("账号类别", 0),
      col<&Row::code, as::code>("证券代码", 2),
      col<&Row::name, as::text>("长简称", 4),
      col<&Row::balance, as::num>("持仓量", 5),
      col<&Row::qty, as::num>("当前负债数量", 7),
      col<&Row::amount, as::num>("当前负债金额", 9),
  };
  using Index = mem::Unique<&Row::code>;
  static void check(const Shape &t, const Row &r); // 负债非负
};

struct Asset {
  std::array<std::pair<std::string, std::string>, 3> kv; // 三组 (名称, 数值原文)
  bool operator==(const Asset &) const = default;
};
struct Assets {
  using Row = Asset;
  static constexpr Shape kShape{"信用资产"};
  static constexpr Open kOpen{"信用资产", "信用资金额度"};
  static constexpr std::array<int, 1> kViews{11};
  static constexpr std::array<int, 1> kFuncs{3200};
  static constexpr auto kCols = std::tuple{
      col<&Row::kv, as::groups>("信用资金额度", 0),
  };
  using Index = mem::Pairs;                        // 「无」「300.00%」等非数原样
  static void check(const Shape &t, const Row &r); // 首组名称非空
};

struct Margin : Sec {
  double ratio = 0;           // 融资保证金比例
  std::string remark, status; // 融资标的,融券标的 / 融资状态
  bool operator==(const Margin &) const = default;
};
struct Margins {
  using Row = Margin;
  static constexpr Shape kShape{"标的证券"};
  static constexpr Open kOpen{"标的证券查询", "融资保证金比例"};
  static constexpr std::array<int, 1> kViews{14};
  static constexpr std::array<int, 1> kFuncs{3216};
  static constexpr auto kCols = std::tuple{
      col<&Row::code, as::code>("证券代码", 0),
      col<&Row::name, as::text>("长简称", 2),
      col<&Row::mkt, as::flag>("账号类别", 3),
      col<&Row::ratio, as::num>("融资保证金比例", 5),
      col<&Row::remark, as::text>("备注", 7),
      col<&Row::status, as::text>("融资状态", 10),
  };
  using Index = mem::Unique<&mem::sym>;
  static void check(const Shape &t, const Row &r); // 保证金比例为正
};

struct Collat : Sec {
  double rate = 0;            // 担保折算率
  std::string status, remark; // 担保状态
  bool operator==(const Collat &) const = default;
};
struct Collats {
  using Row = Collat;
  static constexpr Shape kShape{"担保品"};
  static constexpr Open kOpen{"担保品查询", "担保状态"};
  static constexpr std::array<int, 1> kViews{15};
  static constexpr std::array<int, 1> kFuncs{3220};
  static constexpr auto kCols = std::tuple{
      col<&Row::mkt, as::flag>("账号类别", 0),
      col<&Row::code, as::code>("证券代码", 2),
      col<&Row::name, as::text>("长简称", 4),
      col<&Row::rate, as::num>("担保折算率", 5),
      col<&Row::status, as::text>("担保状态", 6),
      col<&Row::remark, as::text>("备注", 12),
  };
  using Index = mem::Unique<&mem::sym>;
  static void check(const Shape &t, const Row &r); // 折算率在 [0, 1]
};

struct CashFlows {
  using Row = CashFlow;
  static constexpr Shape kShape{"资金流水"};
  static constexpr Open kOpen{"资金流水", "剩余金额"};
  static constexpr std::array<int, 1> kViews{14};
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
  using Index = mem::Plain;
  static void check(const Shape &t, const Row &r); // 带股数的行有代码
};

// 插件发过 S 的功能 (直发有入参可照搬)
template <class S>
bool seen() {
  return std::any_of(S::kFuncs.begin(), S::kFuncs.end(), req::seen);
}

// 直发读表 S: 功能号取 kFuncs 里插件发过的那个, 带定位字串的功能翻页到底; over 覆盖入参 (字段号 → 值)
template <class S>
std::vector<typename S::Row> read(const std::map<int, std::string> &over = {}) {
  const auto f = std::find_if(S::kFuncs.begin(), S::kFuncs.end(), req::seen);
  need(f != S::kFuncs.end(), strf("%s: 插件还没发过 (先经界面查一次), 入参无从照搬", S::kShape.name));
  const req::Answer a = req::ask_all(*f, over);
  return mem::parse<S>(*f, a.cols, a.rows);
}

} // namespace live::tdx::tables
