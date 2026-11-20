// 通达信适配: 8 张应答表并排声明 + 组合查询 (信用账户 / 两融条款 / 资金流水)
//   列序 = 协议字典出参序 (各列字段名由 shim test fields 逐列核); 无魔法列数, 列越界即时报错.
//   引导页 kOpen: 首次经界面查一次让插件发该功能, 之后一律直发.
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
  static void check(const Row &r);
};

struct Fills {
  using Row = Fill;
  static constexpr const char *kName = "成交";
  static constexpr ui::Open kOpen{"成交", "成交编号"};
  static constexpr std::array<int, 1> kFuncs{1108};
  static constexpr auto kCols = std::tuple{
      col<&Row::sec, as::clock>("成交时间", 0),
      col<&Row::code, as::code>("证券代码", 1),
      col<&Row::name, as::text>("长简称", 3),
      col<&Row::side, as::side>("买卖方向", 5),
      col<nullptr, as::is<"成交">>("状态说明", 6),
      col<&Row::price, as::num>("成交价格", 7),
      col<&Row::qty, as::shares>("成交数量", 8),
      col<&Row::amount, as::num>("成交金额", 9),
      col<&Row::fill_id, as::text>("成交编号", 10),
      col<&Row::order_id, as::text>("委托编号", 11),
      col<&Row::mkt, as::flag>("账号类别", 13),
  };
  using Index = cols::Tally;
  static void check(const Row &r);
};

struct Pendings {
  using Row = Pending;
  static constexpr const char *kName = "挂单";
  static constexpr ui::Open kOpen{"撤单", "委托价格", "废单原因"};
  static constexpr std::array<int, 1> kFuncs{1100};
  static constexpr auto kCols = std::tuple{
      col<&Row::sec, as::clock>("委托时间", 0),
      col<&Row::code, as::code>("证券代码", 1),
      col<&Row::name, as::text>("长简称", 3),
      col<&Row::side, as::side>("买卖方向", 5),
      col<&Row::kind, as::text>("委托类别", 6),
      col<&Row::status, as::text>("状态说明", 7),
      col<&Row::price, as::num>("委托价格", 8),
      col<&Row::qty, as::shares>("委托数量", 9),
      col<&Row::order_id, as::text>("委托编号", 10),
      col<&Row::filled, as::shares>("成交数量", 12),
      col<&Row::mkt, as::flag>("账号类别", 16),
  };
  using Index = cols::Unique<&Row::order_id>;
  static void check(const Row &r);
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
  static void check(const Row &r);
};

struct Asset {
  std::array<std::pair<std::string, std::string>, 3> kv;
  bool operator==(const Asset &) const = default;
};
struct Assets {
  using Row = Asset;
  static constexpr const char *kName = "信用资产";
  static constexpr ui::Open kOpen{"信用资产", "信用资金额度"};
  static constexpr std::array<int, 1> kFuncs{3200};
  static constexpr auto kCols = std::tuple{
      col<&Row::kv, as::groups>("信用资金额度", 0),
  };
  using Index = cols::Pairs;
  static void check(const Row &r);
};

struct Margin : Sec {
  double ratio = 0;
  std::string remark, status;
  bool operator==(const Margin &) const = default;
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
  static void check(const Row &r);
};

struct Collat : Sec {
  double rate = 0;
  std::string status, remark;
  bool operator==(const Collat &) const = default;
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
  static void check(const Row &r);
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
  static void check(const Row &r);
};

// 按 Driver 用处分三组: 四读 / 两融条款 / 资金流水
inline constexpr std::tuple<Fills, Positions, Pendings, Assets, Debts> kReads{};
inline constexpr std::tuple<Margins, Collats> kTerms{};
inline constexpr std::tuple<CashFlows> kFlows{};
inline constexpr auto kAll = std::tuple_cat(kReads, kTerms, kFlows);

template <class S>
int seen_func() {
  const auto it = std::find_if(S::kFuncs.begin(), S::kFuncs.end(), [](int f) { return req::seen(f); });
  return it == S::kFuncs.end() ? 0 : *it;
}

template <class S>
std::vector<typename S::Row> rows(const std::map<int, std::string> &over = {}) {
  const int f = seen_func<S>();
  need(f != 0, strf("%s: 插件还没发过 (先经界面查一次), 入参无从照搬", S::kName));
  const req::Answer a = req::ask(f, over);
  return cols::parse<S>(f, a.cols, a.rows);
}

template <class S>
auto read(const std::map<int, std::string> &over = {}) {
  return S::Index::build(S::kName, rows<S>(over));
}

// 组合查询 (均须对应表已引导过)
Account read_acct();
Terms read_terms();
std::vector<CashFlow> read_flows(misc::Ymd from);

} // namespace live::tdx::tables
