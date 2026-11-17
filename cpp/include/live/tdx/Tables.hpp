// 通达信适配: 客户端查过的各明文表 (依赖具体内存布局). 每表一份声明: 形状 / 列 / 读法 / 索引四轴 (含义与流程见 Mem.hpp) + 行级校验.
// 明文表即请求层应答原样: 列序 = 客户端功能定义里该功能号的出参序, 行尾多一空列; 各列的字段名对协议字典核 (shim test fields).
// 「证券名称」对 ETF 是短码名 (如 5GETF), 各表名称一律取其后的「长简称」.
//
// 持仓      Live   0||<n>||P00. 每查在网络缓冲多留一份 n = 行数的副本 (永不更新); 客户端在用的活动块 n = 0、全进程唯一,
//                  新查询到来即释放旧块、另建新块, 总在同一个堆段里来回换址 (切页后首个应答到时).
//                  两种布局: 23 列 (功能 1228) / 18 列 (功能 1114, 重登后常见), 后者没有融资数量与担保折算率.
// 成交      Union  0|<日期 8 位><10 位>|<n>||P|. 每查在网络缓冲与客户端各留几份副本、旧的不即时释放、没有活动块标记;
//                  成交只增不改, 当日全部副本取并集即全量. 无成交时只回 0||0||P| (不带日期, 认不出), 并集为空、页上 0 条.
// 挂单      Ask    交易栏「撤单」页 (还挂着的单), 0||<n>||P|, 与融资负债同形、行首为时间可分.
// 融资负债  Ask    「融资负债汇总」, 0||<n>||P|.
// 信用资产  Ask    0||<n>||| (n = 19), 每行三组「名称|数值|」.
//                  以上三表每查在新址留一两份、旧的不释放, 同一次应答各份逐字节相同, 表内没有新旧标记, 只认 act 后冒出的新块
//                  (空表 0||0||P| 与无成交的成交表同形, 落回同址认不出: 查完页上 0 条即空表).
// 标的证券  Whole  「查询全部」后客户端分页收应答 (每页一块), 收齐另拼一整块: 表头 0||<总数>|||、其后恰总数行, 分页块行数不够.
// 担保品    Whole  同上: 分页块 0||<该页行数>||P<定位串>| (n > 0, 不认), 整块 0||0||P<末页定位串>|、不带总数,
//                  查完后整块行数须恰为页上条数.
//                  两表整块可能留着几份 (每查全部一次一份); 代码跨市场会重 (沪市债券与深市基金同号), 以 (市场, 代码) 为键.
// 资金流水  Ask    「资金流水」页按起止日查, 0||<n>||P|, 行首为 8 位日期; 一次只许查 30 天. 空应答认不出, 查完页上 0 条即空.
#pragma once

#include "live/Broker.hpp"
#include "live/tdx/Mem.hpp"

#include <array>
#include <string>
#include <tuple>
#include <utility>

namespace live::tdx::tables {

namespace as = mem::as;
using mem::col;
using mem::kNone;
using mem::Shape;

struct Positions {
  using Row = Position;
  static constexpr Shape kShape{"持仓", "0||{t}||P00...", "{code}|*|*|*|*|...", false, 64 * 1024}; // slack: 容块在两次读之间长大
  using Mode = mem::Live;
  static constexpr const char *kTag = "0";
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
  static constexpr Shape kShape{"成交", "0|{t8}{d10}|{n}||P|", "{clock}|{code}|...||"};
  using Mode = mem::Union;
  static std::string tag(); // 今日 YYYYMMDD
  static std::tuple<std::string, std::string, std::string> uid(const Row &r) { return {r.fill_id, r.order_id, r.code}; }
  static std::pair<std::string, std::string> order(const Row &r) { return {r.time, r.fill_id}; }
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
  static constexpr Shape kShape{"挂单", "0||{n}||P|", "{clock}|{code}|...", true};
  using Mode = mem::Ask;
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
  static constexpr Shape kShape{"融资负债", "0||{n}||P|", "{d}|*|{code}|...", true};
  using Mode = mem::Ask;
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
  static constexpr Shape kShape{"信用资产", "0||{n}|||", "{name}|..."};
  using Mode = mem::Ask;
  static constexpr std::array<int, 1> kViews{11}; // 出参 9 个, 明文表行尾多两空列
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
  static constexpr Shape kShape{"标的证券", "0||{n}|||", "{code}|..."};
  using Mode = mem::Whole;
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
  static constexpr Shape kShape{"担保品", "0||0||P{t}|", "{d}|*|{code}|..."};
  using Mode = mem::Whole;
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
  static constexpr Shape kShape{"资金流水", "0||{n}||P|", "{d8}|..."};
  using Mode = mem::Ask;
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

} // namespace live::tdx::tables
