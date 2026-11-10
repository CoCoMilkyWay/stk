// 通达信适配: 客户端查过的各明文表 (依赖具体内存布局)
// 每表一份声明: 形状 (锚点 / 表头 / 行)、列、读法、索引, 四条轴的含义与流程见 Mem.hpp.
// 明文 GBK, \r\n 分行、| 分列, 首行表头 0|…|; 列号对照客户端各页表头核过, 列数含行尾空列.
// 列 1 / 2 对 ETF 是短码名 (如 5GETF), 全称在下一列, 各表名称一律取全称.
//
// 持仓      0||<n>||P00. 每查在网络缓冲多留一份 n = 行数的副本 (永不更新); 客户端在用的活动块 n = 0、全进程唯一,
//           新查询到来即释放旧块、另建新块, 总在同一个堆段里来回换址 (点「刷新」后约 100ms).
//           两种布局: 23 列完整视图 / 18 列精简视图 (重登后常见); 精简视图少的三列 (今买 / 今卖 / 融资) 对不上号, 宁缺.
// 成交      0|<日期 8 位><10 位>|<n>||P|. 每查在网络缓冲与客户端各留几份副本、旧的不即时释放、没有活动块标记;
//           成交只增不改, 当日全部副本取并集即全量. 无成交时只回 0||0||P| (不带日期, 认不出), 重查只能等一会儿全量读.
// 挂单      交易栏「撤单」页 (还挂着的单), 0||<n>||P|, 与融资负债同形、行首为时间可分.
// 融资负债  「融资负债汇总」, 0||<n>||P|.
// 信用资产  0||<n>||| (n = 19), 每行三组「名称|数值|」.
//           以上三表每查在新址留一两份、旧的不释放, 同一次应答各份逐字节相同, 表内没有新旧标记, 只认 ask 冒出的新块
//           (空表 0||0||P| 与无成交的成交表同形).
// 标的证券  「查询全部」后客户端分页收应答 (每页一块), 收齐另拼一整块: 表头 0||<总数>|||、其后恰总数行, 分页块行数不够.
// 担保品    同上: 分页块 0||<该页行数>||P<定位串>| (n > 0, 不认), 整块 0||0||P<末页定位串>|、不带总数,
//           收齐与否由调用方对页上条数.
//           两表整块可能留着几份 (每查全部一次一份); 代码跨市场会重 (沪市债券与深市基金同号), 以 (市场, 代码) 为键.
#pragma once

#include "live/Broker.hpp"
#include "live/tdx/Mem.hpp"

#include <array>
#include <map>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace live::tdx::tables {

using Fields = std::vector<std::string_view>;
using mem::Mode;
using mem::Shape;

namespace match {
bool positions_head(std::string_view l, std::string_view &tag, long &n);
bool positions_row(std::string_view l);
bool fills_head(std::string_view l, std::string_view &tag, long &n);
bool fills_row(std::string_view l);
bool pending_head(std::string_view l, std::string_view &tag, long &n); // 挂单 / 融资负债
bool orders_row(std::string_view l);
bool exch_row(std::string_view l);                                   // 融资负债 / 担保品: 市场标志 + 交易所 + 6 位代码
bool pairs_head(std::string_view l, std::string_view &tag, long &n); // 信用资产 / 标的证券
bool credit_row(std::string_view l);
bool margin_row(std::string_view l);
bool collat_head(std::string_view l, std::string_view &tag, long &n);
} // namespace match

struct Positions {
  using Row = Position;
  using Index = Keyed<Position>;
  static constexpr Shape kShape{"持仓", "||P00", 12, match::positions_head, match::positions_row, false, 64 * 1024}; // slack: 容块在两次读之间长大
  static constexpr Mode kMode = Mode::Live;
  static constexpr const char *kTag = "0";
  static constexpr std::array<int, 2> kViews{23, 18};
  static Row row(const Shape &t, const Fields &f, size_t view);
  static Index index(const Shape &t, const std::vector<Row> &rows);
};

struct Fills {
  using Row = Fill;
  using Index = Keyed<Flow>;
  static constexpr Shape kShape{"成交", "||P|\r\n", 40, match::fills_head, match::fills_row, false, 0};
  static constexpr Mode kMode = Mode::Union;
  static constexpr std::array<int, 1> kViews{17};
  static std::string tag(); // 今日 YYYYMMDD
  static std::tuple<std::string, std::string, std::string> uid(const Row &r) { return {r.fill_id, r.order_id, r.code}; }
  static std::pair<std::string, std::string> order(const Row &r) { return {r.time, r.fill_id}; }
  static Row row(const Shape &t, const Fields &f, size_t view);
  static Index index(const Shape &t, const std::vector<Row> &rows);
};

struct Orders {
  using Row = Pending;
  struct Index {};
  static constexpr Shape kShape{"挂单", "||P|\r\n", 12, match::pending_head, match::orders_row, true, 0};
  static constexpr Mode kMode = Mode::Ask;
  static constexpr std::array<int, 1> kViews{23};
  static Row row(const Shape &t, const Fields &f, size_t view);
  static Index index(const Shape &t, const std::vector<Row> &rows); // 委托编号不重
};

struct Debt : Sec {
  double balance = 0, qty = 0, amount = 0; // 持仓量 / 当前负债数量 [股] / 当前负债金额 [元]
  bool operator==(const Debt &) const = default;
};
struct Debts {
  using Row = Debt;
  using Index = std::map<std::string, Debt>;
  static constexpr Shape kShape{"融资负债", "||P|\r\n", 12, match::pending_head, match::exch_row, true, 0};
  static constexpr Mode kMode = Mode::Ask;
  static constexpr std::array<int, 1> kViews{15};
  static Row row(const Shape &t, const Fields &f, size_t view);
  static Index index(const Shape &t, const std::vector<Row> &rows);
};

struct Pairs {
  std::string k[3], v[3];
  bool operator==(const Pairs &) const = default;
};
struct Credit {
  using Row = Pairs;
  using Index = std::map<std::string, std::string>; // 名称 → 数值原文 (「无」「300.00%」等非数原样)
  static constexpr Shape kShape{"信用资产", "|||\r\n", 12, match::pairs_head, match::credit_row, false, 0};
  static constexpr Mode kMode = Mode::Ask;
  static constexpr std::array<int, 1> kViews{11};
  static Row row(const Shape &t, const Fields &f, size_t view);
  static Index index(const Shape &t, const std::vector<Row> &rows); // 名称不重
};

using MktCode = std::pair<Mkt, std::string>;
struct MarginRow : Sec {
  double ratio = 0;           // 融资保证金比例
  std::string remark, status; // 融资标的,融券标的 / 融资状态
  bool operator==(const MarginRow &) const = default;
};
struct Margin {
  using Row = MarginRow;
  using Index = std::map<MktCode, MarginRow>;
  static constexpr Shape kShape{"标的证券", "|||\r\n", 16, match::pairs_head, match::margin_row, false, 0};
  static constexpr Mode kMode = Mode::Whole;
  static constexpr std::array<int, 1> kViews{14};
  static Row row(const Shape &t, const Fields &f, size_t view);
  static Index index(const Shape &t, const std::vector<Row> &rows);
};

struct CollatRow : Sec {
  double rate = 0;            // 担保折算率
  std::string status, remark; // 担保状态
  bool operator==(const CollatRow &) const = default;
};
struct Collat {
  using Row = CollatRow;
  using Index = std::map<MktCode, CollatRow>;
  static constexpr Shape kShape{"担保品", "||0||P", 1, match::collat_head, match::exch_row, false, 0};
  static constexpr Mode kMode = Mode::Whole;
  static constexpr std::array<int, 1> kViews{15};
  static Row row(const Shape &t, const Fields &f, size_t view);
  static Index index(const Shape &t, const std::vector<Row> &rows);
};

} // namespace live::tdx::tables
