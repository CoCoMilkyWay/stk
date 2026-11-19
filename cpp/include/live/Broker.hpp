// 实盘: 券商无关的一层. 各客户端适配 (live/<客户端>/) 实现 Driver 并登记进 live/Clients.hpp, 上层 (gui/task_live) 只认这里的类型与 Broker
//   Driver (一个客户端一份): 同步, 调用线程跑完; 刷新线程与事务线程会同时调, 自身线程安全
//     接: connect 接上客户端 (换了进程即重接, 接上即可读) → 是谁
//     读: fills / positions / pendings / account 四样同形, 一轮按此序
//     拉: terms / flows 两样同形 (慢, 事务里跑, 进度经 Report)
//     写: place 一张限价单 (逐步报进度, steps 即各步名)
//   Broker (券商无关, 只此一份): 包一个 Driver + 刷新线程; 读由刷新线程发布, 拉 / 写由调用线程跑 (段 + 接上 + Driver), 两类同一个接入
//     读: 一轮四样逐样读进底稿, 轮末 (成败都) 整份发布 (State, 不可变; 指针换了 = 换了一份), 上层不见半轮; 有人在看 (touch) 才转
//       起因: request 要一轮 (不等) / sync 要一轮并等完 / 有人在看而还没刷过或距上轮满 every 秒 /
//       下单成功后隔几秒 (限价单可能稍后才成交). 失败不自动重试, 等下一个起因
//       hush (批量下单期间不扫客户端): 只认 request / sync, 不自起、不排下单后的
//   客户端状态与预期不符 (界面没弹出 / 应答不认识 / 回报失败) 是外部结局, 不是本程序的错: fail 先在当时的段下记一行 (live/Trace.hpp)
//   再抛 Fail, 在各轮 / 各事务的边界收成原因文本; 本程序自身的不变式一律 assert
// 编码: 方向 买 +1 / 卖 −1 (BySide 下标 s < 0); 市场由代码前缀定 (market_of), 新浪码 = 前缀 + 代码, AssetInfo 码 = 代码 + 后缀
#pragma once

#include "misc/date.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace live {

struct Fail {
  std::string why;
};
[[noreturn]] void fail(std::string why);
inline void need(bool ok, const char *why) {
  if (!ok)
    fail(why);
}
inline void need(bool ok, const std::string &why) {
  if (!ok)
    fail(why);
}
std::string strf(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
std::string clock_now(); // 本地时刻 HH:MM:SS

inline constexpr int kBuy = 1, kSell = -1;
inline constexpr int kSides[2] = {kBuy, kSell};
inline const char *side_name(int s) { return s > 0 ? "买入" : "卖出"; }

struct BySide {
  double v[2] = {0, 0};
  double &operator[](int s) { return v[s < 0]; }
  double operator[](int s) const { return v[s < 0]; }
  bool operator==(const BySide &) const = default;
};

enum class Mkt : uint8_t { SZ,
                           SH,
                           BJ };
inline constexpr const char *kMktPrefix[3] = {"sz", "sh", "bj"};                   // 新浪
inline constexpr const char *kMktSuffix[3] = {".SZ", ".SH", ".BJ"};                // AssetInfo::codes
Mkt market_of(std::string_view code);                                              // 北 4 / 8 / 92 开头, 沪 5 / 6 / 9, 其余深
inline const char *mkt_name(Mkt m) { return kMktSuffix[static_cast<int>(m)] + 1; } // SZ / SH / BJ
Mkt mkt_named(std::string_view s);                                                 // mkt_name 的逆
inline std::string sina_sym(Mkt m, const std::string &code) { return kMktPrefix[static_cast<int>(m)] + code; }

struct Sec {
  std::string code, name;
  Mkt mkt = Mkt::SZ;
  bool operator==(const Sec &) const = default;
};

// 一只持仓 (券商口径); financed / collateral 精简视图没有
struct Position : Sec {
  double balance = 0, available = 0;                     // 证券数量 / 可卖 [股]
  BySide traded;                                         // 今买 / 今卖 [股], 与成交合计同形
  std::optional<double> financed;                        // 融资数量 [股]
  double cost = 0, price = 0, market_value = 0, pnl = 0; // 参考成本价 / 现价 / 市值 / 参考浮动盈亏 (券商口径, ≠ (现价 − 成本价) × 量)
  std::optional<double> position_pct;                    // 仓位 [%]
  std::optional<double> collateral;                      // 担保折算率
  bool operator==(const Position &) const = default;
};
struct Fill : Sec {
  std::string time; // HH:MM:SS
  int side = kBuy;
  double price = 0, qty = 0, amount = 0; // 价 / 股 / 元
  std::string fill_id, order_id;
  bool operator==(const Fill &) const = default;
};
// 还挂着的委托
struct Pending : Sec {
  std::string time;
  int side = kBuy;
  std::string kind, status; // 委托类别 / 已报 · 部成 …
  double price = 0, qty = 0;
  std::string order_id;
  double filled = 0; // 已成 [股]
  bool operator==(const Pending &) const = default;
};
// 一只当日成交的合计: {方向: Σ 股} / {方向: Σ 元}
struct Traded : Sec {
  BySide qty, amount;
  bool operator==(const Traded &) const = default;
};
// 一只融资负债
struct Debt : Sec {
  double qty = 0, amount = 0; // 负债数量 [股] / 负债金额 [元]
  bool operator==(const Debt &) const = default;
};

// 按键索引的表 (键多为代码): list 首见序, at = 键 → 下标
template <class T>
struct Keyed {
  std::vector<T> list;
  std::unordered_map<std::string, size_t> at;
  const T *find(const std::string &key) const {
    const auto i = at.find(key);
    return i == at.end() ? nullptr : &list[i->second];
  }
  bool operator==(const Keyed &o) const { return list == o.list; }
};

struct Fills {
  std::vector<Fill> rows; // 时间序
  Keyed<Traded> by_code;
};
// 持仓 h 的今买 / 今卖与成交合计 f (空 = 今日没有成交) 相符; 余额为 0 的不核. 不符 = 两问之间又有成交, 其一旧了
bool synced(const Position &h, const Traded *f);

// 信用账户 (读时)
struct Account {
  double assets = 0, line = 0, debt = 0, net = 0; // 担保资产 / 可融额度余量 (最高融资额度 − 融资负债含费用) / 负债总额 / 净资产
  double avail = 0, cash = 0;                     // 可用保证金 / 现金
  double rate = 0;                                // 融资年利率
  Keyed<Debt> debts;                              // 按代码
  std::shared_ptr<const Keyed<Position>> held;    // 读时的持仓 (同一轮刚读的那份; Broker 填)
};

// 两融条款 (一只)
struct Term {
  std::string name;
  Mkt mkt = Mkt::SZ;
  std::optional<double> ratio; // 融资保证金比例; 不在标的表 = 无
  bool financeable = false;    // 融资标的且融资状态正常
  double rate = 0;             // 担保折算率; 不在担保品表 = 0
  bool pledgeable = false;     // 担保状态正常 (可作担保品买入)
};
using Terms = std::map<std::string, Term>;

// 资金流水一行 (券商口径, 截至上一交易日): 日期升序、同日按发生序; balance = 记账后的资金余额,
//   发生金额 0 的行 (申购配号等) 不动资金、其 balance 不算数; 股数为本行带的证券变动 (买卖 / 划入 / 入账等, 无 = 0)
struct CashFlow {
  misc::Ymd date = 0;
  std::string code, name; // 无证券的行 (利息 / 转账) 为空
  double price = 0, qty = 0;
  std::string kind; // 业务名称
  double amount = 0, balance = 0;
  bool operator==(const CashFlow &) const = default;
};
// 读到的一样: v 值 (nullptr = 还没读过), at 读于 HH:MM:SS
template <class T>
struct Got {
  std::shared_ptr<const T> v;
  std::string at;
  explicit operator bool() const { return v != nullptr; }
  const T &operator*() const { return *v; }
  const T *operator->() const { return v.get(); }
};

// 客户端读到的一份; 各样按一轮的刷新序
struct State {
  std::string who; // 客户端 (进程)
  Got<Fills> fills;
  Got<Keyed<Position>> pos;
  Got<Keyed<Pending>> orders;
  Got<Account> acct;

  static const char *name(Got<Fills> State::*) { return "成交"; }
  static const char *name(Got<Keyed<Position>> State::*) { return "持仓"; }
  static const char *name(Got<Keyed<Pending>> State::*) { return "挂单"; }
  static const char *name(Got<Account> State::*) { return "信用账户"; }
  // f(名称, 读于) 逐样
  template <class F>
  void each(F &&f) const {
    f(name(&State::fills), fills.at);
    f(name(&State::pos), pos.at);
    f(name(&State::orders), orders.at);
    f(name(&State::acct), acct.at);
  }
};

// 刷新进度
struct Refresh {
  std::string step; // 正做的 (空 = 闲)
  std::string err;  // 上一轮失败原因
  std::string down; // 上一轮接不上客户端的原因 (空 = 在线)
  double every = 0; // 定时周期 [s], 0 = 不定时
};

enum class Credit : uint8_t { Plain,    // 普通 (担保品买卖)
                              Margin }; // 融资买入
inline const char *credit_name(Credit c) { return c == Credit::Margin ? "融资" : "普通"; }

// 一张限价单: 量为 qty 股或 size 元 (恰给其一); px = 委托价 (未舍入, 客户端按其报价精度舍入)
struct OrderReq {
  Sec sec;
  int side = kBuy;
  Credit credit = Credit::Plain;
  double px = 0, qty = 0, size = 0;
};
nlohmann::json to_json(const OrderReq &o);
// 委托回报: 实际股数 / 价, 回报原文
struct Placed {
  double qty = 0, px = 0;
  std::string msg;
};
// 下单进度: at = steps 下标; detail 非空即更新概要
using Step = std::function<void(size_t at, const std::string &detail)>;
// 长事务的报告: log 记一条 (种类 + 文字给页面 + 字段落盘), live 实时进度一行 (覆盖上一行), step 走到哪一步 (短词, 状态标签的第二级); 缺省不报
struct Report {
  std::function<void(const char *kind, std::string text, nlohmann::json kw)> log = [](const char *, std::string, nlohmann::json) {};
  std::function<void(std::string line)> live = [](std::string) {};
  std::function<void(std::string name)> step = [](std::string) {};
};

// 一个客户端的适配 (见文件头); 失败 → Fail
class Driver {
public:
  virtual ~Driver() = default;

  virtual std::string connect() = 0;

  virtual Fills fills() = 0;
  virtual Keyed<Position> positions() = 0;
  virtual Keyed<Pending> pendings() = 0;
  virtual Account account() = 0;

  virtual Terms terms(const Report &r) = 0;                                 // 两融条款全表
  virtual std::vector<CashFlow> flows(misc::Ymd from, const Report &r) = 0; // from 日起 (含; 0 = 开户起) 至昨日的资金流水

  virtual const std::vector<std::string> &steps() const = 0;
  virtual Placed place(const OrderReq &o, const Step &step) = 0; // 停在失败的那一步
};

// 券商无关的客户端 (见文件头); 刷新线程持核心的 shared_ptr, 析构只置停 (在途的一问做完即退)
class Broker {
public:
  struct Core;

  explicit Broker(std::unique_ptr<Driver> d);
  ~Broker();
  Broker(const Broker &) = delete;
  Broker &operator=(const Broker &) = delete;

  std::shared_ptr<const State> state() const;
  Refresh refresh() const;

  void touch();         // GUI 每帧: 有人在看
  void request();       // 要一轮 (不等)
  void sync();          // 要一轮并等它刷完; 失败 → Fail
  void hush(bool on);   // 批量下单期间: 只跑要的轮 (request / sync), 不自起刷新、不排下单后的
  void every(double s); // 定时周期 [s], 0 = 不定时

  Terms terms(const Report &r);
  std::vector<CashFlow> flows(misc::Ymd from, const Report &r);

  const std::vector<std::string> &steps() const;
  Placed place(const OrderReq &o, const Step &step);

private:
  std::shared_ptr<Core> c_;
};

} // namespace live
