// 实盘: 券商无关的一层. 各客户端适配 (live/<客户端>/) 实现 Broker 并登记进 live/Clients.hpp, 上层 (gui/task_live) 只认这里的类型
//   读: 持仓 / 当日成交 / 挂单 / 信用账户 由客户端重查后整份发布 (State, 不可变; ver 变 = 换了一份); 两融条款 / 资金流水另拉 (pull_terms / pull_flows, 慢)
//   写: 限价单 place (调用线程跑完, 逐步报进度); 一整批独占客户端 exclusive (批内连下, 其间不插刷新)
//   刷新: request 要一轮 (不等) / sync 要一轮并等完 / every 定时 / kick 刚下过单 (隔几秒再刷); 有人在看 (touch / Watch) 才转
//   客户端状态与预期不符 (界面没弹出 / 内存正被改写 / 回报失败) 是外部结局, 不是本程序的错: fail 抛 Fail, 在各轮 / 各事务的边界收成原因文本;
//   本程序自身的不变式一律 assert
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
#include <utility>
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
inline constexpr const char *kMktPrefix[3] = {"sz", "sh", "bj"};    // 新浪
inline constexpr const char *kMktSuffix[3] = {".SZ", ".SH", ".BJ"}; // AssetInfo::codes
Mkt market_of(std::string_view code);                               // 北 4 / 8 / 92 开头, 沪 5 / 6 / 9, 其余深
inline std::string sina_sym(Mkt m, const std::string &code) { return kMktPrefix[static_cast<int>(m)] + code; }

struct Sec {
  std::string code, name;
  Mkt mkt = Mkt::SZ;
  bool operator==(const Sec &) const = default;
};

// 一只持仓 (券商口径); traded / financed 精简视图没有
struct Position : Sec {
  double balance = 0, available = 0;                     // 证券数量 / 可卖 [股]
  std::optional<BySide> traded;                          // 今买 / 今卖 [股], 与成交合计同形
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
// 按代码合计 (当日成交): {方向: Σ 股} / {方向: Σ 元}
struct Flow : Sec {
  BySide traded, amount;
  bool operator==(const Flow &) const = default;
};

// 按键索引的表 (键多为代码): list 首见序, at = 键 → 下标
template <class T>
struct Keyed {
  std::vector<T> list;
  std::unordered_map<std::string, size_t> at;
  const T *find(const std::string &code) const {
    const auto i = at.find(code);
    return i == at.end() ? nullptr : &list[i->second];
  }
  bool operator==(const Keyed &o) const { return list == o.list; }
};

struct Fills {
  std::vector<Fill> rows; // 时间序
  Keyed<Flow> by_code;
};

// 信用账户 (读时)
struct Account {
  double assets = 0, line = 0, debt = 0, net = 0;         // 担保资产 / 可融额度余量 (最高融资额度 − 融资负债含费用) / 负债总额 / 净资产
  double avail = 0, cash = 0;                             // 可用保证金 / 现金
  double rate = 0;                                        // 融资年利率
  std::map<std::string, std::pair<double, double>> debts; // 代码 → (负债数量 [股], 负债金额 [元])
  std::map<std::string, double> bal;                      // 同一轮持仓 代码 → 股
};
// 持仓 → 代码 → 股 (含余额 0 的行)
std::map<std::string, double> balances(const Keyed<Position> &pos);

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
// 读到的一样: v 值 (nullptr = 还没读过), at 客户端重查后读于 HH:MM:SS (空 = 只经被动扫描读过)
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
  uint64_t ver = 0;
  std::string who; // 客户端 (进程)
  Got<Fills> fills;
  Got<Keyed<Position>> pos;
  Got<std::vector<Pending>> orders;
  Got<Account> acct;

  static const char *name(Got<Fills> State::*) { return "成交"; }
  static const char *name(Got<Keyed<Position>> State::*) { return "持仓"; }
  static const char *name(Got<std::vector<Pending>> State::*) { return "挂单"; }
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
  std::string down; // 接入失败原因 (盯客户端的上一步出错; 空 = 在线)
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
// 委托回报: 实际股数 / 价, 填单概要, 回报原文
struct Placed {
  double qty = 0, px = 0;
  std::string detail, msg;
};
// 下单进度: at = order_steps 下标; detail 非空即更新概要
using Step = std::function<void(size_t at, const std::string &detail)>;
// 事务日志: 一条 = 种类 + 字段 (落盘), 实时进度 = 一行文字 (覆盖上一行)
using Log = std::function<void(const char *kind, nlohmann::json kw)>;
using Live = std::function<void(std::string line)>;

class Broker {
public:
  virtual ~Broker() = default;
  virtual const char *name() const = 0;

  virtual std::shared_ptr<const State> state() const = 0;
  virtual Refresh refresh() const = 0;

  virtual void touch() = 0;     // GUI 每帧: 有人在看
  virtual void hold(int d) = 0; // 后台事务期间有人在看 (Watch)

  virtual uint64_t request() = 0;   // 要一轮刷新 (不等) → 轮号
  virtual void sync() = 0;          // 要一轮并等它刷完; 失败 → Fail
  virtual void every(double s) = 0; // 定时周期 [s], 0 = 不定时
  virtual void kick() = 0;          // 刚下过单: 隔几秒各刷一次 (限价单可能稍后才成交)

  virtual const std::vector<std::string> &order_steps() const = 0;
  virtual Placed place(const OrderReq &o, const Step &step) = 0;                // 当前线程跑完; 失败 → Fail (停在当时那一步)
  virtual void exclusive(const char *who, const std::function<void()> &fn) = 0; // fn 期间独占客户端 (同一线程可重入)
  virtual Terms pull_terms(const Log &log, const Live &live) = 0;               // 客户端重查两融条款; 失败 → Fail
  // 客户端重查 from 日起 (含; 0 = 开户起) 至昨日的资金流水; 失败 → Fail
  virtual std::vector<CashFlow> pull_flows(misc::Ymd from, const Log &log, const Live &live) = 0;

  virtual void console() = 0; // GUI 线程每帧: 画该客户端自带的调试台 (Live 的 Client 页; ImGui, 占满当前区域)
};

// 后台事务期间有人在看
class Watch {
public:
  explicit Watch(Broker &b) : b_(b) { b_.hold(1); }
  ~Watch() { b_.hold(-1); }
  Watch(const Watch &) = delete;
  Watch &operator=(const Watch &) = delete;

private:
  Broker &b_;
};

} // namespace live
