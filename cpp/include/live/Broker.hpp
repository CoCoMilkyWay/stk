// 实盘: 券商无关的一层. 各客户端适配 (live/<客户端>/) 实现 Driver 并登记进 live/Clients.hpp, 上层只认这里与 Broker
//   双翼对仗:
//     Driver (同步 · 线程安全 · 纯原语):
//       接   connect() → 客户端标识
//       读   fills() / pos() / pends() / acct()  (四样同形, 一轮按此序)
//       事务 terms() / flows(from) / place(o)    (三样同形; 进度与日志统一走 live::trace)
//     Broker (异步调度 · 券商无关 · 唯一通道):
//       读   一轮四样读入局部 Snapshot, 全齐且成交/持仓核平后原子发布 (上层绝不见半轮或脏混合);
//            失败保留上份完整快照并记 err/down, 不自动重试, 等下次起因 (request / sync / 有人在看且满 every / 下单后定点)
//       事务 txn() / batch() 在调用线程跑: 期间自动抑制自起刷新
//   边界: 外部客户端行为不可控 → fail() 记 trace 并抛 Fail, 在各轮/事务边界收成原因文本; 内部不变式一律 assert
//   编码: 方向 买 +1 / 卖 −1 (Sides<T> 下标 s < 0); 股数 int64_t; 时刻 int 当日秒 (0..86399, -1 = 无); 市场由代码前缀定
#pragma once

#include "live/Mirror.hpp"
#include "live/Trace.hpp"
#include "misc/date.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace live {

struct Fail {
  std::string why;
};
[[noreturn]] void fail(std::string why);
inline void need(bool ok, std::string_view why) {
  if (!ok)
    fail(std::string(why));
}

std::string strf(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
int sec_now();                       // 本地当日秒 0..86399
std::string fmt_sec(int sec);        // 当日秒 → HH:MM:SS (< 0 → "—")
int parse_sec(std::string_view str); // HH:MM:SS → 当日秒 (非法 → -1)

inline constexpr int kBuy = 1, kSell = -1;
inline constexpr int kSides[2] = {kBuy, kSell};
inline const char *side_name(int s) { return s > 0 ? "买入" : "卖出"; }

// 买/卖镜像对仗容器: 下标 +1 (买) → v[0], −1 (卖) → v[1]
template <class T>
struct Sides {
  T v[2]{};
  T &operator[](int s) { return v[s < 0]; }
  const T &operator[](int s) const { return v[s < 0]; }
  bool operator==(const Sides &) const = default;
};

enum class Mkt : uint8_t { SZ,
                           SH,
                           BJ };
inline constexpr const char *kMktPrefix[3] = {"sz", "sh", "bj"};
inline constexpr const char *kMktSuffix[3] = {".SZ", ".SH", ".BJ"};
Mkt market_of(std::string_view code);
inline const char *mkt_name(Mkt m) { return kMktSuffix[static_cast<int>(m)] + 1; }
Mkt mkt_named(std::string_view s);
inline std::string sina_sym(Mkt m, const std::string &code) { return kMktPrefix[static_cast<int>(m)] + code; }

struct Sec {
  std::string code, name;
  Mkt mkt = Mkt::SZ;
  bool operator==(const Sec &) const = default;
};

struct Position : Sec {
  int64_t balance = 0, available = 0;                    // 证券数量 / 可卖 [股]
  Sides<int64_t> traded;                                 // 今买 / 今卖 [股]
  std::optional<int64_t> financed;                       // 融资数量 [股]
  double cost = 0, price = 0, market_value = 0, pnl = 0; // 参考成本 / 现价 / 市值 / 参考浮盈 (券商口径)
  std::optional<double> position_pct, collateral;        // 仓位 [%] / 担保折算率
  bool operator==(const Position &) const = default;
};

struct Entrust : Sec {
  int sec = -1; // 当日秒
  int side = kBuy;
  double price = 0;
  int64_t qty = 0; // [股]
  std::string order_id;
  bool operator==(const Entrust &) const = default;
};

struct Fill : Entrust {
  double amount = 0; // [元]
  std::string fill_id;
  bool operator==(const Fill &) const = default;
};

struct Pending : Entrust {
  std::string kind, status;
  int64_t filled = 0; // 已成 [股]
  bool operator==(const Pending &) const = default;
};

struct Traded : Sec {
  Sides<int64_t> qty;
  Sides<double> amount;
  bool operator==(const Traded &) const = default;
};

struct Debt : Sec {
  int64_t qty = 0;   // 负债数量 [股]
  double amount = 0; // 负债金额 [元]
  bool operator==(const Debt &) const = default;
};

// 按键索引的保序表: list 首见序, at 键 → 下标
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

// 持仓 h 的今买/今卖与成交合计 f 相符 (余额为 0 的不核)
bool synced(const Position &h, const Traded *f);

struct Account {
  double assets = 0, line = 0, debt = 0, net = 0; // 担保资产 / 可融余量 / 负债总额 / 净资产
  double avail = 0, cash = 0, rate = 0;           // 可用保证金 / 现金 / 融资年利率
  Keyed<Debt> debts;
};

// 整轮原子快照: 四样同轮读齐、核平后整份发布 (上层绝不见半轮)
struct Snapshot {
  std::string who;
  int at = -1; // 本轮完成时刻 (当日秒)
  Fills fills;
  Keyed<Position> pos;
  Keyed<Pending> pends;
  Account acct;
};

struct Term {
  std::string name;
  Mkt mkt = Mkt::SZ;
  std::optional<double> ratio; // 融资保证金比例
  bool financeable = false;
  double rate = 0; // 担保折算率
  bool pledgeable = false;
};
using Terms = std::map<std::string, Term>;

struct CashFlow {
  misc::Ymd date = 0;
  std::string code, name;
  double price = 0, qty = 0;
  std::string kind;
  double amount = 0, balance = 0;
  bool operator==(const CashFlow &) const = default;
};

struct Refresh {
  trace::Spot at;   // 刷新线程此刻在哪 (段栈空 = 闲)
  std::string who;  // 最近一次接入的客户端标识
  std::string err;  // 上一轮失败原因
  std::string down; // 上一轮接不上原因 (空 = 在线)
  double every = 0; // 定时周期 [s], 0 = 不定时
};

enum class Credit : uint8_t { Plain,
                              Margin };
inline const char *credit_name(Credit c) { return c == Credit::Margin ? "融资" : "普通"; }

// 下单量: 按股或按元, 二选一显式标记
struct Qty {
  enum Kind : uint8_t { Shares,
                        Yuan } kind = Shares;
  double v = 0;
  static Qty shares(int64_t n) { return {Shares, static_cast<double>(n)}; }
  static Qty yuan(double a) { return {Yuan, a}; }
  bool is_shares() const { return kind == Shares; }
};

struct OrderReq {
  Sec sec;
  int side = kBuy;
  Credit credit = Credit::Plain;
  double px = 0;
  Qty qty;
};
nlohmann::json to_json(const OrderReq &o);

struct Placed {
  int64_t qty = 0;
  double px = 0;
  std::string msg;
};

// 客户端适配 (见文件头); 同步, 自身线程安全; 外部失败 → Fail
class Driver {
public:
  virtual ~Driver() = default;

  virtual std::string connect() = 0;

  virtual Fills fills() = 0;
  virtual Keyed<Position> pos() = 0;
  virtual Keyed<Pending> pends() = 0;
  virtual Account acct() = 0;

  virtual Terms terms() = 0;
  virtual std::vector<CashFlow> flows(misc::Ymd from) = 0;
  virtual Placed place(const OrderReq &o) = 0;
  virtual std::span<const std::string_view> steps() const = 0;
  virtual std::unique_ptr<Mirror> mirror() { return nullptr; }
};

// 券商无关调度器: 刷新线程发布 Snapshot, 事务在调用线程串行跑且自动抑制自起刷新
class Broker {
public:
  struct Core;

  explicit Broker(std::unique_ptr<Driver> d);
  ~Broker();
  Broker(const Broker &) = delete;
  Broker &operator=(const Broker &) = delete;

  std::shared_ptr<const Snapshot> snapshot() const; // nullptr = 还没成功刷过
  Refresh refresh() const;

  void touch();         // GUI 每帧: 有人在看
  void request();       // 要一轮 (不等)
  void sync();          // 要一轮并等完; 失败 → Fail
  void every(double s); // 定时周期 [s], 0 = 不定时

  Terms terms();
  std::vector<CashFlow> flows(misc::Ymd from);
  Placed place(const OrderReq &o);
  // 批量事务区 (如执行调仓): 期间不自起定时/下单后刷新, 出作用域自动恢复
  void batch(const std::function<void()> &fn);

  std::span<const std::string_view> steps() const;
  std::unique_ptr<Mirror> mirror() const;

private:
  std::shared_ptr<Core> c_;
};

} // namespace live
