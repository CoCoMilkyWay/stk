// 实盘: 券商无关的一层. 各客户端适配 (live/<客户端>/) 实现 Driver 并登记进 live/Clients.hpp, 上层 (gui/task_live) 只认这里的类型与 Broker
//   Driver (一个客户端一份): 同步, 调用线程跑完; 刷新线程与事务线程会同时调, 自身线程安全
//     接: connect 接上客户端 (换了进程即重接, 接上即可读) → 是谁
//     读: read 一问读齐四样 (fills / pos / pends / acct, 与 Snapshot 的四样同名; 适配可让几张表同时在途, 一趟往返)
//     事务: 拉 terms / flows (慢) 与写 place (一张限价单, steps 即各步名) 三样同形; 进度 (段 / 实时一行) 与记一条统一经 live::trace
//     画面: mirror 可选, 给 GUI 复现客户端窗口 (live/Mirror.hpp)
//   Broker (券商无关, 只此一份): 包一个 Driver + 刷新线程; 读由刷新线程发布, 事务由调用线程跑 (段 + 接上 + Driver), 两类同一个接入
//     读: 一轮读进一份局部的 Snapshot, 轮末整份发布 (不可变; 指针换了 = 换了一份), 上层不见半轮也不见两轮的混合;
//       成交与持仓读的那一刻之间又有成交即对不上, 重读至多几遍仍不对照样发布, 留给上层逐只标出 (synced).
//       一轮失败 = 不发布、保留上一份完整的, 原因记在 err / down. 有人在看 (touch) 才转
//       起因: request 要一轮 (不等) / sync 要一轮并等完 / 有人在看而还没刷过或距上轮满 every 秒 /
//       下单成功后隔几秒 (限价单可能稍后才成交). 失败不自动重试, 等下一个起因
//       batch (批量下单期间不扫客户端): 只认 request / sync, 不自起、不排下单后的; 单件事务不抑制
//   客户端状态与预期不符 (界面没弹出 / 应答不认识 / 回报失败) 是外部结局, 不是本程序的错: fail 先在当时的段下记一行 (live/Trace.hpp)
//   再抛 Fail, 在各轮 / 各事务 / 各单的边界 (guard) 收成原因文本; 应答畸形时库里抛的异常同样收下, 绝不越过边界崩掉进程.
//   本程序自身的不变式一律 assert
// 编码: 方向 买 +1 / 卖 −1 (Sides<T> 下标 s < 0); 股数 int64_t; 时刻 int 当日秒 (0..86399, −1 = 无);
//   市场由代码前缀定 (market_of), 新浪码 = 前缀 + 代码, AssetInfo 码 = 代码 + 后缀
#pragma once

#include "live/Mirror.hpp"
#include "live/Trace.hpp"
#include "misc/date.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace live {

struct Fail {
  std::string why;
};
[[noreturn]] void fail(std::string why);
std::string strf(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
// ok 不成立即 fail: 原因原样, 或 printf 格式 + 参数 (只在不成立时才拼, 逐行逐格的校验照用; 参数只收标量与 C 串)
inline void need(bool ok, std::string_view why) {
  if (!ok)
    fail(std::string(why));
}
template <class... A>
  requires(sizeof...(A) > 0)
void need(bool ok, const char *fmt, const A &...a) {
  static_assert(((std::is_arithmetic_v<std::decay_t<A>> || std::is_enum_v<std::decay_t<A>> || std::is_pointer_v<std::decay_t<A>>) && ...),
                "need 的格式参数只收标量与 C 串");
  if (!ok)
    fail(strf(fmt, a...));
}
// 一个错误边界 (各轮 / 各事务 / 各单 / 后台线程): 跑 fn, 外部结局收成原因文本, 空 = 成了.
//   Fail 原样; 别的异常 (客户端 / 行情应答畸形时 JSON 取值、数值转换抛的) 一并收下、先记一行 fail, 不让它越过边界崩掉进程
template <class F>
std::string guard(F &&fn) {
  try {
    fn();
    return {};
  } catch (const Fail &f) {
    return f.why.empty() ? std::string("(无原因)") : f.why;
  } catch (const std::exception &e) {
    trace::ev("fail", {{"why", e.what()}});
    return e.what()[0] ? std::string(e.what()) : std::string("(无原因)");
  }
}

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
  int64_t balance = 0, available = 0;                    // 证券数量 / 可卖 [股]
  Sides<int64_t> traded;                                 // 今买 / 今卖 [股], 与成交合计同形
  std::optional<int64_t> financed;                       // 融资数量 [股]
  double cost = 0, price = 0, market_value = 0, pnl = 0; // 参考成本价 / 现价 / 市值 / 参考浮动盈亏 (券商口径, ≠ (现价 − 成本价) × 量)
  std::optional<double> position_pct, collateral;        // 仓位 [%] / 担保折算率
  bool operator==(const Position &) const = default;
};

// 一笔委托 (成交 / 挂单共有)
struct Entrust : Sec {
  int sec = -1; // 时刻 (当日秒)
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

// 还挂着的委托
struct Pending : Entrust {
  std::string kind, status; // 委托类别 / 已报 · 部成 …
  int64_t filled = 0;       // 已成 [股]
  bool operator==(const Pending &) const = default;
};

// 一只当日成交的合计: {方向: Σ 股} / {方向: Σ 元}
struct Traded : Sec {
  Sides<int64_t> qty;
  Sides<double> amount;
  bool operator==(const Traded &) const = default;
};

// 一只融资负债
struct Debt : Sec {
  int64_t qty = 0;   // 负债数量 [股]
  double amount = 0; // 负债金额 [元]
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
  double avail = 0, cash = 0, rate = 0;           // 可用保证金 / 现金 / 融资年利率
  Keyed<Debt> debts;                              // 按代码
};

// 读一次的四样
struct Reads {
  Fills fills;
  Keyed<Position> pos;
  Keyed<Pending> pends;
  Account acct;
};
// 客户端读到的一份: 四样同一轮读齐才整份发布 (见文件头); 信用账户与持仓同轮, 可用保证金按读时校准即以这一份为准
struct Snapshot : Reads {
  std::string who; // 客户端 (进程)
  int at = -1;     // 本轮读完于 (当日秒)
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

// 刷新进度
struct Refresh {
  trace::Spot at;   // 刷新线程此刻在哪 (段栈空 = 闲)
  std::string who;  // 最近一次接入的客户端标识
  std::string err;  // 上一轮失败原因
  std::string down; // 上一轮接不上原因 (空 = 在线)
  double every = 0; // 定时周期 [s], 0 = 不定时
};

enum class Credit : uint8_t { Plain,    // 普通 (担保品买卖)
                              Margin }; // 融资买入
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

// 一张限价单: px = 委托价 (未舍入, 客户端按其报价精度舍入)
struct OrderReq {
  Sec sec;
  int side = kBuy;
  Credit credit = Credit::Plain;
  double px = 0;
  Qty qty;
};
nlohmann::json to_json(const OrderReq &o);

// 委托回报: 实际股数 / 价, 回报原文
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

  virtual Reads read() = 0; // 成交 / 持仓 / 挂单 / 信用账户

  virtual Terms terms() = 0;                               // 两融条款全表
  virtual std::vector<CashFlow> flows(misc::Ymd from) = 0; // from 日起 (含; 0 = 开户起) 至昨日的资金流水
  virtual Placed place(const OrderReq &o) = 0;             // 停在失败的那一步

  virtual std::span<const std::string_view> steps() const = 0; // place 的各步名 (即其各步的段名)
  virtual std::unique_ptr<Mirror> mirror() { return nullptr; } // 不支持复现画面 = 空
};

// 券商无关的客户端 (见文件头); 刷新线程持核心的 shared_ptr, 析构只置停 (在途的一问做完即退)
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
  // 批量下单期间: 只跑要的轮 (request / sync), 不自起刷新、不排下单后的; fn 跑完 (成败都) 即恢复
  void batch(const std::function<void()> &fn);

  std::span<const std::string_view> steps() const;
  std::unique_ptr<Mirror> mirror() const;

private:
  std::shared_ptr<Core> c_;
};

} // namespace live
