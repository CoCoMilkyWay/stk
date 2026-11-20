// 实盘: 一键调仓试算 — 名单按排名入选、等权补齐; 可选先卖, 再融资买入融资标的, 再现金买其余 (纯函数)
//
// 名单: output/config/live/lists/<名>.csv, 每行首列一个 6 位代码 (可有 code 表头), 行序即排名; 按修改时间新在前.
// 卖出还款 (券商行为): 担保品卖出一只有融资负债的票, 回款先自动偿还本只负债 (负债股数按额同比减), 余下才进现金.
// 融资加仓: 先卖有自有部分的持仓 (混合持仓整只卖; 卖满不管最小单元), 再只融资买名单里的融资标的 (不出现金腿).
// 可用保证金校准: X = 读时客户端 X + S(现在) − S(读时), S = Σ 折算率 × 非融资市值 + Σ g(融资市值 − 负债) − Σ 负债 × 保证金比例.
#pragma once

#include "live/Book.hpp"
#include "live/Broker.hpp"

#include "nlohmann/json.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace live::rebal {

inline constexpr double kCommission = 1e-4, kStamp = 5e-4, kTransfer = 1e-5;
inline constexpr double kTol = 1; // V 二分精度 [元]

using Prices = std::map<std::string, double>; // 代码 → 价
using Bal = std::map<std::string, int64_t>;   // 代码 → 股

double fee(double amt, int s);
double surplus(const Bal &bal, const Keyed<Debt> &debts, const Prices &px, const Terms &terms);
double avail(const Snapshot &s, const Prices &px0, const Bal &bal, const Keyed<Debt> &debts, const Quotes &quotes, const Terms &terms);

struct Cfg {
  std::string list;
  double unit = 10000, pct = 100;
  PxMode px = PxMode::Sweep;
  std::map<std::string, int64_t> sells; // 只存 > 0
  bool lever = false;
};

std::string why(const Term *t); // 不可买原因; 可买 → 空

struct Order : Sec {
  int side = kBuy;
  Credit credit = Credit::Plain;
  int64_t qty = 0;
  double px = 0, amount = 0, slip = 0, fee = 0;
  bool full = false;
};

struct Hold {
  std::string code, name;
  bool financeable = false;
  double value = 0, sellable = 0;
  int64_t bal = 0, avail = 0, own = 0; // 持仓 / 可卖 / 非融资 [股]
  double debt = 0;                     // 本只融资负债额 [元]
  int64_t full = 0, base = 0;          // 融资加仓的卖满与分母 [股]
  double px = 0;
  int64_t lot = 0, step = 0, sell = 0;
  double repay = 0;
  std::optional<Order> order;
  std::string skip;
};

int64_t snap(const Hold &h, double unit, double x, bool down = false, bool lever = false);

struct Leg {
  Credit credit = Credit::Plain;
  double want = 0;
  std::optional<Order> order;
  std::string skip;
};

struct Target {
  int rank = 0;
  std::string code, name, why, skip;
  bool financeable = false;
  std::optional<double> ratio, rate, held, need;
  std::vector<Leg> legs;
};

struct Sheet {
  double assets = 0, net = 0, debt = 0, cash = 0, mv = 0;
  std::optional<double> X;
  double L = 0, lev = 0;
  std::optional<double> maint;
};

struct Plan {
  Sheet now, exp;
  double power = 0, use = 0, financed = 0, cost = 0;
  double C = 0, X = 0, repaid = 0;
  double V = 0;
  int picked = 0, buyable = 0;
  std::vector<Hold> holds;
  std::vector<Target> targets;
  std::vector<Order> orders; // 卖单在前, 买单融资在前
  int sec = -1;              // 行情最新当日秒
};

std::optional<Plan> plan(const Snapshot &s, const Prices &px0, const Quotes &quotes, const Terms &terms,
                         const std::vector<std::string> &codes, const Cfg &cfg);
Sheet now_of(const Account &a);
std::vector<Sec> buyable(const std::vector<std::string> &codes, const Terms &terms);

nlohmann::json to_json(const Plan &p);
nlohmann::json to_json(const Cfg &c);

std::vector<std::string> lists();
std::vector<std::string> read_list(const std::string &name);

} // namespace live::rebal
