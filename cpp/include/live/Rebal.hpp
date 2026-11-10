// 实盘: 一键调仓试算 — 名单按排名入选、等权补齐; 可选先卖, 再融资买入融资标的, 再现金买其余 (纯函数)
//
// 名单: config/local/live/lists/<名>.csv, 每行首列一个 6 位代码 (可有 code 表头), 行序即排名 (首行最高); 按修改时间新在前.
// 配置 Cfg: 名单、unit 最小调仓单元 (元, 不足即不做)、pct 行使比例 (0–100)、px 委托价、sells {代码: 卖出股数} (整步长, 或全部可卖).
// 试算:
//   卖出: 股数 (超过可卖按全部可卖) × 参考价计额, 回款 = 额 − 滑点 − 费用;
//     现金 C' = 现金 + 回款, 可用保证金 X' = 按卖后持仓重算 (avail) + 回款, L = 可融额度余量.
//   购买力 = C' + min(L, max(0, X')) (融资保证金比例按 100% 计).
//   行使: C'、X'、L 各 × pct 为本次可用额度, 以下都在其内.
//   买入: 名单里可作担保品买入的 (否则不可买: 不在担保品表 / 担保状态非正常 / 北交所) 等权补到共同目标市值 V,
//     缺口 need = V − 卖后市值, 不足该只最小单元即不补 (多了不卖). 融资标的先融资: 按保证金比例从低到高分,
//     Σ 比例 × 融资额 ≤ X'、Σ 融资额 ≤ L; 其余 (含融不到的部分) 用现金, 现金买入 + 全部买入费用 ≤ C'.
//     每只的融资腿、现金腿也各为 0 或够一个最小单元: 融不满时融资腿让出一个最小单元给现金腿, 融资腿不足一个最小单元即全用现金.
//     V 在 [0, 上界] 里二分取可行的 (精度 kTol), 并入下一只时以上一个 V 为上界;
//     现金需求大体随 V 增, 只在缺口与腿够上最小单元处分段跳变, 二分取到的未必是最大.
//   入选: 先融资标的、后其余, 各按排名逐只试并入, 并入后的 V 够得上其中卖后无持仓各只的最小单元才收, 否则跳过;
//     已持有的不设门槛, 缺口够一个最小单元即补. 购买力少时只开融资标的里排名靠前的, 多了即铺开、近于等权.
//   委托价: 买 n 股按 n × 参考价的额定价 (与下单同算); 一腿 a 元买 n × 委托价 ≤ a 的最大整步长股数.
//     最小单元 = max(起买股数, unit / 参考价) 取整步长的股数 × 其委托价, 即恰能买出这些股的最小额.
//   下单: 各腿按上述折股; 预期行使购买力 = Σ 买单额 (参考价计).
//     卖单与买单同形: 持仓行一单、名单行融资 / 现金两腿, 各带 不做的原因.
//   成本 = 各单按 5 档吃出的滑点 (相对参考价, 竞价中计 0) + 费用 (佣金万 1 双向、印花税 0.05% 卖、过户费 0.001% 双向).
//   账户 现 / 调仓后 两栏同形: 预期总资产 = 现总资产 + 融资额 − 成本, 预期净资产 = 现净资产 − 成本,
//     杠杆 = 总资产 / 净资产, 维持担保比例 = 总资产 / 负债. 现总资产 = 读时现金 + 现持仓市值.
//
// 可用保证金 (券商口径) X = 现金 + S − 负债利息, 持仓部分
//   S = Σ 折算率 × 非融资部分市值 + Σ g(融资部分市值 − 负债金额) − Σ 负债金额 × 融资保证金比例,
//   融资部分股数 = min(负债数量, 持仓), g(x) = 折算率 × x (浮盈) / x (浮亏).
//   公式与券商口径差几千元, 现值按读时校准: X = 读时客户端 X + S(现在) − S(读时); 读时现价 px0 由上层在账户读到后即取.
#pragma once

#include "live/Book.hpp"
#include "live/Broker.hpp"

#include "nlohmann/json.hpp"

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace live::rebal {

inline constexpr double kCommission = 1e-4, kStamp = 5e-4, kTransfer = 1e-5;
inline constexpr double kTol = 1; // V 二分精度 [元]

using Prices = std::map<std::string, double>; // 代码 → 价
using Bal = std::map<std::string, double>;    // 代码 → 股

double fee(double amt, int s);
// 持仓部分 S: px 须含 bal 与 debts 各只; 条款缺 → Fail
double surplus(const Bal &bal, const std::map<std::string, std::pair<double, double>> &debts, const Prices &px, const Terms &terms);
// 持仓为 bal、行情为 quotes 时的可用保证金 (不含现金变动)
double avail(const Account &a, const Prices &px0, const Bal &bal, const Quotes &quotes, const Terms &terms);

struct Cfg {
  std::string list;
  double unit = 10000, pct = 100;
  PxMode px = PxMode::Sweep;
  std::map<std::string, int> sells; // 只存 > 0
};

// 不可买的原因; 可买 → 空
std::string why(const Term *t);

struct Order : Sec {
  int s = kBuy;
  Credit credit = Credit::Plain;
  double qty = 0, px = 0, amount = 0, slip = 0, fee = 0;
  bool full = false;
};
struct Hold {
  std::string code, name;
  bool financeable = false;
  double value = 0, sellable = 0;
  int avail = 0;
  double px = 0;
  int lot = 0, step = 0, sell = 0;
  std::optional<Order> order;
  std::string skip;
};
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
  double assets = 0, net = 0, debt = 0, cash = 0;
  std::optional<double> X; // 调仓后不出 (要按买后持仓与负债重算)
  double L = 0, lev = 0;
  std::optional<double> maint;
};
struct Plan {
  Sheet now, exp;
  double power = 0, use = 0, financed = 0, cost = 0;
  double C = 0, X = 0; // 卖后
  double V = 0;
  int picked = 0, buyable = 0;
  std::vector<Hold> holds;
  std::vector<Target> targets;
  std::vector<Order> orders; // 卖单在前, 买单融资在前
  std::string time;
};
// 行情还没覆盖到 → wait 非空
struct Result {
  std::optional<Plan> plan;
  std::string wait;
};
Result plan(const Keyed<Position> &pos, const Account &acct, const Prices &px0, const Quotes &quotes, const Terms &terms,
            const std::vector<std::string> &codes, const Cfg &cfg);
// 名单里可买的 (试算要其行情)
std::vector<Sec> buyable(const std::vector<std::string> &codes, const Terms &terms);

nlohmann::json to_json(const Order &o);
nlohmann::json to_json(const Plan &p);
nlohmann::json to_json(const Cfg &c);

std::vector<std::string> lists();                            // 名单名, 新在前
std::vector<std::string> read_list(const std::string &name); // 名单 → 代码 (去重保序); 非 6 位代码 → Fail

} // namespace live::rebal
