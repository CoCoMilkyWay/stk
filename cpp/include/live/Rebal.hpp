// 实盘: 一键调仓试算 — 名单按排名入选、等权补齐; 可选先卖, 再融资买入融资标的, 再现金买其余 (纯函数)
//
// 名单: output/config/live/lists/<名>.csv, 每行首列一个 6 位代码 (可有 code 表头), 行序即排名 (首行最高); 按修改时间新在前.
// 配置 Cfg: 名单、unit 最小调仓单元 (元, 不足即不做)、pct 行使比例 (0–100)、px 委托价、
//   sells {代码: 卖出股数} (整步长、连同零股的整步长, 或全部可卖)、mode 普通 / 融资加仓 / 担保减仓.
// 融资加仓与担保减仓的卖出股数由页面按同一比例分配 (见 Hold::full), 卖满 (全部可卖) 时不管最小单元, 其余照守.
// 融资加仓 (A 股加杠杆须先卖出自有部分、再融资买入, 回款留作现金日后择机买):
//   买入只融资买名单里的融资标的, 不出现金腿, 费用从卖后现金出; 行使比例只作用于 X'、L.
// 担保减仓: 只卖非融资标的的持仓, 只卖不买, 回款留作现金.
// 卖出还款 (券商行为, 国金实测, 控制不了): 担保品卖出一只有融资负债的票, 回款先自动偿还本只的负债 (负债额 − min(回款, 负债额),
//   负债股数按额同比减), 余下才进现金. 故混合持仓 (有负债也有自有部分) 只卖自有部分做不到: 卖后剩下的反倒大半成了自有.
//   实测 5 只中 4 只吻合; 600218 卖 200 股只还了 70 元 (疑为部成), 未核实.
// 试算:
//   卖出: 股数 (超过可卖按全部可卖) × 参考价计额, 回款 = 额 − 滑点 − 费用; 各只还款 = min(回款, 本只负债额) (Hold::repay), R = Σ 还款;
//     现金 C' = 现金 + 回款 − R, 负债按还款减 (见上), 可用保证金 X' = 按卖后持仓与负债重算 (avail) + 回款 − R, L' = 可融额度余量 + R.
//   购买力 = C' + min(L', max(0, X')) (融资保证金比例按 100% 计; 融资加仓不计 C').
//   行使: C'、X'、L 各 × pct 为本次可用额度, 以下都在其内 (融资加仓的现金额度为 0, 缺口须全融到).
//   买入: 名单里可作担保品买入的 (否则不可买: 不在担保品表 / 担保状态非正常 / 北交所) 等权补到共同目标市值 V,
//     缺口 need = V − 卖后市值, 不足该只最小单元即不补 (多了不卖). 融资标的先融资: 按保证金比例从低到高分,
//     Σ 比例 × 融资额 ≤ X'、Σ 融资额 ≤ L; 其余 (含融不到的部分) 用现金, 现金买入 + 全部买入费用 ≤ C'.
//     每只的融资腿、现金腿也各为 0 或够一个最小单元: 融不满时融资腿让出一个最小单元给现金腿, 融资腿不足一个最小单元即全用现金.
//     V 在 [0, 上界] 里二分取可行的 (精度 kTol), 并入下一只时以上一个 V 为上界; 各只按下标存, 二分里不建表 (零哈希、零分配);
//     现金需求大体随 V 增, 只在缺口与腿够上最小单元处分段跳变, 二分取到的未必是最大.
//   入选: 先融资标的、后其余, 各按排名逐只试并入, 并入后的 V 够得上其中卖后无持仓各只的最小单元才收, 否则跳过;
//     已持有的不设门槛, 缺口够一个最小单元即补. 购买力少时只开融资标的里排名靠前的, 多了即铺开、近于等权.
//   委托价: 买 n 股按 n × 参考价的额定价 (与下单同算); 一腿 a 元买 n × 委托价 ≤ a 的最大整步长股数.
//     最小单元 = max(起买股数, unit / 参考价) 取整步长的股数 × 其委托价, 即恰能买出这些股的最小额.
//   下单: 各腿按上述折股; 预期行使购买力 = Σ 买单额 (参考价计).
//     卖单与买单同形: 持仓行一单、名单行融资 / 现金两腿, 各带 不做的原因.
//   成本 = 各单按 5 档吃出的滑点 (相对参考价, 竞价中计 0) + 费用 (佣金万 1 双向、印花税 0.05% 卖、过户费 0.001% 双向).
//   账户 现 / 调仓后 两栏同形: 预期总资产 = 现总资产 + 融资额 − 成本 − R, 预期净资产 = 现净资产 − 成本, 预期负债 = 现负债 − R + 融资额,
//     持仓市值 = 总资产 − 现金, 杠杆 = 总资产 / 净资产, 维持担保比例 = 总资产 / 负债. 现总资产 = 读时现金 + 现持仓市值.
//     融资持仓 = Σ 融资部分股数 × 现价 (调仓后 = 卖后按还款后的负债重算 + 融资买入额), 自有持仓 = 持仓市值 − 融资持仓.
//     调仓后的可用保证金只在没有买单时出 (担保减仓、只卖): 即卖后 X'.
//
// 可用保证金 (券商口径) X = 现金 + S − 负债利息, 持仓部分
//   S = Σ 折算率 × 非融资部分市值 + Σ g(融资部分市值 − 负债金额) − Σ 负债金额 × 融资保证金比例,
//   融资部分股数 = min(负债数量, 持仓), g(x) = 折算率 × x (浮盈) / x (浮亏).
//   公式与券商口径差几千元, 现值按读时校准: X = 读时客户端 X + S(现在) − S(读时); 读时现价 px0 由上层在快照读到后即取.
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
// 持仓部分 S: px 须含 bal 与 debts 各只; 条款缺 → Fail
double surplus(const Bal &bal, const Keyed<Debt> &debts, const Prices &px, const Terms &terms);
// 持仓为 bal、负债为 debts、行情为 quotes 时的可用保证金 (不含现金变动); 读时 = 快照 s 的持仓与负债 × 读时现价 px0
double avail(const Snapshot &s, const Prices &px0, const Bal &bal, const Keyed<Debt> &debts, const Quotes &quotes, const Terms &terms);

enum class Mode : uint8_t { Plain,
                            Lever,
                            Trim };
inline constexpr const char *kModeNames[3] = {"普通", "融资加仓", "担保减仓"};

struct Cfg {
  std::string list;
  double unit = 10000, pct = 100;
  PxMode px = PxMode::Sweep;
  std::map<std::string, int64_t> sells; // 只存 > 0
  Mode mode = Mode::Plain;
};

// 不可买的原因; 可买 → 空
std::string why(const Term *t);

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
  // 持仓 / 可卖 / 非融资持仓 (= 持仓 − 融资部分, 融资部分 = min(负债数量, 持仓)) [股]; 可卖不含今日买入的 (T+1)
  int64_t bal = 0, avail = 0, own = 0;
  double debt = 0; // 本只融资负债额 [元]; > 0 即卖出回款先还它 (见文件头「卖出还款」)
  // 卖出总 bar 的卖满 full 与其分母 base [股]: 要卖的即 全部可卖 / 持仓, 不卖的 0 / 0. base 含今日买入的 ⇒ full < base 即今日卖不满
  //   融资加仓: 有自有部分 (own > 0) 的要卖, 纯融资的已是要的形; 混合持仓也整只卖: 只卖自有部分做不到 (回款先还本只负债), 卖净了再融资买回
  //   担保减仓: 非融资标的要卖, 融资标的不卖; 普通: 都 0 / 0
  int64_t full = 0, base = 0;
  double px = 0;
  int64_t lot = 0, step = 0, sell = 0;
  double repay = 0; // 卖单的预计还款 = min(回款, debt)
  std::optional<Order> order;
  std::string skip;
};

// 能成单的卖出股数里离 x 最近的 (down = 不超过 x 的最大的): 0 不卖、起卖股数 (够起买股数且够 unit 元) 起按步长递增 (或连同零股)、
//   全部可卖 (够 unit 元才有; by_bar = 有卖出总 bar 的模式 (融资加仓 / 担保减仓): 不管 unit, 卖满即可零碎)
//   零股: 沪深主板 / 创业板步长 100, 不足 100 的零股只能一次卖完 ⇒ 部分卖出的股数 ≡ 0 或 ≡ 可卖 % 100 (连同全部零股); 科创板步长 1
int64_t snap(const Hold &h, double unit, double x, bool down = false, bool by_bar = false);

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
  double assets = 0, net = 0, debt = 0, cash = 0, mv = 0; // mv = 持仓市值 = 总资产 − 现金
  double fin = 0;                                         // 其中融资持仓 (各只融资部分 min(负债数量, 持仓) × 价); 自有持仓 = mv − fin
  std::optional<double> X;                                // 调仓后只在没有买单时出 (= 卖后 X'); 有买单不出 (要按买后持仓与负债重算)
  double L = 0, lev = 0;
  std::optional<double> maint;
};

struct Plan {
  Sheet now, exp;
  double power = 0, use = 0, financed = 0, cost = 0;
  double C = 0, X = 0, repaid = 0; // 卖后 (现金 / 可用保证金) / 卖出还款 R
  double V = 0;
  int picked = 0, buyable = 0;
  std::vector<Hold> holds;
  std::vector<Target> targets;
  std::vector<Order> orders; // 卖单在前, 买单融资在前
  int sec = -1;              // 行情最新当日秒
};

// 行情还没覆盖到 (持仓 / 名单里可买的 / 负债各只) → 无
std::optional<Plan> plan(const Snapshot &s, const Prices &px0, const Quotes &quotes, const Terms &terms,
                         const std::vector<std::string> &codes, const Cfg &cfg);
// 读时的账户 (客户端口径; 融资持仓按券商现价): 试算没出时账户的「现」栏
Sheet now_of(const Snapshot &s);
// 名单里可买的 (试算要其行情)
std::vector<Sec> buyable(const std::vector<std::string> &codes, const Terms &terms);

nlohmann::json to_json(const Plan &p);
nlohmann::json to_json(const Cfg &c);

std::vector<std::string> lists();                            // 名单名, 新在前
std::vector<std::string> read_list(const std::string &name); // 名单 → 代码 (去重保序); 非 6 位代码 → Fail

} // namespace live::rebal
