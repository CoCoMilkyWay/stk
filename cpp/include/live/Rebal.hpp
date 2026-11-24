// 实盘: 一键调仓试算 — 名单按排名入选、等权补齐; 先卖, 再融资买入融资标的, 再现金买其余 (纯函数)
//
// 名单: output/config/live/lists/<名>.csv, 每行首列一个 6 位代码 (可有 code 表头), 行序即排名 (首行最高); 按修改时间新在前.
// 配置 Cfg: 名单、unit 最小调仓单元 (元, 不足即不做)、pct 行使比例 (0–100)、px 委托价、
//   sells {代码: 卖出股数} (整步长、连同零股的整步长, 或全部可卖)、mode、sell_over 卖超配、no_margin 不融资.
// 模式 = 声明表 kModes 的一行, 四个维度: 卖出总 bar 分给谁 (Bar) × 买谁 (Buy) × 买入的现金腿 × 融资腿; 试算与页面只查表.
//   普通      不分 (逐只手拖)       × 名单里可买的 × 现金 ✓ 融资 ✓   补到等权 (敞口 ↑)
//   融资加仓  有自有部分的 (整只)   × 其中融资标的 × 现金 ✗ 融资 ✓   A 股加杠杆须先卖出自有部分、再融资买入; 回款留作现金 (杠杆 ↑)
//   融资减仓  融资部分              × 名单里可买的 × 现金 ✓ 融资 ✗   卖出回款先还本只负债 (见「卖出还款」), 再现金买回 (杠杆 ↓)
//   担保减仓  非融资标的 (整只)     × 不买                           回款留作现金 (敞口 ↓)
//   融资加仓 ⇄ 融资减仓 镜像 (自有部分 ⇄ 融资部分, 融资腿 ⇄ 现金腿), 担保减仓 ⇄ 普通 镜像 (只卖 ⇄ 只买).
//   有 bar 的模式卖出股数由页面按同一比例分配 (见 Hold::full / base), 卖满 (全部可卖) 时不管最小单元, 其余照守.
//   普通另有两个开关 (缺省关): sell_over 卖超配 = 卖出不看 sells, 名单外的清掉、名单里超配的减到 W, W 与补到的 V 自洽 (二分 W 取 V ≥ W 的最大者;
//     减过的不再补); no_margin 不融资 = 关掉融资腿 (只用现金买).
// 卖出还款 (券商行为, 国金实测, 控制不了): 担保品卖出一只有融资负债的票, 回款先自动偿还本只的负债 (负债额 − min(回款, 负债额),
//   负债股数按额同比减), 余下才进现金. 故混合持仓 (有负债也有自有部分) 只卖自有部分做不到: 卖后剩下的反倒大半成了自有.
//   实测 5 只中 4 只吻合; 600218 卖 200 股只还了 70 元 (疑为部成), 未核实.
// 腿: 每单的额分融资腿 fin 与现金腿, 买卖镜像 — 买 = 融资买入 (负债 +) / 现金付, 卖 = 回款先还本只负债 (负债 −) / 余下进现金.
//   F = Σ 买单 fin, R = Σ 卖单 fin; 账户各项都按腿汇总.
// 试算:
//   卖出: 股数 (超过可卖按全部可卖) × 参考价计额, 回款 = 额 − 滑点 − 费用; 卖单 fin = min(回款, 本只负债额);
//     现金 C' = 现金 + 回款 − R, 负债按还款减 (见上), 可用保证金 X' = 按卖后持仓与负债重算 (avail) + 回款 − R, L' = 可融额度余量 + R.
//   购买力 = 现金腿 C' + 融资腿 min(L', max(0, X')) (融资保证金比例按 100% 计; 没有的腿不计).
//   行使: C'、X'、L' 各 × pct 为本次可用额度, 以下都在其内 (没有现金腿 = 现金额度 0, 缺口须全融到; 没有融资腿 = 融资额度 0).
//   买入: 名单里可作担保品买入的 (否则不可买: 不在担保品表 / 担保状态非正常 / 北交所) 等权补到共同目标市值 V,
//     缺口 need = V − 卖后市值, 不足该只最小单元即不补 (多了不卖, 除非 sell_over). 融资标的先融资: 按保证金比例从低到高分,
//     Σ 比例 × 融资额 ≤ X'、Σ 融资额 ≤ L'; 其余 (含融不到的部分) 用现金, 现金买入 + 全部买入费用 ≤ C' (没有现金腿: 费用从卖后现金出, 不占额度).
//     每只的融资腿、现金腿也各为 0 或够一个最小单元: 融不满时融资腿让出一个最小单元给现金腿, 融资腿不足一个最小单元即全用现金.
//     V 在 [0, 上界] 里二分取可行的 (精度 kTol), 并入下一只时以上一个 V 为上界; 各只按下标存, 二分里不建表 (零哈希、零分配);
//     现金需求大体随 V 增, 只在缺口与腿够上最小单元处分段跳变, 二分取到的未必是最大.
//   入选: 有融资腿时先融资标的、后其余, 否则只按排名; 逐只试并入, 并入后的 V 够得上其中卖后无持仓各只的最小单元才收, 否则跳过;
//     已持有的不设门槛, 缺口够一个最小单元即补. 购买力少时只开排名靠前的, 多了即铺开、近于等权.
//   委托价: 买 n 股按 n × 参考价的额定价 (与下单同算); 一腿 a 元买 n × 委托价 ≤ a 的最大整步长股数.
//     最小单元 = max(起买股数, unit / 参考价) 取整步长的股数 × 其委托价, 即恰能买出这些股的最小额.
//   下单: 各腿按上述折股; 预期行使购买力 = Σ 买单额 (参考价计).
//     卖与买同形: 持仓行一腿 (卖)、名单行融资 / 现金两腿 (买), 各带 不做的原因.
//   成本 = 各单按 5 档吃出的滑点 (相对参考价, 竞价中计 0) + 费用 (佣金万 1 双向、印花税 0.05% 卖、过户费 0.001% 双向).
//   账户 现 / 调仓后 两栏同形: 预期总资产 = 现总资产 + F − 成本 − R, 预期净资产 = 现净资产 − 成本, 预期负债 = 现负债 − R + F,
//     持仓市值 = 总资产 − 现金, 杠杆 = 总资产 / 净资产, 维持担保比例 = 总资产 / 负债. 现总资产 = 读时现金 + 现持仓市值.
//     融资持仓 = Σ 融资部分股数 × 现价 (调仓后 = 卖后按还款后的负债重算 + F), 自有持仓 = 持仓市值 − 融资持仓.
//     调仓后的可用保证金只在没有买单时出 (只卖): 即卖后 X'.
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

// 卖出总 bar 分给谁: 不分 (逐只手拖) / 有自有部分的持仓 (整只) / 持仓的融资部分 / 非融资标的的持仓 (整只)
enum class Bar : uint8_t { None,
                           Own,
                           Fin,
                           Plain };
// 买谁: 不买 / 名单里可买的 / 其中融资标的
enum class Buy : uint8_t { None,
                           All,
                           Financeable };

enum class Mode : uint8_t { Plain,
                            Lever,
                            Delever,
                            Trim };
struct ModeSpec {
  const char *name, *hint;
  Bar bar;
  Buy buy;
  bool cash, margin; // 买入的现金腿 / 融资腿
};
inline constexpr ModeSpec kModes[] = {
    {"普通", "名单等权补齐: 先融资标的融资买、其余现金买; 卖出逐只手拖", Bar::None, Buy::All, true, true},
    {"融资加仓", "A 股加杠杆须先卖出自有部分、再融资买入: 卖出有自有部分的持仓 (卖出总 bar; 混合持仓整只卖, 回款先还其负债) → 只融资买名单里的融资标的 (不出现金腿), 回款留作现金日后择机买",
     Bar::Own, Buy::Financeable, false, true},
    {"融资减仓", "去杠杆: 按同一比例卖出各持仓的融资部分 (卖出总 bar; 回款先还本只负债) → 现金买名单 (不融资); 行使 0 即只卖还款", Bar::Fin, Buy::All, true, false},
    {"担保减仓", "按同一比例卖出非融资标的的持仓 (卖出总 bar), 融资标的不卖; 只卖不买, 回款留作现金", Bar::Plain, Buy::None, false, false},
};
inline const ModeSpec &spec(Mode m) { return kModes[static_cast<int>(m)]; }

struct Cfg {
  std::string list;
  double unit = 10000, pct = 100;
  PxMode px = PxMode::Sweep;
  std::map<std::string, int64_t> sells; // 只存 > 0
  Mode mode = Mode::Plain;
  bool sell_over = false; // 卖超配 (只普通): 名单外的清掉、超配的减到 W, 不看 sells
  bool no_margin = false; // 不融资 (只普通): 关掉融资腿
};
inline bool auto_sells(const Cfg &c) { return spec(c.mode).bar == Bar::None && c.sell_over; } // 卖出由试算自定 (sell_over 生效)
inline bool margin_leg(const Cfg &c) { return spec(c.mode).margin && !(spec(c.mode).cash && c.no_margin); }

// 不可买的原因; 可买 → 空
std::string why(const Term *t);

// 试算的一单: 限价单 (委托价按试算时的盘口, 下单时按新取的盘口再定) + 参考价计的额 / 滑点 / 费用 / 5 档吃满
//   + 融资腿额 fin (买 = 融资买入, 负债 +; 卖 = 回款先还本只负债, 负债 −); 余下为现金腿 (买 = 现金付, 卖 = 进现金)
struct Order : OrderReq {
  double amount = 0, slip = 0, fee = 0, fin = 0;
  bool full = false;
};

// 一腿: 买 = 融资 / 现金 (credit), 卖 = 一单 (credit 恒普通, 其融资腿见 Order::fin); want = 想做的额, 不成单即 skip 为原因
struct Leg {
  Credit credit = Credit::Plain;
  double want = 0;
  std::optional<Order> order;
  std::string skip;
};

struct Hold {
  std::string code, name;
  bool financeable = false;
  double value = 0, sellable = 0;
  // 持仓 / 可卖 / 非融资持仓 (= 持仓 − 融资部分, 融资部分 = min(负债数量, 持仓)) [股]; 可卖不含今日买入的 (T+1)
  int64_t bal = 0, avail = 0, own = 0;
  double debt = 0; // 本只融资负债额 [元]; > 0 即卖出回款先还它 (见文件头「卖出还款」)
  // 卖出总 bar 的卖满 full 与其分母 base [股] (见 Bar): 要卖的即 全部可卖 / 持仓 (融资部分: 盖住融资部分的可卖股数 / 融资部分), 不卖的 0 / 0.
  //   base 含今日买入的 ⇒ full < base 即今日卖不满
  int64_t full = 0, base = 0;
  double px = 0;
  int64_t lot = 0, step = 0, sell = 0; // sell = 卖出股数 (sells 里的, 或 sell_over 自定的)
  Leg leg;                             // 卖
};

// 能成单的卖出股数里离 x 最近的 (down = 不超过 x 的最大的): 0 不卖、起卖股数 (够起买股数且够 unit 元) 起按步长递增 (或连同零股)、
//   全部可卖 (够 unit 元才有; by_bar = 有卖出总 bar 的模式: 不管 unit, 卖满即可零碎)
//   零股: 沪深主板 / 创业板步长 100, 不足 100 的零股只能一次卖完 ⇒ 部分卖出的股数 ≡ 0 或 ≡ 可卖 % 100 (连同全部零股); 科创板步长 1
int64_t snap(const Hold &h, double unit, double x, bool down = false, bool by_bar = false);

struct Target {
  int rank = 0;
  std::string code, name, why, skip;
  bool financeable = false;
  std::optional<double> ratio, rate, held, need;
  std::vector<Leg> legs; // 买: 融资在前
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
