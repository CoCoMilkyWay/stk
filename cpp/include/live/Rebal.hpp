// 实盘: 一键调仓试算 — 名单按排名入选、等权补齐; 先卖, 再融资买入融资标的, 再现金买其余 (纯函数)
//
// 名单: output/config/live/lists/<名>.csv, 每行首列一个 6 位代码 (可有 code 表头), 行序即排名 (首行最高); 按修改时间新在前.
// 配置 Cfg (名单另交, 见 plan 的 codes): 卖的方式 how + 卖 sell / 买 buy 两侧同形 (Side = 部分 × 范围), unit 最小调仓单元 (元, 不足即不做)、
//   pct 行使比例 (0–100)、px 委托价、sells {代码: 卖出股数} (整步长、连同零股的整步长, 或全部可卖; 手拖与按比例用).
// 两侧同形 Side:
//   部分 Parts {own, fin}: 持仓的自有部分 / 融资部分 (融资部分 = min(负债数量, 持仓)). 卖 = 卖哪部分 (自有卖了进现金, 融资卖了还负债);
//     买 = 买成哪部分 (自有 = 现金买入, 融资 = 融资买入). 两侧镜像: 自有 ⇄ 现金, 融资 ⇄ 负债.
//   范围 Scope: 全部 / 融资标的 / 非融资标的 (按合并后的两融条款); 卖 = 动哪些持仓, 买 = 补名单里哪些 (融资部分只买得到融资标的).
// 卖的方式 How (kHows 一行一个; 买恒为等权补齐, 与卖超配对偶):
//   手拖    逐只拖各持仓的卖出滑杆 (sells); 不认部分与范围
//   按比例  卖出总 bar 按同一比例分给范围内各持仓的所选部分, 写进各只滑杆 (可再逐只调):
//             只融资 = 卖到盖住融资部分; 只自有 = 有自有部分的持仓整只卖 (混合持仓回款先还其负债, 只卖自有部分做不到, 见「卖出还款」);
//             两部分 = 整只卖. 卖满 (全部可卖) 不管最小单元, 其余照守 (见 Hold::span / part / full)
//   卖超配  试算自定, 不看 sells: 范围内名单外的清掉、名单里超配的减到 W, W 与补到的 V 自洽 (二分 W 取 V ≥ W 的最大者; 减过的不再补);
//           按市值减不分部分; 须有买入部分
//   常用的几个点: 手拖 × 买两部分 = 补到等权 (敞口 ↑); 卖自有 × 买融资 = A 股加杠杆 (先卖自有、再融资买, 回款留作现金; 杠杆 ↑);
//   卖融资 × 买自有 = 去杠杆 (卖了还款、现金买回; 杠杆 ↓); 卖非融资标的 × 不买 = 担保减仓 (回款留作现金; 敞口 ↓).
// 卖出还款 (券商行为, 国金实测, 控制不了): 担保品卖出一只有融资负债的票, 回款先自动偿还本只的负债 (负债额 − min(回款, 负债额),
//   负债股数按额同比减), 余下才进现金. 故混合持仓 (有负债也有自有部分) 只卖自有部分做不到: 卖后剩下的反倒大半成了自有.
//   实测 5 只中 4 只吻合; 600218 卖 200 股只还了 70 元 (疑为部成), 未核实.
// 腿: 每单的额分融资腿 fin 与自有腿, 买卖镜像 — 买 = 融资买入 (负债 +) / 现金付, 卖 = 回款先还本只负债 (负债 −) / 余下进现金.
//   F = Σ 买单 fin, R = Σ 卖单 fin (Plan::fin 的买 / 卖两侧); 账户各项都按腿汇总.
// 试算:
//   卖出: 股数 (超过可卖按全部可卖) × 参考价计额, 回款 = 额 − 滑点 − 费用; 卖单 fin = min(回款, 本只负债额);
//     现金 C' = 现金 + 回款 − R, 负债按还款减 (见上), 可用保证金 X' = 按卖后持仓与负债重算 (见末段「可用保证金」) + 回款 − R, L' = 可融额度余量 + R.
//   购买力 = 现金腿 C' + 融资腿 min(L', max(0, X')) (融资保证金比例按 100% 计; 没有的腿不计).
//   行使: C'、X'、L' 各 × pct 为本次可用额度, 以下都在其内 (没有现金腿 = 现金额度 0, 缺口须全融到; 没有融资腿 = 融资额度 0).
//   买入: 名单里可作担保品买入的 (否则不可买: 不在担保品表 / 担保状态非正常 / 北交所) 在买的范围内、所选部分买得到的 (只融资 = 只融资标的) 等权补到共同目标市值 V,
//     缺口 need = V − 卖后市值, 不足该只最小单元即不补 (多了不卖, 除非卖超配). 融资标的先融资: 按保证金比例从低到高分,
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

inline constexpr double kTol = 1; // V 二分精度 [元]

using Prices = std::map<std::string, double>; // 代码 → 价
using Bal = std::map<std::string, int64_t>;   // 代码 → 股

// 部分 (卖买同形, 见文件头): 自有 / 融资
struct Parts {
  bool own = true, fin = true;
  bool any() const { return own || fin; }
};
// 范围 (卖买同形, 见文件头): 全部 / 融资标的 / 非融资标的
enum class Scope : uint8_t { All,
                             Fin,
                             NonFin };
inline constexpr const char *kScopeNames[3] = {"全部", "融资标的", "非融资标的"};
inline bool in_scope(Scope s, bool financeable) { return s == Scope::All || (s == Scope::Fin) == financeable; }
// 一侧 = 部分 × 范围
struct Side {
  Parts parts;
  Scope scope = Scope::All;
};

// 卖的方式 (见文件头): name 单选的名字 / hint 悬停 / parts · scope 认不认卖一侧的部分 · 范围
enum class How : uint8_t { Hand,
                           Bar,
                           Over };
struct HowSpec {
  const char *name, *hint;
  bool parts, scope;
};
inline constexpr HowSpec kHows[] = {
    {"手拖", "逐只拖各持仓的卖出滑杆; 不看部分与范围", false, false},
    {"按比例",
     "卖出总 bar 按同一比例分给范围内各持仓的所选部分, 写进各只卖出滑杆 (可再逐只调)\n"
     "• 只融资 = 卖到盖住融资部分 (回款先还本只负债)\n"
     "• 只自有 = 有自有部分的持仓整只卖: 混合持仓 (有负债也有自有部分) 回款先还其负债, 只卖自有部分做不到; 其卖出标橙\n"
     "• 两部分 = 整只卖",
     true, true},
    {"卖超配", "卖出由试算自定 (不看各只卖出滑杆): 范围内名单外的持仓清掉, 名单里超配的减到 W, 再把其余补到 V; W 取与 V 自洽的 (二分), 减过的不再补\n按市值减不分部分; 须有买入部分",
     false, true},
};
inline const HowSpec &spec(How h) { return kHows[static_cast<int>(h)]; }

struct Cfg {
  How how = How::Hand;
  Side sell, buy;
  double unit = 10000, pct = 100;
  PxMode px = PxMode::Sweep;
  std::map<std::string, int64_t> sells; // 只存 > 0
};
inline bool valid(const Cfg &c) { return c.how != How::Over || c.buy.parts.any(); } // 卖超配须有买入部分 (V 定 W)

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
  // 卖出总 bar 的三段口径 [股] (按比例时; 否则皆 0): span = 计入 bar 全长的 (卖的范围内 = 持仓, 外 = 0) ⊇ part = 其中所选部分
  //   (只融资 = 融资部分, 只自有 = 有自有部分即持仓, 两部分 = 持仓) ⊇ full = 其中今日卖得满的 (盖住所选部分的可卖股数, 至多可卖).
  //   part − full = T+1 未解禁 (今日买入), span − part = 所选部分里没有仓位; 拖 bar 即各只卖 part 的同一比例, 至多 full
  int64_t span = 0, part = 0, full = 0;
  double px = 0;
  int64_t lot = 0, step = 0, sell = 0; // sell = 卖出股数 (sells 里的, 或卖超配自定的)
  Leg leg;                             // 卖
};

// 能成单的卖出股数里离 x 最近的 (down = 不超过 x 的最大的): 0 不卖、起卖股数 (够起买股数且够 unit 元) 起按步长递增 (或连同零股)、
//   全部可卖 (够 unit 元才有; by_bar = 按比例卖: 不管 unit, 卖满即可零碎)
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
  double power = 0, use = 0, cost = 0;
  Sides<double> fin;   // 融资腿额: 买 = 融资买入 F / 卖 = 卖出还款 R
  double C = 0, X = 0; // 卖后 现金 / 可用保证金
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

// 卖出股数: 卖超配 = 试算自定的, 否则 c.sells 里的; 至多可卖 (试算未随配置重算时, 滑杆照配置显示)
int64_t selling(const Hold &h, const Cfg &c);
// 卖出总 bar (按比例) 的读 ⇄ 写, 三段口径见 Hold::span / part / full:
//   gauge  各段额: all = Σ span (全长) / part = Σ max(part, full) / cap = Σ full (可拖上限) / got = Σ min(卖出, full) (显示)
//   spread 拖到 pct (0–100, 占全长) → 新的卖出设置: 各持仓卖其 part 的同一比例 r, 至多 full, 合计恰为 pct × 全长;
//          按能成单的档向下取 (snap, 照守最小单元; 卖满不管), 剩下不足一个最小单元且已过半的即卖满; 其余代码的设置照旧
struct Gauge {
  double all = 0, part = 0, cap = 0, got = 0;
};
Gauge gauge(const Plan &p, const Cfg &c);
std::map<std::string, int64_t> spread(const Plan &p, const Cfg &c, double pct);

nlohmann::json to_json(const Plan &p);
nlohmann::json to_json(const Cfg &c);

std::vector<std::string> lists();                            // 名单名, 新在前
std::vector<std::string> read_list(const std::string &name); // 名单 → 代码 (去重保序); 非 6 位代码 → Fail

} // namespace live::rebal
