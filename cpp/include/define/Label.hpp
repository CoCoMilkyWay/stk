#pragma once

// =============================================================================
// 标签协议的唯一真相: 持有期表 HOLDS / 冲击金额段 LABEL_AMTS / 交易时钟与结清窗 / 持有期 × 层 Hold + 协议表 1 / 2
// =============================================================================
//   写端 (features 标签节点; 字段表宏经 FieldsGenerated 引入本头) 与评估端 (factor Stat / 算子 / gui engine) 同一份, 本头只依赖标准库与 define/Geo.hpp.
//   持有期的身份 = HoldId (表下标 = LB 组序 = 时间序); 族 / 天数由 hold_kind / hold_n 取 (主机 / 设备两用), 文字由 token 表取;
//   落在一层上的全部几何 (行数 / 开仓 / 交接 / 平仓行 / 记日 / 可知行 / 资金口径) = Hold (hold_on; 扫出的导出量 span / reach 查编译期表 kHoldGeom).
//   行号一律取段内行 (seg_row): 张量与分块都以段首起, 与张量从哪一日起无关.
//   持有期集合 = IdList Holds (成员升序, 下标 i ↔ 第 i 个成员, 评估端的紧凑数组按它排); 盘口段集合 = 位集 LobMask.
// =============================================================================

#include "define/Geo.hpp"
#include "define/HostDev.hpp"

#include <array>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>

// ---- 持有期表: X(Id, token, Kind, n) ----
//   token = 列名片段 (lb_<token> 做多收益 / lb_nom_<token> 名义收益) = 落盘写法 = 表达式 h= 写法
//   Min    持仓 n 分钟: 名义平仓 = 行 m+n 的 entry 价点 (越过收盘 → 收盘价点)
//   Close  持有到当日收盘 (n = 0)
//   Open   T+n 开盘平仓 (跨日回填)
//   Same   T+n 同刻平仓 = D+n 日行 m 的 entry 价点 (与那时行 m 的建仓同一价点 → 两批交接; L_1day 行 = T+n 收盘)
// 按层截取: HOLDS_FROM_<LVL> = 本层可落的组 (滤掉比本层行还短的持有期; L_1day 只剩日级型), 序同 HOLDS; 与评估端 hold_ok 一致
#define HOLDS(X, ...) HOLDS_FROM_L_1min(X, __VA_ARGS__)
#define HOLDS_FROM_L_1min(X, ...) X(M1, 1m, Min, 1, __VA_ARGS__) HOLDS_FROM_L_5min(X, __VA_ARGS__)
#define HOLDS_FROM_L_5min(X, ...) X(M5, 5m, Min, 5, __VA_ARGS__) HOLDS_FROM_L_15min(X, __VA_ARGS__)
#define HOLDS_FROM_L_15min(X, ...) X(M15, 15m, Min, 15, __VA_ARGS__) HOLDS_FROM_L_30min(X, __VA_ARGS__)
#define HOLDS_FROM_L_30min(X, ...) X(M30, 30m, Min, 30, __VA_ARGS__) HOLDS_FROM_L_60min(X, __VA_ARGS__)
#define HOLDS_FROM_L_60min(X, ...) X(M60, 60m, Min, 60, __VA_ARGS__) X(Close, close, Close, 0, __VA_ARGS__) HOLDS_FROM_L_1day(X, __VA_ARGS__)
#define HOLDS_FROM_L_1day(X, ...) X(T1O, t1o, Open, 1, __VA_ARGS__) X(T1, t1, Same, 1, __VA_ARGS__) X(T2, t2, Same, 2, __VA_ARGS__) X(T3, t3, Same, 3, __VA_ARGS__)

enum class HoldKind : uint8_t { Min,
                                Close,
                                Open,
                                Same };
#define HOLD_ID_ONE(Id, tok, kind, n, ...) kHold##Id,
enum HoldId : uint8_t { HOLDS(HOLD_ID_ONE) kHoldCount };
#undef HOLD_ID_ONE

FACTOR_HD constexpr HoldKind hold_kind(HoldId h) {
  switch (h) {
#define HOLD_KIND_CASE(Id, tok, kind, n, ...) \
  case kHold##Id:                             \
    return HoldKind::kind;
    HOLDS(HOLD_KIND_CASE)
#undef HOLD_KIND_CASE
  default:
    break;
  }
  return HoldKind::Min;
}
FACTOR_HD constexpr int hold_n(HoldId h) {
  switch (h) {
#define HOLD_N_CASE(Id, tok, kind, n, ...) \
  case kHold##Id:                          \
    return n;
    HOLDS(HOLD_N_CASE)
#undef HOLD_N_CASE
  default:
    break;
  }
  return 0;
}
// 两族: 偏移型 (分钟型 / T+N 同刻: 平仓 = h 行后那一行的建仓价点, 两批恒交接; 落名义收益列) ⇄ 锚点型 (close / T+N 开盘: 固定时点平仓, 从不交接)
FACTOR_HD constexpr bool hold_offset(HoldId h) { return hold_kind(h) == HoldKind::Min || hold_kind(h) == HoldKind::Same; }
// 日级型 (跨日平仓: T+N 开盘 / 同刻)
FACTOR_HD constexpr bool hold_daily(HoldId h) { return hold_kind(h) == HoldKind::Open || hold_kind(h) == HoldKind::Same; }
// 日级型平仓日偏移 N (分钟型 / close 为 0)
FACTOR_HD constexpr int hold_days(HoldId h) { return hold_daily(h) ? hold_n(h) : 0; }

#define HOLD_TOKEN_ONE(Id, tok, kind, n, ...) #tok,
inline constexpr const char *kHoldToken[] = {HOLDS(HOLD_TOKEN_ONE)};
#undef HOLD_TOKEN_ONE
static_assert(sizeof(kHoldToken) / sizeof(kHoldToken[0]) == kHoldCount);
inline std::string_view hold_token(HoldId h) { return kHoldToken[h]; }
// 显示名: "5m" / "close" / "T+1O" / "T+2"
inline std::string hold_name(HoldId h) {
  switch (hold_kind(h)) {
  case HoldKind::Open:
    return "T+" + std::to_string(hold_n(h)) + "O";
  case HoldKind::Same:
    return "T+" + std::to_string(hold_n(h));
  default:
    return kHoldToken[h];
  }
}
// token → 持有期; 不在表里 → false
inline bool hold_parse(std::string_view s, HoldId &h) {
  for (int i = 0; i < kHoldCount; ++i)
    if (s == kHoldToken[i]) {
      h = static_cast<HoldId>(i);
      return true;
    }
  return false;
}

// ---- 冲击金额段: 盘口段 LobAmt (= Lob 档落盘列 lb_cost_{buy,sell}_<a>w, 万元升序; 平面 / 流水 / 装载一律按它索引) ⇄ 成本口径段 AmtId (固定段 + 各盘口段; 只作口径选择) ----
//   盘口段 k ↔ 口径段 k + 1 (amt_of / lob_of); 固定段无盘口列, 恒有 (全量 × Fee::impact); 装了哪些盘口段 = 位集 LobMask
#define LABEL_AMTS(X, ...) X(5, __VA_ARGS__) X(30, __VA_ARGS__)
#define LOB_AMT_ONE(a, ...) kLob##a##w,
enum LobAmt : uint8_t { LABEL_AMTS(LOB_AMT_ONE) kLobAmts };
#undef LOB_AMT_ONE
#define AMT_ID_ONE(a, ...) kAmt##a##w,
enum AmtId : uint8_t { kAmtFixed,
                       LABEL_AMTS(AMT_ID_ONE) kAmtCount };
#undef AMT_ID_ONE
#define LOB_WAN_ONE(a, ...) a,
inline constexpr int kLobWan[] = {LABEL_AMTS(LOB_WAN_ONE)};
#undef LOB_WAN_ONE
static_assert(sizeof(kLobWan) / sizeof(kLobWan[0]) == kLobAmts && kAmtCount == kLobAmts + 1);
static_assert([] {
  for (int k = 1; k < kLobAmts; ++k)
    if (kLobWan[k] <= kLobWan[k - 1])
      return false;
  return true;
}(),
              "LABEL_AMTS 须严格升序");
FACTOR_HD constexpr bool is_lob(AmtId a) { return a != kAmtFixed; }
constexpr LobAmt lob_of(AmtId a) { return static_cast<LobAmt>(a - 1); }
constexpr AmtId amt_of(LobAmt k) { return static_cast<AmtId>(k + 1); }
constexpr int amt_wan(AmtId a) { return is_lob(a) ? kLobWan[lob_of(a)] : 0; } // 万元 (固定段 0)
inline std::string amt_name(AmtId a) { return is_lob(a) ? std::to_string(amt_wan(a)) + "w" : std::string("固定"); }
// 万元 → 口径段 (0 = 固定段); 不在表里 → false
inline bool amt_of_wan(int wan, AmtId &a) {
  for (int i = 0; i < kAmtCount; ++i)
    if (amt_wan(static_cast<AmtId>(i)) == wan) {
      a = static_cast<AmtId>(i);
      return true;
    }
  return false;
}
// 盘口段位集 (位 k = 盘口段 k)
using LobMask = uint8_t;
static_assert(kLobAmts <= 8, "LobMask 用 uint8_t");
inline constexpr LobMask kAllLob = static_cast<LobMask>((1u << kLobAmts) - 1);
constexpr LobMask lob_bit(LobAmt k) { return static_cast<LobMask>(1u << k); }
constexpr bool lob_has(LobMask m, LobAmt k) { return (m >> k & 1u) != 0; }
// 口径段 a 在装了 m 的作用域上可算 (固定段恒可算)
constexpr bool amt_in(LobMask m, AmtId a) { return !is_lob(a) || lob_has(m, lob_of(a)); }

// ---- 集合 = IdList (唯一表示: 成员按 id 升序, 下标 i ↔ 第 i 个成员; 评估端的紧凑数组按它排) ----
template <class Id, int N>
struct IdList {
  int n = 0;
  Id k[N] = {};
  // id 的下标; 不在 → −1
  FACTOR_HD constexpr int at(Id id) const {
    for (int i = 0; i < n; ++i)
      if (k[i] == id)
        return i;
    return -1;
  }
  FACTOR_HD constexpr bool has(Id id) const { return at(id) >= 0; }
  // 并入 id (保持升序; 已在 → 不变)
  constexpr IdList with(Id id) const {
    if (has(id))
      return *this;
    IdList o;
    for (int i = 0; i < n && k[i] < id; ++i)
      o.k[o.n++] = k[i];
    o.k[o.n++] = id;
    for (int i = 0; i < n; ++i)
      if (k[i] > id)
        o.k[o.n++] = k[i];
    return o;
  }
  constexpr bool operator==(const IdList &o) const {
    if (n != o.n)
      return false;
    for (int i = 0; i < n; ++i)
      if (k[i] != o.k[i])
        return false;
    return true;
  }
  static constexpr IdList of(std::initializer_list<Id> ids) {
    IdList o;
    for (const Id id : ids)
      o = o.with(id);
    return o;
  }
};
using Holds = IdList<HoldId, kHoldCount>;
// 盘口段位集 m 下可算的口径段 (固定段起, 升序; 只给 UI 逐段列举, 存储一律 LobMask)
using AmtList = IdList<AmtId, kAmtCount>;
constexpr AmtList amts_of(LobMask m) {
  AmtList o;
  for (int i = 0; i < kAmtCount; ++i)
    if (amt_in(m, static_cast<AmtId>(i)))
      o.k[o.n++] = static_cast<AmtId>(i);
  return o;
}

// ---- 交易时钟 (L_1min 行号) ----
inline constexpr int kAuctionMin = 10;                   // 09:25 开盘撮合 bar
inline constexpr int kContOpenMin = 15;                  // 连续竞价 [09:30, 14:57) = L_1min 行 [kContOpenMin, kContCloseMin)
inline constexpr int kContCloseMin = 252;                // 收盘集合竞价起点 (14:57; 行 252..254)
inline constexpr int kLastAnchorRow = kContCloseMin - 2; // 最后一个 entry 锚点可成交的行 (锚点 = 下一分钟起始, 须在 14:57 前)

// ---- 结清窗 ----
inline constexpr int kHoldDaysMax = [] { // 日级型最远名义平仓日 (行所在日起第几个交易日)
  int m = 0;
  for (int h = 0; h < kHoldCount; ++h)
    m = hold_days(static_cast<HoldId>(h)) > m ? hold_days(static_cast<HoldId>(h)) : m;
  return m;
}();
inline constexpr int kDeferDays = 5;                            // 名义平仓日之后至少还能顺延的交易日数 (环满按最近收盘价结清)
inline constexpr int kPendDays = kHoldDaysMax + kDeferDays + 1; // 悬挂日环 (含当日): 同时未写完的日数上限
inline constexpr int kChainDays = kPendDays - 1;                // 交接链至多回走的交易日数 (= 平仓结清窗)
static_assert(kPendDays == 9, "LABEL_DELAY_NOTE 的天数随 kPendDays − 1 改");
#define LABEL_DELAY_NOTE "行所在日起8个交易日"

// =============================================================================
// 持有期 × 层 = Hold (协议表的一行落在层几何 g 上; 主机 / 设备两用, 构造一次, 热循环只读成员)
// =============================================================================
//   【协议表 1: 持有期 → 开仓 / 清仓 / 价】(features 标签 / Stat / 净值 / UI hold_points 同一份; 改口径先改这里, 标签 kLabelSemver +1)
//     价点: E    = 行末分钟价点 = 之后首个有成交连续竞价分钟的 VWAP; 锚点后再无连续竞价 (≥ 14:57) → C(D)
//           C(D) = D 日收盘集合竞价撮合价 (末笔成交);  O(D) = D 日 09:25 开盘撮合价 (无撮合 → 首个有成交连续竞价 bar 的 open)
//     | 显示   | token | 适用层            | 开仓                   | 清仓                        | h (rows)  | h̄ (span; J = 日内开仓行数) |
//     |--------|-------|-------------------|------------------------|-----------------------------|-----------|----------------------------|
//     | <n>m   | <n>m  | 行长 ≤ n 的分钟层 | C(行) @ E              | C(行)+n @ E (过 14:57 掩)   | n / N     | n / N · J / S              |
//     | close  | close | 非 day            | C(行) @ E              | C(D) @ 收盘撮合             | S         | Σ_{j<J} (S − 1 − j) / S    |
//     | T+1O   | t1o   | 全部              | C(行) @ E; day 层 C(D) | O(D+1) @ 开盘撮合           | S         | (S + 1) / 2 + open_row     |
//     | T+N    | t<N>  | 全部              | C(行) @ E; day 层 C(D) | D+N 日同一行 @ E (day: C)   | N · S     | N · S                      |
//
//   【协议表 2: 开关仓策略】
//     | 环节       | 规则                                                                                                      | 落在              |
//     |------------|-----------------------------------------------------------------------------------------------------------|-------------------|
//     | 可成交判据 | 分钟价点 (Tradable): 可买 = low < 涨停 − ε, 可卖 = high > 跌停 + ε; 开盘 / 收盘撮合价同式; 涨跌停 NaN → 可成交 | LabelBar          |
//     | 开仓       | 组合只做多: E 价点可买才新建; 交接的续持者不必成交 (真持有); 不顺延开仓                                  | Stat【仓位感知】  |
//     | 续持       | 下一批交接且同组合 → 不真平仓, 名义价点估值 (lv 或 nv)                                                    | Stat【仓位感知】  |
//     | 平仓       | 名义价点卖不出或名义日停牌 → 顺延到之后首个可卖价点; 行所在日起 kPendDays − 1 个交易日仍未平 → 最近收盘价结清 | 标签              |
//     | 尾部       | 分钟型 / 收盘型 minute(t) + n > kLastAnchorRow 的行不开仓 (opens 为假)                                    | Hold              |
//     | 仓位       | 每行一批, 资金 1/h̄, 批内按目标权 (等权 1 / max(只数, cap) 或外给权); 全日平均满仓                        | Stat【换手】/ 净值 |
//     | 换手       | 前一批的权随价漂移到交接点, 与本批目标权逐格之差 (只在交接时; 否则整建整平, 平仓量亦随价漂移)            | Stat【换手】      |
//     | 成本       | 两侧逐格费率: 税佣 (Fee::tax) + 冲击 (盘口段 = 真实买 / 卖冲击封顶 CostSpec::top; 固定段 = Fee::impact);     | Stat【换手】      |
//     |            | 撮合价点 (收盘 / 开盘; 日层每行) 盘口冲击 0; 整批平仓 (不交接) 按平仓价点: E (Hold::exit_e) 付平仓行卖出冲击,   |                   |
//     |            | 撮合价 0; 价点卖不出 (封跌停) 的卖出量付固定冲击; 固定段一律照付 Fee::impact                              |                   |
//     | 净值       | 行 t 的批收益在 exit(t) 实现 × 1/h̄, 日内求和, 跨日复利; 成本在建仓行即付                                  | 净值 Realize      |
//     | 记日       | 排序统计 (rIC / 分组) 记在建仓日 stat_day; 钱 (日收益 / 净值 / 年化) 记在平仓日 money_day = 建仓日 + hold_days | Hold              |
//     | 复权       | 标签价 × 当日复权比: 跨日收益不含除权缺口                                                                 | 标签              |
// =============================================================================

// 行号约定: 张量 / 分块恒以段首起 (整日对齐), 行 t 的段内行 = t mod S 与张量从哪一日起无关; 负行 (前一批越过块首) 照取模
FACTOR_HD constexpr int seg_row(int t, const Geo &g) {
  const int j = t % g.S;
  return j < 0 ? j + g.S : j;
}
// 行 t 按收盘价建仓: 行末分钟越过最后可成交锚点, 价点 E 退化为 C(D) (日层每行都是)
FACTOR_HD constexpr bool close_entry(int t, const Geo &g) { return g.minute(seg_row(t, g)) > kLastAnchorRow; }
// 开盘撮合价 O(D) 所在的段内行: 末分钟不早于 09:25 撮合 bar 的首行 (L_1min = 行 10; 分钟箱 / 日 = 行 0)
FACTOR_HD constexpr int open_row(const Geo &g) {
  int j = 0;
  while (g.minute(j) < kAuctionMin)
    ++j;
  return j;
}
// 持有期在层 g 上是否合法: 分钟型 n 是行长 N 的整数倍且首行 minute(0) + n ≤ kLastAnchorRow; close 要求 S > 1; 日级型各层皆可
FACTOR_HD constexpr bool hold_ok(HoldId h, const Geo &g) {
  switch (hold_kind(h)) {
  case HoldKind::Min:
    return hold_n(h) % g.N == 0 && g.minute(0) + hold_n(h) <= kLastAnchorRow;
  case HoldKind::Close:
    return g.S > 1;
  default:
    return true;
  }
}

struct Hold {
  HoldId id = kHoldM1;
  Geo g;
  int h = 1; // 独立样本口径的行数 (lag / 交接间距 / 重叠折算): 分钟型 = n / N; close / T+1O = 一段 S 行; T+N 同刻 = N 段

  // 行 t 开仓 (非段末尾部): 分钟型 exit 锚点越过最后可成交锚点 (持有期缩短) / 收盘型 entry 越过 (即建即平) → 不开仓; 日级型恒开
  FACTOR_HD constexpr bool opens(int t) const {
    const HoldKind k = hold_kind(id);
    return !((k == HoldKind::Min || k == HoldKind::Close) && g.minute(seg_row(t, g)) + (k == HoldKind::Min ? hold_n(id) : 0) > kLastAnchorRow);
  }
  // 交接: 行 t 建仓与行 t − h 平仓是同一价点 (偏移型恒是, 锚点型从不); 行 t − h 在不在块内由调用方 (Stat Chain) 判
  FACTOR_HD constexpr bool nets(int t) const { return hold_offset(id) && opens(t) && opens(t - h); }
  // 平仓行: 行 t (≥ 0) 这一批的平仓价点所在行 (收益在此实现). 偏移型 = t + h; 收盘型 = 当日末行; T+N 开盘型 = D+N 日 open_row. 顺延不改实现行
  FACTOR_HD constexpr int exit(int t) const {
    if (hold_offset(id))
      return t + h;
    const int d = t / g.S + hold_days(id);
    return hold_kind(id) == HoldKind::Open ? d * g.S + open_row(g) : d * g.S + g.S - 1;
  }
  // 记日一对 (协议表 2【记日】的唯一实现): 排序统计记建仓日 ⇄ 钱记平仓日; 开仓行恒有 平仓日 = 建仓日 + hold_days (编译期表核), 按日的钱线直接平移
  FACTOR_HD constexpr int stat_day(int t) const { return t / g.S; }
  FACTOR_HD constexpr int money_day(int t) const { return exit(t) / g.S; }
  FACTOR_HD constexpr int money_lag() const { return hold_days(id); }
  // 行 t 这一批的平仓价点是分钟价点 E (连续竞价, 付盘口冲击) 而非撮合价 (收盘 / 开盘, 冲击 0): 偏移型且平仓行不是收盘建仓行
  FACTOR_HD constexpr bool exit_e(int t) const { return hold_offset(id) && (hold_kind(id) == HoldKind::Min || !close_entry(t, g)); }
  // 行 s 的标签在哪一行收盘时已实现 (N 元回归的样本行): 撮合价点在平仓行收盘即知; 分钟价点 E 至迟下一行收盘可知 (+1)
  FACTOR_HD constexpr int known(int s) const { return exit(s) + (exit_e(s) ? 1 : 0); }
  // 行 t 可用的最末标签行 (无 → −1)
  FACTOR_HD constexpr int last(int t) const {
    int u = t;
    while (u >= 0 && known(u) > t)
      --u;
    return u;
  }
  // t − last(t) 的上界 (行): N 元回归的回看 = d + 它 (查编译期表, 宿主)
  constexpr int reach() const;
  // 资金口径行数 h̄: 一日各开仓行的在持行数 (建仓行起, exit 止; 尾部行不开仓不计) 之和 / S (查编译期表, 宿主)
  constexpr double span() const;
};
FACTOR_HD constexpr Hold hold_on(HoldId id, const Geo &g) {
  const int n = hold_days(id);
  return {id, g, hold_kind(id) == HoldKind::Min ? hold_n(id) / g.N : g.S * (n < 1 ? 1 : n)};
}

// ---- 持有期 × 层的导出几何表 (编译期一次扫出; 不合法的格不用) ----
//   reach: known 沿行单调不减 (表内核) ⇒ [known(s), known(s + 1)) 内的行 u 都有 last(u) = s, 沿 s 一趟扫 (每行 known 只算一次);
//   ok: known 单调 ∧ 开仓行的平仓日 = 建仓日 + hold_days
struct HoldGeom {
  int reach = 0;
  double span = 0.0;
  bool ok = true;
};
constexpr HoldGeom hold_geom_of(const Hold &x) {
  HoldGeom o;
  const int t0 = (hold_days(x.id) + 2) * x.g.S + x.h + 1, t1 = t0 + x.g.S; // 扫的行 [t0, t1)
  int k0 = -1 << 30;                                                       // known(s), s = −1 时为 −∞
  for (int s = -1; k0 < t1; ++s) {
    const int k1 = x.known(s + 1);
    o.ok = o.ok && k1 >= k0;
    const int lo = k0 > t0 ? k0 : t0, hi = k1 < t1 ? k1 : t1; // last(u) = s 的行 ∩ 扫的行
    if (lo < hi && hi - 1 - s > o.reach)
      o.reach = hi - 1 - s;
    k0 = k1;
  }
  int s = 0;
  for (int j = 0; j < x.g.S; ++j) {
    o.ok = o.ok && (!x.opens(j) || x.money_day(j) == x.stat_day(j) + x.money_lag());
    s += x.opens(j) ? x.exit(j) - j : 0;
  }
  o.span = static_cast<double>(s) / x.g.S;
  return o;
}
// 一格一个常量求值 (各自的求值步数预算), 再拼成表 [层 · kHoldCount + 持有期]
template <int G, int K>
inline constexpr HoldGeom kHoldGeomAt = hold_ok(static_cast<HoldId>(K), kGeos[G]) ? hold_geom_of(hold_on(static_cast<HoldId>(K), kGeos[G])) : HoldGeom{};
template <size_t... I>
constexpr std::array<HoldGeom, sizeof...(I)> hold_geom_table(std::index_sequence<I...>) {
  return {kHoldGeomAt<static_cast<int>(I / kHoldCount), static_cast<int>(I % kHoldCount)>...};
}
inline constexpr auto kHoldGeom = hold_geom_table(std::make_index_sequence<static_cast<size_t>(kGeoCount) * kHoldCount>{});
static_assert([] {
  for (const HoldGeom &x : kHoldGeom)
    if (!x.ok)
      return false;
  return true;
}(),
              "持有期几何: known 须沿行单调, 开仓行平仓日须 = 建仓日 + hold_days");
constexpr int Hold::reach() const { return kHoldGeom[static_cast<size_t>(geo_index(g.S)) * kHoldCount + id].reach; }
constexpr double Hold::span() const { return kHoldGeom[static_cast<size_t>(geo_index(g.S)) * kHoldCount + id].span; }

// ---- 字段描述片段 (英 / 中, 按族 token 拼) ----
#define LABEL_ENTRY_TEX R"(\mathbf{1}[\mathrm{buy}_{entry}] + 2\cdot\mathbf{1}[\mathrm{sell}_{entry}])"
#define LABEL_EN_Min(n) #n "min"
#define LABEL_EN_Close(n) "to-Close"
#define LABEL_EN_Open(n) "T+" #n " Open"
#define LABEL_EN_Same(n) "T+" #n " Same-Time"
#define LABEL_CN_Min(n) #n "分钟"
#define LABEL_CN_Close(n) "至收盘"
#define LABEL_CN_Open(n) "至T+" #n "开盘"
#define LABEL_CN_Same(n) "至T+" #n "同刻"
