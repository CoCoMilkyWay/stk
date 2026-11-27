#pragma once

// =============================================================================
// 标签协议的唯一真相: 持有期表 HOLDS / 冲击金额段 LABEL_AMTS / 交易时钟与结清窗 / 持有期 × 层 Hold + 协议表 1 / 2
// =============================================================================
//   写端 (features 标签节点; 字段表宏经 FieldsGenerated 引入本头) 与评估端 (factor Stat / 算子 / gui engine) 同一份, 本头只依赖标准库与 define/Geo.hpp.
//   持有期的身份 = HoldId (表下标 = LB 组序 = 时间序); 族 / 天数由 hold_kind / hold_n 取 (主机 / 设备两用), 文字由 token 表取;
//   落在一层上的全部几何 (行数 / 开仓 / 交接 / 平仓行 / 记日 / 可知行 / 资金口径) = Hold (hold_on; 扫出的导出量 span / reach 查表 hold_geom).
//   行号一律取段内行 (seg_row): 张量与分块都以段首起, 与张量从哪一日起无关.
//   集合 = 位集 HoldSet / AmtSet, 有序视图 = IdList (Holds / Amts: 下标 i ↔ 第 i 个成员, 评估端的紧凑数组按它排).
// =============================================================================

#include "define/Geo.hpp"
#include "define/HostDev.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

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

// ---- 冲击金额段: 固定段 (无盘口列, 恒有) + 盘口段 LABEL_AMTS (万元, 升序; Lob 档 lb_cost_{buy,sell}_<a>w) ----
#define LABEL_AMTS(X, ...) X(5, __VA_ARGS__) X(30, __VA_ARGS__)
#define AMT_ID_ONE(a, ...) kAmt##a##w,
enum AmtId : uint8_t { kAmtFixed,
                       LABEL_AMTS(AMT_ID_ONE) kAmtCount };
#undef AMT_ID_ONE
#define AMT_WAN_ONE(a, ...) a,
inline constexpr int kAmtWan[] = {0, LABEL_AMTS(AMT_WAN_ONE)};
#undef AMT_WAN_ONE
static_assert(sizeof(kAmtWan) / sizeof(kAmtWan[0]) == kAmtCount);
inline std::string amt_name(AmtId a) { return a == kAmtFixed ? std::string("固定") : std::to_string(kAmtWan[a]) + "w"; }
// 万元 → 金额段 (0 = 固定段); 不在表里 → false
inline bool amt_of_wan(int wan, AmtId &a) {
  for (int i = 0; i < kAmtCount; ++i)
    if (kAmtWan[i] == wan) {
      a = static_cast<AmtId>(i);
      return true;
    }
  return false;
}

// ---- 集合 (位集) 与有序视图 (IdList: 成员按 id 升序) ----
using HoldSet = uint16_t;
using AmtSet = uint8_t;
static_assert(kHoldCount <= 16 && kAmtCount <= 8);
constexpr HoldSet hold_bit(HoldId h) { return static_cast<HoldSet>(1u << h); }
constexpr AmtSet amt_bit(AmtId a) { return static_cast<AmtSet>(1u << a); }
template <class Id, int N>
struct IdList {
  int n = 0;
  Id k[N] = {};
  // id 的下标; 不在 → −1
  int at(Id id) const {
    for (int i = 0; i < n; ++i)
      if (k[i] == id)
        return i;
    return -1;
  }
  uint32_t set() const {
    uint32_t s = 0;
    for (int i = 0; i < n; ++i)
      s |= 1u << k[i];
    return s;
  }
  bool operator==(const IdList &o) const { return set() == o.set(); }
  static IdList of(uint32_t set) {
    IdList o;
    for (int i = 0; i < N; ++i)
      if (set >> i & 1u)
        o.k[o.n++] = static_cast<Id>(i);
    return o;
  }
};
using Holds = IdList<HoldId, kHoldCount>;
using Amts = IdList<AmtId, kAmtCount>;

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
//     |            | 整批平仓 (不交接) 恒在撮合价点: 盘口段冲击 0, 固定段照付 Fee::impact                                     |                   |
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
  // 记日一对 (协议表 2【记日】的唯一实现): 排序统计记建仓日 ⇄ 钱记平仓日
  FACTOR_HD constexpr int stat_day(int t) const { return t / g.S; }
  FACTOR_HD constexpr int money_day(int t) const { return exit(t) / g.S; }
  // 行 s 的标签在哪一行收盘时已实现 (N 元回归的样本行): 撮合价点在平仓行收盘即知; 分钟价点 E 至迟下一行收盘可知 (+1)
  FACTOR_HD constexpr int known(int s) const {
    const bool at_e = hold_offset(id) && (hold_kind(id) == HoldKind::Min || !close_entry(s, g));
    return exit(s) + (at_e ? 1 : 0);
  }
  // 行 t 可用的最末标签行 (无 → −1)
  FACTOR_HD constexpr int last(int t) const {
    int u = t;
    while (u >= 0 && known(u) > t)
      --u;
    return u;
  }
  // t − last(t) 的上界 (行): N 元回归的回看 = d + 它 (查表)
  int reach() const;
  // 资金口径行数 h̄: 一日各开仓行的在持行数 (建仓行起, exit 止; 尾部行不开仓不计) 之和 / S (查表)
  double span() const;
};
FACTOR_HD constexpr Hold hold_on(HoldId id, const Geo &g) {
  const int n = hold_days(id);
  return {id, g, hold_kind(id) == HoldKind::Min ? hold_n(id) / g.N : g.S * (n < 1 ? 1 : n)};
}

// ---- 持有期 × 层的导出几何表 (各层各持有期一次扫出, 首次用到时建; 不合法的格不用) ----
struct HoldGeom {
  int reach = 0;
  double span = 0.0;
};
inline const HoldGeom &hold_geom(HoldId id, const Geo &g) {
  static const auto tab = [] {
    std::array<std::array<HoldGeom, kHoldCount>, kGeoCount> t{};
    for (int i = 0; i < kGeoCount; ++i)
      for (int k = 0; k < kHoldCount; ++k) {
        const Hold x = hold_on(static_cast<HoldId>(k), kGeos[i]);
        HoldGeom &o = t[static_cast<size_t>(i)][static_cast<size_t>(k)];
        const int t0 = (hold_days(x.id) + 2) * x.g.S + x.h + 1;
        for (int u = t0; u < t0 + x.g.S; ++u)
          o.reach = u - x.last(u) > o.reach ? u - x.last(u) : o.reach;
        int s = 0;
        for (int j = 0; j < x.g.S; ++j)
          s += x.opens(j) ? x.exit(j) - j : 0;
        o.span = static_cast<double>(s) / x.g.S;
      }
    return t;
  }();
  return tab[static_cast<size_t>(geo_index(g.S))][id];
}
inline int Hold::reach() const { return hold_geom(id, g).reach; }
inline double Hold::span() const { return hold_geom(id, g).span; }

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
