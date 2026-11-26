#pragma once

// =============================================================================
// 标签协议的唯一真相: 持有期表 HOLDS / 冲击金额段 LABEL_AMTS / 交易时钟与结清窗
// =============================================================================
//   写端 (features 标签节点; 字段表宏经 FieldsGenerated 引入本头) 与评估端 (factor Stat / gui engine) 同一份, 本头只依赖标准库.
//   持有期的身份 = HoldId (表下标 = LB 组序 = 时间序); 族 / 天数由 hold_kind / hold_n 取 (主机 / 设备两用), 文字由 token 表取.
//   集合 = 位集 HoldSet / AmtSet, 有序视图 = IdList (Holds / Amts: 下标 i ↔ 第 i 个成员, 评估端的紧凑数组按它排).
// =============================================================================

#include "define/HostDev.hpp"

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
