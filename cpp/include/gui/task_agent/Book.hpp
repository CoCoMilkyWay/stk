#pragma once

// =============================================================================
// Book — 盘口重放 (view_book) 的原料: 一只股票一天里的一段逐笔重放进订单簿 (BookSpec → Book); 不经特征库, 不限评估期
// =============================================================================
//   时刻一律钟面秒 (h·3600 + m·60 + s); 交易秒 = 钟面秒在两个交易时段上的连续编号 (trade_sec ⇄ trade_clock)
//   产出方 task_agent/services/ReplayService (Lab 的 Replay 道), 成文方 task_agent/Views; 本头不碰项目
// =============================================================================

#include "gui/task_agent/Enum.hpp"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Agent {

// 交易时段 [kAmOpen, kAmClose) ∪ [kPmOpen, kPmClose); 开盘竞价 [kAmOpen, kOpenCall) / 收盘竞价 [kCloseCall, kPmClose)
inline constexpr int kAmOpen = 9 * 3600 + 15 * 60, kAmClose = 11 * 3600 + 30 * 60, kPmOpen = 13 * 3600, kPmClose = 15 * 3600;
inline constexpr int kOpenCall = 9 * 3600 + 25 * 60, kCloseCall = 14 * 3600 + 57 * 60;
inline constexpr int kTradeSecs = (kAmClose - kAmOpen) + (kPmClose - kPmOpen);
constexpr bool in_session(int clock) { return (clock >= kAmOpen && clock < kAmClose) || (clock >= kPmOpen && clock < kPmClose); }
// 钟面秒 → 交易秒: 09:15:00 → 0 … 11:29:59 → 8099, 13:00:00 → 8100 … 14:59:59 → 15299; 午休钳到 8100, 盘前 0, 盘后 15299.
// 与 L_sec 不同, 收盘集合竞价 14:57–15:00 逐秒可分
constexpr int trade_sec(int clock) {
  return clock < kAmOpen    ? 0
         : clock < kAmClose ? clock - kAmOpen
         : clock < kPmOpen  ? kAmClose - kAmOpen
         : clock < kPmClose ? kAmClose - kAmOpen + clock - kPmOpen
                            : kTradeSecs - 1;
}
constexpr int trade_clock(int sec) { return sec < kAmClose - kAmOpen ? kAmOpen + sec : kPmOpen + sec - (kAmClose - kAmOpen); }
static_assert(kTradeSecs == 15300 && trade_sec(kAmClose - 1) == 8099 && trade_sec(kPmOpen) == 8100 && trade_sec(kPmClose - 1) == kTradeSecs - 1 && trade_clock(8100) == kPmOpen);

inline constexpr float kBigQuantile = 0.99f; // 大单门槛缺省 = 当日新挂单额的这一分位

struct BookSpec {
  std::string code;             // 经 Lab::code_of
  int date = 0;                 // YYYYMMDD
  int from = 0, to = 0, at = 0; // 窗口 [from, to) / 全簿剖面取 at 这一秒末 (∈ 窗口); 都在交易时段内
  double big = 0;               // 大单门槛 (元); 0 = 当日新挂单额的 kBigQuantile 分位
};
// 窗口里的一秒 (下标 0 = 买侧 / 1 = 卖侧; 额 = 元)
struct BookSec {
  int clock = 0;                                      // 钟面秒
  float bid = 0, ask = 0;                             // 秒末买一 / 卖一 (该侧无真实价位 = 0)
  float depth[2] = {};                                // 秒末前 5 价位挂单额
  float ref = 0, matched = 0, imbalance = 0;          // 秒末竞价预撮合: 参考价 / 匹配额 / 未匹配额 (+ 买剩 − 卖剩); 非竞价或簿未交叉 = 0
  float taker[2] = {}, maker[2] = {}, cancel[2] = {}; // 秒内主动成交额 (按主动方) / 新挂额 / 撤单额
  float last = 0;                                     // 秒内末笔成交价 (无成交 = 0)
};
enum class BookKind : uint8_t { New,     // 新挂
                                Cancel,  // 撤单
                                Filled,  // 挂单被吃完
                                Sweep }; // 扫单: 连续竞价里一个主动单连吃 ≥ 2 个价位, 或成交额 ≥ 门槛
// 事件表 (下标 = BookKind): cn = 成文里的名字
struct BookKindDef {
  const char *cn;
};
inline constexpr BookKindDef kBookKinds[] = {{"新挂"}, {"撤单"}, {"被吃完"}, {"扫单"}};
AGENT_ROWS(BookKind, kBookKinds, Sweep);
struct BookEvent {
  int clock = 0, cs = 0; // 钟面秒 + 10ms
  BookKind kind = BookKind::New;
  int side = 0;     // 委托所在侧 (Sweep = 主动方)
  int levels = 0;   // Sweep 吃过的价位数
  float price = 0;  // 元 (Sweep = 末笔价)
  float first = 0;  // Sweep = 首笔价; 其余 = price
  float amount = 0; // 元: New / Cancel = 本笔委托 / 撤单额; Filled = 申报额; Sweep = 成交额
  float life = -1;  // Cancel / Filled: 距挂出的交易秒 (午休不计); 挂单时刻不可信 (乱序占位单) = −1
};
struct Book {
  BookSpec spec;     // 所放的那段
  std::string error; // 非空 = 没重放成 (当日无逐笔), 以下字段空
  int orders = 0;    // 当日逐笔条数
  float open = 0, high = 0, low = 0, close = 0;
  double amount = 0;                         // 当日成交额 (元)
  double big = 0;                            // 所用大单门槛 (元)
  std::vector<BookSec> secs;                 // 窗口逐秒
  std::vector<BookEvent> events;             // 窗口内的大单事件, 时序
  std::vector<std::pair<float, float>> book; // 剖面: (价, 挂单额 元; + 买 − 卖), 价升序
};

} // namespace GUI::Agent
