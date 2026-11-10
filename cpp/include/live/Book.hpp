// 实盘: 5 档盘口, 相对参考价算滑点与价位; 委托价与整手规则 (下单与调仓试算同用)
//   Quote::side[] = 各方向吃的那侧: 买 +1 吃卖盘 (价位往上), 卖 −1 吃买盘 (价位往下); 空档已滤掉
//   滑点与价位两组列同构: (量, 方向), 滑点乘方向 → 正 = 成本. 只出数字, 格式化归 UI
#pragma once

#include "api/sina/client.hpp"
#include "live/Broker.hpp"

#include <array>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace live {

struct Level {
  double px = 0, vol = 0;
};

struct Quote {
  std::string sym; // 新浪码
  misc::Ymd date = 0;
  int sec = 0;      // 行情时刻: 当日秒
  double price = 0; // 现价; 未开盘 / 停牌取昨收
  double last_close = 0, open = 0, high = 0, low = 0, volume = 0;
  std::vector<Level> side[2];
  const std::vector<Level> &book(int s) const { return side[s < 0]; }
};
Quote quote_of(const sina::Quote &q);
using Quotes = std::unordered_map<std::string, Quote>; // 代码 → 行情
std::string quote_time(const Quote &q);                // HH:MM:SS

inline constexpr double kNotionals[3] = {1e4, 5e4, 15e4}; // 吃 n 元
inline constexpr double kPcts[2] = {1, 2};                // 偏 p%

// 委托价: 扫档 (对手盘累计恰好吃满该额那档) / 对手价 / 对手价再朝成交方向偏 1% / 本方价
enum class PxMode : uint8_t { Sweep,
                              Opp,
                              Opp1,
                              Own };
inline constexpr const char *kPxModeNames[4] = {"扫档", "对手价", "对手价 ±1%", "本方价"};

struct Col {
  double n; // 元 / %
  int s;
};
inline constexpr std::array<Col, 6> kSlipCols = {{{15e4, kBuy}, {5e4, kBuy}, {1e4, kBuy}, {1e4, kSell}, {5e4, kSell}, {15e4, kSell}}};
inline constexpr std::array<Col, 4> kLevelCols = {{{1, kBuy}, {1, kSell}, {2, kBuy}, {2, kSell}}};

struct Slip {
  std::optional<double> pct; // 万分比 × 方向 (正 = 成本)
  bool full = false;         // 5 档吃满; 否则实际更差
};

bool auction(const Quote &q);              // 集合竞价: 盘口只给虚拟撮合价, 卖一 ≤ 买一 (锁定 / 交叉), 此时滑点无意义
std::optional<double> ref(const Quote &q); // 参考价: micro price → 现价 → 昨收, 取首个可用
// 方向 s 做 notional 元按 mode 定委托价 (未舍入); 竞价中或该侧空盘 → 无
std::optional<double> order_px(const Quote &q, int s, PxMode mode, double notional);
std::pair<int, int> lot(const std::string &code); // (起买股数, 递增步长): 科创板起 200 零股递增, 北交所起 100 零股递增, 其余整手
// 吃 notional 元相对参考价的滑点 [元, 正 = 成本] 与 5 档是否吃满; 无参考价或竞价中 → 无
std::pair<std::optional<double>, bool> cost(const Quote &q, int s, double notional);
std::array<Slip, kSlipCols.size()> slips(const Quote &q, double r);
std::array<double, kLevelCols.size()> levels(double r); // r 朝方向 s 偏 p%

} // namespace live
