// 实盘: 5 档盘口, 相对参考价算滑点; 委托价与整手规则 (下单与调仓试算同用)
//   Quote::book = 各方向吃的那侧 (Sides): 买 +1 吃卖盘 (价位往上), 卖 −1 吃买盘 (价位往下); 空档已滤掉
//   滑点 Slips = 各方向 × 各额 (kNotionals), 买卖镜像同形; 滑点乘方向 → 正 = 成本. 只出数字, 格式化归 UI
#pragma once

#include "api/sina/client.hpp"
#include "live/Sides.hpp"
#include "misc/date.hpp"

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
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
  int sec = -1;     // 行情时刻: 当日秒
  double price = 0; // 现价 (micro price, 见 sina::Quote); 未开盘 / 停牌取昨收
  double last_close = 0, open = 0, high = 0, low = 0, volume = 0;
  Sides<std::vector<Level>> book;
};
Quote quote_of(const sina::Quote &q);
using Quotes = std::unordered_map<std::string, Quote>; // 代码 → 行情

inline constexpr std::array<double, 3> kNotionals = {1e4, 5e4, 15e4}; // 吃 n 元

// 委托价: 扫档 (对手盘累计恰好吃满该额那档) / 对手价 / 对手价再朝成交方向偏 1% / 本方价
enum class PxMode : uint8_t { Sweep,
                              Opp,
                              Opp1,
                              Own };
inline constexpr const char *kPxModeNames[4] = {"扫档", "对手价", "对手价 ±1%", "本方价"};

// 吃 n 元: 对手盘空 → px / pct / cost 皆无
struct Slip {
  std::optional<double> px;   // 预期成交价 = 吃到的各档均价
  std::optional<double> pct;  // 万分比 × 方向 (正 = 成本)
  std::optional<double> cost; // 元 × 方向 (正 = 成本) = pct × n
  bool full = false;          // 5 档吃满; 否则实际更差
};
using Slips = Sides<std::array<Slip, kNotionals.size()>>; // [方向][额]

bool auction(const Quote &q);              // 集合竞价: 盘口只给虚拟撮合价, 卖一 ≤ 买一 (锁定 / 交叉), 此时滑点无意义
std::optional<double> ref(const Quote &q); // 参考价 = 现价 (含昨收兜底); 都无 → 无
// 方向 s 做 notional 元按 mode 定委托价 (未舍入); 该侧空盘 → 无.
// 竞价中盘口只一档虚拟撮合价: 扫档 / 对手价 / 本方价都取它, 对手价 ±1% 照偏 (集合竞价按单一价成交, 偏出去不多花钱、更易成)
std::optional<double> order_px(const Quote &q, int s, PxMode mode, double notional);
std::pair<int64_t, int64_t> lot(std::string_view code); // (起买股数, 递增步长): 科创板起 200 零股递增, 北交所起 100 零股递增, 其余整手
// 方向 s 按额 yuan 元、委托价 px 折股: 向下取整步长; 卖出不足起卖量即抬到起卖量 (可卖不足由客户端的最大可卖封顶)
int64_t shares_of(std::string_view code, int s, double yuan, double px);
// 吃 notional 元相对参考价的滑点 [元, 正 = 成本] 与 5 档是否吃满; 无参考价或竞价中 → 无
std::pair<std::optional<double>, bool> cost(const Quote &q, int s, double notional);
Slips slips(const Quote &q, double r); // 各方向 × 各额的滑点, 相对参考价 r

} // namespace live
