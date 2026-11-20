// 实盘: 5 档盘口、相对参考价算滑点、委托价与整手规则 (下单与调仓试算同用)
//   Quote::book = Sides<vector<Level>>: 买 +1 吃卖盘 (价往上), 卖 −1 吃买盘 (价往下); 空档已滤
//   Slips = Sides<array<Slip, 3>>: 买 3 档 ⇄ 卖 3 档镜像对仗, 滑点乘方向 → 正 = 成本
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
  int sec = -1;     // 行情时刻: 当日秒
  double price = 0; // 现价 (micro price); 未开盘 / 停牌取昨收
  double last_close = 0, open = 0, high = 0, low = 0, volume = 0;
  Sides<std::vector<Level>> book;
};
Quote quote_of(const sina::Quote &q);
using Quotes = std::unordered_map<std::string, Quote>; // 代码 → 行情

inline constexpr std::array<double, 3> kNotionals = {1e4, 5e4, 15e4}; // 吃 1W / 5W / 15W

enum class PxMode : uint8_t { Sweep,
                              Opp,
                              Opp1,
                              Own };
inline constexpr const char *kPxModeNames[4] = {"扫档", "对手价", "对手价 ±1%", "本方价"};

struct Slip {
  std::optional<double> px;   // 预期成交价 = 吃到的各档均价
  std::optional<double> pct;  // 万分比 × 方向 (正 = 成本)
  std::optional<double> cost; // 元 × 方向 (正 = 成本)
  bool full = false;          // 5 档吃满
};
using Slips = Sides<std::array<Slip, kNotionals.size()>>;

bool auction(const Quote &q);              // 集合竞价: 卖一 ≤ 买一
std::optional<double> ref(const Quote &q); // 参考价 = 现价 (含昨收兜底)
std::optional<double> order_px(const Quote &q, int s, PxMode mode, double notional);
std::pair<int64_t, int64_t> lot(std::string_view code); // (起买股数, 递增步长)
std::pair<std::optional<double>, bool> cost(const Quote &q, int s, double notional);
Slips slips(const Quote &q, double r);

} // namespace live
