// Live 的行情取法: 订阅 (中枢每帧) / 试算现取 / 下单盘口三处同一个 — 新浪分批 (一次快照至多 sina::kQuoteMax 只)、每批网络错有界重试, 有一批取不到即整份不要
//   盘口 Books (下单用): 批量 = 后台一条常驻连接每 kGap 秒取一遍这批各只, 取用最新的一份; 收到已超 kStale 秒或还没这只 → 当场现取.
//     单笔 (secs 空) = 每次现取. 流取不到即留着上一份; 现取取不到即该单失败, 且 kStale 秒内不再现取 (断网时后面各单立刻失败、不排队等超时)
//     后台线程持 Feed 的 shared_ptr, 析构只置停 (在途的一问做完即退)
#pragma once

#include "api/sina/client.hpp"
#include "live/Book.hpp"

#include <chrono>
#include <memory>
#include <optional>
#include <vector>

namespace GUI::Live {

void merge(live::Quotes &q, const std::vector<sina::Quote> &xs); // 新浪的快照并进 q (按代码)
// 新浪取 secs 的行情: 超一次快照的分几批, 每批网络错至多试 tries 次; 有一批取不到 → 空
std::optional<live::Quotes> quotes_of(sina::Client &cl, const std::vector<live::Sec> &secs, int tries);

class Books {
public:
  struct Book {
    live::Quote q;
    double age = 0; // 收到距今 [s]; 现取 = 0
    bool fed = false;
  };
  explicit Books(const std::vector<live::Sec> &secs); // 空 = 单笔
  ~Books();
  Books(const Books &) = delete;
  Books &operator=(const Books &) = delete;

  Book get(const live::Sec &s); // 取不到 → Fail

private:
  using Clock = std::chrono::steady_clock;
  struct Feed;
  std::shared_ptr<Feed> feed_;
  std::optional<sina::Client> cl_;        // 现取的连接 (头一回现取才建)
  std::optional<Clock::time_point> down_; // 上一回现取取不到于
};

} // namespace GUI::Live
