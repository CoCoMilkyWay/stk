#pragma once

#include "misc/date.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/beast/core/tcp_stream.hpp>

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace sina {

// 代码一律新浪形 "sh600000" / "sz000001" / "bj920000"
inline constexpr int kLevels = 5;     // 盘口档数
inline constexpr int kQuoteMax = 800; // 一次批量快照至多几只 (实测 1000 只 URL 超长回 431)

inline constexpr const char *kQuoteHost = "hq.sinajs.cn";              // 批量快照 /list=
inline constexpr const char *kBarHost = "quotes.sina.cn";              // 1 分钟 K getKLineData
inline constexpr const char *kReferer = "https://finance.sina.com.cn"; // 明文 GET 须带 Referer
inline constexpr int kTimeoutSeconds = 5;                              // 单次连接 + 读写整体时长

// 一只的实时快照 (hq.sinajs.cn 一行; 名称 GBK 不取)
//   价 [元], 0 = 未开盘 / 停牌; 量 [股]、额 [元] 为当日累计; 盘口 0 = 空档
//   现价 price = micro price (卖一 / 买一按对手量加权); 任一侧空档 (涨跌停 / 未开盘 / 停牌) 取成交价
struct Quote {
  std::string sym;
  misc::Ymd date = 0;
  int sec = 0; // 行情时刻: 当日秒
  float open = 0, last_close = 0, price = 0, high = 0, low = 0;
  double volume = 0, amount = 0;
  float bid[kLevels] = {}, ask[kLevels] = {};
  double bid_vol[kLevels] = {}, ask_vol[kLevels] = {};
};

// 1 分钟 K (quotes.sina.cn getKLineData scale=1). 标号 = bar 结束分钟 (09:31 … 11:30, 13:01 … 15:00);
// 首根含开盘集合竞价, 无成交的分钟没有这根
struct Bar {
  misc::Ymd date = 0;
  int minute = 0; // 标号: hh · 60 + mm
  float open = 0, high = 0, low = 0, close = 0;
  double volume = 0, amount = 0; // [股] [元]
};

// 新浪行情 HTTP 客户端 — 与 bigquant / tushare::Client 对仗: 纯传输, 不知资产轴与缓存.
//   一个对象 = 一条 keep-alive 连接 (单线程用; 并发 = 每线程一个对象). 闲置超 kIdle 先重建 (对端会关闲连接),
//   要 gzip (批量快照 99K→29K、240 根分钟线 30K→5K). 网络瞬抖重建连接隔 1s 无限重试; 回包不符 → assert.
class Client {
public:
  explicit Client(std::string host); // kQuoteHost / kBarHost

  // 批量快照 (≤ kQuoteMax 只); 回包里查无此码 (空串) 的不出现, 顺序同回包
  std::vector<Quote> quotes(const std::vector<std::string> &syms);

  // sym 最近 n 根 1 分钟 K, 旧在前 (可跨日)
  std::vector<Bar> kline(std::string_view sym, int n);

private:
  std::string get(const std::string &target);

  std::string host_;
  boost::asio::io_context ioc_;
  std::optional<boost::beast::tcp_stream> stream_;
  std::chrono::steady_clock::time_point used_{};
};

} // namespace sina
