#pragma once

#include "misc/date.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/beast/core/tcp_stream.hpp>

#include <cassert>
#include <cctype>
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
inline constexpr const char *kBarHost = "quotes.sina.cn";              // 1 分钟 K / 日 K getKLineData
inline constexpr const char *kReferer = "https://finance.sina.com.cn"; // 明文 GET 须带 Referer
inline constexpr int kTimeoutSeconds = 5;                              // 单次连接 + 读写整体时长

// 全局轴码 "600000.SH" → "sh600000"
inline std::string sym_of(std::string_view code) {
  const size_t dot = code.find('.');
  assert(dot != std::string_view::npos && "sina: 轴码缺后缀");
  std::string s(code.substr(dot + 1));
  s += code.substr(0, dot);
  for (char &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

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

// 1 分钟 K 一根 (quotes.sina.cn getKLineData, scale=1): 标号 = bar 结束分钟 (09:31 … 11:30, 13:01 … 15:00),
// 首根含开盘集合竞价, 无成交的分钟没有这根
struct Bar {
  misc::Ymd date = 0;
  int minute = 0; // 标号: hh · 60 + mm
  float open = 0, high = 0, low = 0, close = 0;
  double volume = 0, amount = 0; // [股] [元]
};

// 新浪行情 HTTP 客户端 — 与 bigquant / tushare::Client 对仗: 纯传输, 不知资产轴与缓存.
//   一个对象 = 一条 keep-alive 连接 (单线程用; 并发 = 每线程一个对象). 闲置超 kIdle 先重建 (对端会关闲连接),
//   要 gzip (批量快照 99K→29K、240 根分钟线 30K→5K).
//   一次调用 = 一试 (≤ kTimeoutSeconds; 复用的连接被对端关了即换新连接重发, 不算失败): 网络错 / 非 200 / 回包不认识 → 空
//   (诊断见 netlog: 连着失败只记头一回, 恢复时记一行),
//   不重试、不等; 再来与否、何时再来归调用方 (行情各路按节拍再来, 缺的由其补齐机制补回). 回包认得但字段不符 → assert.
class Client {
public:
  explicit Client(std::string host); // kQuoteHost / kBarHost

  // 批量快照 (≤ kQuoteMax 只); 回包里查无此码 (空串) 的不出现, 顺序同回包
  std::optional<std::vector<Quote>> quotes(const std::vector<std::string> &syms);
  // sym 最近 n 根 1 分钟 K, 旧在前 (可跨日)
  std::optional<std::vector<Bar>> kline(std::string_view sym, int n);

private:
  std::optional<std::string> fetch(const std::string &target);
  std::nullopt_t fail(const std::string &why); // 记一行 (连着失败只记头一回) → 空
  void ok();                                   // 一次取成: 之前在失败里则记 "恢复"
  static std::string list_target(const std::vector<std::string> &syms);
  static std::optional<std::vector<Quote>> parse_quotes(std::string_view body, size_t n);

  std::string host_;
  boost::asio::io_context ioc_;
  std::optional<boost::beast::tcp_stream> stream_;
  std::chrono::steady_clock::time_point used_{};
  bool down_ = false; // 在连着失败里
};

} // namespace sina
