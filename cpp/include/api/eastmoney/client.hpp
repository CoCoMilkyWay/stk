#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/beast/core/tcp_stream.hpp>

#include <chrono>
#include <optional>
#include <string>
#include <vector>

namespace eastmoney {

// 板块: 代码 "BK0493" + 名称 (UTF-8)
struct Board {
  std::string code, name;
};

// 个股: 代码 "600000.SH" / "000001.SZ" / "920000.BJ" (A 轴码形) + 所属概念板块名称 (无 = 空)
struct Stock {
  std::string sym;
  std::vector<std::string> concepts;
};

// 东方财富行情列表 HTTP 客户端 (push2 /api/qt/clist/get) — 与 sina::Client 对仗: 纯传输, 不知资产轴与缓存.
//   一个对象 = 一条 keep-alive 连接 (单线程用). 闲置超 kIdle 先重建; 列表按代码升序分页 (每页 ≤ kPage), 页数按回包 total 定,
//   拼齐条数须恰 = total 且代码不重. 网络瞬抖重建连接重试 (预算见 config), 用尽 / 回包不符 → assert.
class Client {
public:
  Client(); // config::EASTMONEY_HOST

  // 全部概念板块 (fs = m:90 t:3)
  std::vector<Board> concepts();

  // 全 A 个股 (沪深主板 / 创业板 / 科创板 / 北交所) 及所属概念 (f103: 名称逗号分隔, 与 concepts() 名称同一套)
  std::vector<Stock> stocks();

private:
  std::string get(const std::string &target);

  boost::asio::io_context ioc_;
  std::optional<boost::beast::tcp_stream> stream_;
  std::chrono::steady_clock::time_point used_{};
};

} // namespace eastmoney
