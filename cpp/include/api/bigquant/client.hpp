#pragma once

#include <arrow/flight/client.h>
#include <arrow/table.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace bigquant {

// filters 与 SDK 一致: {column -> [v1, v2, ...]}. server-side 列裁剪 + 谓词下推关键入口.
//   - 用 std::string 而非具体 type: 所有 filter 值最终都序列化为 JSON 字符串数组.
//   - 例: {{"date", {"2024-12-31", "2024-12-31"}}} == python {"date": ["2024-12-31","2024-12-31"]}
using Filters = std::map<std::string, std::vector<std::string>>;

// 一次查询 (值类型) — 与 tushare::Query 对仗
//   sql:     标准 DuckDB-flavored SQL (DAI 底层用 DuckDB)
//   filters: 服务端列过滤 / 分区下推
//   order:   结果行序 (ORDER BY 子句体, 列 = sql 的输出列); 空 = 不定
struct Query {
  std::string sql;
  Filters filters;
  std::string order = {};
};

// 账户周配额 (Flight DoAction("quota") 响应的 weekly_quota / used_quota).
//   计量单位 = 查询返回结果的顶层 cell 数 (rows × cols), 与服务端扫描窗口无关
//   (实测: 同一条 MAX(date) 子查询, 窗口取 3 个月与取 11 年 delta 完全相同);
//   LIST / STRUCT 内的元素不计 ⇒ query 每条恒 1 cell (见 Client::query).
//   used 每周重置一次, 服务端不返回重置时刻.
struct Quota {
  std::int64_t weekly = 0;
  std::int64_t used = 0;
};

// BigQuant DAI 客户端 — 纯数据面 (Arrow Flight) 等价实现. 与 tushare::Client 对仗: 纯传输, 不知 Spec.
//
// 协议:
//   grpc+tcp://bigquant.com:17010 (明文 gRPC, 无 TLS)
//   认证: Basic Token (ak/sk) -> JWT bearer, 每个 DoGet 自动挂 authorization header
//   ticket payload = UTF-8 JSON {sql, full_db_scan, filters, params}
//
// 生命周期:
//   - 构造即可用; Flight 客户端 + JWT 在首次 query() 时 lazy 建立.
//   - JWT TTL 约 12h, 过期由调用方重建 Client.
//   - 所有错误一律 assert; gRPC 自带 backoff, 这里不做应用层重试.
class Client {
public:
  // 缺省构造: ak/sk 取 config.hpp::BIGQUANT_AK/SK
  Client();
  // 显式指定 (测试 / 多账号切换)
  Client(std::string ak, std::string sk);

  // 执行一次查询, 走 Arrow Flight DoGet; 结果收成一格 (1 cell) 传回再拆开:
  //   SELECT LIST(q ORDER BY order) AS p FROM (sql) q  →  1 行 × 1 列  →  sql 的结果表
  //   拆回与直取逐列同形 (列序 / 列名 / 列型 / 可空); 0 行 = NULL 列表, 列型照带; 行序只由 order 保证
  // 返回 arrow::Table (整张读完一次性返); server-side 配额耗尽 / 权限缺失 / SQL 错误均 assert.
  std::shared_ptr<arrow::Table> query(const Query &q);

  // 取账户周配额, 走 Flight DoAction("quota") (与 query 共用同一条认证连接).
  // preflight 展示用; 不消耗配额本身.
  Quota quota();

private:
  void ensure_flight();

  std::string ak_;
  std::string sk_;
  std::unique_ptr<arrow::flight::FlightClient> flight_;
  // Pair<header_name, header_value> 从 AuthenticateBasicToken 返回, e.g. ("authorization", "Bearer <JWT>").
  std::pair<std::string, std::string> flight_auth_;
};

// preflight 探针 — 取本周配额 (不消耗配额), 终端彩色打印一行剩余比例 (绿 ≥ 50% / 黄 ≥ 20% / 红),
// 返回原值. 是否放行由调用方按 config::BIGQUANT_QUOTA_CONFIRM_RATIO 决定 (GUI 弹窗), 这里只报不判.
Quota preflight(Client &client);

} // namespace bigquant
