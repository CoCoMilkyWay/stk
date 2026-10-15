#pragma once

#include <arrow/table.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace tushare {

// 请求参数 {key, value}; 值一律字符串 (日期 "YYYYMMDD")
using Params = std::vector<std::pair<std::string, std::string>>;

// 一次查询 (值类型) — 与 bigquant::Query 对仗
//   api:    接口名 (api_name)
//   params: 请求参数
//   num:    声明为 double 的列, 其余 → string (见 Client::query)
struct Query {
  std::string api;
  Params params;
  std::vector<std::string> num;
};

// Tushare HTTP 客户端 — 与 bigquant::Client 对仗: 纯传输, 不知 Spec.
class Client {
public:
  // 缺省构造: token 取 config.hpp::TUSHARE_TOKEN
  Client();
  // 显式指定 (测试 / 多账号切换)
  explicit Client(std::string token);

  // POST http://api.tushare.pro
  // body = {api_name, token, params, fields:""} — 始终拉默认显示列 (默认隐藏列无需求).
  // 响应 data.{fields, items} → arrow::Table (0 行 → 0 行表, schema 仍完整):
  //   num 内的列 → double, 其余 → string. 不按数据推断: 全 null 列无从推断, 同一列会跨调用漂移.
  //   num 声明的列必须出现在响应 fields 里 (拼错 / API 改名立刻暴露).
  // 网络瞬抖 / 频率超限 (40203) 共用重试预算; 其它 code != 0 / 解析失败 → assert.
  std::shared_ptr<arrow::Table> query(const Query &q);

private:
  std::string token_;
};

} // namespace tushare
