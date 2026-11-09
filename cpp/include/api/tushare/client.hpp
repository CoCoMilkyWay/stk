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
// pro token; *_vip 接口需 5000+ 积分
inline constexpr const char *kToken = "6b5ea435a4626b1eeedefb2115bcf9e84fc64a0d212d21cf2be03d54";
inline constexpr const char *kHost = "api.tushare.pro";
inline constexpr const char *kPort = "80"; // 走 80, 省掉 SSL 依赖
inline constexpr int kTimeoutSeconds = 60; // 单次连接 + 读写整体时长; range 接口序列化耗时可达 20s
inline constexpr int kRetryMax = 4;        // 额外重试次数 (共 5 次尝试), 线性间隔
inline constexpr int kRetryIntervalSeconds = 30;

class Client {
public:
  // 缺省构造: token 取 kToken
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
