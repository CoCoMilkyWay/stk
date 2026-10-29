#pragma once

// =============================================================================
// LLM 对话客户端: 纯传输, 不知因子. 一个 Session = 一段多轮对话, send 阻塞到整轮收齐
// =============================================================================
//   Zhipu   chat/completions: 本地留全部 messages 每轮重发; stream=true (思考可达数分钟, 非流式会读超时);
//           JSON 模式 (response_format json_object) + 思考 (thinking enabled, reasoning_effort); system 逐字节不变 → 平台隐式缓存
//   Cursor  Cloud Agents API: 首轮 POST /v1/agents 建无仓库 agent (无 system 角色: system 拼在首条 prompt 前), 之后 POST runs 续同一段对话;
//           回复走 run 的 SSE 流 (assistant / thinking 增量, result 收尾; 断线按 Last-Event-ID 续); 无 JSON 模式, 靠提示词约束
//   HTTPS: asio ssl::stream (OpenSSL) 上跑 beast http, 校验证书链与主机名 (SNI). key / 端点 / 超时 / 重试见 agent/Config.hpp
//   连接: 直连 / 经 HTTP 代理 (CONNECT 隧道, TLS 仍端到端到目标主机); 每个请求按调用方给的 Net 走
// =============================================================================

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace agent::llm {

enum class Backend : uint8_t { Zhipu,
                               Cursor };
inline constexpr const char *kBackendNames[] = {"Zhipu", "Cursor"};
inline constexpr int kBackendCount = 2;

// 该后端的 key (首次调用读 kKeyFile, 之后不再读); 空 = 未填
const std::string &key(Backend b);

struct Net {
  bool proxy = false; // false = 直连
  std::string host;   // HTTP 代理 (proxy 时)
  int port = 0;
};

struct Model {
  std::string id, name; // name = 显示名 (Zhipu 不给, = id)
};
// 嗅探该后端账号可用的模型 (GET models, 阻塞; key 须已填): Zhipu 新的在前 / Cursor 按服务端序 (default = 账号默认模型在首).
// 网络瞬抖按 Config 一直重试; 非 2xx / 代理拒绝 → error; cancel 置位 → 都空
struct Models {
  std::vector<Model> list;
  std::string error; // 非空 = 嗅探失败的原因 (UI 显示)
};
Models models(Backend b, const Net &net, const std::atomic<bool> &cancel);

struct Options {
  Backend backend = Backend::Zhipu;
  Net net;
  std::string model;  // models() 的 id; Cursor "default" = 账号默认模型 (请求不带 model)
  std::string system; // 静态前缀
};

struct Usage {
  int prompt = 0, cached = 0, completion = 0; // Cursor 流里不报 (0)
};

struct Reply {
  std::string text, thinking;
  std::string finish; // Zhipu finish_reason (stop / length / sensitive …) | Cursor 终态 (FINISHED / ERROR / EXPIRED …) | cancelled (本地取消) | error (接口失败)
  std::string error;  // 失败原因 (finish = error: HTTP 非 2xx / 代理拒绝 / 流异常收尾 / Cursor 的 POST 发出后断网; Cursor ERROR: 流里的错误)
  Usage usage;
  std::string id; // 对账: Zhipu 响应 id / Cursor run id
  double seconds = 0.0;
  bool ok() const { return finish == "stop" || finish == "FINISHED"; }
};

// 增量回调 (send 所在线程): 正文流 / 思考流 / 重来 (整轮重发, 已收的增量作废; chunk 空)
enum class Part : uint8_t { Text,
                            Thinking,
                            Restart };
using Delta = std::function<void(Part part, std::string_view chunk)>;

class Session {
public:
  explicit Session(Options o); // 该后端的 key 为空 → assert

  const Options &options() const { return o_; }
  int rounds() const { return rounds_; }
  const std::string &agent_id() const { return agent_; } // Cursor (首轮后)

  // 发一条 user 消息, 阻塞到整轮收齐. cancel 置位 → 1 秒内断流, finish = "cancelled" (Zhipu: 本轮不进上下文;
  // Cursor: 已拿到 run id 则远端 run 一并取消, 建 run 的 POST 途中取消则远端那轮不管).
  // 网络瞬抖 / 智谱限流按 Config 一直重试 (Cursor 的 POST 非幂等, 只重试发出前 —— 连接 / 代理隧道 / TLS 握手 —— 的网络错);
  // 其余失败不抛, finish = "error" + error 说明 (stderr 另打响应体)
  Reply send(const std::string &user, const Delta &on_delta, const std::atomic<bool> &cancel);

private:
  Reply send_zhipu(const std::string &user, const Delta &on_delta, const std::atomic<bool> &cancel);
  Reply send_cursor(const std::string &user, const Delta &on_delta, const std::atomic<bool> &cancel);

  Options o_;
  std::vector<std::pair<std::string, std::string>> msgs_; // Zhipu: (role, content), 不含 system
  std::string agent_;                                     // Cursor agent id
  int rounds_ = 0;
};

} // namespace agent::llm
