#pragma once

// =============================================================================
// LLM 对话客户端: 纯传输, 不知因子, 不留对话态. send 每次交整份对话 (Turns), 阻塞到整轮收齐
// =============================================================================
//   两后端同一个 send, 都当通用接口: 上下文由调用方每次整份给 (task_agent/Context), 远端不留历史; 结局在边界归一成 End
//   Zhipu   chat/completions: system + Turns 逐条作 messages; stream=true (思考可达数分钟, 非流式会读超时);
//           JSON 模式 (response_format json_object) + 思考 (thinking enabled, reasoning_effort); system 逐字节不变 → 平台隐式缓存
//   Cursor  Cloud Agents API: 每次 POST /v1/agents 建一个无仓库 agent, prompt = system + 摊平的 Turns (Flat), 只跑这一轮;
//           回复走 run 的 SSE 流 (assistant / thinking 增量, result 收尾; 断线按 Last-Event-ID 续); 无 JSON 模式, 靠提示词约束
//   HTTPS: asio ssl::stream (OpenSSL) 上跑 beast http, 校验证书链与主机名 (SNI); 全部连接共用一个 TLS 上下文
//   连接: 直连 / 经 HTTP 代理 (CONNECT 隧道, TLS 仍端到端到目标主机); 每个请求按调用方给的 Net 走
//   重试: 网络瞬抖 / 智谱限流按 Config 一直重试; 非幂等的 POST (Cursor 建 agent) 只重发未发出的 (连接 / 代理隧道 / TLS 握手)
// =============================================================================

#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Enum.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace GUI::Agent::llm {

enum class Backend : uint8_t { Zhipu,
                               Cursor }; // 下标 = kBackends
AGENT_ROWS(Backend, kBackends, Cursor);

// 该后端的 key (首次调用读 output/config/agent/keys.json, 之后不再读); 空 = 未填
const std::string &key(Backend b);

struct Net {
  bool proxy = false; // false = 直连
  std::string host;   // HTTP 代理 (proxy 时)
  int port = 0;
  bool operator==(const Net &) const = default;
};

struct Model {
  std::string id, name; // name = 显示名 (Zhipu 不给, = id)
};
// 嗅探该后端账号可用的模型 (GET models, 阻塞; key 须已填): Zhipu 新的在前 / Cursor 按服务端序 (default = 账号默认模型在首).
// 非 2xx / 代理拒绝 → error; cancel 置位 → 都空
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

// 一次请求的对话 (system 之外): user / assistant 按序交替, 首末都是 user
struct Turn {
  bool user;
  std::string text;
};
using Turns = std::vector<Turn>;
// 摊成一段文本 (Cursor 的 prompt / 会话日志): 只一条 = 原文; 多条 = 逐条标明谁说的, 末条 = 本条消息
std::string Flat(const Turns &t);

struct Usage {
  int prompt = 0, cached = 0, completion = 0; // Cursor 流里不报 (0)
  Usage &operator+=(const Usage &o) {
    prompt += o.prompt, cached += o.cached, completion += o.completion;
    return *this;
  }
};

// 一轮的结局: Ok 正常收尾 / Cut 平台收尾但不完整 (截断 / 拒答 / 过期 …) / Error 接口失败 / Cancelled 本地取消
enum class End : uint8_t { Ok,
                           Cut,
                           Error,
                           Cancelled };
// 结局表 (下标 = End): id = 流水里的 end
struct EndDef {
  const char *id;
};
inline constexpr EndDef kEnds[] = {{"ok"}, {"cut"}, {"error"}, {"cancelled"}};
AGENT_ROWS(End, kEnds, Cancelled);

// 一轮回复: 在收 (增量累积) 与收齐同形
struct Reply {
  std::string text, thinking;
  End end = End::Error;
  std::string error; // 非 Ok 的原因: Cut = 平台原话 (Zhipu finish_reason / Cursor 终态); Error = HTTP 非 2xx / 代理拒绝 / 流异常收尾 / 流里的错误 / POST 发出后断网
  Usage usage;
  std::string id; // 对账: Zhipu 响应 id / Cursor run id
  double seconds = 0.0;
  bool ok() const { return end == End::Ok; }
};

// 增量回调 (send 所在线程): 正文流 / 思考流 / 重来 (整轮重发, 已收的增量作废; chunk 空)
enum class Part : uint8_t { Text,
                            Thinking,
                            Restart };
using Delta = std::function<void(Part part, std::string_view chunk)>;

class Session {
public:
  explicit Session(Options o); // 该后端的 key 为空 → assert

  // 发整份对话 t, 阻塞到整轮收齐. cancel 置位 → 1 秒内断流, End::Cancelled (Cursor: 已拿到 run id 则远端 run 一并取消,
  // 建 agent 的 POST 途中取消则远端那轮不管). 失败不抛, End::Error + error (stderr 另打响应体)
  Reply send(const Turns &t, const Delta &on_delta, const std::atomic<bool> &cancel);

private:
  Reply zhipu(const Turns &t, const Delta &on_delta, const std::atomic<bool> &cancel);
  Reply cursor(const Turns &t, const Delta &on_delta, const std::atomic<bool> &cancel);

  Options o_;
};

} // namespace GUI::Agent::llm
