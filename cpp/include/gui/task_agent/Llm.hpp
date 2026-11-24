#pragma once

// =============================================================================
// Llm — 左翼 · 大模型传输与异步对话管道 Chat (纯传输, 不知因子, 不留对话态)
// =============================================================================
//   三后端由驱动表 kDrivers 分派, 同一签名 models(net, cancel) / send(opt, system, turns, on_delta, cancel):
//     Zhipu   chat/completions: system + Turns 逐条作 messages; stream=true, JSON 模式 + 思考; system 不变命中平台缓存
//     Cursor  Cloud Agents API: POST /v1/agents 建无仓库单轮 agent, prompt = system + Flat(Turns); SSE 流支持断线按 Last-Event-ID 续传
//     Devin   本机 devin acp 子进程: authenticate (key) → session/new → 选模型 → session/prompt, prompt 同 Cursor; 思考 / 正文分片流式, 工具权限一律拒, 收尾删会话
//   HTTPS: asio ssl::stream (OpenSSL) + beast http, 全程 error_code 返回值流转, 零异常控制流
//   Chat:  Async<ChatReq, ChatStep, Reply, ChatLive>, 与右翼 Lab 同构同接口 (send / poll / live / busy / cancel)
// =============================================================================

#include "gui/task_agent/Async.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Enum.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace GUI::Agent::llm {

enum class Backend : uint8_t { Zhipu,
                               Cursor,
                               Devin,
                               Mcp }; // 下标 = kBackends
AGENT_ROWS(Backend, kBackends, Mcp);

// 该后端的 key (首次调用读 output/config/agent/keys.json); 空 = 未填
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
struct Models {
  std::vector<Model> list;
  std::string error; // 非空 = 嗅探失败的原因
};
Models models(Backend b, const Net &net, const std::atomic<bool> &cancel);

struct Options {
  Backend backend = Backend::Zhipu;
  Net net;
  std::string model;
  bool operator==(const Options &) const = default;
};

// 一次请求的对话 (system 之外): user / assistant 按序交替, 首末都是 user
struct Turn {
  bool user;
  std::string text;
};
using Turns = std::vector<Turn>;
std::string Flat(const Turns &t);

struct Usage {
  int prompt = 0, cached = 0, completion = 0;
  Usage &operator+=(const Usage &o) {
    prompt += o.prompt, cached += o.cached, completion += o.completion;
    return *this;
  }
};

// 一轮的结局: Ok 正常收尾 / Cut 平台截断或拒答 / Error 接口失败 / Cancelled 本地取消
enum class End : uint8_t { Ok,
                           Cut,
                           Error,
                           Cancelled };
struct EndDef {
  const char *id;
};
inline constexpr EndDef kEnds[] = {{"ok"}, {"cut"}, {"error"}, {"cancelled"}};
AGENT_ROWS(End, kEnds, Cancelled);

struct Reply {
  std::string text, thinking;
  End end = End::Error;
  std::string error;
  Usage usage;
  std::string id;
  double seconds = 0.0;
  bool ok() const { return end == End::Ok; }
};

enum class Part : uint8_t { Text,
                            Thinking,
                            Restart };
using Delta = std::function<void(Part part, std::string_view chunk)>;

// 发送整份对话 (阻塞到整轮收齐)
Reply send(const Options &o, std::string_view system, const Turns &t, const Delta &on_delta, const std::atomic<bool> &cancel);

} // namespace GUI::Agent::llm

namespace GUI::Agent {

enum class ChatStep : uint8_t { Think,
                                Write,
                                Restart,
                                End };
struct ChatStepDef {
  const char *cn;
};
inline constexpr ChatStepDef kChatSteps[] = {{"思考"}, {"输出"}, {"重试"}, {"完"}};
AGENT_ROWS(ChatStep, kChatSteps, End);
struct ChatReq {
  llm::Options opt;
  std::shared_ptr<const std::string> system;
  llm::Turns turns;
};
struct ChatLive {
  uint64_t gen = 0;
  llm::Reply reply;
};
inline void sync_live(ChatLive &dst, const ChatLive &src, uint64_t gen) {
  if (dst.gen != gen) {
    dst.gen = gen, dst.reply = src.reply;
    return;
  }
  dst.reply.text.append(src.reply.text, dst.reply.text.size());
  dst.reply.thinking.append(src.reply.thinking, dst.reply.thinking.size());
}

struct Chat : Async<ChatReq, ChatStep, llm::Reply, ChatLive> {
  Chat();
};

} // namespace GUI::Agent
