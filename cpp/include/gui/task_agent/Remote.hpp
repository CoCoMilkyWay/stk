#pragma once

// =============================================================================
// Remote — 外部 agent 的 MCP 接口: 外部 agent 当会话 (Mine) 的大脑 (说明书 = 仓库 AGENT.md)
// =============================================================================
//   传输  MCP Streamable HTTP 的最小子集: 本机 kRemoteHost:kRemotePort kRemotePath 收 POST 的 JSON-RPC 2.0 (一条一个请求, 回 application/json;
//         不开 SSE 流, GET 回 405); 通知与应答 (没有 id / 没有 method) 回 202; Origin 不是本机的回 403 (防 DNS 重绑)
//         网络线程 (asio 单 io_context) 当场答 initialize / ping / tools/list; tools/call 入队交 GUI 线程, 答复由 GUI 线程投回网络线程写出
//         监听即把地址写进 output/run/agent/remote.json (py/agent.py 读它)
//   工具  tools/list = 接口工具 + 研究工具表的各工具 (inputSchema 由同一张参数表出; 直接调 = 只有它一个调用的批)
//         open    开会话 (大脑 = 外部 agent; 作用域 / 本会话要求 / 自检可给, 其余规则取 Config 页的) → 首条消息;
//                 已有研究员开好的、在等外部 agent 的会话 → 接上它, 交回在等的那条消息
//         doc     系统提示 / recap 研究树 (TreeText) / run 交一批 (Mine::reply) / wait 续等 / cancel 作废本批余下的 (Mine::drop)
//         一批交出后至多等 kRemoteWait 秒 (wait 至多 kRemoteWaitMax): 下一条消息发出即交回它 (TextOf; 重发要求 isError), 停下 / 关掉 / 回退即交回原因, 超时交回进度
// =============================================================================

#include "gui/task_agent/Mine.hpp"

#include <memory>
#include <string>

struct SharedData;

namespace GUI::Agent {

class Remote {
public:
  Remote(); // 开监听 (端口被占即终止) + 写地址文件
  ~Remote();
  Remote(const Remote &) = delete;
  Remote &operator=(const Remote &) = delete;

  // GUI 线程每帧 (Mine::update 之后): 收到的 tools/call 交会话 m; sc = Agent 模块的作用域 (open 换它); setup / leg = Config 页此刻的 (open 时取);
  // bench = 工作台有调用在跑 (不开会话)
  void update(SharedData &data, Mine &m, Scope &sc, const Mine::Setup &setup, const Mine::Leg &leg, bool bench);
  const std::string &listen() const; // 监听地址 "http://127.0.0.1:47821/mcp"

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace GUI::Agent
