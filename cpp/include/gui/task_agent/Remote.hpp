#pragma once

// =============================================================================
// Remote — 外部 agent 的 MCP 接口: 外部 agent 当会话 (Mine) 的大脑 (说明书 = 仓库 AGENT.md)
// =============================================================================
//   传输  MCP Streamable HTTP 的最小子集: 本机 kRemoteHost:kRemotePort kRemotePath 收 POST 的 JSON-RPC 2.0 (一条一个请求, 回 application/json;
//         不开 SSE 流, GET 回 405); 通知与应答 (没有 id / 没有 method) 回 202; Origin 不是本机的回 403 (防 DNS 重绑)
//         网络线程 (asio 单 io_context) 当场答 initialize / ping / tools/list; tools/call 入队交 GUI 线程, 答复由 GUI 线程投回网络线程写出
//         监听即把地址写进 output/run/agent/remote.json (py/agent.py 读它), 退出时删除
//   工具  tools/list = open / doc / run; 研究工具只经 run 的 calls 交, 工具名与参数表唯一以系统提示里为准
//         open    同 GUI 的 New 开新会话 (有会话在跑则退回, 停着的换下): 作用域 / 本会话要求 / 自检可给, 其余规则取 Config 页的 → 首条消息
//         doc     本会话的系统提示
//         run     给 calls = 交一批调用 (Mine::reply); 不给 calls = 接上已有外部会话 / 续等执行中调用 / 取当前状态.
//                 后台执行中至多等 kRemoteWait 秒 (未完先交回进度); ask_human / 停下即刻交回状态, 不空等; finish 收尾那批的结果随结束提示交回
//   消息  常规每轮只交回本条增量 TextOf(rd); 首次接上有历史轮次的会话或研究员回退后自动补完整上下文 llm::Flat(TurnsOf(timeline))
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

  // GUI 线程每帧 (Mine::update 之后): 收到的 tools/call 交会话 m; setup / leg = Config 页此刻的 (open 时取); bench = 工作台有调用在跑 (不开会话)
  void update(SharedData &data, Mine &m, const Mine::Setup &setup, const Mine::Leg &leg, bool bench);
  const std::string &listen() const; // 监听地址 "http://127.0.0.1:47821/mcp"

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace GUI::Agent
