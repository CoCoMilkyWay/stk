#pragma once

// =============================================================================
// Mcp — 外部 agent 经 MCP 调本模块的工具 (说明书 = 仓库 AGENT.md; 没有 MCP 客户端的用 py/agent.py)
// =============================================================================
//   传输  本机 Streamable HTTP (kMcpHost:kMcpPort kMcpPath; 只回 application/json, 不开 SSE 流): 网络线程 (asio 单 io_context) 收发 JSON-RPC
//         initialize / ping / tools/list / tools/call; tools/call 入队交 GUI 线程, 答复由 GUI 线程投回网络线程写出;
//         监听即把地址写进 output/run/agent/mcp.json (py/agent.py 读它)
//   执行  GUI 线程每帧 update: 取请求 → 自带的 Rig (Lab + Notes + 时间线; 与 Mine / Bench 同一执行路径) 按序执行一批调用
//         口径 = 不查假设板 (Seat::free) + 真落全局记录 / 达门槛真入正式库; 一批 = 时间线的一轮 = 一个作业, 调用编号 R<批>#<序>
//   工具  doc (随作用域的完整说明) / scope (看与换作用域) / run (交一批调用) / wait (续取) / cancel (取消在跑的批) / unlock (交还);
//         run / wait 至多等 kMcpWait / kMcpWaitMax 秒, 没做完交回作业号与进度
//   锁    只在内存 (程序一开 = 没锁): agent 的头一个请求即替它锁上 (研究员在 GUI 里有会话 / 工作台在跑时退回让它稍后);
//         锁着时 Agent 模块的 GUI (Mine / 工作台) 全禁用, 人解不了, 只有 agent 调 unlock 才解; 锁上时重绑作用域、重读全局记录
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/tools/Kit.hpp"

#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Agent {

class Mcp {
public:
  Mcp(); // 开监听 (端口被占即断言) + 写地址文件
  ~Mcp();
  Mcp(const Mcp &) = delete;
  Mcp &operator=(const Mcp &) = delete;

  // GUI 线程每帧: sc / gate / gpu = Agent 模块的作用域 (scope 工具就地改它) / 入库门槛 / 回测上 GPU; human = 研究员在 GUI 里用着 (有会话 / 工作台在跑)
  void update(SharedData &data, Scope &sc, const Gate &gate, bool gpu, bool human);
  bool locked() const; // agent 锁着 Agent 模块
  bool busy() const;   // 有批在跑 (或 Lab 在收尾)

  const std::string &listen() const; // 监听地址 "http://127.0.0.1:8765/mcp"
  const std::vector<Round> &timeline() const;
  void live(LabLive &dst) const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace GUI::Agent
