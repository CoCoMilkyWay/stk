#pragma once

// =============================================================================
// Bench — Agent → Tools: 主栏按类折叠, 一工具一行一页, 三部分对仗 —— 输入 (随机 / 载入日志里的真实调用 / 手改) → 展示 (该工具独有的界面) ↔ 原文 (交给 agent 的那段)
// =============================================================================
//   作用域自带 (与 Mine 页分开选), 执行走与会话同一条路径 (Kit 的调用一生: Begin → Enter … → Settle / Answer / Abort → End), 原文同样按 kOutChars 截断
//   快照: 载入本作用域一段存过的会话的前 k 轮折叠出假设板; 工作台自己是第 k+1 轮 (调用编号 R<k+1>#<序>, 可作 judge 的证据; 空白快照 = 第 1 轮)
//   快照只在内存: 全局记录只给编号不落盘, 提交只查门槛不拷进正式库; eval_factor 照常落候选文件 (本作用域的候选目录)
//   各工具页共用一份作用域 / 快照 / 工作台轮; 与 Mine 页不并跑: 会话在走时这里不能 Run, 这里在跑时 Mine 页不能 New / 继续
// =============================================================================

#include "gui/task_agent/Bridge.hpp"

#include <memory>
#include <optional>

struct SharedData;

namespace GUI::Agent {

class Bench {
public:
  Bench();
  ~Bench();
  Bench(const Bench &) = delete;
  Bench &operator=(const Bench &) = delete;

  void update(); // 每帧: 收后台结果
  void draw(SharedData &data, Tool tool, Scope &sc, const Gate &gate, bool gpu, bool blocked);
  bool busy() const;
  std::optional<Tool> running() const; // 在跑的调用的工具 (主栏该行标 running)

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace GUI::Agent
