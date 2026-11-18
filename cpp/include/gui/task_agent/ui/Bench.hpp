#pragma once

// =============================================================================
// Bench — Agent → Tools: 主栏按类折叠, 一工具一行一页 —— 上半左输入 (随机 / 载入日志里的真实调用 / 手改; Run 行后即当前输入的调用) ↔ 右原文 (交给 agent 的那段),
//         下半全宽展示 (该工具独有的界面)
// =============================================================================
//   作用域就是 Mine 页的那份 (这里不显示不改; 不能用时只给警告), 执行走与会话同一条路径 (Kit 的调用一生: Begin → Enter … → Settle / Answer / Abort → End), 原文同样按 kOutChars 截断
//   调用随输入实时对上: 工作台里跑过同样参数的显示它的编号 / 结局 / 原文 / 展示; 下拉选跑过的一次即载回它的参数
//   快照: 载入日志里的一次调用, 即取那段会话在它之前的时间线 (前几轮 + 本轮它之前的调用) 折出假设板, 工作台接着那一轮编号 (与日志同号); 空白 = 第 1 轮
//   快照只在内存: 全局记录只给编号不落盘, 提交只查门槛不拷进正式库; eval_factor 照常落候选文件 (本作用域的候选目录)
//   各工具页共用一份快照 / 工作台轮; 与 Mine 页不并跑: 会话在走时这里不能 Run, 这里在跑时 Mine 页不能 New / 继续 / 载入, 也不能改作用域
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
  // sc = Mine 页的作用域; blocked = Mine 页的会话在走 (不能 Run)
  void draw(const SharedData &data, Tool tool, const Scope &sc, const Gate &gate, bool gpu, bool blocked);
  bool busy() const;                   // 有调用在跑 (含等研究员答)
  std::optional<Tool> running() const; // 在跑的调用的工具 (主栏该行标 running)

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace GUI::Agent
