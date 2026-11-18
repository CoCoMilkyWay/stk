#pragma once

// =============================================================================
// Kit — 工具的实现表、调用的一生与输入取材: 活会话 (Mine) 与工作台 (Bench) 走同一条执行路径
// =============================================================================
//   Seat       当场台面 (GUI 线程): 候选与正式库 (Lab) / 全局记录 / 门槛 / 假设板 / 时间线 (末轮 = 本调用所在轮) / 本调用编号 / GPU 开关
//              dry = 工作台: 全局记录只给编号不落盘, 提交只查门槛不拷进正式库
//   实现表     kImpls (Kit.cpp, 由 AGENT_TOOLS 拼名): 每个工具 = 执行 RunFn + 取材 Sample<工具>; 后台工具另可挂投递前 / 收结果两钩 (仅 eval_factor)
//   调用的一生 Begin → (Enter …) → Settle (后台) | Answer (等人) | Abort (取消) → End
//   Feed       随机输入的取材 (开放的叶 / 候选名 / 评估期交易日 / 池内代码 / 本层时刻 / 假设板 / 时间线); SampleArgs 按工具出一组合参数表的参数
// =============================================================================

#include "gui/task_agent/Bridge.hpp"
#include "gui/task_agent/Notes.hpp"
#include "gui/task_agent/Prompt.hpp"
#include "gui/task_agent/Tools.hpp"
#include "nlohmann/json.hpp"

#include <random>
#include <string>
#include <vector>

namespace GUI::Agent {

struct Seat {
  Lab &lab;
  Notes &notes;
  const Gate &gate;
  const State &st;
  Rounds timeline; // 末轮 = 本调用所在轮
  Ref me;          // 本调用的编号
  std::string scope, session;
  bool dry = false;
  bool gpu = false; // eval_factor 的回测上 GPU
};

// 后台工具的两钩 (GUI 线程): 投递前 (false = 当场已收, 不投) / 收结果
using StageFn = bool (*)(Act &, Seat &);
using SettleFn = void (*)(Act &, ToolOut, Seat &);

// ---- 调用的一生 ----
Exec Begin(Act &a, Seat &s, double t);         // 开始并分派; 返回 = 这一步落在谁手上 (Sync = 已当场收)
void Enter(Act &a, LabStep step, double t);    // 后台报步 (eval_factor 的 Search / Run 计时)
void Settle(Act &a, ToolOut out, Seat &s);     // 后台结果落进 Act
void Answer(Act &a);                           // 研究员答完, 成文落进 Act
std::string Abort(Act &a);                     // 取消在跑的调用; 返回 = 要删的候选文件 (落了文件的回测, 否则空)
void End(Act &a, State &st, Ref at, double t); // 收尾: 时段 / 折进状态 / 原文超 kOutChars 字截断

struct Feed {
  const Context &ctx;
  const std::vector<Result> &tried;
  const Board &board;
  Rounds timeline;
  Ref me;
  std::mt19937_64 &rng;

  int below(int n); // [0, n)
  bool coin(double p);
  template <class T>
  const T &one(const std::vector<T> &v) {
    return v[static_cast<size_t>(below(static_cast<int>(v.size())))];
  }
  std::string leaf();                   // 开放的叶
  std::string factor();                 // 可见且有权重 / 代表点的候选名; 没有就拿开放的叶套 CsRank
  std::string day();                    // 评估期内的交易日 YYYY-MM-DD
  std::string code();                   // 池内代码
  std::string clock();                  // 本层某一行的行末时刻 (日层空)
  std::string hyp(bool supported);      // 假设板上的编号 (supported: 判为支持且变体有余的优先); 板空给 H1
  std::vector<std::string> refs(int n); // 做成了的调用编号 (新到旧)
};
nlohmann::ordered_json SampleArgs(Tool t, Feed &p);

} // namespace GUI::Agent
