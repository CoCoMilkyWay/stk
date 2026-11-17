#pragma once

// =============================================================================
// Kit — 工具的执行台与输入取材: 活会话 (Mine) 与工作台 (Bench) 走同一条执行路径
// =============================================================================
//   Seat       当场工具读写的会话台面: 候选与正式库 (Lab) / 全局记录 / 门槛 / 假设板 / 时间线 (末轮 = 本调用所在轮) / 本调用编号
//              dry = 工作台: 全局记录只给编号不落盘, 提交只查门槛不拷进正式库
//   RunSync    当场工具做完, 结果 (原文 / 关键数 / 结构化结果) 落进 Act
//   SendLab    后台工具投递: eval_factor 先核假设并落候选文件 (没落成 = 当场填结果, 返回 false); 其余直接投 Lab
//   SettleLab  后台结果落进 Act
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
};

void Put(Act &a, ToolOut o);
void Clip(std::string &s); // 原文超 kOutChars 字截断并说明
void RunSync(Act &a, Seat &s);
bool SendLab(Act &a, Seat &s, bool gpu);
void SettleLab(Act &a, LabOut out, const Gate &g);

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
