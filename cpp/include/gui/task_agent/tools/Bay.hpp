#pragma once

// =============================================================================
// Bay — 后台工具的台面 (Lab 线程): 与当场工具的台面 Seat (tools/Kit.hpp) 对仗
// =============================================================================
//   GUI 线程投递时备好值 (观察口径 / 回测作用域 / 候选文件), worker 只读; 本件的计算流 / 取消旗 / 报步由 Lab 给
//     观察 (tools/look)    读 env, 在 flow 上分块流式全核归约 / 逐笔重放
//     研报 (tools/query)   只看取消旗
//     回测 (eval_factor)   读 fsc / file, Search → Run → 代表点分层; 评估产物交回 graded (Lab 收下后重扫候选)
// =============================================================================

#include "gui/engine/Table.hpp"
#include "gui/task_agent/Bridge.hpp"

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

struct OrdersCatalog;

namespace GUI::Engine {
struct FactorFlow;
}

namespace GUI::Agent {

struct CandRef {
  std::string expr, label, error;
  int sign = 1;
};

// 观察口径
struct ViewEnv {
  Engine::FactorScope fsc;
  int hold = 0;
  int first = 0, last = 0; // 当前作用域评估期首末日 YYYYMMDD
  int open = 0;            // 每日开头集合竞价行数
  std::string level;
  std::vector<std::string> clock; // [seg] 行末时刻 HH:MM
  bool lob_open = true;
  std::set<std::string> open_leaves;
  std::vector<bool> open_ops;
  std::map<std::string, CandRef> cands;
  std::shared_ptr<const OrdersCatalog> orders;
  L2::ExchangeType exchange = L2::ExchangeType::UNKNOWN;
};

// eval_factor 交回 Lab 的评估产物
struct Graded {
  Surface surf;    // Search 评过的参数曲面
  std::string rep; // 无显著时代表点的分层
};

struct Bay {
  const ViewEnv &env;
  const Engine::FactorScope &fsc; // eval_factor 的回测作用域 (按 GPU 开关)
  const std::string &file;        // eval_factor 的候选文件
  Engine::FactorFlow &flow;       // 本件的计算流 (观察 = 交互优先, 回测 = 批量)
  const std::atomic<bool> &cancel;
  const std::function<void(LabStep)> &step;
  Graded &graded;
  bool cancelled() const { return cancel.load(std::memory_order_relaxed); }
};

} // namespace GUI::Agent
