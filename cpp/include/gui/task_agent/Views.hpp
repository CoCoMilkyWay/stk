#pragma once

// =============================================================================
// Views — 观察核: 9 个特征库视图 (分块流式 + ForkJoin 全核归约, 零中间 [T][A] 平面搬运) + view_book 逐笔重放
// =============================================================================
//   每个观察工具一个入口 ToolOut(Call, ViewEnv, FactorFlow) (tools/look/<工具>.cpp, 公共件 tools/look/Kernel.hpp), 分派表在 Views.cpp:
//     特征库视图  K 线叶直接钉进 Block::x 原地读 (不进 DAG, 不跑 Stat), 特征子式直接走 DAG 求值 (不包假 CsRank),
//                 因子根只跑一次 Stat; 快照 / 个股视图在装载前先核日期并跳过无关块, 全期视图分块流式全核归约
//     view_book   单 (资产, 日) 逐笔帧重放进常驻 LimitOrderBook, 原地汇总
// =============================================================================

#include "gui/engine/Table.hpp"
#include "gui/task_agent/Tools.hpp"

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

class ViewRunner {
public:
  struct Replay;
  ViewRunner();
  ~ViewRunner();
  ViewRunner(const ViewRunner &) = delete;
  ViewRunner &operator=(const ViewRunner &) = delete;

  ToolOut run(const Call &c, const ViewEnv &env, Engine::FactorFlow &F);
  std::string rep_layers(const std::string &expr, int sign, const ViewEnv &env, Engine::FactorFlow &F);

private:
  std::unique_ptr<Replay> book_;
};

} // namespace GUI::Agent
