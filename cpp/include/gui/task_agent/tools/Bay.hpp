#pragma once

// =============================================================================
// Bay — 后台工具的只读计算台面 (Lab 线程): 与当场工具的会话台面 Seat (tools/Kit.hpp) 严格对仗
// =============================================================================
//   Opened   稠密下标位表 ([LeafId] / [OpId]), O(1) 判开放, Bridge 与观察核共用同一份
//   ViewEnv  观察口径不可变快照 (Lab 缓存 shared_ptr<const ViewEnv>, 仅作用域 / 候选变动时刷新, 跨线程零拷贝)
//   Bay      全部后台工具统一收这四件: env / flow / cancel / step (无任何单工具专属字段)
// =============================================================================

#include "gui/engine/Table.hpp"
#include "gui/task_agent/Bridge.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
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

struct Opened {
  std::vector<uint8_t> leaves; // [LeafId]
  std::vector<uint8_t> ops;    // [OpId]
  bool lob_open = true, has_lob = false;

  bool has(const Engine::FeatureTable &ft, const factor::expr::Node &n) const {
    if (n.target)
      return true;
    return n.op >= 0 ? ops[static_cast<size_t>(n.op)] != 0 : leaves[ft.x.index.at(n.feat)] != 0;
  }
  bool all_open(const Engine::FeatureTable &ft, const factor::expr::Expr &e) const {
    return std::ranges::all_of(e.nodes, [&](const factor::expr::Node &n) { return has(ft, n); });
  }
  std::string closed_err(const Engine::FeatureTable &ft, const factor::expr::Expr &e) const;
};

struct ViewEnv {
  Engine::FactorScope fsc;
  int hold = 0;
  int first = 0, last = 0; // 当前作用域评估期首末日 YYYYMMDD
  int open = 0;            // 每日开头集合竞价行数
  std::string level;
  std::vector<std::string> clock; // [seg] 行末时刻 HH:MM
  Opened opened;
  std::unordered_map<std::string, CandRef> cands;
  std::shared_ptr<const OrdersCatalog> orders;
  std::vector<L2::ExchangeType> exchanges; // [asset_id]
};

struct Bay {
  const ViewEnv &env;
  Engine::FactorFlow &flow;
  const std::atomic<bool> &cancel;
  const std::function<void(LabStep)> &step;
  bool cancelled() const { return cancel.load(std::memory_order_relaxed); }
};

Engine::FactorScope FactorScopeOf(const Lab &lab, bool gpu);

} // namespace GUI::Agent
