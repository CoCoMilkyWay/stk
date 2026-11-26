#pragma once

// =============================================================================
// 回测 (eval_factor) 的结果: 计算与原文在 tools/trial/<工具>.cpp, 展示在 ui/trial/<工具>.cpp
// =============================================================================
//   eval_factor   Lab 形: 核验落盘 (GUI 备料) → Search + Run (Lab) → 入表封存 (GUI 收尾 Commit); 账本作用 = 落下的候选文件 + 结局; 作废 = 删候选记未落; 存档复原
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <memory>
#include <string>
#include <vector>

namespace GUI::Layer {
struct LayerDerived;
}

namespace GUI::Agent {

struct TrialRes final : Res {
  Staged st;
  Gate gate;
  std::string level, hold;                          // 频率 (因子层) / 预测周期 (持有期) = 会话作用域的 Context::level / hold
  mutable int hot = -1;                             // 展示: 上帧悬停的参数点 (Sweep::pts 下标; 图 ↔ 表联动)
  mutable int cursor = -1;                          // 展示: 分层图的光标日
  mutable std::shared_ptr<Layer::LayerDerived> lay; // 展示: st.res.daily 派生的分层线 (首画时建)
  const Weight *shown() const;                      // 可展示统计的最佳权重 (有权重且在当前评估期上; 否则空)
  std::string text() const override;
  std::string gist() const override;  // 折叠后的一行: 名 + 模板 + 思路 + 结局 (落了文件才有)
  std::string title() const override; // 英文名 + 中文名 + 结局
  Effect effect() const override;
  bool abort() override;
  nlohmann::ordered_json save() const override;
  static ResPtr load(const Call &c, const nlohmann::ordered_json &j, Seat &s); // 候选的结局取候选台此刻的
  void draw() const override;
};

// eval_factor 原文与展示同一组节 (两边逐节对仗; 都只在 shown() 非空时)
std::string Formula(const Weight &w);                       // 权重的具体式, 符号为负 (已整体取反) 即前置 -
std::string ScopeLine(const TrialRes &k);                   // 频率 · 预测周期
std::string WeightLine(const TrialRes &k, const Weight &w); // 一个权重的统计: 最佳权重接门槛; 其余先写具体式
std::string YearsLine(const std::vector<Year> &ys);         // 最佳权重分年: 年份(中位rIC,年换手), …
std::string GroupsHead();                                   // 最佳权重的 20 组
std::string SweepHead(const Sweep &sw);                     // 参数扫描 (点数)

} // namespace GUI::Agent
