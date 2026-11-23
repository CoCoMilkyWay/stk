#pragma once

// =============================================================================
// 回测 (eval_factor / submit_factor) 的结果: 计算与原文在 tools/trial/<工具>.cpp, 展示在 ui/trial/<工具>.cpp
// =============================================================================
//   eval_factor   后台三拍对仗 (StagedLab): OpenEvalFactor 核假设落盘 (GUI) → LabTask 跑 Search + Run (Lab) → CloseEvalFactor 入表封存 (GUI)
//   submit_factor 当场 (SyncFn): 查门槛并拷进正式库 (自检会话 / 工作台只查门槛)
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace GUI::Layer {
struct LayerDerived;
}

namespace GUI::Agent {

struct TrialRes final : Res {
  Staged st;
  Span search, run;
  Gate gate;
  mutable int hot = -1;                             // 展示: 上帧悬停的参数点 (Sweep::pts 下标; 图 ↔ 表联动)
  mutable int cursor = -1;                          // 展示: 分层图的光标日
  mutable std::shared_ptr<Layer::LayerDerived> lay; // 展示: st.res.daily 派生的分层线 (首画时建)
  std::string text() const override;
  void draw() const override;
};

// eval_factor 原文与展示同一组节标题 (两边逐节对仗)
std::string GroupsHead();                           // 最佳权重的 20 组
std::string YearsHead(const std::vector<Year> &ys); // 最佳权重的分年 rIC (年数 / 为负几年 / 最差年)
std::string SweepHead(const Sweep &sw);             // 参数扫描 (点数)

inline TrialRes *AsTrial(Act &a) { return a.call.tool == Tool::EvalFactor ? static_cast<TrialRes *>(a.out.res.get()) : nullptr; }
inline const TrialRes *AsTrial(const Act &a) { return a.call.tool == Tool::EvalFactor ? static_cast<const TrialRes *>(a.out.res.get()) : nullptr; }

struct SubmitRes final : Res {
  std::string name, why; // why 非空 = 未入库
  bool test = false, dry = false;
  std::optional<Result> cand;
  std::string text() const override;
  void draw() const override;
};

std::optional<LabTask> OpenEvalFactor(Act &a, Seat &s);
void CloseEvalFactor(Act &a, ToolOut out, Seat &s);
inline constexpr StagedLab EvalFactor{OpenEvalFactor, CloseEvalFactor};

ToolOut SubmitFactor(const Call &c, Seat &s);

nlohmann::ordered_json SampleEvalFactor(Feed &p);
nlohmann::ordered_json SampleSubmitFactor(Feed &p);

} // namespace GUI::Agent
