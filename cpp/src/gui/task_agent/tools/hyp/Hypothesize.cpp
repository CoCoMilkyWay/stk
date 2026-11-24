// hypothesize — 提假设: 输入 title / actor / why / expect / refute → HypRes (编号 + 预测) → 原文; 展示见 ui/hyp/Hypothesize.cpp
#include "gui/task_agent/tools/Hyp.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

std::string HypRes::text() const { return "已记为 " + id + " (待检验)"; }

ToolOut Hypothesize(const Call &c, Seat &s) {
  auto r = std::make_shared<HypRes>();
  r->id = s.st.board.next(), r->fields = c.args;
  return Done(std::move(r));
}

json SampleHypothesize(Feed &p) {
  const std::string leaf = p.leaf();
  return {{"title", "示例: " + leaf + " 高的票此后跑赢"},
          {"actor", "因子高 = " + leaf + " 处在池内高位的票 (谁在做什么待补)"},
          {"why", "待补: 信息 / 流动性冲击 / 行为偏差, 收益何时兑现"},
          {"expect", "view_layers factor=CsRank(" + leaf + ") 的 mono 在 0.5 以上"},
          {"refute", "同一观察 mono 低于 0.2"}};
}

} // namespace GUI::Agent
