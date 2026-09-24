// Factors→Inspect: 单因子展示页. 对象 = Factors 页表格里点行高光的那一个 (FactorsUIState::view_file); 分层 / 分组收益等图
// 在这里画. 目前只有骨架: 顶部显示选中的因子 (文件 / 规范串 / note), 没选则提示.
#pragma once

#include "gui/task_factors/services/FactorsService.hpp"
#include "gui/task_factors/ui/TabFactors.hpp" // FactorsUIState / FactorsUIContext

namespace GUI::Factors {

void RenderTabInspect(FactorsService &svc, const FactorsUIState &ui, const FactorsUIContext &ctx);

} // namespace GUI::Factors
