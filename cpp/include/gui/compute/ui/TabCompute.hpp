// Tab Compute - 特征计算面板: 库表 (一行一池, 每池各档一库) + 核布局 + Rebuild / Append / 删除 + 进度 (Prefetch + TS + CS + IO workers)
#pragma once

#include "gui/compute/ComputeHub.hpp"

struct SharedData;

namespace GUI::Compute {

void RenderTabCompute(ComputeHub &hub, SharedData &data, LibSide side);

} // namespace GUI::Compute
