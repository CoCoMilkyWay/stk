// Factors→Operators: OpTable 全部算子一张表. 静态列 = 表属性, 动态列 = worker 逐行发布的
// 对拍结果 (stream / gpu 各对 naive) 与三后端耗时. 顶部一行参数 + Run / Cancel.
#pragma once

#include "gui/task_factors/services/OperatorsService.hpp"

#include <cstdint>

namespace GUI::Factors {

struct OperatorsUIState {
  OperatorsRequest req; // 页面正在编辑的参数 (Run 时快照给 service)
  // 表列宽贴合: 发布代变了 (限速) → 连发几帧 TableSetColumnWidthAutoAll (对仗 TabFeature)
  uint64_t fit_epoch = ~0ull;
  double fit_last_time = 0.0;
  int fit_frames = 0;
  // 排序 (tristate)
  int sort_column = -1;
  bool sort_ascending = true;
};

// 返回值: 1 = Run 按下, -1 = Cancel 按下, 0 = 无 (由 TaskFactors 转成 service 调用)
int RenderTabOperators(OperatorsService &svc, OperatorsUIState &ui);

} // namespace GUI::Factors
