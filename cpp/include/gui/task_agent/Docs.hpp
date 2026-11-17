#pragma once

// =============================================================================
// Docs — 研报库工具: search_docs / search_factors / read_doc (在 Lab worker 内执行, 不卡界面)
// =============================================================================
//   库 = output/data/doc 下的加工结果, 只读:
//     resources-tag/report/<机构>/<系列>/<id>.json        标注索引, 首次调用加载一次
//     resources-tag/tag_stat.md                          阶段 → 流派分面树骨架
//     resources-proc/report/<机构>/<系列>/<id>/report.md  全文 (去图片行); 按篇懒加载并缓存, 全文检索逐篇看取消旗
// =============================================================================

#include "gui/task_agent/Tools.hpp"

#include <atomic>

namespace GUI::Agent {

ViewOut Browse(const Call &c, const std::atomic<bool> &cancel);

} // namespace GUI::Agent
