#pragma once

// =============================================================================
// Task Agent — LLM 挖因子的 GUI 入口 (左栏顶层任务, 与 Factors 页互不依赖); 只画, 会话逻辑在 agent/Mine.hpp
// =============================================================================
//   Mine    作用域 (池 / 因子层 / 持有期) + New / Start / Stop / Cancel; 左栏 问题 (Choose 中) / 回复 + 特征 / 算子
//           (本会话发给 agent 的同一份表, 人读题看模板用); 右栏结果表 (候选目录, 本持有期 score 降序)
//   Config  LLM 后端 / 直连或代理 / 模型 (嗅探), 评估后端, 入库门槛, 提示词模板 (查看 / 编辑 / 保存)
//   Log     本段对话逐轮: user / 思考 / 回复 / 用量
//   选项 (作用域 + Config) 改了即写 kUiFile, 首次 Draw 读回; 与项目只经 agent/Bridge.hpp; 配置 agent/Config.hpp
// =============================================================================

#include "gui/Tasks.hpp"

namespace agent {

GUI::TaskHandle CreateAgentTask();

} // namespace agent
